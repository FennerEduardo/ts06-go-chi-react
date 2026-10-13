package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"transactional-system-go-chi/internal/domain"
)

// DefaultAggregate is the aggregate type used when a request does not name one.
const DefaultAggregate = "OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi"

// applied is an aggregate's new state after a command and the recorded event.
type applied struct {
	state   string
	version int64
	event   domain.DomainEvent
}

type aggregateFunc func(id string, state *string, version int64, command string, cmd domain.Command) (applied, error)

// aggregates holds every aggregate of the service, by aggregate type.
var aggregates = map[string]aggregateFunc{
	"OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi": func(id string, state *string, version int64, command string, cmd domain.Command) (applied, error) {
		var aggregate *domain.OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate
		var err error
		if state == nil {
			aggregate, err = domain.NewOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate(id)
		} else {
			aggregate, err = domain.RestoreOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate(id, domain.State(*state), version)
		}
		if err != nil {
			return applied{}, err
		}
		var event domain.DomainEvent
		switch command {
		case "process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi":
			event, err = aggregate.ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi(cmd)
		default:
			return applied{}, fmt.Errorf("%w: unknown command %s", domain.ErrValidation, command)
		}
		if err != nil {
			return applied{}, err
		}
		return applied{state: string(aggregate.State), version: aggregate.Version, event: event}, nil
	},
}

type CommandRequest struct {
	TenantID string
	// AggregateType is the feature's aggregate (e.g. "OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi"); empty means DefaultAggregate.
	AggregateType  string
	AggregateID    string
	Command        string
	Payload        map[string]any
	IdempotencyKey string
	// Traceparent is the incoming W3C trace context (e.g. from the HTTP request).
	Traceparent string
}

type CommandResult struct {
	Status      string `json:"status"`
	AggregateID string `json:"aggregateId"`
	EventType   string `json:"eventType,omitempty"`
	Version     int64  `json:"version,omitempty"`
}

// CommandService executes a command in one transaction: idempotency claim, aggregate load
// (FOR UPDATE), domain logic, aggregate save and outbox insert. A domain error rolls back everything.
type CommandService struct {
	pool *pgxpool.Pool
	s    string
}

func NewCommandService(pool *pgxpool.Pool, schema string) (*CommandService, error) {
	s, err := SchemaName(schema)
	if err != nil {
		return nil, err
	}
	return &CommandService{pool: pool, s: s}, nil
}

func (svc *CommandService) Handle(ctx context.Context, req CommandRequest) (result CommandResult, err error) {
	if req.TenantID == "" {
		return result, fmt.Errorf("%w: tenant id is required", domain.ErrValidation)
	}
	aggregateType := req.AggregateType
	if aggregateType == "" {
		aggregateType = DefaultAggregate
	}
	apply, ok := aggregates[aggregateType]
	if !ok {
		return result, fmt.Errorf("%w: unknown aggregate type %s", domain.ErrValidation, aggregateType)
	}
	ctx, span := Tracer().Start(ContextFrom(ctx, req.Traceparent), aggregateType+"."+req.Command,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("tenant.id", req.TenantID), attribute.String("aggregate.id", req.AggregateID)))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	tx, err := svc.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	if req.IdempotencyKey != "" {
		tag, err := tx.Exec(ctx, "INSERT INTO "+svc.s+".ghk_idempotency (tenant_id, key, status) VALUES ($1, $2, 'PROCESSING') ON CONFLICT DO NOTHING", req.TenantID, req.IdempotencyKey)
		if err != nil {
			return result, err
		}
		if tag.RowsAffected() == 0 {
			var status string
			var response *string
			if err := tx.QueryRow(ctx, "SELECT status, response FROM "+svc.s+".ghk_idempotency WHERE tenant_id = $1 AND key = $2", req.TenantID, req.IdempotencyKey).Scan(&status, &response); err != nil {
				return result, err
			}
			if err := tx.Commit(ctx); err != nil {
				return result, err
			}
			if status == "COMPLETED" && response != nil {
				if err := json.Unmarshal([]byte(*response), &result); err != nil {
					return result, err
				}
				result.Status = "replayed"
				return result, nil
			}
			return CommandResult{Status: "in-progress", AggregateID: req.AggregateID}, nil
		}
	}

	var state string
	var version int64
	var loaded *string
	err = tx.QueryRow(ctx, "SELECT state, version FROM "+svc.s+".ghk_aggregates WHERE tenant_id = $1 AND aggregate_type = $2 AND id = $3 FOR UPDATE", req.TenantID, aggregateType, req.AggregateID).Scan(&state, &version)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		err = nil
	case err == nil:
		loaded = &state
	}
	if err != nil {
		return result, err
	}
	done, err := apply(req.AggregateID, loaded, version, req.Command, domain.Command{ID: req.AggregateID, Payload: req.Payload})
	if err != nil {
		return result, err
	}
	event := done.event
	if _, err = tx.Exec(ctx, "INSERT INTO "+svc.s+".ghk_aggregates (tenant_id, aggregate_type, id, state, version) VALUES ($1, $2, $3, $4, $5) "+
		"ON CONFLICT (tenant_id, aggregate_type, id) DO UPDATE SET state = EXCLUDED.state, version = EXCLUDED.version, updated_at = now()",
		req.TenantID, aggregateType, req.AggregateID, done.state, done.version); err != nil {
		return result, err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return result, err
	}
	var traceparent *string
	if tp := TraceparentOf(ctx); tp != "" {
		traceparent = &tp
	}
	if _, err = tx.Exec(ctx, "INSERT INTO "+svc.s+".ghk_outbox (id, tenant_id, aggregate_id, event_type, payload, traceparent) VALUES ($1, $2, $3, $4, $5, $6)",
		uuid.NewString(), req.TenantID, req.AggregateID, string(event.Type), string(payload), traceparent); err != nil {
		return result, err
	}
	result = CommandResult{Status: "created", AggregateID: req.AggregateID, EventType: string(event.Type), Version: event.Version}
	if req.IdempotencyKey != "" {
		response, _ := json.Marshal(result)
		if _, err = tx.Exec(ctx, "UPDATE "+svc.s+".ghk_idempotency SET status = 'COMPLETED', response = $3 WHERE tenant_id = $1 AND key = $2", req.TenantID, req.IdempotencyKey, string(response)); err != nil {
			return result, err
		}
	}
	return result, tx.Commit(ctx)
}

// Snapshot is an aggregate as stored for a tenant.
type Snapshot struct {
	State   string
	Version int64
}

// Load returns the aggregate as tenantID sees it (nil for other tenants' aggregates).
func (svc *CommandService) Load(ctx context.Context, tenantID, aggregateID string) (*Snapshot, error) {
	return svc.LoadType(ctx, DefaultAggregate, tenantID, aggregateID)
}

// LoadType is Load for a given aggregate type.
func (svc *CommandService) LoadType(ctx context.Context, aggregateType, tenantID, aggregateID string) (*Snapshot, error) {
	var snap Snapshot
	err := svc.pool.QueryRow(ctx, "SELECT state, version FROM "+svc.s+".ghk_aggregates WHERE tenant_id = $1 AND aggregate_type = $2 AND id = $3", tenantID, aggregateType, aggregateID).Scan(&snap.State, &snap.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &snap, err
}
