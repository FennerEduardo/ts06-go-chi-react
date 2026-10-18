package runtime

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// OutboxRelay publishes pending outbox rows. Rows are claimed with FOR UPDATE SKIP LOCKED and a
// lease, so concurrent relays never publish the same row; the publisher returns only once the broker
// has accepted the message (RabbitMQ confirms, Pub/Sub / Service Bus send), before a row is marked
// published. Use one relay (and publisher) per goroutine.
type OutboxRelay struct {
	pool         *pgxpool.Pool
	publisher    EventPublisher
	s            string
	LeaseSeconds int
	MaxAttempts  int
}

func NewOutboxRelay(pool *pgxpool.Pool, publisher EventPublisher, schema string) (*OutboxRelay, error) {
	s, err := SchemaName(schema)
	if err != nil {
		return nil, err
	}
	return &OutboxRelay{pool: pool, publisher: publisher, s: s, LeaseSeconds: 30, MaxAttempts: 5}, nil
}

func (r *OutboxRelay) PublishBatch(ctx context.Context, workerID string, limit int) (int, error) {
	rows, err := r.pool.Query(ctx, "UPDATE "+r.s+".ghk_outbox SET claimed_by = $1, claimed_until = now() + make_interval(secs => $2), attempts = attempts + 1 "+
		"WHERE id IN (SELECT id FROM "+r.s+".ghk_outbox WHERE published_at IS NULL AND failed_at IS NULL AND (claimed_until IS NULL OR claimed_until < now()) "+
		"ORDER BY created_at LIMIT $3 FOR UPDATE SKIP LOCKED) RETURNING id, tenant_id, event_type, payload, traceparent", workerID, r.LeaseSeconds, limit)
	if err != nil {
		return 0, err
	}
	type claimed struct{ id, tenant, eventType, payload, traceparent string }
	var batch []claimed
	for rows.Next() {
		var c claimed
		var tp *string
		if err := rows.Scan(&c.id, &c.tenant, &c.eventType, &c.payload, &tp); err != nil {
			rows.Close()
			return 0, err
		}
		if tp != nil {
			c.traceparent = *tp
		}
		batch = append(batch, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	published := 0
	for _, c := range batch {
		destination := r.publisher.Destination()
		spanCtx, span := Tracer().Start(ContextFrom(ctx, c.traceparent), destination+" publish",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attribute.String("messaging.system", r.publisher.System()), attribute.String("messaging.destination.name", destination),
				attribute.String("messaging.message.id", c.id), attribute.String("tenant.id", c.tenant)))
		err := r.publisher.Publish(spanCtx, BrokerMessage{MessageID: c.id, EventType: c.eventType, TenantID: c.tenant, Traceparent: TraceparentOf(spanCtx), Body: []byte(c.payload)})
		if err == nil {
			_, err = r.pool.Exec(ctx, "UPDATE "+r.s+".ghk_outbox SET published_at = now(), claimed_until = NULL WHERE id = $1 AND claimed_by = $2", c.id, workerID)
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			_, _ = r.pool.Exec(ctx, "UPDATE "+r.s+".ghk_outbox SET claimed_until = NULL, last_error = $2, failed_at = CASE WHEN attempts >= $3 THEN now() ELSE NULL END WHERE id = $1", c.id, err.Error(), r.MaxAttempts)
		} else {
			published++
		}
		span.End()
	}
	return published, nil
}
