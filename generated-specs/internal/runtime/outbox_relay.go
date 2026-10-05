package runtime

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Topology struct{ Exchange, Queue, DLX, DLQ string }

// TopologyFor returns the topology for a prefix (use one per environment or test).
func TopologyFor(prefix string) Topology {
	return Topology{prefix + ".events", prefix + ".consumer", prefix + ".dlx", prefix + ".dlq"}
}

var DefaultTopology = Topology{"orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi.events", "orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi.consumer", "orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi.dlx", "orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi.dlq"}

// DeclareTopology declares a topic exchange -> consumer queue that dead-letters to a fanout DLX -> DLQ.
func DeclareTopology(ch *amqp.Channel, t Topology) error {
	if err := ch.ExchangeDeclare(t.Exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(t.DLX, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(t.DLQ, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(t.DLQ, "", t.DLX, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(t.Queue, true, false, false, false, amqp.Table{"x-dead-letter-exchange": t.DLX}); err != nil {
		return err
	}
	return ch.QueueBind(t.Queue, "#", t.Exchange, false, nil)
}

// OutboxRelay publishes pending outbox rows. Rows are claimed with FOR UPDATE SKIP LOCKED and a
// lease, so concurrent relays never publish the same row; publisher confirms guarantee delivery to
// the broker before a row is marked published. Use one relay (and channel) per goroutine.
type OutboxRelay struct {
	pool         *pgxpool.Pool
	ch           *amqp.Channel
	exchange     string
	s            string
	LeaseSeconds int
	MaxAttempts  int
}

func NewOutboxRelay(pool *pgxpool.Pool, ch *amqp.Channel, exchange, schema string) (*OutboxRelay, error) {
	s, err := SchemaName(schema)
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		return nil, err
	}
	return &OutboxRelay{pool: pool, ch: ch, exchange: exchange, s: s, LeaseSeconds: 30, MaxAttempts: 5}, nil
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
		spanCtx, span := Tracer().Start(ContextFrom(ctx, c.traceparent), r.exchange+" publish",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attribute.String("messaging.system", "rabbitmq"), attribute.String("messaging.destination.name", r.exchange),
				attribute.String("messaging.message.id", c.id), attribute.String("tenant.id", c.tenant)))
		headers := amqp.Table{"tenant_id": c.tenant}
		if tp := TraceparentOf(spanCtx); tp != "" {
			headers["traceparent"] = tp
		}
		err := r.publish(spanCtx, c.eventType, c.id, c.payload, headers)
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

func (r *OutboxRelay) publish(ctx context.Context, key, id, body string, headers amqp.Table) error {
	confirm, err := r.ch.PublishWithDeferredConfirmWithContext(ctx, r.exchange, key, false, false, amqp.Publishing{
		MessageId: id, DeliveryMode: amqp.Persistent, ContentType: "application/json", Type: key, Headers: headers, Body: []byte(body),
	})
	if err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	acked, err := confirm.WaitContext(waitCtx)
	if err != nil {
		return err
	}
	if !acked {
		return amqp.ErrClosed
	}
	return nil
}
