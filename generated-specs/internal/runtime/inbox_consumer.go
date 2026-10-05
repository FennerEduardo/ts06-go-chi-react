package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Meta describes a delivered message; Tx is the transaction the inbox record is written in.
type Meta struct {
	MessageID string
	TenantID  string
	EventType string
	Tx        pgx.Tx
}

type Handler func(ctx context.Context, event map[string]any, meta Meta) error

// InboxConsumer records the message id in ghk_inbox in the same transaction as the handler, so
// redeliveries are acknowledged without running the handler twice. A handler error rejects the
// message without requeue, so RabbitMQ dead-letters it to the DLQ.
type InboxConsumer struct {
	pool    *pgxpool.Pool
	ch      *amqp.Channel
	queue   string
	name    string
	handler Handler
	s       string
}

func NewInboxConsumer(pool *pgxpool.Pool, ch *amqp.Channel, queue, name string, handler Handler, schema string) (*InboxConsumer, error) {
	s, err := SchemaName(schema)
	if err != nil {
		return nil, err
	}
	return &InboxConsumer{pool: pool, ch: ch, queue: queue, name: name, handler: handler, s: s}, nil
}

// Drain processes messages until the queue stays idle for `idle`. It returns how many were processed.
func (c *InboxConsumer) Drain(ctx context.Context, idle time.Duration) (int, error) {
	deliveries, err := c.ch.Consume(c.queue, c.name, false, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	defer c.ch.Cancel(c.name, false) //nolint:errcheck
	processed := 0
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return processed, nil
			}
			c.process(ctx, d)
			processed++
		case <-time.After(idle):
			return processed, nil
		}
	}
}

func (c *InboxConsumer) process(ctx context.Context, d amqp.Delivery) {
	traceparent, _ := d.Headers["traceparent"].(string)
	tenant, _ := d.Headers["tenant_id"].(string)
	ctx, span := Tracer().Start(ContextFrom(ctx, traceparent), c.queue+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("messaging.system", "rabbitmq"), attribute.String("messaging.destination.name", c.queue), attribute.String("messaging.message.id", d.MessageId)))
	defer span.End()
	if err := c.handle(ctx, d, tenant); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		_ = d.Nack(false, false)
		return
	}
	_ = d.Ack(false)
}

func (c *InboxConsumer) handle(ctx context.Context, d amqp.Delivery, tenant string) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, "INSERT INTO "+c.s+".ghk_inbox (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", c.name, d.MessageId)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		var event map[string]any
		if err := json.Unmarshal(d.Body, &event); err != nil {
			return fmt.Errorf("invalid message body: %w", err)
		}
		if err := c.handler(ctx, event, Meta{MessageID: d.MessageId, TenantID: tenant, EventType: d.Type, Tx: tx}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
