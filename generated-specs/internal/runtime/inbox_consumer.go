package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
// redeliveries are acknowledged without running the handler twice. A handler error dead-letters the
// message (RabbitMQ DLX, Pub/Sub / Service Bus dead-letter topic): it is not redelivered.
type InboxConsumer struct {
	pool         *pgxpool.Pool
	subscription EventSubscription
	name         string
	handler      Handler
	s            string
}

func NewInboxConsumer(pool *pgxpool.Pool, subscription EventSubscription, name string, handler Handler, schema string) (*InboxConsumer, error) {
	s, err := SchemaName(schema)
	if err != nil {
		return nil, err
	}
	return &InboxConsumer{pool: pool, subscription: subscription, name: name, handler: handler, s: s}, nil
}

// Drain processes messages until the source stays idle for idle. It returns how many were processed.
func (c *InboxConsumer) Drain(ctx context.Context, idle time.Duration) (int, error) {
	processed := 0
	idleSince := time.Now()
	for time.Since(idleSince) < idle {
		d, err := c.subscription.Poll(ctx)
		if err != nil {
			return processed, err
		}
		if d == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		c.process(ctx, d)
		processed++
		idleSince = time.Now()
	}
	return processed, nil
}

func (c *InboxConsumer) process(ctx context.Context, d *Delivery) {
	m := d.Message
	source := c.subscription.Source()
	ctx, span := Tracer().Start(ContextFrom(ctx, m.Traceparent), source+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("messaging.system", c.subscription.System()), attribute.String("messaging.destination.name", source), attribute.String("messaging.message.id", m.MessageID)))
	defer span.End()
	if err := c.handle(ctx, m); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		_ = d.DeadLetter(ctx)
		return
	}
	_ = d.Ack(ctx)
}

func (c *InboxConsumer) handle(ctx context.Context, m BrokerMessage) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, "INSERT INTO "+c.s+".ghk_inbox (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", c.name, m.MessageID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		var event map[string]any
		if err := json.Unmarshal(m.Body, &event); err != nil {
			return fmt.Errorf("invalid message body: %w", err)
		}
		if err := c.handler(ctx, event, Meta{MessageID: m.MessageID, TenantID: m.TenantID, EventType: m.EventType, Tx: tx}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
