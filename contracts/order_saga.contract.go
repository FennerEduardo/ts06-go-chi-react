package contract

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type CreateOrderCommand struct {
	OrderID        uuid.UUID `json:"order_id"`
	CustomerID     uuid.UUID `json:"customer_id"`
	TotalAmount    float64   `json:"total_amount"`
	Currency       string    `json:"currency"`
	IdempotencyKey string    `json:"idempotency_key"`
}

type OrderProcessedEvent struct {
	EventID       uuid.UUID `json:"event_id"`
	OrderID       uuid.UUID `json:"order_id"`
	Status        string    `json:"status"`
	OccurredAt    time.Time `json:"occurred_at"`
	CorrelationID string    `json:"correlation_id"`
}

type OrderSagaContract interface {
	ExecuteSaga(ctx context.Context, cmd CreateOrderCommand) (*OrderProcessedEvent, error)
	ValidateIdempotency(ctx context.Context, key string) (bool, error)
	CompensateOrder(ctx context.Context, orderID uuid.UUID, reason string) error
}
