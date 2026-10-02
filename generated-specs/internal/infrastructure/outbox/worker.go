package outbox

import (
	"context"
	"log"
	"time"
)

// MessageBroker defines the interface for publishing events to external systems.
type MessageBroker interface {
	Publish(ctx context.Context, eventType string, payload string) error
}

type OutboxWorker struct {
	repo   *OutboxRepository
	broker MessageBroker
}

func NewOutboxWorker(repo *OutboxRepository, broker MessageBroker) *OutboxWorker {
	return &OutboxWorker{
		repo:   repo,
		broker: broker,
	}
}

// Start begins the background processing loop for outbox messages.
func (w *OutboxWorker) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.processBatch(ctx)
			}
		}
	}()
}

func (w *OutboxWorker) processBatch(ctx context.Context) {
	// Claim messages atomically (avoids race conditions across instances)
	messages, err := w.repo.ClaimPendingMessages(ctx, 50)
	if err != nil {
		log.Printf("Failed to claim outbox messages: %v", err)
		return
	}

	for _, msg := range messages {
		err := w.broker.Publish(ctx, msg.EventType, msg.Payload)
		if err != nil {
			log.Printf("Failed to publish message %s: %v", msg.ID, err)
			err = w.repo.MarkFailed(ctx, msg.ID, err.Error())
			if err != nil {
				log.Printf("Failed to update outbox message status to FAILED: %v", err)
			}
			continue
		}

		err = w.repo.MarkPublished(ctx, msg.ID)
		if err != nil {
			log.Printf("Failed to update outbox message status to PUBLISHED: %v", err)
		}
	}
}
