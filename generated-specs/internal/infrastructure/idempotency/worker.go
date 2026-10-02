package idempotency

import (
	"context"
	"log"
	"time"
)

type IdempotencyCleanupWorker struct {
	repo *IdempotencyRepository
}

func NewIdempotencyCleanupWorker(repo *IdempotencyRepository) *IdempotencyCleanupWorker {
	return &IdempotencyCleanupWorker{repo: repo}
}

// Start runs a background loop to clean up expired idempotency keys (older than 24h).
func (w *IdempotencyCleanupWorker) Start(ctx context.Context) {
	go func() {
		// Run every hour
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cutoff := time.Now().UTC().Add(-24 * time.Hour)
				deleted, err := w.repo.DeleteExpired(ctx, cutoff)
				if err != nil {
					log.Printf("Error cleaning up idempotency keys: %v", err)
				} else if deleted > 0 {
					log.Printf("Cleaned up %d expired idempotency records.", deleted)
				}
			}
		}
	}()
}
