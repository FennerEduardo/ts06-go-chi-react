package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"transactional-system-go-chi/internal/httpapi"
	"transactional-system-go-chi/internal/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("DATABASE_URL: %v", err)
	}
	defer pool.Close()
	commands, err := runtime.NewCommandService(pool, "public")
	if err != nil {
		log.Fatal(err)
	}
	if runtime.BrokerConfigured() {
		go relayOutbox(ctx, pool)
	} else {
		log.Print("no event broker is configured (AMQP_URL, PUBSUB_PROJECT_ID or SERVICEBUS_*): outbox events are stored but not published")
	}

	server := &http.Server{Addr: ":" + envOr("PORT", "8080"), Handler: httpapi.NewRouter(commands, os.Getenv("JWT_SECRET"))}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// relayOutbox publishes the outbox every 1000 ms through the configured broker (internal/runtime/broker.go),
// reconnecting after broker errors.
func relayOutbox(ctx context.Context, pool *pgxpool.Pool) {
	workerID := "orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi-" + uuid.NewString()
	for ctx.Err() == nil {
		if err := relayUntilError(ctx, pool, workerID); err != nil {
			log.Printf("outbox relay: %v (retrying)", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
}

func relayUntilError(ctx context.Context, pool *pgxpool.Pool, workerID string) error {
	broker, err := runtime.ConnectBroker(ctx)
	if err != nil {
		return err
	}
	defer broker.Close()
	if err := broker.Declare(ctx, runtime.DefaultTopology); err != nil {
		return err
	}
	publisher, err := broker.Publisher(ctx, runtime.DefaultTopology.Exchange)
	if err != nil {
		return err
	}
	defer publisher.Close()
	relay, err := runtime.NewOutboxRelay(pool, publisher, "public")
	if err != nil {
		return err
	}
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := relay.PublishBatch(ctx, workerID, 50); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
