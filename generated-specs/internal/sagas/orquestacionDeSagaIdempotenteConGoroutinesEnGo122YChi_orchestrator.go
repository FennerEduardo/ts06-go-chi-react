package sagas

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
)

// Events
type OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiInitiatedEvent struct {
	CorrelationID uuid.UUID
	OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID     uuid.UUID
	Metadata      map[string]interface{}
}

type OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAuthorizedEvent struct {
	CorrelationID uuid.UUID
}

type OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompletedEvent struct {
	CorrelationID uuid.UUID
}

type OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiFailedEvent struct {
	CorrelationID uuid.UUID
	Reason        string
}

// Commands
type AuthorizeOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCommand struct {
	OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID uuid.UUID
	Metadata  map[string]interface{}
}

type CompleteOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCommand struct {
	OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID uuid.UUID
}

type CompensateOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCommand struct {
	OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID uuid.UUID
	Reason    string
}

// CommandPublisher dispatches saga commands (bridge it to your broker, ideally through the outbox).
type CommandPublisher interface {
	Publish(ctx context.Context, command string, payload interface{}) error
}

// Orchestrator
type OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaOrchestrator struct {
	repo      *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaRepository
	publisher CommandPublisher
}

func NewOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaOrchestrator(repo *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaRepository, publisher CommandPublisher) *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaOrchestrator {
	return &OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaOrchestrator{
		repo:      repo,
		publisher: publisher,
	}
}

func (o *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaOrchestrator) HandleInitiated(ctx context.Context, event OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiInitiatedEvent) error {
	log.Printf("Saga initiated: %s", event.CorrelationID)
	
	// Convert metadata map to JSON string
	metaJSON, _ := json.Marshal(event.Metadata)

	saga := &OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaInstance{
		CorrelationID: event.CorrelationID,
		OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID:     event.OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID,
		CurrentState:  SagaStateStarted,
		Metadata:      string(metaJSON),
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	if err := o.repo.Save(ctx, saga); err != nil {
		return err
	}

	return o.publisher.Publish(ctx, "AuthorizeOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCommand", AuthorizeOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCommand{OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID: event.OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID, Metadata: event.Metadata})
}

func (o *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaOrchestrator) HandleAuthorized(ctx context.Context, event OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAuthorizedEvent) error {
	log.Printf("Saga authorized: %s", event.CorrelationID)

	saga, err := o.repo.Get(ctx, event.CorrelationID)
	if err != nil {
		return err
	}

	saga.CurrentState = SagaStateCompleting
	saga.UpdatedAt = time.Now()

	if err := o.repo.Save(ctx, saga); err != nil {
		return err
	}

	return o.publisher.Publish(ctx, "CompleteOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCommand", CompleteOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCommand{OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID: saga.OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID})
}

func (o *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaOrchestrator) HandleCompleted(ctx context.Context, event OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompletedEvent) error {
	log.Printf("Saga completed: %s", event.CorrelationID)

	saga, err := o.repo.Get(ctx, event.CorrelationID)
	if err != nil {
		return err
	}

	saga.CurrentState = SagaStateCompleted
	saga.UpdatedAt = time.Now()

	return o.repo.Save(ctx, saga)
}

func (o *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaOrchestrator) HandleFailed(ctx context.Context, event OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiFailedEvent) error {
	log.Printf("Saga failed: %s, reason: %s", event.CorrelationID, event.Reason)

	saga, err := o.repo.Get(ctx, event.CorrelationID)
	if err != nil {
		return err
	}

	saga.CurrentState = SagaStateCompensating
	saga.ErrorReason = event.Reason
	saga.UpdatedAt = time.Now()

	if err := o.repo.Save(ctx, saga); err != nil {
		return err
	}

	if err := o.publisher.Publish(ctx, "CompensateOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCommand", CompensateOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCommand{OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID: saga.OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID, Reason: event.Reason}); err != nil {
		return err
	}

	saga.CurrentState = SagaStateFailed
	saga.UpdatedAt = time.Now()

	return o.repo.Save(ctx, saga)
}
