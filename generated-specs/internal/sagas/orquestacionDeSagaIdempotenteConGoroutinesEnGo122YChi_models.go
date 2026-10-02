package sagas

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SagaState string

const (
	SagaStateStarted      SagaState = "STARTED"
	SagaStateAuthorized   SagaState = "AUTHORIZED"
	SagaStateCompleting   SagaState = "COMPLETING"
	SagaStateCompleted    SagaState = "COMPLETED"
	SagaStateCompensating SagaState = "COMPENSATING"
	SagaStateFailed       SagaState = "FAILED"
)

type OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaInstance struct {
	CorrelationID uuid.UUID `json:"correlation_id"`
	OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID     uuid.UUID `json:"orquestaciondesagaidempotentecongoroutinesengo122ychiid"`
	CurrentState  SagaState `json:"current_state"`
	Metadata      string    `json:"metadata"`
	ErrorReason   string    `json:"error_reason"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaRepository struct {
	pool *pgxpool.Pool
}

func NewOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaRepository(pool *pgxpool.Pool) *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaRepository {
	return &OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaRepository{pool: pool}
}

func (r *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaRepository) Save(ctx context.Context, saga *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaInstance) error {
	query := `
		INSERT INTO orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi_sagas (correlation_id, orquestaciondesagaidempotentecongoroutinesengo122ychiid, current_state, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (correlation_id) DO UPDATE SET
		current_state = EXCLUDED.current_state,
		error_reason = EXCLUDED.error_reason,
		updated_at = EXCLUDED.updated_at
	`
	
	_, err := r.pool.Exec(ctx, query, saga.CorrelationID, saga.OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID, saga.CurrentState, saga.Metadata, saga.CreatedAt, saga.UpdatedAt)
	return err
}

func (r *OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaRepository) Get(ctx context.Context, correlationID uuid.UUID) (*OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaInstance, error) {
	query := `
		SELECT correlation_id, orquestaciondesagaidempotentecongoroutinesengo122ychiid, current_state, metadata, error_reason, created_at, updated_at
		FROM orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi_sagas
		WHERE correlation_id = $1
	`
	
	var saga OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSagaInstance
	err := r.pool.QueryRow(ctx, query, correlationID).Scan(
		&saga.CorrelationID, &saga.OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiID, &saga.CurrentState, &saga.Metadata, &saga.ErrorReason, &saga.CreatedAt, &saga.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &saga, nil
}
