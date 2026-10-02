package idempotency

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type IdempotencyStatus string

const (
	IdempotencyStatusProcessing IdempotencyStatus = "PROCESSING"
	IdempotencyStatusCompleted  IdempotencyStatus = "COMPLETED"
	IdempotencyStatusFailed     IdempotencyStatus = "FAILED"
)

type IdempotencyRecord struct {
	IdempotencyKey string
	RequestPath    string
	ResponseBody   *string
	StatusCode     *int
	Status         IdempotencyStatus
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type IdempotencyRepository struct {
	pool *pgxpool.Pool
}

func NewIdempotencyRepository(pool *pgxpool.Pool) *IdempotencyRepository {
	return &IdempotencyRepository{pool: pool}
}

// TryInsert Atomically tries to insert a new idempotency key
func (r *IdempotencyRepository) TryInsert(ctx context.Context, key, path string) (bool, error) {
	query := `
		INSERT INTO idempotency_keys (idempotency_key, request_path, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (idempotency_key) DO NOTHING
	`
	
	now := time.Now().UTC()
	tag, err := r.pool.Exec(ctx, query, key, path, IdempotencyStatusProcessing, now, now)
	if err != nil {
		return false, err
	}
	
	return tag.RowsAffected() > 0, nil
}

func (r *IdempotencyRepository) Get(ctx context.Context, key string) (*IdempotencyRecord, error) {
	query := `
		SELECT idempotency_key, request_path, response_body, status_code, status, created_at, updated_at
		FROM idempotency_keys
		WHERE idempotency_key = $1
	`
	
	var rec IdempotencyRecord
	err := r.pool.QueryRow(ctx, query, key).Scan(
		&rec.IdempotencyKey, &rec.RequestPath, &rec.ResponseBody, &rec.StatusCode, 
		&rec.Status, &rec.CreatedAt, &rec.UpdatedAt,
	)
	
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (r *IdempotencyRepository) Update(ctx context.Context, rec *IdempotencyRecord) error {
	query := `
		UPDATE idempotency_keys
		SET response_body = $1, status_code = $2, status = $3, updated_at = $4
		WHERE idempotency_key = $5
	`
	
	_, err := r.pool.Exec(ctx, query, rec.ResponseBody, rec.StatusCode, rec.Status, time.Now().UTC(), rec.IdempotencyKey)
	return err
}

func (r *IdempotencyRepository) DeleteExpired(ctx context.Context, cutoff time.Time) (int64, error) {
	query := "DELETE FROM idempotency_keys WHERE created_at < $1"
	tag, err := r.pool.Exec(ctx, query, cutoff)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
