package runtime

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SagaStep struct {
	Name       string
	Action     func(ctx context.Context) error
	Compensate func(ctx context.Context) error
}

// SagaOrchestrator persists progress after every step; when a step fails, the completed steps are
// compensated in reverse order. Re-running a saga id resumes after its completed steps.
type SagaOrchestrator struct {
	pool *pgxpool.Pool
	s    string
}

func NewSagaOrchestrator(pool *pgxpool.Pool, schema string) (*SagaOrchestrator, error) {
	s, err := SchemaName(schema)
	if err != nil {
		return nil, err
	}
	return &SagaOrchestrator{pool: pool, s: s}, nil
}

func (o *SagaOrchestrator) Run(ctx context.Context, sagaID, tenantID string, steps []SagaStep) (string, error) {
	if _, err := o.pool.Exec(ctx, "INSERT INTO "+o.s+".ghk_sagas (id, tenant_id, status) VALUES ($1, $2, 'RUNNING') ON CONFLICT (id) DO NOTHING", sagaID, tenantID); err != nil {
		return "", err
	}
	var done string
	if err := o.pool.QueryRow(ctx, "SELECT completed_steps FROM "+o.s+".ghk_sagas WHERE id = $1 AND tenant_id = $2", sagaID, tenantID).Scan(&done); err != nil {
		return "", err
	}
	completed := []string{}
	if done != "" {
		completed = strings.Split(done, ",")
	}
	byName := map[string]SagaStep{}
	for _, step := range steps {
		byName[step.Name] = step
	}
	contains := func(name string) bool {
		for _, c := range completed {
			if c == name {
				return true
			}
		}
		return false
	}
	for _, step := range steps {
		if contains(step.Name) {
			continue
		}
		if err := step.Action(ctx); err != nil {
			if err := o.save(ctx, sagaID, "COMPENSATING", completed); err != nil {
				return "", err
			}
			for i := len(completed) - 1; i >= 0; i-- {
				if err := byName[completed[i]].Compensate(ctx); err != nil {
					return "", err
				}
				completed = completed[:i]
				if err := o.save(ctx, sagaID, "COMPENSATING", completed); err != nil {
					return "", err
				}
			}
			return "COMPENSATED", o.save(ctx, sagaID, "COMPENSATED", completed)
		}
		completed = append(completed, step.Name)
		if err := o.save(ctx, sagaID, "RUNNING", completed); err != nil {
			return "", err
		}
	}
	return "COMPLETED", o.save(ctx, sagaID, "COMPLETED", completed)
}

// Status returns the persisted status and completed steps of a saga.
func (o *SagaOrchestrator) Status(ctx context.Context, sagaID string) (string, []string, error) {
	var status, done string
	if err := o.pool.QueryRow(ctx, "SELECT status, completed_steps FROM "+o.s+".ghk_sagas WHERE id = $1", sagaID).Scan(&status, &done); err != nil {
		return "", nil, err
	}
	if done == "" {
		return status, []string{}, nil
	}
	return status, strings.Split(done, ","), nil
}

func (o *SagaOrchestrator) save(ctx context.Context, sagaID, status string, completed []string) error {
	_, err := o.pool.Exec(ctx, "UPDATE "+o.s+".ghk_sagas SET status = $2, completed_steps = $3, updated_at = now() WHERE id = $1", sagaID, status, strings.Join(completed, ","))
	return err
}
