package multitenancy

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ExampleTenantAwareRepository shows how to use the context tenant ID
// in your data access layer to isolate data globally.
type ExampleTenantAwareRepository struct {
	pool *pgxpool.Pool
}

func NewExampleTenantAwareRepository(pool *pgxpool.Pool) *ExampleTenantAwareRepository {
	return &ExampleTenantAwareRepository{pool: pool}
}

func (r *ExampleTenantAwareRepository) FindAll(ctx context.Context) ([]string, error) {
	tenantID := GetTenantID(ctx)

	// Inject tenantID into every query to enforce isolation
	query := `
		SELECT data_column
		FROM some_tenant_aware_table
		WHERE tenant_id = $1
	`

	rows, err := r.pool.Query(ctx, query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		results = append(results, data)
	}

	return results, nil
}
