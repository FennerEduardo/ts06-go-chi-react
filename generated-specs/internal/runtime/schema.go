// Package runtime is the verified persistence, messaging and tracing path (docs/RUNTIME-KERNEL.md).
package runtime

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

var statements = []string{
	"CREATE SCHEMA IF NOT EXISTS __SCHEMA__",
	"CREATE TABLE IF NOT EXISTS __SCHEMA__.ghk_aggregates (tenant_id TEXT NOT NULL, aggregate_type TEXT NOT NULL, id TEXT NOT NULL, state TEXT NOT NULL, version INT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (tenant_id, aggregate_type, id))",
	"CREATE TABLE IF NOT EXISTS __SCHEMA__.ghk_outbox (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, aggregate_id TEXT NOT NULL, event_type TEXT NOT NULL, payload TEXT NOT NULL, traceparent TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), claimed_by TEXT, claimed_until TIMESTAMPTZ, published_at TIMESTAMPTZ, attempts INT NOT NULL DEFAULT 0, last_error TEXT, failed_at TIMESTAMPTZ)",
	"CREATE INDEX IF NOT EXISTS ghk_outbox_pending ON __SCHEMA__.ghk_outbox (created_at) WHERE published_at IS NULL AND failed_at IS NULL",
	"CREATE TABLE IF NOT EXISTS __SCHEMA__.ghk_idempotency (tenant_id TEXT NOT NULL, key TEXT NOT NULL, status TEXT NOT NULL, response TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (tenant_id, key))",
	"CREATE TABLE IF NOT EXISTS __SCHEMA__.ghk_inbox (consumer TEXT NOT NULL, message_id TEXT NOT NULL, processed_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (consumer, message_id))",
	"CREATE TABLE IF NOT EXISTS __SCHEMA__.ghk_sagas (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT NOT NULL, completed_steps TEXT NOT NULL DEFAULT '', updated_at TIMESTAMPTZ NOT NULL DEFAULT now())",
}

// Defense in depth (optional): enforce tenant isolation in PostgreSQL as well.
// ALTER TABLE ghk_aggregates ENABLE ROW LEVEL SECURITY;
// CREATE POLICY tenant_isolation ON ghk_aggregates USING (tenant_id = current_setting('app.tenant_id'));
// and run SET LOCAL app.tenant_id = '<tenant>' at the start of each transaction.

var schemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// SchemaName validates a PostgreSQL schema identifier.
func SchemaName(schema string) (string, error) {
	if schema == "" {
		schema = "public"
	}
	if !schemaPattern.MatchString(schema) {
		return "", fmt.Errorf("invalid schema name: %s", schema)
	}
	return schema, nil
}

// Migrate creates the runtime tables (idempotent).
func Migrate(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	s, err := SchemaName(schema)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, strings.ReplaceAll(statement, "__SCHEMA__", s)); err != nil {
			return err
		}
	}
	return nil
}
