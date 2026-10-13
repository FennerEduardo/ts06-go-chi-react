//go:build integration

// HTTP contract of the application layer against PostgreSQL (needs DATABASE_URL).
package runtime_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"transactional-system-go-chi/internal/httpapi"
	"transactional-system-go-chi/internal/runtime"
)

const httpSecret = "integration-test-secret-with-32-chars!"

func TestHTTPCommandReachesTheOutboxForTheTokenTenant(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := runtime.Migrate(ctx, pool, "public"); err != nil {
		t.Fatal(err)
	}
	commands, err := runtime.NewCommandService(pool, "public")
	if err != nil {
		t.Fatal(err)
	}
	router := httpapi.NewRouter(commands, httpSecret)
	tenant := "tenant-" + uuid.NewString()
	id := uuid.NewString()
	key := uuid.NewString()
	send := func(headers map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/"+id+"/process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi", strings.NewReader("{}"))
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	if rec := send(map[string]string{"X-Tenant-Id": tenant}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("X-Tenant-Id alone: status %d", rec.Code)
	}
	headers := map[string]string{"Authorization": "Bearer " + httpapi.SignJWT(map[string]any{httpapi.TenantClaim: tenant}, httpSecret), "Idempotency-Key": key, "X-Tenant-Id": "someone-else"}
	if rec := send(headers); rec.Code != http.StatusAccepted {
		t.Fatalf("first: status %d: %s", rec.Code, rec.Body.String())
	}
	replay := send(headers)
	var body map[string]any
	_ = json.Unmarshal(replay.Body.Bytes(), &body)
	if replay.Code != http.StatusOK || body["status"] != "replayed" {
		t.Fatalf("replay: status %d body %v", replay.Code, body)
	}
	var stored string
	if err := pool.QueryRow(ctx, "SELECT tenant_id FROM ghk_outbox WHERE aggregate_id = $1", id).Scan(&stored); err != nil || stored != tenant {
		t.Fatalf("outbox tenant %q, err %v", stored, err)
	}
}
