package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"transactional-system-go-chi/internal/domain"
	"transactional-system-go-chi/internal/runtime"
)

const secret = "unit-test-secret-with-at-least-32-chars"

type fakeCommands struct{ last runtime.CommandRequest }

func (f *fakeCommands) Handle(_ context.Context, req runtime.CommandRequest) (runtime.CommandResult, error) {
	f.last = req
	if req.Command == "does_not_exist" {
		return runtime.CommandResult{}, fmt.Errorf("%w: unknown command", domain.ErrValidation)
	}
	status := "created"
	if req.IdempotencyKey == "seen" {
		status = "replayed"
	}
	return runtime.CommandResult{Status: status, AggregateID: req.AggregateID, EventType: "ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompleted", Version: 1}, nil
}

func post(h http.Handler, path string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"source":"api-test"}`))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func bearer(claims map[string]any, key string) string { return "Bearer " + SignJWT(claims, key) }

func TestHealth(t *testing.T) {
	rec := httptest.NewRecorder()
	NewRouter(&fakeCommands{}, secret).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestRunsACommandForTheTokenTenant(t *testing.T) {
	fake := &fakeCommands{}
	rec := post(NewRouter(fake, secret), "/api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/agg-api/process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi", map[string]string{
		"Authorization": bearer(map[string]any{TenantClaim: "acme"}, secret), "X-Tenant-Id": "someone-else"})
	if rec.Code != http.StatusAccepted || fake.last.TenantID != "acme" || fake.last.Payload["source"] != "api-test" {
		t.Fatalf("status %d, request %+v", rec.Code, fake.last)
	}
}

func TestIdempotentReplayReturns200(t *testing.T) {
	rec := post(NewRouter(&fakeCommands{}, secret), "/api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/agg-api/process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi", map[string]string{
		"Authorization": bearer(map[string]any{TenantClaim: "acme"}, secret), "Idempotency-Key": "seen"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestRejectsMissingOrInvalidTokensAndXTenantID(t *testing.T) {
	h := NewRouter(&fakeCommands{}, secret)
	path := "/api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/agg-api/process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi"
	for _, headers := range []map[string]string{
		{},
		{"X-Tenant-Id": "acme"},
		{"Authorization": bearer(map[string]any{"sub": "u1"}, secret)},
		{"Authorization": bearer(map[string]any{TenantClaim: "acme"}, "wrong")},
	} {
		if rec := post(h, path, headers); rec.Code != http.StatusUnauthorized {
			t.Fatalf("headers %v: status %d", headers, rec.Code)
		}
	}
}

func TestDomainValidationErrorIs422(t *testing.T) {
	rec := post(NewRouter(&fakeCommands{}, secret), "/api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/agg-api/does_not_exist", map[string]string{
		"Authorization": bearer(map[string]any{TenantClaim: "acme"}, secret)})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status %d", rec.Code)
	}
}
