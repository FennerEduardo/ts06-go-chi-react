package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealth(t *testing.T) {
	rec := httptest.NewRecorder()
	NewRouter().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestExecutesADomainCommand(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/agg-api/process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi", strings.NewReader(`{"source":"api-test"}`))
	NewRouter().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["type"] != "ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompleted" || body["version"] != float64(1) {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestUnknownCommandReturns404(t *testing.T) {
	rec := httptest.NewRecorder()
	NewRouter().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/agg-api/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d", rec.Code)
	}
}
