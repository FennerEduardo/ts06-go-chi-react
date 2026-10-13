// Package httpapi exposes the runtime kernel's commands over HTTP (chi).
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"transactional-system-go-chi/internal/domain"
	"transactional-system-go-chi/internal/runtime"
)

// CommandPort is what the HTTP layer needs from the runtime kernel (*runtime.CommandService implements it).
type CommandPort interface {
	Handle(ctx context.Context, req runtime.CommandRequest) (runtime.CommandResult, error)
}

var statusCodes = map[string]int{"created": http.StatusAccepted, "replayed": http.StatusOK, "in-progress": http.StatusConflict}

// NewRouter wires POST /api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/{id}/{command} (process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi).
// ghk:implements OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi
func NewRouter(commands CommandPort, jwtSecret string) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	for path, aggregateType := range map[string]string{"/api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/{id}/{command}": "OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi"} {
		r.Post(path, commandHandler(commands, jwtSecret, aggregateType))
	}
	return r
}

func commandHandler(commands CommandPort, jwtSecret, aggregateType string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		tenant := TenantFrom(req.Header.Get("Authorization"), jwtSecret)
		if tenant == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"detail": "A valid bearer token with a tenant is required"})
			return
		}
		var payload map[string]any
		_ = json.NewDecoder(req.Body).Decode(&payload)
		result, err := commands.Handle(req.Context(), runtime.CommandRequest{
			TenantID:       tenant,
			AggregateType:  aggregateType,
			AggregateID:    chi.URLParam(req, "id"),
			Command:        chi.URLParam(req, "command"),
			Payload:        payload,
			IdempotencyKey: req.Header.Get("Idempotency-Key"),
			Traceparent:    req.Header.Get("traceparent"),
		})
		if errors.Is(err, domain.ErrValidation) {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": err.Error()})
			return
		}
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "internal error"})
			return
		}
		status, ok := statusCodes[result.Status]
		if !ok {
			status = http.StatusOK
		}
		writeJSON(w, status, result)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
