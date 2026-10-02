// Package httpapi exposes the domain kernel over HTTP (chi).
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"transactional-system-go-chi/internal/domain"
)

type commandFunc func(*domain.OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate, domain.Command) (domain.DomainEvent, error)

var commands = map[string]commandFunc{
	"process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi": (*domain.OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate).ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi,
}

// NewRouter wires the routes. The in-memory store is a placeholder for a repository.
func NewRouter() http.Handler {
	var mu sync.Mutex
	store := map[string]*domain.OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate{}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	r.Post("/api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/{id}/{command}", func(w http.ResponseWriter, req *http.Request) {
		handler, ok := commands[chi.URLParam(req, "command")]
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"detail": "unknown command"})
			return
		}
		id := chi.URLParam(req, "id")
		var payload map[string]any
		_ = json.NewDecoder(req.Body).Decode(&payload)

		mu.Lock()
		defer mu.Unlock()
		aggregate, exists := store[id]
		if !exists {
			created, err := domain.NewOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate(id)
			if err != nil {
				writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": err.Error()})
				return
			}
			aggregate = created
			store[id] = aggregate
		}
		event, err := handler(aggregate, domain.Command{ID: id, Payload: payload})
		if errors.Is(err, domain.ErrValidation) {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"type": event.Type, "aggregateId": event.AggregateID, "version": event.Version})
	})
	return r
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
