package idempotency

import (
	"bytes"
	"log"
	"net/http"
	"time"
)

const ProcessingTTL = 2 * time.Minute

// responseRecorder is a custom http.ResponseWriter to capture the body and status code
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	body       *bytes.Buffer
}

func (rec *responseRecorder) WriteHeader(statusCode int) {
	rec.statusCode = statusCode
	rec.ResponseWriter.WriteHeader(statusCode)
}

func (rec *responseRecorder) Write(b []byte) (int, error) {
	rec.body.Write(b)
	return rec.ResponseWriter.Write(b)
}

func IdempotencyMiddleware(repo *IdempotencyRepository) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-Idempotency-Key")

			if key == "" || (r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodPatch) {
				next.ServeHTTP(w, r)
				return
			}

			ctx := r.Context()
			inserted, err := repo.TryInsert(ctx, key, r.URL.Path)
			if err != nil {
				http.Error(w, `{"error": "Internal server error"}`, http.StatusInternalServerError)
				return
			}

			if !inserted {
				// Key already exists, check status
				record, err := repo.Get(ctx, key)
				if err != nil || record == nil {
					http.Error(w, `{"error": "Internal server error"}`, http.StatusInternalServerError)
					return
				}

				if record.Status == IdempotencyStatusCompleted {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(*record.StatusCode)
					if record.ResponseBody != nil {
						w.Write([]byte(*record.ResponseBody))
					}
					return
				}

				if record.Status == IdempotencyStatusProcessing {
					if time.Since(record.UpdatedAt) < ProcessingTTL {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusConflict)
						w.Write([]byte(`{"error": "Request already in progress. Retry after a moment."}`))
						return
					}
					log.Printf("Idempotency key %s stuck in PROCESSING. Allowing retry.", key)
					record.Status = IdempotencyStatusProcessing
					repo.Update(ctx, record)
					// Proceed to process the retry
				}
			}

			// Capture response
			recorder := &responseRecorder{
				ResponseWriter: w,
				statusCode:     http.StatusOK, // Default if WriteHeader is not called
				body:           new(bytes.Buffer),
			}

			// Process Request
			defer func() {
				if r := recover(); r != nil {
					// Handle Panic
					log.Printf("Panic during request processing: %v", r)
					errResp := `{"error": "Internal server error"}`
					statusCode := http.StatusInternalServerError
					repo.Update(ctx, &IdempotencyRecord{
						IdempotencyKey: key,
						ResponseBody:   &errResp,
						StatusCode:     &statusCode,
						Status:         IdempotencyStatusFailed,
					})
					panic(r)
				}
			}()

			next.ServeHTTP(recorder, r)

			// Save Result
			bodyStr := recorder.body.String()
			repo.Update(ctx, &IdempotencyRecord{
				IdempotencyKey: key,
				ResponseBody:   &bodyStr,
				StatusCode:     &recorder.statusCode,
				Status:         IdempotencyStatusCompleted,
			})
		})
	}
}
