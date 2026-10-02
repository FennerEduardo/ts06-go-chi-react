package main

import (
	"log"
	"net/http"
	"os"

	"transactional-system-go-chi/internal/httpapi"
)

func main() {
	addr := ":" + envOr("PORT", "8080")
	log.Printf("listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, httpapi.NewRouter()))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
