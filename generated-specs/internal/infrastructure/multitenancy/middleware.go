package multitenancy

import (
	"context"
	"net/http"
)

type contextKey string

const TenantIDKey contextKey = "tenant_id"

// TenantMiddleware extracts the X-Tenant-Id header and injects it into the request context.
func TenantMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.Header.Get("X-Tenant-Id")
		
		if tenantID == "" {
			tenantID = "default"
		}

		ctx := context.WithValue(r.Context(), TenantIDKey, tenantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetTenantID retrieves the current tenant ID from the context.
func GetTenantID(ctx context.Context) string {
	if val, ok := ctx.Value(TenantIDKey).(string); ok {
		return val
	}
	return "default"
}
