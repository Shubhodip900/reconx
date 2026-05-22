// Package middleware provides HTTP middleware for the ReconX API Gateway.
package middleware

import (
	"net/http"
	"strings"
)

// APIKeyAuth returns an HTTP middleware that enforces X-API-Key authentication
// for all /v1/* routes. The /health route is always exempt because it is used
// as a liveness probe by load balancers and container orchestrators.
//
// The key is loaded from config / env (RECONX_GATEWAY_API_KEY). If the key is
// empty, authentication is disabled — useful for local development. In
// production always set the env variable.
//
// Unauthenticated requests receive 401 Unauthorized:
//
//	{"error": "unauthorized"}
func APIKeyAuth(key string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// /health is always reachable without a key (liveness / readiness probes).
			if r.URL.Path == "/health" {
				next.ServeHTTP(w, r)
				return
			}

			// Only enforce auth on the versioned API surface.
			if strings.HasPrefix(r.URL.Path, "/v1/") {
				if key == "" {
					// Auth disabled — allow the request through (dev / test mode).
					next.ServeHTTP(w, r)
					return
				}
				if r.Header.Get("X-API-Key") != key {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
					return
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}
