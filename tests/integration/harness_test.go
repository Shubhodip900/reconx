package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

// testConfig holds configuration derived from environment variables.
type testConfig struct {
	GatewayURL   string
	APIKey       string
	PollTimeout  time.Duration
	PollInterval time.Duration
}

// cfg is the package-level config, set in TestMain.
var cfg testConfig

// TestMain is the integration test entry point.
//
// It reads configuration from environment variables, waits up to 30 s for the
// gateway to become reachable, and exits 0 (skip) rather than 1 (fail) when
// the gateway is unreachable — so CI pipelines that don't start the stack are
// unaffected.
//
// Environment variables:
//
//	INTEGRATION_GATEWAY_URL     gateway base URL (default: http://localhost:8090)
//	RECONX_GATEWAY_API_KEY      X-API-Key header value (default: empty → no auth)
//	INTEGRATION_POLL_TIMEOUT    max wait for status changes (default: 60s)
//	INTEGRATION_POLL_INTERVAL   sleep between status polls   (default: 2s)
func TestMain(m *testing.M) {
	cfg = testConfig{
		GatewayURL:   envOrDefault("INTEGRATION_GATEWAY_URL", "http://localhost:8090"),
		APIKey:       os.Getenv("RECONX_GATEWAY_API_KEY"),
		PollTimeout:  durationEnv("INTEGRATION_POLL_TIMEOUT", 60*time.Second),
		PollInterval: durationEnv("INTEGRATION_POLL_INTERVAL", 2*time.Second),
	}

	if !waitForGateway(cfg.GatewayURL) {
		fmt.Fprintf(os.Stderr,
			"integration: gateway not reachable at %s — skipping\n", cfg.GatewayURL)
		os.Exit(0)
	}

	os.Exit(m.Run())
}

// waitForGateway polls GET /health until it returns 200 or 30 s elapses.
func waitForGateway(baseURL string) bool {
	deadline := time.Now().Add(30 * time.Second)
	client := &http.Client{Timeout: 3 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(2 * time.Second)
	}
	return false
}

// envOrDefault returns the value of the environment variable key, or def when
// the variable is unset or empty.
func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// durationEnv parses a time.Duration from the named environment variable.
// Returns def when the variable is unset, empty, or unparseable.
func durationEnv(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
