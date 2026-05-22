// Package config provides configuration for the ReconX API Gateway.
package config

import "github.com/spf13/viper"

// Config holds all runtime configuration for the gateway.
type Config struct {
	HTTP    HTTPConfig
	Metrics MetricsConfig
	// Upstream service addresses.
	Ingestion  UpstreamConfig
	Engine     UpstreamConfig
	Resolution ResolutionConfig
	Log        LogConfig
	// APIKey is the shared secret for X-API-Key authentication.
	// Loaded from RECONX_GATEWAY_API_KEY.
	// If empty, authentication is disabled (local dev mode).
	APIKey string `mapstructure:"api_key"`
}

// HTTPConfig holds the public-facing HTTP server settings.
type HTTPConfig struct {
	Port int `mapstructure:"port"`
}

// MetricsConfig holds Prometheus metrics export settings.
type MetricsConfig struct {
	Port int    `mapstructure:"port"`
	Path string `mapstructure:"path"`
}

// UpstreamConfig is the address of an upstream gRPC service plus its HTTP
// health-check URL (used by the aggregate /health handler).
type UpstreamConfig struct {
	Address   string `mapstructure:"address"`    // gRPC host:port
	HealthURL string `mapstructure:"health_url"` // HTTP liveness URL
}

// ResolutionConfig extends UpstreamConfig with the HTTP REST address needed
// to proxy auto-resolve, retry, and audit routes.
type ResolutionConfig struct {
	Address     string `mapstructure:"address"`      // gRPC host:port
	HealthURL   string `mapstructure:"health_url"`   // HTTP liveness URL
	HTTPAddress string `mapstructure:"http_address"` // HTTP REST base URL (for proxying)
}

// LogConfig controls structured logging behaviour.
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// Load reads configuration from environment variables and optional config file.
func Load() (*Config, error) {
	v := viper.New()

	// ── Defaults ─────────────────────────────────────────────────────────────
	v.SetDefault("http.port", 8090)

	v.SetDefault("metrics.port", 9093)
	v.SetDefault("metrics.path", "/metrics")

	// Default gRPC addresses assume all services run on localhost (dev mode).
	// In Docker Compose these are overridden via env vars.
	v.SetDefault("ingestion.address", "localhost:50051")
	v.SetDefault("engine.address", "localhost:50052")
	v.SetDefault("resolution.address", "localhost:50053")

	// Default HTTP health-check URLs for aggregate /health.
	v.SetDefault("ingestion.health_url", "http://localhost:8080/health")
	v.SetDefault("engine.health_url", "http://localhost:9091/health")
	v.SetDefault("resolution.health_url", "http://localhost:8082/health")

	// HTTP REST address for the Resolution Service (used to proxy routes).
	v.SetDefault("resolution.http_address", "http://localhost:8082")

	// API key (empty = auth disabled).
	v.SetDefault("api_key", "")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")

	// ── Config file (optional) ────────────────────────────────────────────────
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/reconx/gateway")
	_ = v.ReadInConfig()

	// ── Environment variables ─────────────────────────────────────────────────
	// e.g. RECONX_GATEWAY_ENGINE_ADDRESS=engine:50052
	//      RECONX_GATEWAY_API_KEY=s3cr3t
	v.SetEnvPrefix("RECONX_GATEWAY")
	v.AutomaticEnv()

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
