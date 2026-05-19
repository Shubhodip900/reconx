// Package config provides configuration for the ReconX API Gateway.
package config

import "github.com/spf13/viper"

// Config holds all runtime configuration for the gateway.
type Config struct {
	HTTP    HTTPConfig
	Metrics MetricsConfig
	// Upstream service addresses (gRPC host:port).
	Ingestion  UpstreamConfig
	Engine     UpstreamConfig
	Resolution UpstreamConfig
	Log        LogConfig
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

// UpstreamConfig is the address of an upstream gRPC service.
type UpstreamConfig struct {
	Address string `mapstructure:"address"` // host:port
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

	// Default addresses assume all services run on localhost (dev mode).
	// In Docker Compose these are overridden via env vars.
	v.SetDefault("ingestion.address", "localhost:50051")
	v.SetDefault("engine.address", "localhost:50052")
	v.SetDefault("resolution.address", "localhost:50053")

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
	v.SetEnvPrefix("RECONX_GATEWAY")
	v.AutomaticEnv()

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
