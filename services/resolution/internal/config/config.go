// Package config provides configuration for the ReconX Resolution Service.
package config

import (
	"time"

	"github.com/spf13/viper"
)

// Config holds all runtime configuration for the resolution service.
type Config struct {
	GRPC     GRPCConfig
	Database DatabaseConfig
	Engine   EngineClientConfig
	Metrics  MetricsConfig
	Log      LogConfig
}

// GRPCConfig holds gRPC server settings.
type GRPCConfig struct {
	Port int `mapstructure:"port"`
}

// DatabaseConfig holds PostgreSQL connection settings.
type DatabaseConfig struct {
	DSN             string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

// EngineClientConfig is the address of the Reconciliation Engine gRPC server.
// The Resolution Service calls it to stream mismatched state responses.
type EngineClientConfig struct {
	Address string `mapstructure:"address"` // host:port
}

// MetricsConfig holds Prometheus metrics export settings.
type MetricsConfig struct {
	Port int    `mapstructure:"port"`
	Path string `mapstructure:"path"`
}

// LogConfig controls structured logging behaviour.
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// Load reads configuration from environment variables and optional config file.
func Load() (*Config, error) {
	v := viper.New()

	// ── Defaults ────────────────────────────────────────────────────────────
	v.SetDefault("grpc.port", 50053)

	v.SetDefault("database.dsn", "postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable")
	v.SetDefault("database.max_open_conns", 10)
	v.SetDefault("database.max_idle_conns", 3)
	v.SetDefault("database.conn_max_lifetime", "5m")

	// Engine address: the Resolution service queries the engine for mismatch data.
	v.SetDefault("engine.address", "localhost:50052")

	v.SetDefault("metrics.port", 9092)
	v.SetDefault("metrics.path", "/metrics")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")

	// ── Config file (optional) ───────────────────────────────────────────────
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/reconx/resolution")
	_ = v.ReadInConfig()

	// ── Environment variables ────────────────────────────────────────────────
	// e.g. RECONX_RESOLUTION_GRPC_PORT=50053
	v.SetEnvPrefix("RECONX_RESOLUTION")
	v.AutomaticEnv()

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
