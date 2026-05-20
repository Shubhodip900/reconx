// Package config provides configuration for the ReconX Resolution Service.
package config

import (
	"time"

	"github.com/spf13/viper"
)

// Config holds all runtime configuration for the resolution service.
type Config struct {
	GRPC        GRPCConfig
	HTTP        HTTPConfig
	Database    DatabaseConfig
	Engine      EngineClientConfig
	Metrics     MetricsConfig
	Log         LogConfig
	Retry       RetryConfig
	AutoResolve AutoResolveConfig
}

// GRPCConfig holds gRPC server settings.
type GRPCConfig struct {
	Port int `mapstructure:"port"`
}

// HTTPConfig holds the REST API server settings.
type HTTPConfig struct {
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
// The Resolution Service calls it to re-trigger matching and query state.
type EngineClientConfig struct {
	Address        string        `mapstructure:"address"` // host:port
	DialTimeout    time.Duration `mapstructure:"dial_timeout"`
	RequestTimeout time.Duration `mapstructure:"request_timeout"`
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

// RetryConfig controls the background retry worker.
//
// The worker polls for MISMATCHED transactions at PollIntervalSecs intervals.
// Each retry calls the engine's ReTriggerMatch gRPC. If a transaction remains
// MISMATCHED after MaxAttempts retries, it is marked EXHAUSTED and requires
// manual resolution.
//
// Backoff formula: min(BaseBackoffSecs * 2^attempt, MaxBackoffSecs)
type RetryConfig struct {
	// Whether the retry worker is active (default: true).
	Enabled bool `mapstructure:"enabled"`

	// How often the worker scans for due retries (seconds, default: 30).
	PollIntervalSecs int `mapstructure:"poll_interval_secs"`

	// Maximum number of retry attempts before marking EXHAUSTED (default: 5).
	MaxAttempts int `mapstructure:"max_attempts"`

	// Base backoff in seconds for exponential backoff (default: 60).
	BaseBackoffSecs int `mapstructure:"base_backoff_secs"`

	// Maximum backoff cap in seconds (default: 3600 = 1 hour).
	MaxBackoffSecs int `mapstructure:"max_backoff_secs"`

	// How many transactions the worker processes per poll cycle (default: 50).
	BatchSize int `mapstructure:"batch_size"`
}

// AutoResolveConfig controls automatic conflict resolution behaviour.
//
// When the retry worker exhausts all retries, or when explicitly requested via
// the HTTP API, the auto-resolver applies a deterministic strategy to select
// the "golden record" without human intervention.
type AutoResolveConfig struct {
	// Default strategy applied when none is specified in the API request.
	// One of: "source_priority" | "latest_record" | "highest_amount" |
	//         "lowest_amount"   | "first_submitted"
	// Default: "latest_record"
	DefaultStrategy string `mapstructure:"default_strategy"`

	// Comma-separated ordered list of trusted source systems used by the
	// "source_priority" strategy. The first matching source wins.
	// Example: "payment_gateway,erp_system,vendor_portal"
	SourcePriority string `mapstructure:"source_priority"`

	// If true, automatically apply the DefaultStrategy when the retry worker
	// exhausts all attempts. If false, exhausted transactions require manual
	// intervention. Default: false.
	AutoApplyOnExhaustion bool `mapstructure:"auto_apply_on_exhaustion"`
}

// Load reads configuration from environment variables and optional config file.
func Load() (*Config, error) {
	v := viper.New()

	// ── Defaults ────────────────────────────────────────────────────────────
	v.SetDefault("grpc.port", 50053)

	v.SetDefault("http.port", 8082)

	v.SetDefault("database.dsn", "postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable")
	v.SetDefault("database.max_open_conns", 10)
	v.SetDefault("database.max_idle_conns", 3)
	v.SetDefault("database.conn_max_lifetime", "5m")

	// Engine address: the Resolution service queries the engine for mismatch data.
	v.SetDefault("engine.address", "localhost:50052")
	v.SetDefault("engine.dial_timeout", "5s")
	v.SetDefault("engine.request_timeout", "10s")

	v.SetDefault("metrics.port", 9092)
	v.SetDefault("metrics.path", "/metrics")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")

	// Retry worker defaults
	v.SetDefault("retry.enabled", true)
	v.SetDefault("retry.poll_interval_secs", 30)
	v.SetDefault("retry.max_attempts", 5)
	v.SetDefault("retry.base_backoff_secs", 60)
	v.SetDefault("retry.max_backoff_secs", 3600)
	v.SetDefault("retry.batch_size", 50)

	// Auto-resolve defaults
	v.SetDefault("auto_resolve.default_strategy", "latest_record")
	v.SetDefault("auto_resolve.source_priority", "")
	v.SetDefault("auto_resolve.auto_apply_on_exhaustion", false)

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
