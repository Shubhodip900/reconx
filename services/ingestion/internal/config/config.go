// Package config provides configuration management for the ReconX Ingestion Service.
// It loads settings from environment variables and config files using Viper.
package config

import (
	"time"

	"github.com/spf13/viper"
)

// Config holds all runtime configuration for the ingestion service.
type Config struct {
	GRPC       GRPCConfig
	HTTP       HTTPConfig
	Database   DatabaseConfig
	Kafka      KafkaConfig
	RateLimit  RateLimitConfig
	DLQ        DLQConfig
	Metrics    MetricsConfig
	Log        LogConfig
	Idempotency IdempotencyConfig
}

// GRPCConfig holds gRPC server settings.
type GRPCConfig struct {
	Port            int           `mapstructure:"port"`
	MaxRecvMsgSize  int           `mapstructure:"max_recv_msg_size_mb"`
	ConnectionTimeout time.Duration `mapstructure:"connection_timeout"`
}

// HTTPConfig holds the REST/webhook HTTP server settings.
type HTTPConfig struct {
	Port            int           `mapstructure:"port"`
	ReadTimeout     time.Duration `mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`
	MaxBodySizeMB   int64         `mapstructure:"max_body_size_mb"`
}

// DatabaseConfig holds PostgreSQL connection settings.
type DatabaseConfig struct {
	DSN             string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

// KafkaConfig holds Kafka consumer settings.
type KafkaConfig struct {
	Brokers         []string      `mapstructure:"brokers"`
	GroupID         string        `mapstructure:"group_id"`
	Topic           string        `mapstructure:"topic"`
	MinBytes        int           `mapstructure:"min_bytes"`
	MaxBytes        int           `mapstructure:"max_bytes"`
	CommitInterval  time.Duration `mapstructure:"commit_interval"`
	Enabled         bool          `mapstructure:"enabled"`
}

// RateLimitConfig holds per-source rate-limiting settings.
type RateLimitConfig struct {
	// DefaultRPS is the default requests per second per source system.
	DefaultRPS  float64            `mapstructure:"default_rps"`
	// Overrides maps source_system name → custom RPS.
	Overrides   map[string]float64 `mapstructure:"overrides"`
}

// DLQConfig configures the dead-letter queue behaviour.
type DLQConfig struct {
	// TableName is the PostgreSQL table for failed records.
	TableName   string `mapstructure:"table_name"`
	// MaxRetries before a record is permanently failed.
	MaxRetries  int    `mapstructure:"max_retries"`
}

// MetricsConfig holds Prometheus metrics export settings.
type MetricsConfig struct {
	Port int    `mapstructure:"port"`
	Path string `mapstructure:"path"`
}

// LogConfig controls structured logging behaviour.
type LogConfig struct {
	Level  string `mapstructure:"level"`  // debug | info | warn | error
	Format string `mapstructure:"format"` // json | console
}

// IdempotencyConfig controls idempotency store behaviour.
type IdempotencyConfig struct {
	// TTL is how long idempotency keys are retained.
	TTL time.Duration `mapstructure:"ttl"`
}

// Load reads configuration from environment variables and optional config file.
// Environment variables take precedence, following 12-factor app principles.
func Load() (*Config, error) {
	v := viper.New()

	// ── Defaults ──────────────────────────────────────────────────────────────
	v.SetDefault("grpc.port", 50051)
	v.SetDefault("grpc.max_recv_msg_size_mb", 16)
	v.SetDefault("grpc.connection_timeout", "30s")

	v.SetDefault("http.port", 8080)
	v.SetDefault("http.read_timeout", "30s")
	v.SetDefault("http.write_timeout", "30s")
	v.SetDefault("http.max_body_size_mb", 10)

	v.SetDefault("database.dsn", "postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable")
	v.SetDefault("database.max_open_conns", 25)
	v.SetDefault("database.max_idle_conns", 5)
	v.SetDefault("database.conn_max_lifetime", "5m")

	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.group_id", "reconx-ingestion")
	v.SetDefault("kafka.topic", "reconx.records.raw")
	v.SetDefault("kafka.min_bytes", 1)
	v.SetDefault("kafka.max_bytes", 10_000_000)
	v.SetDefault("kafka.commit_interval", "1s")
	v.SetDefault("kafka.enabled", false)

	v.SetDefault("ratelimit.default_rps", 1000.0)

	v.SetDefault("dlq.table_name", "ingestion_dlq")
	v.SetDefault("dlq.max_retries", 3)

	v.SetDefault("metrics.port", 9090)
	v.SetDefault("metrics.path", "/metrics")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")

	v.SetDefault("idempotency.ttl", "24h")

	// ── Config file (optional) ────────────────────────────────────────────────
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/reconx/ingestion")
	_ = v.ReadInConfig() // not fatal if missing

	// ── Environment variables ─────────────────────────────────────────────────
	v.SetEnvPrefix("RECONX")
	v.AutomaticEnv()

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
