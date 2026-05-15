// Kafka Consumer Adapter — consumes records from a Kafka topic.
// Uses segmentio/kafka-go for idiomatic Go Kafka support.
// Designed for high-throughput scenarios where upstream systems publish
// to Kafka rather than calling our gRPC/REST APIs directly.
//
// Backpressure is managed via the bounded out channel; when the channel
// is full, the consumer pauses (blocked Recv) which gRPC flow-controls upstream.
package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/reconx/services/ingestion/internal/pipeline"
)

// KafkaConfig configures a Kafka consumer adapter.
type KafkaConsumerConfig struct {
	// ID is a stable adapter instance identifier.
	ID string

	// SourceSystem tags all records consumed from this topic.
	SourceSystem string

	// Brokers is the list of Kafka bootstrap servers.
	Brokers []string

	// Topic is the Kafka topic to consume.
	Topic string

	// GroupID is the Kafka consumer group ID.
	GroupID string

	// MinBytes / MaxBytes control fetch sizes.
	MinBytes int
	MaxBytes int

	// CommitInterval controls how often offsets are committed.
	CommitInterval time.Duration
}

// KafkaConsumer reads messages from a Kafka topic and emits NormalizedRecords.
type KafkaConsumer struct {
	cfg    KafkaConsumerConfig
	reader *kafka.Reader
	log    *zap.Logger
}

// NewKafkaConsumer creates a KafkaConsumer with the given configuration.
func NewKafkaConsumer(cfg KafkaConsumerConfig, log *zap.Logger) *KafkaConsumer {
	if cfg.MinBytes == 0 {
		cfg.MinBytes = 1
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 10_000_000
	}
	if cfg.CommitInterval == 0 {
		cfg.CommitInterval = time.Second
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		CommitInterval: cfg.CommitInterval,
		// Start from the latest message when there is no committed offset.
		StartOffset: kafka.LastOffset,
	})

	return &KafkaConsumer{
		cfg:    cfg,
		reader: reader,
		log:    log.With(zap.String("adapter", cfg.ID), zap.String("topic", cfg.Topic)),
	}
}

func (k *KafkaConsumer) ID() string                     { return k.cfg.ID }
func (k *KafkaConsumer) AdapterType() pipeline.AdapterType { return pipeline.AdapterKafka }

// Start reads messages from Kafka and pushes them to out until ctx is cancelled.
func (k *KafkaConsumer) Start(ctx context.Context, out chan<- *pipeline.NormalizedRecord) error {
	defer k.reader.Close()
	k.log.Info("Kafka consumer started")

	for {
		msg, err := k.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				k.log.Info("Kafka consumer stopped")
				return ctx.Err()
			}
			k.log.Warn("Kafka fetch error", zap.Error(err))
			continue
		}

		rec, err := k.toRecord(msg)
		if err != nil {
			k.log.Warn("failed to convert Kafka message", zap.Error(err),
				zap.String("topic", msg.Topic),
				zap.Int64("offset", msg.Offset))
			// Commit anyway to avoid infinite retry of malformed messages.
			_ = k.reader.CommitMessages(ctx, msg)
			continue
		}

		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}

		// Commit after successful queue insertion.
		if err := k.reader.CommitMessages(ctx, msg); err != nil {
			k.log.Warn("commit failed", zap.Error(err))
		}
	}
}

// toRecord converts a Kafka message into a NormalizedRecord skeleton.
// Message value must be JSON. The Kafka key is used as the idempotency_key.
func (k *KafkaConsumer) toRecord(msg kafka.Message) (*pipeline.NormalizedRecord, error) {
	if len(msg.Value) == 0 {
		return nil, fmt.Errorf("empty Kafka message value at offset %d", msg.Offset)
	}

	var partial struct {
		IdempotencyKey string `json:"idempotency_key"`
		TransactionRef string `json:"transaction_ref"`
		SourceSystem   string `json:"source_system"`
	}
	_ = json.Unmarshal(msg.Value, &partial)

	// Use Kafka message key as idempotency_key if not embedded in value.
	idempKey := partial.IdempotencyKey
	if idempKey == "" && len(msg.Key) > 0 {
		idempKey = string(msg.Key)
	}
	if idempKey == "" {
		idempKey = fmt.Sprintf("kafka-%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)
	}

	// Use configured source system or fall back to embedded value.
	src := k.cfg.SourceSystem
	if src == "" {
		src = partial.SourceSystem
	}
	if src == "" {
		src = "kafka-unknown"
	}

	payload := make([]byte, len(msg.Value))
	copy(payload, msg.Value)

	// Extract trace ID from Kafka headers.
	traceID := ""
	for _, h := range msg.Headers {
		if h.Key == "X-Trace-Id" || h.Key == "trace_id" {
			traceID = string(h.Value)
			break
		}
	}
	if traceID == "" {
		traceID = uuid.New().String()
	}

	return &pipeline.NormalizedRecord{
		IdempotencyKey: idempKey,
		TransactionRef: partial.TransactionRef,
		SourceSystem:   src,
		AdapterType:    pipeline.AdapterKafka,
		RawPayload:     payload,
		TraceID:        traceID,
		Tags: map[string]string{
			"kafka_topic":     msg.Topic,
			"kafka_partition": fmt.Sprintf("%d", msg.Partition),
			"kafka_offset":    fmt.Sprintf("%d", msg.Offset),
		},
	}, nil
}
