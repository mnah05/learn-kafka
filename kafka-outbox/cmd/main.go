package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	user "kafka-learn/kafka-outbox/internal/User"
	"kafka-learn/kafka-outbox/internal/consumer"
	"kafka-learn/kafka-outbox/internal/worker"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/labstack/echo/v4"
	_ "github.com/lib/pq"
)

func main() {
	// [1] Load configuration from environment variables with sensible defaults
	dbURL := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	kafkaBrokers := env("KAFKA_BROKERS", "localhost:9092")

	// [2] Connect to PostgreSQL
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to database")

	// [3] Auto-create tables (users, outbox_events, processed_events) and indexes
	migrate(db)

	// [4] Create the Kafka producer — sends events from the outbox relay to Kafka
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBrokers})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// [5] Handle async delivery reports in a background goroutine
	go handleDeliveryReports(producer)

	// [6] Create the Kafka consumer — reads events from Kafka and processes them idempotently
	consumerClient, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaBrokers,
		"group.id":           "outbox-consumer",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false, // Manual commit after idempotent processing
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumerClient.Close()

	// [7] Create a cancellable context for graceful shutdown of background workers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// [8] Start the outbox relay — polls DB for unprocessed events, publishes to Kafka,
	//     and periodically cleans up old processed records (TTL = 7 days)
	publisher := &outboxPublisher{p: producer}
	ttl := 7 * 24 * time.Hour
	go worker.StartOutBoxRelay(ctx, db, publisher, ttl)

	// [9] Start the downstream consumer — consumes from Kafka and processes idempotently
	go consumer.Start(ctx, db, consumerClient)

	// [10] Start the HTTP API — creates users (writes to DB + outbox in one transaction)
	e := echo.New()
	e.POST("/users", func(c echo.Context) error {
		var req struct {
			Username string `json:"username"`
			Email    string `json:"email"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request"})
		}
		if err := user.CreateUser(db, req.Username, req.Email); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return c.JSON(http.StatusCreated, map[string]string{"status": "created"})
	})

	// [11] Start the HTTP server in a background goroutine
	go func() {
		log.Println("HTTP server listening on :8080")
		if err := e.Start(":8080"); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// [12] Wait for SIGINT/SIGTERM, then gracefully shut everything down
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down...")
	cancel()             // Stop outbox relay and consumer
	producer.Flush(5000) // Drain any in-flight messages

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	e.Shutdown(shutdownCtx) // Gracefully stop the HTTP server
}

// outboxPublisher implements worker.KafkaPublisher using the Confluent Go client.
// It publishes synchronously — waits for a delivery report before returning.
type outboxPublisher struct {
	p *kafka.Producer
}

func (w *outboxPublisher) Publish(msg worker.Message) error {
	// [1] Build the Kafka message from the outbox Message struct
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &msg.Topic, Partition: int32(-1)},
		Key:            msg.Key,
		Value:          msg.Value,
	}
	// [2] Copy headers (includes event_id for downstream consumer idempotency)
	for k, v := range msg.Headers {
		kafkaMsg.Headers = append(kafkaMsg.Headers, kafka.Header{Key: k, Value: []byte(v)})
	}

	// [3] Produce synchronously: create a delivery channel, produce, then block
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	if err := w.p.Produce(kafkaMsg, deliveryChan); err != nil {
		return err
	}

	// [4] Wait for the delivery report
	e := <-deliveryChan
	m := e.(*kafka.Message)
	return m.TopicPartition.Error
}

// handleDeliveryReports processes async delivery notifications for messages
// produced outside the outbox path (e.g. any stray delivery reports).
func handleDeliveryReports(p *kafka.Producer) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
			}
		}
	}
}

// migrate creates the required database tables and indexes if they don't exist.
func migrate(db *sql.DB) {
	queries := []string{
		// [1] Business data table
		`CREATE TABLE IF NOT EXISTS users (
			id UUID PRIMARY KEY,
			username VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL
		)`,
		// [2] Outbox table — stores events atomically with business data
		`CREATE TABLE IF NOT EXISTS outbox_events (
			id UUID PRIMARY KEY,
			aggregate_type VARCHAR(255) NOT NULL,
			aggregate_id UUID NOT NULL,
			event_type VARCHAR(255) NOT NULL,
			payload JSONB NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			processed_at TIMESTAMP
		)`,
		// [3] Partial index — fast lookups of unprocessed events for the relay
		`CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed
		 ON outbox_events (processed_at, created_at)
		 WHERE processed_at IS NULL`,
		// [4] Idempotency table — tracks which events the consumer has processed
		`CREATE TABLE IF NOT EXISTS processed_events (
			event_id UUID PRIMARY KEY,
			event_type VARCHAR(255) NOT NULL,
			processed_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
	}
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			log.Fatalf("Migration failed: %v", err)
		}
	}
	log.Println("Database migration completed")
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
