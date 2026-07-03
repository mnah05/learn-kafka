package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type UserEvent struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Email    string `json:"email"`
}

func Start(ctx context.Context, db *sql.DB, c *kafka.Consumer) {
	// [1] Subscribe to the topic where the outbox relay publishes events
	if err := c.Subscribe("user-events-topic", nil); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	log.Println("Consumer started, waiting for messages...")

	// [2] Continuously poll for new messages until context is cancelled
	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping consumer")
			return
		default:
			ev := c.Poll(500) // Poll with 500ms timeout
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				// [3] Handle each message with idempotent processing
				handleMessage(db, c, e)
			case kafka.Error:
				log.Printf("Consumer error: %v", e)
			}
		}
	}
}

func handleMessage(db *sql.DB, c *kafka.Consumer, msg *kafka.Message) {
	// [1] Extract the unique event ID from Kafka headers
	// This ID was set by the outbox relay when it published the message.
	eventID := getHeader(msg.Headers, "event_id")
	if eventID == "" {
		log.Printf("Message missing event_id header, skipping")
		commit(c, msg)
		return
	}

	// [2] Deserialize the JSON payload into a structured event
	var event UserEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("Failed to unmarshal event %s: %v", eventID, err)
		commit(c, msg)
		return
	}

	// [3] Idempotency check: try to insert into processed_events
	// If the event_id already exists (duplicate delivery), ON CONFLICT
	// makes this a no-op and RETURNING returns no rows.
	var inserted string
	err := db.QueryRow(
		`INSERT INTO processed_events (event_id, event_type, processed_at)
		 VALUES ($1, 'user.created', $2)
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING event_id`,
		eventID, time.Now(),
	).Scan(&inserted)

	if err == sql.ErrNoRows {
		// [3a] Event was already processed by a previous consumer run
		log.Printf("Event %s already processed, skipping", eventID)
		commit(c, msg)
		return
	}
	if err != nil {
		log.Printf("Failed to record processed event %s: %v", eventID, err)
		return // Don't commit - retry on rebalance
	}

	// [4] First time seeing this event — execute business logic
	log.Printf("Processing user event: user_id=%s, username=%s, email=%s",
		event.UserID, event.Username, event.Email)

	// [5] Commit the offset so we don't re-process on restart
	commit(c, msg)
}

func commit(c *kafka.Consumer, msg *kafka.Message) {
	if _, err := c.CommitMessage(msg); err != nil {
		log.Printf("Failed to commit offset: %v", err)
	}
}

func getHeader(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
