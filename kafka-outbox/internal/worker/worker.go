package worker

import (
	"context"
	"database/sql"
	"log"
	"time"
)

type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

type KafkaPublisher interface {
	Publish(msg Message) error
}

func StartOutBoxRelay(ctx context.Context, db *sql.DB, kafka KafkaPublisher, cleanupTTL time.Duration) {
	relayTick := time.NewTicker(2 * time.Second)
	cleanupTick := time.NewTicker(1 * time.Hour)
	defer relayTick.Stop()
	defer cleanupTick.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping outbox relay")
			return
		case <-relayTick.C:
			processOutbox(db, kafka)
		case <-cleanupTick.C:
			cleanupOutbox(db, cleanupTTL)
		}
	}
}

func processOutbox(db *sql.DB, kafka KafkaPublisher) {
	rows, err := db.Query(`
		SELECT id, event_type, aggregate_id, payload
		FROM outbox_events
		WHERE processed_at IS NULL
		ORDER BY created_at ASC
		LIMIT 100
	`)
	if err != nil {
		log.Printf("Error querying outbox: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id          string
			eventType   string
			aggregateID string
			payload     []byte
		)
		if err := rows.Scan(&id, &eventType, &aggregateID, &payload); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		err = kafka.Publish(Message{
			Topic:   "user-events-topic",
			Key:     []byte(aggregateID),
			Value:   payload,
			Headers: map[string]string{"event_id": id},
		})
		if err != nil {
			log.Printf("Failed to publish event %s: %v", id, err)
			return
		}

		_, err = db.Exec(
			"UPDATE outbox_events SET processed_at = $1 WHERE id = $2",
			time.Now(), id,
		)
		if err != nil {
			log.Printf("Failed to mark event %s as processed: %v", id, err)
		}
	}
}

func cleanupOutbox(db *sql.DB, ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	result, err := db.Exec(
		`DELETE FROM outbox_events WHERE processed_at IS NOT NULL AND processed_at < $1`,
		cutoff,
	)
	if err != nil {
		log.Printf("Failed to cleanup outbox: %v", err)
		return
	}
	n, _ := result.RowsAffected()
	if n > 0 {
		log.Printf("Cleaned up %d old outbox events", n)
	}
}
