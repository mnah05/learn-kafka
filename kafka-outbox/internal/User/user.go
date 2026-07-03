package user

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

func CreateUser(db *sql.DB, username, email string) error {
	//[1] Begin Transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback()

	userID := uuid.New()

	// [2] Insert the actual business data (the User)
	_, err = tx.Exec(
		"INSERT INTO users (id, username, email) VALUES ($1, $2, $3)",
		userID, username, email,
	)
	if err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}
	// [3] Prepare the event payload for Kafka
	eventPayload := map[string]string{
		"user_id":  userID.String(),
		"username": username,
		"email":    email,
	}
	payloadBytes, err := json.Marshal(eventPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal event payload: %w", err)
	}
	//[4] Insert into outbox event
	eventID := uuid.New()
	_, err = tx.Exec(
		`INSERT INTO outbox_events
			(id, aggregate_type, aggregate_id, event_type, payload)
			VALUES ($1, $2, $3, $4, $5)`,
		eventID, "User", userID, "user.created", payloadBytes,
	)
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}
	// [5] Commit the transaction. Both rows are saved permanently.
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
