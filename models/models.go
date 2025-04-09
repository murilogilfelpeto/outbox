package models

import (
	"github.com/google/uuid"
	"time"
)

type Message struct {
	ID        uuid.UUID
	EventType string `bson:"event_type"`
	Payload   []byte `bson:"payload"`
	Status    string `bson:"status"` // pending, processed, failed
	CreatedAt int64  `bson:"created_at"`
	UpdatedAt int64  `bson:"updated_at"`
}

type OrderCreatedPayload struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}

type ProcessedMessage struct {
	ID        uuid.UUID `bson:"_id"`
	MessageID string    `bson:"message_id"`
	CreatedAt time.Time `bson:"created_at"`
}
