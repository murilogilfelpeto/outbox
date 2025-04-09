package models

import (
	"time"
)

type Message struct {
	ID        string    `bson:"_id"`
	EventType string    `bson:"event_type"`
	Payload   []byte    `bson:"payload"`
	Status    string    `bson:"status"` // pending, processed, failed
	CreatedAt time.Time `bson:"created_at"`
	UpdatedAt time.Time `bson:"updated_at"`
}

type OrderPayload struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}

type ProcessedMessage struct {
	ID        string    `bson:"_id"`
	MessageID string    `bson:"message_id"`
	CreatedAt time.Time `bson:"created_at"`
}
