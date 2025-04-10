package models

import (
	"time"
)

type Status string

const (
	Published Status = "published"
	Processed Status = "processed"
	Created   Status = "created"
)

type EventType string

const (
	OrderCreated EventType = "OrderCreated"
)

type Message struct {
	ID            string    `bson:"_id"`
	AggregateID   string    `bson:"aggregate_id"`
	AggregateType string    `bson:"aggregate_type"`
	EventType     EventType `bson:"event_type"`
	Payload       []byte    `bson:"payload"`
	Status        Status    `bson:"status"` // pending, processed, failed
	CreatedAt     time.Time `bson:"created_at"`
	UpdatedAt     time.Time `bson:"updated_at"`
	Topic         string    `bson:"topic"`
}

type OrderPayload struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}
