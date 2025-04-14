package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/murilogilfelpeto/outbox/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log/slog"
	"math/rand"
	"os"
	"strings"
	"time"
)

var (
	mongoURI         = getEnv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
	mongoHost        = getEnv("MONGO_HOST", "127.0.0.1")
	mongoDatabase    = getEnv("MONGO_DATABASE", "outbox_demo")
	outboxCollection = getEnv("OUTBOX_COLLECTION", "outbox")
	orderCollection  = getEnv("ORDER_COLLECTION", "orders")
)

func main() {
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	client, err := connectToMongo(ctx)
	if err != nil {
		slog.Error("Error connecting to MongoDB", "ERROR", err)
		panic("failed to connect to MongoDB")
	}
	defer disconnectMongo(ctx, client)

	db := client.Database(mongoDatabase)
	outbox := db.Collection(outboxCollection)
	orders := db.Collection(orderCollection)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := processOrder(ctx, client, orders, outbox); err != nil {
			slog.Error("Error processing order", "ERROR", err)
		}
	}
}

func connectToMongo(ctx context.Context) (*mongo.Client, error) {
	hosts := strings.Split(mongoHost, ",")
	opts := options.Client().
		SetConnectTimeout(5 * time.Second).
		SetSocketTimeout(5 * time.Second).
		SetServerSelectionTimeout(5 * time.Second).
		SetMaxPoolSize(5).
		SetMinPoolSize(2).
		SetHosts(hosts).
		ApplyURI(mongoURI)

	return mongo.Connect(ctx, opts)
}

func disconnectMongo(ctx context.Context, client *mongo.Client) {
	if err := client.Disconnect(ctx); err != nil {
		slog.Error("Error disconnecting from MongoDB", "ERROR", err)
	}
}

func processOrder(ctx context.Context, client *mongo.Client, orders, outbox *mongo.Collection) error {
	amount := generateAmount(20.0, 1000.0)
	orderID := uuid.New().String()
	order := models.OrderPayload{
		OrderID:    orderID,
		CustomerID: uuid.New().String(),
		Amount:     amount,
	}

	payload, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("error marshalling order payload: %w", err)
	}

	session, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("error starting session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) {
		orderDocument := bson.M{
			"_id":         orderID,
			"customer_id": order.CustomerID,
			"amount":      order.Amount,
		}

		if _, err := orders.InsertOne(sc, orderDocument); err != nil {
			return nil, fmt.Errorf("error inserting order: %w", err)
		}

		slog.Info("Order created", "ORDER_ID", orderID)

		message := models.Message{
			ID:            uuid.New().String(),
			AggregateID:   orderID,
			AggregateType: "order",
			EventType:     models.OrderCreated,
			Payload:       payload,
			Status:        models.Created,
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
			Topic:         "orders",
		}

		if _, err := outbox.InsertOne(sc, message); err != nil {
			return nil, fmt.Errorf("error inserting event at outbox: %w", err)
		}

		slog.Info("Message prepared for Kafka",
			"MESSAGE_ID", message.ID,
			"TOPIC", message.Topic,
			"ORDER_ID", message.AggregateID)

		return nil, nil
	})

	return err
}

func generateAmount(min, max float64) float64 {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)

	randomValue := random.Float64()
	return float64(int((min+randomValue*(max-min))*100)) / 100
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	slog.Warn("NO ENV VAR", "KEY", key, "FALLBACK", fallback)
	return fallback
}
