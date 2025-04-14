package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/murilogilfelpeto/outbox/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log/slog"
	"os"
	"strings"
	"time"
)

var (
	mongoURI         = getEnv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
	mongoHost        = getEnv("MONGO_HOST", "127.0.0.1")
	mongoDatabase    = getEnv("MONGO_DATABASE", "outbox_demo")
	outboxCollection = getEnv("OUTBOX_COLLECTION", "outbox")
	kafkaBrokers     = getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic       = getEnv("KAFKA_TOPIC", "orders")
	kafkaGroupID     = getEnv("KAFKA_GROUP_ID", "order-consumer-group")
)

func main() {
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	client, err := initializeMongoClient(ctx)
	if err != nil {
		slog.Error("Error initializing MongoDB client", "ERROR", err)
		panic("failed to connect to MongoDB")
	}
	defer disconnectMongoClient(ctx, client)

	outbox := client.Database(mongoDatabase).Collection(outboxCollection)

	consumer, err := initializeKafkaConsumer()
	if err != nil {
		slog.Error("Error initializing Kafka consumer", "ERROR", err)
		panic("failed to initialize Kafka consumer")
	}
	defer closeKafkaConsumer(consumer)

	if err := consumer.SubscribeTopics([]string{kafkaTopic}, nil); err != nil {
		slog.Error("Error subscribing to Kafka topic", "ERROR", err)
		panic("failed to subscribe to Kafka topic")
	}

	slog.Info("Starting to consume messages...")
	consumeMessages(ctx, consumer, outbox)
}

func initializeMongoClient(ctx context.Context) (*mongo.Client, error) {
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

func disconnectMongoClient(ctx context.Context, client *mongo.Client) {
	if err := client.Disconnect(ctx); err != nil {
		slog.Error("Error disconnecting from MongoDB", "ERROR", err)
	}
}

func initializeKafkaConsumer() (*kafka.Consumer, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"group.id":          kafkaGroupID,
		"auto.offset.reset": "earliest",
	}
	return kafka.NewConsumer(configMap)
}

func closeKafkaConsumer(consumer *kafka.Consumer) {
	if err := consumer.Close(); err != nil {
		slog.Error("Error closing Kafka consumer", "ERROR", err)
	}
}

func consumeMessages(ctx context.Context, consumer *kafka.Consumer, outbox *mongo.Collection) {
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			handleKafkaError(err)
			continue
		}

		if err := processKafkaMessage(ctx, msg, outbox); err != nil {
			slog.Error("Error processing Kafka message", "ERROR", err)
		}
	}
}

func handleKafkaError(err error) {
	if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.IsFatal() {
		slog.Error("Fatal error while consuming message", "ERROR", err)
		panic("fatal error while consuming message")
	} else {
		slog.Error("Error while consuming message", "ERROR", err)
	}
}

func processKafkaMessage(ctx context.Context, msg *kafka.Message, outbox *mongo.Collection) error {
	slog.Info("Received message",
		"TOPIC", *msg.TopicPartition.Topic,
		"PARTITION", msg.TopicPartition.Partition,
		"OFFSET", msg.TopicPartition.Offset,
		"KEY", string(msg.Key),
		"VALUE", string(msg.Value))

	var payload models.OrderPayload
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return fmt.Errorf("error unmarshalling message payload: %w", err)
	}

	if isMessageProcessed(ctx, outbox, payload.OrderID) {
		slog.Info("Message already processed", "ID", payload.OrderID)
		return nil
	}

	slog.Info("Processing order", "ORDER_ID", payload.OrderID, "CUSTOMER_ID", payload.CustomerID, "AMOUNT", payload.Amount)
	time.Sleep(200 * time.Millisecond) // Simulate processing time
	slog.Info("Order processed successfully", "ORDER_ID", payload.OrderID)

	return markMessageAsProcessed(ctx, outbox, payload.OrderID)
}

func isMessageProcessed(ctx context.Context, outbox *mongo.Collection, orderID string) bool {
	filter := bson.M{"aggregate_id": orderID, "event_type": models.OrderCreated, "aggregate_type": "order", "status": models.Processed}
	count, err := outbox.CountDocuments(ctx, filter)
	if err != nil {
		slog.Error("Error verifying message processed", "ERROR", err)
		return false
	}
	return count > 0
}

func markMessageAsProcessed(ctx context.Context, outbox *mongo.Collection, orderID string) error {
	filter := bson.M{"aggregate_id": orderID, "event_type": models.OrderCreated, "aggregate_type": "order"}
	_, err := outbox.UpdateOne(ctx, filter, bson.M{"$set": bson.M{"status": models.Processed}})
	return err
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	slog.Warn("NO ENV VAR", "KEY", key, "FALLBACK", fallback)
	return fallback
}
