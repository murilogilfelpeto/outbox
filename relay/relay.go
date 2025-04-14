package main

import (
	"context"
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
	relayInterval    = 10 * time.Second
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

	producer, err := initializeKafkaProducer()
	if err != nil {
		slog.Error("Error initializing Kafka producer", "ERROR", err)
		panic("failed to initialize Kafka producer")
	}
	defer producer.Close()

	outbox := client.Database(mongoDatabase).Collection(outboxCollection)

	slog.Info("Starting relay...")
	startRelay(ctx, outbox, producer)
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

func initializeKafkaProducer() (*kafka.Producer, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
	}
	return kafka.NewProducer(configMap)
}

func startRelay(ctx context.Context, outbox *mongo.Collection, producer *kafka.Producer) {
	ticker := time.NewTicker(relayInterval)
	defer ticker.Stop()

	for range ticker.C {
		processOutboxMessages(ctx, outbox, producer)
	}
}

func processOutboxMessages(ctx context.Context, outbox *mongo.Collection, producer *kafka.Producer) {
	cursor, err := outbox.Find(ctx, bson.M{"status": models.Created})
	if err != nil {
		slog.Error("Error fetching pending messages", "ERROR", err)
		return
	}
	defer func() {
		if err := cursor.Close(ctx); err != nil {
			slog.Error("Error closing cursor", "ERROR", err)
		}
	}()

	for cursor.Next(ctx) {
		var message models.Message
		if err := cursor.Decode(&message); err != nil {
			slog.Error("Error decoding message", "ERROR", err)
			continue
		}

		if err := processMessage(ctx, outbox, producer, message); err != nil {
			slog.Error("Error processing message", "MESSAGE_ID", message.ID, "ERROR", err)
		}
	}

	if err := cursor.Err(); err != nil {
		slog.Error("Error iterating cursor", "ERROR", err)
	}
}

func processMessage(ctx context.Context, outbox *mongo.Collection, producer *kafka.Producer, message models.Message) error {
	if err := publishToKafka(producer, message.Topic, message.Payload); err != nil {
		return fmt.Errorf("error publishing message to Kafka: %w", err)
	}

	_, err := outbox.UpdateOne(ctx, bson.M{"_id": message.ID}, bson.M{"$set": bson.M{"status": models.Published}})
	if err != nil {
		return fmt.Errorf("error updating message status: %w", err)
	}

	slog.Info("Message processed and status updated to 'processed'", "MESSAGE_ID", message.ID)
	return nil
}

func publishToKafka(producer *kafka.Producer, topic string, payload []byte) error {
	deliverChannel := make(chan kafka.Event)
	defer close(deliverChannel)

	partition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: kafka.PartitionAny,
	}
	message := &kafka.Message{
		TopicPartition: partition,
		Value:          payload,
	}

	err := producer.Produce(message, deliverChannel)
	if err != nil {
		return fmt.Errorf("error queuing message for delivery: %w", err)
	}

	e := <-deliverChannel
	switch ev := e.(type) {
	case *kafka.Message:
		slog.Info("Message produced",
			"TOPIC", *ev.TopicPartition.Topic,
			"PARTITION", ev.TopicPartition.Partition,
			"OFFSET", ev.TopicPartition.Offset)
		return nil
	case *kafka.Error:
		return fmt.Errorf("error producing message: %v", ev)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	slog.Warn("NO ENV VAR", "KEY", key, "FALLBACK", fallback)
	return fallback
}
