package main

import (
	"context"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/murilogilfelpeto/outbox/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"strings"
	"time"
)

const (
	mongoURI            = "mongodb://localhost:27017/?directConnection=true"
	mongoHost           = "127.0.0.1"
	mongoDatabase       = "outbox_demo"
	processedCollection = "processed_messages"
	kafkaBrokers        = "localhost:9092"
	kafkaTopic          = "orders"
	kafkaGroupID        = "order-consumer-group"
)

func main() {
	ctx := context.Background()
	hosts := strings.Split(mongoHost, ",")
	opts := options.Client().
		SetConnectTimeout(5 * time.Second).
		SetSocketTimeout(5 * time.Second).
		SetServerSelectionTimeout(5 * time.Second).
		SetMaxPoolSize(5).
		SetMinPoolSize(2).
		SetHosts(hosts).
		ApplyURI(mongoURI)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		log.Fatalf("Error connecting to mongoDB: %v", err)
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Printf("Error disconnecting from mongoDB: %v", err)
		}
	}()

	db := client.Database(mongoDatabase)
	processed := db.Collection(processedCollection)

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"group.id":          kafkaGroupID,
		"auto.offset.reset": "earliest",
	}
	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		log.Fatalf("Error creating kafka consumer: %v", err)
	}
	defer func(consumer *kafka.Consumer) {
		err := consumer.Close()
		if err != nil {
			log.Printf("Error closing kafka consumer: %v", err)
		}
	}(consumer)

	err = consumer.SubscribeTopics([]string{kafkaTopic}, nil)
	if err != nil {
		log.Fatalf("Error subscribing to kafka topic: %v", err)
	}

	log.Printf("Starting consuming messages")

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			if err.(kafka.Error).IsFatal() {
				log.Fatalf("Fatal error while consuming message: %v", err)
			} else {
				log.Printf("Error while consuming message: %v", err)
				continue
			}
		}

		log.Printf("Received message: Topic=%s, Partition=%d, Offset=%v, Key=%s, Message=%s\\n",
			*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), string(msg.Value))

		var payload models.OrderPayload
		if err := json.Unmarshal(msg.Value, &payload); err != nil {
			log.Printf("Error unmarshalling message payload: %v", err)
			continue
		}

		if isMessageProcessed(ctx, processed, payload.OrderID) {
			log.Printf("Message with ID %s already processed, skipping", string(msg.Key))
			continue
		}

		log.Printf("Processing order %s for client %s with amount %.2f", payload.OrderID, payload.CustomerID, payload.Amount)
		time.Sleep(200 * time.Millisecond)
		log.Printf("Order %s processed successfully", payload.OrderID)

		err = markMessageAsProcessed(context.Background(), processed, payload.OrderID)
		if err != nil {
			log.Printf("Error marking message as processed: %v", err)
			continue
		}
	}
}

func isMessageProcessed(ctx context.Context, processedCollection *mongo.Collection, messageID string) bool {
	filter := bson.M{"message_id": messageID}
	count, err := processedCollection.CountDocuments(ctx, filter)
	if err != nil {
		log.Printf("Error verifying message processed %v", err)
		return false
	}
	return count > 0
}

func markMessageAsProcessed(ctx context.Context, processedCollection *mongo.Collection, messageID string) error {
	_, err := processedCollection.InsertOne(ctx, models.ProcessedMessage{
		ID:        uuid.New().String(),
		MessageID: messageID,
		CreatedAt: time.Now(),
	})
	return err
}
