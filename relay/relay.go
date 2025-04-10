package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/murilogilfelpeto/outbox/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
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
	outbox := db.Collection(outboxCollection)

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
	}
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Fatalf("Error creating kafka producer")
	}
	defer producer.Close()

	log.Print("Starting relay...")

	ticker := time.NewTicker(relayInterval)
	defer ticker.Stop()

	for range ticker.C {
		cursor, err := outbox.Find(ctx, bson.M{"status": models.Created})
		if err != nil {
			log.Printf("Error fetching pending messages: %v", err)
			continue
		}
		func() {
			defer func(cursor *mongo.Cursor, ctx context.Context) {
				err := cursor.Close(ctx)
				if err != nil {
					log.Printf("Error closing cursor: %v", err)
				}
			}(cursor, ctx)

			for cursor.Next(ctx) {
				var message models.Message
				if err := cursor.Decode(&message); err != nil {
					log.Printf("Error decoding message: %v", err)
					continue
				}

				err := publishToKafka(producer, message.Topic, message.Payload)
				if err != nil {
					log.Printf("Error publishing message to Kafka: %v", err)
					continue
				}

				_, err = outbox.UpdateOne(ctx, bson.M{"_id": message.ID}, bson.M{"$set": bson.M{"status": models.Published}})
				if err != nil {
					log.Printf("Error updating message status: %v", err)
					continue
				}
				log.Printf("Message %s processed and status updated to 'processed'", message.ID)
			}
		}()
		if err := cursor.Err(); err != nil {
			log.Printf("Error iterating cursor: %v", err)
		}
	}
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
		log.Printf("Message produced to topic %s [%d] at offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
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
	return fallback
}
