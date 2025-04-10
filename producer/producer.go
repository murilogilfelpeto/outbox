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
	"log"
	"math/rand"
	"strings"
	"time"
)

const (
	mongoURI         = "mongodb://localhost:27017/?directConnection=true"
	mongoHost        = "127.0.0.1"
	mongoDatabase    = "outbox_demo"
	outboxCollection = "outbox"
	orderCollection  = "orders"
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
	orders := db.Collection(orderCollection)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		amount := generateAmount(20.0, 1000.0)
		order := models.OrderPayload{
			OrderID:    uuid.New().String(),
			CustomerID: uuid.New().String(),
			Amount:     amount,
		}

		payload, err := json.Marshal(order)
		if err != nil {
			log.Printf("Error marshalling order payload: %v", err)
			continue
		}

		session, err := client.StartSession()
		if err != nil {
			log.Printf("Error starting session: %v", err)
			continue
		}

		_, err = session.WithTransaction(context.Background(), func(sc mongo.SessionContext) (interface{}, error) {
			orderDocument := bson.M{
				"_id":         uuid.New().String(),
				"order_id":    order.OrderID,
				"customer_id": order.CustomerID,
				"amount":      order.Amount,
			}

			_, err := orders.InsertOne(sc, orderDocument)
			if err != nil {
				return nil, fmt.Errorf("error inserting order: %w", err)
			}

			message := models.Message{
				ID:        uuid.New().String(),
				EventType: "OrderCreated",
				Payload:   payload,
				Status:    "pending",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			}
			_, err = outbox.InsertOne(sc, message)
			if err != nil {
				return nil, fmt.Errorf("error inserting event at outbox: %w", err)
			}

			return nil, nil
		})

		if err != nil {
			log.Printf("Error during transaction: %v", err)
			continue
		}

		session.EndSession(ctx)
		log.Printf("Order created successfully with ID: %s", order.OrderID)
	}
}

func generateAmount(min, max float64) float64 {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)

	randomValue := random.Float64()
	return float64(int((min+randomValue*(max-min))*100)) / 100
}
