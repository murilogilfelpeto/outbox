# Project variables
PROJECT_NAME := outbox
BUILD_DIR := bin

# Go parameters
GO := go
GO_CLEAN := $(GO) clean
GO_BUILD := $(GO) build
GO_TEST := $(GO) test
GO_MOD := $(GO) mod
GO_GET := $(GO) get

# Binary names
PRODUCER_BIN := producer
CONSUMER_BIN := consumer
RELAY_BIN := relay

# Source directories
PRODUCER_SRC := ./producer
CONSUMER_SRC := ./consumer
RELAY_SRC := ./relay

# Environment variables
export MONGO_URI ?= mongodb://localhost:27017/?directConnection=true
export MONGO_HOST ?= 127.0.0.1
export MONGO_DATABASE ?= outbox_demo
export OUTBOX_COLLECTION ?= outbox
export KAFKA_BROKERS ?= localhost:9092
export KAFKA_TOPIC ?= orders
export KAFKA_GROUP_ID ?= order-consumer-group

# Build targets
.PHONY: all build clean test tidy run-producer run-consumer run-relay deps

all: docker-build docker-up

build: build-producer build-consumer build-relay

build-producer:
	@echo "Building producer..."
	@mkdir -p $(BUILD_DIR)
	$(GO_BUILD) -o $(BUILD_DIR)/$(PRODUCER_BIN) $(PRODUCER_SRC)

build-consumer:
	@echo "Building consumer..."
	@mkdir -p $(BUILD_DIR)
	$(GO_BUILD) -o $(BUILD_DIR)/$(CONSUMER_BIN) $(CONSUMER_SRC)

build-relay:
	@echo "Building relay..."
	@mkdir -p $(BUILD_DIR)
	$(GO_BUILD) -o $(BUILD_DIR)/$(RELAY_BIN) $(RELAY_SRC)

clean:
	@echo "Cleaning up..."
	@rm -rf $(BUILD_DIR)
	$(GO_CLEAN)

test:
	$(GO_TEST) -v ./...

tidy:
	$(GO_MOD) tidy

deps:
	$(GO_GET) -u ./...

# Run targets
run-producer: build-producer
	$(BUILD_DIR)/$(PRODUCER_BIN)

run-consumer: build-consumer
	$(BUILD_DIR)/$(CONSUMER_BIN)

run-relay: build-relay
	$(BUILD_DIR)/$(RELAY_BIN)

# Docker targets (if needed)
.PHONY: docker-build docker-up docker-down

docker-build:
	docker-compose build

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down