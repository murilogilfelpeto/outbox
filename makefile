# Variables
GO_CMD=go
MONGO_URI=mongodb://localhost:27017/?directConnection=true
MONGO_DATABASE=outbox_demo
KAFKA_BROKER=localhost:9092
DOCKER_CMD=docker
DOCKER_COMPOSE_CMD=docker-compose
BIN_DIR=bin
LOG_DIR=logs

# Ensure directories exist
$(shell mkdir -p $(BIN_DIR))
$(shell mkdir -p $(LOG_DIR))

# Targets
.PHONY: start-services stop-services build-producer build-consumer build-relay run-producer run-consumer run-relay clean start all

start-services:
	@echo "Starting all services..."
	$(DOCKER_COMPOSE_CMD) up -d
	@echo "Waiting for services to be ready..."
	sleep 10

stop-services:
	@echo "Stopping all services..."
	$(DOCKER_COMPOSE_CMD) down

build-producer:
	@echo "Building producer..."
	$(GO_CMD) build -o $(BIN_DIR)/producer producer/producer.go

build-consumer:
	@echo "Building consumer..."
	$(GO_CMD) build -o $(BIN_DIR)/consumer consumer/consumer.go

build-relay:
	@echo "Building relay..."
	$(GO_CMD) build -o $(BIN_DIR)/relay relay/relay.go

run-producer:
	@echo "Running producer in background..."
	./$(BIN_DIR)/producer > $(LOG_DIR)/producer.log 2>&1 &
	@echo "Producer started (logs in $(LOG_DIR)/producer.log)"

run-consumer:
	@echo "Running consumer in background..."
	./$(BIN_DIR)/consumer > $(LOG_DIR)/consumer.log 2>&1 &
	@echo "Consumer started (logs in $(LOG_DIR)/consumer.log)"

run-relay:
	@echo "Running relay in background..."
	./$(BIN_DIR)/relay > $(LOG_DIR)/relay.log 2>&1 &
	@echo "Relay started (logs in $(LOG_DIR)/relay.log)"

build: build-producer build-consumer build-relay
	@echo "All services built successfully"

clean:
	@echo "Cleaning up binaries..."
	rm -f $(BIN_DIR)/producer $(BIN_DIR)/consumer $(BIN_DIR)/relay
	@echo "Cleaning up logs..."
	rm -f $(LOG_DIR)/*.log

start: start-services build run-producer run-consumer run-relay
	@echo "All services started successfully"

all: clean start
	@echo "Project setup completed"

tail-logs:
	@echo "Tailing all logs..."
	tail -f $(LOG_DIR)/*.log