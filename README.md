# Outbox Pattern Implementation in Go
This project demonstrates the Outbox pattern implementation using Go, MongoDB, and Kafka. It provides a reliable way to ensure consistency between database transactions and message publishing to Kafka.


## Architecture Overview
The project consists of three main services:


### Producer: 
Creates messages and stores them in the MongoDB outbox collection
### Relay:
Periodically polls the outbox collection for pending messages and publishes them to Kafka
### Consumer: 
Consumes messages from Kafka and processes them, tracking which messages have been processed

## Technologies Used
* Go
* MongoDB (with replica set)
* Apache Kafka
* Docker & Docker Compose

## Prerequisites
* Docker and Docker Compose
* Go 1.16+

## Setup and Installation
Clone the repository:
```git clone https://github.com/murilogilfelpeto/outbox.git```
```cd outbox```

## Start all services:
make all

## Project Structure
├── consumer/          # Consumer service <br />
├── producer/          # Producer service <br />
├── relay/             # Relay service <br />
├── models/            # Shared data models <br />
├── docker-compose.yml # Docker services configuration <br />
├── Makefile           # Build and run commands <br />
└── README.md          # Project documentation

## How It Works
### Producer Service:
Creates messages/orders and stores them in MongoDB's outbox collection with "pending" status

### Relay Service:
Polls MongoDB outbox collection every 10 seconds
Finds messages with "pending" status
Publishes them to Kafka
Updates message status to "processed" in MongoDB

### Consumer Service:
Subscribes to Kafka topic
Processes incoming messages
Tracks processed messages to prevent duplicate processing

## Available Commands
The Makefile provides several commands:
### Builds and starts all containers in detached mode (recommended)`
```
make all
```
### Builds and starts all containers in detached mode
```
make docker-build
```

### Starts all containers in detached mode
```
make docker-up
```

### Stops and removes all containers
```
make docker-down
```

### Local Development Commands
### Builds all components locally
```
make build
```
### Build producer
```
make build-producer
```

### Build relay
```
make build-relay
```

### Build consumer
```
make build-consumer
```

### Run producer
```
make run-producer
```

### Run relay
```
make run-relay
```

### Run consumer
```
make run-consumer
```

### Clean build artifacts
```
make clean
```

### Run tests
```
make test
```

### Tidy Go modules
```
make tidy
```

### Update dependencies
```
make deps
```