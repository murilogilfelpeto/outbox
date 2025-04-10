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
make start

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
```
make start           # Starts all containers and services
```
```
make start-services  # Starts only Docker containers
```
```
make stop-services   # Stops all Docker containers
```
```
make build-producer  # Builds the producer service
```
```
make build-consumer  # Builds the consumer service
```
```
make build-relay     # Builds the relay service
```
```
make run-producer    # Runs the producer service in background
```
```
make run-consumer    # Runs the consumer service in background
```
```
make run-relay       # Runs the relay service in background
```
```
make clean           # Removes all built binaries and logs
```
```
make logs            # Displays logs from all services```
```
```
make tail-logs      # Tails logs from all services```
```

### The services output logs to the `logs` directory:
- Producer logs: `logs/producer.log`
- Consumer logs: `logs/consumer.log`
- Relay logs: `logs/relay.log`