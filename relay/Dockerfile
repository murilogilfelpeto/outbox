FROM golang:1.24 AS builder

# Install required dependencies for building
RUN apt-get update && apt-get install -y librdkafka-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Copy application files
COPY relay/ ./relay/
COPY models/ ./models/

# Build with dynamic tag to use system librdkafka
RUN go build -tags dynamic -o /app/relay ./relay/relay.go

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates librdkafka1 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/relay .

# Environment variables
ENV MONGO_URI=mongodb://mongodb:27017/?directConnection=true
ENV MONGO_HOST=mongodb
ENV MONGO_DATABASE=outbox_demo
ENV OUTBOX_COLLECTION=outbox
ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_TOPIC=orders

CMD ["./relay"]