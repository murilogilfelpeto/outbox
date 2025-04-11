FROM golang:1.24 AS builder

# Install required dependencies for building
RUN apt-get update && apt-get install -y librdkafka-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Copy application files
COPY consumer/ ./consumer/
COPY models/ ./models/

# Build with dynamic tag to use system librdkafka
RUN go build -tags dynamic -o /app/consumer ./consumer/consumer.go

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates librdkafka1 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/consumer .

CMD ["./consumer"]