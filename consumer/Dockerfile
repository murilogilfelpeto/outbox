FROM golang:1.19-alpine AS builder

WORKDIR /app
# Copy go.mod and go.sum from project root
COPY ../go.mod ../go.sum ./
RUN go mod download

# Copy consumer code and shared packages
COPY . ./consumer/
COPY ../models/ ./models/

# Build the consumer
RUN go build -o /app/consumer ./consumer/consumer.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/consumer .

# Set environment variables with default values
ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_TOPIC=orders
ENV KAFKA_GROUP_ID=order-consumer-group

CMD ["./consumer"]