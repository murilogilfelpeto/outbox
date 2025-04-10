FROM golang:1.19-alpine AS builder

WORKDIR /app
# Copy go.mod and go.sum from project root
COPY ../go.mod ../go.sum ./
RUN go mod download

# Copy relay code and shared packages
COPY . ./relay/
COPY ../models/ ./models/

# Build the relay
RUN go build -o /app/relay ./relay/relay.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/relay .

# Set environment variables with default values
ENV MONGO_URI=mongodb://mongodb:27017/?directConnection=true
ENV MONGO_HOST=mongodb
ENV MONGO_DATABASE=outbox_demo
ENV OUTBOX_COLLECTION=outbox
ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_TOPIC=orders

CMD ["./relay"]