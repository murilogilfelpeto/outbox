FROM golang:1.24 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Copy application files
COPY producer/ ./producer/
COPY models/ ./models/

# Build the application
RUN go build -o /app/producer ./producer/producer.go

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/producer .

# Environment variables
ENV MONGO_URI=mongodb://mongodb:27017/?directConnection=true
ENV MONGO_HOST=mongodb
ENV MONGO_DATABASE=outbox_demo
ENV OUTBOX_COLLECTION=outbox
ENV ORDER_COLLECTION=orders

CMD ["./producer"]