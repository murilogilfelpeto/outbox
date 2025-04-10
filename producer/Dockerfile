FROM golang:1.19-alpine AS builder

WORKDIR /app
# Copy go.mod and go.sum from project root
COPY ../go.mod ../go.sum ./
RUN go mod download

# Copy producer code and any needed packages
COPY . ./producer/
COPY ../models/ ./models/

# Build the producer
RUN go build -o /app/producer ./producer/producer.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/producer .

# Set environment variables with default values
ENV MONGO_URI=mongodb://mongodb:27017/?directConnection=true
ENV MONGO_HOST=mongodb
ENV MONGO_DATABASE=outbox_demo
ENV OUTBOX_COLLECTION=outbox
ENV ORDER_COLLECTION=orders

CMD ["./producer"]