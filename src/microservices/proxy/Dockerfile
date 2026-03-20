# Stage 1: Build
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o proxy-service main.go

# Stage 2: Run
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/proxy-service .

EXPOSE 8000
CMD ["./proxy-service"]
