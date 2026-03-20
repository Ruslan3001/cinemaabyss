package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

/**
 * Event представляет собой абстрактное событие в системе
 */
type Event struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

var kafkaBrokers []string

func main() {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		kafkaBrokers = []string{"localhost:9092"}
	} else {
		kafkaBrokers = strings.Split(brokersEnv, ",")
	}

	// Запускаем асинхронные consumers в отдельных горутинах
	go startConsumer("movie-events")
	go startConsumer("user-events")
	go startConsumer("payment-events")

	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", makeEventHandler("movie-events", "movie"))
	http.HandleFunc("/api/events/user", makeEventHandler("user-events", "user"))
	http.HandleFunc("/api/events/payment", makeEventHandler("payment-events", "payment"))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Starting events microservice on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

/**
 * healthHandler отдает статус сервиса
 */
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

/**
 * makeEventHandler фабрика для создания обработчиков под разные типы событий
 */
func makeEventHandler(topic string, eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		event := Event{
			Type:    eventType,
			Payload: payload,
		}

		err := produceEvent(topic, event)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to produce event: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	}
}

func produceEvent(topic string, event Event) error {
	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(event.Type),
			Value: eventBytes,
		},
	)
	if err != nil {
		log.Printf("[PRODUCER] Failed to write messages: %v", err)
		return err
	}
	log.Printf("[PRODUCER] Successfully produced event to topic %s: %s", topic, string(eventBytes))
	return nil
}

func startConsumer(topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   kafkaBrokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	log.Printf("[CONSUMER] Started consuming from topic: %s", topic)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[CONSUMER] Error reading message from %s: %v", topic, err)
			time.Sleep(time.Second * 5) // Backoff в случае ошибки соединения с брокером
			continue
		}
		log.Printf("[CONSUMER] Processed event from topic %s: %s\n", topic, string(m.Value))
	}
}