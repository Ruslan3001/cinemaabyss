package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Config хранит настройки прокси-сервиса
type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

// loadConfig считывает настройки из переменных окружения
func loadConfig() Config {
	percent, _ := strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "50"))

	return Config{
		Port:                   getEnv("PORT", "8000"),
		MonolithURL:            getEnv("MONOLITH_URL", "http://monolith:8080"),
		MoviesServiceURL:       getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"),
		EventsServiceURL:       getEnv("EVENTS_SERVICE_URL", "http://events-service:8082"),
		GradualMigration:       getEnv("GRADUAL_MIGRATION", "true") == "true",
		MoviesMigrationPercent: percent,
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// createProxy создает ReverseProxy для целевого URL
func createProxy(targetURL string) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(targetURL)
	if err != nil {
		return nil, err
	}
	return httputil.NewSingleHostReverseProxy(target), nil
}

func main() {
	cfg := loadConfig()

	monolithProxy, err := createProxy(cfg.MonolithURL)
	if err != nil {
		log.Fatalf("Invalid Monolith URL: %v", err)
	}

	moviesProxy, err := createProxy(cfg.MoviesServiceURL)
	if err != nil {
		log.Fatalf("Invalid Movies Service URL: %v", err)
	}

	eventsProxy, err := createProxy(cfg.EventsServiceURL)
	if err != nil {
		log.Fatalf("Invalid Events Service URL: %v", err)
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Strangler Fig Proxy is healthy"))
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// 1. Роутинг для микросервиса Events
		if strings.HasPrefix(path, "/api/events") {
			log.Printf("[PROXY] Events -> %s", cfg.EventsServiceURL)
			eventsProxy.ServeHTTP(w, r)
			return
		}

		// 2. Роутинг для микросервиса Movies (Strangler Fig)
		if strings.HasPrefix(path, "/api/movies") {
			// Если GradualMigration выключен, отдаем 100% трафика в микросервис
			if !cfg.GradualMigration || rand.Intn(100) < cfg.MoviesMigrationPercent {
				log.Printf("[PROXY] Movies (Microservice) -> %s", cfg.MoviesServiceURL)
				moviesProxy.ServeHTTP(w, r)
			} else {
				log.Printf("[PROXY] Movies (Monolith) -> %s", cfg.MonolithURL)
				monolithProxy.ServeHTTP(w, r)
			}
			return
		}

		// 3. Fallback: Все остальные запросы летят в Монолит
		monolithProxy.ServeHTTP(w, r)
	})

	log.Printf("Starting API Gateway on port %s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, nil))
}
