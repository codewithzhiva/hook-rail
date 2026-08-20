package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codewithzhiva/hook-rail/internal/delivery"
)

var requests atomic.Uint64

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	store, err := delivery.OpenJournal(env("JOURNAL_PATH", "data/hookrail.wal"))
	if err != nil {
		log.Error("open journal", "error", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher := &delivery.Dispatcher{Store: store, Workers: envInt("WORKERS", 8)}
	dispatcher.Run(ctx)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { respond(w, 200, map[string]string{"status": "ready"}) })
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "hookrail_http_requests_total %d\nhookrail_events_total %d\n", requests.Load(), len(store.ListEvents()))
	})
	mux.HandleFunc("/v1/destinations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		var destination delivery.Destination
		if err := decode(r, &destination); err != nil {
			respond(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if destination.ID == "" {
			destination.ID = "dst-" + randomID()
		}
		if err := store.PutDestination(destination); err != nil {
			respond(w, 422, map[string]string{"error": err.Error()})
			return
		}
		destination.Secret = ""
		respond(w, 201, destination)
	})
	mux.HandleFunc("/v1/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			respond(w, 200, store.ListEvents())
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		var input struct {
			ID            string          `json:"id"`
			Source        string          `json:"source"`
			Type          string          `json:"type"`
			Data          json.RawMessage `json:"data"`
			DestinationID string          `json:"destination_id"`
		}
		if err := decode(r, &input); err != nil {
			respond(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if input.ID == "" || input.Source == "" || input.Type == "" {
			respond(w, 422, map[string]string{"error": "CloudEvent id, source and type are required"})
			return
		}
		created, err := store.PutEvent(delivery.Event{ID: input.ID, Source: input.Source, Type: input.Type, Data: input.Data, DestinationID: input.DestinationID})
		if err != nil {
			respond(w, 422, map[string]string{"error": err.Error()})
			return
		}
		status := 201
		if !created {
			status = 200
		}
		event, _ := store.Event(input.ID)
		respond(w, status, event)
	})
	mux.HandleFunc("/v1/events/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 4 || parts[3] != "replay" || r.Method != http.MethodPost {
			http.Error(w, "not found", 404)
			return
		}
		event, ok := store.Event(parts[2])
		if !ok {
			http.Error(w, "event not found", 404)
			return
		}
		if event.Status != "dead_letter" && event.Status != "delivered" {
			respond(w, 409, map[string]string{"error": "only terminal events may be replayed"})
			return
		}
		event.Status = "pending"
		event.Attempts = 0
		event.LastError = ""
		event.NextAttempt = time.Now().UTC()
		_ = store.UpdateEvent(event)
		respond(w, 202, event)
	})
	server := &http.Server{Addr: env("LISTEN_ADDR", ":8080"), Handler: count(mux), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	_ = server.Shutdown(shutdown)
	dispatcher.Wait()
}

func decode(r *http.Request, value any) error {
	defer r.Body.Close()
	return json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(value)
}
func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func count(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); next.ServeHTTP(w, r) })
}
func randomID() string {
	var value [8]byte
	_, _ = rand.Read(value[:])
	return hex.EncodeToString(value[:])
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func envInt(key string, fallback int) int {
	var value int
	if _, err := fmt.Sscan(os.Getenv(key), &value); err == nil && value > 0 {
		return value
	}
	return fallback
}
