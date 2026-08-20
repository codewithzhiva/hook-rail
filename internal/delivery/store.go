package delivery

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Destination struct {
	ID          string `json:"id"`
	URL         string `json:"url"`
	Secret      string `json:"secret,omitempty"`
	MaxAttempts int    `json:"max_attempts"`
}

type Event struct {
	ID            string          `json:"id"`
	Source        string          `json:"source"`
	Type          string          `json:"type"`
	Data          json.RawMessage `json:"data"`
	DestinationID string          `json:"destination_id"`
	Status        string          `json:"status"`
	Attempts      int             `json:"attempts"`
	NextAttempt   time.Time       `json:"next_attempt"`
	LastError     string          `json:"last_error,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

type record struct {
	Kind        string       `json:"kind"`
	Destination *Destination `json:"destination,omitempty"`
	Event       *Event       `json:"event,omitempty"`
}

// Journal is an fsync'd append-only reference store. The Postgres adapter can
// implement the same operations without changing delivery semantics.
type Journal struct {
	mu           sync.Mutex
	path         string
	destinations map[string]Destination
	events       map[string]Event
}

func OpenJournal(path string) (*Journal, error) {
	j := &Journal{path: path, destinations: map[string]Destination{}, events: map[string]Event{}}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return j, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 8<<20)
	for scanner.Scan() {
		var rec record
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			return nil, fmt.Errorf("corrupt journal: %w", err)
		}
		j.apply(rec)
	}
	return j, scanner.Err()
}

func (j *Journal) PutDestination(destination Destination) error {
	if destination.ID == "" || destination.URL == "" || destination.Secret == "" {
		return errors.New("id, url and secret are required")
	}
	if destination.MaxAttempts == 0 {
		destination.MaxAttempts = 8
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	rec := record{Kind: "destination", Destination: &destination}
	if err := j.append(rec); err != nil {
		return err
	}
	j.apply(rec)
	return nil
}

func (j *Journal) PutEvent(event Event) (bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, exists := j.events[event.ID]; exists {
		return false, nil
	}
	if _, exists := j.destinations[event.DestinationID]; !exists {
		return false, errors.New("destination not found")
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	event.Status = "pending"
	event.NextAttempt = time.Now().UTC()
	rec := record{Kind: "event", Event: &event}
	if err := j.append(rec); err != nil {
		return false, err
	}
	j.apply(rec)
	return true, nil
}

func (j *Journal) UpdateEvent(event Event) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	rec := record{Kind: "event", Event: &event}
	if err := j.append(rec); err != nil {
		return err
	}
	j.apply(rec)
	return nil
}
func (j *Journal) Destination(id string) (Destination, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	v, ok := j.destinations[id]
	return v, ok
}
func (j *Journal) Event(id string) (Event, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	v, ok := j.events[id]
	return v, ok
}
func (j *Journal) ListEvents() []Event {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]Event, 0, len(j.events))
	for _, v := range j.events {
		out = append(out, v)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].CreatedAt.After(out[b].CreatedAt) })
	return out
}
func (j *Journal) Due(now time.Time, limit int) []Event {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := []Event{}
	for _, v := range j.events {
		if (v.Status == "pending" || v.Status == "retrying") && !v.NextAttempt.After(now) {
			out = append(out, v)
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}

func (j *Journal) append(rec record) error {
	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err = file.Write(append(data, '\n')); err != nil {
		return err
	}
	return file.Sync()
}
func (j *Journal) apply(rec record) {
	if rec.Destination != nil {
		j.destinations[rec.Destination.ID] = *rec.Destination
	}
	if rec.Event != nil {
		j.events[rec.Event.ID] = *rec.Event
	}
}
