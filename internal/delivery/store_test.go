package delivery

import (
	"context"
	"crypto/hmac"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestJournalRecoversAndDeduplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	store, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PutDestination(Destination{ID: "dest", URL: "http://example.test", Secret: "secret"}); err != nil {
		t.Fatal(err)
	}
	created, err := store.PutEvent(Event{ID: "evt-1", Source: "test", Type: "created", Data: []byte(`{"ok":true}`), DestinationID: "dest"})
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if created, _ = store.PutEvent(Event{ID: "evt-1", DestinationID: "dest"}); created {
		t.Fatal("duplicate was created")
	}
	reopened, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if event, ok := reopened.Event("evt-1"); !ok || event.Status != "pending" {
		t.Fatalf("recovered %#v %v", event, ok)
	}
}

func TestDispatcherSignsAndDelivers(t *testing.T) {
	received := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		parts := strings.Split(r.Header.Get("HookRail-Signature"), ",")
		timestamp := strings.TrimPrefix(parts[0], "t=")
		signature := strings.TrimPrefix(parts[1], "v1=")
		expected := Sign("secret", timestamp, body)
		received <- hmac.Equal(mustDecode(signature), mustDecode(expected))
		w.WriteHeader(204)
	}))
	defer server.Close()
	store, _ := OpenJournal(filepath.Join(t.TempDir(), "events.wal"))
	_ = store.PutDestination(Destination{ID: "dest", URL: server.URL, Secret: "secret", MaxAttempts: 2})
	_, _ = store.PutEvent(Event{ID: "evt", Source: "test", Type: "created", Data: []byte(`{}`), DestinationID: "dest"})
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher := Dispatcher{Store: store, Workers: 1, BaseBackoff: time.Millisecond}
	dispatcher.Run(ctx)
	select {
	case valid := <-received:
		if !valid {
			t.Fatal("invalid signature")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delivery timed out")
	}
	deadline := time.Now().Add(time.Second)
	event, _ := store.Event("evt")
	for event.Status != "delivered" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		event, _ = store.Event("evt")
	}
	cancel()
	dispatcher.Wait()
	if event.Status != "delivered" {
		t.Fatalf("status %s", event.Status)
	}
}

func mustDecode(value string) []byte { out, _ := hex.DecodeString(value); return out }
