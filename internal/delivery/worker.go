package delivery

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Dispatcher struct {
	Store       *Journal
	Client      *http.Client
	Workers     int
	BaseBackoff time.Duration
	wg          sync.WaitGroup
}

func (d *Dispatcher) Run(ctx context.Context) {
	if d.Workers < 1 {
		d.Workers = 4
	}
	if d.Client == nil {
		d.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if d.BaseBackoff == 0 {
		d.BaseBackoff = time.Second
	}
	for i := 0; i < d.Workers; i++ {
		d.wg.Add(1)
		go d.worker(ctx)
	}
}
func (d *Dispatcher) Wait() { d.wg.Wait() }
func (d *Dispatcher) worker(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, event := range d.Store.Due(time.Now().UTC(), 1) {
				d.deliver(ctx, event)
			}
		}
	}
}

func (d *Dispatcher) deliver(ctx context.Context, event Event) {
	destination, ok := d.Store.Destination(event.DestinationID)
	if !ok {
		return
	}
	payload, _ := json.Marshal(map[string]any{"specversion": "1.0", "id": event.ID, "source": event.Source, "type": event.Type, "data": json.RawMessage(event.Data)})
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	signature := Sign(destination.Secret, timestamp, payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, destination.URL, bytes.NewReader(payload))
	if err == nil {
		req.Header.Set("Content-Type", "application/cloudevents+json")
		req.Header.Set("HookRail-Signature", "t="+timestamp+",v1="+signature)
		var resp *http.Response
		resp, err = d.Client.Do(req)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				event.Status = "delivered"
				event.Attempts++
				event.LastError = ""
				_ = d.Store.UpdateEvent(event)
				return
			}
			err = fmt.Errorf("destination returned %d", resp.StatusCode)
		}
	}
	event.Attempts++
	event.LastError = err.Error()
	if event.Attempts >= destination.MaxAttempts {
		event.Status = "dead_letter"
	} else {
		event.Status = "retrying"
		backoff := float64(d.BaseBackoff) * math.Pow(2, float64(event.Attempts-1))
		if backoff > float64(time.Hour) {
			backoff = float64(time.Hour)
		}
		event.NextAttempt = time.Now().UTC().Add(time.Duration(backoff))
	}
	_ = d.Store.UpdateEvent(event)
}
func Sign(secret, timestamp string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}
