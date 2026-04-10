package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace/noop"
)

// newTestMonitor creates a monitor with isolated Prometheus metrics for testing.
func newTestMonitor(t *testing.T, cfg config) *monitor {
	t.Helper()
	reg := prometheus.NewRegistry()

	messagesReceived := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tank_messages_received_total",
	})
	messagesDropped := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tank_messages_dropped_total",
	})
	pushErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tank_push_errors_total",
	})
	pushDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "tank_push_duration_seconds",
		Buckets: prometheus.DefBuckets,
	})
	reg.MustRegister(messagesReceived, messagesDropped, pushErrors, pushDuration)

	return &monitor{
		cfg:    cfg,
		queue:  make(chan TankMsg, cfg.queueDepth),
		client: &http.Client{Timeout: 5 * time.Second},
		metrics: monitorMetrics{
			messagesReceived: messagesReceived,
			messagesDropped:  messagesDropped,
			pushErrors:       pushErrors,
			pushDuration:     pushDuration,
		},
		tracer: noop.NewTracerProvider().Tracer("test"),
	}
}

// --- onMessage tests ---

type fakeMessage struct {
	topic   string
	payload []byte
}

func (f *fakeMessage) Duplicate() bool   { return false }
func (f *fakeMessage) Qos() byte         { return 0 }
func (f *fakeMessage) Retained() bool    { return false }
func (f *fakeMessage) Topic() string     { return f.topic }
func (f *fakeMessage) MessageID() uint16 { return 0 }
func (f *fakeMessage) Payload() []byte   { return f.payload }
func (f *fakeMessage) Ack()              {}

func TestOnMessage_ValidPayload(t *testing.T) {
	cfg := config{sensor: "test_sensor", queueDepth: 5}
	mon := newTestMonitor(t, cfg)

	msg := &fakeMessage{
		topic:   "tele/test_sensor/SENSOR",
		payload: []byte(`{"Distance": 100.5, "Temperature": 23.7, "Humidity": 45.2}`),
	}

	mon.onMessage(nil, msg)

	select {
	case got := <-mon.queue:
		if got.Distance != 100.5 {
			t.Errorf("Distance = %v, want 100.5", got.Distance)
		}
		if got.Temperature != 23.7 {
			t.Errorf("Temperature = %v, want 23.7", got.Temperature)
		}
		if got.Humidity != 45.2 {
			t.Errorf("Humidity = %v, want 45.2", got.Humidity)
		}
	default:
		t.Fatal("expected message in queue, got none")
	}
}

func TestOnMessage_InvalidJSON(t *testing.T) {
	cfg := config{sensor: "test_sensor", queueDepth: 5}
	mon := newTestMonitor(t, cfg)

	msg := &fakeMessage{
		topic:   "tele/test_sensor/SENSOR",
		payload: []byte(`not json`),
	}

	mon.onMessage(nil, msg)

	select {
	case <-mon.queue:
		t.Fatal("expected no message in queue for invalid JSON")
	default:
		// expected
	}
}

func TestOnMessage_QueueFull(t *testing.T) {
	cfg := config{sensor: "test_sensor", queueDepth: 1}
	mon := newTestMonitor(t, cfg)

	payload := []byte(`{"Distance": 1, "Temperature": 2, "Humidity": 3}`)

	// Fill the queue
	mon.onMessage(nil, &fakeMessage{topic: "t", payload: payload})
	// This should be dropped
	mon.onMessage(nil, &fakeMessage{topic: "t", payload: payload})

	if len(mon.queue) != 1 {
		t.Errorf("queue length = %d, want 1", len(mon.queue))
	}
}

// --- push tests ---

func TestPush_Success(t *testing.T) {
	var receivedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") != "gzip" {
			t.Error("expected gzip Content-Encoding")
		}
		if r.Header.Get("Content-Type") != "text/plain" {
			t.Error("expected text/plain Content-Type")
		}
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Fatalf("gzip reader: %v", err)
		}
		body, err := io.ReadAll(gz)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		receivedBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := config{
		sensor:            "test_sensor",
		vmPushURL:         srv.URL,
		vmPushURLRedacted: srv.URL,
		queueDepth:        5,
	}
	mon := newTestMonitor(t, cfg)
	mon.client = srv.Client()

	msg := TankMsg{Distance: 42.5, Temperature: 20.1, Humidity: 55.3}
	if err := mon.push(context.Background(), msg); err != nil {
		t.Fatalf("push() error: %v", err)
	}

	if !strings.Contains(receivedBody, `distance{sensor="test_sensor"} 42.5`) {
		t.Errorf("body missing distance metric: %s", receivedBody)
	}
	if !strings.Contains(receivedBody, `temperature{sensor="test_sensor"} 20.1`) {
		t.Errorf("body missing temperature metric: %s", receivedBody)
	}
	if !strings.Contains(receivedBody, `humidity{sensor="test_sensor"} 55.3`) {
		t.Errorf("body missing humidity metric: %s", receivedBody)
	}
}

func TestPush_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	cfg := config{
		sensor:            "test_sensor",
		vmPushURL:         srv.URL,
		vmPushURLRedacted: srv.URL,
		queueDepth:        5,
	}
	mon := newTestMonitor(t, cfg)
	mon.client = srv.Client()

	err := mon.push(context.Background(), TankMsg{})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention status code: %v", err)
	}
}

func TestPush_NetworkError(t *testing.T) {
	cfg := config{
		sensor:            "test_sensor",
		vmPushURL:         "http://127.0.0.1:1", // nothing listening
		vmPushURLRedacted: "http://127.0.0.1:1",
		queueDepth:        5,
	}
	mon := newTestMonitor(t, cfg)
	mon.client = &http.Client{Timeout: 100 * time.Millisecond}

	err := mon.push(context.Background(), TankMsg{})
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

// --- probe endpoint tests ---

func TestHealthzProbe(t *testing.T) {
	mon := newTestMonitor(t, config{sensor: "test", queueDepth: 1})
	mux := mon.buildMux()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("healthz status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestReadyzProbe_NotReady(t *testing.T) {
	mon := newTestMonitor(t, config{sensor: "test", queueDepth: 1})
	mux := mon.buildMux()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz (not ready) status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestReadyzProbe_Ready(t *testing.T) {
	mon := newTestMonitor(t, config{sensor: "test", queueDepth: 1})
	mon.ready.Store(true)
	mux := mon.buildMux()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("readyz (ready) status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestStartupzProbe_NotStarted(t *testing.T) {
	mon := newTestMonitor(t, config{sensor: "test", queueDepth: 1})
	mux := mon.buildMux()

	req := httptest.NewRequest(http.MethodGet, "/startupz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("startupz (not started) status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestStartupzProbe_Started(t *testing.T) {
	mon := newTestMonitor(t, config{sensor: "test", queueDepth: 1})
	mon.started.Store(true)
	mux := mon.buildMux()

	req := httptest.NewRequest(http.MethodGet, "/startupz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("startupz (started) status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// --- run loop test ---

func TestRun_ProcessesQueue(t *testing.T) {
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := config{
		sensor:            "test_sensor",
		vmPushURL:         srv.URL,
		vmPushURLRedacted: srv.URL,
		queueDepth:        5,
	}
	mon := newTestMonitor(t, cfg)
	mon.client = srv.Client()

	ctx, cancel := context.WithCancel(context.Background())
	go mon.run(ctx)

	mon.queue <- TankMsg{Distance: 1, Temperature: 2, Humidity: 3}
	time.Sleep(100 * time.Millisecond)
	cancel()

	if !called {
		t.Error("expected push to be called from run loop")
	}
}

// --- TankMsg JSON parsing ---

func TestTankMsg_JSONParsing(t *testing.T) {
	raw := `{"Distance": 1000, "Temperature": 23.760967, "Humidity": 33.665981}`
	var msg TankMsg
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if msg.Distance != 1000 {
		t.Errorf("Distance = %v, want 1000", msg.Distance)
	}
	if msg.Temperature != 23.760967 {
		t.Errorf("Temperature = %v, want 23.760967", msg.Temperature)
	}
	if msg.Humidity != 33.665981 {
		t.Errorf("Humidity = %v, want 33.665981", msg.Humidity)
	}
}

func TestTankMsg_JSONExtraFields(t *testing.T) {
	raw := `{"Distance": 500, "Temperature": 22, "Humidity": 40, "Extra": "ignored"}`
	var msg TankMsg
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if msg.Distance != 500 {
		t.Errorf("Distance = %v, want 500", msg.Distance)
	}
}
