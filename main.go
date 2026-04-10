package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TankMsg struct {
	Distance    float64 `json:"Distance"`
	Temperature float64 `json:"Temperature"`
	Humidity    float64 `json:"Humidity"`
}

type config struct {
	broker             string
	vmPushURL          string
	vmPushURLRedacted  string
	sensor             string
	mqttUser           string
	mqttPassword       string
	insecureSkipVerify bool
	queueDepth         int
	qos                int
	probeAddr          string
	otlpEndpoint       string
}

type monitorMetrics struct {
	messagesReceived prometheus.Counter
	messagesDropped  prometheus.Counter
	pushErrors       prometheus.Counter
	pushDuration     prometheus.Histogram
}

type monitor struct {
	cfg     config
	queue   chan TankMsg
	client  *http.Client
	started atomic.Bool
	ready   atomic.Bool
	metrics monitorMetrics
	tracer  trace.Tracer
}

func newMonitor(cfg config) *monitor {
	messagesReceived := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "tank_messages_received_total",
		Help:        "Total MQTT messages successfully parsed.",
		ConstLabels: prometheus.Labels{"sensor": cfg.sensor},
	})
	messagesDropped := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "tank_messages_dropped_total",
		Help:        "Total messages dropped due to a full queue.",
		ConstLabels: prometheus.Labels{"sensor": cfg.sensor},
	})
	pushErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "tank_push_errors_total",
		Help:        "Total failed VictoriaMetrics push attempts.",
		ConstLabels: prometheus.Labels{"sensor": cfg.sensor},
	})
	pushDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "tank_push_duration_seconds",
		Help:        "Latency of VictoriaMetrics push requests.",
		ConstLabels: prometheus.Labels{"sensor": cfg.sensor},
		Buckets:     prometheus.DefBuckets,
	})
	prometheus.MustRegister(messagesReceived, messagesDropped, pushErrors, pushDuration)

	return &monitor{
		cfg:    cfg,
		queue:  make(chan TankMsg, cfg.queueDepth),
		client: &http.Client{Timeout: 20 * time.Second},
		metrics: monitorMetrics{
			messagesReceived: messagesReceived,
			messagesDropped:  messagesDropped,
			pushErrors:       pushErrors,
			pushDuration:     pushDuration,
		},
		tracer: otel.Tracer("tank-monitor"),
	}
}

func initTracer(ctx context.Context, endpoint string) (*sdktrace.TracerProvider, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("gRPC dial %s: %w", endpoint, err)
	}
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("OTLP exporter: %w", err)
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("tank-monitor"),
			semconv.ServiceVersion("1.0.0"),
			attribute.String("sensor", ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("OTel resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}

func (m *monitor) onMessage(_ mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message on topic %s: %s", msg.Topic(), msg.Payload())
	var parsed TankMsg
	if err := json.Unmarshal(msg.Payload(), &parsed); err != nil {
		log.Printf("ERROR: failed to parse message payload: %s", err)
		return
	}
	m.metrics.messagesReceived.Inc()
	select {
	case m.queue <- parsed:
	default:
		m.metrics.messagesDropped.Inc()
		log.Printf("WARN: queue full, dropping message for sensor %s", m.cfg.sensor)
	}
}

func (m *monitor) run(ctx context.Context) {
	for {
		select {
		case msg := <-m.queue:
			if err := m.push(ctx, msg); err != nil {
				log.Printf("ERROR: failed to push metrics: %s", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *monitor) push(ctx context.Context, msg TankMsg) error {
	ctx, span := m.tracer.Start(ctx, "push",
		trace.WithAttributes(
			attribute.String("sensor", m.cfg.sensor),
			attribute.Float64("distance", msg.Distance),
			attribute.Float64("temperature", msg.Temperature),
			attribute.Float64("humidity", msg.Humidity),
		),
	)
	defer span.End()

	timer := prometheus.NewTimer(m.metrics.pushDuration)
	defer timer.ObserveDuration()

	text := fmt.Sprintf(
		"distance{sensor=%q} %g\ntemperature{sensor=%q} %g\nhumidity{sensor=%q} %g\n",
		m.cfg.sensor, msg.Distance,
		m.cfg.sensor, msg.Temperature,
		m.cfg.sensor, msg.Humidity,
	)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(text)); err != nil {
		return fmt.Errorf("gzip write: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("gzip close: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.cfg.vmPushURL, &buf)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := m.client.Do(req)
	if err != nil {
		m.metrics.pushErrors.Inc()
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return fmt.Errorf("POST to %s: %w", m.cfg.vmPushURLRedacted, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		err := fmt.Errorf("status %d from %s: %s", resp.StatusCode, m.cfg.vmPushURLRedacted, body)
		m.metrics.pushErrors.Inc()
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return err
	}

	span.SetStatus(otelcodes.Ok, "")
	return nil
}

func (m *monitor) buildMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !m.ready.Load() {
			http.Error(w, "MQTT not connected", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/startupz", func(w http.ResponseWriter, _ *http.Request) {
		if !m.started.Load() {
			http.Error(w, "not yet connected", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func (m *monitor) serveProbes(ctx context.Context) {
	srv := &http.Server{
		Addr:    m.cfg.probeAddr,
		Handler: m.buildMux(),
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("WARN: probe server shutdown: %s", err)
		}
	}()
	log.Printf("Probe/metrics server listening on %s", m.cfg.probeAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("ERROR: probe server: %s", err)
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	sensor, ok := os.LookupEnv("SENSOR")
	if !ok {
		log.Fatal("SENSOR environment variable is required")
	}

	broker := "tcp://mosquitto:1883"
	if v := os.Getenv("BROKER"); v != "" {
		broker = v
	}
	vmPushURL := "http://victoria-metrics-victoria-metrics-single-server:8428/api/v1/import/prometheus"
	if v := os.Getenv("VM_PUSH_URL"); v != "" {
		vmPushURL = v
	}
	mqttUser := "emqx"
	if v := os.Getenv("MQTT_USER"); v != "" {
		mqttUser = v
	}
	mqttPassword := "public"
	if v := os.Getenv("MQTT_PASSWORD"); v != "" {
		mqttPassword = v
	}
	insecureSkipVerify := os.Getenv("MQTT_TLS_INSECURE") == "true"

	probeAddrDefault := ":8080"
	if v := os.Getenv("PROBE_ADDR"); v != "" {
		probeAddrDefault = v
	}
	otlpEndpointDefault := "tempo.observability:4317"
	if v := os.Getenv("OTLP_ENDPOINT"); v != "" {
		otlpEndpointDefault = v
	}

	qos := flag.Int("qos", 0, "QoS level to subscribe at")
	queueDepth := flag.Int("queue-depth", 20, "MQTT message queue depth before dropping")
	probeAddr := flag.String("probe-addr", probeAddrDefault, "Address for Kubernetes probe/metrics HTTP server")
	otlpEndpoint := flag.String("otlp-endpoint", otlpEndpointDefault, "OTLP gRPC endpoint for traces")
	flag.Parse()

	vmPushURLRedacted := vmPushURL
	if pu, err := url.Parse(vmPushURL); err == nil {
		vmPushURLRedacted = pu.Redacted()
	}

	cfg := config{
		broker:             broker,
		vmPushURL:          vmPushURL,
		vmPushURLRedacted:  vmPushURLRedacted,
		sensor:             sensor,
		mqttUser:           mqttUser,
		mqttPassword:       mqttPassword,
		insecureSkipVerify: insecureSkipVerify,
		queueDepth:         *queueDepth,
		qos:                *qos,
		probeAddr:          *probeAddr,
		otlpEndpoint:       *otlpEndpoint,
	}

	log.Printf("Config: broker=%s vmPushURL=%s sensor=%s insecureSkipVerify=%v queueDepth=%d otlpEndpoint=%s",
		cfg.broker, cfg.vmPushURLRedacted, cfg.sensor, cfg.insecureSkipVerify, cfg.queueDepth, cfg.otlpEndpoint)

	var tp *sdktrace.TracerProvider
	if cfg.otlpEndpoint != "" {
		var err error
		tp, err = initTracer(ctx, cfg.otlpEndpoint)
		if err != nil {
			log.Fatalf("ERROR: failed to init tracer: %s", err)
		}
		defer tp.Shutdown(context.Background()) //nolint:errcheck
	} else {
		log.Println("OTLP endpoint not set, tracing disabled")
	}

	mon := newMonitor(cfg)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.broker)
	opts.SetClientID("monitor-" + cfg.sensor)
	opts.SetUsername(cfg.mqttUser)
	opts.SetPassword(cfg.mqttPassword)
	opts.SetCleanSession(true)
	opts.SetOrderMatters(false)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("WARN: MQTT connection lost: %s", err)
		mon.ready.Store(false)
	})
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.insecureSkipVerify, ClientAuth: tls.NoClientCert}
	opts.SetTLSConfig(tlsConfig)

	topic := "tele/" + cfg.sensor + "/SENSOR"
	opts.OnConnect = func(c mqtt.Client) {
		if token := c.Subscribe(topic, byte(cfg.qos), mon.onMessage); token.Wait() && token.Error() != nil {
			log.Printf("ERROR: failed to subscribe to %s: %s", topic, token.Error())
			return
		}
		mon.started.Store(true)
		mon.ready.Store(true)
		log.Printf("Subscribed to topic: %s", topic)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("ERROR: failed to connect to broker %s: %s", cfg.broker, token.Error())
	}
	log.Printf("Connected to %s", cfg.broker)

	go mon.run(ctx)
	go mon.serveProbes(ctx)

	<-ctx.Done()
	log.Println("Shutting down")
	client.Disconnect(250)
}

// Received message: {"Distance": 1000,"Temperature": 23.760967,"Humidity": 33.665981} from topic: tele/taylor_water_tank_level1/SENSOR

