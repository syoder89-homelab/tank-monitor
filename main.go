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
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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
}

type monitor struct {
	cfg    config
	queue  chan TankMsg
	client *http.Client
}

func newMonitor(cfg config) *monitor {
	return &monitor{
		cfg:    cfg,
		queue:  make(chan TankMsg, cfg.queueDepth),
		client: &http.Client{Timeout: 20 * time.Second},
	}
}

func (m *monitor) onMessage(_ mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message on topic %s: %s", msg.Topic(), msg.Payload())
	var parsed TankMsg
	if err := json.Unmarshal(msg.Payload(), &parsed); err != nil {
		log.Printf("ERROR: failed to parse message payload: %s", err)
		return
	}
	select {
	case m.queue <- parsed:
	default:
		log.Printf("WARN: queue full, dropping message for sensor %s", m.cfg.sensor)
	}
}

func (m *monitor) run(ctx context.Context) {
	for {
		select {
		case msg := <-m.queue:
			if err := m.push(msg); err != nil {
				log.Printf("ERROR: failed to push metrics: %s", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *monitor) push(msg TankMsg) error {
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

	req, err := http.NewRequest(http.MethodPost, m.cfg.vmPushURL, &buf)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST to %s: %w", m.cfg.vmPushURLRedacted, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d from %s: %s", resp.StatusCode, m.cfg.vmPushURLRedacted, body)
	}
	return nil
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

	qos := flag.Int("qos", 0, "QoS level to subscribe at")
	queueDepth := flag.Int("queue-depth", 20, "MQTT message queue depth before dropping")
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
	}

	log.Printf("Config: broker=%s vmPushURL=%s sensor=%s insecureSkipVerify=%v queueDepth=%d",
		cfg.broker, cfg.vmPushURLRedacted, cfg.sensor, cfg.insecureSkipVerify, cfg.queueDepth)

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
	})
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.insecureSkipVerify, ClientAuth: tls.NoClientCert}
	opts.SetTLSConfig(tlsConfig)

	topic := "tele/" + cfg.sensor + "/SENSOR"
	opts.OnConnect = func(c mqtt.Client) {
		if token := c.Subscribe(topic, byte(cfg.qos), mon.onMessage); token.Wait() && token.Error() != nil {
			log.Printf("ERROR: failed to subscribe to %s: %s", topic, token.Error())
			return
		}
		log.Printf("Subscribed to topic: %s", topic)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("ERROR: failed to connect to broker %s: %s", cfg.broker, token.Error())
	}
	log.Printf("Connected to %s", cfg.broker)

	go mon.run(ctx)

	<-ctx.Done()
	log.Println("Shutting down")
	client.Disconnect(250)
}

// Received message: {"Distance": 1000,"Temperature": 23.760967,"Humidity": 33.665981} from topic: tele/taylor_water_tank_level1/SENSOR

