package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/syoder89-homelab/tank-monitor/vmclient"
)

type TankMsg struct {
	Distance    float64 `json:"Distance"`
	Temperature float64 `json:"Temperature"`
	Humidity    float64 `json:"Humidity"`
}

var tmsg TankMsg
var sensor string
var broker = "tcp://mosquitto:1883"
var vmPushURL = "http://victoria-metrics-victoria-metrics-single-server:8428/api/v1/import/prometheus"

func onMessageReceived(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
	if err := json.Unmarshal(msg.Payload(), &tmsg); err != nil {
		log.Printf("ERROR: failed to parse message payload: %s", err)
		return
	}
	fmt.Println(tmsg)
	if err := vmclient.Push(vmPushURL, 20*time.Second, `sensor="`+sensor+`"`, false); err != nil {
		log.Printf("ERROR: failed to push metrics: %s", err)
	}
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	if val, ok := os.LookupEnv("SENSOR"); ok {
		sensor = val
	} else {
		log.Fatal("SENSOR environment variable is required")
	}

	if val, ok := os.LookupEnv("BROKER"); ok {
		broker = val
	}
	if val, ok := os.LookupEnv("VM_PUSH_URL"); ok {
		vmPushURL = val
	}

	mqttUser := "emqx"
	if val, ok := os.LookupEnv("MQTT_USER"); ok {
		mqttUser = val
	}
	mqttPassword := "public"
	if val, ok := os.LookupEnv("MQTT_PASSWORD"); ok {
		mqttPassword = val
	}
	insecureSkipVerify := os.Getenv("MQTT_TLS_INSECURE") == "true"

	qos := flag.Int("qos", 0, "The QoS to subscribe to messages at")
	flag.Parse()

	log.Printf("Config: broker=%s vmPushURL=%s sensor=%s insecureSkipVerify=%v", broker, vmPushURL, sensor, insecureSkipVerify)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID("monitor-" + sensor)
	opts.SetUsername(mqttUser)
	opts.SetPassword(mqttPassword)
	opts.SetCleanSession(true)
	opts.SetOrderMatters(false)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("WARN: MQTT connection lost: %s", err)
	})
	tlsConfig := &tls.Config{InsecureSkipVerify: insecureSkipVerify, ClientAuth: tls.NoClientCert}
	opts.SetTLSConfig(tlsConfig)

	metrics.NewGauge(`distance`, func() float64 { return tmsg.Distance })
	metrics.NewGauge(`temperature`, func() float64 { return tmsg.Temperature })
	metrics.NewGauge(`humidity`, func() float64 { return tmsg.Humidity })

	topic := "tele/" + sensor + "/SENSOR"
	opts.OnConnect = func(c mqtt.Client) {
		if token := c.Subscribe(topic, byte(*qos), onMessageReceived); token.Wait() && token.Error() != nil {
			log.Printf("ERROR: failed to subscribe to topic %s: %s", topic, token.Error())
			return
		}
		fmt.Printf("Subscribed to topic: %s\n", topic)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("ERROR: failed to connect to broker %s: %s", broker, token.Error())
	}
	fmt.Printf("Connected to %s\n", broker)

	<-c
}

// Received message: {"Distance": 1000,"Temperature": 23.760967,"Humidity": 33.665981} from topic: tele/taylor_water_tank_level1/SENSOR

