// Command example is a tiny CLI that connects a client to a broker and
// round-trips a message, demonstrating the embedding API.
//
// Usage:
//
//	go run ./cmd/example -broker localhost:1883 -client demo -qos 0
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/karthik20066002/MQTTaton/src/client"
)

func main() {
	var (
		broker   = flag.String("broker", "localhost:1883", "broker host:port")
		clientID = flag.String("client", "demo", "client identifier")
		username = flag.String("user", "", "username")
		password = flag.String("pass", "", "password")
		topic    = flag.String("topic", "demo/topic", "topic to publish/subscribe")
		qos      = flag.Int("qos", 0, "quality of service (0,1,2)")
		keepalive = flag.Duration("keepalive", 60*time.Second, "keepalive interval")
	)
	flag.Parse()

	c := client.New(client.Options{
		Broker:      *broker,
		ClientID:    *clientID,
		Username:    *username,
		Password:    *password,
		CleanSession: true,
		KeepAlive:   *keepalive,
	})
	if err := c.Connect(); err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Disconnect()

	if err := c.Subscribe(*topic, byte(*qos), func(c *client.Client, m client.Message) {
		log.Printf("received [%s] %q", m.Topic, m.Payload)
	}); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	if err := c.Publish(*topic, byte(*qos), false, []byte("hello from MQTTaton")); err != nil {
		log.Fatalf("publish: %v", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
}
