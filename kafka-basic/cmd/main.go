package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"kafka-learn/kafka-basic/internal/admin"
	"kafka-learn/kafka-basic/internal/api"
	"kafka-learn/kafka-basic/internal/consumer"
	"kafka-learn/kafka-basic/internal/producer"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/labstack/echo/v4"
)

func main() {
	brokers := "localhost:9092"
	topic := "myOrder"

	if err := admin.EnsureTopic(brokers, topic, 2); err != nil {
		log.Fatalf("Failed to ensure topic: %s", err)
	}

	p, err := producer.New(brokers)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Flush(10000)
	defer p.Close()

	c1, err := consumer.New(brokers, "order-processors", "earliest")
	if err != nil {
		log.Fatalf("Failed to create consumer 1: %s", err)
	}
	defer c1.Close()

	c2, err := consumer.New(brokers, "order-processors", "earliest")
	if err != nil {
		log.Fatalf("Failed to create consumer 2: %s", err)
	}
	defer c2.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c1.Subscribe([]string{topic}); err != nil {
		log.Fatalf("Failed to subscribe consumer 1: %s", err)
	}
	c1.Start(ctx, func(msg *kafka.Message) error {
		fmt.Printf("[Consumer 1] Processing: %s\n", string(msg.Value))
		return consumer.WriteToTempFile("1", msg.Value)
	})

	if err := c2.Subscribe([]string{topic}); err != nil {
		log.Fatalf("Failed to subscribe consumer 2: %s", err)
	}
	c2.Start(ctx, func(msg *kafka.Message) error {
		fmt.Printf("[Consumer 2] Processing: %s\n", string(msg.Value))
		return consumer.WriteToTempFile("2", msg.Value)
	})

	e := echo.New()
	api.New(p, topic).Register(e)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Println("Server listening on :8080")
		if err := e.Start(":8080"); err != nil {
			log.Fatalf("Server error: %s", err)
		}
	}()

	<-sig
	log.Println("Shutting down...")
	cancel()
}
