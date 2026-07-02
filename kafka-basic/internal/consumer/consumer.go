package consumer

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func WriteToTempFile(label string, data []byte) error {
	f, err := os.CreateTemp("", fmt.Sprintf("consumer-%s-*.bin", label))
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	log.Printf("[Consumer %s] wrote %d bytes to %s", label, len(data), f.Name())
	return nil
}

type Consumer struct {
	c *kafka.Consumer
}

func New(brokers, groupID, offsetReset string) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  offsetReset,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}
	return &Consumer{c: c}, nil
}

func (c *Consumer) Subscribe(topics []string) error {
	return c.c.SubscribeTopics(topics, nil)
}

func (c *Consumer) Start(ctx context.Context, handler func(msg *kafka.Message) error) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				ev := c.c.Poll(100)
				if ev == nil {
					continue
				}
				switch e := ev.(type) {
				case *kafka.Message:
					fmt.Printf("Message on %s [%d] at offset %s: %s\n",
						*e.TopicPartition.Topic,
						e.TopicPartition.Partition,
						e.TopicPartition.Offset,
						string(e.Value))

					if err := handler(e); err != nil {
						fmt.Printf("Handler failed (will retry): %s\n", err)
						continue
					}

					if _, err := c.c.CommitMessage(e); err != nil {
						fmt.Printf("Commit failed: %s\n", err)
					}
				case kafka.Error:
					fmt.Printf("Consumer error: %v\n", e)
				}
			}
		}
	}()
}

func (c *Consumer) Close() {
	c.c.Close()
}
