package admin

import (
	"context"
	"fmt"
	"log"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func EnsureTopic(brokers, topic string, partitions int) error {
	admin, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results, err := admin.CreateTopics(ctx, []ckafka.TopicSpecification{{
		Topic:         topic,
		NumPartitions: partitions,
		ReplicationFactor: 1,
	}}, ckafka.SetAdminOperationTimeout(10000))
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	for _, r := range results {
		if r.Error.Code() == ckafka.ErrTopicAlreadyExists {
			log.Printf("Topic %s already exists", topic)
		} else if r.Error.Code() != 0 {
			return fmt.Errorf("topic creation error for %s: %s", r.Topic, r.Error.String())
		} else {
			log.Printf("Topic %s created with %d partitions", topic, partitions)
		}
	}

	return nil
}
