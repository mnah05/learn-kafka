package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	p *kafka.Producer
}

func New(brokers string) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		return nil, err
	}

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				fmt.Printf("Producer error: %v\n", ev)
			}
		}
	}()

	return &Producer{p: p}, nil
}

// Send produces a message to the given topic and partition.
// Use partition=-1 for automatic partition assignment.
func (p *Producer) Send(topic string, partition int32, value []byte) error {
	return p.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
		Value:          value,
	}, nil)
}

func (p *Producer) Flush(timeoutMs int) int {
	return p.p.Flush(timeoutMs)
}

func (p *Producer) Close() {
	p.p.Close()
}
