package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type FoodDelivered struct {
	OrderID     string    `json:"order_id"`
	CustomerID  string    `json:"customer_id"`
	Restaurant  string    `json:"restaurant"`
	DeliveredAt time.Time `json:"delivered_at"`
}

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		log.Fatal(err.Error())
	}

	defer producer.Close()

	orders := []FoodDelivered{
		{OrderID: "ORD-1001", CustomerID: "USER-1", Restaurant: "Burger House"},
		{OrderID: "ORD-1002", CustomerID: "USER-2", Restaurant: "Pizza Palace"},
		{OrderID: "ORD-1003", CustomerID: "USER-3", Restaurant: "Sushi World"},
		{OrderID: "ORD-1004", CustomerID: "USER-4", Restaurant: "Taco Town"},
		{OrderID: "ORD-1005", CustomerID: "USER-5", Restaurant: "Curry Corner"},
		{OrderID: "ORD-1006", CustomerID: "USER-6", Restaurant: "Noodle Nest"},
		{OrderID: "ORD-1007", CustomerID: "USER-7", Restaurant: "BBQ Barn"},
		{OrderID: "ORD-1008", CustomerID: "USER-8", Restaurant: "Vegan Vibes"},
		{OrderID: "ORD-1009", CustomerID: "USER-9", Restaurant: "Steak Station"},
		{OrderID: "ORD-1010", CustomerID: "USER-10", Restaurant: "Pasta Place"},
	}

	for i := range orders {
		orders[i].DeliveredAt = time.Now()

		payload, err := json.Marshal(orders[i])
		if err != nil {
			log.Fatal(err)
		}
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     strPtr("food-deliveries"),
				Partition: 0,
			},
			Key:   []byte(orders[i].OrderID),
			Value: payload,
		}, nil)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Wait for delivery reports
	for i := 0; i < len(orders); i++ {
		e := <-producer.Events()
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("delivery failed: %v", ev.TopicPartition.Error)
			} else {
				log.Printf(
					"message delivered to partition=%d offset=%d",
					ev.TopicPartition.Partition,
					ev.TopicPartition.Offset,
				)
			}
		}
	}

	producer.Flush(5000)

}
func strPtr(s string) *string {
	return &s
}
