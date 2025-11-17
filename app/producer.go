package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ProduceMessages(config *kafka.ConfigMap, topic string, messages []string) {
	p, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Producer %s: Ошибка при создании продьюсера: %v\n", topic, err)
	}
	defer p.Close()

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)
	for _, message := range messages {
		bytes := []byte(message)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: bytes,
		}, deliveryChan)

		if err != nil {
			log.Printf("Producer %s: Ошибка при отправке сообщения: %v\n", topic, err)
			continue
		}

		event := <-deliveryChan
		msg := event.(*kafka.Message)

		if msg.TopicPartition.Error != nil {
			log.Printf("Producer %s: Ошибка доставки сообщения: %v\n", topic, msg.TopicPartition.Error)
		} else {
			log.Printf("Producer %s: Сообщение отправлено в %s:\n\t%+v\n",
				topic,
				msg.TopicPartition,
				message)
		}
	}
}
