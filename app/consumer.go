package main

import (
	"context"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ConsumeMessages(ctx context.Context, cfg *kafka.ConfigMap, topic string) {
	consumer, err := kafka.NewConsumer(cfg)

	if err != nil {
		log.Fatalf("Consumer %s: Невозможно создать консьюмера: %s\n", topic, err)
	}

	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Consumer %s: Ошибка при подписке консьюмера: %s\n", topic, err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := consumer.ReadMessage(200 * time.Millisecond)
			if err != nil {
				if !err.(kafka.Error).IsTimeout() {
					log.Printf("Consumer %s: Ошибки кафки при получения сообщения: %v\n", topic, err)
				}
			} else {
				message := string(msg.Value)
				log.Printf("Consumer %s: Получено сообщение '%s' из партиции %s:\n",
					topic,
					message,
					msg.TopicPartition)
			}
		}
	}
}
