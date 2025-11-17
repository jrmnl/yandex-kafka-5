package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type AppSettings struct {
	BootstrapServers       string
	SslCaLocation          string
	SslCertificateLocation string
	SslKeyLocation         string
	Topic1                 string
	Topic2                 string
	ProducerUsername       string
	ProducerPassword       string
	ConsumerUsername       string
	ConsumerPassword       string
	ConsumerGroup          string
}

func getAppSettings() AppSettings {
	return AppSettings{
		BootstrapServers:       getEnv("BOOTSTRAP_SERVERS", "kafka-1:19093,kafka-2:19093,kafka-3:19093"),
		SslCaLocation:          getEnv("SSL_CA_LOCATION", "./ca.crt"),
		SslCertificateLocation: getEnv("SSL_CERTIFICATE_LOCATION", "./kafka-1-creds/kafka-1.crt"),
		SslKeyLocation:         getEnv("SSL_KEY_LOCATION", "./kafka-1-creds/kafka-1.key"),
		Topic1:                 getEnv("TOPIC_1", "topic1"),
		Topic2:                 getEnv("TOPIC_2", "topic2"),
		ProducerUsername:       getEnv("PRODUCER_USERNAME", "producer"),
		ProducerPassword:       getEnv("PRODUCER_PASSWORD", "producer-secret"),
		ConsumerUsername:       getEnv("CONSUMER_USERNAME", "consumer"),
		ConsumerPassword:       getEnv("CONSUMER_PASSWORD", "consumer-secret"),
		ConsumerGroup:          getEnv("CONSUMER_GROUP", "app_group"),
	}
}

func main() {
	settings := getAppSettings()

	log.Printf("конфиги: \n%v\n", settings)

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	adminConfig := &kafka.ConfigMap{
		"bootstrap.servers":        settings.BootstrapServers,
		"ssl.ca.location":          settings.SslCaLocation,
		"ssl.certificate.location": settings.SslCertificateLocation,
		"ssl.key.location":         settings.SslKeyLocation,
		"security.protocol":        "SASL_SSL",
		"sasl.mechanism":           "PLAIN",
		"sasl.username":            settings.ProducerUsername,
		"sasl.password":            settings.ProducerPassword,
	}

	WaitTopic(ctx, adminConfig, settings.Topic1)
	WaitTopic(ctx, adminConfig, settings.Topic2)

	producerConfig := &kafka.ConfigMap{
		"bootstrap.servers":        settings.BootstrapServers,
		"ssl.ca.location":          settings.SslCaLocation,
		"ssl.certificate.location": settings.SslCertificateLocation,
		"ssl.key.location":         settings.SslKeyLocation,
		"message.timeout.ms":       10000,
		"message.send.max.retries": 100,
		"acks":                     "all",
		"security.protocol":        "SASL_SSL",
		"sasl.mechanism":           "PLAIN",
		"sasl.username":            settings.ProducerUsername,
		"sasl.password":            settings.ProducerPassword,
	}

	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":        settings.BootstrapServers,
		"ssl.ca.location":          settings.SslCaLocation,
		"ssl.certificate.location": settings.SslCertificateLocation,
		"ssl.key.location":         settings.SslKeyLocation,
		"session.timeout.ms":       6000,
		"enable.auto.commit":       true, // упрощаем так как тестовое задание
		"auto.offset.reset":        "earliest",
		"security.protocol":        "SASL_SSL",
		"sasl.mechanism":           "PLAIN",
		"sasl.username":            settings.ConsumerUsername,
		"sasl.password":            settings.ConsumerPassword,
		"group.id":                 settings.ConsumerGroup,
	}

	messages := []string{"1", "2", "3", "4", "5"}

	var wg sync.WaitGroup
	wg.Go(func() {
		ProduceMessages(producerConfig, settings.Topic1, messages)
	})

	wg.Go(func() {
		ProduceMessages(producerConfig, settings.Topic2, messages)
	})

	wg.Go(func() {
		ConsumeMessages(ctx, consumerConfig, settings.Topic1)
	})

	wg.Go(func() {
		ConsumeMessages(ctx, consumerConfig, settings.Topic2)
	})

	wg.Wait()
	log.Println("Завершено")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
