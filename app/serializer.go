package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

// Serializer инкапсулирует работу с Schema Registry для объектов Person.
type Serializer[T any] struct {
	serializer   *jsonschema.Serializer
	deserializer *jsonschema.Deserializer
	topic        string
}

// Создаёт новый Serializer[T] для заданного топика и настроек приложения.
func NewSerializer[T any](registryUrl string, topic string) *Serializer[T] {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(registryUrl))
	if err != nil {
		log.Fatalf("Ошибка при подключении к Schema Registry: %v", err)
	}

	serializer, err := jsonschema.NewSerializer(client, serde.ValueSerde, jsonschema.NewSerializerConfig())
	if err != nil {
		log.Fatalf("Ошибка при создании сериализатора: %v", err)
	}

	deserializer, err := jsonschema.NewDeserializer(client, serde.ValueSerde, jsonschema.NewDeserializerConfig())
	if err != nil {
		log.Fatalf("Ошибка при создании десериализатора: %v", err)
	}

	return &Serializer[T]{
		serializer:   serializer,
		deserializer: deserializer,
		topic:        topic,
	}
}

// Преобразует T в []byte для отправки в Kafka.
func (s *Serializer[T]) Serialize(p T) []byte {
	bytes, err := s.serializer.Serialize(s.topic, p)
	if err != nil {
		log.Fatalf("Не удалось сериализовать сообщение: %v", err)
	}
	return bytes
}

// Преобразует []byte в T.
func (s *Serializer[T]) Deserialize(data []byte) (T, error) {
	var msg T
	err := s.deserializer.DeserializeInto(s.topic, data, &msg)
	return msg, err
}
