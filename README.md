# yandex-kafka-5

Как проверить.

Запустить `docker compose up -d`

Дождаться создания топиков. Увидеть в логах как producer записывает в topic1 и topic2.
Увидеть как консьюмер получает сообщения из topic1 и следующий лог:
```
Consumer topic2: Ошибки кафки при получения сообщения: Subscribed topic not available: topic2: Broker: Topic authorization failed
```