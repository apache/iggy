# RabbitMQ Sink

The RabbitMQ sink connector publishes messages from Iggy streams to RabbitMQ exchanges via AMQP 0.9.1.

## Configuration

- `amqp_url`: RabbitMQ connection URL (e.g. `amqp://guest:guest@localhost:5672`).
- `exchange`: Exchange name to publish to.
- `exchange_type`: Exchange type (`direct`, `topic`, `fanout`, `headers`). Defaults to `"topic"`.
- `routing_key`: Routing key for published messages.

```toml
[plugin_config]
amqp_url = "amqp://guest:guest@localhost:5672"
exchange = "iggy_events"
exchange_type = "topic"
routing_key = "iggy.messages"
```
