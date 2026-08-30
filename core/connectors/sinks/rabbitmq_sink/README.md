# RabbitMQ Sink

The RabbitMQ sink connector publishes messages from Iggy streams to RabbitMQ exchanges via AMQP 0.9.1.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `amqp_url` | string | `amqp://guest:guest@localhost:5672` | RabbitMQ connection URL, including credentials. Treated as a secret: never logged or serialized verbatim. |
| `exchange` | string | `iggy_events` | Exchange name to publish to. |
| `exchange_type` | string | `topic` | Exchange type: `direct`, `topic`, `fanout`, `headers`. |
| `routing_key` | string | `iggy.messages` | Routing key for published messages. |
| `durable_exchange` | bool | `true` | Declare the exchange as durable. Must match the durability of an operator-pre-created exchange, otherwise RabbitMQ closes the channel with `PRECONDITION_FAILED`. |
| `delivery_mode` | string | `persistent` | AMQP delivery mode: `persistent` (2) or `non_persistent` (1). Persistent messages survive broker restarts when the exchange and queue are durable; it forces an fsync on publish, so non-durable topologies may prefer `non_persistent`. |
| `include_metadata` | bool | `true` | Add `iggy_stream`, `iggy_topic`, `iggy_partition_id`, `iggy_offset` message headers. User-supplied headers are always preserved, regardless of this flag. |
| `verbose_logging` | bool | `false` | Log each published batch at `info` level instead of `debug`. |
| `max_retries` | u32 | `3` | Maximum transient publish retries before failing the batch. |
| `retry_delay_secs` | u64 | `1` | Base retry delay in seconds. |
| `max_retry_delay_secs` | u64 | `5` | Upper bound for exponential backoff. |
| `timeout_secs` | u64 | `30` | Timeout for connection, publish, and publisher-confirmation operations. |

User headers on consumed Iggy messages are forwarded as AMQP headers: string values become AMQP `LongString`, raw binary values become `ByteArray`. This allows routing through a `headers` exchange on original user headers.

Publishes are confirmed via `ConfirmSelect`. With `mandatory = true`, a message with a routing key that matches no binding is returned by RabbitMQ and the batch fails with a permanent error.

**Delivery guarantee:** in-request retry only. Transient failures (connection loss, `Nack`, channel exceptions,
timeouts) are retried within a single `consume()` call up to `max_retries`, resuming at the first unconfirmed
message. Publishes are pipelined (all messages sent, then confirmed in order), so a failure mid-batch can already
have delivered the in-flight tail; resuming re-publishes those, so delivery is **at-least-once within a batch**.
However, the connectors runtime commits the consumer offset at poll time and discards `consume()`'s return value,
so there is no cross-poll redrive or DLQ: a failure that outlives the retry budget, or a crash mid-batch, is
**at-most-once** across polls.

```toml
[plugin_config]
amqp_url = "amqp://guest:guest@localhost:5672"
exchange = "iggy_events"
exchange_type = "topic"
routing_key = "iggy.messages"
durable_exchange = true
delivery_mode = "persistent"
include_metadata = true
verbose_logging = false
max_retries = 3
retry_delay_secs = 1
max_retry_delay_secs = 5
timeout_secs = 30
```
