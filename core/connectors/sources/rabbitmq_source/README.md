# Apache Iggy RabbitMQ Source Connector (`iggy_connector_rabbitmq_source`)

The RabbitMQ source connector reads messages from one existing RabbitMQ queue and writes them to one Apache Iggy stream and topic. It uses AMQP 0-9-1 and manual acknowledgements.

## Architecture Overview

```text
RabbitMQ queue → RabbitMQ source plugin → Iggy connectors runtime → Iggy stream and topic
```

Create the queue and its bindings in RabbitMQ. The connector does not create broker topology.

## Features

- Manual acknowledgements after Iggy accepts a batch.
- Batch collection by message count, payload bytes, and timeout.
- Raw mode for unchanged message bodies and selected AMQP properties.
- JSON envelope mode for message properties, AMQP headers, and delivery details.
- TLS with a trusted or custom CA, with optional mutual TLS.
- Reconnect after RabbitMQ loses its connection after source startup.

## Configuration Architecture

The runtime configuration defines the plugin, its path, and the Iggy destination. The `[plugin_config]` table defines the RabbitMQ connection and queue.

The runtime converts `[plugin_config]` to JSON and passes it to the plugin. See the complete [runtime example](../../runtime/example_config/connectors/rabbitmq_source.toml) and the plugin [sample configuration](config.toml).

Keep the top-level runtime setting `exactly_once` set to `false`, its default value. The RabbitMQ source does not support `exactly_once = true`.
The source keeps pending batches in memory and does not persist them. After a restart, RabbitMQ can redeliver unacknowledged messages, but the source cannot reliably restore the same batch.
If you enable `exactly_once`, the runtime can reuse an earlier producer sequence for new messages after a restart.
Iggy can treat those messages as duplicates without writing them, and the source can acknowledge them to RabbitMQ.
This can cause message loss. The runtime currently accepts `exactly_once = true` and does not reject it for this source.

## Configuration Options

| Field | Required | Default | Description |
| :--- | :---: | :--- | :--- |
| `amqp_url` | Yes | None | RabbitMQ URL with the `amqp://` or `amqps://` scheme. |
| `queue` | Yes | None | Name of an existing queue. The name must contain 1 to 255 bytes. |
| `batch_size` | No | `100` | Maximum messages in a batch. It must be greater than zero. RabbitMQ prefetch is capped at 65,535. |
| `batch_timeout` | No | `"10ms"` | Maximum wait after the first delivery before the connector returns a batch. It must be greater than zero. |
| `include_metadata` | No | `false` | Set to `true` to emit the JSON envelope. Use the `json` destination schema. |
| `timeout_secs` | No | `30` | Timeout for connection setup, consumer setup, and acknowledgements. It must be greater than zero. |
| `verbose_logging` | No | `false` | Logs prepared batches at info level when enabled. |
| `tls.ca_file` | No | None | Optional PEM file for a custom CA. |
| `tls.client_cert_file` | No | None | Optional PEM client certificate for mutual TLS. Set it with `tls.client_key_file`. |
| `tls.client_key_file` | No | None | Optional PEM client key for mutual TLS. Set it with `tls.client_cert_file`. |

When `batch_size` exceeds 65,535, RabbitMQ prefetch limits the number of unacknowledged deliveries.
The source returns a partial batch when `batch_timeout` expires, so it can acknowledge those deliveries.

Each batch contains at most 64,000,000 bytes of prepared payloads. The source keeps
one copy for retry and makes another copy for the runtime, for up to 128,000,000
payload bytes across those copies. It can also retain one delivery of up to
64,000,000 payload bytes for the next batch. Together, these payloads can use
up to 192,000,000 bytes. Headers, serialization, RabbitMQ prefetch, and runtime buffers use more
memory, so this limit does not bound total process memory. In metadata mode, the
limit counts the JSON envelope, including its Base64 body.

Use `amqps://` when the RabbitMQ listener uses TLS. If you add a `[plugin_config.tls]` table, the URL must use `amqps://`. The client certificate and key must appear together.

## Runtime Configuration Examples

This complete example uses raw mode:

```toml
type = "source"
key = "rabbitmq"
enabled = true
version = 1
name = "RabbitMQ source"
path = "target/debug/libiggy_connector_rabbitmq_source"
plugin_config_format = "json"

[[streams]]
stream = "events"
topic = "rabbitmq"
schema = "raw"
batch_length = 100
linger_time = "5ms"

[plugin_config]
amqp_url = "amqp://rabbit_user:replace-me@127.0.0.1:5672/%2f"
queue = "events"
batch_size = 100
batch_timeout = "10ms"
include_metadata = false
timeout_secs = 30
verbose_logging = false
```

This complete example uses a JSON envelope for each delivery:

```toml
type = "source"
key = "rabbitmq"
enabled = true
version = 1
name = "RabbitMQ source"
path = "target/debug/libiggy_connector_rabbitmq_source"
plugin_config_format = "json"

[[streams]]
stream = "events"
topic = "rabbitmq"
schema = "json"
batch_length = 100
linger_time = "5ms"

[plugin_config]
amqp_url = "amqp://rabbit_user:replace-me@127.0.0.1:5672/%2f"
queue = "events"
batch_size = 100
batch_timeout = "10ms"
include_metadata = true
timeout_secs = 30
verbose_logging = false
```

Use one example per connector configuration file. Change the stream, topic, queue, URL, and plugin path for your environment.

The `amqp_url` value identifies the broker, port, credentials, and virtual host. Use a broker host name and port that the connectors runtime can reach.

Use this form for a RabbitMQ listener without TLS:

```text
amqp://user:password@broker.example:5672/%2f
```

Use this form when the RabbitMQ listener uses TLS:

```text
amqps://user:password@broker.example:5671/%2f
```

The default virtual host is `/`. Encode it as `%2f` in the URL path. Encode reserved characters in usernames and passwords. Do not commit real credentials.

## TLS and Mutual TLS

For TLS, use an `amqps://` URL. If the broker certificate uses a trusted CA, the connector can use the platform trust store. Set `ca_file` for a private CA:

```toml
[plugin_config.tls]
ca_file = "/absolute/path/to/ca.pem"
```

For mutual TLS, add the client certificate and key:

```toml
[plugin_config.tls]
ca_file = "/absolute/path/to/ca.pem"
client_cert_file = "/absolute/path/to/client-cert.pem"
client_key_file = "/absolute/path/to/client-key.pem"
```

The broker must trust the client certificate's CA and require client certificates. RabbitMQ username and password authentication still applies. Use absolute certificate paths because the runtime reads them from its own filesystem.

## Message Payloads and Headers

Set the destination schema to match `include_metadata`. With `false`, Iggy receives the original body as raw bytes. The source adds selected AMQP values, such as the exchange, routing key, redelivery flag, and message ID, as Iggy headers. With `true`, the source sends a JSON envelope with the body as Base64. The envelope contains AMQP properties, headers, and delivery details.

| `include_metadata` | Schema | Output |
| :--- | :--- | :--- |
| `false` | `raw` | Original body bytes and selected AMQP properties in Iggy headers. |
| `true` | `json` | Versioned JSON envelope with a Base64 body and AMQP metadata. |

In raw mode, the connector adds these headers when values are available:

| Iggy header | AMQP value |
| :--- | :--- |
| `amqp.exchange` | Exchange name. Omitted for the default exchange. |
| `amqp.routing_key` | Routing key. Omitted when empty. |
| `amqp.redelivered` | Redelivery flag. |
| `amqp.message_id` | Message ID. |

Raw mode does not copy user-defined AMQP headers. It omits `amqp.message_id` when its value is empty or exceeds 255 bytes. It does not add the other AMQP properties as Iggy headers.

The connector leaves Iggy's message ID unset. Iggy assigns its own ID. The RabbitMQ `message_id` remains in AMQP metadata. Consumers can use that value to recognize duplicates after redelivery. The connector does not remove duplicates automatically.

Metadata mode sets the `json` schema and wraps the body as Base64. The envelope contains `version`, `body_base64`, `properties`, `headers`, and `delivery`. AMQP header values include their type. Binary header values use Base64.

The `properties` object contains each AMQP basic property that the publisher set, except `headers`, which has its own envelope field. String properties retain their values. `delivery_mode` and `priority` are JSON numbers. `timestamp` is a decimal string representing seconds since the Unix epoch. The AMQP message type uses the `type` key. Properties that the publisher did not set are omitted.

```json
{
  "version": 1,
  "body_base64": "SGVsbG8gZnJvbSBSYWJiaXRNUS4=",
  "properties": {
    "content_type": "application/octet-stream"
  },
  "headers": {
    "sensor_id": {
      "type": "short_string",
      "value": "sensor-7"
    }
  },
  "delivery": {
    "exchange": "",
    "routing_key": "events",
    "redelivered": false
  }
}
```

## Redelivery and Acknowledgements

RabbitMQ holds each supported delivery until Iggy accepts its batch.

```text
RabbitMQ → source holds delivery → runtime writes batch to Iggy
         ← source acknowledges after Iggy accepts the batch
```

The source keeps one batch in flight. If Iggy rejects a batch, the source retries that batch before it reads newer deliveries. RabbitMQ does not receive an acknowledgement for that batch.

The source stops after five consecutive batch failures. Restore the Iggy connection and restart the source or connectors runtime. RabbitMQ can redeliver messages that remain unacknowledged.

If the connector stops while a delivery is unacknowledged, RabbitMQ can requeue it. Iggy can receive a duplicate if RabbitMQ redelivers a message after Iggy accepted it but before RabbitMQ received its acknowledgement.

After Iggy accepts a batch, the source sends one acknowledgement with `multiple: true` for the last delivery. This acknowledges all deliveries in the batch. If an acknowledgement fails or times out, the source
attempts to close its RabbitMQ connection and stops with an error. Lapin acknowledgement handles are single-use, so the source
does not retry an acknowledgement or replay the batch. RabbitMQ requeues deliveries that remain unacknowledged when the
connection closes. After the source restarts, RabbitMQ can redeliver those messages, so Iggy can receive duplicates. An
acknowledgement error can have an uncertain outcome, so RabbitMQ can already have accepted the acknowledgement for a
delivery. Consumers must handle duplicates.

## Failure Handling

| Condition | Behavior |
| :--- | :--- |
| RabbitMQ is unavailable at startup | Source initialization fails. Start RabbitMQ, then restart the connectors runtime. |
| RabbitMQ disconnects after startup | The source waits for a random delay from 1 to 5 seconds, then retries connection and consume operations. |
| Iggy rejects a batch | The source retains and retries the batch. RabbitMQ deliveries remain unacknowledged. |
| A broker acknowledgement fails | The source attempts to close its AMQP connection and stops with an error. After restart, RabbitMQ can redeliver unacknowledged messages. |
| Empty body in raw mode | The connector logs and rejects the delivery without requeuing it. RabbitMQ dead-letters it if a route exists. Otherwise, RabbitMQ drops it. |
| Payload exceeds 64,000,000 bytes | The connector logs and rejects the delivery without requeuing it. RabbitMQ dead-letters it if a route exists. Otherwise, RabbitMQ drops it. |
| Delivery preparation fails | The connector logs and rejects the delivery without requeuing it. RabbitMQ dead-letters it if a route exists. Otherwise, RabbitMQ drops it. |
| Empty body in metadata mode | The connector can encode the empty body in the JSON envelope. |

The 64,000,000-byte limit applies to each emitted payload and to the combined payloads in a batch. In metadata mode, the payload includes the JSON envelope.

## Operations

Read the connectors runtime logs for startup, connection, and delivery errors. A running source status alone does not prove that RabbitMQ has an active consumer. Inspect the queue's consumer count and its ready and unacknowledged message counts in RabbitMQ. Publish a message to the queue. Read it from the configured Iggy topic to make sure that the full path works.

If the source fails to start, inspect the plugin path, broker URL, queue name, credentials, and TLS files. After you correct the problem, restart the source or connectors runtime. If RabbitMQ disconnects after startup, the source retries the connection.
