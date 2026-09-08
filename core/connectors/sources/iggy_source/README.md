# Iggy Source Connector

The Iggy source connector replicates a topic from an upstream Apache Iggy
cluster into a stream and topic managed by the connectors runtime. It polls
every upstream partition and persists the last acknowledged offset for each
partition so replication can resume after a restart.

## Features

- Polls every partition of one upstream topic.
- Preserves payload bytes and non-zero message IDs.
- Preserves user headers by default, with an option to omit them.
- Stores per-partition offsets in the connector runtime's state storage.
- Retries polling failures with exponential backoff and jitter.
- Creates the configured upstream stream or topic when it does not exist.

## Configuration

```toml
type = "source"
key = "iggy"
enabled = true
version = 0
name = "Iggy source"
path = "target/release/libiggy_connector_iggy_source"
verbose = false
benchmark = false

[[streams]]
stream = "downstream_stream"
topic = "downstream_topic"
schema = "raw"
batch_length = 100
linger_time = "5ms"

[plugin_config]
connection_string = "iggy+tcp://iggy:iggy@127.0.0.1:8090"
upstream_stream = "upstream_stream"
upstream_topic = "upstream_topic"
poll_interval = "1s"
batch_size = 100
initial_offset = "earliest"
include_user_headers = true
retry_interval = "1s"
max_retry_interval = "60s"
verbose_logging = false
```

Use `schema = "raw"` on the downstream stream to replicate payload bytes
without decoding or re-encoding them. The runtime-level `batch_length` controls
downstream producer batching, while `plugin_config.batch_size` controls how many
messages are requested from each upstream partition in one poll cycle.

### Plugin Fields

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `connection_string` | yes | none | Connection string for the upstream Iggy cluster. |
| `upstream_stream` | yes | none | Name of the stream to replicate from. |
| `upstream_topic` | yes | none | Name of the topic to replicate from. |
| `poll_interval` | no | `2s` | Delay before each upstream poll cycle. |
| `batch_size` | no | `100` | Maximum messages requested from each upstream partition per poll cycle. |
| `initial_offset` | no | `earliest` | Starting position for a partition without a saved offset. Accepts `earliest`, `latest`, or an absolute numeric offset. |
| `include_user_headers` | no | `true` | Copy user headers to downstream messages. |
| `retry_interval` | no | `1s` | Base delay used for exponential backoff after a failed poll cycle. |
| `max_retry_interval` | no | `60s` | Maximum delay between poll retries. |
| `verbose_logging` | no | `false` | Log per-cycle connector details at info level instead of debug level. |

The `connection_string` can use any transport supported by the Rust client,
including TCP, QUIC, HTTP, and WebSocket. It may contain credentials, so keep
the connector configuration private or supply the value through the
`IGGY_CONNECTORS_SOURCE_IGGY_PLUGIN_CONFIG_CONNECTION_STRING` environment
variable.

## Offset and Delivery Semantics

For each partition, the connector resumes at one offset after the last saved
offset. `initial_offset` is used only when that partition has no saved offset:

- `earliest` starts with the oldest available message.
- `latest` requests up to `batch_size` of the most recent messages.
- A numeric value starts at that absolute offset.

The connector stages new offsets while polling. It commits them only after the
runtime sends the complete downstream batch and saves the connector state. If
the batch is rejected, the previous offsets remain committed and the messages
are eligible for replay. Consumers should therefore tolerate duplicate
delivery after failures or crashes.

> When `initial_offset = "latest"` and no offset has been committed yet, each
> retry recalculates the current tail of the upstream partition. If a first
> batch is rejected while new messages arrive, messages from that failed batch
> can be skipped. Use `earliest` or a numeric offset when replay continuity is
> required.

If retention makes a saved offset invalid, the connector clears that
partition's saved position and applies `initial_offset` again.

## Message Mapping

| Upstream field | Downstream behavior |
| --- | --- |
| Payload | Copied without modification when the downstream schema is `raw`. |
| Message ID | Copied when the upstream ID is non-zero. |
| User headers | Copied when `include_user_headers = true`. |
| Offset and partition | Reassigned by the downstream Iggy topic. |
| Timestamp and checksum | Generated for the downstream message. |

## Operational Notes

- The connector discovers upstream partitions when it opens. Restart it after
  adding partitions to the upstream topic.
- If the upstream stream is missing, the connector creates it. If the topic is
  missing, it creates the topic with one partition and no compression.
- Invalid `initial_offset` values produce a warning and fall back to
  `earliest`.
- If user headers cannot be decoded, the connector rejects the complete polled
  batch so its offsets are not advanced. Set `include_user_headers = false` to
  replicate payloads without parsing upstream headers.
