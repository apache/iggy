# MongoDB Sink Connector

Consumes messages from Iggy streams and stores them in a MongoDB collection.

## Try It

Send a JSON message through Iggy and see it land in MongoDB.

**Prerequisites**: Docker running, project built (`cargo build` from repo root).

```bash
# Start MongoDB
docker run -d --name mongo-test -p 127.0.0.1:27017:27017 mongo:7

# Start iggy-server (terminal 2)
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy ./target/debug/iggy-server --replica-id 0

# Create stream and topic
./target/debug/iggy -u iggy -p iggy stream create demo_stream
./target/debug/iggy -u iggy -p iggy topic create demo_stream demo_topic 1 none 1d

# Setup connector config
mkdir -p /tmp/mdb-sink-test/connectors
cat > /tmp/mdb-sink-test/config.toml << 'TOML'
[iggy]
address = "localhost:8090"
username = "iggy"
password = "iggy"
[state]
path = "/tmp/mdb-sink-test/state"
[connectors]
config_type = "local"
config_dir = "/tmp/mdb-sink-test/connectors"
TOML
cat > /tmp/mdb-sink-test/connectors/sink.toml << 'TOML'
type = "sink"
key = "mongodb"
enabled = true
version = 0
name = "test"
path = "target/debug/libiggy_connector_mongodb_sink"
[[streams]]
stream = "demo_stream"
topics = ["demo_topic"]
schema = "json"
batch_length = 100
poll_interval = "100ms"
consumer_group = "test_cg"
[plugin_config]
connection_uri = "mongodb://localhost:27017"
database = "test_db"
collection = "messages"
payload_format = "json"
auto_create_collection = true
TOML

# Start connector (terminal 3)
IGGY_CONNECTORS_CONFIG_PATH=/tmp/mdb-sink-test/config.toml ./target/debug/iggy-connectors

# Send a message
./target/debug/iggy -u iggy -p iggy message send --partition-id 0 demo_stream demo_topic '{"hello":"mongodb"}'

# Verify in MongoDB
docker exec mongo-test mongosh --quiet --eval \
  'db.getSiblingDB("test_db").messages.find().pretty()'
```

Output includes:

```json
{ "payload": { "hello": "mongodb" }, "iggy_offset": 0, "iggy_stream": "demo_stream" }
```

Cleanup: `docker rm -f mongo-test && rm -rf /tmp/mdb-sink-test`

## Quick Start

```toml
[[streams]]
stream = "demo_stream"
topics = ["demo_topic"]
schema = "json"
batch_length = 100
poll_interval = "100ms"
consumer_group = "mongodb_cg"

[plugin_config]
connection_uri = "mongodb://localhost:27017"
database = "iggy_data"
collection = "messages"
payload_format = "json"
```

## Configuration

| Option | Default | Description |
| ------ | ------- | ----------- |
| `connection_uri` | **required** | MongoDB URI |
| `database` | **required** | Target database |
| `collection` | **required** | Target collection |
| `batch_size` | `100` | Maximum documents per `insertMany` call; `0` behaves as `1` |
| `payload_format` | `binary` | `binary`, `json`, or `string` |
| `include_metadata` | `true` | Add iggy offset, timestamp, stream, topic, partition |
| `include_checksum` | `true` | Add message checksum |
| `include_origin_timestamp` | `true` | Add origin timestamp |
| `auto_create_collection` | `false` | Explicitly create a missing collection at startup; `false` still permits implicit creation by an insert |
| `max_pool_size` | driver default | Connection pool size |
| `verbose_logging` | `false` | Log at info instead of debug |
| `max_retries` | `3` | Total attempts for transient insert errors, including the first; `0` behaves as `1` |
| `retry_delay` | `1s` | Base delay (`retry_delay * attempt`) |

## Testing

Requires Docker. Testcontainers starts MongoDB 7 + iggy-server automatically.

```bash
cargo nextest run -p integration -E 'test(connectors::mongodb::)'
```

The integration suite covers payloads, batching, duplicate handling, validation failures,
write concern and retryable writes against real MongoDB instances. Cases include:

- `json_messages_sink_to_mongodb` - JSON payloads stored as embedded BSON documents
- `binary_messages_sink_as_bson_binary` - binary payloads stored as BSON Binary
- `large_batch_processed_correctly` - batch insertion with configurable batch size
- `auto_create_collection_on_open` - collection created automatically when missing

Unit tests (no Docker):

```bash
cargo test -p iggy_connector_mongodb_sink
```

## Delivery Semantics

The runtime auto-commits while polling, records plugin errors and continues
without replaying failed batches. This does not provide an end-to-end
at-least-once guarantee.

### Behavior

- Each document has a deterministic `_id`: `stream:topic:partition:message_id`.
  Offset is not included; reuse of an ID can discard a different message.
- Payloads remain under `payload`, as BSON Binary, a JSON-derived BSON value,
  or a UTF-8 string. JSON can be an object, array or scalar; invalid conversion
  rejects its entire chunk. Formatting follows stream decoding and transforms.
- Message headers are not stored. Checksum and origin timestamp flags are
  independent of `include_metadata`. BSON datetimes truncate microseconds to
  milliseconds; large offsets use `iggy_offset_str`, and large checksums use
  decimal strings when they do not fit a signed 64-bit integer.
- Current-poll chunks are inserted immediately with unordered `insert_many`.
  Later chunks are still attempted after errors; the last chunk error is
  returned. There is no cross-poll buffer or batch-wide transaction.
- All-duplicate write errors (`11000`) without a write-concern error are treated
  as success. This applies to every unique index, not only `_id`, without
  comparing payload contents. A conflict on another unique index can discard
  a distinct message while runtime statistics report it processed.
- The sink is insert-only and does not update existing documents.

### Retry and failure limits

`max_retries` counts total plugin attempts. Linear backoff waits `retry_delay`
multiplied by the retry number; an invalid duration silently falls back to `1s`.
Driver-level retryable writes can retry independently, depending on the URI and
deployment. Startup parses the URI, pings the target database and optionally
creates the collection, without a connector-level retry loop. An explicit
`max_pool_size` overrides the URI pool setting.

MongoDB can store part of an unordered insert before returning an error.
Timeouts and write-concern failures can leave the outcome uncertain. The
plugin's inserted-message counter reports known or estimated insert counts;
its error counter counts failed chunks. Runtime counters cover whole callbacks,
so they are not exact MongoDB document counts.
