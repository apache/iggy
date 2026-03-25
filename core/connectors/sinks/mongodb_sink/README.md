# MongoDB Sink Connector

Consumes messages from Iggy streams and stores them in a MongoDB collection.

## Try It

Send a JSON message through Iggy and see it land in MongoDB.

**Prerequisites**: Docker running, project built (`cargo build` from repo root).

```bash
# Start MongoDB
docker run -d --name mongo-test -p 27017:27017 mongo:7

# Start iggy-server (terminal 2)
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy ./target/debug/iggy-server

# Create stream and topic
./target/debug/iggy -u iggy -p iggy stream create demo_stream
./target/debug/iggy -u iggy -p iggy topic create demo_stream demo_topic 1

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
./target/debug/iggy -u iggy -p iggy message send demo_stream demo_topic '{"hello":"mongodb"}'

# Verify in MongoDB
docker exec mongo-test mongosh --quiet --eval \
  'db.getSiblingDB("test_db").messages.find().pretty()'
```

Expected:

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
| `batch_size` | `100` | Documents per `insertMany` call |
| `payload_format` | `binary` | `binary`, `json`, or `string` |
| `include_metadata` | `true` | Add iggy offset, timestamp, stream, topic, partition |
| `include_checksum` | `true` | Add message checksum |
| `include_origin_timestamp` | `true` | Add origin timestamp |
| `auto_create_collection` | `false` | Create collection if missing |
| `max_pool_size` | driver default | Connection pool size |
| `verbose_logging` | `false` | Log at info instead of debug |
| `max_retries` | `3` | Retry attempts for transient errors |
| `retry_delay` | `1s` | Base delay (`retry_delay * attempt`) |

## Testing

Requires Docker. Testcontainers starts MongoDB 7 + iggy-server automatically.

```bash
cargo test --test mod -- mongodb_sink
```

This runs 4 E2E tests against a real MongoDB instance:

- `json_messages_sink_to_mongodb` — JSON payloads stored as embedded BSON documents
- `binary_messages_sink_as_bson_binary` — binary payloads stored as BSON Binary
- `large_batch_processed_correctly` — batch insertion with configurable batch size
- `auto_create_collection_on_open` — collection created automatically when missing

Unit tests (no Docker):

```bash
cargo test -p iggy_connector_mongodb_sink
```

## Delivery Semantics

This connector provides **at-least-once** delivery semantics.

### Behavior

- Messages may be delivered more than once on retry or restart
- Uses a deterministic composite MongoDB `_id`: `stream:topic:partition:message_id`
- Duplicate key collisions are treated as idempotent replay of already-written messages
- The sink remains insert-only; it does not upsert existing documents

### Known Limitations

- On network timeout during insert, MongoDB may partially commit a batch before returning an error
- The sink does not upsert on duplicate; replay safety relies on deterministic `_id` values and duplicate-key tolerance
