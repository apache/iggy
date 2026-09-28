# MongoDB Source Connector with State Management

This MongoDB source connector polls a MongoDB collection, produces each document as a JSON message into Apache Iggy, and persists its progress so ingestion resumes where it left off after a restart.

## Features

- **Incremental Data Processing**: Track the last processed timestamp (`timestamp_field`) to avoid reprocessing documents
- **Timestamp-ordered Batches**: Incremental polls filter with `$gt`, sort ascending on the timestamp field, and cap each batch with `limit`
- **Custom Filters**: Restrict the documents read with a MongoDB `query` filter
- **Connection Pooling**: Configurable driver pool size via `max_pool_size`
- **Persistent State Storage**: State is persisted by the connectors runtime (file or HTTP backend)
- **State Recovery**: Resume processing from the last known timestamp after restart
- **JSON Output**: Every document is emitted with the `json` schema

## Configuration

### Basic Configuration

```toml
type = "source"
key = "mongodb"
enabled = true
version = 0
name = "MongoDB source"
path = "target/release/libiggy_connector_mongodb_source"

[[streams]]
stream = "mongodb_stream"
topic = "documents"
schema = "json"
batch_length = 100
linger_time = "5ms"

[plugin_config]
connection_uri = "mongodb://admin:admin123@localhost:27017"
database = "test_source"
collection = "test_messages"
max_pool_size = 10
polling_interval = "30s"
limit = 100
timestamp_field = "timestamp"
query = { status = "active" }
```

| Field              | Required | Default         | Description                                                                 |
| ------------------ | -------- | --------------- | --------------------------------------------------------------------------- |
| `connection_uri`   | yes      |                 | MongoDB connection string. Treated as a secret and redacted when serialized |
| `database`         | yes      |                 | Database to read from                                                       |
| `collection`       | yes      |                 | Collection to read from                                                     |
| `max_pool_size`    | no       | driver default  | Maximum number of connections in the driver pool                            |
| `query`            | no       | `{}`            | MongoDB filter document applied to every poll                               |
| `timestamp_field`  | no       | none            | BSON `Date` field used for incremental polling                              |
| `limit`            | no       | `100`           | Maximum documents per incremental poll                                      |
| `polling_interval` | no       | `"10s"`         | Delay before each poll (humantime format). Invalid values fall back to 10s  |

### State Management Configuration

The plugin has no state settings of its own. State is configured once for the whole connectors runtime, in the runtime `config.toml`:

```toml
[state]
path = "local_state"  # used by storage = "file"
storage = "file"      # "file" | "http"
```

## State Information

The connector tracks the following state information:

### Processing State

- `last_poll_timestamp`: Highest `timestamp_field` value seen so far
- `total_documents_fetched`: Total number of documents produced
- `poll_count`: Number of polling cycles executed

### Error Tracking

The connector does not persist error counters. Errors are returned from `poll()` to the runtime, which logs them.

### Performance Statistics

The connector does not record performance statistics in its state. Use the runtime logs and metrics instead.

## Storage Backends

### File Storage (Default)

State is written to `{path}/source_{key}.state`, where `key` is the connector `key`. The runtime writes to a temporary file and renames it, so a crash never leaves a half-written state file.

```toml
[state]
storage = "file"
path = "local_state"
```

### HTTP Storage

State is stored at `{url}/source_{key}` with optimistic concurrency (ETag/If-Match) and idempotent retries.

```toml
[state]
storage = "http"

[state.http]
url = "http://127.0.0.1:8080/connectors/state"
timeout = "5s"
```

## Usage Examples

### Basic Usage with State Management

The connectors runtime normally drives this lifecycle. Calling it directly looks like this:

```rust
use iggy_connector_mongodb_source::{MongodbSource, MongodbSourceConfig};

// `state` is the previously persisted ConnectorState, if any
let mut connector = MongodbSource::new(id, config, state);

// Open connector (creates the MongoDB client)
connector.open().await?;

// Poll documents. The returned ProducedMessages carries the updated state
let produced = connector.poll().await?;

// Close connector
connector.close().await?;
```

### Building

```bash
cargo build --release -p iggy_connector_mongodb_source
```

The resulting `target/release/libiggy_connector_mongodb_source` is what the connector `path` points to.

## State File Format

State is serialized with MessagePack, so the file is binary. Its content is equivalent to:

```json
{
  "last_poll_timestamp": "2024-01-15T10:30:00Z",
  "total_documents_fetched": 15000,
  "poll_count": 150
}
```

Messages themselves are the documents serialized with `serde_json`, so BSON types such as `ObjectId` and `Date` appear in extended JSON form, for example `{"_id": {"$oid": "65a4f0c2e1b2c3d4e5f60718"}}`.

## Best Practices

1. **Key Uniqueness**: Use a unique connector `key` per instance, since the state file is named after it
2. **Index the Timestamp Field**: Create an index on `timestamp_field` so the sorted `$gt` query stays fast
3. **Use BSON Dates**: Store `timestamp_field` as a BSON `Date`. Other types do not advance the state
4. **Storage Location**: Point `[state].path` at persistent storage in production
5. **Batch Tuning**: Balance `limit` and `polling_interval` against your write rate
6. **Credentials**: Keep real credentials in `connection_uri` out of committed config files

## Troubleshooting

### Common Issues

1. **Duplicate Messages on Every Poll**: Without `timestamp_field`, each poll reads every document that matches `query`. Set `timestamp_field` for incremental ingestion
2. **Large First Poll**: With no saved timestamp, the first poll runs without `limit` or sort and reads the whole matching collection
3. **State Not Advancing**: `timestamp_field` values that are strings or numbers are ignored. Only BSON `Date` values update `last_poll_timestamp`
4. **Skipped Documents**: The filter uses `$gt`, so documents sharing the last seen timestamp that fall past a `limit` boundary are not read on the next poll. Prefer unique, high-resolution timestamps
5. **Connection Failures**: Invalid URIs or unreachable hosts fail in `open()` with an init error. Check `connection_uri` and credentials
6. **Starting Fresh**: Delete `source_<key>.state` to reset progress

### Monitoring

Monitor the following:

- Runtime logs (restored state on startup, total documents processed on close)
- State file updates in `[state].path`
- Poll errors reported by the runtime
- MongoDB server metrics for the queried collection

## Migration

To enable incremental processing on an existing connector:

1. Add `timestamp_field` to `plugin_config`
2. Restart the connector
3. The first poll reads all matching documents, then later polls only fetch newer ones

To migrate between state storage backends:

1. Stop the connectors runtime
2. Update the `[state]` configuration
3. Move the existing state to the new backend, or accept a full re-read on the first poll
4. Restart the runtime
