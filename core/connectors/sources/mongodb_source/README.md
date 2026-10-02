# MongoDB Source Connector with State Management

This MongoDB source connector polls a MongoDB collection, produces each document as a JSON message into Apache Iggy, and persists its progress so ingestion resumes where it left off after a restart.

## Features

- **Incremental Data Processing**: Resume from the last acknowledged `timestamp_field` position; retries can duplicate documents
- **Timestamp-ordered Batches**: Incremental polls filter with `$gt`, sort ascending on the timestamp field, and cap each batch with `batch_size`
- **Custom Filters**: Restrict the documents read with a MongoDB `query` filter
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
polling_interval = "30s"
batch_size = 100
timestamp_field = "timestamp"
query = { status = "active" }
```

| Field              | Required | Default         | Description                                                                 |
| ------------------ | -------- | --------------- | --------------------------------------------------------------------------- |
| `connection_uri`   | yes      |                 | MongoDB connection string. Redacted in debug output and never serialized    |
| `database`         | yes      |                 | Database to read from                                                       |
| `collection`       | yes      |                 | Collection to read from                                                     |
| `query`            | no       | `{}`            | MongoDB filter document applied to every poll                               |
| `timestamp_field`  | no       | none            | BSON `Date` field used for incremental polling. Dotted paths are supported  |
| `batch_size`       | no       | `100`           | Maximum documents per incremental poll. Must be greater than 0              |
| `polling_interval` | no       | `"10s"`         | Delay before each poll (humantime format). Invalid values fall back to 10s  |

Read concern defaults to `majority` unless `connection_uri` sets one. A dotted `timestamp_field` is followed through embedded documents only; a path that crosses an array is rejected at startup.

### State Storage

The plugin has no state settings of its own. Where state is stored, and how, is configured once for the whole connectors runtime; see [State storage](../../runtime/README.md#state-storage) in the runtime README.

## Delivery Guarantees

Each batch is delivered at least once, so consumers must tolerate duplicates. The candidate cursor is returned with the batch and committed in memory only after the runtime sends the messages, saves the state and returns `Ack`. A `Nack`, or a crash between sending and saving, replays the whole batch on the next poll. Replayed documents keep the same `_id`, so messages with an `ObjectId`, UUID, `Int32` or `Int64` `_id` carry a stable Iggy message ID; other `_id` types do not.

Documents that commit with a `timestamp_field` at or below the cursor after it has advanced, and updates to documents already read, are never produced. This is not a complete change-data-capture feed.

## State Information

The connector tracks the following state information:

### Processing State

- `last_poll_timestamp`: Highest `timestamp_field` value acknowledged so far, as milliseconds since the Unix epoch
- `total_documents_fetched`: Total number of documents produced
- `poll_count`: Number of polling cycles executed
- `last_id`: `_id` of the document at `last_poll_timestamp`, stored as canonical extended JSON and used to break timestamp ties

### Error Tracking

The connector does not persist error counters. Errors are returned from `poll()` to the runtime, which logs them.

### Performance Statistics

The connector does not record performance statistics in its state. Use the runtime logs and metrics instead.

## Usage Examples

### Basic Usage with State Management

The connectors runtime normally drives this lifecycle. Calling it directly looks like this:

```rust
use iggy_connector_mongodb_source::{MongoDbSource, MongoDbSourceConfig};

// `state` is the previously persisted ConnectorState, if any
let mut connector = MongoDbSource::new(id, config, state);

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
  "last_poll_timestamp": 1705314600000,
  "total_documents_fetched": 15000,
  "poll_count": 150,
  "last_id": "{\"$oid\":\"65a4f0c2e1b2c3d4e5f60718\"}"
}
```

## Message Format

Each document is emitted as MongoDB relaxed extended JSON, so no BSON type is lost: `ObjectId` becomes `{"$oid": ...}`, `Date` becomes an RFC 3339 string under `$date`, binary data keeps its subtype under `$binary`, and `NaN` or `Infinity` doubles are preserved as `{"$numberDouble": ...}`. Plain numbers, strings, booleans, arrays and nested documents appear as regular JSON values.

```json
{
  "_id": { "$oid": "65a4f0c2e1b2c3d4e5f60718" },
  "name": "doc_1",
  "value": 10,
  "timestamp": { "$date": "2024-01-15T10:30:00Z" }
}
```

## Best Practices

1. **Key Uniqueness**: Use a unique connector `key` per instance, since the state file is named after it
2. **Index the Sort**: Polls sort on `(timestamp_field, _id)`, which a single-field index cannot serve, so MongoDB sorts every document past the cursor in memory. Create a compound index `{ <timestamp_field>: 1, _id: 1 }`, with any equality fields from `query` placed before it
3. **Use BSON Dates**: Store `timestamp_field` as a BSON `Date`. Documents where it is missing or another type are skipped
4. **Storage Location**: Point `[state].path` at persistent storage in production
5. **Batch Tuning**: Balance `batch_size` and `polling_interval` against your write rate
6. **Credentials**: Keep real credentials in `connection_uri` out of committed config files

## Troubleshooting

### Common Issues

1. **Duplicate Messages on Every Poll**: Without `timestamp_field`, each poll reads every document that matches `query`. Set `timestamp_field` for incremental ingestion
2. **Unbounded Polls Without `timestamp_field`**: Without `timestamp_field`, every poll runs without `batch_size` or sort and reads the whole matching collection into one batch
3. **Missing Documents**: Incremental polls only match documents whose `timestamp_field` is a BSON `Date`. Documents where it is missing, a string or a number are skipped and never produced
4. **Shared Timestamps**: Batches are sorted by `(timestamp_field, _id)`, and the next poll reads documents with a later timestamp, or the same timestamp and a greater `_id`. The `_id` comparison uses `$expr`, so it follows BSON sort order even when `_id` types are mixed. Documents sharing a timestamp across a `batch_size` boundary are read on the next poll
5. **Connection Failures**: Invalid URIs, unreachable hosts or a collection the user cannot read fail in `open()`. The runtime reports `Plugin initialization failed`; the reason is in the connector log
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
3. Polls read matching documents in timestamp order, `batch_size` at a time, then only fetch newer ones
