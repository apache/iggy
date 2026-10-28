# ScopeDB Sink Connector

The ScopeDB sink appends Iggy messages to an existing ScopeDB table through the ScopeDB Rust SDK. Each `consume()` call waits for its writes and retries to settle. It keeps no buffer across calls.

## Configuration

```toml
type = "sink"
key = "scopedb"
enabled = true
version = 0
name = "ScopeDB sink"
path = "target/release/libiggy_connector_scopedb_sink"
verbose = false

[[streams]]
stream = "orders"
topics = ["events"]
schema = "json"
batch_length = 1000
consumer_group = "scopedb_sink_connector"

[plugin_config]
endpoint = "https://<workspace-endpoint>"
api_key = "<api-key>"
table = "events"
mode = "mapped"
include_metadata = true
include_headers = false
```

Inject credentials with `IGGY_CONNECTORS_SINK_SCOPEDB_PLUGIN_CONFIG_API_KEY`. Plugin config uses `SecretString` for the API key and redacts the endpoint in debug output. Embedded URL credentials, query parameters, and fragments are rejected. HTTPS, custom base paths, and the HTTP client's platform proxy settings are supported.

| Option | Default | Meaning |
| ------ | ------- | ------- |
| `endpoint` | required | ScopeDB HTTP(S) endpoint |
| `api_key` | none | Bearer API key, when required by the endpoint |
| `database` | `scopedb` | Target database |
| `schema` | `public` | Target schema |
| `table` | required | Existing target table |
| `mode` | `mapped` | `mapped` or `raw` |
| `payload_column` | `payload` | Destination column in raw mode |
| `include_metadata` | `true` | Populate the six metadata columns below |
| `include_headers` | `false` | Populate `iggy_headers`, independently of `include_metadata` |
| `batch_size` | `100` | Maximum rows per HTTP request, at most 200,000 |
| `max_request_bytes` | `8388608` | Maximum uncompressed NDJSON request bytes; oversized individual rows are rejected |
| `max_concurrent_requests` | `1` | Concurrent requests per consume call; larger values permit out-of-order completion |
| `max_retries` | `3` | Total attempts per request, including the first, clamped to 1..3 |
| `retry_delay` | `1s` | Initial exponential backoff |
| `max_retry_delay` | `30s` | Maximum backoff, including `Retry-After` |
| `retry_unknown` | `false` | Explicitly allow replay of transient unknown outcomes; duplicates are possible |
| `timeout` | `30s` | Overall startup deadline and timeout per HTTP write attempt, including response body reads |
| `verbose_logging` | `false` | Log consume summaries at info rather than debug |

Durations use humantime strings. Invalid duration strings fall back to the documented default with a warning; reversed retry delays are swapped. Zero write timeouts and invalid request limits fail startup.

The identity needs permission to read the target table definition and append rows. The connector never creates tables, changes schemas, or submits transformation statements. Restart the connector after changing the table definition.

## Modes and schema validation

`open()` reads the table's complete Catalog definition. Missing metadata columns, computed metadata columns, and unsupported raw payload columns fail startup.

In `mapped` mode, `Json`, `Text`, and `Proto` payloads must represent JSON objects. Writable table columns select top-level fields by name; unmatched fields and computed columns are omitted. Enabled Iggy metadata overrides payload fields with the same names. Missing columns follow ScopeDB's normal null/computed-column behavior. The connector does not implement BigQuery's default-value or missing-value policies.

In `raw` mode, the configured payload column determines the representation:

| Column type | Payloads |
| ----------- | -------- |
| `binary` | All payload variants, encoded as canonical padded base64 for NDJSON |
| `string` | UTF-8 bytes from Text, Proto, Raw, FlatBuffer, Avro, or serialized Json |
| `object` | Json objects, or Text/Proto/Raw values that parse as JSON objects |
| `any` | Json values, or Text/Proto/Raw values that parse as JSON |

Raw mode preserves the representation delivered by Iggy's decoder/transforms. A Json value is serialized again; it does not preserve the original wire whitespace or original Avro/Protobuf bytes after decoding.

NDJSON integers, including nested values and `uint` columns, must fit signed 64-bit integers. Oversized integers are rejected without narrowing. Store identifiers outside that range as strings. Schema/type validation errors returned by ScopeDB retain their request-relative row indexes through the SDK.

## Metadata

With `include_metadata = true`, the table must contain these writable columns:

| Column | ScopeDB type | Value |
| ------ | ------------ | ----- |
| `iggy_stream` | `string` | Source stream |
| `iggy_topic` | `string` | Source topic |
| `iggy_partition_id` | `uint` or `int` | Partition ID |
| `iggy_offset` | `uint` or `int` | Each message's offset, checked against the NDJSON integer limit |
| `iggy_timestamp` | `timestamp` | Message timestamp converted from microseconds to UTC RFC 3339 |
| `iggy_id` | `string` | Full decimal u128 message ID |
| `iggy_headers` | `object` or `any` | Only required with `include_headers = true` |

Text headers become strings. Binary headers become `{"data":"<base64>","iggy_header_encoding":"base64"}`. Missing headers become an empty object. `messages_metadata.current_offset` is a poll high-water mark and is not used as the row offset or a batch identity.

Example table for mapped mode:

```sql
CREATE TABLE events (
    id int,
    event string,
    iggy_stream string,
    iggy_topic string,
    iggy_partition_id uint,
    iggy_offset uint,
    iggy_timestamp timestamp,
    iggy_id string,
    iggy_headers object
);
```

## Failures and delivery semantics

The SDK splits requests at the configured byte and row limits. Temporary rejections retry with bounded exponential backoff; error metadata and `Retry-After` are preserved. Unknown outcomes are not replayed by default. Client-side write timeouts are never automatically replayed, including when `retry_unknown = true`. The connector adds no HTTP retry middleware or redirects that could replay a write behind the SDK's back.

A complete, atomic rejection with row errors identifies rows known to be invalid. The connector deduplicates the indexes, logs and drops those rows, then appends the remaining rows from that request once more. Successful sibling requests are never replayed. Truncated or out-of-range row errors do not authorize this recovery. A second row-error response fails the remaining rows instead of starting an unbounded filtering loop.

Every local rejection and terminal failed/unconfirmed row is logged with stream, topic, partition, and message offset. `stats()` and the shutdown log distinguish written, rejected, failed, and unconfirmed rows. These are logical input counts; opt-in unknown replay can create extra physical copies. If every message is rejected, `consume()` returns an error. There is no dead-letter queue.

The Iggy connectors runtime currently commits consumer group offsets during polling, before the sink persists the messages. A failed consume call is not redelivered and stops the affected consumer task. A crash after the offset commit can lose that batch. Split requests can partially succeed.

This connector does not promise crash-safe at-least-once or exactly-once delivery. Use the source metadata for reconciliation and downstream deduplication; stronger guarantees require runtime offset handling changes.

## Build and verification

```bash
cargo build -p iggy_connector_scopedb_sink
cargo test -p iggy_connector_scopedb_sink
```

The SDK dependency is pinned to a Git revision providing finite batch writes and input-index reports. Builds require access to that repository until a published SDK release replaces the Git dependency.

The default tests use an in-process HTTP server and the real Rust SDK. They verify zstd NDJSON, schema discovery, authentication, raw/mapped payloads, metadata, request splitting, bounded retries, row-error recovery, partial success, and unknown outcomes without cloud access.

To verify against a real ScopeDB endpoint, set credentials with permission to create/delete an isolated schema, read its catalog, append rows, and query results:

```bash
export SCOPEDB_ENDPOINT='https://<workspace-endpoint>'
export SCOPEDB_API_KEY='<api-key>'
cargo test -p iggy_connector_scopedb_sink --test live_scopedb -- --ignored --nocapture
```

The test creates a uniquely named schema, writes mapped events including a bad row, reads back metadata, writes non-UTF-8 binary payloads, and removes its schema even when verification panics.

For an Iggy runtime smoke test, create the sample table, build the plugin, and copy `config.toml` into the runtime's configured connectors directory. Set the endpoint/key, start the runtime, and produce `{"id":1,"event":"created"}` to the configured stream/topic. Query the target table and check the source offset and message ID. Also produce a wrong-typed `id`; verify that it is logged and counted while valid messages still arrive.
