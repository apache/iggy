# Apache Iggy S3 Sink Connector

Writes messages from Iggy streams to Amazon S3 and S3-compatible object stores (MinIO, Cloudflare R2, DigitalOcean Spaces, Backblaze B2).

## Features

- Buffered uploads with configurable file rotation (by size or message count)
- Multiple output formats: JSON Lines, JSON Array, Raw
- Configurable path templates with variables for stream, topic, date, hour, partition
- S3 keys include offset ranges for human-readable object naming
- Optional metadata and header inclusion in output
- Support for custom endpoints (MinIO, R2) and path-style addressing
- Retry with exponential backoff and jitter on transient upload failures

## Configuration

### Connector Runtime Config

```toml
type = "sink"
key = "s3"
enabled = true
version = 0
name = "S3 sink"
path = "../../target/release/libiggy_connector_s3_sink"
verbose = false

[[streams]]
stream = "application_logs"
topics = ["api_requests", "errors"]
schema = "json"
batch_length = 1000
poll_interval = "100ms"
consumer_group = "s3_sink"
```

### Plugin Configuration

```toml
[plugin_config]
bucket = "my-data-lake"
prefix = "iggy/raw"
region = "us-east-1"
# endpoint = "http://localhost:9000"       # for MinIO / S3-compatible stores
# access_key_id = "AKIA..."               # omit to use env vars / instance profile
# secret_access_key = "..."               # omit to use env vars / instance profile
path_template = "{stream}/{topic}/{date}/{hour}"
file_rotation = "size"
max_file_size = "8MiB"
output_format = "json_lines"
include_metadata = true
include_headers = true
max_attempts = 3
retry_delay = "1s"
```

### Options Reference

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `bucket` | String | **required** | S3 bucket name |
| `region` | String | **required** | AWS region (e.g. `us-east-1`) |
| `prefix` | String | `None` | Key prefix prepended to all objects |
| `endpoint` | String | `None` | Custom S3 endpoint for MinIO, R2, etc. |
| `access_key_id` | String | `None` | AWS access key; omit for env/instance profile |
| `secret_access_key` | String | `None` | AWS secret key; omit for env/instance profile |
| `path_template` | String | `{stream}/{topic}/{date}/{hour}` | Template for S3 key directory structure |
| `file_rotation` | String | `size` | Rotation strategy: `size` or `messages` |
| `max_file_size` | String | `8MiB` | Max file size before rotation (when `file_rotation = "size"`) |
| `max_messages_per_file` | Integer | `None` | Max messages per file (required when `file_rotation = "messages"`) |
| `output_format` | String | `json_lines` | Output format: `json_lines`, `json_array`, or `raw` |
| `include_metadata` | Boolean | `true` | Include stream/topic/partition/offset in output |
| `include_headers` | Boolean | `false` | Include message headers in output |
| `max_attempts` | Integer | `3` | Max total upload attempts per file (also accepts `max_retries` as alias) |
| `retry_delay` | String | `1s` | Base delay for exponential backoff (humantime format) |
| `path_style` | Boolean | auto | Force path-style S3 addressing; auto-enabled when `endpoint` is set |

### Path Template Variables

| Variable | Description | Example |
| -------- | ----------- | ------- |
| `{stream}` | Iggy stream name | `application_logs` |
| `{topic}` | Iggy topic name | `api_requests` |
| `{partition}` | Partition ID | `1` |
| `{date}` | UTC date from first message in buffer | `2026-03-16` |
| `{hour}` | UTC hour from first message in buffer | `14` |
| `{timestamp}` | Epoch millis derived from first message timestamp in buffer (deterministic) | `1710597600000` |

**Note:** `{timestamp}`, `{date}`, and `{hour}` are all derived from the first message timestamp in each buffer. They are deterministic across retries within the same process. However, a process restart resets in-memory buffers, so batch boundaries (and therefore timestamps in the key) may differ after recovery.

### Credentials

Credentials can be provided in three ways (in order of precedence):

1. **Explicit config**: Set both `access_key_id` and `secret_access_key`
2. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
3. **Instance profile / IAM role**: Automatic when running on EC2/ECS/EKS

Both `access_key_id` and `secret_access_key` must be provided together or both omitted.

## Output Example

With `output_format = "json_lines"` and `include_metadata = true`, writing `api_requests` messages produces:

```text
s3://my-data-lake/iggy/raw/application_logs/api_requests/2026-03-16/14/000000-000999.jsonl
```

Each line:

```json
{"offset":42,"timestamp":"2026-03-16T14:02:31Z","stream":"application_logs","topic":"api_requests","partition_id":1,"payload":{"method":"GET","path":"/api/users","status":200}}
```

## S3-Compatible Stores

### MinIO

```toml
[plugin_config]
bucket = "my-bucket"
region = "us-east-1"
endpoint = "http://localhost:9000"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
```

### Cloudflare R2

```toml
[plugin_config]
bucket = "my-bucket"
region = "auto"
endpoint = "https://<account-id>.r2.cloudflarestorage.com"
access_key_id = "..."
secret_access_key = "..."
```

## Delivery Semantics

All retry logic lives inside `consume()`. The connector runtime invokes `consume()` via an FFI callback that returns an `i32` status code. The runtime does not inspect this return value (see `process_messages()` in `runtime/src/sink.rs`), so errors logged by the sink are not propagated to the runtime's retry or alerting mechanisms. Additionally, consumer group offsets are committed before processing ([runtime issue #1](#known-limitations)). This means:

- Failed messages are **not retried by the runtime** — only by the sink's internal retry loop
- Messages are committed **before delivery** — a crash after commit but before delivery loses messages

The effective delivery guarantee is **at-most-once** at the runtime level. The sink's internal retries provide best-effort delivery within each `consume()` call.

## Known Limitations

1. **Runtime ignores `consume()` status**: The connector runtime invokes `consume()` via an FFI callback returning `i32`. The `process_messages()` function in `runtime/src/sink.rs` does not inspect the return value. Errors are logged internally by the sink but do not trigger runtime-level retry or alerting. ([#2927](https://github.com/apache/iggy/issues/2927))

2. **Offsets committed before processing**: The `PollingMessages` auto-commit strategy commits consumer group offsets before `consume()` is called. Combined with limitation 1, at-least-once delivery is not achievable. ([#2928](https://github.com/apache/iggy/issues/2928))

3. **In-memory buffering only**: There is no write-ahead log. A process crash loses all in-memory buffered messages that have not yet been flushed to S3.

4. **No dead letter queue**: Failed messages are logged at `error!` level but not persisted to a DLQ. DLQ support would be a runtime-level feature.

## Building

```bash
cargo build --release -p iggy_connector_s3_sink
```

The compiled plugin will be at `target/release/libiggy_connector_s3_sink.{so,dylib,dll}`.
