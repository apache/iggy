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

Build and start the matching 0.9.0/edge broker and connector runtime from the checkout root. Use the [sink guide](https://iggy.apache.org/docs/connectors/sinks/sink/) for broker credentials and the main runtime configuration. Save the following connector entry in its configured connector directory and append one `[plugin_config]` table. Create the bucket separately before startup.

```toml
type = "sink"
key = "s3"
enabled = true
version = 0
name = "S3 sink"
path = "target/release/libiggy_connector_s3_sink"
verbose = false
plugin_config_format = "toml"

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
| `prefix` | String | `None` | Prefix for data objects and loss markers; outer slashes are removed |
| `endpoint` | String | `None` | Custom S3 endpoint for MinIO, R2, etc. |
| `access_key_id` | String | `None` | AWS access key; omit for env/instance profile |
| `secret_access_key` | String | `None` | AWS secret key; omit for env/instance profile |
| `path_template` | String | `{stream}/{topic}/{date}/{hour}` | Template for S3 key directory structure |
| `file_rotation` | String | `size` | Rotation strategy: `size` or `messages` |
| `max_file_size` | String | `8MiB` | Size rotation threshold, positive and at most `5GiB`; validated in both modes |
| `max_messages_per_file` | Integer | `None` | Positive count required in `messages` mode; ignored by size rotation |
| `output_format` | String | `json_lines` | Output format: `json_lines`, `json_array`, or `raw` |
| `include_metadata` | Boolean | `true` | Include stream/topic/partition/offset/timestamp in JSON output |
| `include_headers` | Boolean | `false` | Include available headers in JSON output, independently of metadata |
| `max_attempts` | Integer | `3` | Outer upload attempts; `0` and `1` both allow one (alias: `max_retries`) |
| `retry_delay` | String | `1s` | Base exponential delay (humantime format); zero allowed, invalid values reject startup |
| `path_style` | Boolean | auto | Force path-style S3 addressing; auto-enabled when `endpoint` is set |

Each stream/topic/partition has its own buffer across polls. Rotation checks run after appending a record. Size mode counts formatted record bytes without JSON delimiters/brackets, so the threshold is not a hard object-size limit. Message-count mode does not also enforce the size threshold. There is no timer flush: partial buffers wait for the threshold or close. Uploads use one `PutObject`, not multipart upload, and must fit the destination's single-upload limit.

Format names are case-insensitive; `jsonl` and `jsonlines` alias `json_lines`.

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

Stream/topic substitutions keep ASCII letters, digits, `.`, `_` and `-`, replacing other characters with `_`. Ensure names remain distinct after sanitization and keep stream/topic separation in the template. Different inputs can otherwise overwrite the same object key.
The filename includes partition ID (at least five digits) and first/last offsets (twenty digits each), with `.jsonl`, `.json` or `.bin` extension. Bucket versioning can retain overwritten versions; offset naming is not a global deduplication guarantee.

### Credentials

The pinned `aws-creds` chain tries these sources in order:

1. **Explicit config**: both `access_key_id` and `secret_access_key`, or neither. This path has no session-token option.
2. **Environment**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN` / `AWS_SECURITY_TOKEN`.
3. **Credentials file**: `AWS_SHARED_CREDENTIALS_FILE` or `~/.aws/credentials`, using `[default]`. The plugin does not select `AWS_PROFILE`.
4. **STS web identity**: `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`.
5. **ECS relative-URI credentials**: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`.
6. **EC2 instance metadata**: IMDSv2, then v1.

For temporary key pairs, supply the token through the environment or shared credentials file. This is not the AWS SDK credential chain.

Startup writes an empty object at bucket-root `.iggy-sink-probe`, ignoring `prefix`, and then attempts deletion. It requires `PutObject` on the probe key, not `ListBucket`; non-success HTTP statuses prevent startup. Delete permission is optional and cleanup errors are ignored. Reserve this key: any existing object there is overwritten and, if cleanup succeeds, deleted.

## Output Example

With `output_format = "json_lines"` and `include_metadata = true`, writing `api_requests` messages produces:

```text
s3://my-data-lake/iggy/raw/application_logs/api_requests/2026-03-16/14/00001-00000000000000000000-00000000000000000999.jsonl
```

Each line:

```json
{"offset":42,"timestamp":"2026-03-16T14:02:31Z","stream":"application_logs","topic":"api_requests","partition_id":1,"payload":{"method":"GET","path":"/api/users","status":200}}
```

Both JSON formats wrap the payload under `payload`, including when metadata is disabled. JSON values retain their shape; text and Proto variants become strings. Valid JSON raw bytes are parsed; other raw bytes, Avro and FlatBuffer variants become base64 strings without an encoding tag.
Metadata timestamps use UTC second precision; IDs, checksums and origin timestamps are omitted. Headers are independent of metadata: strings remain strings, raw values become base64, booleans remain booleans, supported numeric values become JSON numbers, and other values use strings.

Raw output concatenates payload bytes without delimiters and ignores both inclusion flags. Use `schema = "raw"` without payload-changing transforms to preserve original bytes; JSON decoding followed by raw output can reserialize the payload.

## S3-Compatible Stores

Replace the earlier `[plugin_config]` table with one of these examples. Supply an existing bucket and credentials with the required permissions. The [website walkthrough](https://iggy.apache.org/docs/connectors/sinks/s3/) includes local MinIO setup and Iggy CLI commands.

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

The runtime auto-commits while polling, logs/counts a failed plugin callback, and continues without replaying that batch. A successful callback may only mean that messages were buffered; processed counts do not establish S3 delivery. A crash loses unflushed buffers.

The sink retries HTTP 408, 429, 5xx and client errors, using exponential backoff with jitter capped at 60 seconds. Other HTTP statuses stop that upload. `Retry-After` is not parsed. The S3 library also retries transport errors once internally and has a 60-second request timeout, so `max_attempts` counts outer `PutObject` calls, not individual network requests or a total time budget. The startup probe does not use the sink's upload retry loop.

## Known Limitations

1. **No runtime replay**: Auto-commit happens while polling, before the plugin finishes delivery. Callback failures are counted, but the runtime continues polling. There is no end-to-end delivery or deduplication guarantee.

2. **In-memory buffering only**: There is no write-ahead log or timer flush. A crash loses unflushed buffers. Buffers are cleared before upload; a failed upload loses that file's messages and skips the remainder of the current poll.

3. **Loss markers are best effort**: After an upload failure the sink tries to write `<object-key>.lost` with offset range, message count and error. The marker has no message payload, is not a dead-letter queue, and can also fail. Its upload uses the same retry policy.

4. **Close does not report flush failures**: Graceful close attempts remaining uploads, logs errors and returns success. Shutdown success does not establish that every message was stored.

## Building

```bash
cargo build --release -p iggy_connector_s3_sink
```

The Linux plugin is `target/release/libiggy_connector_s3_sink.so`. The runtime configuration uses the extensionless path shown above.
