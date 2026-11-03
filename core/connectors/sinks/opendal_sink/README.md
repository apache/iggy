# Apache Iggy OpenDAL Sink Connector

Writes Iggy messages to storage services supported by Apache OpenDAL.

## Configuration

```toml
type = "sink"
key = "opendal"
enabled = true
version = 0
name = "OpenDAL sink"
path = "../../target/release/libiggy_connector_opendal_sink"
plugin_config_format = "toml"
verbose = false
benchmark = false

[[streams]]
stream = "events"
topics = ["orders"]
schema = "json"
batch_length = 100
consumer_group = "opendal_sink"

[plugin_config]
service = "fs"
path_prefix = "archive"
path_template = "{stream}/{topic}/{date}/{hour}"
output_format = "json_lines"
include_metadata = true
include_headers = false
max_attempts = 3
retry_delay = "1s"
max_file_size = "8MiB"
write_timeout = "60s"
if_not_exists = false
verbose_logging = false

[plugin_config.options]
root = "/var/lib/iggy-objects"
```

### Plugin options

| Option             | Type     | Default                          | Description                                                                            |
| ------------------ | -------- | -------------------------------- | -------------------------------------------------------------------------------------- |
| `service`          | String   | **required**                     | OpenDAL service name                                                                   |
| `path_prefix`      | String   | empty                            | Prefix before the rendered object path                                                 |
| `path_template`    | String   | `{stream}/{topic}/{date}/{hour}` | Object directory template                                                              |
| `options`          | Map      | empty                            | Secret OpenDAL options as quoted TOML strings; values of other types will fail to load |
| `output_format`    | String   | `json_lines`                     | `json_lines` (also accepts `jsonl` or `jsonlines`), `json_array`, or `raw`             |
| `include_metadata` | Boolean  | `true`                           | Add message metadata to JSON output                                                    |
| `include_headers`  | Boolean  | `false`                          | Add message headers to JSON output                                                     |
| `max_attempts`     | Integer  | `3`                              | Total write attempts (`max_retries` alias)                                             |
| `retry_delay`      | Duration | `1s`                             | Delay before the first retry                                                           |
| `max_file_size`    | Size     | `8MiB`                           | Maximum object size, including JSON framing                                            |
| `write_timeout`    | Duration | `60s`                            | Base timeout for each write attempt                                                    |
| `if_not_exists`    | Boolean  | `false`                          | Reject writes that would replace an existing object                                    |
| `verbose_logging`  | Boolean  | `false`                          | Log each consumed batch at info level                                                  |

### Filesystem atomic writes

The `fs` service (also available as `file`) writes to a temporary file, then
renames it to the final path. By default, temporary files go in
`<root>/.iggy-opendal-tmp`. Use `plugin_config.options.atomic_write_dir` to
choose another directory on the same filesystem as `root`.

Set `atomic_write_dir = ""` to disable atomic writes. Readers can then see
partial files if a write fails.

After a failed write, the sink tries to remove the temporary file by aborting
the writer, with a 5-second timeout. A timeout during writer creation or a
failed abort can leave temporary files that you need to remove manually.

OpenDAL syncs the file before renaming it, but does not sync the parent
directory. A power failure can still lose a file after a successful write.

### Default services

The sink's default crate feature enables these OpenDAL services:

- Azure Blob Storage
- Filesystem
- Google Cloud Storage
- Amazon S3 and compatible stores

The `memory` service is not supported because it loses all data when the
operator stops.

Use `--no-default-features` when building to disable the four default services.

### Additional services

When building the sink directly, enable another OpenDAL service through Cargo's
feature syntax. For example, this command builds the sink with CompFS and
without the default services:

```bash
cargo build --release -p iggy_connector_opendal_sink \
    --no-default-features \
    --features opendal/services-compfs
```

See the [complete list of OpenDAL service features][opendal-service-features]
for other values accepted by `--features`.

[opendal-service-features]: https://github.com/apache/opendal/blob/main/core/Cargo.toml

### S3 example

```toml
[plugin_config]
service = "s3"
path_prefix = "archive"

# OpenDAL-specific options go into plugin_config.options
[plugin_config.options]
bucket = "iggy-events"
region = "us-east-1"
access_key_id = "..."
secret_access_key = "..."
# endpoint = "http://localhost:9000"
```

### Object layout

Each object path contains an optional prefix, the rendered path template, and a
batch filename:

```text
{path_prefix}/{rendered_path_template}/{partition_id:05}-{offset_start:020}-{offset_end:020}.{extension}
```

The default `{stream}/{topic}/{date}/{hour}` template produces paths such as:

```text
archive/events/orders/2024-03-16/14/00007-00000000000000000042-00000000000000000141.jsonl
```

A custom `path_template` must contain both `{stream}` and `{topic}` to keep
different streams and topics separate. The `{partition}` variable is optional.
The partition ID and offset range are always present in the filename.
Stream and topic template values use percent encoding.

Output formats:

- `json_lines` writes one JSON object per line and uses `.jsonl`.
- `json_array` writes one JSON array and uses `.json`.
- `raw` concatenates payload bytes without separators and uses `.bin`.

JSON output includes message metadata by default. Set `include_metadata = false`
to write only the `payload` field. Set `include_headers = true` to include
message headers.

Headers are a JSON object. String keys keep their original names and must
contain valid UTF-8. Other key types use their text form, without a type prefix.
If different typed keys produce the same JSON name, such as String `"1"` and
Int8 `1`, the sink rejects that record with a serialization error instead of
writing duplicate names or overwriting a header.

In JSON output, `Raw`, `FlatBuffer`, and `Avro` payloads become Base64 strings,
even if the bytes contain valid JSON. This preserves the original bytes and
keeps each JSON Lines record on one line. `Json` payloads stay as structured
JSON. `Proto` payloads also stay as structured JSON if they contain a JSON
document; otherwise, they become strings, as do `Text` payloads.

### Object limits and write timeouts

Each batch is written without keeping a buffer between `consume()` calls.
`max_file_size` limits the size of each object, including JSON brackets,
commas, and line endings. Larger batches are split at message boundaries.
The default is 8 MiB, with values from 1 byte to 5000 MiB allowed.

If a single message is too large, the sink rejects it, counts it as failed,
and continues with the rest of the batch. `batch_length` limits how many
messages arrive in a batch, not their total size in bytes.

`write_timeout` covers writer creation, writing, and closing. The sink adds
one second per MiB of formatted data, so an 8 MiB object gets 68 seconds with
the default 60-second timeout. Failed or timed-out writers are aborted before
a retry. `max_attempts` allows 1 to 3 attempts, including the first write.
All attempts use the same object key, so retries do not create duplicate objects.

Delivery is at-most-once. The runtime commits consumer offsets when it polls
messages, before the sink writes them. If all write attempts fail, the batch
is not replayed. This can leave permanent gaps in the archive.

### Startup access check

At startup, the sink writes and deletes a small object with a unique name
under `path_prefix`. If either operation fails, the sink will not start.
With `if_not_exists = true`, this also checks conditional write support.

If input validation fails, the sink keeps the raw configuration. A later
`open()` checks the same input again and returns the same error.

Create a new sink after `close()` or a startup failure after validation.
If the sink has no state, `open()` returns
`OpenDAL sink was closed or failed to initialize`.

### Path template variables

| Variable      | Value                                                    |
| ------------- | -------------------------------------------------------- |
| `{stream}`    | Stream name                                              |
| `{topic}`     | Topic name                                               |
| `{partition}` | Partition ID without padding                             |
| `{date}`      | Message timestamp date in UTC, formatted as `YYYY-MM-DD` |
| `{hour}`      | Message timestamp hour in UTC, formatted as `00` to `23` |
| `{timestamp}` | Message timestamp as Unix milliseconds                   |

File extensions are selected by `output_format`: `jsonl`, `json`, or `bin`.

## Building

Build the sink with its default services:

```bash
cargo build --release -p iggy_connector_opendal_sink
```

Build without the default services:

```bash
cargo build --release -p iggy_connector_opendal_sink --no-default-features
```

The plugin is created at `target/release/libiggy_connector_opendal_sink.{so,dylib,dll}`.
