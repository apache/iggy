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
poll_interval = "100ms"
consumer_group = "opendal_sink"

[plugin_config]
service = "fs"
path_prefix = "archive"
path_template = "{stream}/{topic}/{date}/{hour}"
max_attempts = 3
retry_delay = "1s"
verbose_logging = false

[plugin_config.options]
root = "/var/lib/iggy-objects"
```

### Plugin options

| Option            | Type     | Default                          | Description                            |
| ----------------- | -------- | -------------------------------- | -------------------------------------- |
| `service`         | String   | **required**                     | OpenDAL service name                   |
| `path_prefix`     | String   | empty                            | Prefix before the rendered object path |
| `path_template`   | String   | `{stream}/{topic}/{date}/{hour}` | Object directory template              |
| `options`         | Map      | empty                            | OpenDAL service options.               |
| `max_attempts`    | Integer  | `3`                              | Total write attempts.                  |
| `retry_delay`     | Duration | `1s`                             | Delay before the first retry           |
| `verbose_logging` | Boolean  | `false`                          | Log each consumed batch at info level  |

### Default services

The sink's default crate feature enables these OpenDAL services:

- Azure Blob Storage
- Filesystem
- Google Cloud Storage
- Amazon S3 and compatible stores

OpenDAL's in-memory service is always available and does not require a service feature.

Use `--no-default-features` when building to disable the four default services.

### Additional services

When building the sink directly, enable another OpenDAL service through Cargo's feature syntax. For example, this command builds the sink with CompFS and without the default services:

```bash
cargo build --release -p iggy_connector_opendal_sink \
    --no-default-features \
    --features opendal/services-compfs
```

When using the sink as a dependency, add `opendal` as a direct dependency and enable additional features:

```toml
iggy_connector_opendal_sink = { version = "0.5.0-edge.4", default-features = false }
opendal = { version = "0.59.1", default-features = false, features = ["services-compfs"] }
```

The direct OpenDAL dependency must use a version compatible with the sink. See the [complete list of OpenDAL service features](https://github.com/apache/opendal/blob/main/core/Cargo.toml).

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

Each object path contains an optional prefix, the rendered path template, and a per-message filename:

```text
{path_prefix}/{rendered_path_template}/{partition_id:05}-{offset:020}.{extension}
```

The default `{stream}/{topic}/{date}/{hour}` template produces paths such as:

```text
archive/events/orders/2024-03-16/14/00007-00000000000000000042.json
```

A custom `path_template` replaces the default template part. The `{partition}` variable can appear in that template, but the partition ID and offset are always present in the filename.

### Path template variables

| Variable      | Value                                                    |
| ------------- | -------------------------------------------------------- |
| `{stream}`    | Stream name                                              |
| `{topic}`     | Topic name                                               |
| `{partition}` | Partition ID without padding                             |
| `{date}`      | Message timestamp date in UTC, formatted as `YYYY-MM-DD` |
| `{hour}`      | Message timestamp hour in UTC, formatted as `00` to `23` |
| `{timestamp}` | Message timestamp as Unix milliseconds                   |

File extensions are `json`, `txt`, `bin`, `proto`, `flatbuffer`, and `avro`.

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
