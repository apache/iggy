# BigQuery Sink Connector

The BigQuery sink connector consumes messages from Iggy topics and writes them into a Google BigQuery table through the [Storage Write API](https://cloud.google.com/bigquery/docs/write-api) `_default` stream, with rows encoded as Arrow record batches.

## Features

- **Storage Write API**: gRPC `AppendRows` on the table's `_default` stream. No legacy `insertAll`.
- **Two modes**: `mapped` maps top-level JSON fields to table columns by name. `raw` stores the whole payload in one JSON, STRING or BYTES column.
- **Schema validation at startup**: the table schema is read with `tables.get` and checked before any data flows. Unsupported column types fail `open()`.
- **Iggy metadata columns**: stream, topic, partition, offset, timestamp and message id, plus optional headers, for lineage and deduplication.
- **Per-row error handling**: a row that cannot be written is dropped and logged with its offset. The rest of the batch is still written.
- **Request sizing**: batches are split below the 10 MB `AppendRows` limit.
- **Retries**: transient gRPC failures are retried with jittered exponential backoff.

## Configuration

```toml
type = "sink"
key = "bigquery"
enabled = true
version = 0
name = "BigQuery sink"
path = "target/release/libiggy_connector_bigquery_sink"

[[streams]]
stream = "example_stream"
topics = ["example_topic"]
schema = "json"
batch_length = 1000
poll_interval = "5ms"
consumer_group = "bigquery_sink_connector"

[plugin_config]
project_id = "my-project"
dataset = "iggy"
table = "events"
mode = "mapped"
credentials_path = "/secrets/bigquery-writer.json"
include_metadata = true
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `project_id` | string | required | Google Cloud project that owns the dataset |
| `dataset` | string | required | BigQuery dataset |
| `table` | string | required | Target table. It must already exist |
| `mode` | string | `"mapped"` | `mapped` or `raw` |
| `payload_column` | string | `"payload"` | Payload column in `raw` mode |
| `credentials_path` | string | none | Service account key file |
| `credentials_json` | string | none | Inline service account key. Mutually exclusive with `credentials_path` |
| `include_metadata` | bool | `true` | Write the `iggy_*` metadata columns listed below |
| `include_headers` | bool | `false` | Write message headers into `iggy_headers` |
| `missing_value` | string | `"default"` | How BigQuery fills columns no row in a request sets: `default` or `null` |
| `max_request_bytes` | usize | `8388608` | Upper bound for one `AppendRows` request, clamped to 64 KiB..9 MiB |
| `max_retries` | u32 | `3` | Total attempts per call, including the first |
| `retry_delay` | string | `"1s"` | Base retry delay |
| `max_retry_delay` | string | `"30s"` | Upper bound for one retry delay |
| `timeout` | string | `"30s"` | Timeout for `tables.get` and each `AppendRows` call |
| `verbose_logging` | bool | `false` | Log each batch at info level instead of debug |
| `endpoint` | string | none | REST endpoint override, for local fakes and emulators. Must be set with `grpc_endpoint`; no credentials are used |
| `grpc_endpoint` | string | none | Storage Write API `host:port` override |

## Authentication

The connector picks the first source that applies:

1. `endpoint` and `grpc_endpoint`: no credentials, plain-text connections. For tests only.
2. `credentials_path`: a service account key file.
3. `credentials_json`: an inline service account key.
4. Application Default Credentials: `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default login`, or the metadata server on GCE, GKE and Cloud Run.

```toml
# Key file
credentials_path = "/secrets/bigquery-writer.json"

# Inline key, for example injected through an environment override:
# IGGY_CONNECTORS_SINK_BIGQUERY_PLUGIN_CONFIG_CREDENTIALS_JSON='{"type":"service_account",...}'
```

### IAM permissions

The identity needs `bigquery.tables.get` (read the schema) and `bigquery.tables.updateData` (append rows) on the target table. The predefined role `roles/bigquery.dataEditor`, granted on the table or its dataset, includes both.

## Modes

### `mapped` (default)

Each payload must be a JSON object. Top-level fields map to table columns by name. Fields that match no column are ignored.

- **Payload types**: `Json`, `Text` and `Proto` text are accepted when they hold a JSON object. `Raw`, `FlatBuffer` and `Avro` payloads are rejected.
- **Missing columns**: a column that no row in a request sets is left out of the request, and BigQuery fills it according to `missing_value`. A REQUIRED column without a default value must be present in every row.
- **Defaults apply per request, not per row**: Arrow has no "unset" marker, so once one row in a request sets a column, the other rows send NULL for it. For a REQUIRED column with a default, rows that omit it while other rows in the same batch set it are rejected. Set such columns in every message or in none.
- **REPEATED** columns that are absent or `null` are written as empty arrays.

### `raw`

The payload is stored in `payload_column`. Every other table column is left to its default, so a REQUIRED column without a default fails `open()`.

| Payload column type | Accepted payloads |
| ------------------- | ----------------- |
| `JSON` | `Json`, plus `Text`, `Proto` and UTF-8 `Raw` payloads that parse as JSON |
| `STRING` | `Json` (serialized), `Text`, `Proto`, UTF-8 `Raw` |
| `BYTES` | every payload type, as its raw bytes |

## Metadata Columns

With `include_metadata = true` the table must contain these columns. A missing column fails `open()` with the DDL to add it.

| Column | BigQuery type | Value |
| ------ | ------------- | ----- |
| `iggy_stream` | `STRING` | Stream name |
| `iggy_topic` | `STRING` | Topic name |
| `iggy_partition_id` | `INT64` | Partition id |
| `iggy_offset` | `INT64` | Message offset |
| `iggy_timestamp` | `TIMESTAMP` | Message timestamp |
| `iggy_id` | `STRING` | Message id, as a decimal string (it is a u128) |
| `iggy_headers` | `JSON` or `STRING` | Headers, only with `include_headers = true`. Binary values are written as `{"data": "<base64>", "iggy_header_encoding": "base64"}` |

## Type Mapping

| BigQuery | Accepted JSON values |
| -------- | -------------------- |
| `STRING`, `GEOGRAPHY` (WKT) | string |
| `BYTES` | base64 string |
| `INT64` | integer, or a string holding one |
| `FLOAT64` | number, or a string holding one |
| `BOOL` | `true` / `false` |
| `NUMERIC`, `BIGNUMERIC` | number or string. Prefer strings to avoid float rounding |
| `TIMESTAMP` | RFC 3339 string, or an integer in microseconds since the epoch |
| `DATETIME` | `YYYY-MM-DDTHH:MM:SS[.ffffff]` string |
| `DATE` | `YYYY-MM-DD` string |
| `TIME` | `HH:MM:SS[.ffffff]` string |
| `JSON` | any JSON value, stored as that value |
| `RECORD` | object |
| `REPEATED` | array |

`INTERVAL` and `RANGE` columns are not supported and fail `open()`.

## Reliability

### Errors and retries

- `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, `ABORTED` and `RESOURCE_EXHAUSTED` are retried up to `max_retries` attempts in total.
- Every other gRPC error, including `INVALID_ARGUMENT`, `PERMISSION_DENIED` and `UNAUTHENTICATED`, fails the request without a retry.
- `tables.get` retries HTTP 429, 5xx and transport errors. A 403 or 404 fails `open()` immediately.

### Bad rows

A message that cannot become a row (wrong payload type, a value that does not fit its column, a missing REQUIRED value, a row larger than `max_request_bytes`) is dropped before the request is sent. When BigQuery reports row-level errors, it appends nothing from that request: the connector drops the reported rows and appends the remaining rows once more. If that second append also reports row errors, the remaining rows of that request are dropped and the batch returns an error.

Every dropped row is logged at `warn` with its stream, topic, partition and offset. There is no dead-letter queue. On shutdown the connector logs rows written, rejected and failed.

### Delivery semantics

The connector runtime commits consumer group offsets when messages are polled, before `consume()` runs, and a batch for which `consume()` returns an error is not redelivered. As a result:

- A batch that still fails after the last retry, or hits a permanent error, is lost.
- A process crash between the offset commit and a successful append also loses that batch.
- The `_default` stream has no append offsets. When an append result is lost and the request is retried, rows can be written twice. Deduplicate downstream on `iggy_stream`, `iggy_topic`, `iggy_partition_id` and `iggy_offset`, for example:

```sql
SELECT * EXCEPT(rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY iggy_stream, iggy_topic, iggy_partition_id, iggy_offset
    ORDER BY iggy_timestamp
  ) AS rn
  FROM `my-project.iggy.events`
) WHERE rn = 1
```

### Schema changes

The schema is read once in `open()`. Restart the connector after changing the table.

## Testing

Unit tests cover configuration, credential source selection, schema mapping, Arrow encoding, request splitting and error classification.

`tests/bigquery_sink.rs` runs the sink against an in-process fake (`tests/common/mod.rs`): an axum `tables.get` endpoint and a tonic `BigQueryWrite` service that records every `AppendRows` request as an Arrow batch and answers from a script. It covers successful writes, request splitting, local and BigQuery-reported row errors, retryable and permanent failures, and `open()` failures. No Google Cloud access is needed.

The fake cannot check what only BigQuery knows: that it accepts the Arrow encoding, its type coercion, and real credentials. Verify those manually as follows.

### Manual verification against BigQuery

1. Create a dataset and table (requires `gcloud` and `bq`):

    ```bash
    export PROJECT=my-project
    gcloud auth application-default login
    gcloud services enable bigquery.googleapis.com bigquerystorage.googleapis.com --project "$PROJECT"
    bq --project_id="$PROJECT" mk --dataset iggy
    bq --project_id="$PROJECT" query --use_legacy_sql=false "
    CREATE TABLE iggy.events (
      user_id INT64 NOT NULL,
      event STRING,
      amount NUMERIC,
      created TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
      attrs JSON,
      iggy_stream STRING,
      iggy_topic STRING,
      iggy_partition_id INT64,
      iggy_offset INT64,
      iggy_timestamp TIMESTAMP,
      iggy_id STRING
    )"
    ```

2. Build the binaries and the plugin, and write a runtime config that loads only this connector:

    ```bash
    cargo build --bin iggy-server --bin iggy --bin iggy-connectors
    cargo build -p iggy_connector_bigquery_sink
    mkdir -p /tmp/bq-sink-test/connectors
    sed 's#^config_dir = .*#config_dir = "/tmp/bq-sink-test/connectors"#' \
      core/connectors/runtime/example_config/config.toml > /tmp/bq-sink-test/config.toml
    cat > /tmp/bq-sink-test/connectors/bigquery.toml <<EOF
    type = "sink"
    key = "bigquery"
    enabled = true
    version = 0
    name = "BigQuery sink"
    path = "$PWD/target/debug/libiggy_connector_bigquery_sink"

    [[streams]]
    stream = "demo_stream"
    topics = ["demo_topic"]
    schema = "json"
    batch_length = 100
    poll_interval = "100ms"
    consumer_group = "bigquery_sink_test"

    [plugin_config]
    project_id = "$PROJECT"
    dataset = "iggy"
    table = "events"
    verbose_logging = true
    EOF
    ```

3. Start the server, create the stream, then start the runtime:

    ```bash
    ./target/debug/iggy-server                                   # terminal 1
    ./target/debug/iggy -u iggy -p iggy stream create demo_stream   # terminal 2
    ./target/debug/iggy -u iggy -p iggy topic create demo_stream demo_topic 1 none
    IGGY_CONNECTORS_CONFIG_PATH=/tmp/bq-sink-test/config.toml ./target/debug/iggy-connectors
    ```

4. Produce a few messages, including one bad row (terminal 3):

    ```bash
    ./target/debug/iggy -u iggy -p iggy message send demo_stream demo_topic '{"user_id": 1, "event": "signup", "amount": "9.99", "attrs": {"plan": "pro"}}'
    ./target/debug/iggy -u iggy -p iggy message send demo_stream demo_topic '{"event": "no user id"}'
    ```

5. Check the rows, and look for the `dropped message` warning for the second message in the runtime log:

    ```bash
    bq --project_id="$PROJECT" query --use_legacy_sql=false \
      "SELECT user_id, event, amount, created, attrs, iggy_offset FROM iggy.events ORDER BY iggy_offset"
    ```
