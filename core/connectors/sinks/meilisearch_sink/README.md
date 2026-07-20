# Meilisearch Sink Connector

A sink connector that consumes messages from Iggy streams and writes them to a
Meilisearch index through the official Rust SDK.

## Configuration

```toml
[plugin_config]
url = "https://meilisearch.example.com"
index = "iggy_messages"
# api_key = "..."
primary_key = "iggy_id"
document_action = "replace"
create_index_if_not_exists = true
include_metadata = true
batch_size = 1000
timeout = "30s"
wait_for_tasks = true
task_timeout = "30s"
task_poll_interval = "100ms"
max_retries = 3
retry_delay = "500ms"
max_retry_delay = "5s"
max_open_retries = 5
```

- `url`: Meilisearch base URL. Paths, query strings, and fragments are ignored.
- `index`: Target index UID.
- `api_key`: Optional Meilisearch API key sent as `Authorization: Bearer`.
  Use HTTPS for non-local Meilisearch hosts; HTTP sends the key without
  transport encryption.
- `primary_key`: Index primary key field. Defaults to `iggy_id`.
- `document_action`: `replace` uses SDK add-or-replace semantics; `update`
  uses SDK add-or-update semantics. Defaults to `replace`.
- `create_index_if_not_exists`: Create the index during `open()` when missing. Defaults to `true`.
- `include_metadata`: Add Iggy metadata fields to each document. Defaults to `true`.
- `batch_size`: Maximum documents per Meilisearch document request. Defaults to `1000`.
- `timeout`: Request timeout as a humantime string, for example `30s`. Defaults to `30s`.
- `wait_for_tasks`: Poll Meilisearch tasks until terminal state before
  returning from `consume()`. Defaults to `true`. Setting this to `false` makes
  document indexing fire-and-forget, so asynchronous Meilisearch task failures
  are not observed by the connector.
- `task_timeout`: Maximum time to wait for each Meilisearch task. Defaults to `30s`.
- `task_poll_interval`: Delay between task polls. Defaults to `100ms`.
- `max_retries`: Maximum transient retries after the initial request. Defaults to `3`.
- `retry_delay`: Initial transient retry delay. Defaults to `500ms`.
- `max_retry_delay`: Maximum transient retry delay. Defaults to `5s`.
- `max_open_retries`: Maximum transient retries after the initial request while
  opening the index. Defaults to `5`. This also applies to `get_task` polls
  while waiting for index creation during `open()`. Each retried open operation
  uses the configured request `timeout` plus backoff; there is no single total
  open deadline.

## Behavior

JSON object payloads are indexed as documents. JSON arrays or scalar values are
wrapped in a `value` field because Meilisearch documents must be objects. Raw
payloads are parsed as JSON when possible; otherwise, they are indexed as base64
data. Text payloads are indexed in a `text` field. Unsupported payload schemas
are skipped with a warning and counted as sink errors, matching the connector
runtime's per-record drop behavior for malformed records. Because the sink
returns success after dropping an unsupported-schema record, the runtime can
commit the consumer offset for that record. There is no built-in dead-letter
queue for these drops.

When the configured primary key is absent, the connector injects a stable value
derived from the exact Iggy stream, topic, partition, offset, and message ID.
This avoids Meilisearch primary-key inference failures. If the payload already
contains the configured primary key, that value is preserved. Operators must
ensure user-provided primary keys are unique, otherwise Meilisearch
add-or-replace semantics can collapse distinct messages into one document.

When `include_metadata` is enabled, the connector writes reserved `iggy_*`
provenance fields after payload parsing. These fields overwrite same-named
payload fields so audit metadata reflects the actual stream, topic, partition,
offset, checksum, and timestamps. `iggy_checksum` is stored as a string to avoid
JSON number precision loss in Meilisearch clients. Offset and timestamp metadata
remain JSON numbers. If `primary_key` is set to a field other than `iggy_id`,
the connector also writes `iggy_id` as stable Iggy metadata.

## Delivery Semantics

The connector runtime invokes `consume()` through an FFI callback whose status
code is not currently used to gate offset commits. A batch error returned by the
sink is logged by the sink, but the runtime does not redeliver that batch. The
effective runtime-level delivery guarantee is at-most-once on sink errors. The
sink's retry settings only provide best-effort retries inside a single
`consume()` call.

`wait_for_tasks=false` only skips waiting for document indexing tasks during
`consume()`. In that mode, successful submission lets the runtime commit the
consumer offset before Meilisearch has confirmed indexing, so later task
failures are not retried, logged, or counted by this connector. If
`create_index_if_not_exists=true` and the connector creates the index during
`open()`, it still waits for that index-creation task so the first batch cannot
race the index creation. This mode is fire-and-forget and does not provide
durability.

The close-time counters are attempt counters. `documents_enqueued` counts
documents accepted by completed Meilisearch SDK calls in this process, and
`documents_confirmed` counts those same documents only when task waiting is
enabled and the corresponding task reached success. `errors` includes invalid
records plus documents in failed chunks and trailing chunks that were not
attempted after an earlier chunk failed.
