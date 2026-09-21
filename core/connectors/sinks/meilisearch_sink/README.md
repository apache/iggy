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
- `primary_key`: Index primary key field. Defaults to `iggy_id`. Match the
  existing index key: a mismatch only warns at startup and can fail writes.
- `document_action`: `replace` uses SDK add-or-replace semantics; `update`
  uses SDK add-or-update semantics. Defaults to `replace`.
- `create_index_if_not_exists`: Create the index during `open()` when missing. Defaults to `true`.
- `include_metadata`: Add Iggy metadata fields to each document. Defaults to `true`.
- `batch_size`: Maximum documents per Meilisearch document request. Defaults to `1000`.
- `timeout`: Total deadline for one SDK operation including retries and backoff.
  Health checks apply it separately to each attempt. Defaults to `30s`.
- `wait_for_tasks`: Wait for indexing success or failure, bounded by `task_timeout`, before
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
  uses one `timeout` deadline including backoff; there is no single total
  open deadline.

## Behavior

JSON object payloads are indexed as documents. JSON arrays or scalar values are
wrapped in a `value` field because Meilisearch documents must be objects. Raw
payloads are parsed as JSON when possible; otherwise, they are indexed as base64
data. Text payloads are indexed in a `text` field. Unsupported payload schemas
are skipped with a warning and counted in the plugin's private error counter.
The callback returns success after these drops, so runtime statistics can count
those records as processed. Offsets are auto-committed while polling, before
indexing completes. There is no built-in dead-letter queue for these drops.

When the configured primary key is absent, the connector injects a stable value
derived from the exact Iggy stream, topic, partition, offset, and message ID.
This avoids Meilisearch primary-key inference failures. If the payload already
contains the configured primary key, that value is preserved unless a reserved
metadata field below overwrites it. A present but invalid or null key is not
replaced automatically. Operators must
ensure user-provided primary keys are unique, otherwise Meilisearch
add-or-replace semantics can collapse distinct messages into one document.

When `include_metadata` is enabled, the connector writes reserved `iggy_*`
provenance fields after payload parsing. These fields overwrite same-named
payload fields so audit metadata reflects the actual stream, topic, partition,
offset, checksum, and timestamps. `iggy_checksum` is stored as a string to avoid
JSON number precision loss in Meilisearch clients. Offset and timestamp metadata
remain JSON numbers. If `primary_key` is set to a field other than `iggy_id`,
the connector also writes `iggy_id` as stable Iggy metadata. A supplied `iggy_id`
is preserved when it is the configured primary key; avoid other reserved
metadata fields as a primary key because metadata overwrites them. Message
and origin timestamps are microseconds; `iggy_ingested_at` uses wall-clock
milliseconds. Disabling metadata leaves payload fields intact and still injects
a missing primary key.

## Delivery Semantics

The connector runtime invokes `consume()` through an FFI callback whose status
code is not currently used to gate offset commits. A batch error returned by the
sink is logged by the sink, but the runtime does not redeliver that batch. The
runtime records a plugin error and continues polling. There is no end-to-end
at-least-once guarantee; retries or manual replay after uncertain task outcomes
can repeat writes. The sink retries transient submission/status requests within
a single `consume()` call. Failed tasks are not resubmitted, and task timeouts
do not cancel remote tasks.

`wait_for_tasks=false` only skips waiting for document indexing tasks during
`consume()`. In that mode, submission succeeds before Meilisearch confirms
indexing, while offsets have already been auto-committed during polling, so later task
failures are not retried, logged, or counted by this connector. If
`create_index_if_not_exists=true` and the connector creates the index during
`open()`, it still waits for that index-creation task so the first batch cannot
race the index creation. This mode is fire-and-forget and does not provide
durability. Closing does not await outstanding document tasks.

Each polled batch is chunked immediately, including its final partial chunk;
there is no accumulation across polls. `batch_size = 0` behaves as `1`. A failed
chunk stops the loop, so earlier chunks may be stored while later chunks are
never attempted.

The close-time counters cover this process. `documents_enqueued` counts
successfully handled chunks: submission must succeed and, when task waiting
is enabled, the task must also succeed. `documents_confirmed` counts those same
documents only with task waiting enabled. Submitted tasks that fail or time out
are excluded from both success counters, even if a timed-out task later succeeds. `errors` includes invalid
records plus documents in failed chunks and trailing chunks that were not
attempted after an earlier chunk failed.
