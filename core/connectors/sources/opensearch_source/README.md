# OpenSearch Source Connector

Polls documents from an OpenSearch index and publishes each one to an Iggy
topic as a JSON message. The connector is read-only: it never writes to
OpenSearch.

Progress is tracked with `search_after` on `(timestamp_field, _id, _index)`.
The cursor is saved by the connectors runtime, so a restart resumes where the
last acknowledged batch ended.

## Configuration

Build the plugin from the repository root:

```bash
cargo build --release -p iggy_connector_opensearch_source
```

Then point the runtime at a config like this one (a ready copy lives in
`runtime/example_config/connectors/opensearch_source.toml`):

```toml
type = "source"
key = "opensearch"
enabled = true
version = 0
name = "OpenSearch source"
path = "target/release/libiggy_connector_opensearch_source"
plugin_config_format = "json"

[[streams]]
stream = "opensearch_stream"
topic = "documents"
schema = "json"
batch_length = 100
linger_time = "5ms"

[plugin_config]
url = "http://localhost:9200"
index = "logs-*"
polling_interval = "10s"
batch_size = 100
timestamp_field = "@timestamp"
query = { match_all = {} }
```

| Field | Default | Behavior |
| --- | --- | --- |
| `url` | required | OpenSearch base URL. |
| `index` | required | Index name or pattern. `open()` fails for a name that does not exist. A pattern that matches nothing yet is accepted, and the connector waits for matching indices to appear. |
| `timestamp_field` | required | Field used for sort order. It must be sortable in the index mapping. |
| `username` / `password` | none | HTTP basic authentication, used only when both are set. |
| `query` | `match_all` | Query DSL object, applied on every poll. |
| `polling_interval` | `10s` | Wait before each poll, so the first poll also waits. |
| `batch_size` | `100` | Documents per search, `1` to `10000` (the OpenSearch default `max_result_window`). |
| `request_timeout` | `30s` | Limit per HTTP request, from connecting to the last byte of the response. A timeout counts as a failed attempt and is retried. |
| `max_retries` | `3` | Total HTTP attempts per search, not the number of retries. |
| `retry_delay` | `1s` | Base delay for exponential backoff. |
| `retry_max_delay` | `30s` | Backoff cap. |
| `max_open_retries` | `5` | Total attempts for the index check in `open()`. |
| `open_retry_max_delay` | `30s` | Backoff cap during `open()`. |
| `circuit_breaker_threshold` | `5` | Consecutive failed polls before the breaker opens. |
| `circuit_breaker_cool_down` | `60s` | How long polls are skipped while the breaker is open. |
| `verbose_logging` | `false` | Log batch counts at `info` instead of `debug`. |

## How polling works

Each poll sleeps for `polling_interval`, then sends `POST /{index}/_search`
sorted ascending by `timestamp_field`, `_id` and `_index`, with `search_after`
set to the previous batch's last sort tuple. The `_id` and `_index` keys break
ties between documents with the same timestamp, so none are skipped at a batch
boundary, including across the indices of a pattern.

Documents that lack `timestamp_field` are sorted first (`missing: "_first"`).
OpenSearch would otherwise sort them last under a value larger than any real
timestamp, and the cursor stored there would hide every document indexed later.
They are published on the initial scan, but one indexed after the cursor has
moved on is skipped, like any document with an older timestamp. If a hit ever
comes back sorted on that last-place value, the poll fails with `SchemaMismatch`
instead of storing it.

For every hit:

- The `_source` becomes the JSON payload.
- The message `id` is a 128-bit BLAKE3 hash of `_index` and `_id`. It names the
  document, not a version of it: an update that is fetched again keeps the same
  id, so a consumer that drops repeated ids also drops the updated body. Read
  the id from the message instead of recomputing it.
- The headers `opensearch.document_id` and `opensearch.index` carry the raw
  values. A header over 255 bytes is dropped with a warning.
- `origin_timestamp` is taken from `timestamp_field` when it parses as an RFC
  3339 string or an integer (above `1e12` means milliseconds, otherwise
  seconds, so milliseconds before September 2001 are misread). Anything else
  leaves it unset and the document is still published.

Two cases do not produce a message. A hit without `_source` (for example an
index mapped with `"_source": false`) is skipped with a warning, and the cursor
moves past it so the poll does not repeat forever. A hit without a sort tuple
fails the poll with `SchemaMismatch` and the cursor stays put, since that means
the mapping cannot be paginated.

## Delivery and state

Delivery is at-least-once. `poll()` returns the batch together with a candidate
state. The runtime sends the batch, saves the state and reports the result:
the connector adopts the candidate on `Ack` and drops it on `Nack`, so a
rejected batch is fetched again. If the batch reached Iggy but the state save
failed, the batch is replayed as well. Consumers should dedupe on the message
`id`.

A page where every hit lacks `_source` publishes nothing, but its cursor still
moves, so it goes through the same `Ack` and `Nack` handling and is saved by the
runtime. A poll that returns no hits at all has no cursor to protect and is
counted directly.

The state holds the `search_after` cursor, `last_poll_timestamp` (informational
only), document and poll counters, the last error, and processing stats. A
corrupt saved state makes `open()` fail with `InitError` instead of silently
restarting from the first document.

With no saved state the connector starts at the beginning of the index and
republishes everything. If you lose the runtime state file, keep `state_path` on
durable storage, rely on the message `id` downstream, or bound the rescan with a
time-window `query`.

## Failures and retries

Network errors, HTTP `429` and `5xx` are retried with exponential backoff and
jitter. Other `4xx` responses and unparsable search responses fail immediately.
The delay is always computed locally, so a `Retry-After` header is ignored.

Searches are sent with `allow_partial_search_results=false`, and a response that
reports `timed_out` or failed shards is rejected before the cursor moves. A
partial page could omit documents that sort before its last hit, and the cursor
would skip them for good.

After the retry budget is spent the poll fails. Network errors and `429`/`5xx`
failures count toward the circuit breaker; permanent errors do not, because
pausing polls cannot fix a bad query or mapping and the error should stay
visible. While the breaker is open, `poll()` logs a warning, sleeps
`polling_interval` and returns an empty batch without touching the cursor. After
the cool-down, polls resume and the breaker reopens once the threshold is hit
again. A successful search resets it.

| Error | Raised when |
| --- | --- |
| `InitError` | Corrupt saved state, missing index or unreachable cluster in `open()`. |
| `InvalidConfigValue` | Invalid `url`, missing `timestamp_field`, `batch_size` outside `1..=10000`, or a `query` that is not an object. |
| `Connection` | Network failure during a search. |
| `HttpRequestFailed` | `429` or `5xx` after the retries are used up, or a search that timed out or failed on some shards. |
| `PermanentHttpError` | Other `4xx`, or a search response that cannot be parsed. |
| `SchemaMismatch` | A hit has no sort tuple, or sorted on the missing-value sentinel. |

## Limits

- One sequential reader with one cursor. There is no Scroll, point-in-time or
  sliced parallel export.
- Throughput is `batch_size` per `polling_interval`. The defaults give about 10
  documents per second, so a large first load needs tuning, for example
  `polling_interval = "100ms"` and `batch_size = 5000`.
- Use one mapping for `timestamp_field` across all indices of a pattern. A `date`
  index and a `date_nanos` index sort as raw numbers (milliseconds against
  nanoseconds), so the order across them is wrong and the cursor can skip
  documents. The sort does not set `numeric_type`, because OpenSearch rejects it
  on a `keyword` timestamp.
- The cursor only moves forward. Documents indexed later with a
  `timestamp_field` older than the cursor are not read until the state is reset.
- Only the whole `_source` is published, as JSON. There is no field projection.
- The client talks to the single `url` and does not discover other nodes.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `open()` fails on a missing index | Index name, URL and credentials. |
| `open()` fails with `state restore failed` | Repair or delete the runtime state file. |
| Nothing new after a restart | `timestamp_field` mapping must match the indexed documents. |
| Duplicate messages | Expected after a `Nack` or a failed state save. Dedupe on the message `id`. |
| Repeated warnings about `_source` | The index disables `_source`, so those hits are skipped. |
| Backfilled documents missing | Their timestamps are older than the cursor. Reset the state or adjust `query`. |
