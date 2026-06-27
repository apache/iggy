# Apache Doris Sink

The Doris sink connector consumes JSON messages from Iggy streams and writes them to a pre-created Apache Doris table via Doris's [Stream Load HTTP API](https://doris.apache.org/docs/data-operate/import/import-way/stream-load-manual).

## Requirements

- The target Doris **database and table must be pre-created** before enabling the sink. The connector never issues DDL.
- `database` and `table` config values must match `[A-Za-z0-9_]+`. Anything else is rejected at startup with `Error::InvalidConfigValue` — this also prevents path traversal in the constructed `/api/{db}/{table}/_stream_load` URL.
- Messages must arrive with `Payload::Json` (i.e. the configured stream schema is `json`). If a non-JSON payload reaches the connector it logs at `error!` and aborts the whole poll; since the consumer offset is already committed at poll time the batch is not replayed — effectively silent data loss — so the upstream schema must be guaranteed JSON. (Under `schema = "json"` the SDK drops non-JSON before the connector sees it, so this abort is a defensive guard.)
- The Iggy message JSON shape must match the target table columns. Use the optional `columns` plugin setting if the column order differs from the JSON keys.

## How it works

1. For each batch of messages, the connector serializes the JSON payloads into the configured output format: a JSON array by default, or CSV when `output_format = "csv"` (see Configuration).
2. It computes a deterministic Stream Load `label` of the form `{label_prefix}-{stream_san}-{topic_san}-{hash16}-{partition}-{first_offset}-{last_offset}`.
   - `hash16` is a single 64-bit blake3 hash computed over the *raw* (un-sanitized), length-prefixed `(label_prefix, stream, topic)` triple. So identities that sanitize to the same string get distinct labels — whether the collision is in the names (`events.v1` vs `events_v1`) or in two tenants' prefixes that truncate alike (`prod_events_us_east_1` vs `..._2`) — and no boundary-shift aliasing is possible (`("ab","c")` ≠ `("a","bc")`).
   - The total label is bounded under Doris's 128-char cap regardless of input length (worst case 120 chars).
   - Doris dedupes loads by label inside its `label_keep_max_second` window. The in-request retry (step 6) re-PUTs a transiently-failed batch under the same label, so a prior attempt that actually landed (e.g. a `2xx` whose body we couldn't read) is absorbed, not doubled. This protects **in-request retry only**: the runtime commits the offset before `consume()` runs and discards its return, so a failure outliving the retry budget or a crash mid-load is **at-most-once**.
3. It `PUT`s the batch to `{fe_url}/api/{database}/{table}/_stream_load` with HTTP Basic auth, `Expect: 100-continue`, `label: <label>`, and the format headers (`format: json` + `strip_outer_array: true` for JSON; `format: csv` with control-char `column_separator`/`line_delimiter` + `enclose`/`escape` for CSV). `Expect: 100-continue` is required by Doris's Stream Load endpoint, which rejects PUTs that omit it; it also lets Doris reject auth/4xx before the body uploads.
4. The Doris frontend (FE) responds with a `307 Temporary Redirect` to a backend (BE). The connector follows the redirect manually so that the `Authorization` header is preserved across the hop (`reqwest`'s default policy strips it on cross-host redirects).
   `308 Permanent Redirect` is also followed as a defensive measure; redirects beyond a hard cap of 5 (or a redirect with no usable `Location`) are rejected as a permanent `PermanentHttpError`, since retrying a malformed/looping redirect cannot help.
5. The HTTP body is parsed as JSON and the `Status` field decides the outcome:
   - `Success` → batch accepted.
   - `Label Already Exists` → idempotent replay, treated as success.
   - `Publish Timeout` or HTTP `5xx`/`408`/`429` → transient error (`Error::CannotStoreData`): retried in-request up to `max_retries` attempts (exponential backoff + jitter) under the same label before being surfaced.
   - `Fail`, any other `4xx`, or an unparsable response body → permanent error (`Error::PermanentHttpError`); never retried — re-PUTing bad data would just hammer the FE.
6. A *transient* failure (the classifications above, plus a transport-level error) is retried in-request: the same batch is re-`PUT` under the same label, up to `max_retries` attempts with backoff and ±20% jitter (`iggy_connector_sdk::retry`). Since the runtime commits the offset at poll time, this is the connector's only redelivery path; once the budget is exhausted the worst error is surfaced and the batch is not retried again — **at-most-once** across polls.

## Configuration

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `fe_url` | yes | — | Doris frontend HTTP base URL, e.g. `http://localhost:8030`. |
| `database` | yes | — | Target database. Must match `[A-Za-z0-9_]+`. |
| `table` | yes | — | Target table. Must match `[A-Za-z0-9_]+`. |
| `username` | yes | — | Doris user with `LOAD_PRIV` on the table. |
| `password` | yes | — | Doris user password. Stored as a `secrecy::SecretString` and never logged. |
| `label_prefix` | no | `iggy` | Prefix for the deterministic Stream Load label. |
| `batch_size` | no | `1000` | Maximum number of messages per Stream Load request. |
| `timeout` | no | `30s` | Per-request HTTP timeout (total request budget), as a human-readable duration (e.g. `30s`, `1m`). |
| `connect_timeout` | no | `5s` | TCP connect timeout, independent of `timeout`, as a human-readable duration. Raise it for cross-region or cold-start FEs. |
| `max_retries` | no | `3` | Total Stream Load attempts per batch on a *transient* failure (`1` disables retries). Each retry re-PUTs under the same label, which Doris dedupes. |
| `retry_delay` | no | `200ms` | Base backoff before the first retry; doubles each attempt up to `max_retry_delay`, with ±20% jitter. |
| `max_retry_delay` | no | `5s` | Upper bound on a single retry backoff. Keep `max_retries × max_retry_delay` inside `label_keep_max_second`. |
| `max_filter_ratio` | no | unset | Forwarded as the `max_filter_ratio` Stream Load header. Must be a finite value in `[0.0, 1.0]`; an out-of-range value fails `open()`. |
| `columns` | no | unset | Forwarded as the `columns` Stream Load header. Validated at startup; an invalid value fails `open()`. |
| `where` | no | unset | Forwarded as the `where` Stream Load header. Validated at startup; an invalid value fails `open()`. |
| `output_format` | no | `json` | Stream Load output format: `json` or `csv`. CSV is opt-in for throughput; it **requires `columns`** (CSV is positional, unlike name-mapped JSON) and emits control-char-framed, `enclose`/`escape`-quoted rows. `open()` fails if `output_format = "csv"` and `columns` is unset. (Named `output_format`, not `format`, to avoid an env-override collision with `plugin_config_format`.) |
| `allow_insecure_redirect` | no | `false` | Permit a Stream Load redirect that downgrades `https://` → `http://`. Refused by default because it would push credentials onto a cleartext hop. |
| `allowed_redirect_hosts` | no | unset | Allowlist of redirect targets. Each entry is `host` (pins the host, any port) or `host:port` (pins the exact endpoint). When set and non-empty, any other redirect target is refused. |

### Example

```toml
type = "sink"
key = "doris"
enabled = true
version = 0
name = "Doris sink"
path = "target/release/libiggy_connector_doris_sink"
plugin_config_format = "toml"

[[streams]]
stream = "events"
topics = ["doris_events"]
schema = "json"
batch_length = 100
poll_interval = "5ms"
consumer_group = "doris_sink"

[plugin_config]
fe_url = "http://localhost:8030"
database = "iggy_demo"
table = "events"
username = "root"
password = "replace_with_secret"
label_prefix = "iggy"
batch_size = 1000
timeout = "30s"
```

## Security notes

- **Use `https://` in production.** The connector accepts `http://` URLs and logs a `warn!` when `fe_url` points at a non-loopback host over plain HTTP, but it does not refuse. Over `http://`, the HTTP Basic credentials travel in cleartext.
- **Trust boundary on the FE.** The connector intentionally preserves the `Authorization` header across the FE → BE 307 redirect (reqwest would otherwise strip it on cross-host redirects).
  A compromised or MITM'd FE could try to exfiltrate credentials by responding with `Location: http://attacker/`. Before re-attaching credentials, the connector validates the redirect target: it **refuses a scheme downgrade** (`https://` → `http://`) unless `allow_insecure_redirect = true`, requires an **absolute** `Location` (a relative one is rejected, not silently resolved), and — if `allowed_redirect_hosts` is set — refuses any target outside that allowlist.
  **When `allowed_redirect_hosts` is unset (the default), any same-scheme host is accepted** — that is the price of supporting the normal cross-host FE → BE topology out of the box. For lockdown in hostile networks, set `allowed_redirect_hosts` to your known BE endpoints and deploy Doris over TLS. List a bare `host` to pin only the host, or `host:port` to pin the exact endpoint — pinning the port closes the "allowlisted host, attacker port" vector.
- **`columns` and `where` are SQL-expression pass-throughs.** Whatever you put in those config fields is forwarded verbatim to Doris's Stream Load and evaluated as a SQL expression. Keep this config trusted.

## Operational guidance

- **`label_keep_max_second`.** The connector's in-request retry re-PUTs a transiently-failed batch under the same label, so Doris must retain that label for at least the connector's full retry budget for the replay to dedupe. The Doris default is 3 days, which is conservative. If you set this lower on the Doris side, make sure `max_retries × max_retry_delay` fits inside the window — once a label expires, a retry re-loads instead of deduping, producing duplicate rows.
- **Keep `batch_size` stable across a redrive.** The label includes the chunk's `first_offset` and `last_offset`, which are a function of `batch_size`. If you change `batch_size` between a failed load and its redrive, the chunk boundaries shift, the offsets differ, and the new label no longer matches the old one — so Doris re-loads instead of deduping, producing duplicate rows.
- **Filtered-row alerts.** When Doris reports `number_filtered_rows > 0`, the connector emits a `warn!`. This is your signal that upstream message shapes have drifted from the table schema; alert on it.
- **Multi-chunk batches are best-effort for operational failures.** A poll larger than `batch_size` is split into chunks, each loaded as its own labelled Stream Load (with its own in-request retry budget for transient failures). If a chunk still fails after its retries (serialize, HTTP, or status-classification error), the connector keeps the worst error, attempts the remaining chunks, and returns that error at the end — it does **not** stop at the first such failure.
  The runtime commits the consumer offset for the whole poll before `consume()` runs, so a chunk that exhausts its in-request retries is not replayed across polls; pushing the other chunks through maximizes delivered data, and the worst error is surfaced at the end (logged at `error!` for observability — the runtime currently discards `consume()`'s return value, so there is no cross-poll redrive or DLQ).
  The one deliberate exception is a **non-JSON payload**, which is treated as a schema-contract violation and aborts the whole poll immediately (see the Requirements note above). Under `schema = "json"` this is unreachable, so it is a defensive guard rather than a normal path.

## Limitations

- Output is JSON (default) or CSV (`output_format = "csv"`, which requires `columns`). Both serialize from `Payload::Json`; raw-text/CSV passthrough and Parquet are not supported. CSV maps JSON `null` and missing keys to SQL `NULL` (`\N`), an empty string to `""`, emits numbers and booleans bare (`true`/`false`), and stringifies nested objects/arrays as JSON.
- HTTP Basic auth only.
- No automatic table creation.
- In-request retry only. Transient backend failures are retried within a single `consume()` call (step 6), but the runtime commits the consumer offset at poll time and discards `consume()`'s return value, so there is no cross-poll redrive or DLQ — delivery is at-most-once under a crash or a failure that outlives the retry budget.
