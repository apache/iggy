# Apache Doris Sink

The Doris sink connector consumes JSON messages from Iggy streams and writes them to a pre-created Apache Doris table via Doris's [Stream Load HTTP API](https://doris.apache.org/docs/data-operate/import/import-way/stream-load-manual).

## Requirements

- The target Doris **database and table must be pre-created** before enabling the sink. The connector never issues DDL.
- `database` and `table` config values must match `[A-Za-z0-9_]+`. Anything else is rejected at startup with `Error::InvalidConfigValue` — this also prevents path traversal in the constructed `/api/{db}/{table}/_stream_load` URL.
- Messages must arrive with `Payload::Json` (i.e. the configured stream schema is `json`). Other payload types are dropped with a `warn!` and the consumer offset advances past them — this is silent data loss for any non-JSON message, so the upstream schema must be guaranteed JSON.
- The Iggy message JSON shape must match the target table columns. Use the optional `columns` plugin setting if the column order differs from the JSON keys.

## How it works

1. For each batch of messages, the connector serializes the JSON payloads into a JSON array.
2. It computes a deterministic Stream Load `label` of the form `{label_prefix}-{stream}_{hash8}-{topic}_{hash8}-{partition}-{first_offset}-{last_offset}`.
   - Each variable-length segment carries a 32-bit blake3 hash of the raw name, so names that sanitize to the same string (e.g. `events.v1` vs `events_v1`) cannot collide.
   - The total label is bounded under Doris's 128-char cap regardless of input length.
   - Doris dedupes loads by label inside its `label_keep_max_second` window, so a replayed batch (after restart, retry, etc.) is silently absorbed instead of producing duplicates.
3. It `PUT`s the batch to `{fe_url}/api/{database}/{table}/_stream_load` with HTTP Basic auth and the headers `Expect: 100-continue`, `format: json`, `strip_outer_array: true`, `label: <label>`. (`Expect: 100-continue` lets Doris reject auth/4xx failures before the connector uploads the whole body — important for large batches and required if a reverse proxy sits in front of Doris.)
4. The Doris frontend (FE) responds with a `307 Temporary Redirect` to a backend (BE). The connector follows the redirect manually so that the `Authorization` header is preserved across the hop (`reqwest`'s default policy strips it on cross-host redirects). `308 Permanent Redirect` is also followed as a defensive measure; redirects beyond a hard cap of 5 are rejected as `HttpRequestFailed`.
5. The HTTP body is parsed as JSON and the `Status` field decides the outcome:
   - `Success` → batch accepted.
   - `Label Already Exists` → idempotent replay, treated as success.
   - `Publish Timeout` or HTTP `5xx`/`408`/`429` → transient error (`Error::CannotStoreData`); the runtime can retry.
   - `Fail`, any other `4xx`, or an unparsable response body → permanent error (`Error::PermanentHttpError`); retrying is not useful.

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
| `timeout_secs` | no | `30` | Per-request HTTP timeout (total request budget). |
| `connect_timeout_secs` | no | `5` | TCP connect timeout, independent of `timeout_secs`. Raise it for cross-region or cold-start FEs. |
| `max_filter_ratio` | no | unset | Forwarded as the `max_filter_ratio` Stream Load header. |
| `columns` | no | unset | Forwarded as the `columns` Stream Load header. |
| `where` | no | unset | Forwarded as the `where` Stream Load header. |
| `allow_insecure_redirect` | no | `false` | Permit a Stream Load redirect that downgrades `https://` → `http://`. Refused by default because it would push credentials onto a cleartext hop. |
| `allowed_redirect_hosts` | no | unset | Allowlist of hosts a redirect may target. When set and non-empty, a redirect to any other host is refused. |

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
timeout_secs = 30
```

## Security notes

- **Use `https://` in production.** The connector accepts `http://` URLs and logs a `warn!` when `fe_url` points at a non-loopback host over plain HTTP, but it does not refuse. Over `http://`, the HTTP Basic credentials travel in cleartext.
- **Trust boundary on the FE.** The connector intentionally preserves the `Authorization` header across the FE → BE 307 redirect (reqwest would otherwise strip it on cross-host redirects). A compromised or MITM'd FE could try to exfiltrate credentials by responding with `Location: http://attacker/`. Before re-attaching credentials, the connector validates the redirect target: it **refuses a scheme downgrade** (`https://` → `http://`) unless `allow_insecure_redirect = true`, and — if `allowed_redirect_hosts` is set — refuses any host outside that allowlist. Cross-host redirects on the same scheme are allowed, since that is the normal FE → BE topology. For maximum lockdown in hostile networks, set `allowed_redirect_hosts` to your known BE hosts and deploy Doris over TLS.
- **`columns` and `where` are SQL-expression pass-throughs.** Whatever you put in those config fields is forwarded verbatim to Doris's Stream Load and evaluated as a SQL expression. Keep this config trusted.

## Operational guidance

- **`label_keep_max_second`.** Idempotent replay relies on Doris retaining each label for at least as long as it could take the Iggy runtime to redrive a failed batch. The Doris default is 3 days, which is conservative. If you set this lower on the Doris side, make sure your runtime retry budget fits inside the window — once a label expires, a replay re-loads instead of deduping, producing duplicate rows.
- **Filtered-row alerts.** When Doris reports `number_filtered_rows > 0`, the connector emits a `warn!`. This is your signal that upstream message shapes have drifted from the table schema; alert on it.
- **Multi-chunk batches are best-effort.** A poll larger than `batch_size` is split into chunks, each loaded as its own labelled Stream Load. If one chunk fails, the connector still attempts the remaining chunks and then returns the worst error — it does **not** stop at the first failure. The runtime commits the consumer offset for the whole poll before `consume()` runs, so a failed chunk is not replayed regardless; pushing the other chunks through maximizes delivered data, and the surfaced error still drives the runtime's error path.

## Limitations (todo)

- JSON payload only. CSV and raw-text payloads are not supported yet.
- HTTP Basic auth only.
- No automatic table creation.
- No built-in retry middleware or circuit breaker — the runtime decides whether to redrive a failing batch. A hardening pass with `iggy_connector_sdk::retry::*` is planned as a follow-up.
