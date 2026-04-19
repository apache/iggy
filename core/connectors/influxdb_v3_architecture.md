# InfluxDB V2/V3 Connector — Layered Architecture

## InfluxDB V2 vs V3 — API Delta (Feasibility Checklist)

| Concern | InfluxDB V2 | InfluxDB V3 | Shared? |
| --- | --- | --- | --- |
| **Write body** | Line Protocol | Line Protocol | **Yes — identical** |
| **Write endpoint** | `POST /api/v2/write` | `POST /api/v3/write_lp` | No (URL differs) |
| **Write params** | `?org=X&bucket=Y&precision=P` | `?db=X&precision=P` | Partial |
| **Auth header** | `Authorization: Token {t}` | `Authorization: Bearer {t}` | No |
| **Query endpoint** | `POST /api/v2/query` | `POST /api/v3/query_sql` | No |
| **Query language** | Flux | SQL or InfluxQL | No |
| **Query response** | Annotated CSV | JSONL / JSON / CSV / Parquet | No |
| **Health check** | `GET /health` | `GET /health` | **Yes** |
| **Retry/backoff** | 429 / 5xx transient | 429 / 5xx transient | **Yes** |
| **Circuit breaker** | Per batch | Per batch | **Yes** |
| **Line Protocol builder** | Escaping, precision | Escaping, precision | **Yes** |
| **Cursor state mgmt** | Timestamp-based | Timestamp-based | **Yes** |
| **Data org concept** | `org` + `bucket` | `db` (org optional) | No |
| **V3 compat write** | — | `/api/v2/write` still works | Migration bridge |

**Verdict:** ~70% of code is version-independent and can live in a shared common layer. Only URL construction, auth headers, query language, and response parsing diverge.

---

## Layered Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                          LAYER 1 — PUBLIC INTERFACE                            ║
║                                                                                  ║
║  ┌──────────────────────────────────┐   ┌──────────────────────────────────┐   ║
║  │        InfluxDbSinkConfig        │   │       InfluxDbSourceConfig       │   ║
║  │  url, token, api_version         │   │  url, token, api_version         │   ║
║  │  org, bucket (V2) │ db (V3)      │   │  org, bucket (V2) │ db (V3)      │   ║
║  │  measurement, precision          │   │  query, query_language           │   ║
║  │  batch_size, payload_format      │   │  poll_interval, cursor_field     │   ║
║  │  ── resilience fields ──         │   │  ── resilience fields ──         │   ║
║  │  retry_delay, max_retries        │   │  retry_delay, max_retries        │   ║
║  │  circuit_breaker_threshold       │   │  circuit_breaker_threshold       │   ║
║  └──────────────────────────────────┘   └──────────────────────────────────┘   ║
║                                                                                  ║
║  ┌──────────────────────────────────┐   ┌──────────────────────────────────┐   ║
║  │     impl Sink for InfluxDbSink   │   │  impl Source for InfluxDbSource  │   ║
║  │  open() → health + retry         │   │  open() → health + retry         │   ║
║  │  consume() → batch + write       │   │  poll() → query + parse + emit   │   ║
║  │  close() → metrics log           │   │  close() → flush state           │   ║
║  └──────────────────────────────────┘   └──────────────────────────────────┘   ║
╚══════════════════════════════════════════════════════════════════════════════════╝
                                        │
                                        ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║               LAYER 2 — SHARED ORCHESTRATION (version-agnostic)                ║
║                                                                                  ║
║  ┌─────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐  ║
║  │   Batch Accumulator  │  │  Retry + Exp Backoff  │  │   Circuit Breaker    │  ║
║  │  (sink)              │  │  (write + query)      │  │  open/half-open/     │  ║
║  │  Vec<String> buffer  │  │  transient: 429/5xx   │  │  closed state        │  ║
║  │  flush at batch_size │  │  max_delay cap        │  │  consecutive fail    │  ║
║  └─────────────────────┘  └──────────────────────┘  └──────────────────────┘  ║
║                                                                                  ║
║  ┌─────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐  ║
║  │  Line Protocol       │  │  Payload Format       │  │  Cursor State        │  ║
║  │  Builder (shared)    │  │  Handler (shared)     │  │  Manager (source)    │  ║
║  │  escape_measurement  │  │  JSON / Text / Base64 │  │  persist last_time   │  ║
║  │  escape_tag_value    │  │  (sink encoding)      │  │  cursor_row_count    │  ║
║  │  to_precision_ts     │  │  JSON / Text / Raw    │  │  serde forward-compat│  ║
║  └─────────────────────┘  │  (source decode)      │  └──────────────────────┘  ║
║                            └──────────────────────┘                            ║
║                                                                                  ║
║  ┌─────────────────────────────────────────────────────────────────────────┐   ║
║  │                       Metrics (AtomicU64)                               │   ║
║  │  messages_attempted  write_success  write_errors  messages_dropped      │   ║
║  └─────────────────────────────────────────────────────────────────────────┘   ║
╚══════════════════════════════════════════════════════════════════════════════════╝
                                        │
                                        ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║               LAYER 3 — VERSION ADAPTER TRAIT  (InfluxDbAdapter)               ║
║                                                                                  ║
║  trait InfluxDbAdapter: Send + Sync {                                           ║
║      fn auth_header(token: &SecretString) -> (HeaderName, HeaderValue);        ║
║      fn write_request(lines: &str, cfg: &Config) -> RequestBuilder;            ║
║      fn query_request(cursor: &str, limit: u32, cfg: &Config) -> RequestBuilder║
║      fn parse_rows(response_body: &str) -> Result<Vec<Row>, Error>;            ║
║      fn health_url(base: &Url) -> Url;                                         ║
║  }                                                                              ║
║                                                                                  ║
║  ┌──────────────────────────────────────────────────────────────────────────┐  ║
║  │  ApiVersion enum   { V2, V3, Auto }                                      │  ║
║  │  fn make_adapter(cfg) → Box<dyn InfluxDbAdapter>                         │  ║
║  │    Auto → GET /ping → parse X-Influxdb-Version header → pick V2 or V3   │  ║
║  └──────────────────────────────────────────────────────────────────────────┘  ║
╚══════════════════════════════════════════════════════════════════════════════════╝
              │                                              │
              ▼                                              ▼
╔═════════════════════════════╗              ╔══════════════════════════════════╗
║     V2Adapter               ║              ║     V3Adapter                    ║
║                             ║              ║                                  ║
║  auth_header →              ║              ║  auth_header →                   ║
║    "Token {token}"          ║              ║    "Bearer {token}"              ║
║                             ║              ║                                  ║
║  write_request →            ║              ║  write_request →                 ║
║    POST /api/v2/write       ║              ║    POST /api/v3/write_lp         ║
║    ?org=X&bucket=Y          ║              ║    ?db=X                         ║
║    &precision=P             ║              ║    &precision=P                  ║
║                             ║              ║    (Content-Encoding: gzip opt)  ║
║  query_request →            ║              ║                                  ║
║    POST /api/v2/query       ║              ║  query_request →                 ║
║    ?org=X                   ║              ║    POST /api/v3/query_sql        ║
║    body: Flux template      ║              ║    body: SQL template   ─ OR ─   ║
║      $cursor → timestamp    ║              ║    POST /api/v3/query_influxql   ║
║      $limit  → row count    ║              ║    ?db=X  format=jsonl           ║
║                             ║              ║    $cursor / $limit substituted  ║
║  parse_rows →               ║              ║                                  ║
║    RFC 4180 annotated CSV   ║              ║  parse_rows →                    ║
║    skip #datatype header    ║              ║    JSONL (one JSON obj / line)   ║
║    extract payload_column   ║              ║    extract payload_column        ║
║                             ║              ║                                  ║
║  health_url → /health       ║              ║  health_url → /health or /ping   ║
╚═════════════════════════════╝              ╚══════════════════════════════════╝
              │                                              │
              └───────────────────┬──────────────────────────┘
                                  ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║                      LAYER 4 — HTTP CLIENT (shared)                             ║
║                                                                                  ║
║  reqwest::ClientWithMiddleware                                                   ║
║  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────────────┐  ║
║  │  RetryMiddleware   │  │  Timeout Policy   │  │  Connection Pool          │  ║
║  │  ExponentialBackoff│  │  per-request (cfg)│  │  keep-alive, max-idle     │  ║
║  │  max_retries (cfg) │  │  30s default      │  │                           │  ║
║  └───────────────────┘  └───────────────────┘  └───────────────────────────┘  ║
╚══════════════════════════════════════════════════════════════════════════════════╝
                                  │
                                  ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║                           INFLUXDB SERVER                                        ║
║                                                                                  ║
║      InfluxDB OSS 2.x / Cloud 2.x          InfluxDB 3.x Core / Enterprise      ║
║      (TSM engine, Flux, org+bucket)         (IOx engine, SQL, db, ∞ cardinality)║
╚══════════════════════════════════════════════════════════════════════════════════╝
```

---

## Config Schema Design

```toml
# Common to all versions
url          = "http://localhost:8086"
token        = "my_token"
api_version  = "auto"          # "v2" | "v3" | "auto" (detects via /ping)

# V2 identity fields  (required when api_version = "v2")
org          = "my_org"
bucket       = "events"         # sink
# bucket     = "events"         # source (also used for query from-clause)

# V3 identity fields  (required when api_version = "v3")
# db         = "events"         # replaces org+bucket

# Source query — user provides version-appropriate template
# V2 (Flux):
query = '''
  from(bucket: "$bucket")
  |> range(start: time(v: "$cursor"))
  |> filter(fn: (r) => r._measurement == "iggy")
  |> limit(n: $limit)
'''

# V3 (SQL):
query = '''
  SELECT _time, payload FROM iggy
  WHERE _time > '$cursor'
  ORDER BY _time
  LIMIT $limit
'''

query_language  = "flux"   # "flux" | "sql" | "influxql"  (V3 only)
response_format = "jsonl"  # "jsonl" | "json" | "csv"      (V3 only)
```

---

## Code Reuse Summary

| Component | Reuse | Notes |
|---|---|---|
| `Sink` / `Source` trait impls | 100% | Same `open/consume/poll/close` logic |
| Line Protocol builder | 100% | Body format identical in V2 and V3 |
| Batch accumulator | 100% | Flush logic unchanged |
| Retry + circuit breaker | 100% | Same HTTP status codes trigger retries |
| Metrics counters | 100% | Atomic counters are version-agnostic |
| Cursor state management | 100% | RFC 3339 timestamp cursors work in both |
| Payload format handling | 100% | Encoding/decoding is connector-internal |
| Auth header construction | 0% | `Token` vs `Bearer` — adapter handles |
| Write URL + params | ~20% | Precision param shared; endpoint & org/bucket vs db differ |
| Query URL + body | 0% | Flux vs SQL — fully different languages |
| Response parsing | 0% | Annotated CSV vs JSONL — different parsers |

**Conclusion:** The approach is fully feasible. The adapter trait boundary is clean — only 3 methods diverge (auth, write URL, query+parse). Everything above that layer compiles once and serves both versions. The `Auto` mode via `/ping` header detection means zero config burden on users running standard installations.
