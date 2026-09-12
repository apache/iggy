# Apache Airflow Sink Connector

Consumes messages from Iggy streams and triggers Apache Airflow DAG runs via the
stable REST API (`POST /api/v1/dags/{dag_id}/dagRuns`).

## Overview

| | |
| --- | --- |
| **Type** | Sink (batch trigger) |
| **Direction** | Iggy → Airflow |
| **API** | Airflow REST (`api_prefix` default `/api/v1`) |
| **Unit of work** | One DAG run per poll batch (not per message) |
| **Idempotency** | Deterministic batch `dag_run_id`; HTTP **409** treated as success |

Airflow runs are jobs, not stream events. Each connector poll becomes **one DAG
run** whose `conf.messages` holds the batch. Tune volume with stream
`batch_length` / `poll_interval`. Retries re-use the same `dag_run_id` so
redelivery does not create duplicate runs.

If `dag_id_header` is set and messages resolve to different DAG ids, the poll is
split into **one run per DAG id group** (still batches of messages, never one
run per message by default).

## Configuration

```toml
type = "sink"
key = "airflow"
enabled = true
version = 0
name = "Airflow trigger"
path = "target/release/libiggy_connector_airflow_sink"

[[streams]]
stream = "events"
topics = ["orders"]
schema = "json"
batch_length = 50
poll_interval = "100ms"
consumer_group = "airflow_sink_group"

[plugin_config]
base_url = "http://localhost:8080"
dag_id = "example_dag"
api_prefix = "/api/v1"
auth = "basic"          # none | basic | bearer
username = "admin"
password = "admin"
# token = "..."         # when auth = bearer
# dag_id_header = "airflow_dag_id"
conf_mode = "payload"
include_iggy_metadata_in_conf = false
health_check_enabled = true
health_path = "/api/v1/version"
timeout = "30s"
max_retries = 3
retry_delay = "1s"
retry_backoff_multiplier = 2
max_retry_delay = "30s"
```

### Plugin fields

| Field | Required | Default | Description |
| --- | :---: | --- | --- |
| `base_url` | yes | — | Airflow webserver base URL |
| `dag_id` | yes* | — | Default DAG id (*optional if every message sets `dag_id_header`) |
| `api_prefix` | no | `/api/v1` | REST prefix for version differences |
| `auth` | no | `none` | `none`, `basic`, or `bearer` |
| `username` / `password` | basic | — | Basic auth credentials (`password` is secret) |
| `token` | bearer | — | Bearer/JWT token (secret) |
| `dag_id_header` | no | unset | Message header that overrides `dag_id` (groups batch by value) |
| `conf_mode` | no | `payload` | How each message body is placed under `conf.messages[].payload` |
| `include_iggy_metadata_in_conf` | no | `false` | Nest batch stream/topic/offset range under `conf.iggy` |
| `health_check_enabled` | no | `true` | `GET` health path in `open()` |
| `health_path` | no | `/api/v1/version` | Path relative to `base_url` |
| `timeout` | no | `30s` | HTTP timeout |
| `max_retries` | no | `3` | Total attempts including the first |
| `retry_delay` / `max_retry_delay` | no | `1s` / `30s` | Exponential backoff bounds |
| `tls_danger_accept_invalid_certs` | no | `false` | Dev only |
| `verbose_logging` | no | `false` | Extra per-trigger logs |

Credentials use `SecretString` and are redacted in logs and `/stats` serialization.

## Request shape

```http
POST {base_url}{api_prefix}/dags/{dag_id}/dagRuns
Content-Type: application/json

{
  "dag_run_id": "iggy-{partition}-{first_offset}-{last_offset}-{message_count}",
  "conf": {
    "messages": [
      {
        "offset": 0,
        "id": "0000...0001",
        "timestamp": 1700000000000000,
        "payload": { "order_id": 1 }
      },
      {
        "offset": 1,
        "id": "0000...0002",
        "timestamp": 1700000000000001,
        "payload": { "order_id": 2 }
      }
    ],
    "iggy": {
      "stream": "events",
      "topic": "orders",
      "partition_id": 0,
      "first_offset": 0,
      "last_offset": 1,
      "message_count": 2
    }
  }
}
```

`conf.iggy` is present only when `include_iggy_metadata_in_conf = true`.

The sink always sets `dag_run_id` explicitly. Airflow does **not** fall back to
`manual__timestamp` when the field is provided. Replaying the same batch yields
the same id; Airflow responds **409**, which this sink treats as success.

### Status handling

| Status | Behavior |
| --- | --- |
| 2xx | Success |
| 409 | Success (run already exists — idempotent replay) |
| 400 / 401 / 403 / 404 / 422 | Permanent — fail the batch (do not advance as if triggered) |
| 429 / 5xx | Transient — retry with exponential backoff |
| Network errors | Transient — retry |

Permanent failures return an error for the whole batch so consumer offsets are
not committed past an untriggered poll. Fix config/auth/DAG availability and
retry.

### Throughput guidance

- Prefer larger `batch_length` so one Airflow run processes many messages.
- Point this sink at **work-sized** streams (export ready, file landed, window
  closed), not raw high-volume domain firehoses.
- For a single-message command topic, set `batch_length = 1` so each poll is
  still one batch run with one entry in `conf.messages`.

## Out of scope (v1)

- Waiting for DAG completion
- Airflow source (task/DAG state → Iggy)
- Airflow provider package on the Airflow side
- In-sink content filters (`apply_function`-style); use a dedicated topic or a
  future transform if you need selective triggering

## Build and test

```bash
cargo build -p iggy_connector_airflow_sink
cargo test -p iggy_connector_airflow_sink
cargo clippy -p iggy_connector_airflow_sink --all-targets -- -D warnings
```

Integration (Docker + WireMock):

```bash
cargo build -p iggy_connector_airflow_sink
cargo test -p integration -- connectors::airflow
```

## Related

- Issue: [#3715](https://github.com/apache/iggy/issues/3715)
- Roadmap: [#2753](https://github.com/apache/iggy/issues/2753)
