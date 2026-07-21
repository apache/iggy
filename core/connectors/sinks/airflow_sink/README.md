# Apache Airflow Sink Connector

Consumes messages from Iggy streams and triggers Apache Airflow DAG runs via the
stable REST API (`POST /api/v1/dags/{dag_id}/dagRuns`).

## Overview

| | |
|---|---|
| **Type** | Sink (trigger) |
| **Direction** | Iggy → Airflow |
| **API** | Airflow REST (`api_prefix` default `/api/v1`) |
| **Idempotency** | Deterministic `dag_run_id`; HTTP **409** treated as success |

Each message becomes one DAG run. The message payload is sent as the run's
`conf` object. Retries re-use the same `dag_run_id` so redelivery does not
create duplicate runs.

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
|-------|:--------:|---------|-------------|
| `base_url` | yes | — | Airflow webserver base URL |
| `dag_id` | yes* | — | Default DAG id (*optional if every message sets `dag_id_header`) |
| `api_prefix` | no | `/api/v1` | REST prefix for version differences |
| `auth` | no | `none` | `none`, `basic`, or `bearer` |
| `username` / `password` | basic | — | Basic auth credentials (`password` is secret) |
| `token` | bearer | — | Bearer/JWT token (secret) |
| `dag_id_header` | no | unset | Message header that overrides `dag_id` |
| `conf_mode` | no | `payload` | Whole payload → `conf` |
| `include_iggy_metadata_in_conf` | no | `false` | Nest stream/topic/offset under `conf.iggy` |
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
  "dag_run_id": "iggy-{partition}-{offset}-{message_id_hex}",
  "conf": { ... message payload ... }
}
```

### Status handling

| Status | Behavior |
|--------|----------|
| 2xx | Success |
| 409 | Success (run already exists — idempotent replay) |
| 400 / 401 / 403 / 404 / 422 | Permanent — drop message, do not retry |
| 429 / 5xx | Transient — retry with exponential backoff |
| Network errors | Transient — retry |

## Out of scope (v1)

- Waiting for DAG completion
- Airflow source (task/DAG state → Iggy)
- Batching multiple messages into one DAG run
- Airflow provider package on the Airflow side

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

- Issue: https://github.com/apache/iggy/issues/3715
- Roadmap: https://github.com/apache/iggy/issues/2753
