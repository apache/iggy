# Meilisearch Sink Connector

A sink connector that consumes messages from Iggy streams and writes them to a
Meilisearch index through the official Rust SDK.

## Configuration

- `url`: Meilisearch base URL.
- `index`: Target index UID.
- `api_key`: Optional Meilisearch API key sent as `Authorization: Bearer`.
- `primary_key`: Index primary key field. Defaults to `iggy_id`.
- `document_action`: `replace` uses SDK add-or-replace semantics; `update` uses SDK add-or-update semantics.
- `create_index_if_not_exists`: Create the index during `open()` when missing. Defaults to `true`.
- `include_metadata`: Add Iggy metadata fields to each document. Defaults to `true`.
- `batch_size`: Maximum documents per Meilisearch document request. Defaults to `1000`.
- `timeout`: Request timeout as a humantime string, for example `30s`.
- `wait_for_tasks`: Poll Meilisearch tasks until terminal state before returning from `consume()`. Defaults to `true`.
- `task_timeout`: Maximum time to wait for each Meilisearch task. Defaults to `30s`.
- `task_poll_interval`: Delay between task polls. Defaults to `100ms`.
- `max_retries`, `retry_delay`, `max_retry_delay`: Transient HTTP retry settings.

## Behavior

JSON object payloads are indexed as documents. JSON arrays or scalar values are
wrapped in a `value` field because Meilisearch documents must be objects. Raw
payloads are parsed as JSON when possible; otherwise, they are indexed as base64
data. Text payloads are indexed in a `text` field.

When the configured primary key is absent, the connector injects a stable value
derived from Iggy stream, topic, partition, offset, and message ID. This avoids
Meilisearch primary-key inference failures and keeps repeated delivery
idempotent for the same message.
