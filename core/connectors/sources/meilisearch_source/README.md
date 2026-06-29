# Meilisearch Source Connector

A source connector that polls documents from a Meilisearch index and produces
them as JSON messages into Iggy.

## Configuration

- `url`: Meilisearch base URL.
- `index`: Source index UID.
- `api_key`: Optional Meilisearch API key sent as `Authorization: Bearer`.
- `query`: Optional search query. Defaults to an empty query.
- `filter`: Optional Meilisearch filter expression string or nested JSON array.
- `batch_size`: Maximum documents fetched per poll. Defaults to `100`.
- `polling_interval`: Delay between polls as a humantime string. Defaults to `5s`.
- `include_metadata`: Wrap each hit with Meilisearch metadata. Defaults to `false`.
- `timeout`: Request timeout as a humantime string. Defaults to `30s`.
- `max_retries`: Maximum transient retry attempts during polling. Defaults to `3`.
- `retry_delay`: Initial retry delay. Defaults to `500ms`.
- `max_retry_delay`: Maximum retry delay. Defaults to `5s`.
- `max_open_retries`: Maximum transient retry attempts during `open()`. Defaults to `5`.

## Filter Syntax

String filters work in regular TOML plugin config:

```toml
filter = "category = alpha"
```

Nested array filters require JSON plugin config because the connector receives
the field as `serde_json::Value`:

```json
{
  "filter": [["category = alpha", "category = beta"], "enabled = true"]
}
```

Top-level filter array entries are combined with `AND`; nested arrays are
combined with `OR`.

## Behavior

The connector requires the source index to define a primary key. Each poll sends
a `/search` request sorted by that primary key and stores the last emitted
primary-key value in connector state. This avoids offset pagination skips when
documents are inserted or deleted between polls.

The primary-key field must be an integer, filterable, and sortable in
Meilisearch, because the connector adds a cursor filter and primary-key sort to
each search request. The connector validates the sortable setting during
`open()`, but Meilisearch settings do not expose document value types, so the
integer-value requirement is validated while polling. Documents with missing or
non-integer primary-key values are skipped with a warning, and the cursor
advances to the last valid integer primary key in the batch. String primary keys
are not supported until the Meilisearch version used for validation supports
greater-than filters on string attributes. Returned hits are serialized as JSON
message payloads.

Configure the primary-key field as both filterable and sortable before starting
the connector. For example, when the primary key is `id`:

```bash
curl -X PATCH "$MEILISEARCH_URL/indexes/iggy_messages/settings" \
  -H 'Content-Type: application/json' \
  --data '{
    "filterableAttributes": ["id"],
    "sortableAttributes": ["id"]
  }'
```

Meilisearch state is advanced in memory when a batch is returned from `poll()`.
If the runtime fails to send that batch to Iggy, the source trait does not
provide an acknowledgment callback that would let this connector roll the cursor
back before the next poll. This is a known limitation of the current connector
source API.

When `include_metadata` is enabled, each payload has this shape:

```json
{
  "document": {},
  "meilisearch": {
    "index": "iggy_messages",
    "primary_key": "id"
  }
}
```
