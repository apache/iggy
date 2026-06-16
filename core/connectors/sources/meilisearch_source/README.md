# Meilisearch Source Connector

A source connector that polls documents from a Meilisearch index and produces
them as JSON messages into Iggy.

## Configuration

- `url`: Meilisearch base URL.
- `index`: Source index UID.
- `api_key`: Optional Meilisearch API key sent as `Authorization: Bearer`.
- `query`: Optional search query. Defaults to an empty query.
- `filter`: Optional Meilisearch filter expression or array.
- `batch_size`: Maximum documents fetched per poll. Defaults to `100`.
- `polling_interval`: Delay between polls as a humantime string. Defaults to `5s`.
- `include_metadata`: Wrap each hit with Meilisearch metadata. Defaults to `false`.
- `timeout`: Request timeout as a humantime string. Defaults to `30s`.
- `max_retries`: Maximum transient retry attempts during polling. Defaults to `3`.
- `retry_delay`: Initial retry delay. Defaults to `500ms`.
- `max_retry_delay`: Maximum retry delay. Defaults to `5s`.
- `max_open_retries`: Maximum transient retry attempts during `open()`. Defaults to `5`.

## Behavior

The connector requires the source index to define a primary key. Each poll sends
a `/search` request sorted by that primary key and stores the last emitted
primary-key value in connector state. This avoids offset pagination skips when
documents are inserted or deleted between polls.

The primary-key field must be numeric, filterable, and sortable in Meilisearch,
because the connector adds a cursor filter and primary-key sort to each search
request. The connector logs this numeric primary-key requirement during
`open()`. String primary keys are not supported until the Meilisearch version
used for validation supports greater-than filters on string attributes. Returned
hits are serialized as JSON message payloads.

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
