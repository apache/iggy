# Meilisearch Source Connector

A source connector that polls documents from a Meilisearch index and produces
them as JSON messages into Iggy.

## Configuration

- `url`: Meilisearch base URL.
- `index`: Source index UID.
- `api_key`: Optional Meilisearch API key sent as `Authorization: Bearer`.
- `query`: Optional search query. Defaults to an empty query.
- `filter`: Optional Meilisearch filter expression or array.
- `sort`: Optional list of Meilisearch sort expressions, for example `["created_at:asc"]`.
- `batch_size`: Maximum documents fetched per poll. Defaults to `100`.
- `polling_interval`: Delay between polls as a humantime string. Defaults to `5s`.
- `include_metadata`: Wrap each hit with Meilisearch metadata. Defaults to `false`.

## Behavior

The connector stores the next search offset in connector state. Each poll sends
a `/search` request with the configured query, filter, sort, and current offset.
Returned hits are serialized as JSON message payloads. Configure a stable sort
when the source index changes while the connector is running.

When `include_metadata` is enabled, each payload has this shape:

```json
{
  "document": {},
  "meilisearch": {
    "index": "iggy_messages",
    "offset": 0
  }
}
```
