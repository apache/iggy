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

## Behavior

The connector requires the source index to define a primary key. Each poll sends
a `/search` request sorted by that primary key and stores the last emitted
primary-key value in connector state. This avoids offset pagination skips when
documents are inserted or deleted between polls.

The primary-key field must be configured as filterable and sortable in
Meilisearch, because the connector adds a cursor filter and primary-key sort to
each search request. Returned hits are serialized as JSON message payloads.

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
