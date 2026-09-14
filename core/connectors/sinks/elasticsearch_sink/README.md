# Elasticsearch Sink Connector

A sink connector that consumes messages from Iggy streams and indexes them to Elasticsearch.

## Configuration

- `url`: Elasticsearch cluster URL
- `index`: Target index name
- `username/password`: Optional authentication credentials
- `batch_size`: Accepted but unused; the stream-level `batch_length` determines each incoming batch
- `timeout_seconds`: Client-wide HTTP timeout for `open()` and bulk `consume()` (default: 30s). Values of `0` are clamped to 1s. Raise for slow bulk workloads; a timed-out bulk fails the batch after the poll offset is already committed
- `create_index_if_not_exists`: Automatically create index (default: true)
- `index_mapping`: Index mapping configuration

## Features

- Bulk indexing optimization
- Automatic index creation
- Items rejected with HTTP 429 or 5xx receive up to three attempts with exponential backoff and jitter. Only rejected items are retried; accepted documents are not resubmitted. Request failures or malformed responses fail the batch without replaying an ambiguous write. Any remaining rejection fails the callback, including partial failures, while successful items remain indexed
- Metadata field injection
- Support for multiple data formats
