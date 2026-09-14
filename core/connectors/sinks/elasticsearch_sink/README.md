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
- Request errors and bulk requests with no successfully indexed documents fail the batch without connector retries. Partial bulk failures log the rejected count for the batch and are counted in closing statistics without failing the batch
- Metadata field injection
- Support for multiple data formats
