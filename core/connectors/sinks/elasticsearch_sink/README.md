# Elasticsearch Sink Connector

A sink connector that consumes messages from Iggy streams and indexes them to Elasticsearch.

## Configuration

- `url`: Elasticsearch cluster URL
- `index`: Target index name
- `username/password`: Optional authentication credentials
- `batch_size`: Bulk indexing batch size (default: 100)
- `timeout_seconds`: Request timeout (default: 30s)
- `create_index_if_not_exists`: Automatically create index (default: true)
- `index_mapping`: Index mapping configuration

## Features

- Bulk indexing optimization
- Automatic index creation
- Error handling and retry mechanisms
- Metadata field injection
- Support for multiple data formats
