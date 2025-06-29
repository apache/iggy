# Elasticsearch Source Connector

A source connector that reads documents from Elasticsearch indices and sends them to Iggy streams.

## Configuration

- `url`: Elasticsearch cluster URL
- `index`: Index name to read from (supports wildcards)
- `username/password`: Optional authentication credentials
- `polling_interval`: Polling interval (default: 10s)
- `batch_size`: Number of documents per query (default: 100)
- `timestamp_field`: Timestamp field for incremental polling (default: @timestamp)
- `query`: Custom query conditions (default: match_all)

## Features

- Incremental data synchronization
- Support for complex queries
- Automatic reconnection mechanism
- Performance monitoring