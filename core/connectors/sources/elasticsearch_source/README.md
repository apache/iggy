# Elasticsearch Source Connector with State Management

This Elasticsearch source connector provides comprehensive state management capabilities to track processing progress and enable fault-tolerant data ingestion.

## Features

- **Incremental Data Processing**: Track last processed timestamp to avoid reprocessing data
- **Cursor-based Pagination**: Support for document ID-based cursors
- **Scroll-based Pagination**: Support for Elasticsearch scroll API
- **Error Tracking**: Monitor error counts and last error messages
- **Processing Statistics**: Track performance metrics and processing times
- **Persistent State Storage**: Multiple storage backends (file, Elasticsearch, Redis)
- **Auto-save**: Configurable automatic state persistence
- **State Recovery**: Resume processing from last known position after restart

## Configuration

### Basic Configuration

```toml
type = "source"
key = "elasticsearch"
enabled = true
version = 0
name = "Elasticsearch source"
path = "target/release/libiggy_connector_elasticsearch_source"

[[streams]]
stream = "elasticsearch_stream"
topic = "documents"
schema = "json"
batch_length = 100
linger_time = "5ms"

[plugin_config]
url = "http://localhost:9200"
index = "logs-*"
polling_interval = "30s"
batch_size = 100
timestamp_field = "@timestamp"
query = {
  "match_all": {}
}
```

### State Management Configuration

```toml
[plugin_config]
# ... basic config ...
state = {
  enabled = true
  storage_type = "file"  # "file", "elasticsearch", "redis"
  storage_config = {
    base_path = "./connector_states"  # for file storage
    # index = "connector_states"      # for elasticsearch storage
    # url = "redis://localhost:6379"  # for redis storage
  }
  state_id = "elasticsearch_logs_connector"
  auto_save_interval = "5m"
  tracked_fields = [
    "last_poll_timestamp",
    "last_document_id",
    "total_documents_fetched"
  ]
}
```

## State Information

The connector tracks the following state information:

### Processing State

- `last_poll_timestamp`: Last successful poll timestamp
- `total_documents_fetched`: Total number of documents processed
- `poll_count`: Number of polling cycles executed
- `last_document_id`: Last processed document ID (for cursor pagination)
- `last_scroll_id`: Last scroll ID (for scroll pagination)
- `last_offset`: Last processed offset

### Error Tracking

- `error_count`: Total number of errors encountered
- `last_error`: Last error message

### Performance Statistics

- `total_bytes_processed`: Total bytes processed
- `avg_batch_processing_time_ms`: Average processing time per batch
- `last_successful_poll`: Timestamp of last successful poll
- `empty_polls_count`: Number of polls that returned no documents
- `successful_polls_count`: Number of successful polls

## Storage Backends

### File Storage (Default)

```toml
state = {
  enabled = true
  storage_type = "file"
  storage_config = {
    base_path = "./connector_states"
  }
}
```

### Elasticsearch Storage

```toml
state = {
  enabled = true
  storage_type = "elasticsearch"
  storage_config = {
    index = "connector_states"
    url = "http://localhost:9200"
  }
}
```

### Redis Storage

```toml
state = {
  enabled = true
  storage_type = "redis"
  storage_config = {
    url = "redis://localhost:6379"
    key_prefix = "connector_states:"
  }
}
```

## Usage Examples

### Basic Usage with State Management

```rust
use elasticsearch_source::{ElasticsearchSource, StateManagerExt};

// Create connector with state management enabled
let mut connector = ElasticsearchSource::new(id, config);

// Open connector (automatically loads state if available)
connector.open().await?;

// Start polling (automatically saves state)
let messages = connector.poll().await?;

// Close connector (automatically saves final state)
connector.close().await?;
```

### Manual State Management

```rust
use elasticsearch_source::{ElasticsearchSource, StateManagerExt};

let mut connector = ElasticsearchSource::new(id, config);

// Load state manually
connector.load_state().await?;

// Get current state
let state = connector.get_state().await?;
println!("Current state: {:?}", state);

// Export state to JSON
let state_json = connector.export_state().await?;
println!("State JSON: {}", serde_json::to_string_pretty(&state_json)?);

// Import state from JSON
connector.import_state(state_json).await?;

// Reset state
connector.reset_state().await?;
```

### State Manager Utilities

```rust
use elasticsearch_source::{ElasticsearchSource, StateManagerExt};

let connector = ElasticsearchSource::new(id, config);

// Get state manager
if let Some(state_manager) = connector.get_state_manager() {
    // Get state statistics
    let stats = state_manager.get_state_stats().await?;
    println!("Total states: {}", stats.total_states);

    // Clean up old states (older than 30 days)
    let deleted_count = state_manager.cleanup_old_states(30).await?;
    println!("Deleted {} old states", deleted_count);
}
```

## State File Format

State files are stored as JSON with the following structure:

```json
{
  "id": "elasticsearch_logs_connector",
  "last_updated": "2024-01-15T10:30:00Z",
  "version": 1,
  "data": {
    "last_poll_timestamp": "2024-01-15T10:30:00Z",
    "total_documents_fetched": 15000,
    "poll_count": 150,
    "last_document_id": "doc_12345",
    "last_scroll_id": "scroll_abc123",
    "last_offset": 15000,
    "error_count": 2,
    "last_error": "Connection timeout",
    "processing_stats": {
      "total_bytes_processed": 1048576,
      "avg_batch_processing_time_ms": 125.5,
      "last_successful_poll": "2024-01-15T10:30:00Z",
      "empty_polls_count": 5,
      "successful_polls_count": 145
    }
  },
  "metadata": {
    "connector_type": "elasticsearch_source",
    "connector_id": 1,
    "index": "logs-*",
    "url": "http://localhost:9200"
  }
}
```

## Best Practices

1. **State ID Uniqueness**: Use unique state IDs for different connector instances
2. **Auto-save Interval**: Set appropriate auto-save intervals based on your data volume
3. **Storage Location**: Use persistent storage locations for production deployments
4. **State Cleanup**: Regularly clean up old state files to prevent disk space issues
5. **Error Handling**: Monitor error counts and implement appropriate alerting
6. **Backup**: Regularly backup state files for disaster recovery

## Troubleshooting

### Common Issues

1. **State Not Loading**: Check file permissions and storage path
2. **State Corruption**: Delete corrupted state files to start fresh
3. **Performance Issues**: Adjust auto-save interval and batch sizes
4. **Storage Full**: Implement state cleanup policies

### Monitoring

Monitor the following metrics:

- State save/load success rates
- Processing statistics
- Error counts and types
- Storage usage for state files

## Migration

To migrate from a connector without state management:

1. Add state configuration to your connector config
2. Set `enabled = true` in state config
3. Restart the connector
4. The connector will start tracking state from the next poll cycle

To migrate between storage backends:

1. Export state from current storage
2. Update storage configuration
3. Import state to new storage
4. Restart connector
