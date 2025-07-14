# PostgreSQL Source Connector

The PostgreSQL source connector allows you to fetch data from PostgreSQL databases and stream it to Iggy topics. It supports both table polling and Change Data Capture (CDC) modes.

## Features

- **Table Polling**: Incrementally fetch data from PostgreSQL tables
- **Change Data Capture**: Monitor database changes using PostgreSQL logical replication (coming in part 2)
- **Configurable Polling Intervals**: Control how often to check for new data
- **Batch Processing**: Fetch data in configurable batch sizes
- **Offset Tracking**: Keep track of processed records to avoid duplicates
- **Multiple Tables**: Monitor multiple tables simultaneously
- **Column Mapping**: Transform column names (e.g., to snake_case)
- **Custom Queries**: Use custom SQL queries instead of simple table polling

## Configuration

```json
{
  "connection_string": "postgresql://username:password@localhost:5432/database",
  "mode": "polling",
  "tables": ["users", "orders", "products"],
  "poll_interval": "30s",
  "batch_size": 1000,
  "tracking_column": "updated_at",
  "initial_offset": "2024-01-01T00:00:00Z",
  "max_connections": 10,
  "enable_wal_cdc": false,
  "custom_query": "SELECT * FROM users WHERE updated_at > $1 ORDER BY updated_at LIMIT $2",
  "snake_case_columns": true,
  "include_metadata": true
}
```

### Configuration Options

- `connection_string`: PostgreSQL connection string
- `mode`: Operation mode - "polling" (CDC mode coming in part 2)
- `tables`: List of tables to monitor
- `poll_interval`: How often to poll for new data (e.g., "30s", "5m")
- `batch_size`: Maximum number of rows to fetch per poll (default: 1000)
- `tracking_column`: Column to track for incremental updates (default: "id")
- `initial_offset`: Starting value for the tracking column
- `max_connections`: Maximum database connections (default: 10)
- `enable_wal_cdc`: Enable WAL-based CDC (requires logical replication setup)
- `custom_query`: Custom SQL query (overrides table + tracking_column)
- `snake_case_columns`: Convert column names to snake_case (default: false)
- `include_metadata`: Include table name, operation type, etc. (default: true)

## Output Format

Each message contains:

```json
{
  "table_name": "users",
  "operation_type": "SELECT",
  "timestamp": "2024-01-15T10:30:00Z",
  "data": {
    "id": 123,
    "name": "John Doe",
    "email": "john@example.com",
    "updated_at": "2024-01-15T10:29:50Z"
  },
  "old_data": null
}
```

## Usage Example

1. Configure the connector in your Iggy connectors runtime
2. The connector will start polling the specified tables
3. Data changes will be streamed to the configured Iggy topic
4. Each row becomes a separate message in JSON format
