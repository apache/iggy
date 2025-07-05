# PostgreSQL Sink Connector

The PostgreSQL sink connector allows you to consume messages from Iggy topics and store them in PostgreSQL databases.

## Features

- **Automatic Table Creation**: Optionally create tables automatically
- **Batch Processing**: Insert messages in configurable batches for performance
- **Metadata Storage**: Store Iggy message metadata (offset, timestamp, topic, etc.)
- **Raw Payload Storage**: Store original message payload as JSONB
- **Flexible Data Mapping**: Store JSON message data in structured columns
- **Connection Pooling**: Efficient database connection management

## Configuration

```json
{
  "connection_string": "postgresql://username:password@localhost:5432/database",
  "target_table": "iggy_messages",
  "batch_size": 100,
  "max_connections": 10,
  "auto_create_table": true,
  "include_metadata": true,
  "store_raw_payload": true,
  "raw_payload_column": "raw_payload"
}
```

### Configuration Options

- `connection_string`: PostgreSQL connection string
- `target_table`: Name of the table to insert messages into
- `batch_size`: Number of messages to insert in each batch (default: 100)
- `max_connections`: Maximum database connections (default: 10)
- `auto_create_table`: Automatically create the target table if it doesn't exist (default: false)
- `include_metadata`: Include Iggy metadata columns (default: true)
- `store_raw_payload`: Store the original message payload (default: true)
- `raw_payload_column`: Name of the column for raw payload (default: "raw_payload")

## Table Schema

When `auto_create_table` is enabled, the following table structure is created:

```sql
CREATE TABLE iggy_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    iggy_offset BIGINT,
    iggy_timestamp TIMESTAMP WITH TIME ZONE,
    iggy_stream TEXT,
    iggy_topic TEXT,
    iggy_partition_id INTEGER,
    raw_payload JSONB,
    data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## Data Storage

### JSON Messages

JSON message payloads are stored in the `data` column as JSONB:

```json
{
  "user_id": 123,
  "name": "John Doe",
  "email": "john@example.com"
}
```

### Text Messages

Text messages are wrapped in a JSON object:

```json
{
  "text": "Hello, World!"
}
```

### Raw Binary Messages

Binary data is base64-encoded:

```json
{
  "raw_base64": "SGVsbG8gV29ybGQ="
}
```

### Protobuf Messages

Protobuf messages are stored as text:

```json
{
  "proto": "protobuf_text_representation"
}
```

## Usage Example

1. Configure the sink connector in your Iggy connectors runtime
2. Messages consumed from the specified topics will be inserted into PostgreSQL
3. Query the data using standard SQL:

```sql
SELECT * FROM iggy_messages WHERE iggy_stream = 'user_events';

SELECT data->>'user_id' as user_id, data->>'name' as name 
FROM iggy_messages 
WHERE data->>'user_id' IS NOT NULL;

SELECT * FROM iggy_messages 
WHERE created_at >= '2024-01-01' 
AND created_at < '2024-02-01';
```

## Requirements

- PostgreSQL 10+
- Database user with INSERT permissions on target table
- If `auto_create_table` is true: CREATE TABLE permissions
- Network connectivity between connector and PostgreSQL

## Performance Considerations

- Use appropriate `batch_size` for your workload (larger batches = better throughput)
- Consider creating indexes on frequently queried columns
- Monitor connection pool usage with `max_connections`
- Use JSONB indexes for efficient JSON queries:

```sql
CREATE INDEX idx_iggy_messages_data_gin ON iggy_messages USING GIN (data);
CREATE INDEX idx_iggy_messages_stream ON iggy_messages (iggy_stream);
CREATE INDEX idx_iggy_messages_topic ON iggy_messages (iggy_topic);
```
