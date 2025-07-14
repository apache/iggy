# PostgreSQL Sink Connector

The PostgreSQL sink connector allows you to consume messages from Iggy topics and store them in PostgreSQL databases.

## Features

- **Automatic Table Creation**: Optionally create tables automatically
- **Batch Processing**: Insert messages in configurable batches for performance
- **Metadata Storage**: Store Iggy message metadata (offset, timestamp, topic, etc.)
- **Raw Payload Storage**: Store original message payload as raw bytes
- **Flexible Data Handling**: Works with any payload type (JSON, text, binary, protobuf, etc.)
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
  "include_checksum": true,
  "include_origin_timestamp": true
}
```

### Configuration Options

- `connection_string`: PostgreSQL connection string
- `target_table`: Name of the table to insert messages into
- `batch_size`: Number of messages to insert in each batch (default: 100)
- `max_connections`: Maximum database connections (default: 10)
- `auto_create_table`: Automatically create the target table if it doesn't exist (default: false)
- `include_metadata`: Include Iggy metadata columns (default: true)
- `include_checksum`: Include message checksum (default: true)
- `include_origin_timestamp`: Include original message timestamp (default: true)

## Table Schema

When `auto_create_table` is enabled, the following table structure is created:

```sql
CREATE TABLE iggy_messages (
    id DECIMAL(39, 0) PRIMARY KEY,
    iggy_offset BIGINT,
    iggy_timestamp TIMESTAMP WITH TIME ZONE,
    iggy_stream TEXT,
    iggy_topic TEXT,
    iggy_partition_id INTEGER,
    iggy_checksum BIGINT,
    iggy_origin_timestamp TIMESTAMP WITH TIME ZONE,
    payload BYTEA,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## Data Storage

The payload is stored as raw bytes in the `payload` column, regardless of the original format:

### JSON Messages

JSON payloads are stored as UTF-8 encoded bytes. You can query them using PostgreSQL's JSON functions:

```sql
SELECT id, payload::text::jsonb->>'user_id' as user_id 
FROM iggy_messages 
WHERE payload::text::jsonb->>'user_id' IS NOT NULL;
```

### Text Messages

Text payloads are stored as UTF-8 encoded bytes:

```sql
SELECT id, convert_from(payload, 'UTF8') as message_text
FROM iggy_messages;
```

### Binary Messages

Binary data is stored directly as bytes:

```sql
SELECT id, encode(payload, 'base64') as payload_base64
FROM iggy_messages;
```

### Protocol Buffer Messages

Protobuf messages are stored as raw bytes and can be processed by your application:

```sql
SELECT id, payload, length(payload) as payload_size
FROM iggy_messages;
```

## Usage Example

1. Configure the sink connector in your Iggy connectors runtime
2. Messages consumed from the specified topics will be inserted into PostgreSQL
3. Query the data using standard SQL:

```sql
-- Get all messages from a specific stream
SELECT * FROM iggy_messages WHERE iggy_stream = 'user_events';

-- Get messages with their payload as text (for text/JSON payloads)
SELECT id, iggy_offset, convert_from(payload, 'UTF8') as payload_text
FROM iggy_messages 
WHERE iggy_stream = 'user_events';

-- Get messages from a specific time range
SELECT * FROM iggy_messages 
WHERE created_at >= '2024-01-01' 
AND created_at < '2024-02-01';

-- Get payload size statistics
SELECT 
    iggy_stream,
    iggy_topic,
    COUNT(*) as message_count,
    AVG(length(payload)) as avg_payload_size,
    MAX(length(payload)) as max_payload_size
FROM iggy_messages 
GROUP BY iggy_stream, iggy_topic;
```

## Performance Considerations

- Use appropriate `batch_size` for your workload (larger batches = better throughput)
- Consider creating indexes on frequently queried columns
- Monitor connection pool usage with `max_connections`
- Create indexes for efficient queries:

```sql
CREATE INDEX idx_iggy_messages_stream ON iggy_messages (iggy_stream);
CREATE INDEX idx_iggy_messages_topic ON iggy_messages (iggy_topic);
CREATE INDEX idx_iggy_messages_created_at ON iggy_messages (created_at);
CREATE INDEX idx_iggy_messages_offset ON iggy_messages (iggy_offset);
```

## Working with Different Payload Types

### JSON Payloads

For JSON data, you can use PostgreSQL's JSON operators:

```sql
-- Extract JSON fields (assuming payload is JSON)
SELECT 
    id,
    payload::text::jsonb->>'name' as name,
    payload::text::jsonb->>'email' as email
FROM iggy_messages 
WHERE payload::text::jsonb->>'name' IS NOT NULL;

-- Create a GIN index for faster JSON queries
CREATE INDEX idx_iggy_messages_payload_gin ON iggy_messages USING GIN ((payload::text::jsonb));
```

### Text Payloads

For text data, convert bytes to text:

```sql
-- Full-text search on text payloads
SELECT id, convert_from(payload, 'UTF8') as message
FROM iggy_messages 
WHERE convert_from(payload, 'UTF8') LIKE '%error%';
```

### Binary Payloads

For binary data, work with raw bytes or encode as needed:

```sql
-- Get binary payload as hex
SELECT id, encode(payload, 'hex') as payload_hex
FROM iggy_messages;

-- Get payload size
SELECT id, length(payload) as payload_size
FROM iggy_messages
ORDER BY payload_size DESC;
```
