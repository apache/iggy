# DynamoDB Sink Connector

A sink connector that consumes messages from Iggy streams and writes them to an
Amazon DynamoDB table with `BatchWriteItem` through the official AWS SDK.

## Configuration

```toml
[plugin_config]
table = "iggy_messages"
region = "us-east-1"
# endpoint = "http://localhost:8000"
# access_key_id = "..."
# secret_access_key = "..."
# session_token = "..."
partition_key_field = "iggy_id"
# sort_key_field = "iggy_offset"
batch_size = 25
include_metadata = true
include_checksum = true
include_origin_timestamp = true
max_item_size = 409600
max_retries = 3
retry_delay = "500ms"
max_retry_delay = "5s"
verbose_logging = false
```

- `table`: Target DynamoDB table. The table must already exist.
- `region`: AWS region. Falls back to the default AWS region chain when unset.
- `endpoint`: Custom endpoint URL, for DynamoDB Local or a VPC endpoint.
- `access_key_id` / `secret_access_key` / `session_token`: Static credentials.
  Provide both `access_key_id` and `secret_access_key`, or neither. When both
  are omitted the connector uses the default AWS credential chain.
- `partition_key_field`: Item attribute used as the table partition key.
  Defaults to `iggy_id`.
- `sort_key_field`: Item attribute used as the table sort key. Only set this
  when the table has a sort key.
- `batch_size`: Items per `BatchWriteItem` request. Defaults to `25`, which is
  also the DynamoDB limit, so larger values are clamped.
- `include_metadata`: Add `iggy_stream`, `iggy_topic`, `iggy_partition_id`,
  `iggy_offset`, and `iggy_timestamp` to each item. Defaults to `true`.
- `include_checksum`: Add `iggy_checksum`. Defaults to `true`.
- `include_origin_timestamp`: Add `iggy_origin_timestamp`. Defaults to `true`.
- `max_item_size`: Maximum item size in bytes. Defaults to `409600` (400 KB),
  which is also the DynamoDB limit, so larger values are clamped.
- `max_retries`: Retries after the first attempt. Defaults to `3`.
- `retry_delay`: First retry delay as a humantime string. Defaults to `500ms`.
- `max_retry_delay`: Upper bound of the exponential backoff. Defaults to `5s`.
- `verbose_logging`: Log per-batch results at info level. Defaults to `false`.

## Behavior

JSON objects are written attribute by attribute, so a message field becomes a
DynamoDB attribute of the matching type. JSON arrays and scalars are nested
under a `payload` attribute, because a DynamoDB item must be a map. Text
payloads go into `payload` as a string. Raw payloads are parsed as JSON when
possible, otherwise they are stored as binary. Protobuf, FlatBuffer, and Avro
payloads are not supported and are skipped with a warning.

Metadata attributes are written after the payload, so they overwrite payload
fields of the same name.

## Keys and Idempotency

`BatchWriteItem` uses `PutRequest`, which overwrites an existing item with the
same primary key. Redelivery of the same message therefore writes the same item
again as long as the key is deterministic.

When the payload does not carry the configured `partition_key_field`, the
connector injects a key built from the stream, topic, partition, and message ID.
When `sort_key_field` is configured and missing, the message offset is injected.
A payload value always wins over the injected one, so a message that carries the
key field with an empty value, or with a value that is neither a string, a
number, nor binary, is skipped rather than falling back to the injected key,
because DynamoDB would reject the whole batch.

The key fields are checked against the table on startup. A `partition_key_field`
or `sort_key_field` that does not match the table key schema fails the connector
while it opens, instead of on the first write.

DynamoDB rejects a request that writes the same key twice, so within one
`consume()` call only the newest item per key is sent.

## Retries

Unprocessed items returned by `BatchWriteItem` are retried with exponential
backoff, so a partially throttled batch is not silently dropped. Throttling and
server errors such as `ProvisionedThroughputExceededException`,
`ThrottlingException`, and `InternalServerError` are retried the same way, which
covers both provisioned and on-demand capacity modes. Validation and access
errors are permanent and returned without a retry.

Items larger than `max_item_size` are logged and skipped instead of failing the
whole batch.
