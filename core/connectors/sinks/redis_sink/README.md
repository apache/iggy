# Redis Sink

Writes messages from Iggy streams into [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) using pipelined `XADD`.

## Configuration

```toml
[plugin_config]
connection_url = "redis://localhost:6379"
mode = "stream"
stream_key = "iggy:events"
batch_size = 100
payload_format = "json"
include_metadata = true
deterministic_ids = true
max_len = 100000
max_retries = 3
retry_delay = "1s"
response_timeout = "5s"
```

| Field               | Default  | Description                                                                                                        |
| ------------------- | -------- | ------------------------------------------------------------------------------------------------------------------ |
| `connection_url`    | required | Redis connection URL. May carry credentials, never logged. `url` is accepted as a legacy alias                     |
| `mode`              | `stream` | Connector mode. Redis Streams is the only v1 mode                                                                  |
| `stream_key`        | required | Redis Stream key prefix, see [Idempotency](#idempotency)                                                           |
| `batch_size`        | `100`    | Entries per pipeline round trip                                                                                    |
| `payload_format`    | `binary` | `binary` stores bytes as-is. `text` requires UTF-8. `json` requires a valid JSON document                          |
| `include_metadata`  | `true`   | Adds `message_id`, `offset`, `partition_id`, `stream`, `topic`, `timestamp`, `origin_timestamp`, `checksum` fields |
| `deterministic_ids` | `true`   | Derives the entry ID from the message offset                                                                       |
| `max_len`           | none     | Approximate `MAXLEN ~` trim applied on every `XADD`                                                                |
| `max_retries`       | `3`      | Total attempts for connection failures, including the first attempt and capped at 3                                |
| `retry_delay`       | `1s`     | Base backoff delay                                                                                                 |
| `response_timeout`  | `5s`     | Time to wait for Redis to answer one pipeline before the attempt counts as a transient failure                     |

Each entry holds a `payload` field plus the optional metadata fields. User headers are stored in a `headers` JSON object when `include_metadata` is `true`; raw values use base64 with an `iggy_header_encoding` marker.

## Idempotency

With `deterministic_ids = true` the entry ID is `<offset + 1>-0`. Redis refuses an explicit ID that is not greater than the stream's last ID, so an entry that is sent twice, for example when the sink retries after a lost response, is rejected and counted as a skipped duplicate instead of being written twice. Skipped entries are logged at `warn`.

Offsets only grow within one partition, so the sink writes to `<stream_key>:<stream>:<topic>:<partition_id>`.

The same rule skips any entry whose offset is not above the stream's last ID. If a topic is recreated and its offsets restart from 0 while the Redis key still exists, new messages are skipped until the offsets pass the old ones. Delete the key or use a new `stream_key` in that case.

With `deterministic_ids = false` Redis assigns IDs (`*`), all messages go to `stream_key` as is, and a retried batch can be written twice (at-least-once).

## Errors

Connection-level failures are retried with backoff. If the retries run out, `consume` returns an error and the sink does not write the remaining chunks of that call.

If Redis rejects some entries of a pipeline for another reason (for example `WRONGTYPE` or an out-of-memory error), the other entries of that pipeline are still written, and `consume` then returns an error. The connectors runtime logs the error and counts it, then carries on with the next batch. It does not redeliver the failed batch, so the rejected entries are not written.

A message that fails the `text` or `json` payload check, or whose headers cannot be encoded, is skipped and counted in the `errors` total. The rest of the batch is still written. The sink skips these messages, like the runtime skips messages it cannot decode, because an error return would drop the valid messages of the same batch too. Skipped messages are logged at `error` with their offset.
