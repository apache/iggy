# Random Source

The Random Source connector generates random data and sends it to the configured stream and topic.

## Configuration

- `interval`: A string representing the interval at which the connector generates random data. Defaults to `"1s"`; invalid duration strings also fall back to `"1s"`.
- `max_count`: An integer representing the maximum number of messages to generate. Omit for no limit; `0` produces no messages. The last batch is capped by the remaining count.
- `messages_range`: An array of two integers specifying the half-open message-count range (lower included, upper excluded). Defaults to `[10, 50]`. Non-increasing ranges return a configuration error from polling.
- `payload_size`: An integer representing the size in bytes of the `text` field payload to generate. Defaults to `100`.

```toml
[plugin_config]
interval = "100ms"
max_count = 1000
messages_range = [10, 50]
payload_size = 200
```

The count is staged while a batch is in flight and committed on acknowledgement, after delivery and checkpoint storage. A restored checkpoint preserves the count across restarts; missing or undecodable state starts from zero. After reaching the limit, polling continues with empty batches and no new checkpoint.
