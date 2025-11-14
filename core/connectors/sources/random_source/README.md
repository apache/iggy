# Random Source

The Random Source connector generates random data and sends it to the specified stream(s).

## Configuration

- `interval`: A string representing the interval at which the connector generates random data. Defaults to `"1s"`.
- `max_count`: An integer representing the maximum number of messages to generate. Defaults to none.
- `messages_range`: An array of two integers representing the range of amount of messages to generate. Defaults to `[10, 50]`.
- `payload_size`: An integer representing the size in bytes of the `text` field payload to generate. Defaults to `100`.

```toml
[plugin_config]
interval = "100ms"
max_count = 1000
messages_range = [10, 50]
payload_size = 200
```
