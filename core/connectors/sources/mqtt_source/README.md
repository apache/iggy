# MQTT source connector

This dynamically loaded source plugin subscribes to an MQTT broker through
`rumqttc` and forwards each received MQTT publish payload to the Iggy connector
runtime through the existing `iggy_connector_sdk::source_connector!` ABI.

The connector is an external plugin. It does not modify the Iggy server or
define a private C ABI. The SDK macro exposes the lifecycle symbols consumed by
the connectors runtime.

## Current implementation boundary

- The plugin supports MQTT 3.1.1 through `rumqttc`'s default API and MQTT 5
  through `rumqttc::v5`. The `mqtt311` configuration name means MQTT 3.1.1;
  literal MQTT 3.1 is not supported by the current `rumqttc` release.
- The plugin accepts QoS 0, 1, and 2. Acknowledgement behavior is derived
  entirely from QoS: QoS 0 has no MQTT acknowledgement, QoS 1 is acknowledged
  with PUBACK after Iggy Ack, and QoS 2 starts its acknowledgement handshake
  with PUBREC after Iggy Ack.
- The driver always enables `rumqttc` manual acknowledgements for QoS 1/2 so
  the broker acknowledgement cannot be sent before Iggy accepts the message.
- For QoS 2, `AsyncClient::try_ack` sends `PUBREC` after Iggy Ack. `rumqttc`
  records the incoming packet identifier, then its event loop automatically
  answers the broker's `PUBREL` with `PUBCOMP`. The connector does not need to
  implement a second QoS 2 acknowledgement API.
- `session_expiry_interval` applies to MQTT 5. MQTT 3.1.1 uses its persistent
  session behavior through `clean_start = false`.
- `open` parses the broker URL, creates the `AsyncClient` and `EventLoop`,
  subscribes to every configured topic, and performs an initial broker poll.
- `poll` drives the protocol-specific event loop, accumulates up to
  `batch_size` publishes for at most `batch_timeout` after the first publish,
  normalizes them into the internal message shape, and returns one raw Iggy
  `ProducedMessages` batch.
- The source holds one bounded pending batch. Its acknowledgement token vector
  contains one token for each QoS 1/2 message in the batch and no token for QoS
  0 messages.
- The runtime's `Ack` result commits connector state and asks the driver to
  send the protocol-appropriate MQTT acknowledgement.
- A `Nack` drops the pending slot without acknowledging the publish. QoS 1/2
  may then be redelivered by the broker/session; QoS 0 cannot be redelivered.
- The persisted state currently records acknowledged-message count for
  lifecycle validation. It is not a broker cursor, so restart/reconnect
  delivery remains at-least-once and duplicates are possible.
- The plugin does not add a custom ABI. The SDK macro generates the runtime
  symbols: `iggy_source_open`, `iggy_source_handle_v2`,
  `iggy_source_batch_result`, `iggy_source_close`, and `iggy_source_version`.

The payload is emitted as `Schema::Raw` because MQTT payloads are opaque bytes.
The source also maps MQTT delivery metadata into Iggy message headers. Headers
survive the runtime's source forwarding path and can be consumed by transforms
or a future MQTT sink.

## MQTT header contract

Every produced message contains these headers:

| Header | Type | Meaning |
| --- | --- | --- |
| `mqtt.protocol` | string | `mqtt311` or `mqtt5` |
| `mqtt.topic` | string | Topic on which the broker delivered the message |
| `mqtt.qos` | unsigned integer | Delivered MQTT QoS: `0`, `1`, or `2` |
| `mqtt.dup` | boolean | MQTT duplicate-delivery flag |
| `mqtt.retain` | boolean | MQTT retained-message flag |
| `mqtt.packet_id` | unsigned integer, optional | Packet identifier for QoS 1/2; absent for QoS 0 |

MQTT 5 publish properties are added when present:

| Header | Type | Meaning |
| --- | --- | --- |
| `mqtt.payload_format_indicator` | boolean | MQTT payload format indicator |
| `mqtt.message_expiry_interval` | unsigned integer | Message expiry interval |
| `mqtt.topic_alias` | unsigned integer | MQTT topic alias |
| `mqtt.response_topic` | string | MQTT response topic |
| `mqtt.correlation_data` | raw bytes | MQTT correlation data |
| `mqtt.content_type` | string | MQTT content type |
| `mqtt.user_property.<index>.key` | string | MQTT user-property key |
| `mqtt.user_property.<index>.value` | string | MQTT user-property value |
| `mqtt.subscription_identifier.<index>` | unsigned integer | Matching subscription identifier |

The index for repeated properties starts at zero and preserves the order
provided by the broker. Header values are subject to Iggy's header size limits;
an oversized mapped property causes message normalization to fail rather than
silently truncating metadata.

`mqtt.packet_id` and `mqtt.dup` describe the incoming broker delivery. They
must not be reused as identifiers for a future outgoing MQTT publish. An MQTT
sink must let its own client session generate outgoing packet identifiers and
duplicate flags.

The driver abstraction is private to the plugin. It keeps the MQTT 3.1.1 and
MQTT 5 client types, event loops, and acknowledgement tokens out of the Iggy
source trait and FFI layer.

## Source batching decisions

The source-side batch is separate from the Iggy producer batch. Plugin batching
reduces FFI calls, serialization overhead, and acknowledgement bookkeeping
before the runtime receives the messages.

The agreed behavior is:

```text
first MQTT publish arrives
    │
    ├── collect until batch_size is reached
    └── or flush when batch_timeout expires
         │
         ▼
ProducedMessages with one bounded pending batch
```

- A partial batch is flushed when `batch_timeout` expires.
- `batch_timeout` starts when the first message in the batch arrives and does
  not reset for later messages.
- QoS 0 contributes a message but no acknowledgement token.
- QoS 1 contributes one token and is acknowledged with PUBACK after Iggy Ack.
- QoS 2 contributes one token and begins its PUBREC/PUBREL/PUBCOMP sequence
  after Iggy Ack through `rumqttc`.
- A batch Nack acknowledges none of its MQTT messages.
- MQTT acknowledgement failures are retried within the current batch. The
  driver makes at most six attempts for each token, pumping the rumqttc event
  loop between attempts and waiting up to 10 ms between attempts. Exhausted
  retries preserve the unacknowledged tokens and stop the source so they can
  be redelivered rather than discarded.
- The driver buffers incoming messages only within bounded batch capacity while
  making progress on a full rumqttc request channel.

The source remains at-least-once. Acknowledging a batch does not guarantee that
no duplicate will appear after a process or broker restart.

## Integration-test execution decision

MQTT integration tests use the `emqx/emqx:latest` image. The current latest
image can take longer to initialize than older EMQX images, especially when
several containers start simultaneously. The MQTT integration suite should
therefore be executed serially:

```bash
cargo test -p integration --test mod connectors::mqtt::mqtt_source -- \
  --test-threads=1
```

This avoids treating host-side container startup contention as a connector
failure. A future fixture improvement may reuse one authenticated EMQX
container for the complete MQTT suite.

## Remaining batching improvements

The following items are intentionally tracked separately from the initial
batching implementation:

1. Add packet-level QoS 2 ordering assertions for every token in a full batch.
2. Measure throughput and memory usage at different `batch_size` and
   `request_capacity` values.
3. Consider reusing one EMQX fixture instead of serially creating a container
   per test.

These improvements do not require changes to the Iggy server, connectors
runtime, SDK trait, or FFI symbols.

## Configuration

The runtime configuration has two layers:

1. The connector file configures the plugin path and the destination Iggy
   stream/topic.
2. The `[plugin_config]` table configures the MQTT broker connection. The
   runtime serializes this table and passes it to the plugin as JSON.

The repository contains two useful examples:

- [`config.toml`](config.toml) is a standalone source connector configuration.
  It is safe to copy into a local connector configuration directory after
  replacing the broker settings.
- [`../../runtime/example_config/connectors/mqtt_source.toml`](../../runtime/example_config/connectors/mqtt_source.toml)
  is the runtime example used with the example connector configuration.

The MQTT plugin fields are:

| Field | Required | Description |
| --- | --- | --- |
| `broker_url` | yes | `mqtt://` or another URL supported by `rumqttc` |
| `subscriptions` | yes | Non-empty list of MQTT topic filters |
| `protocol` | no | `mqtt311` or `mqtt5`; defaults to `mqtt5` |
| `qos` | no | Subscription QoS `0`, `1`, or `2`; defaults to `1` |
| `subscription_qos` | no | Exact topic-filter overrides for `qos`; every key must be listed in `subscriptions` and every value must be `0`, `1`, or `2` |
| `client_id` | no | MQTT client identifier; the runtime ID is used when absent |
| `username` / `password` | no | Must be supplied together |
| `clean_start` | no | MQTT clean-session/clean-start setting |
| `session_expiry_interval` | no | MQTT 5 session expiry interval |
| `keep_alive` | no | Duration; defaults to `30s` |
| `poll_timeout` | no | Duration; defaults to `1s` |
| `request_capacity` | no | Positive rumqttc request-channel capacity; defaults to `32` |
| `batch_size` | no | Maximum MQTT messages returned by one source poll; defaults to `100` |
| `batch_timeout` | no | Maximum accumulation time after the first message; defaults to `10ms` |
| `verbose_logging` | no | Enables additional plugin logging |

The plugin-side batch is bounded by `batch_size`. The runtime still applies its
own Iggy producer `batch_length` and `linger_time` settings after the FFI
boundary. `request_capacity` remains the bounded rumqttc request-channel
capacity; when it temporarily fills while acknowledgements are queued, the
driver advances the event loop and buffers incoming messages up to the bounded
batch capacity instead of dropping them.

### Per-subscription QoS

`qos` is the default subscription QoS. The optional `subscription_qos` table
overrides it for individual filters using exact string matches:

```toml
subscriptions = [
  "devices/+/telemetry",
  "devices/+/status",
]
qos = 1

[plugin_config.subscription_qos]
"devices/+/telemetry" = 2
"devices/+/status" = 0
```

The connector rejects duplicate filters, overrides for filters that are not in
`subscriptions`, and values outside `0`, `1`, and `2`. The same resolution is
used for MQTT 3.1.1 and MQTT 5 subscription setup. The QoS of an incoming
publish and its acknowledgement token still come from the broker delivery,
not from this configuration table.

## Route-specific destinations

The current source FFI contract does not carry a destination stream or topic,
and the runtime creates one Iggy producer for each source configuration. The
first production routing model therefore uses one MQTT source instance per
route. Each instance owns a non-overlapping MQTT subscription and one static
Iggy destination:

```text
mqtt_site_a.toml
  devices/site-a/# → site_a / telemetry

mqtt_site_b.toml
  devices/site-b/# → site_b / telemetry
```

The example files are [`mqtt_site_a.toml`](../../runtime/example_config/connectors/mqtt_site_a.toml)
and [`mqtt_site_b.toml`](../../runtime/example_config/connectors/mqtt_site_b.toml).
Use unique connector keys and MQTT client IDs for every route. Keep the topic
filters non-overlapping unless duplicate delivery to multiple Iggy destinations
is intentional.

This approach requires no SDK, FFI, runtime, or plugin code change. The tradeoff
is one MQTT connection, source lifecycle, state file, and set of metrics per
route. A future single-instance dynamic-routing design would require the FFI
message to carry a destination and the runtime to manage producers keyed by
that destination.

Credentials use secret-aware types inside the plugin and must not be committed
to this repository. Use a local secret file, an environment override, or a
secret-management system for deployed credentials.

## Configuration paths and environment overrides

With the local configuration provider, the runtime reads one main runtime
configuration file and then loads connector `.toml` files from its configured
connector directory:

```toml
# core/connectors/runtime/config.toml
[connectors]
config_type = "local"
config_dir = "core/connectors/runtime/example_config/connectors"
```

The main runtime file can be selected with:

```bash
IGGY_CONNECTORS_CONFIG_PATH=core/connectors/runtime/config.toml \
  cargo run --bin iggy-connectors
```

The connector directory can be overridden with
`IGGY_CONNECTORS_CONNECTORS_CONFIG_DIR`. This is useful for testing one MQTT
configuration without loading the repository's other connector files:

```bash
mqtt_config_dir="$(mktemp -d)"
cp core/connectors/runtime/example_config/connectors/mqtt_source.toml \
  "$mqtt_config_dir/mqtt_source.toml"

IGGY_CONNECTORS_CONFIG_PATH=core/connectors/runtime/config.toml \
IGGY_CONNECTORS_CONNECTORS_CONFIG_DIR="$mqtt_config_dir" \
  cargo run --bin iggy-connectors
```

For a local-provider connector with key `mqtt`, plugin fields can be overridden
without editing the connector file. The prefix is:

```text
IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_<FIELD>
```

Examples:

```bash
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_BROKER_URL="mqtt://127.0.0.1:1883"
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_USERNAME="emqx-user"
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_PASSWORD="replace-me"
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_QOS=1
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_PROTOCOL="mqtt5"
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_SUBSCRIPTIONS='["devices/+/telemetry"]'
```

Scalar values are parsed according to their JSON/TOML type. Lists such as
`subscriptions` must be supplied as a JSON array. The runtime applies these
overrides after reading the connector file, so an environment value wins over
the matching file value. The runtime loads a `.env` file from the current
directory by default, or a specific dotenv file using
`IGGY_CONNECTORS_ENV_PATH`.

Build the plugin with:

```bash
cargo build -p iggy_connector_mqtt_source --release
```

The resulting shared library is placed under `target/release/` and can be
referenced by the connectors runtime using its normal plugin path setting.

Before starting the runtime, make sure the configured `path` points to the
library built for the selected profile. For a release build this is normally
`target/release/libiggy_connector_mqtt_source.so` on Linux.
