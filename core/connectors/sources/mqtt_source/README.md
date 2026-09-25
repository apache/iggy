# Apache Iggy MQTT Source Connector (`iggy_connector_mqtt_source`)

The **MQTT Source Connector** is a dynamically loaded shared library plugin (`.so` / `.dylib` / `.dll`) for Apache Iggy. Built using the `iggy_connector_sdk::source_connector!` C-FFI ABI and the [`rumqttc`](https://crates.io/crates/rumqttc) asynchronous client, it ingests telemetry and event streams from external MQTT brokers (such as **EMQX** or Mosquitto) directly into persistent **Apache Iggy** streams, topics, and partitions.

---

## Architecture Overview

```text
  [ IoT Edge Devices ]
           │
           │ MQTT 3.1.1 / MQTT 5 (Sensor Telemetry & Events)
           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           EMQX BROKER                                    │
│  • Client Authentication, Authorizations, & Session Queues              │
│  • Topic Subscriptions, Wildcards (+, #), & QoS Management              │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     │ MQTT / TLS Connection
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                   IGGY CONNECTORS RUNTIME PROCESS                        │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  MQTT Source Plugin (libiggy_connector_mqtt_source.so)             │  │
│  │  • Embedded `rumqttc` Client (`AsyncClient` + `EventLoop`)         │  │
│  │  • Normalizes MQTT Packets ──► Iggy Payload & Metadata Headers     │  │
│  │  • Holds QoS 1/2 PUBACK until Iggy Confirms Persistence            │  │
│  └─────────────────────────────────┬──────────────────────────────────┘  │
│                                    │                                     │
│                                    │ C-ABI FFI Boundary                  │
│                                    │ (iggy_source_open, poll, ack, etc)  │
│                                    ▼                                     │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  Iggy Connector Runtime Core                                       │  │
│  │  • Schema Decoders / Encoders (`Schema::Raw`)                      │  │
│  │  • Optional Field Transformations (Add/Delete/Rename)              │  │
│  │  • State Checkpoint Storage (File / HTTP endpoint)                 │  │
│  └─────────────────────────────────┬──────────────────────────────────┘  │
└────────────────────────────────────┼─────────────────────────────────────┘
                                     │
                                     │ Native Binary Client Protocol
                                     ▼ (TCP / QUIC)
┌──────────────────────────────────────────────────────────────────────────┐
│                            IGGY SERVER                                   │
│  • Thread-Per-Core Shared-Nothing Storage Engine (`io_uring`)            │
│  • Appends Messages to Stream ──► Topic ──► Partition Disk Logs          │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Features

* **Multi-Protocol Support**: Supports **MQTT 3.1.1** (`mqtt311`) and **MQTT 5** (`mqtt5`) protocols.
* **Quality of Service (QoS)**: Ingests QoS 0, QoS 1, and QoS 2 messages.
* **Deferred Acknowledgment (Manual ACKs)**: Configures `rumqttc` with manual acknowledgments enabled. For QoS 1/2 messages, broker acknowledgments (`PUBACK` / `PUBREC`) are held pending and only sent after `iggy-server` confirms persistence.
* **Per-Subscription QoS Overrides**: Configure a global fallback QoS alongside per-subscription overrides for specific topic filters.
* **TLS & Mutual TLS (mTLS)**: Supports secure broker URLs (`mqtts://` / `ssl://`) with system trust roots or custom CA bundles (`ca_file`), client certificates (`client_cert_file`), and client keys (`client_key_file`) via Rustls.
* **Source-Side Micro-Batching**: Micro-batches incoming messages up to `batch_size` or until `batch_timeout` fires, minimizing FFI serialization and network overhead.
* **Rich Header Mapping**: Preserves MQTT metadata (topic, protocol, QoS, retain, dup, packet ID) and MQTT 5 properties (content type, response topic, correlation data, user properties) as Iggy message headers.
* **Route Isolation**: Run multiple connector instances in parallel to route distinct MQTT topic filters into separate Iggy streams and topics.
* **Process Isolation**: Operates inside the `iggy-connectors` process memory space via C-ABI FFI, keeping the core `iggy-server` decoupled and untouched.

---

## Configuration Architecture

Configuration operates in **two distinct layers**:

1. **Runtime Layer**: Configured in the connector TOML file (e.g., `mqtt_source.toml`). Defines the connector identity, shared library path, and destination Iggy stream/topic mapping.
2. **Plugin Layer (`[plugin_config]`)**: Defines MQTT-specific connection parameters, credentials, subscriptions, QoS levels, and TLS configurations. The runtime passes this table to the plugin serialized as JSON across the FFI boundary.

---

## Configuration Options (`[plugin_config]`)

| Field | Type | Required | Default | Description |
| :--- | :--- | :---: | :--- | :--- |
| `broker_url` | String | **Yes** | — | Broker URL scheme (`mqtt://`, `mqtts://`, or `ssl://`) and host/port. |
| `subscriptions` | Array[String] | **Yes** | — | Non-empty list of unique MQTT topic filters (wildcards `+` and `#` supported). |
| `protocol` | String | No | `"mqtt5"` | MQTT protocol version: `"mqtt5"` or `"mqtt311"`. |
| `qos` | Integer | No | `1` | Default global subscription QoS (`0`, `1`, or `2`). |
| `subscription_qos` | Table | No | `{}` | Map of topic-filter strings to explicit QoS overrides (`0`, `1`, or `2`). |
| `client_id` | String | No | `iggy-mqtt-source-{id}` | MQTT client identifier. |
| `username` | String | No | None | Broker authentication username (must be supplied together with `password`). |
| `password` | String | No | None | Broker authentication password (wrapped as a secret, never logged). |
| `clean_start` | Boolean | No | `false` | MQTT clean start flag (or clean session for MQTT 3.1.1). |
| `session_expiry_interval` | Integer | No | None | MQTT 5 session expiry interval in seconds. |
| `keep_alive` | Duration | No | `"30s"` | Ping interval string (minimum `1s` for MQTT 3.1.1, minimum `5s` for MQTT 5). |
| `poll_timeout` | Duration | No | `"1s"` | Maximum duration spent waiting on network events per driver poll tick. |
| `request_capacity` | Integer | No | `32` | Bounded capacity for `rumqttc` internal request channel (must be `> 0`). |
| `batch_size` | Integer | No | `100` | Maximum messages accumulated into a single source batch. |
| `batch_timeout` | Duration | No | `"10ms"` | Maximum wait duration after the first message arrives before flushing a batch. |
| `verbose_logging` | Boolean | No | `false` | Enables additional debug logging inside the plugin driver. |
| `tls.ca_file` | String | No | None | File path to custom CA root certificates in PEM format. |
| `tls.client_cert_file` | String | No | None | File path to mTLS client certificate in PEM format (must pair with `client_key_file`). |
| `tls.client_key_file` | String | No | None | File path to mTLS client private key in PEM format (must pair with `client_cert_file`). |
| `tls.server_name` | String | No | None | TLS Server Name Indication (SNI) string (must match `broker_url` host). |

---

## Configuration Examples

### 1. Basic Production Telemetry (QoS 1 with Credentials)

```toml
type = "source"
key = "mqtt_telemetry"
enabled = true
version = 1
name = "MQTT Telemetry Source"
path = "target/release/libiggy_connector_mqtt_source"
plugin_config_format = "json"

[[streams]]
stream = "iot"
topic = "telemetry"
schema = "raw"
batch_length = 100
linger_time = "5ms"

[plugin_config]
broker_url = "mqtt://127.0.0.1:1883"
subscriptions = ["devices/+/telemetry"]
protocol = "mqtt5"
qos = 1
client_id = "iggy-telemetry-source"
username = "iggy_app"
password = "local-secret-password"
clean_start = false
session_expiry_interval = 3600
keep_alive = "30s"
batch_size = 100
batch_timeout = "10ms"
```

### 2. Per-Subscription QoS Overrides

```toml
[plugin_config]
broker_url = "mqtt://127.0.0.1:1883"
subscriptions = [
  "devices/+/telemetry",
  "devices/+/alerts",
  "devices/+/diagnostics"
]
qos = 1 # Fallback for devices/+/alerts

[plugin_config.subscription_qos]
"devices/+/telemetry" = 2   # High-importance telemetry via QoS 2
"devices/+/diagnostics" = 0 # Disposable diagnostic metrics via QoS 0
```

### 3. Secure TLS & Mutual TLS (mTLS) Setup

```toml
[plugin_config]
broker_url = "mqtts://emqx.example.com:8883"
subscriptions = ["factory/+/metrics"]
protocol = "mqtt5"
qos = 1

[plugin_config.tls]
ca_file = "/etc/iggy/certs/ca.pem"
client_cert_file = "/etc/iggy/certs/client-cert.pem"
client_key_file = "/etc/iggy/certs/client-key.pem"
server_name = "emqx.example.com"
```

### 4. Multi-Instance Route Isolation

To route distinct MQTT topic filters to different Iggy streams or topics, run
one source connector instance per route. The runtime loads every connector TOML
from its configured `config_dir`. For the standard example layout, create these
files locally:

`core/connectors/runtime/example_config/connectors/mqtt_site_a.toml`:

```toml
type = "source"
key = "mqtt_site_a"
enabled = true
version = 1
name = "MQTT source - site A"
path = "target/release/libiggy_connector_mqtt_source"
plugin_config_format = "json"

[[streams]]
stream = "site_a"
topic = "telemetry"
schema = "raw"
batch_length = 100
linger_time = "5ms"

[plugin_config]
broker_url = "mqtt://127.0.0.1:1883"
subscriptions = ["devices/site-a/#"]
protocol = "mqtt5"
qos = 1
client_id = "iggy-mqtt-source-site-a"
clean_start = false
session_expiry_interval = 3600
keep_alive = "30s"
poll_timeout = "1s"
request_capacity = 32
batch_size = 100
batch_timeout = "10ms"
verbose_logging = false
```

`core/connectors/runtime/example_config/connectors/mqtt_site_b.toml`:

```toml
type = "source"
key = "mqtt_site_b"
enabled = true
version = 1
name = "MQTT source - site B"
path = "target/release/libiggy_connector_mqtt_source"
plugin_config_format = "json"

[[streams]]
stream = "site_b"
topic = "telemetry"
schema = "raw"
batch_length = 100
linger_time = "5ms"

[plugin_config]
broker_url = "mqtt://127.0.0.1:1883"
subscriptions = ["devices/site-b/#"]
protocol = "mqtt5"
qos = 1
client_id = "iggy-mqtt-source-site-b"
clean_start = false
session_expiry_interval = 3600
keep_alive = "30s"
poll_timeout = "1s"
request_capacity = 32
batch_size = 100
batch_timeout = "10ms"
verbose_logging = false
```

Use unique connector keys and MQTT client IDs for every route. Keep topic
filters non-overlapping unless duplicate delivery to multiple Iggy destinations
is intentional. Build the plugin first, copy these files into the active
connector `config_dir`, and restart `iggy-connectors`.

---

## Environment Variable Overrides

Any property inside `[plugin_config]` can be overridden at runtime using environment variables without modifying configuration files:

```bash
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_BROKER_URL="mqtt://127.0.0.1:1883"
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_USERNAME="emqx-user"
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_PASSWORD="secret-password"
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_QOS=1
export IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_SUBSCRIPTIONS='["devices/+/telemetry"]'
```

---

## Message Header Reference

The connector attaches MQTT metadata directly to `ProducedMessage.headers`:

| Header Key | Type | Description |
| :--- | :--- | :--- |
| `mqtt.protocol` | String | `"mqtt311"` or `"mqtt5"`. |
| `mqtt.topic` | String | Topic filter on which the broker delivered the message. |
| `mqtt.qos` | Integer | Delivered MQTT QoS (`0`, `1`, or `2`). |
| `mqtt.dup` | Boolean | Duplicate delivery flag from broker. |
| `mqtt.retain` | Boolean | Retained message flag. |
| `mqtt.packet_id` | Integer | MQTT Packet ID for QoS 1/2 (absent for QoS 0). |
| `mqtt.content_type` | String | *(MQTT 5)* Content type descriptor. |
| `mqtt.response_topic` | String | *(MQTT 5)* Response topic for request/reply flows. |
| `mqtt.correlation_data` | Bytes | *(MQTT 5)* Correlation binary data. |
| `mqtt.message_expiry_interval` | Integer | *(MQTT 5)* Message expiry interval in seconds. |
| `mqtt.user_property.<N>.key` | String | *(MQTT 5)* User property key at index `N`. |
| `mqtt.user_property.<N>.value` | String | *(MQTT 5)* User property value at index `N`. |

---

## Redelivery & Acknowledgment Mechanism

The connector guarantees **At-Least-Once Delivery** for QoS 1 and QoS 2 messages using manual protocol acknowledgments.

```text
  MQTT Broker             MQTT Source Plugin            Connector Runtime           Iggy Server
       │                         │                              │                        │
       │ 1. MQTT PUBLISH (QoS 1) │                              │                        │
       ├────────────────────────►│                              │                        │
       │                         │ [Message held pending]       │                        │
       │                         │ [NO PUBACK SENT YET]         │                        │
       │                         │                              │                        │
       │                         │ 2. Source::poll() batch      │                        │
       │                         ├─────────────────────────────►│                        │
       │                         │                              │ 3. Send over TCP/QUIC  │
       │                         │                              ├───────────────────────►│
       │                         │                              │                        │
       │                         │                              │ 4. Persists to log     │
       │                         │                              │◄───────────────────────┤
       │                         │                              │    Returns Iggy ACK    │
       │                         │                              │                        │
       │                         │ 5. SourceBatchResult::Ack    │                        │
       │                         │◄─────────────────────────────┤                        │
       │                         │                              │                        │
       │ 6. MQTT PUBACK          │                              │                        │
       │◄────────────────────────┤                              │                        │
       │                         │ [Pending message cleared]    │                        │
```

### Transaction Steps

1. **Inbound Staging**: When `rumqttc` receives a QoS 1 or QoS 2 `PUBLISH` packet, the driver stages the message into an internal pending buffer along with its deferred `AckToken`. **No `PUBACK` or `PUBREC` is sent to the broker yet**.
2. **Polling & FFI Handoff**: The runtime calls `Source::poll()`. The plugin packages staged messages into a batch, records `AckTokens` inside `pending_batch`, serializes candidate `ConnectorState`, and returns `ProducedMessages` with `Schema::Raw`.
3. **Iggy Persistence & State Save**: The runtime sends the batch to `iggy-server` over TCP/QUIC and saves candidate state checkpoints to disk or HTTP storage.
4. **Ack Callback**: Upon successful Iggy write, the runtime calls `Source::on_batch_result(Ack)`.
5. **Broker Acknowledgment**: The plugin retrieves in-flight `AckTokens` and executes `client.try_ack(&publish)`. `rumqttc` transmits the `PUBACK` (for QoS 1) or initiates `PUBREC` (for QoS 2) to EMQX.
6. **Nack Rollback**: If Iggy delivery fails or times out, the runtime returns `SourceBatchResult::Nack`. The plugin drops the candidate state **without acknowledging the MQTT tokens**. The broker's QoS 1/2 retry loop will subsequently redeliver the unacknowledged messages.

---

## Failure Modes & Observed Redelivery Behaviors

| Failure Scenario | Current Observation | Explanation & Root Cause |
| :--- | :--- | :--- |
| **Broker terminates / restarts** | Message may disappear | EMQX may lose its in-flight MQTT session state upon restart. Even with persistent storage enabled, a standard Docker data volume does not guarantee the preservation of every unacknowledged packet across container recreations. |
| **Iggy terminates / restarts only** | No immediate redelivery | The MQTT source plugin and runtime remain connected to the broker. The plugin holds `PUBACK`/`PUBREC` pending while waiting for Iggy to recover. Because the active TCP/MQTT session never drops, the broker does not trigger an immediate redelivery while the session timer remains active. |
| **Iggy and Connector restart** | Message redelivered | Restarting the connector drops the underlying TCP socket and initiates a new MQTT connection. Upon reconnecting (with `clean_start = false`), EMQX detects the unacknowledged QoS 1/2 message in the session queue and redelivers it. |
| **Connector terminates / restarts** | Message redelivered | EMQX remains online and retains the unacknowledged packet in its active MQTT session. Once the connector restarts and re-establishes its session, EMQX immediately redelivers the unacknowledged packet. |

---

## Critical End-User & Operational Notes

1. **At-Least-Once Delivery & Duplicate Tolerance**:
   * The persisted `ConnectorState` records an `acknowledged_messages: u64` count for runtime tracking—**it is not a durable MQTT broker cursor**.
   * If a crash or network drop occurs after Iggy persists a batch but before `PUBACK` reaches the MQTT broker, the broker will redeliver those messages upon reconnecting. **Downstream Iggy consumers must be designed to handle duplicate messages idempotently**.

2. **Single Destination per Connector Instance**:
   * A single source connector instance writes to **one** static Iggy stream and topic.
   * Dynamic per-message destination routing based on MQTT topics is not supported within a single instance; use separate connector files for distinct target streams/topics.

3. **Packet ID Scope**:
   * MQTT `packet_id` values (e.g., `42`) are ephemeral session tokens generated by the client/broker for connection handshakes. They are reused across connections and **must never be used as durable deduplication keys or offset identifiers** in Iggy.

4. **Single In-Flight Batch Constraint**:
   * The Iggy Connector SDK enforces that only **one batch may be in flight** at a time. Calling `poll()` while a previous batch is awaiting `Ack`/`Nack` returns `Err(Error::InvalidState)`.

5. **TLS Server Name (SNI) Coupling**:
   * The underlying `rumqttc` client derives TLS Server Name Indication (SNI) hostname verification directly from `broker_url`. If `tls.server_name` is configured, it **must match the host component** specified in `broker_url`.
