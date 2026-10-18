# Apache Iggy MQTT Sink Connector (`iggy_connector_mqtt_sink`)

The **MQTT Sink Connector** is a dynamically loaded shared library plugin
(`.so` / `.dylib` / `.dll`) for Apache Iggy. It reads Iggy messages and
publishes them to MQTT brokers such as **EMQX** or Mosquitto.

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                            IGGY SERVER                                   │
│  • Stores messages in streams, topics, and partitions                     │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     │ Native Binary Client Protocol
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                   IGGY CONNECTORS RUNTIME PROCESS                        │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  MQTT Sink Plugin (libiggy_connector_mqtt_sink.so)                 │  │
│  │  • Reads Iggy batches through the C-ABI FFI                         │  │
│  │  • Resolves static, template, or dynamic MQTT topics                │  │
│  │  • Tracks QoS 1 and QoS 2 acknowledgements                         │  │
│  │  • Reconnects and republishes unconfirmed messages                  │  │
│  └─────────────────────────────────┬──────────────────────────────────┘  │
│                                    │                                     │
│                                    │ MQTT / TLS Connection                │
│                                    ▼                                     │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                           MQTT BROKER                              │  │
│  │  • Client Authentication and Authorization                         │  │
│  │  • Topic Routing and QoS Acknowledgements                          │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Features

* **Multi-Protocol Support**: Supports **MQTT 3.1.1** (`mqtt311`) and **MQTT 5** (`mqtt5`).
* **Quality of Service (QoS)**: Publishes with QoS 0, QoS 1, and QoS 2.
* **Topic Routing**: Supports static topics, `{stream}` and `{topic}` templates, and dynamic MQTT topics from headers or JSON envelopes.
* **Bounded Pipelining**: Limits outstanding publications with `max_in_flight`.
* **Acknowledgement Tracking**: Correlates QoS 1 and QoS 2 acknowledgements by MQTT packet identifier.
* **Reconnect Recovery**: Discards uncertain connections and retries only unconfirmed publications.
* **Malformed Record Reporting**: Logs malformed marked envelopes and rejects their MQTT batch.
* **MQTT 5 Properties**: Publishes supported envelope properties with the decoded payload.
* **TLS and Mutual TLS**: Supports system roots, custom CA files, client certificates, and client private keys through Rustls.
* **Session Control**: Supports `clean_start` and MQTT 5 `session_expiry_interval`.
* **Shutdown Draining**: Waits for active publication work within `shutdown_timeout` before disconnecting.

---

## Configuration Architecture

Configuration operates in **two distinct layers**:

1. **Runtime Layer**: The connector TOML file defines the connector identity, library path, and Iggy stream and topic mapping.
2. **Plugin Layer (`[plugin_config]`)**: Defines MQTT connection settings, credentials, routing, retries, and TLS. The runtime passes this table to the plugin as JSON across the FFI boundary.

---

## Configuration Options (`[plugin_config]`)

| Field | Type | Required | Default | Description |
| :--- | :--- | :---: | :--- | :--- |
| `broker_url` | String | **Yes** | — | Broker URL with an `mqtt://`, `mqtts://`, or `ssl://` scheme. It must not contain credentials or MQTT transport query options. |
| `protocol` | String | No | `"mqtt5"` | MQTT version: `"mqtt5"` or `"mqtt311"`. |
| `client_id` | String | Conditional | None | MQTT client identifier. It is required and non-empty when `clean_start = false`. |
| `username` | String | No | None | Broker username. It must be supplied with `password`. |
| `password` | String | No | None | Broker password. The connector treats it as a secret and never logs it. |
| `clean_start` | Boolean | No | `false` | Clean start for MQTT 5 or clean session for MQTT 3.1.1. |
| `session_expiry_interval` | Integer | No | `3600` | MQTT 5 session expiry interval in seconds. |
| `topic` | String | No | `iggy.messages` | Static output topic. |
| `topic_mode` | String | No | `static` | Routing mode: `static`, `template`, or `dynamic`. |
| `topic_template` | String | No | `{stream}/{topic}` | Template that supports only `{stream}` and `{topic}`. |
| `fallback_topic` | String | No | `{stream}/{topic}` | Topic used when dynamic routing has no valid topic. |
| `qos` | Integer | No | `1` | MQTT publication QoS: `0`, `1`, or `2`. |
| `retain` | Boolean | No | `false` | Sets the MQTT retain flag. |
| `keep_alive` | Duration | No | `"30s"` | MQTT keep-alive. MQTT 3.1.1 accepts `0s` or at least `1s`. MQTT 5 requires at least `5s`. |
| `request_capacity` | Integer | No | `32` | Bounded `rumqttc` request capacity. It must be greater than zero. |
| `publish_timeout` | Duration | No | `"30s"` | Maximum wait for publication acknowledgements. |
| `shutdown_timeout` | Duration | No | `"30s"` | Maximum time for draining and disconnecting. |
| `max_retries` | Integer | No | `5` | Maximum retry attempts for connection or uncertain publication failures. |
| `retry_delay` | Duration | No | `"1s"` | Initial retry delay. |
| `max_retry_delay` | Duration | No | `"30s"` | Maximum retry delay. |
| `max_in_flight` | Integer | No | `1` | Maximum publications awaiting acknowledgement. It must not exceed `request_capacity`. |
| `verbose_logging` | Boolean | No | `false` | Enables additional plugin logging. |
| `tls.ca_file` | String | No | None | Custom CA certificates in PEM format. The system trust store is used when omitted. |
| `tls.client_cert_file` | String | No | None | mTLS client certificate in PEM format. It must pair with `client_key_file`. |
| `tls.client_key_file` | String | No | None | mTLS client private key in PEM format. It must pair with `client_cert_file`. |

Use `broker_url` only for the broker address. Configure `client_id`, `keep_alive`,
`clean_start`, and `max_in_flight` in `[plugin_config]`. The connector rejects
the URL query options `client_id`, `keep_alive_secs`, `clean_session`, and
`inflight_num`.

---

## Configuration Examples

### 1. Basic MQTT 5 Sink

```toml
type = "sink"
key = "mqtt_telemetry"
enabled = true
version = 1
name = "MQTT Telemetry Sink"
path = "target/release/libiggy_connector_mqtt_sink"
plugin_config_format = "json"

[[streams]]
stream = "telemetry"
topics = ["events"]
schema = "raw"
batch_length = 100
poll_interval = "5ms"
consumer_group = "mqtt_telemetry_sink"

[plugin_config]
broker_url = "mqtt://127.0.0.1:1883"
protocol = "mqtt5"
client_id = "iggy-mqtt-sink"
username = "iggy_app"
password = "local-secret-password"
clean_start = false
session_expiry_interval = 3600
topic = "iggy.messages"
topic_mode = "static"
qos = 1
retain = false
keep_alive = "30s"
max_retries = 5
max_in_flight = 3
```

### 2. Template and Dynamic Routing

```toml
[plugin_config]
broker_url = "mqtt://127.0.0.1:1883"
protocol = "mqtt5"
client_id = "iggy-mqtt-routing"
topic_mode = "template"
topic_template = "iggy/{stream}/{topic}"
fallback_topic = "iggy/fallback"
qos = 1
```

Template mode supports only `{stream}` and `{topic}`. Dynamic mode reads
`mqtt.topic` from raw-message headers or reads the topic from the versioned JSON
envelope. Invalid or missing dynamic topics use `fallback_topic`.

### 3. MQTT 5 Envelope Properties

Dynamic mode accepts the versioned JSON envelope produced by the MQTT source.
The sink decodes `payload_base64` and publishes supported MQTT 5 properties.

```json
{
  "version": 1,
  "topic": "devices/site-a/telemetry",
  "payload_base64": "eyJ0ZW1wZXJhdHVyZSI6MjJ9",
  "properties": {
    "response_topic": "devices/site-a/replies",
    "content_type": "application/json",
    "user_properties": [{"key": "site", "value": "a"}]
  }
}
```

### 4. Secure TLS and Mutual TLS

```toml
[plugin_config]
broker_url = "mqtts://emqx.example.com:8883"
protocol = "mqtt5"
client_id = "iggy-mqtt-tls-sink"
topic = "iggy.messages"
qos = 1

[plugin_config.tls]
ca_file = "/etc/iggy/certs/ca.pem"
client_cert_file = "/etc/iggy/certs/client-cert.pem"
client_key_file = "/etc/iggy/certs/client-key.pem"
```

The connector validates the broker certificate and hostname. It has no option
to disable certificate validation. The nested TLS table belongs in the
connector TOML file because flat environment overrides cannot update it.

### 5. Multiple Sink Routes

Run one connector instance per MQTT route. Give every instance a unique
connector key, client ID, consumer group, and output topic. The runtime loads
each connector TOML file from its configured `config_dir`.

---

## Environment Variable Overrides

Top-level properties inside `[plugin_config]` can be overridden without
editing the connector file:

```bash
export IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_BROKER_URL="mqtt://127.0.0.1:1883"
export IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_USERNAME="emqx-user"
export IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_PASSWORD="secret-password"
export IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_QOS=1
export IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_MAX_IN_FLIGHT=3
```

Nested fields such as `tls.ca_file` cannot be changed through flat environment
variables. Keep the complete `[plugin_config.tls]` table in the connector TOML.

---

## Publication and Acknowledgement Mechanism

The sink provides broker-facing at-least-once MQTT delivery for QoS 1 and QoS
2. A publication is confirmed only after the expected MQTT acknowledgement
arrives.

```text
  Iggy Server          MQTT Sink Plugin             MQTT Broker
       │                       │                         │
       │ 1. Read batch         │                         │
       ├──────────────────────►│                         │
       │                       │ 2. MQTT PUBLISH         │
       │                       ├────────────────────────►│
       │                       │                         │
       │                       │ 3. PUBACK or PUBCOMP    │
       │                       │◄────────────────────────┤
       │                       │                         │
       │ 4. Return success     │                         │
       │◄──────────────────────┤                         │
```

For QoS 0, the sink confirms the publication after it queues the request. For
QoS 1, it waits for `PUBACK`. For QoS 2, it waits for `PUBCOMP`. When an
outcome is uncertain, the sink drops the event loop and retries only
unconfirmed publications. This favors duplicates over loss.

Iggy offset durability depends on the connectors runtime. The current runtime
commits offsets when it polls messages, before MQTT publication completes.
End-to-end Iggy at-least-once delivery requires offset storage after MQTT
confirms the publication.

---

## Failure Modes

| Failure Scenario | Current Behavior | Result |
| :--- | :--- | :--- |
| Invalid configuration | Validation fails before connection | The connector reports an invalid configuration error. |
| Malformed record envelope | The connector logs the record and rejects the batch. | The runtime records an error and keeps the sink running. |
| Transient connection failure | The connection is discarded and retried | Unconfirmed publications can be duplicated. |
| Publication timeout | The connection is treated as uncertain | Unconfirmed publications are retried. |
| MQTT 5 broker rejection | The plugin returns a non-retryable publication error. | The runtime records an error and keeps the sink running. |
| Authentication failure | Connection initialization fails | The sink does not publish records. |
| Shutdown timeout | The sink reports unfinished publications | It returns an error after the timeout. |

---

## Operational Notes

1. **Duplicate tolerance**: QoS 1 and QoS 2 retries can publish duplicates. Downstream consumers must use idempotent processing when duplicates are unacceptable.
2. **Credential separation**: Put `username` and `password` in separate fields. The connector rejects credentials embedded in `broker_url`.
3. **Client identity**: Use a stable, unique `client_id` when `clean_start = false` so the broker can resume the MQTT session.
4. **Malformed records**: The connector logs malformed records and rejects their batch. It does not provide a dead-letter queue.
5. **Deferred broker limits**: The current driver does not retain broker-negotiated topic-alias or maximum-packet-size limits.
6. **Broker rejection classification**: The SDK has no broker-rejection error. The plugin uses `CannotStoreData` with an internal non-retryable flag. The runtime records the error and continues.
