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

## Configuration

The connector TOML file has two parts. The runtime section identifies the
sink and selects the Iggy records that it reads. The `[plugin_config]` section
defines the MQTT connection and publication behavior.

The default sample uses static routing. It publishes every selected Iggy record
to one MQTT topic. Optional authentication, TLS, template routing, and dynamic
routing remain commented. Uncomment the fields that you need and replace the
active `topic_mode` value when you select another routing mode.

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
clean_start = false
session_expiry_interval = 3600

# Select one routing block. Keep one topic_mode line active.
# Static routing publishes every selected Iggy record to one MQTT topic.
topic_mode = "static"
topic = "iggy.messages"
qos = 1
retain = false

# Template routing creates an MQTT topic from the Iggy stream and topic.
# To select it, comment the active static topic and topic_mode lines above.
# topic_mode = "template"
# topic_template = "{stream}/{topic}"

# Dynamic routing reads mqtt.topic from Iggy headers or an MQTT 5 envelope.
# To select it, comment the active static topic and topic_mode lines above.
# topic_mode = "dynamic"
# fallback_topic = "{stream}/{topic}"

# Optional broker authentication.
# username = "emqx-user"
# password = "replace-me"

# Optional TLS configuration for mqtts:// brokers.
# [plugin_config.tls]
# ca_file = "/etc/iggy/certs/emqx-ca.pem"
# client_cert_file = "/etc/iggy/certs/client-cert.pem"
# client_key_file = "/etc/iggy/certs/client-key.pem"
```

`core/connectors/sinks/mqtt_sink/config.toml` uses this layout. The runtime
example uses the same plugin configuration with its own Iggy route.

## Runtime Configuration Reference

| Field | Description |
| :--- | :--- |
| `type` | Set this value to `"sink"`. |
| `key` | Names this connector instance. Each loaded connector needs a unique key. |
| `enabled` | Starts the connector when the runtime starts. |
| `version` | Selects the stored configuration version. |
| `name` | Provides a readable name for logs and the control API. |
| `path` | Gives the path to the MQTT sink shared library. |
| `plugin_config_format` | Set this value to `"json"`. The runtime sends the plugin section as JSON. |
| `streams[].stream` | Selects an Iggy stream to read. |
| `streams[].topics` | Selects one or more Iggy topics in that stream. |
| `streams[].schema` | Selects the decoder for the Iggy record payload. Use `"raw"` for unchanged bytes. |
| `streams[].batch_length` | Limits the number of Iggy records in one runtime batch. |
| `streams[].poll_interval` | Sets the wait time before the next Iggy poll. |
| `streams[].consumer_group` | Stores the Iggy read position for this sink route. Use a unique name for each sink instance. |

## MQTT Routing Modes

Static mode publishes every Iggy record from one sink route to the value of
`topic`. This is the default mode. `topic_template` and `fallback_topic` do
not select the output topic in this mode.

```toml
topic_mode = "static"
topic = "manual/static"
```

Template mode creates an MQTT topic from the Iggy stream and topic names. The
template accepts only `{stream}` and `{topic}`. The `topic` field does not
select the output topic in this mode.

```toml
topic_mode = "template"
topic_template = "iggy/{stream}/{topic}"
```

Dynamic mode reads `mqtt.topic` from raw-message headers. With MQTT 5, it can
also read the topic from a versioned JSON envelope. `fallback_topic` selects
the output topic when the record has no valid MQTT topic.

```toml
topic_mode = "dynamic"
fallback_topic = "iggy/fallback"
```

Dynamic mode accepts this MQTT 5 envelope. The connector decodes
`payload_base64` before it publishes the MQTT payload.

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

## Plugin Configuration Reference

| Field | Used when | Default | Description |
| :--- | :--- | :--- | :--- |
| `broker_url` | Always | None | Sets the MQTT broker URL. Use `mqtt://`, `mqtts://`, or `ssl://`. Do not add credentials or MQTT options to this URL. |
| `protocol` | All modes | `"mqtt5"` | Selects `"mqtt5"` or `"mqtt311"`. |
| `client_id` | When `clean_start = false` | None | Identifies the MQTT client. Set a stable, non-empty value for persistent sessions. |
| `username` | Authenticated brokers | None | Sets the broker user name. Set `password` with this field. |
| `password` | Authenticated brokers | None | Sets the broker password. The connector does not write it to logs. |
| `clean_start` | All modes | `false` | Starts a clean MQTT 5 session or MQTT 3.1.1 clean session. |
| `session_expiry_interval` | All modes | `3600` | Sets the MQTT 5 session expiry time in seconds. |
| `topic_mode` | All modes | `"static"` | Selects `"static"`, `"template"`, or `"dynamic"`. |
| `topic` | Static mode | `"iggy.messages"` | Sets the MQTT output topic for static routing. |
| `topic_template` | Template mode | `"{stream}/{topic}"` | Sets the template for template routing. It accepts only `{stream}` and `{topic}`. |
| `fallback_topic` | Dynamic mode | `"{stream}/{topic}"` | Sets the MQTT output topic when dynamic routing has no valid topic. |
| `qos` | All modes | `1` | Sets MQTT publication QoS. Use `0`, `1`, or `2`. |
| `retain` | All modes | `false` | Sets the MQTT retained-message flag. |
| `keep_alive` | All modes | `"30s"` | Sets the MQTT ping interval. MQTT 3.1.1 accepts `"0s"` or at least `"1s"`. MQTT 5 needs at least `"5s"`. |
| `request_capacity` | All modes | `32` | Limits queued MQTT client requests. Set a value greater than zero. |
| `publish_timeout` | All modes | `"30s"` | Limits the wait for broker connection and publication acknowledgments. |
| `shutdown_timeout` | All modes | `"30s"` | Limits the wait while the connector finishes publications during shutdown. |
| `max_retries` | All modes | `5` | Limits retries after connection or uncertain publication failures. |
| `retry_delay` | All modes | `"1s"` | Sets the first retry delay. |
| `max_retry_delay` | All modes | `"30s"` | Limits the retry delay. |
| `max_in_flight` | All modes | `1` | Limits publications that wait for acknowledgment. It must not exceed `request_capacity`. |
| `verbose_logging` | All modes | `false` | Enables extra plugin log records. |
| `tls.ca_file` | TLS brokers | None | Sets a PEM file that contains custom CA certificates. The system trust store is used when it is omitted. |
| `tls.client_cert_file` | Mutual TLS | None | Sets the PEM client certificate for mutual TLS. Set `tls.client_key_file` with this field. |
| `tls.client_key_file` | Mutual TLS | None | Sets the PEM client key for mutual TLS. Set `tls.client_cert_file` with this field. |

Use `broker_url` only for the broker address. Set `client_id`, `keep_alive`,
`clean_start`, and `max_in_flight` as separate fields. The connector rejects
the URL options `client_id`, `keep_alive_secs`, `clean_session`, and
`inflight_num`.

## TLS and Multiple Routes

The connector validates the broker certificate and host name. It cannot disable
certificate validation. Keep the complete `[plugin_config.tls]` table in the
connector TOML file because environment overrides cannot update nested fields.

Use one connector instance for each MQTT route. Give each instance a unique
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

The sink provides QoS-aware broker delivery for MQTT publications. For QoS 1
and QoS 2, a publication is confirmed only after the expected MQTT
acknowledgement arrives.

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
The MQTT sink provides QoS-aware broker delivery and retries unconfirmed QoS 1
and QoS 2 publications, but the current runtime does not provide end-to-end
at-least-once delivery because it commits the Iggy offset before publication
completes.

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
