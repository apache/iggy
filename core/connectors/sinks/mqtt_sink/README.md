# Apache Iggy MQTT Sink Connector

The MQTT sink publishes Iggy messages to an MQTT broker using `rumqttc`.

The connector provides static, template, and dynamic topic routing, MQTT 3.1.1
and MQTT 5 connections, serial publishing, QoS 0/1/2 configuration, broker
acknowledgements for QoS 1/2, bounded retries, and a stable client ID.

QoS 1 is the default and prefers duplicates over loss. If a publish outcome is
uncertain, the connector discards the connection, creates a new connection, and
republishes the unconfirmed messages. The current connectors runtime automatically
commits while polling, so this connector does not provide a crash-safe end-to-end
Iggy offset guarantee after a failed sink call.

## Configuration

```toml
type = "sink"
key = "mqtt"
enabled = true
version = 0
name = "MQTT sink"
path = "target/release/libiggy_connector_mqtt_sink"
plugin_config_format = "json"

[[streams]]
stream = "telemetry"
topics = ["events"]
schema = "raw"
batch_length = 100
poll_interval = "5ms"
consumer_group = "mqtt_sink_connector"

[plugin_config]
broker_url = "mqtt://127.0.0.1:1883"
protocol = "mqtt5"
client_id = "iggy-mqtt-sink"
topic = "iggy.messages"
topic_mode = "static"
topic_template = "{stream}/{topic}"
fallback_topic = "{stream}/{topic}"
qos = 1
retain = false
keep_alive = "30s"
request_capacity = 32
publish_timeout = "30s"
shutdown_timeout = "30s"
max_retries = 3
retry_delay = "1s"
max_in_flight = 1
verbose_logging = false
```

`qos = 0` is best effort and has no broker acknowledgement. `qos = 1` waits for
`PUBACK` and may duplicate messages during retries. `qos = 2` waits for completion
of the MQTT QoS 2 exchange.

`max_in_flight` bounds the number of publications sent before the connector
waits for their acknowledgements. It must not exceed `request_capacity`.
Acknowledgements are correlated by MQTT packet identifier, so they may arrive
in any order. The connector retries only publications that were not confirmed.

`shutdown_timeout` limits the time that the connector waits for active
publications and the MQTT disconnect. If the timeout expires, the connector
reports the unfinished publication count and returns an error.

`topic_mode = "static"` uses `topic`. `topic_mode = "template"` renders
`topic_template` with `{stream}` and `{topic}`. `topic_mode = "dynamic"` reads
`mqtt.topic` from raw-message headers, or reads the topic, payload, and MQTT 5
publish properties from the versioned JSON envelope. Missing dynamic topics use
`fallback_topic`, which defaults to `{stream}/{topic}`.
