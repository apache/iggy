<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Iggy Sink Connector

The Iggy sink connector consumes messages from an upstream Iggy stream/topic and sinks them directly into a target downstream Iggy cluster, stream, or topic.

## Features

- **Iggy-to-Iggy Replication**: Replicate or forward streaming messages across topics or clusters.
- **Dynamic Topic Overrides**: Optional stream and topic overrides; if omitted, messages retain their original stream and topic names.
- **Header & Payload Preservation**: Forward message payloads and metadata seamlessly.
- **Zero-Clone Hot Path**: Efficient batch forwarding without unnecessary memory copies.

## Configuration

```toml
[[streams]]
stream = "source_stream"
topics = ["source_topic"]
schema = "json"
batch_length = 100
poll_interval = "5ms"
consumer_group = "iggy_sink"

[plugin_config]
server_address = "iggy://iggy:iggy@127.0.0.1:8090"
stream_id = "target_stream"
topic_id = "target_topic"
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `server_address` | string | required | Address of the target Iggy cluster (e.g. `iggy://user:pass@127.0.0.1:8090` or `127.0.0.1:8090`) |
| `stream_id` | string (optional) | `None` | Optional target stream override. If omitted, uses the source stream name. |
| `topic_id` | string (optional) | `None` | Optional target topic override. If omitted, uses the source topic name. |

## Production Guarantees

- **Transient Failures:** Underneath, the Iggy Rust SDK client retries connection attempts and reconnects automatically.
- **Delivery Semantics:** At-least-once delivery; messages are flushed per batch.
- **Idempotency:** Message IDs and offsets are preserved, allowing downstream Iggy topics to deduplicate repeat deliveries.
- **Observability:** Logs at `info!` for connector lifecycle events and `debug!` for batch details using `tracing`.
