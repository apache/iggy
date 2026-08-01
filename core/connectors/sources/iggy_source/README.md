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

# Iggy Source Connector

The Iggy source connector polls messages from an external/remote Apache Iggy stream and topic, converting them into `ProducedMessages` and publishing them into local Iggy streams via the connectors runtime.

## Features

- **Iggy-to-Iggy Replication**: Ingest messages from external Iggy clusters into local Iggy topics.
- **Offset State Persistence**: Persists `current_offset` atomically via MessagePack state files (`source_iggy.state`). Resumes cleanly on restart.
- **Header & ID Preservation**: Preserves original 128-bit message IDs and user headers for downstream deduplication.
- **Configurable Batching & Polling**: Customizable `batch_size` and `poll_interval`.

## Configuration

```toml
key = "iggy"
name = "Iggy source"
type = "source"
path = "target/debug/libiggy_connector_iggy_source"

[plugin_config]
server_address = "iggy://iggy:iggy@127.0.0.1:8090"
stream_id = "source_stream"
topic_id = "source_topic"
partition_id = 1
batch_size = 100
poll_interval = "100ms"
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `server_address` | string | required | Remote Iggy cluster server address (e.g., `iggy://user:pass@127.0.0.1:8090`). |
| `stream_id` | string | required | Remote stream ID or name to poll from. |
| `topic_id` | string | required | Remote topic ID or name to poll from. |
| `partition_id` | u32 (optional) | `1` | Remote partition ID to poll from. |
| `batch_size` | u32 (optional) | `100` | Maximum messages per poll. |
| `poll_interval` | string (optional) | `"100ms"` | Delay duration before each poll cycle. |

## Production Guarantees

- **Transient Failures:** Client auto-reconnects on network drops. `poll()` logs warnings and retries without advancing saved offset state.
- **Delivery Semantics:** At-least-once delivery. State is persisted after successful write to the local stream.
- **Idempotency:** Preserves 128-bit message IDs for downstream deduplication.
- **Observability:** Logs with `tracing` and updates Prometheus metrics (`errors_total`, `messages_filtered`).
