<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# MQTT source failure testing

The MQTT source uses deterministic integration tests for failure recovery.
The tests run serially because each scenario starts an authenticated EMQX
`latest` container.

## Iggy failure and redelivery

`mqtt5_iggy_failure_redelivers_unacknowledged_qos1_message` stops Iggy while
the MQTT source remains connected to EMQX. The broker accepts the QoS 1
publish, but the source cannot persist it. The test restarts Iggy, restarts
the source through the runtime HTTP API, and verifies that the unacknowledged
message is delivered to Iggy.

The test validates the important ordering rule:

```text
Iggy failure → no MQTT acknowledgement → source reconnect → redelivery
```

## Connector restart with a pending batch

`mqtt5_connector_restart_redelivers_pending_qos1_batch` uses a 5-second batch
timeout and a batch size of 100. It publishes one message, restarts only the
MQTT source through `POST /sources/mqtt/restart`, and verifies persistence
after the broker redelivers the unacknowledged message.

The pending batch is intentionally not treated as committed state. A restart
may produce a duplicate, which is valid for QoS 1 at-least-once delivery.

## EMQX interruption during accumulation

`mqtt5_emqx_outage_recovers_batch_accumulation` pauses EMQX for two seconds
and resumes it before publishing the next message. This models a deterministic
broker outage while a source batch is accumulating. Both messages must reach
Iggy after the broker and source recover.

The fixture currently uses Docker pause/resume instead of destroying and
recreating the container. This preserves the mapped broker endpoint and the
authenticated EMQX state while still exercising the network interruption and
rumqttc reconnect path.

## QoS 2 ordering

The existing QoS 2 integration tests verify that the message reaches Iggy and
that the publisher completes its QoS 2 handshake. The source implementation
delegates `PUBREL` and `PUBCOMP` handling to rumqttc.

Packet-level proof that the source's `PUBREC` is emitted only after Iggy
accepts the batch requires broker packet tracing or a protocol proxy. That is
separate from the normal EMQX fixture and remains a follow-up correctness
test.
