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

# MQTT source benchmark scope

Throughput and memory benchmarking are intentionally separate from the source
correctness tests. They should not be required for every connector integration
test run.

The benchmark should measure:

- messages per second at different `batch_size` values;
- end-to-end persistence latency;
- acknowledgement latency for QoS 1 and QoS 2;
- CPU and allocation cost of single-message versus batched FFI calls;
- memory usage while the MQTT request channel and source batch are full;
- behavior with small and large `request_capacity` values.

The benchmark must verify these bounds:

```text
buffered MQTT messages <= batch_size
source batch messages <= batch_size
acknowledgement tokens <= messages in the pending batch
```

Recommended matrix:

```text
protocol: MQTT 3.1.1, MQTT 5
qos:      0, 1, 2
batch:    1, 10, 100, 1000
capacity: 1, 32, 256
```

The benchmark should use a dedicated EMQX fixture and a dedicated Iggy
stream. It should not run as part of the serial correctness suite because
throughput measurements are sensitive to host load and container startup
contention.
