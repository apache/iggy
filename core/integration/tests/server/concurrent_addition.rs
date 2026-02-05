/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::server::scenarios::concurrent_scenario::{
    self, ResourceType, ScenarioType, barrier_off, barrier_on,
};
use iggy_common::TransportProtocol;
use integration::iggy_harness;
use test_case::test_matrix;

// Test matrix for race condition scenarios
// Tests all combinations of:
// - Transport: TCP, HTTP, QUIC, WebSocket (4)
// - Resource: User, Stream, Topic, Partition, ConsumerGroup (5)
// - Path: Hot (unique names), Cold (duplicate names) (2)
// - Barrier: On (synchronized), Off (unsynchronized) (2)
// Total: 4 × 5 × 2 × 2 = 80 test cases
// Note: Partition + Cold is skipped (partitions don't have names)

// TODO: Websocket fails for the `cold` type, cold means that we are creating resources with the same name.
// It fails with the error assertion, instead of `AlreadyExist`, we get generic `Error`.
#[iggy_harness(server(
    quic.max_idle_timeout = "500s",
    quic.keep_alive_interval = "15s"
))]
#[test_matrix(
    [tcp(), http(), quic(), websocket()],
    [user(), stream(), topic(), partition(), consumer_group()],
    [hot(), cold()],
    [barrier_on(), barrier_off()]
)]
async fn matrix(
    harness: &TestHarness,
    transport: TransportProtocol,
    resource_type: ResourceType,
    path_type: ScenarioType,
    use_barrier: bool,
) {
    concurrent_scenario::run(harness, transport, resource_type, path_type, use_barrier).await;
}

fn tcp() -> TransportProtocol {
    TransportProtocol::Tcp
}

fn http() -> TransportProtocol {
    TransportProtocol::Http
}

fn quic() -> TransportProtocol {
    TransportProtocol::Quic
}

fn websocket() -> TransportProtocol {
    TransportProtocol::WebSocket
}

fn user() -> ResourceType {
    ResourceType::User
}

fn stream() -> ResourceType {
    ResourceType::Stream
}

fn topic() -> ResourceType {
    ResourceType::Topic
}

fn partition() -> ResourceType {
    ResourceType::Partition
}

fn consumer_group() -> ResourceType {
    ResourceType::ConsumerGroup
}

fn hot() -> ScenarioType {
    ScenarioType::Hot
}

fn cold() -> ScenarioType {
    ScenarioType::Cold
}
