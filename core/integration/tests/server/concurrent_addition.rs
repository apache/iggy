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
use integration::{
    http_client::HttpClientFactory, quic_client::QuicClientFactory, tcp_client::TcpClientFactory,
    test_server::TestServer, websocket_client::WebSocketClientFactory,
};
use serial_test::parallel;
use test_case::test_matrix;

// Test matrix for race condition scenarios
// Tests all combinations of:
// - Transport: TCP, HTTP, QUIC, WebSocket (4)
// - Resource: User, Stream, Topic (3)
// - Path: Hot (unique names), Cold (duplicate names) (2)
// - Barrier: On (synchronized), Off (unsynchronized) (2)
// Total: 4 × 3 × 2 × 2 = 48 test cases

// TODO: Websocket fails for the `cold` type, cold means that we are creating resources with the same name.
// It fails with the error assertion, instead of `AlreadyExist`, we get generic `Error`.
#[test_matrix(
    [tcp(), http(), quic(), websocket()],
    [user(), stream(), topic()],
    [hot(), cold()],
    [barrier_on(), barrier_off()]
)]
#[tokio::test]
#[parallel]
async fn matrix(
    transport: TransportProtocol,
    resource_type: ResourceType,
    path_type: ScenarioType,
    use_barrier: bool,
) {
    // TODO: Need to do this, in order to avoid timeouts from QUIC connections during tests.
    let mut extra_envs = std::collections::HashMap::new();
    extra_envs.insert("IGGY_QUIC_MAX_IDLE_TIMEOUT".to_string(), "500s".to_string());
    extra_envs.insert(
        "IGGY_QUIC_KEEP_ALIVE_INTERVAL".to_string(),
        "15s".to_string(),
    );
    let mut test_server = TestServer::new(
        Some(extra_envs),
        true,
        None,
        integration::test_server::IpAddrKind::V4,
    );
    test_server.start();

    let client_factory: Box<dyn integration::test_server::ClientFactory> = match transport {
        TransportProtocol::Tcp => {
            let server_addr = test_server.get_raw_tcp_addr().unwrap();
            Box::new(TcpClientFactory {
                server_addr,
                ..Default::default()
            })
        }
        TransportProtocol::Quic => {
            let server_addr = test_server.get_quic_udp_addr().unwrap();
            Box::new(QuicClientFactory { server_addr })
        }
        TransportProtocol::Http => {
            let server_addr = test_server.get_http_api_addr().unwrap();
            Box::new(HttpClientFactory { server_addr })
        }
        TransportProtocol::WebSocket => {
            let server_addr = test_server.get_websocket_addr().unwrap();
            Box::new(WebSocketClientFactory { server_addr })
        }
    };

    concurrent_scenario::run(&*client_factory, resource_type, path_type, use_barrier).await;
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

fn hot() -> ScenarioType {
    ScenarioType::Hot
}

fn cold() -> ScenarioType {
    ScenarioType::Cold
}
