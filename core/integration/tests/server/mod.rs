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

mod cg;
mod concurrent_addition;
mod general;
mod scenarios;
mod specific;

use iggy_common::TransportProtocol;
use integration::{
    http_client::HttpClientFactory,
    quic_client::QuicClientFactory,
    tcp_client::TcpClientFactory,
    test_server::{ClientFactory, IpAddrKind, TestServer},
    websocket_client::WebSocketClientFactory,
};
use scenarios::{
    bench_scenario, consumer_group_auto_commit_reconnection_scenario, consumer_group_join_scenario,
    consumer_group_offset_cleanup_scenario,
    consumer_group_with_multiple_clients_polling_messages_scenario,
    consumer_group_with_single_client_polling_messages_scenario, create_message_payload,
    message_headers_scenario, stream_size_validation_scenario, system_scenario, user_scenario,
};
use std::pin::Pin;
use std::{collections::HashMap, future::Future};

type ScenarioFn = fn(&dyn ClientFactory) -> Pin<Box<dyn Future<Output = ()> + '_>>;

fn system_scenario() -> ScenarioFn {
    |factory| Box::pin(system_scenario::run(factory))
}

fn user_scenario() -> ScenarioFn {
    |factory| Box::pin(user_scenario::run(factory))
}

fn message_headers_scenario() -> ScenarioFn {
    |factory| Box::pin(message_headers_scenario::run(factory))
}

fn create_message_payload_scenario() -> ScenarioFn {
    |factory| Box::pin(create_message_payload::run(factory))
}

fn join_scenario() -> ScenarioFn {
    |factory| Box::pin(consumer_group_join_scenario::run(factory))
}

fn stream_size_validation_scenario() -> ScenarioFn {
    |factory| Box::pin(stream_size_validation_scenario::run(factory))
}

fn single_client_scenario() -> ScenarioFn {
    |factory| Box::pin(consumer_group_with_single_client_polling_messages_scenario::run(factory))
}

fn multiple_clients_scenario() -> ScenarioFn {
    |factory| Box::pin(consumer_group_with_multiple_clients_polling_messages_scenario::run(factory))
}

fn auto_commit_reconnection_scenario() -> ScenarioFn {
    |factory| {
        Box::pin(consumer_group_auto_commit_reconnection_scenario::run(
            factory,
        ))
    }
}

fn offset_cleanup_scenario() -> ScenarioFn {
    |factory| Box::pin(consumer_group_offset_cleanup_scenario::run(factory))
}

fn bench_scenario() -> ScenarioFn {
    |factory| Box::pin(bench_scenario::run(factory))
}

async fn run_scenario(transport: TransportProtocol, scenario: ScenarioFn) {
    // TODO: Need to enable `TCP_NODELAY` flag for TCP transports, due to small messages being used in the test.
    // For some reason TCP in compio can't deal with it, but in tokio it works fine.
    let mut extra_envs = HashMap::new();
    extra_envs.insert(
        "IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS".to_string(),
        "true".to_string(),
    );
    extra_envs.insert("IGGY_TCP_SOCKET_NODELAY".to_string(), "true".to_string());
    extra_envs.insert("IGGY_QUIC_MAX_IDLE_TIMEOUT".to_string(), "500s".to_string());
    extra_envs.insert(
        "IGGY_QUIC_KEEP_ALIVE_INTERVAL".to_string(),
        "15s".to_string(),
    );
    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let client_factory: Box<dyn ClientFactory> = match transport {
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

    scenario(&*client_factory).await;
}
