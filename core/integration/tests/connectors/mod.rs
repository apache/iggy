/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use iggy::prelude::{
    Client, DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, IggyClient, IggyMessage, Partitioning,
};
use iggy_binary_protocol::{MessageClient, StreamClient, TopicClient, UserClient};
use iggy_common::{
    CompressionAlgorithm, Consumer, Identifier, IggyExpiry, IggyTimestamp, MaxTopicSize,
    PolledMessages, PollingStrategy,
};
use integration::{
    tcp_client::TcpClientFactory,
    test_connectors_runtime::TestConnectorsRuntime,
    test_server::{ClientFactory, IpAddrKind, TestServer},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

mod api;
mod http_config_provider;
mod postgres;
mod random;

pub const DEFAULT_TEST_STREAM: &str = "test_stream";
pub const DEFAULT_TEST_TOPIC: &str = "test_topic";
const TEST_CONSUMER_NAME: &str = "test_consumer";
const ONE_DAY_MICROS: u64 = 24 * 60 * 60 * 1_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestMessage {
    pub id: u64,
    pub name: String,
    pub count: u32,
    pub amount: f64,
    pub active: bool,
    pub timestamp: i64,
}

pub struct IggySetup {
    pub stream: String,
    pub topic: String,
}

impl Default for IggySetup {
    fn default() -> Self {
        Self {
            stream: DEFAULT_TEST_STREAM.to_owned(),
            topic: DEFAULT_TEST_TOPIC.to_owned(),
        }
    }
}

struct ConnectorsRuntime {
    iggy_server: TestServer,
    connectors_runtime: Option<TestConnectorsRuntime>,
}

struct ConnectorsIggyClient {
    stream_id: Identifier,
    topic_id: Identifier,
    client: IggyClient,
}

fn create_test_messages(count: usize) -> Vec<TestMessage> {
    let base_timestamp = IggyTimestamp::now().as_micros();
    (1..=count)
        .map(|i| TestMessage {
            id: i as u64,
            name: format!("user_{}", i - 1),
            count: ((i - 1) * 10) as u32,
            amount: (i - 1) as f64 * 99.99,
            active: (i - 1) % 2 == 0,
            timestamp: (base_timestamp + (i - 1) as u64 * ONE_DAY_MICROS) as i64,
        })
        .collect()
}

fn setup_runtime() -> ConnectorsRuntime {
    let mut iggy_envs = HashMap::new();
    iggy_envs.insert("IGGY_QUIC_ENABLED".to_owned(), "false".to_owned());
    iggy_envs.insert("IGGY_WEBSOCKET_ENABLED".to_owned(), "false".to_owned());
    let mut test_server = TestServer::new(Some(iggy_envs), true, None, IpAddrKind::V4);
    test_server.start();
    ConnectorsRuntime {
        iggy_server: test_server,
        connectors_runtime: None,
    }
}

impl ConnectorsRuntime {
    async fn init(
        &mut self,
        config_path: &str,
        envs: Option<HashMap<String, String>>,
        iggy_setup: IggySetup,
    ) {
        if self.connectors_runtime.is_some() {
            return;
        }

        let config_path = format!("tests/connectors/{config_path}");
        let mut all_envs = HashMap::new();
        all_envs.insert(
            "IGGY_CONNECTORS_CONFIG_PATH".to_owned(),
            config_path.to_owned(),
        );

        if let Some(envs) = envs {
            for (k, v) in envs {
                all_envs.insert(k, v);
            }
        }

        let client = self.create_iggy_client().await;
        client
            .create_stream(&iggy_setup.stream)
            .await
            .expect("Failed to create stream");
        let stream_id: Identifier = iggy_setup
            .stream
            .try_into()
            .expect("Failed to parse stream identifier");
        client
            .create_topic(
                &stream_id,
                &iggy_setup.topic,
                1,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::ServerDefault,
                MaxTopicSize::ServerDefault,
            )
            .await
            .expect("Failed to create topic");
        client.shutdown().await.expect("Failed to shutdown client");

        let iggy_server_address = self
            .iggy_server
            .get_raw_tcp_addr()
            .expect("Failed to get Iggy TCP address");
        let mut connectors_runtime =
            TestConnectorsRuntime::with_iggy_address(&iggy_server_address, Some(all_envs));
        connectors_runtime.start();
        connectors_runtime.ensure_started().await;
        self.connectors_runtime = Some(connectors_runtime);
    }

    async fn create_client(&self) -> ConnectorsIggyClient {
        let stream_id: Identifier = DEFAULT_TEST_STREAM
            .try_into()
            .expect("Failed to parse stream identifier");
        let topic_id: Identifier = DEFAULT_TEST_TOPIC
            .try_into()
            .expect("Failed to parse topic identifier");
        ConnectorsIggyClient {
            stream_id,
            topic_id,
            client: self.create_iggy_client().await,
        }
    }

    async fn create_iggy_client(&self) -> IggyClient {
        let server_addr = self
            .iggy_server
            .get_raw_tcp_addr()
            .expect("Failed to get Iggy TCP address");
        let client = TcpClientFactory {
            server_addr,
            ..Default::default()
        }
        .create_client()
        .await;
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .expect("Failed to login as root user");
        IggyClient::create(client, None, None)
    }

    fn connectors_api_address(&self) -> Option<String> {
        self.connectors_runtime
            .as_ref()
            .map(|r| r.get_http_api_address())
    }
}

impl ConnectorsIggyClient {
    async fn send_messages(&self, messages: &mut [IggyMessage]) {
        self.client
            .send_messages(
                &self.stream_id,
                &self.topic_id,
                &Partitioning::partition_id(0),
                messages,
            )
            .await
            .expect("Failed to send messages to Iggy");
    }

    async fn poll_messages(&self) -> Result<PolledMessages, iggy_common::IggyError> {
        let consumer_id: Identifier = TEST_CONSUMER_NAME
            .try_into()
            .expect("Failed to parse consumer identifier");
        self.client
            .poll_messages(
                &self.stream_id,
                &self.topic_id,
                None,
                &Consumer::new(consumer_id),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
    }
}
