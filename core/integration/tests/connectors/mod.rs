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

use iggy::prelude::{Client, DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, IggyClient};
use iggy_binary_protocol::{MessageClient, StreamClient, TopicClient, UserClient};
use iggy_common::{CompressionAlgorithm, IggyExpiry, MaxTopicSize, PolledMessages};
use integration::{
    tcp_client::TcpClientFactory,
    test_connectors_runtime::TestConnectorsRuntime,
    test_server::{ClientFactory, IpAddrKind, TestServer},
};
use std::collections::HashMap;

mod postgres;
mod random;

const DEFAULT_TEST_STREAM: &str = "test_stream";
const DEFAULT_TEST_TOPIC: &str = "test_topic";

fn setup_runtime() -> ConnectorsRuntime {
    let mut iggy_envs = HashMap::new();
    iggy_envs.insert("IGGY_QUIC_ENABLED".to_owned(), "false".to_owned());
    iggy_envs.insert("IGGY_WEBSOCKET_ENABLED".to_owned(), "false".to_owned());
    let mut test_server = TestServer::new(Some(iggy_envs), true, None, IpAddrKind::V4);
    test_server.start();
    ConnectorsRuntime {
        iggy_server: test_server,
        connectors_runtime: None,
        stream: "".to_owned(),
        topic: "".to_owned(),
    }
}

#[derive(Debug)]
struct ConnectorsRuntime {
    stream: String,
    topic: String,
    iggy_server: TestServer,
    connectors_runtime: Option<TestConnectorsRuntime>,
}

#[derive(Debug)]
struct ConnectorsIggyClient {
    stream: String,
    topic: String,
    client: IggyClient,
}

impl ConnectorsIggyClient {
    async fn get_messages(&self) -> Result<PolledMessages, iggy_common::IggyError> {
        self.client
            .poll_messages(
                &self.stream.clone().try_into().unwrap(),
                &self.topic.clone().try_into().unwrap(),
                None,
                &iggy_common::Consumer::new("test_consumer".try_into().unwrap()),
                &iggy_common::PollingStrategy::next(),
                10,
                true,
            )
            .await
    }
}

#[derive(Debug)]
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

impl ConnectorsRuntime {
    pub async fn init(
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
        let stream_id = iggy_setup
            .stream
            .try_into()
            .expect("Invalid stream name in Iggy setup");
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
        self.stream = self.stream.clone();
        self.topic = self.topic.clone();
        self.connectors_runtime = Some(connectors_runtime);
    }

    pub async fn create_client(&self) -> ConnectorsIggyClient {
        ConnectorsIggyClient {
            stream: DEFAULT_TEST_STREAM.to_owned(),
            topic: DEFAULT_TEST_TOPIC.to_owned(),
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
}
