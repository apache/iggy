// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod api;
mod delta;
mod doris;
mod elasticsearch;
mod fixtures;
mod http;
mod http_config_provider;
mod iceberg;
mod influxdb;
mod jdbc;
mod mongodb;
mod postgres;
mod quickwit;
mod random;
mod random_source_liveness;
mod runtime;
mod s3;
mod stdout;

use iggy::prelude::{IggyClient, IggyMessage, Partitioning};
use iggy_common::Client;
use iggy_common::{
    CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize, MessageClient, PolledMessages,
    StreamClient, TopicClient,
};
use integration::harness::{ConnectorsRuntimeConfig, IpAddrKind, TestHarness, TestServerConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const DEFAULT_TEST_STREAM: &str = "test_stream";
const DEFAULT_TEST_TOPIC: &str = "test_topic";

fn setup_runtime() -> ConnectorsRuntime {
    ConnectorsRuntime {
        harness: TestHarness::builder()
            .server(
                TestServerConfig::builder()
                    .ip_kind(IpAddrKind::V4)
                    .quic_enabled(false)
                    .http_enabled(false)
                    .websocket_enabled(false)
                    .extra_envs(HashMap::from([
                        // The harness pre-reserves a fixed TCP port (see PortReserver),
                        // so the server binds a non-zero port. That relies on the server
                        // writing current_config.toml on bind regardless of how the port
                        // was chosen (see tcp_listener.rs); the harness reads that file to
                        // discover the bound address before it considers startup complete.
                        ("IGGY_TCP_ADDRESS".to_owned(), "127.0.0.1:0".to_owned()),
                    ]))
                    .build(),
            )
            .build()
            .unwrap(),
        stream: "".to_owned(),
        topic: "".to_owned(),
    }
}

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

pub fn create_test_messages(count: usize) -> Vec<TestMessage> {
    let base_timestamp = IggyTimestamp::now().as_micros();
    (1..=count)
        .map(|i| TestMessage {
            id: i as u64,
            name: format!("user_{}", i - 1),
            count: ((i - 1) * 10) as u32,
            amount: (i - 1) as f64 * 100.0,
            active: (i - 1) % 2 == 0,
            timestamp: (base_timestamp + (i - 1) as u64 * ONE_DAY_MICROS) as i64,
        })
        .collect()
}

#[derive(Debug)]
struct ConnectorsRuntime {
    stream: String,
    topic: String,
    harness: TestHarness,
}

#[derive(Debug)]
struct ConnectorsIggyClient {
    stream: String,
    topic: String,
    client: IggyClient,
}

impl ConnectorsIggyClient {
    /// Send messages to the configured stream/topic (used by sink connector tests).
    #[allow(dead_code)]
    async fn send_messages(
        &self,
        messages: &mut [IggyMessage],
    ) -> Result<(), iggy_common::IggyError> {
        self.client
            .send_messages(
                &self.stream.clone().try_into().unwrap(),
                &self.topic.clone().try_into().unwrap(),
                &Partitioning::balanced(),
                messages,
            )
            .await
    }

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

        // Start the iggy server
        self.harness
            .start()
            .await
            .expect("Failed to start test harness");

        let client = self.create_iggy_client().await;
        client
            .create_stream(&iggy_setup.stream)
            .await
            .expect("Failed to create stream");
        let stream_id = iggy_setup
            .stream
            .clone()
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

        let connectors_config = ConnectorsRuntimeConfig::builder()
            .extra_envs(all_envs)
            .build();

        self.harness
            .server_mut()
            .set_connectors_runtime_config(connectors_config);
        self.harness
            .server_mut()
            .start_dependents()
            .await
            .expect("Failed to start connectors runtime");

        self.stream = iggy_setup.stream;
        self.topic = iggy_setup.topic;
    }

    pub async fn create_client(&self) -> ConnectorsIggyClient {
        ConnectorsIggyClient {
            stream: self.stream.clone(),
            topic: self.topic.clone(),
            client: self.create_iggy_client().await,
        }
    }

    async fn create_iggy_client(&self) -> IggyClient {
        self.harness
            .tcp_root_client()
            .await
            .expect("Failed to create root TCP client")
    }

    pub fn connectors_api_address(&self) -> Option<String> {
        self.harness
            .server()
            .connectors_runtime()
            .map(|cr| cr.http_address().to_string())
    }
}
