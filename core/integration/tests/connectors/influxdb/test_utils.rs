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

//! Shared test helpers for InfluxDB connector integration tests.
//! File: core/integration/tests/connectors/influxdb/test_utils.rs

use iggy::prelude::IggyClient;
// FIX: every client method lives behind a trait — all must be in scope explicitly
use crate::connectors::Identifier;
use iggy_binary_protocol::Client; // connect()
use iggy_binary_protocol::ConsumerGroupClient; // get_consumer_groups()
use iggy_binary_protocol::MessageClient; // poll_messages()
use iggy_binary_protocol::StreamClient; // create_stream(), delete_stream()
use iggy_binary_protocol::TopicClient; // create_topic()
use iggy_binary_protocol::UserClient; // login_user()
use iggy_common::{Consumer, IggyExpiry, PollingStrategy};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::time::sleep;

pub struct ConnectorRuntimeHandle {
    process: Child,
}

impl Drop for ConnectorRuntimeHandle {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}

pub async fn build_iggy_client() -> IggyClient {
    // FIX: with_server_address takes String not &str
    // FIX: explicit type on `client` breaks E0282 chain for connect()/login_user()
    let client: IggyClient = IggyClient::builder()
        .with_tcp()
        .with_server_address("127.0.0.1:8090".to_string())
        .build()
        .expect("Failed to build IggyClient");

    // FIX: Client trait in scope
    client
        .connect()
        .await
        .expect("Failed to connect to iggy-server");
    // FIX: UserClient trait in scope
    client
        .login_user("iggy", "iggy")
        .await
        .expect("Failed to login");
    client
}

pub async fn create_stream_and_topic(
    iggy: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    partitions: u32,
) {
    // FIX: create_stream takes 1 arg (name only), confirmed from compiler
    iggy.create_stream(stream_name)
        .await
        .expect("create_stream failed");

    // FIX: create_topic takes 7 args; message_expiry is IggyExpiry (not Option)
    // confirmed signature: (stream_id, name, partitions, replication_factor,
    //                       consumer_group_id, message_expiry: IggyExpiry, max_topic_size)
    iggy.create_topic(
        &stream_name.try_into().unwrap(),
        topic_name,
        partitions,
        Default::default(),
        None,
        IggyExpiry::ServerDefault,
        iggy_common::MaxTopicSize::ServerDefault,
    )
    .await
    .expect("create_topic failed");
}

pub async fn cleanup_stream(iggy: &IggyClient, stream_name: &str) {
    let _ = iggy.delete_stream(&stream_name.try_into().unwrap()).await;
}

pub async fn start_connector_runtime(config_toml: &str) -> ConnectorRuntimeHandle {
    let config_path = format!("/tmp/iggy_connector_test_{}.toml", std::process::id());
    std::fs::write(&config_path, config_toml).expect("Failed to write connector config");

    let process = Command::new("target/release/iggy-connectors")
        .arg("--config")
        .arg(&config_path)
        .spawn()
        .expect("Failed to spawn connector runtime");

    sleep(Duration::from_millis(500)).await;
    ConnectorRuntimeHandle { process }
}

pub async fn wait_for_connector(
    iggy: &IggyClient,
    stream: &str,
    topic: &str,
    consumer_group: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "Timed out waiting for connector consumer group '{}' on {}/{}",
                consumer_group, stream, topic
            );
        }
        // Wrap your 'stream' and 'topic' strings into Identifiers
        let stream_id = Identifier::from_str_value(stream).expect("Invalid stream ID");
        let topic_id = Identifier::from_str_value(topic).expect("Invalid topic ID");

        // Change this:
        if let Ok(groups) = iggy.get_consumer_groups(&stream_id, &topic_id).await {
            if groups.iter().any(|g| g.name == consumer_group) {
                return;
            }
        }

        sleep(Duration::from_millis(200)).await;
    }
}

pub async fn wait_for_messages(
    iggy: &IggyClient,
    stream: &str,
    topic: &str,
    expected_count: u32,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    let consumer = Consumer::default();

    loop {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "Timed out waiting for {} messages in {}/{}",
                expected_count, stream, topic
            );
        }

        if let Ok(polled) = iggy
            .poll_messages(
                &stream.try_into().unwrap(),
                &topic.try_into().unwrap(),
                Some(1),
                &consumer,
                &PollingStrategy::offset(0),
                expected_count,
                false,
            )
            .await
        {
            if polled.messages.len() >= expected_count as usize {
                return;
            }
        }

        sleep(Duration::from_millis(300)).await;
    }
}
