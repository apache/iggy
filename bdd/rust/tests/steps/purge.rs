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

use crate::common::purge_context::PurgeContext;
use crate::helpers::test_data::create_test_messages;
use cucumber::{given, then, when};
use iggy::prelude::{
    Client, ClientWrapper, CompressionAlgorithm, Consumer, ConsumerKind, DEFAULT_ROOT_PASSWORD,
    DEFAULT_ROOT_USERNAME, Identifier, IggyError, IggyExpiry, MaxTopicSize, MessageClient,
    Partitioning, PollingStrategy, StreamClient, SystemClient, TcpClient, TcpClientConfig,
    TopicClient, UserClient,
};
use std::sync::Arc;

#[given("I have a running Iggy server")]
pub async fn given_running_server(world: &mut PurgeContext) {
    let server_addr =
        std::env::var("IGGY_TCP_ADDRESS").unwrap_or_else(|_| "localhost:8090".to_string());
    world.server_addr = Some(server_addr);
}

#[given("I am authenticated as the root user")]
pub async fn given_authenticated_as_root(world: &mut PurgeContext) {
    let server_addr = world
        .server_addr
        .as_ref()
        .expect("Server should be running")
        .clone();

    let config = TcpClientConfig {
        server_address: server_addr,
        ..TcpClientConfig::default()
    };

    let tcp_client = TcpClient::create(Arc::new(config)).expect("Failed to create TCP client");
    Client::connect(&tcp_client)
        .await
        .expect("Client should connect");

    let client =
        iggy::clients::client::IggyClient::create(ClientWrapper::Tcp(tcp_client), None, None);

    client.ping().await.expect("Server should respond to ping");
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("Failed to login as root");

    world.client = Some(client);
}

#[given(regex = r#"^I have a stream with name "([^"]*)"$"#)]
pub async fn given_have_stream(world: &mut PurgeContext, name: String) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream = client
        .create_stream(&name)
        .await
        .expect("Should be able to create stream");
    world.stream_ids.insert(name, stream.id);
}

#[given(
    regex = r#"^I have a topic with name "([^"]*)" in stream "([^"]*)" with (\d+) partition[s]?$"#
)]
pub async fn given_have_topic(
    world: &mut PurgeContext,
    topic_name: String,
    stream_name: String,
    partitions_count: u32,
) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = world
        .stream_ids
        .get(&stream_name)
        .copied()
        .unwrap_or_else(|| panic!("Stream {stream_name:?} not found"));

    let topic = client
        .create_topic(
            &Identifier::numeric(stream_id).unwrap(),
            &topic_name,
            partitions_count,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("Should be able to create topic");
    world.topic_ids.insert(topic_name, topic.id);
}

#[given(
    regex = r#"^I have sent (\d+) messages to stream "([^"]*)", topic "([^"]*)", partition (\d+)$"#
)]
pub async fn given_have_sent_messages(
    world: &mut PurgeContext,
    messages_count: u32,
    stream_name: String,
    topic_name: String,
    partition_id: u32,
) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = world
        .stream_ids
        .get(&stream_name)
        .copied()
        .unwrap_or_else(|| panic!("Stream {stream_name:?} not found"));
    let topic_id = world
        .topic_ids
        .get(&topic_name)
        .copied()
        .unwrap_or_else(|| panic!("Topic {topic_name:?} not found"));

    let mut messages = create_test_messages(messages_count);
    client
        .send_messages(
            &Identifier::numeric(stream_id).unwrap(),
            &Identifier::numeric(topic_id).unwrap(),
            &Partitioning::partition_id(partition_id),
            &mut messages,
        )
        .await
        .expect("Should be able to send messages");
}

#[when(regex = r#"^I purge the stream "([^"]*)"$"#)]
pub async fn when_purge_stream(world: &mut PurgeContext, stream_name: String) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = world
        .stream_ids
        .get(&stream_name)
        .copied()
        .unwrap_or_else(|| panic!("Stream {stream_name:?} not found"));

    client
        .purge_stream(&Identifier::numeric(stream_id).unwrap())
        .await
        .expect("Should be able to purge stream");
}

#[when(regex = r#"^I purge topic "([^"]*)" in stream "([^"]*)"$"#)]
pub async fn when_purge_topic(world: &mut PurgeContext, topic_name: String, stream_name: String) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = world
        .stream_ids
        .get(&stream_name)
        .copied()
        .unwrap_or_else(|| panic!("Stream {stream_name:?} not found"));
    let topic_id = world
        .topic_ids
        .get(&topic_name)
        .copied()
        .unwrap_or_else(|| panic!("Topic {topic_name:?} not found"));

    client
        .purge_topic(
            &Identifier::numeric(stream_id).unwrap(),
            &Identifier::numeric(topic_id).unwrap(),
        )
        .await
        .expect("Should be able to purge topic");
}

#[when("I try to purge a non-existing stream")]
pub async fn when_try_purge_non_existing_stream(world: &mut PurgeContext) {
    let client = world.client.as_ref().expect("Client should be available");
    let result = client
        .purge_stream(&Identifier::numeric(u32::MAX).unwrap())
        .await;
    world.last_error = result.err();
}

#[when(regex = r#"^I try to purge a non-existing topic in stream "([^"]*)"$"#)]
pub async fn when_try_purge_non_existing_topic(world: &mut PurgeContext, stream_name: String) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = world
        .stream_ids
        .get(&stream_name)
        .copied()
        .unwrap_or_else(|| panic!("Stream {stream_name:?} not found"));

    let result = client
        .purge_topic(
            &Identifier::numeric(stream_id).unwrap(),
            &Identifier::numeric(u32::MAX).unwrap(),
        )
        .await;
    world.last_error = result.err();
}

#[when(
    regex = r#"^I poll messages from stream "([^"]*)", topic "([^"]*)", partition (\d+) starting from offset (\d+)$"#
)]
pub async fn when_poll_messages(
    world: &mut PurgeContext,
    stream_name: String,
    topic_name: String,
    partition_id: u32,
    start_offset: u64,
) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = world
        .stream_ids
        .get(&stream_name)
        .copied()
        .unwrap_or_else(|| panic!("Stream {stream_name:?} not found"));
    let topic_id = world
        .topic_ids
        .get(&topic_name)
        .copied()
        .unwrap_or_else(|| panic!("Topic {topic_name:?} not found"));

    let consumer = Consumer {
        kind: ConsumerKind::Consumer,
        id: Identifier::numeric(1).unwrap(),
    };

    let polled_messages = client
        .poll_messages(
            &Identifier::numeric(stream_id).unwrap(),
            &Identifier::numeric(topic_id).unwrap(),
            Some(partition_id),
            &consumer,
            &PollingStrategy::offset(start_offset),
            100,
            false,
        )
        .await
        .expect("Should be able to poll messages");

    world.last_polled_messages = Some(polled_messages);
}

#[then(regex = r"^I should receive (\d+) messages$")]
pub async fn then_should_receive_messages(world: &mut PurgeContext, expected_count: u32) {
    let polled_messages = world
        .last_polled_messages
        .as_ref()
        .expect("Should have polled messages");
    assert_eq!(
        polled_messages.messages.len() as u32,
        expected_count,
        "Should receive exactly {expected_count} messages",
    );
}

#[then("the purge operation should fail with a stream not found error")]
pub async fn then_purge_fails_with_stream_not_found(world: &mut PurgeContext) {
    let err = world
        .last_error
        .as_ref()
        .expect("Should have captured an error");
    assert!(
        matches!(err, IggyError::StreamIdNotFound(_)),
        "Expected StreamIdNotFound, got: {err:?}",
    );
}

#[then("the purge topic operation should fail with a topic not found error")]
pub async fn then_purge_topic_fails_with_topic_not_found(world: &mut PurgeContext) {
    let err = world
        .last_error
        .as_ref()
        .expect("Should have captured an error");
    assert!(
        matches!(err, IggyError::TopicIdNotFound(_, _)),
        "Expected TopicIdNotFound, got: {err:?}",
    );
}
