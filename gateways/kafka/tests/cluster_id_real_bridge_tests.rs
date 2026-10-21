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

//! The cluster id stored in a real, spawned `iggy-server`: loaded by the bridge, and reported by
//! the compiled gateway binary in Metadata and `DescribeCluster` across a restart.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;

use bytes::Bytes;
use futures::future::join_all;
use iggy::prelude::{
    Consumer, Durability, Identifier, IggyExpiry, IggyMessage, MaxTopicSize, MessageClient,
    Partitioning, Permissions, PollingStrategy, StreamClient, StreamPermissions, TopicClient,
    TopicCreateOptions, UserClient, UserStatus,
};
use kafka_protocol::messages::{DescribeClusterResponse, MetadataResponse};
use kafka_protocol::protocol::Decodable;
use secrecy::SecretString;
use serial_test::serial;

use iggy_gateway_kafka::bridge::{
    BridgeError, CLUSTER_ID_TOPIC, IggyBridge, TopicMapping, cluster_id_stream,
};
use iggy_gateway_kafka::cluster_id::ClusterId;
use iggy_gateway_kafka::protocol::api::{API_KEY_DESCRIBE_CLUSTER, API_KEY_METADATA};

#[path = "common/codec.rs"]
mod codec;
#[path = "common/gateway_process.rs"]
mod gateway_process;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/tcp.rs"]
mod tcp;

use codec::Encoder;
use gateway_process::TestGateway;
use iggy_server::{TestServer, raw_client};
use tcp::round_trip;

const DEFAULT_STREAM: &str = "kafka";

async fn connect(server: &TestServer) -> IggyBridge {
    IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server")
}

/// Connects a bridge as a new user holding `permissions` on the id stream only, which root
/// creates first.
async fn connect_scoped(
    server: &TestServer,
    username: &str,
    permissions: StreamPermissions,
) -> IggyBridge {
    let raw = raw_client(server).await;
    let stream = raw
        .create_stream(&cluster_id_stream(DEFAULT_STREAM))
        .await
        .expect("create stream");
    raw.create_user(
        username,
        "scoped-password",
        UserStatus::Active,
        Some(Permissions {
            streams: Some(BTreeMap::from([(stream.id as usize, permissions)])),
            ..Permissions::default()
        }),
    )
    .await
    .expect("create user");
    let mut config = server.test_config();
    config.username = username.to_string();
    config.password = SecretString::from("scoped-password");
    IggyBridge::connect(config)
        .await
        .expect("bridge should connect as the scoped user")
}

async fn create_id_topic(server: &TestServer) {
    raw_client(server)
        .await
        .create_topic(
            &Identifier::named(&cluster_id_stream(DEFAULT_STREAM)).expect("stream name"),
            CLUSTER_ID_TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                max_topic_size: Some(MaxTopicSize::Unlimited),
                durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
}

/// Every message in the id topic, in offset order.
async fn stored_ids(server: &TestServer, default_stream: &str) -> Vec<String> {
    let raw = raw_client(server).await;
    let polled = raw
        .poll_messages(
            &Identifier::named(&cluster_id_stream(default_stream)).expect("stream name"),
            &Identifier::named(CLUSTER_ID_TOPIC).expect("topic name"),
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            100,
            false,
        )
        .await
        .expect("poll the id topic");
    polled
        .messages
        .iter()
        .map(|message| String::from_utf8(message.payload.to_vec()).expect("utf-8 id"))
        .collect()
}

#[tokio::test]
#[serial]
async fn given_no_stored_id_when_two_bridges_load_it_should_store_one_and_both_report_it() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;

    let first = connect(&server)
        .await
        .load_cluster_id()
        .await
        .expect("load");
    let second = connect(&server)
        .await
        .load_cluster_id()
        .await
        .expect("load");

    assert_eq!(first, second);
    assert_eq!(
        stored_ids(&server, DEFAULT_STREAM).await,
        vec![first.to_string()]
    );
}

/// The id topic must keep its one message whatever the server's defaults are, through a crash
/// too.
#[tokio::test]
#[serial]
async fn given_a_new_id_when_stored_should_be_persisted_never_expire_and_have_no_size_cap() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    connect(&server)
        .await
        .load_cluster_id()
        .await
        .expect("load");

    let raw = raw_client(&server).await;
    let topic = raw
        .get_topic(
            &Identifier::named(&cluster_id_stream(DEFAULT_STREAM)).expect("stream name"),
            &Identifier::named(CLUSTER_ID_TOPIC).expect("topic name"),
        )
        .await
        .expect("get_topic")
        .expect("the id topic exists");
    assert_eq!(topic.partitions_count, 1);
    assert_eq!(topic.message_expiry, IggyExpiry::NeverExpire);
    assert_eq!(topic.max_topic_size, MaxTopicSize::Unlimited);
    assert_eq!(
        TopicCreateOptions::from_resource_options(&topic.options).durability,
        Durability::Persisted
    );
}

/// A crash right after the write must not lose the id: the next start would report a new
/// cluster.
#[tokio::test]
#[serial]
async fn given_a_stored_id_when_iggy_server_crashes_should_load_the_same_id_after_restart() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let before = connect(&server)
        .await
        .load_cluster_id()
        .await
        .expect("load");
    server.crash();

    let server = TestServer::spawn(data_dir.path()).await;
    let after = connect(&server)
        .await
        .load_cluster_id()
        .await
        .expect("load");

    assert_eq!(before, after);
    assert_eq!(
        stored_ids(&server, DEFAULT_STREAM).await,
        vec![before.to_string()]
    );
}

/// Gateways that start together race to write. Whichever message lands at offset 0 is the id
/// every one of them reports, however many proposals land after it.
#[tokio::test]
#[serial]
async fn given_bridges_loading_at_once_when_none_is_stored_should_all_report_the_first_write() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridges = join_all((0..6).map(|_| connect(&server))).await;

    let loaded: Vec<ClusterId> = join_all(bridges.iter().map(IggyBridge::load_cluster_id))
        .await
        .into_iter()
        .map(|result| result.expect("load"))
        .collect();

    let stored = stored_ids(&server, DEFAULT_STREAM).await;
    let first = stored.first().expect("an id is stored");
    for id in &loaded {
        assert_eq!(id.as_str(), first, "every gateway reports offset 0");
    }
}

#[tokio::test]
#[serial]
async fn given_a_stored_message_that_is_not_an_id_when_loaded_should_refuse_it() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let raw = raw_client(&server).await;
    let stream_name = cluster_id_stream(DEFAULT_STREAM);
    raw.create_stream(&stream_name)
        .await
        .expect("create stream");
    let stream_id = Identifier::named(&stream_name).expect("stream name");
    raw.create_topic(
        &stream_id,
        CLUSTER_ID_TOPIC,
        &TopicCreateOptions {
            partitions_count: Some(1),
            ..TopicCreateOptions::default()
        },
    )
    .await
    .expect("create topic");
    raw.send_messages(
        &stream_id,
        &Identifier::named(CLUSTER_ID_TOPIC).expect("topic name"),
        &Partitioning::partition_id(0),
        &mut [IggyMessage::from("not a cluster id")],
    )
    .await
    .expect("send");

    let error = connect(&server)
        .await
        .load_cluster_id()
        .await
        .expect_err("a stored value that is not an id must not be reported");
    assert!(
        matches!(&error, BridgeError::InvalidClusterId { stream, .. } if *stream == stream_name),
        "got {error:?}"
    );
}

/// Two deployments that map to different default streams are two Kafka clusters.
#[tokio::test]
#[serial]
async fn given_two_default_streams_when_loaded_should_report_two_ids() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let mut other = server.test_config();
    other.topic_mapping =
        TopicMapping::new("other".to_string(), HashMap::new()).expect("valid mapping");

    let kafka = connect(&server)
        .await
        .load_cluster_id()
        .await
        .expect("load");
    let other = IggyBridge::connect(other)
        .await
        .expect("connect")
        .load_cluster_id()
        .await
        .expect("load");

    assert_ne!(kafka, other);
    assert_eq!(stored_ids(&server, "other").await, vec![other.to_string()]);
}

/// The documented least-privilege setup: an operator creates the stream and the topic, and the
/// gateway's user may only poll and send there. Iggy refuses that user a create even for a name
/// that exists, so the bridge must not ask for one.
#[tokio::test]
#[serial]
async fn given_a_pre_created_topic_when_a_user_with_only_poll_and_send_loads_should_store_the_id() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = connect_scoped(
        &server,
        "poll-send-only",
        StreamPermissions {
            poll_messages: true,
            send_messages: true,
            ..StreamPermissions::default()
        },
    )
    .await;
    create_id_topic(&server).await;

    let stored = bridge.load_cluster_id().await.expect("store the id");
    let read = bridge.load_cluster_id().await.expect("read the id");

    assert_eq!(stored, read);
    assert_eq!(
        stored_ids(&server, DEFAULT_STREAM).await,
        vec![stored.to_string()]
    );
}

/// A user that may create topics on a pre-created stream, but not streams, creates only the
/// topic.
#[tokio::test]
#[serial]
async fn given_a_pre_created_stream_when_a_user_with_manage_topics_loads_should_create_the_topic() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = connect_scoped(
        &server,
        "topic-manager",
        StreamPermissions {
            manage_topics: true,
            poll_messages: true,
            send_messages: true,
            ..StreamPermissions::default()
        },
    )
    .await;

    let stored = bridge.load_cluster_id().await.expect("store the id");
    let read = bridge.load_cluster_id().await.expect("read the id");

    assert_eq!(stored, read);
    assert_eq!(
        stored_ids(&server, DEFAULT_STREAM).await,
        vec![stored.to_string()]
    );
}

fn decode<M: Decodable>(mut body: Bytes, version: i16) -> M {
    M::decode(&mut body, version).expect("decode response")
}

fn describe_cluster_request() -> Bytes {
    let mut enc = Encoder::with_capacity(8);
    enc.write_bool(false); // include_cluster_authorized_operations
    enc.write_i8(1); // endpoint_type: brokers
    enc.write_bool(false); // include_fenced_brokers
    enc.write_empty_tagged_fields();
    enc.freeze()
}

fn all_topics_metadata_request() -> Bytes {
    let mut enc = Encoder::with_capacity(8);
    enc.write_varint(0); // null topics: all of them
    enc.write_bool(false); // allow_auto_topic_creation
    enc.write_bool(false); // include_cluster_authorized_operations
    enc.write_bool(false); // include_topic_authorized_operations
    enc.write_empty_tagged_fields();
    enc.freeze()
}

/// The id the gateway at `address` reports in `DescribeCluster` v2, after checking that
/// Metadata v9 reports the same.
async fn reported_id(address: &str) -> String {
    let addr: SocketAddr = address.parse().expect("gateway address");
    let (_, body) = round_trip(
        addr,
        API_KEY_DESCRIBE_CLUSTER,
        2,
        1,
        &describe_cluster_request(),
    )
    .await;
    let described: DescribeClusterResponse = decode(body, 2);
    assert_eq!(described.error_code, 0);

    let (_, body) = round_trip(addr, API_KEY_METADATA, 9, 2, &all_topics_metadata_request()).await;
    let metadata: MetadataResponse = decode(body, 9);
    assert_eq!(
        metadata.cluster_id.as_deref(),
        Some(described.cluster_id.as_str()),
        "Metadata and DescribeCluster report one id"
    );
    described.cluster_id.to_string()
}

/// The id survives a restart of the gateway binary.
#[tokio::test]
#[serial]
async fn given_the_gateway_binary_when_restarted_should_report_the_same_id() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;

    let gateway = TestGateway::spawn(&server).await;
    let before = reported_id(&gateway.address).await;
    drop(gateway);
    let gateway = TestGateway::spawn(&server).await;
    let after = reported_id(&gateway.address).await;

    assert_eq!(before, after);
    assert_eq!(stored_ids(&server, DEFAULT_STREAM).await, vec![before]);
}
