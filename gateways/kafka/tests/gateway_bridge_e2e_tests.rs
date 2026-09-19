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

//! Full-stack `KafkaGateway` + real `IggyBridge` + real `iggy-server` tests, over the actual
//! Kafka wire (raw TCP frames via `tests/common/tcp.rs`) - the one combination no other suite in
//! this crate exercises. `bridge_iggy_integration_tests.rs` calls `IggyBridge` methods directly
//! (never through `KafkaGateway`/the wire protocol); `api_handler_tests.rs`/`server_e2e_tests.rs`
//! run `KafkaGateway` over the wire, but only against `FakeBridge` (never a real Iggy backend).
//! This file is the only place a real Kafka client's exact bytes, decoded by the real protocol
//! layer, reach real `IggyBridge` provisioning/lookup logic, and the real response bytes that
//! come back are decoded again on the way out.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::sync::Arc;

use iggy::prelude::{Identifier, IggyMessage, MessageClient, Partitioning, TopicClient};
use serial_test::serial;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::protocol::api::{
    API_KEY_CREATE_TOPICS, API_KEY_LIST_OFFSETS, API_KEY_METADATA, ERROR_NONE,
    ERROR_TOPIC_ALREADY_EXISTS,
};

use codec::Decoder;
use iggy_server::{TestServer, raw_client};
use server::spawn_test_server;
use tcp::round_trip;

/// Connects a real `IggyBridge` to `server` and spawns a `KafkaGateway` in front of it -
/// the one combination this whole file exists to exercise.
async fn spawn_gateway_over_real_bridge(
    server: &TestServer,
) -> (std::net::SocketAddr, tokio::sync::broadcast::Sender<()>) {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    spawn_test_server(Arc::new(bridge)).await
}

/// `CreateTopics` then `Metadata` for the same topic, both over the real wire against a real
/// Iggy backend: the topic `CreateTopics` provisions must be the exact one `Metadata` reports
/// back, partition count included - the two handlers going through the same real bridge state
/// is the thing no `FakeBridge`-backed test can prove.
#[tokio::test]
#[serial]
async fn e2e_create_topics_then_metadata_round_trip() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (addr, _shutdown) = spawn_gateway_over_real_bridge(&server).await;

    let create_body = wire::build_create_topics_simple_request(5, "orders", 3, 1, false);
    let (_corr, create_resp) = round_trip(addr, API_KEY_CREATE_TOPICS, 5, 1, &create_body).await;
    let mut d = Decoder::new(create_resp);
    let _throttle = d.read_i32().unwrap();
    let _topics = d.read_varint().unwrap();
    let _topic = d.read_compact_nullable_string().unwrap();
    assert_eq!(
        d.read_i16().unwrap(),
        ERROR_NONE,
        "CreateTopics must succeed"
    );

    let metadata_body = wire::build_metadata_flexible_request(&["orders"]);
    let (_corr, metadata_resp) = round_trip(addr, API_KEY_METADATA, 9, 2, &metadata_body).await;
    let mut d = Decoder::new(metadata_resp);
    let _throttle = d.read_i32().unwrap();
    let brokers = d.read_varint().unwrap();
    assert_eq!(brokers, 2, "one broker"); // N+1
    let _node_id = d.read_i32().unwrap();
    let _host = d.read_compact_nullable_string().unwrap();
    let _port = d.read_i32().unwrap();
    let _rack = d.read_compact_nullable_string().unwrap();
    d.read_tagged_fields().unwrap(); // broker tagged fields
    let _cluster_id = d.read_compact_nullable_string().unwrap();
    let _controller_id = d.read_i32().unwrap();
    let topics = d.read_varint().unwrap();
    assert_eq!(topics, 2, "one topic"); // N+1
    let error_code = d.read_i16().unwrap();
    assert_eq!(
        error_code, ERROR_NONE,
        "Metadata must find the topic CreateTopics just provisioned"
    );
    let name = d.read_compact_nullable_string().unwrap();
    assert_eq!(name, Some("orders".to_string()));
    let _internal = d.read_bool().unwrap();
    let partitions = d.read_varint().unwrap();
    assert_eq!(
        partitions, 4,
        "must report the 3 partitions CreateTopics actually provisioned" // N+1
    );
}

/// Wire-level proof of the `CreateTopics` idempotent-success fix: re-sending the exact request
/// an earlier call already satisfied must answer `TOPIC_ALREADY_EXISTS` (36), not silently
/// succeed a second time - through the real protocol decode/bridge/encode path, not `FakeBridge`.
#[tokio::test]
#[serial]
async fn e2e_create_topics_recreate_returns_topic_already_exists() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (addr, _shutdown) = spawn_gateway_over_real_bridge(&server).await;

    let body = wire::build_create_topics_simple_request(5, "orders", 2, 1, false);

    let (_corr, first) = round_trip(addr, API_KEY_CREATE_TOPICS, 5, 1, &body).await;
    let mut d = Decoder::new(first);
    let _throttle = d.read_i32().unwrap();
    let _topics = d.read_varint().unwrap();
    let _topic = d.read_compact_nullable_string().unwrap();
    assert_eq!(
        d.read_i16().unwrap(),
        ERROR_NONE,
        "first create must succeed"
    );

    let (_corr, second) = round_trip(addr, API_KEY_CREATE_TOPICS, 5, 2, &body).await;
    let mut d = Decoder::new(second);
    let _throttle = d.read_i32().unwrap();
    let _topics = d.read_varint().unwrap();
    let _topic = d.read_compact_nullable_string().unwrap();
    assert_eq!(
        d.read_i16().unwrap(),
        ERROR_TOPIC_ALREADY_EXISTS,
        "recreating an existing topic over the real wire must not silently succeed again"
    );
}

/// Wire-level proof that `num_partitions = -1` with a manual `assignments` list resolves the
/// real partition count from the assignment length, not `DEFAULT_PARTITION_COUNT` - verified
/// against the actual Iggy topic `CreateTopics` provisioned, not just the response bytes.
#[tokio::test]
#[serial]
async fn e2e_create_topics_with_manual_assignment_creates_the_real_iggy_partition_count() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (addr, _shutdown) = spawn_gateway_over_real_bridge(&server).await;

    let body = wire::build_create_topics_with_assignments_request(5, "orders", 4);
    let (_corr, resp) = round_trip(addr, API_KEY_CREATE_TOPICS, 5, 1, &body).await;
    let mut d = Decoder::new(resp);
    let _throttle = d.read_i32().unwrap();
    let _topics = d.read_varint().unwrap();
    let _topic = d.read_compact_nullable_string().unwrap();
    assert_eq!(
        d.read_i16().unwrap(),
        ERROR_NONE,
        "create with assignments must succeed"
    );
    let _error_message = d.read_compact_nullable_string().unwrap();
    // `topic_config_error_code` is not a plain sequential field - `kafka_protocol`'s own encoder
    // (create_topics_response.rs) only ever writes it inside the tagged-fields section, and only
    // when non-zero; this bridge's response never sets it, so it never appears on the wire here.
    let num_partitions = d.read_i32().unwrap();
    assert_eq!(
        num_partitions, 4,
        "response must echo the 4 partitions implied by the assignment list"
    );

    let raw = raw_client(&server).await;
    let topic = raw
        .get_topic(
            &Identifier::named("kafka").expect("valid stream name"),
            &Identifier::named("orders").expect("valid topic name"),
        )
        .await
        .expect("get_topic call")
        .expect("topic must exist on the real Iggy backend");
    assert_eq!(
        topic.partitions_count, 4,
        "the real Iggy topic must actually have 4 partitions, not the 1-partition broker default"
    );
}

/// Wire-level proof of the Metadata null-vs-empty-array fix: an explicit empty `topics` array
/// (KIP-4's `describeCluster()` shape) must list no topics even when real topics exist on the
/// backend, while a null array still lists all of them - distinguishable only against a backend
/// that actually has a topic to (not) list.
#[tokio::test]
#[serial]
async fn e2e_metadata_empty_array_lists_no_topics_while_null_lists_all() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed a real topic so empty-vs-null is actually distinguishable");
    let (addr, _shutdown) = spawn_test_server(Arc::new(bridge)).await;

    let empty_body = wire::build_metadata_legacy_request_for_version(0, &[]);
    let (_corr, empty_resp) = round_trip(addr, API_KEY_METADATA, 0, 1, &empty_body).await;
    let mut d = Decoder::new(empty_resp);
    let brokers = d.read_i32().unwrap();
    assert_eq!(brokers, 1);
    let _node_id = d.read_i32().unwrap();
    let _host = d.read_nullable_string().unwrap();
    let _port = d.read_i32().unwrap();
    let topics = d.read_i32().unwrap();
    assert_eq!(
        topics, 0,
        "explicit empty topics array must list no topics, even though 'orders' exists"
    );

    let null_body = wire::build_metadata_all_topics_legacy(0);
    let (_corr, null_resp) = round_trip(addr, API_KEY_METADATA, 0, 2, &null_body).await;
    let mut d = Decoder::new(null_resp);
    let brokers = d.read_i32().unwrap();
    assert_eq!(brokers, 1);
    let _node_id = d.read_i32().unwrap();
    let _host = d.read_nullable_string().unwrap();
    let _port = d.read_i32().unwrap();
    let topics = d.read_i32().unwrap();
    assert_eq!(topics, 1, "null topics array must list the one real topic");
}

/// `ListOffsets` LATEST after a real produce, through the Kafka wire against a real backend:
/// Produce itself is still a stub (not wired to the bridge), so messages are written directly
/// via the Iggy SDK - `ListOffsets` is what must reflect them for real.
#[tokio::test]
#[serial]
async fn e2e_list_offsets_latest_reflects_real_produced_messages() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("stream and topic must exist before producing");

    let stream_id = Identifier::named("kafka").expect("valid stream name");
    let topic_id = Identifier::named("orders").expect("valid topic name");
    let mut messages: Vec<IggyMessage> = (0..5)
        .map(|i| IggyMessage::from(format!("message-{i}")))
        .collect();
    let raw = raw_client(&server).await;
    raw.send_messages(
        &stream_id,
        &topic_id,
        &Partitioning::partition_id(0),
        &mut messages,
    )
    .await
    .expect("send 5 messages directly via the SDK");

    let (addr, _shutdown) = spawn_test_server(Arc::new(bridge)).await;
    let body = wire::build_list_offsets_request(6, "orders", 0);
    let (_corr, resp) = round_trip(addr, API_KEY_LIST_OFFSETS, 6, 1, &body).await;
    let mut d = Decoder::new(resp);
    let _throttle = d.read_i32().unwrap();
    let topics_plus_one = d.read_varint().unwrap();
    assert_eq!(topics_plus_one, 2, "one topic");
    let _name = d.read_compact_nullable_string().unwrap();
    let parts_plus_one = d.read_varint().unwrap();
    assert_eq!(parts_plus_one, 2, "one partition");
    let _partition_index = d.read_i32().unwrap();
    assert_eq!(d.read_i16().unwrap(), ERROR_NONE);
    let _timestamp = d.read_i64().unwrap();
    let offset = d.read_i64().unwrap();
    assert_eq!(
        offset, 5,
        "LATEST must reflect the 5 messages actually produced (offsets 0..4), not a constant"
    );
}
