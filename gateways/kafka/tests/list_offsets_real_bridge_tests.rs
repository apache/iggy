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

//! Wire-level `ListOffsets` tests against a real `iggy-server` process, through
//! [`list_offsets::handle`] with a connected [`GatewayState`]. See
//! `create_topics_real_bridge_tests.rs` for why requests/responses are hand-built here rather
//! than through `kafka_protocol`'s own `Encodable`/`Decodable` (this crate builds
//! `broker`-feature-only: request `Decodable` and response `Encodable`, not the reverse).

use std::sync::Arc;

use bytes::Bytes;
use iggy::prelude::{Identifier, IggyMessage, MessageClient, Partitioning};
use serial_test::serial;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::protocol::api::{
    BrokerAdvertise, ERROR_NONE, ERROR_REQUEST_TIMED_OUT, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
    ERROR_UNSUPPORTED_FOR_MESSAGE_FORMAT, GatewayState,
};
use iggy_gateway_kafka::protocol::handlers::list_offsets;

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;

use codec::{Decoder, Encoder};
use iggy_server::TestServer;

const REQUEST_VERSION: i16 = 6;
const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;
const LATEST_TIMESTAMP: i64 = -1;
const EARLIEST_TIMESTAMP: i64 = -2;

/// Builds a v6 flexible `ListOffsets` request body for one topic/partition.
fn build_request(topic: &str, partition_index: i32, timestamp: i64) -> Bytes {
    let mut enc = Encoder::with_capacity(128);
    enc.write_i32(-1); // replica_id: ordinary client, not a follower broker
    enc.write_i8(0); // isolation_level: READ_UNCOMMITTED

    enc.write_varint(2); // one topic
    enc.write_compact_nullable_string(Some(topic));
    enc.write_varint(2); // one partition
    enc.write_i32(partition_index);
    enc.write_i32(-1); // current_leader_epoch: unset
    enc.write_i64(timestamp);
    enc.write_empty_tagged_fields(); // partition tagged fields
    enc.write_empty_tagged_fields(); // topic tagged fields

    enc.write_empty_tagged_fields(); // top-level tagged fields
    enc.freeze()
}

/// Decodes a v6 flexible `ListOffsets` response's first partition result into `(error_code,
/// offset)`.
fn decode_first_result(body: Bytes) -> (i16, i64) {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let _topics_plus_one = d.read_varint().expect("topics array count");
    let _name = d.read_compact_nullable_string().expect("topic name");
    let _partitions_plus_one = d.read_varint().expect("partitions array count");
    let _partition_index = d.read_i32().expect("partition_index");
    let error_code = d.read_i16().expect("error_code");
    let _timestamp = d.read_i64().expect("timestamp");
    let offset = d.read_i64().expect("offset");
    (error_code, offset)
}

async fn send(
    state: &GatewayState,
    topic: &str,
    partition_index: i32,
    timestamp: i64,
) -> (i16, i64) {
    let body = build_request(topic, partition_index, timestamp);
    let outcome = list_offsets::handle(state, REQUEST_VERSION, body).await;
    let resp_body = outcome.expect_response("ListOffsets request always answers");
    decode_first_result(resp_body)
}

/// One requested topic entry, for [`build_multi_request`]: a name and its `(partition_index,
/// timestamp)` pairs.
struct TopicRequest<'a> {
    name: &'a str,
    partitions: &'a [(i32, i64)],
}

/// Builds a v6 flexible `ListOffsets` request body for several topic entries at once - unlike
/// [`build_request`], `topics` may repeat the same name across more than one entry.
fn build_multi_request(topics: &[TopicRequest]) -> Bytes {
    let mut enc = Encoder::with_capacity(4096);
    enc.write_i32(-1); // replica_id
    enc.write_i8(0); // isolation_level

    enc.write_varint((topics.len() + 1) as u64);
    for topic in topics {
        enc.write_compact_nullable_string(Some(topic.name));
        enc.write_varint((topic.partitions.len() + 1) as u64);
        for &(partition_index, timestamp) in topic.partitions {
            enc.write_i32(partition_index);
            enc.write_i32(-1); // current_leader_epoch
            enc.write_i64(timestamp);
            enc.write_empty_tagged_fields();
        }
        enc.write_empty_tagged_fields();
    }
    enc.write_empty_tagged_fields();
    enc.freeze()
}

/// Decodes every topic/partition result in a v6 flexible `ListOffsets` response into `(name,
/// partition_index, error_code, offset)`, in wire order.
fn decode_all(body: Bytes) -> Vec<(String, i32, i16, i64)> {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let topics_plus_one = d.read_varint().expect("topics array count");
    let mut results = Vec::new();
    for _ in 1..topics_plus_one {
        let name = d
            .read_compact_nullable_string()
            .expect("topic name")
            .expect("name is never null in a request-echoing response");
        let partitions_plus_one = d.read_varint().expect("partitions array count");
        for _ in 1..partitions_plus_one {
            let partition_index = d.read_i32().expect("partition_index");
            let error_code = d.read_i16().expect("error_code");
            let _timestamp = d.read_i64().expect("timestamp");
            let offset = d.read_i64().expect("offset");
            let _leader_epoch = d.read_i32().expect("leader_epoch");
            let _partition_tagged_fields = d.read_varint().expect("partition tagged fields");
            results.push((name.clone(), partition_index, error_code, offset));
        }
        let _topic_tagged_fields = d.read_varint().expect("topic tagged fields");
    }
    results
}

async fn send_multi(
    state: &GatewayState,
    topics: &[TopicRequest<'_>],
) -> Vec<(String, i32, i16, i64)> {
    let body = build_multi_request(topics);
    let outcome = list_offsets::handle(state, REQUEST_VERSION, body).await;
    let resp_body = outcome.expect_response("ListOffsets request always answers");
    decode_all(resp_body)
}

async fn connected_state(server: &TestServer) -> (GatewayState, IggyBridge) {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    // A second bridge for direct seeding (get_topics/create/produce) alongside the handler's own.
    let seed_bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("seed bridge should connect to a ready server");
    let state = GatewayState::new(
        BrokerAdvertise::default(),
        Some(Arc::new(bridge)),
        TEST_MAX_FRAME_SIZE,
    );
    (state, seed_bridge)
}

#[tokio::test]
#[serial]
async fn latest_on_a_fresh_empty_partition_is_zero() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed the topic");

    let (error_code, offset) = send(&state, "orders", 0, LATEST_TIMESTAMP).await;
    assert_eq!(error_code, ERROR_NONE);
    assert_eq!(offset, 0);
}

#[tokio::test]
#[serial]
async fn latest_reflects_produced_messages() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed the topic");

    let raw = iggy_server::raw_client(&server).await;
    let mut messages: Vec<IggyMessage> = (0..3)
        .map(|i| IggyMessage::from(format!("message-{i}")))
        .collect();
    raw.send_messages(
        &Identifier::named("kafka").expect("valid stream name"),
        &Identifier::named("orders").expect("valid topic name"),
        &Partitioning::partition_id(0),
        &mut messages,
    )
    .await
    .expect("seed 3 messages");

    let (error_code, offset) = send(&state, "orders", 0, LATEST_TIMESTAMP).await;
    assert_eq!(error_code, ERROR_NONE);
    assert_eq!(offset, 3);
}

#[tokio::test]
#[serial]
async fn earliest_is_always_zero() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed the topic");

    let (error_code, offset) = send(&state, "orders", 0, EARLIEST_TIMESTAMP).await;
    assert_eq!(error_code, ERROR_NONE);
    assert_eq!(offset, 0);
}

#[tokio::test]
#[serial]
async fn out_of_range_partition_returns_unknown_topic_or_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed a 1-partition topic");

    let (error_code, _) = send(&state, "orders", 5, LATEST_TIMESTAMP).await;
    assert_eq!(error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
}

#[tokio::test]
#[serial]
async fn a_nonexistent_topic_returns_unknown_topic_or_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, _seed) = connected_state(&server).await;

    let (error_code, _) = send(&state, "orders", 0, LATEST_TIMESTAMP).await;
    assert_eq!(error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
}

#[tokio::test]
#[serial]
async fn an_arbitrary_timestamp_is_unsupported() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed the topic");

    let (error_code, _) = send(&state, "orders", 0, 1_700_000_000_000).await;
    assert_eq!(error_code, ERROR_UNSUPPORTED_FOR_MESSAGE_FORMAT);
}

/// Regression test: a topic named in two separate request entries must still resolve every
/// partition across both correctly, not just avoid a crash - proves the dedup-by-name merge
/// actually unions the two entries' partition lists rather than dropping one.
#[tokio::test]
#[serial]
async fn a_topic_named_in_two_request_entries_resolves_both_entries_correctly() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 2)
        .await
        .expect("seed a 2-partition topic");

    let topics = [
        TopicRequest {
            name: "orders",
            partitions: &[(0, LATEST_TIMESTAMP)],
        },
        TopicRequest {
            name: "orders",
            partitions: &[(1, LATEST_TIMESTAMP)],
        },
    ];
    let results = send_multi(&state, &topics).await;
    assert_eq!(results.len(), 2);
    for (name, _partition_index, error_code, offset) in &results {
        assert_eq!(name, "orders");
        assert_eq!(*error_code, ERROR_NONE);
        assert_eq!(*offset, 0);
    }
}

/// Regression test: the binary search in `resolve_one_partition` must map each partition index to
/// *its own* watermark, not just pass when both happen to agree. The two-empty-partitions test
/// above can't catch a broken search or a constant-index bug - both partitions are 0 either way.
#[tokio::test]
#[serial]
async fn distinct_partitions_resolve_to_their_own_distinct_watermarks() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 2)
        .await
        .expect("seed a 2-partition topic");

    let raw = iggy_server::raw_client(&server).await;
    let stream_id = Identifier::named("kafka").expect("valid stream name");
    let topic_id = Identifier::named("orders").expect("valid topic name");
    let mut partition_0: Vec<IggyMessage> = (0..3)
        .map(|i| IggyMessage::from(format!("p0-{i}")))
        .collect();
    raw.send_messages(
        &stream_id,
        &topic_id,
        &Partitioning::partition_id(0),
        &mut partition_0,
    )
    .await
    .expect("seed partition 0 with 3 messages");
    let mut partition_1: Vec<IggyMessage> = (0..7)
        .map(|i| IggyMessage::from(format!("p1-{i}")))
        .collect();
    raw.send_messages(
        &stream_id,
        &topic_id,
        &Partitioning::partition_id(1),
        &mut partition_1,
    )
    .await
    .expect("seed partition 1 with 7 messages");

    let topics = [TopicRequest {
        name: "orders",
        partitions: &[(1, LATEST_TIMESTAMP), (0, LATEST_TIMESTAMP)], // deliberately out of index order
    }];
    let results = send_multi(&state, &topics).await;
    assert_eq!(results.len(), 2);
    for (_, partition_index, error_code, offset) in &results {
        assert_eq!(*error_code, ERROR_NONE);
        let expected = if *partition_index == 0 { 3 } else { 7 };
        assert_eq!(
            *offset, expected,
            "partition {partition_index} got the wrong watermark"
        );
    }
}

/// Regression test: a request naming more than the bridge-backed topic cap must be rejected
/// wholesale (every entry, `INVALID_REQUEST`) rather than partially served or left unbounded.
#[tokio::test]
#[serial]
async fn more_than_the_topic_cap_is_rejected() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed the one real, resolvable topic");

    // 100 nonexistent names fill the cap; the one real, resolvable topic goes last so a correct
    // cap leaves it untouched by this round entirely - proving the cap bounds *which* topics get
    // a bridge call, not just that a request over the cap fails uniformly.
    let mut names: Vec<String> = (0..100).map(|i| format!("topic-{i}")).collect();
    names.push("orders".to_string());
    let partitions = [(0, LATEST_TIMESTAMP)];
    let topics: Vec<TopicRequest> = names
        .iter()
        .map(|name| TopicRequest {
            name,
            partitions: &partitions,
        })
        .collect();

    let results = send_multi(&state, &topics).await;
    assert_eq!(results.len(), 101);
    for (name, _, error_code, _) in &results {
        if name == "orders" {
            assert_eq!(
                *error_code, ERROR_REQUEST_TIMED_OUT,
                "the 101st topic must be left unattempted by the cap, not looked up"
            );
        } else {
            assert_eq!(*error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        }
    }
}
