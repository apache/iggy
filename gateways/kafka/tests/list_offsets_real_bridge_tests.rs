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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use iggy::prelude::{Identifier, IggyMessage, MessageClient, Partitioning};
use serial_test::serial;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{
    ERROR_NONE, ERROR_REQUEST_TIMED_OUT, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState,
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
const MAX_TIMESTAMP: i64 = -3;
/// The first version that may ask for [`MAX_TIMESTAMP`]. Its request and response share v6's shape.
const MAX_TIMESTAMP_VERSION: i16 = 7;

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
    let (error_code, _timestamp, offset) = decode_first_entry(body);
    (error_code, offset)
}

/// As [`decode_first_result`], keeping the timestamp: `(error_code, timestamp, offset)`.
fn decode_first_entry(body: Bytes) -> (i16, i64, i64) {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let _topics_plus_one = d.read_varint().expect("topics array count");
    let _name = d.read_compact_nullable_string().expect("topic name");
    let _partitions_plus_one = d.read_varint().expect("partitions array count");
    let _partition_index = d.read_i32().expect("partition_index");
    let error_code = d.read_i16().expect("error_code");
    let timestamp = d.read_i64().expect("timestamp");
    let offset = d.read_i64().expect("offset");
    (error_code, timestamp, offset)
}

/// One lookup at `version`, answered as `(error_code, timestamp, offset)`.
async fn send_at(
    state: &GatewayState,
    version: i16,
    topic: &str,
    partition_index: i32,
    timestamp: i64,
) -> (i16, i64, i64) {
    let body = build_request(topic, partition_index, timestamp);
    let outcome = list_offsets::handle(state, version, body).await;
    decode_first_entry(outcome.expect_response("ListOffsets request always answers"))
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
        Some(Arc::new(bridge)),
        TEST_MAX_FRAME_SIZE,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
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

/// Milliseconds since the epoch, the unit Kafka timestamps use.
fn now_millis() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past the epoch");
    i64::try_from(elapsed.as_millis()).expect("now fits an i64")
}

/// Sends `values` to partition 0 of `orders` in one batch, so they share one append time.
async fn produce_batch(server: &TestServer, values: &[&str]) {
    produce_batch_to(server, 0, values).await;
}

/// As [`produce_batch`], to `partition`.
async fn produce_batch_to(server: &TestServer, partition: u32, values: &[&str]) {
    let raw = iggy_server::raw_client(server).await;
    let mut messages: Vec<IggyMessage> = values
        .iter()
        .map(|value| IggyMessage::from(*value))
        .collect();
    raw.send_messages(
        &Identifier::named("kafka").expect("valid stream name"),
        &Identifier::named("orders").expect("valid topic name"),
        &Partitioning::partition_id(partition),
        &mut messages,
    )
    .await
    .expect("send a batch");
}

/// Two batches with a measured gap between their append times: offsets 0-1, then 2-4. Returns a
/// time inside the gap.
async fn two_batches_with_a_gap(server: &TestServer) -> i64 {
    produce_batch(server, &["a", "b"]).await;
    // Iggy stamps append time in microseconds and the lookup takes milliseconds, so the gap has to
    // be wide enough that a millisecond inside it is after the first batch and before the second.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let between = now_millis();
    tokio::time::sleep(Duration::from_millis(50)).await;
    produce_batch(server, &["c", "d", "e"]).await;
    between
}

#[tokio::test]
#[serial]
async fn a_timestamp_resolves_to_the_first_record_appended_at_or_after_it() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed the topic");
    let before = now_millis() - 60_000;
    let between = two_batches_with_a_gap(&server).await;

    let (error_code, timestamp, offset) =
        send_at(&state, REQUEST_VERSION, "orders", 0, between).await;
    assert_eq!(error_code, ERROR_NONE);
    assert_eq!(offset, 2, "the second batch starts at offset 2");
    assert!(
        timestamp >= between,
        "the record found was appended after the time asked for, got {timestamp} for {between}"
    );

    let (error_code, _, offset) = send_at(&state, REQUEST_VERSION, "orders", 0, before).await;
    assert_eq!(error_code, ERROR_NONE);
    assert_eq!(offset, 0, "a time before every record finds the first");

    // AKHQ asks for time 0 to find where each partition starts.
    let (error_code, _, offset) = send_at(&state, REQUEST_VERSION, "orders", 0, 0).await;
    assert_eq!(error_code, ERROR_NONE);
    assert_eq!(offset, 0);
}

#[tokio::test]
#[serial]
async fn a_timestamp_after_every_record_is_minus_one_without_an_error() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed the topic");
    produce_batch(&server, &["a", "b"]).await;

    let later = now_millis() + 3_600_000;
    let (error_code, timestamp, offset) =
        send_at(&state, REQUEST_VERSION, "orders", 0, later).await;
    assert_eq!(
        (error_code, timestamp, offset),
        (ERROR_NONE, -1, -1),
        "Kafka answers a time past the last record with no error and -1"
    );
}

#[tokio::test]
#[serial]
async fn a_timestamp_on_an_empty_partition_is_minus_one_without_an_error() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed the topic");

    for (version, requested) in [(REQUEST_VERSION, 0), (MAX_TIMESTAMP_VERSION, MAX_TIMESTAMP)] {
        let (error_code, timestamp, offset) =
            send_at(&state, version, "orders", 0, requested).await;
        assert_eq!((error_code, timestamp, offset), (ERROR_NONE, -1, -1));
    }
}

#[tokio::test]
#[serial]
async fn max_timestamp_resolves_to_the_last_record_at_any_version() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed the topic");
    let between = two_batches_with_a_gap(&server).await;

    for version in [REQUEST_VERSION, MAX_TIMESTAMP_VERSION] {
        let (error_code, timestamp, offset) =
            send_at(&state, version, "orders", 0, MAX_TIMESTAMP).await;
        assert_eq!(error_code, ERROR_NONE, "v{version}");
        assert_eq!(offset, 4, "v{version}: the last of five records");
        assert!(
            timestamp >= between,
            "v{version}: got {timestamp}, the last batch came after {between}"
        );
    }
}

/// A TCP relay to `server` that leaves the next connections [`Self::stall_next`] counts
/// unanswered, so a client on one waits in its handshake until it gives up.
struct StallingRelay {
    address: String,
    stall: Arc<AtomicUsize>,
}

impl StallingRelay {
    async fn start(server: &TestServer) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the relay");
        let address = listener.local_addr().expect("relay address").to_string();
        let upstream = server.test_config().address;
        let stall = Arc::new(AtomicUsize::new(0));
        let stall_count = Arc::clone(&stall);
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                if stall_count
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                        left.checked_sub(1)
                    })
                    .is_ok()
                {
                    tokio::spawn(async move {
                        std::future::pending::<()>().await;
                        drop(inbound);
                    });
                    continue;
                }
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(&upstream).await {
                        let _ = copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
        Self { address, stall }
    }

    fn stall_next(&self, connections: usize) {
        self.stall.store(connections, Ordering::SeqCst);
    }
}

/// The first two lookups' read clients connect through a relay that never answers. Each lookup's
/// own budget ends it, and the lookup after them still answers. With only the request deadline
/// bounding them, the second stall would run the request out and the last lookup would answer 7.
#[tokio::test]
#[serial]
async fn a_stalled_lookup_leaves_the_later_partitions_of_the_request_their_answers() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let relay = StallingRelay::start(&server).await;
    let mut config = server.test_config();
    config.address.clone_from(&relay.address);
    let bridge = IggyBridge::connect(config)
        .await
        .expect("bridge should connect through the relay");
    let state = GatewayState::new(
        Some(Arc::new(bridge)),
        TEST_MAX_FRAME_SIZE,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
    );
    let seed = IggyBridge::connect(server.test_config())
        .await
        .expect("seed bridge should connect to a ready server");
    seed.ensure_stream_and_topic("orders", 3)
        .await
        .expect("seed a 3-partition topic");
    for partition in 0..3 {
        produce_batch_to(&server, partition, &["a", "b"]).await;
    }
    // Connects the probe client now, so the stalled connection is the first read slot's.
    let (error_code, _) = send(&state, "orders", 0, LATEST_TIMESTAMP).await;
    assert_eq!(error_code, ERROR_NONE);

    relay.stall_next(2);
    let topics = [TopicRequest {
        name: "orders",
        partitions: &[(0, 0), (1, 0), (2, 0)],
    }];
    let answers: Vec<(i32, i16, i64)> = send_multi(&state, &topics)
        .await
        .iter()
        .map(|(_, partition, error_code, offset)| (*partition, *error_code, *offset))
        .collect();
    assert_eq!(
        answers,
        vec![
            (0, ERROR_REQUEST_TIMED_OUT, -1),
            (1, ERROR_REQUEST_TIMED_OUT, -1),
            (2, ERROR_NONE, 0),
        ]
    );
}

/// Every lookup by time in one request shares one read slot, handed back after each lookup. If
/// it were not, every partition after the first would answer 7.
#[tokio::test]
#[serial]
async fn several_partitions_looked_up_by_time_in_one_request_each_resolve() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 2)
        .await
        .expect("seed a 2-partition topic");
    produce_batch_to(&server, 0, &["a", "b"]).await;
    produce_batch_to(&server, 1, &["x"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let between = now_millis();
    tokio::time::sleep(Duration::from_millis(50)).await;
    produce_batch_to(&server, 0, &["c"]).await;
    produce_batch_to(&server, 1, &["y", "z"]).await;

    let topics = [TopicRequest {
        name: "orders",
        partitions: &[
            (0, between),
            (1, between),
            (1, 0),
            (0, LATEST_TIMESTAMP),
            (0, between),
        ],
    }];
    let results = send_multi(&state, &topics).await;
    let answers: Vec<(i32, i16, i64)> = results
        .iter()
        .map(|(_, partition, error_code, offset)| (*partition, *error_code, *offset))
        .collect();
    assert_eq!(
        answers,
        vec![
            (0, ERROR_NONE, 2),
            (1, ERROR_NONE, 1),
            (1, ERROR_NONE, 0),
            (0, ERROR_NONE, 3),
            (0, ERROR_NONE, 2),
        ],
        "each lookup by time resolves on its own partition, in request order, and a repeated \
         entry gets the same answer"
    );
}

#[tokio::test]
#[serial]
async fn a_timestamp_on_a_missing_partition_or_topic_is_unknown() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 1)
        .await
        .expect("seed the topic");

    let (error_code, _, _) = send_at(&state, REQUEST_VERSION, "orders", 5, 0).await;
    assert_eq!(error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    let (error_code, _, _) = send_at(&state, REQUEST_VERSION, "missing", 0, 0).await;
    assert_eq!(error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
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
