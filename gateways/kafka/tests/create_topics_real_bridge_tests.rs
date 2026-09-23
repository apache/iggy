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

//! Wire-level `CreateTopics` tests against a real `iggy-server` process, through
//! [`create_topics::handle`] with a connected [`GatewayState`] - not the stub (no-bridge) path
//! `api_handler_tests.rs` covers, and not `IggyBridge` called directly as
//! `bridge_iggy_integration_tests.rs` does. Exercises the handler's own orchestration: the
//! `configs` check, the existence check running before `validate_only` returns, and real error
//! codes reaching the wire.
//!
//! Requests are hand-built at the v5 flexible wire shape (not `kafka_protocol`'s own
//! `Encodable`/`Decodable`): this crate builds with `default-features = false, features =
//! ["broker"]`, which gives request `Decodable` and response `Encodable` (what a broker needs)
//! but not the reverse - the same reason every other wire-level test in this suite hand-builds
//! request bytes and hand-decodes response bytes via `common/codec.rs`.

use std::sync::Arc;

use bytes::Bytes;
use iggy::prelude::{Identifier, StreamClient, TopicClient};
use serial_test::serial;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::protocol::api::{
    BrokerAdvertise, ERROR_INVALID_CONFIG, ERROR_INVALID_PARTITIONS,
    ERROR_INVALID_REPLICA_ASSIGNMENT, ERROR_INVALID_REQUEST, ERROR_INVALID_TOPIC_EXCEPTION,
    ERROR_NONE, ERROR_TOPIC_ALREADY_EXISTS, GatewayState,
};
use iggy_gateway_kafka::protocol::handlers::create_topics;

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;

use codec::{Decoder, Encoder};
use iggy_server::{TestServer, raw_client};

const REQUEST_VERSION: i16 = 5;
const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

/// One requested topic's shape, for [`build_request`]. `assignments` holds partition indices;
/// each gets a single placeholder broker id.
struct TopicSpec<'a> {
    name: &'a str,
    num_partitions: i32,
    replication_factor: i16,
    assignments: &'a [i32],
    has_config: bool,
}

impl<'a> TopicSpec<'a> {
    const fn new(name: &'a str, num_partitions: i32) -> Self {
        Self {
            name,
            num_partitions,
            replication_factor: 1,
            assignments: &[],
            has_config: false,
        }
    }
}

/// Builds a v5 flexible `CreateTopics` request body for one or more topics.
fn build_request(topics: &[TopicSpec], validate_only: bool) -> Bytes {
    let mut enc = Encoder::with_capacity(256);
    enc.write_varint((topics.len() + 1) as u64);
    for topic in topics {
        enc.write_compact_nullable_string(Some(topic.name));
        enc.write_i32(topic.num_partitions);
        enc.write_i16(topic.replication_factor);

        enc.write_varint((topic.assignments.len() + 1) as u64);
        for &partition_index in topic.assignments {
            enc.write_i32(partition_index);
            enc.write_varint(2); // one broker
            enc.write_i32(0); // broker_id
            enc.write_empty_tagged_fields();
        }

        if topic.has_config {
            enc.write_varint(2); // one config
            enc.write_compact_nullable_string(Some("cleanup.policy"));
            enc.write_compact_nullable_string(Some("delete"));
            enc.write_empty_tagged_fields();
        } else {
            enc.write_varint(1); // empty configs
        }

        enc.write_empty_tagged_fields(); // topic tagged fields
    }
    enc.write_i32(5_000); // timeout_ms
    enc.write_bool(validate_only);
    enc.write_empty_tagged_fields();
    enc.freeze()
}

/// Decodes a v5 flexible `CreateTopics` response's first topic result into `(error_code,
/// num_partitions)`. Stops there - fine for tests that only ever send one topic.
fn decode_first_result(body: Bytes) -> (i16, i32) {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let _topics_plus_one = d.read_varint().expect("topics array count");
    let _name = d.read_compact_nullable_string().expect("topic name");
    let error_code = d.read_i16().expect("error_code");
    let _error_message = d.read_compact_nullable_string().expect("error_message");
    // `topic_config_error_code` is not a positional field at v5-7 despite the struct's field
    // list implying it is - the crate's own `Encodable` impl encodes it as a tagged field
    // (present only when non-zero), so `num_partitions` follows `error_message` directly.
    let num_partitions = d.read_i32().expect("num_partitions");
    (error_code, num_partitions)
}

/// Decodes every topic result in a v5 flexible `CreateTopics` response into `(name, error_code)`,
/// in wire order.
fn decode_all_results(body: Bytes) -> Vec<(Option<String>, i16)> {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let topics_plus_one = d.read_varint().expect("topics array count");
    let mut results = Vec::new();
    for _ in 1..topics_plus_one {
        let name = d.read_compact_nullable_string().expect("topic name");
        let error_code = d.read_i16().expect("error_code");
        let _error_message = d.read_compact_nullable_string().expect("error_message");
        let _num_partitions = d.read_i32().expect("num_partitions");
        let _replication_factor = d.read_i16().expect("replication_factor");
        let _configs = d.read_varint().expect("configs array count");
        let _tagged = d.read_varint().expect("topic tagged fields");
        results.push((name, error_code));
    }
    results
}

async fn send(state: &GatewayState, topics: &[TopicSpec<'_>], validate_only: bool) -> (i16, i32) {
    let body = build_request(topics, validate_only);
    let outcome = create_topics::handle(state, REQUEST_VERSION, body).await;
    let resp_body = outcome.expect_response("CreateTopics request always answers");
    decode_first_result(resp_body)
}

async fn send_all(
    state: &GatewayState,
    topics: &[TopicSpec<'_>],
    validate_only: bool,
) -> Vec<(Option<String>, i16)> {
    let body = build_request(topics, validate_only);
    let outcome = create_topics::handle(state, REQUEST_VERSION, body).await;
    let resp_body = outcome.expect_response("CreateTopics request always answers");
    decode_all_results(resp_body)
}

async fn connected_state(server: &TestServer) -> GatewayState {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    GatewayState::new(
        BrokerAdvertise::default(),
        Some(Arc::new(bridge)),
        TEST_MAX_FRAME_SIZE,
    )
}

#[tokio::test]
#[serial]
async fn create_topics_creates_a_real_iggy_topic() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let (error_code, num_partitions) = send(&state, &[TopicSpec::new("orders", 3)], false).await;
    assert_eq!(error_code, ERROR_NONE);
    assert_eq!(num_partitions, 3);

    let raw = raw_client(&server).await;
    let topics = raw
        .get_topics(&Identifier::named("kafka").expect("valid stream name"))
        .await
        .expect("get_topics call");
    assert_eq!(topics.len(), 1, "the handler must have actually created it");
    assert_eq!(topics[0].name, "orders");
    assert_eq!(topics[0].partitions_count, 3);
}

/// Regression test: before the existence check landed, re-`CreateTopics`-ing the same topic went
/// straight to `ensure_stream_and_topic`, whose idempotent-success contract made a same-spec
/// recreate answer `ERROR_NONE` - wrong for `CreateTopics` itself, whose own contract is that a
/// second create of the same topic is `TOPIC_ALREADY_EXISTS`.
#[tokio::test]
#[serial]
async fn create_topics_recreating_the_same_topic_returns_topic_already_exists() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let (first_code, _) = send(&state, &[TopicSpec::new("orders", 3)], false).await;
    assert_eq!(first_code, ERROR_NONE);

    let (second_code, _) = send(&state, &[TopicSpec::new("orders", 3)], false).await;
    assert_eq!(second_code, ERROR_TOPIC_ALREADY_EXISTS);
}

/// The existence check must run before `validate_only` returns - `validate_only` against a topic
/// that already exists must still answer `TOPIC_ALREADY_EXISTS`, not a false `ERROR_NONE`.
#[tokio::test]
#[serial]
async fn create_topics_validate_only_against_an_existing_topic_returns_topic_already_exists() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let (created_code, _) = send(&state, &[TopicSpec::new("orders", 3)], false).await;
    assert_eq!(created_code, ERROR_NONE);

    let (validated_code, _) = send(&state, &[TopicSpec::new("orders", 3)], true).await;
    assert_eq!(validated_code, ERROR_TOPIC_ALREADY_EXISTS);
}

/// `validate_only` against a topic that does not yet exist must succeed without creating it.
#[tokio::test]
#[serial]
async fn create_topics_validate_only_against_a_new_topic_succeeds_without_creating_it() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let (error_code, num_partitions) = send(&state, &[TopicSpec::new("orders", 3)], true).await;
    assert_eq!(error_code, ERROR_NONE);
    assert_eq!(num_partitions, 3);

    let raw = raw_client(&server).await;
    let streams = raw.get_streams().await.expect("get_streams call");
    assert!(
        streams.is_empty(),
        "validate_only must not create the stream or topic"
    );
}

/// Regression test: `validate_only` must still run real name validation, not just report success
/// for anything shaped correctly on the wire. `get_kafka_topic` (the existence check
/// `validate_only` takes) validates the Kafka topic name first, but that path is easy to lose if
/// this handler ever stops routing through it.
#[tokio::test]
#[serial]
async fn create_topics_validate_only_rejects_an_illegal_topic_name() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let (error_code, _) = send(&state, &[TopicSpec::new(" orders ", 3)], true).await;
    assert_eq!(error_code, ERROR_INVALID_TOPIC_EXCEPTION);

    let raw = raw_client(&server).await;
    let streams = raw.get_streams().await.expect("get_streams call");
    assert!(streams.is_empty());
}

#[tokio::test]
#[serial]
async fn create_topics_zero_partitions_returns_invalid_partitions_and_creates_nothing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let (error_code, _) = send(&state, &[TopicSpec::new("orders", 0)], false).await;
    assert_eq!(error_code, ERROR_INVALID_PARTITIONS);

    let raw = raw_client(&server).await;
    let streams = raw.get_streams().await.expect("get_streams call");
    assert!(streams.is_empty());
}

#[tokio::test]
#[serial]
async fn create_topics_with_a_per_topic_config_returns_invalid_config_and_creates_nothing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let topic = TopicSpec {
        has_config: true,
        ..TopicSpec::new("orders", 3)
    };
    let (error_code, _) = send(&state, &[topic], false).await;
    assert_eq!(error_code, ERROR_INVALID_CONFIG);

    let raw = raw_client(&server).await;
    let streams = raw.get_streams().await.expect("get_streams call");
    assert!(streams.is_empty());
}

/// A manual replica assignment resolves the created partition count from its own length, not
/// from `num_partitions` (`-1` here, per KIP-464).
#[tokio::test]
#[serial]
async fn create_topics_resolves_partition_count_from_a_manual_assignment() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let topic = TopicSpec {
        replication_factor: -1,
        assignments: &[0, 1],
        ..TopicSpec::new("orders", -1)
    };
    let (error_code, num_partitions) = send(&state, &[topic], false).await;
    assert_eq!(error_code, ERROR_NONE);
    assert_eq!(num_partitions, 2);

    let raw = raw_client(&server).await;
    let topics = raw
        .get_topics(&Identifier::named("kafka").expect("valid stream name"))
        .await
        .expect("get_topics call");
    assert_eq!(topics[0].partitions_count, 2);
}

/// Regression test: real Kafka rejects an explicit `num_partitions` alongside a manual
/// assignment outright - a manual assignment requires `num_partitions == -1`. Agreeing with
/// `assignments.len()` does not make the combination valid, and this must not silently succeed.
#[tokio::test]
#[serial]
async fn create_topics_rejects_an_explicit_partition_count_alongside_a_manual_assignment() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let topic = TopicSpec {
        replication_factor: -1,
        assignments: &[0, 1],
        ..TopicSpec::new("orders", 2) // agrees with assignments.len(), still invalid
    };
    let (error_code, _) = send(&state, &[topic], false).await;
    assert_eq!(error_code, ERROR_INVALID_REQUEST);

    let raw = raw_client(&server).await;
    let streams = raw.get_streams().await.expect("get_streams call");
    assert!(streams.is_empty());
}

/// Regression test: real Kafka's `ReplicationControlManager` requires a manual assignment's
/// partition indices to be exactly `0..assignments.len()`, each appearing once - a topic whose
/// assignment keys are `{5, 7}` has the right *length* for a 2-partition topic but names neither
/// partition `0` nor `1`. Checking only `assignments.len()` (the pre-fix behavior) would silently
/// create a 2-partition topic whose real partitions are `0`/`1`, disagreeing with what the client
/// asked for.
#[tokio::test]
#[serial]
async fn create_topics_rejects_a_manual_assignment_with_non_consecutive_indices() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let topic = TopicSpec {
        replication_factor: -1,
        assignments: &[5, 7],
        ..TopicSpec::new("orders", -1)
    };
    let (error_code, _) = send(&state, &[topic], false).await;
    assert_eq!(error_code, ERROR_INVALID_REPLICA_ASSIGNMENT);

    let raw = raw_client(&server).await;
    let streams = raw.get_streams().await.expect("get_streams call");
    assert!(streams.is_empty());
}

/// Regression test: a manual assignment repeating one partition index (`{0, 0}`) is the other
/// half of the same real-Kafka rule - same index set size as a valid 2-partition assignment, but
/// not the distinct `0..2` it requires.
#[tokio::test]
#[serial]
async fn create_topics_rejects_a_manual_assignment_with_a_duplicate_index() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let topic = TopicSpec {
        replication_factor: -1,
        assignments: &[0, 0],
        ..TopicSpec::new("orders", -1)
    };
    let (error_code, _) = send(&state, &[topic], false).await;
    assert_eq!(error_code, ERROR_INVALID_REPLICA_ASSIGNMENT);
}

/// Regression test: `error_message` must not re-embed the topic name `CreatableTopicResult.name`
/// already carries. `BridgeError::InvalidKafkaTopicName`'s `Display` embeds the full (invalid)
/// name in its text; validation runs before any bridge I/O, so an unfixed double-echo here costs
/// nothing to trigger and roughly doubles the response per invalid name for free.
#[tokio::test]
#[serial]
async fn create_topics_error_message_does_not_repeat_the_topic_name() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let bad_name = "has a space";
    let topic = TopicSpec::new(bad_name, 1);
    let body = build_request(&[topic], false);
    let outcome = create_topics::handle(&state, REQUEST_VERSION, body).await;
    let resp_body = outcome.expect_response("CreateTopics request always answers");

    let mut d = Decoder::new(resp_body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let _topics_plus_one = d.read_varint().expect("topics array count");
    let _name = d.read_compact_nullable_string().expect("topic name");
    let error_code = d.read_i16().expect("error_code");
    let error_message = d
        .read_compact_nullable_string()
        .expect("error_message")
        .unwrap_or_default();

    assert_eq!(error_code, ERROR_INVALID_TOPIC_EXCEPTION);
    assert!(
        !error_message.contains(bad_name),
        "error_message repeats the topic name `.name` already carries: {error_message:?}"
    );
}

/// Regression test: real Kafka refuses every occurrence of a duplicate topic name in one request
/// with `INVALID_REQUEST` (42) and creates nothing - not a first-wins create plus a
/// `TOPIC_ALREADY_EXISTS` for the rest, which would let a client observe a create it never got a
/// `NONE` result for (`AdminClient` keys its futures by name and drops one of the two results).
#[tokio::test]
#[serial]
async fn create_topics_rejects_every_occurrence_of_a_duplicate_topic_name() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let topics = [TopicSpec::new("orders", 3), TopicSpec::new("orders", 3)];
    let results = send_all(&state, &topics, false).await;
    assert_eq!(results.len(), 2);
    for (name, error_code) in &results {
        assert_eq!(name.as_deref(), Some("orders"));
        assert_eq!(*error_code, ERROR_INVALID_REQUEST);
    }

    let raw = raw_client(&server).await;
    let streams = raw.get_streams().await.expect("get_streams call");
    assert!(streams.is_empty(), "neither occurrence must be created");
}

/// A duplicate name in the request must not block an unrelated, uniquely-named topic in the
/// same batch from being created normally.
#[tokio::test]
#[serial]
async fn create_topics_still_creates_unrelated_topics_alongside_a_duplicate() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let topics = [
        TopicSpec::new("orders", 3),
        TopicSpec::new("orders", 3),
        TopicSpec::new("payments", 1),
    ];
    let results = send_all(&state, &topics, false).await;
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1, ERROR_INVALID_REQUEST);
    assert_eq!(results[1].1, ERROR_INVALID_REQUEST);
    assert_eq!(results[2], (Some("payments".to_string()), ERROR_NONE));

    let raw = raw_client(&server).await;
    let topics = raw
        .get_topics(&Identifier::named("kafka").expect("valid stream name"))
        .await
        .expect("get_topics call");
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "payments");
}

/// Regression test: a request naming more than the bridge-backed topic cap must be rejected
/// wholesale (every entry, `INVALID_REQUEST`, nothing created) rather than partially served.
#[tokio::test]
#[serial]
async fn create_topics_rejects_more_than_the_topic_cap() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let names: Vec<String> = (0..101).map(|i| format!("topic-{i}")).collect();
    let topics: Vec<TopicSpec> = names.iter().map(|name| TopicSpec::new(name, 1)).collect();

    let results = send_all(&state, &topics, false).await;
    assert_eq!(results.len(), 101);
    for (_, error_code) in &results {
        assert_eq!(*error_code, ERROR_INVALID_REQUEST);
    }

    let raw = raw_client(&server).await;
    let streams = raw.get_streams().await.expect("get_streams call");
    assert!(
        streams.is_empty(),
        "an over-cap request must create nothing"
    );
}
