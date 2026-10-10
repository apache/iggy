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

//! Wire-level `DeleteTopics` tests against a real `iggy-server` process, through
//! [`delete_topics::handle`] with a connected [`GatewayState`] - same shape as
//! `create_topics_real_bridge_tests.rs`. Exercises the handler's own orchestration (the
//! topic-cap check, per-topic independent deletion, real error codes reaching the wire) and,
//! critically, the deliberate choice this bridge makes not to delete the backing Iggy stream.
//!
//! Requests are hand-built at the v5 flexible wire shape, for the same reason
//! `create_topics_real_bridge_tests.rs` hand-builds its own: this crate builds with
//! `default-features = false, features = ["broker"]`, which gives request `Decodable` and
//! response `Encodable` but not the reverse.

use std::sync::Arc;

use bytes::Bytes;
use iggy::prelude::{Identifier, StreamClient, TopicClient};
use serial_test::serial;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{
    BrokerAdvertise, ERROR_INVALID_REQUEST, ERROR_NONE, ERROR_POLICY_VIOLATION,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState,
};
use iggy_gateway_kafka::protocol::handlers::delete_topics;

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;

use codec::{Decoder, Encoder};
use iggy_server::{TestServer, raw_client};

const REQUEST_VERSION: i16 = 5;
const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

/// Builds a v5 flexible `DeleteTopics` request body for one or more topic names.
fn build_request(names: &[&str]) -> Bytes {
    let mut enc = Encoder::with_capacity(128);
    enc.write_varint((names.len() + 1) as u64);
    for name in names {
        enc.write_compact_nullable_string(Some(name));
    }
    enc.write_i32(5_000); // timeout_ms
    enc.write_empty_tagged_fields();
    enc.freeze()
}

/// Decodes every topic result in a v5 flexible `DeleteTopics` response into `(name, error_code)`,
/// in wire order.
fn decode_all_results(body: Bytes) -> Vec<(Option<String>, i16)> {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let results_plus_one = d.read_varint().expect("responses array count");
    let mut results = Vec::new();
    for _ in 1..results_plus_one {
        let name = d.read_compact_nullable_string().expect("topic name");
        let error_code = d.read_i16().expect("error_code");
        let _error_message = d.read_compact_nullable_string().expect("error_message");
        let _tagged = d.read_varint().expect("topic tagged fields");
        results.push((name, error_code));
    }
    results
}

/// Decodes the first (only) topic result into `(error_code, error_message)`.
fn decode_first_result(body: Bytes) -> (i16, Option<String>) {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let _results_plus_one = d.read_varint().expect("responses array count");
    let _name = d.read_compact_nullable_string().expect("topic name");
    let error_code = d.read_i16().expect("error_code");
    let error_message = d.read_compact_nullable_string().expect("error_message");
    (error_code, error_message)
}

async fn send(state: &GatewayState, names: &[&str]) -> Vec<(Option<String>, i16)> {
    let body = build_request(names);
    let outcome = delete_topics::handle(state, None, REQUEST_VERSION, body).await;
    let resp_body = outcome.expect_response("DeleteTopics request always answers");
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
        false,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
    )
}

#[tokio::test]
#[serial]
async fn delete_topics_deletes_a_real_iggy_topic() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    let Some(bridge) = &state.bridge else {
        panic!("bridge must be connected")
    };
    bridge
        .create_kafka_topic("orders", 3)
        .await
        .expect("create the topic to delete");

    let results = send(&state, &["orders"]).await;
    assert_eq!(results, vec![(Some("orders".to_string()), ERROR_NONE)]);

    let raw = raw_client(&server).await;
    let topics = raw
        .get_topics(&Identifier::named("kafka").expect("valid stream name"))
        .await
        .expect("get_topics call");
    assert!(
        topics.is_empty(),
        "the handler must have actually deleted the topic"
    );
}

/// The design decision this PR made deliberately (not the issue's literal "delete stream +
/// topic" wording): `DeleteTopics` must never delete the backing Iggy stream, since the shared
/// `default_stream` can hold other Kafka topics' data this bridge has no way to distinguish from
/// an abandoned one. Verified here by creating two topics in the same (default) stream, deleting
/// one, and confirming the stream - and the other topic in it - are both still there.
#[tokio::test]
#[serial]
async fn delete_topics_never_deletes_the_backing_stream_even_when_it_becomes_empty() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    let Some(bridge) = &state.bridge else {
        panic!("bridge must be connected")
    };
    bridge
        .create_kafka_topic("orders", 1)
        .await
        .expect("create orders");
    bridge
        .create_kafka_topic("payments", 1)
        .await
        .expect("create payments");

    // Delete both topics sharing the default stream - if this ever deleted the stream once it
    // emptied out, the second delete (or the stream-existence check below) would fail.
    let results = send(&state, &["orders", "payments"]).await;
    assert_eq!(
        results,
        vec![
            (Some("orders".to_string()), ERROR_NONE),
            (Some("payments".to_string()), ERROR_NONE),
        ]
    );

    let raw = raw_client(&server).await;
    let streams = raw.get_streams().await.expect("get_streams call");
    assert_eq!(
        streams.len(),
        1,
        "the now-empty default stream must still exist - DeleteTopics never touches it"
    );
}

#[tokio::test]
#[serial]
async fn delete_topics_on_a_nonexistent_topic_returns_unknown_topic_or_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let results = send(&state, &["never-existed"]).await;
    assert_eq!(
        results,
        vec![(
            Some("never-existed".to_string()),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        )]
    );
}

/// A name repeated in the same request is rejected outright for every occurrence, the same
/// choice `CreateTopics` makes for a repeated topic name (real Kafka's `ControllerApis.deleteTopics`
/// answers `INVALID_REQUEST`(42) "Duplicate topic name" for every occurrence and deletes nothing -
/// a first-occurrence-wins split would let an `AdminClient` caller observe a delete it never got
/// a clean answer for, since the client keys its futures by name and silently discards the second
/// per-name result regardless of which occurrence this bridge picked).
#[tokio::test]
#[serial]
async fn delete_topics_every_occurrence_of_a_duplicate_name_is_rejected() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    let Some(bridge) = &state.bridge else {
        panic!("bridge must be connected")
    };
    bridge
        .create_kafka_topic("orders", 1)
        .await
        .expect("create orders");

    let results = send(&state, &["orders", "orders"]).await;
    assert_eq!(
        results,
        vec![
            (Some("orders".to_string()), ERROR_INVALID_REQUEST),
            (Some("orders".to_string()), ERROR_INVALID_REQUEST),
        ]
    );

    // Neither occurrence deleted anything - the topic must still exist.
    let topic = bridge
        .get_kafka_topic("orders")
        .await
        .expect("lookup should not fail");
    assert!(
        topic.is_some(),
        "a duplicate name must not delete the topic it names"
    );
}

/// One missing topic in a batch must not block an unrelated, existing topic from being deleted.
#[tokio::test]
#[serial]
async fn delete_topics_deletes_existing_topics_alongside_a_missing_one() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    let Some(bridge) = &state.bridge else {
        panic!("bridge must be connected")
    };
    bridge
        .create_kafka_topic("orders", 1)
        .await
        .expect("create orders");

    let results = send(&state, &["orders", "never-existed"]).await;
    assert_eq!(
        results,
        vec![
            (Some("orders".to_string()), ERROR_NONE),
            (
                Some("never-existed".to_string()),
                ERROR_UNKNOWN_TOPIC_OR_PARTITION
            ),
        ]
    );

    let raw = raw_client(&server).await;
    let topics = raw
        .get_topics(&Identifier::named("kafka").expect("valid stream name"))
        .await
        .expect("get_topics call");
    assert!(topics.is_empty(), "orders must have actually been deleted");
}

/// Regression test: a request naming more than the bridge-backed topic cap must be rejected
/// wholesale (every entry, `POLICY_VIOLATION`, nothing deleted) rather than partially served -
/// same contract `create_topics_rejects_more_than_the_topic_cap` holds for `CreateTopics`.
#[tokio::test]
#[serial]
async fn delete_topics_rejects_more_than_the_topic_cap() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    let Some(bridge) = &state.bridge else {
        panic!("bridge must be connected")
    };
    bridge
        .create_kafka_topic("orders", 1)
        .await
        .expect("create orders");

    let names: Vec<String> = (0..101).map(|i| format!("topic-{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let results = send(&state, &name_refs).await;
    assert_eq!(results.len(), 101);
    for (_, error_code) in &results {
        assert_eq!(*error_code, ERROR_POLICY_VIOLATION);
    }

    let raw = raw_client(&server).await;
    let topics = raw
        .get_topics(&Identifier::named("kafka").expect("valid stream name"))
        .await
        .expect("get_topics call");
    assert_eq!(
        topics.len(),
        1,
        "an over-cap request must delete nothing, including topics not named in it"
    );
}

/// `error_message` carries the real cause for a client-visible rejection path (the topic-cap
/// policy violation), not just a bare code.
#[tokio::test]
#[serial]
async fn delete_topics_over_cap_error_message_names_the_limit() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let names: Vec<String> = (0..101).map(|i| format!("topic-{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let body = build_request(&name_refs);
    let outcome = delete_topics::handle(&state, None, REQUEST_VERSION, body).await;
    let resp_body = outcome.expect_response("DeleteTopics request always answers");
    let (error_code, error_message) = decode_first_result(resp_body);
    assert_eq!(error_code, ERROR_POLICY_VIOLATION);
    assert!(
        error_message.is_some_and(|m| m.contains("100")),
        "error_message must name the actual cap"
    );
}
