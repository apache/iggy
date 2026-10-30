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

//! Wire-level `CreatePartitions` tests against a real `iggy-server` process, through
//! [`create_partitions::handle`] with a connected [`GatewayState`] - same shape as
//! `delete_topics_real_bridge_tests.rs`. Exercises the handler's own orchestration (the grow-only
//! rule, `validate_only`, the manual assignment checks, the topic cap, per-topic independence,
//! the SASL gate, the lock that keeps two overlapping grows from both adding) and that a grow
//! reaches Iggy and Metadata's own topic listing.
//!
//! Requests are hand-built, mostly at the v3 flexible wire shape, for the same reason
//! `create_topics_real_bridge_tests.rs` hand-builds its own: this crate builds with
//! `default-features = false, features = ["broker"]`, which gives request `Decodable` and
//! response `Encodable` but not the reverse.

use std::sync::Arc;

use bytes::Bytes;
use iggy::prelude::{Identifier, PartitionResizePolicy, TopicClient, TopicCreateOptions};
use serial_test::serial;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::auth::AuthenticatedPrincipal;
use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::acl::PrincipalPermissions;
use iggy_gateway_kafka::protocol::api::{
    ERROR_INVALID_PARTITIONS, ERROR_INVALID_REPLICA_ASSIGNMENT, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_POLICY_VIOLATION, ERROR_TOPIC_AUTHORIZATION_FAILED, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
    GatewayState,
};
use iggy_gateway_kafka::protocol::handlers::create_partitions;

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;

use codec::{Decoder, Encoder};
use iggy_server::{TestServer, raw_client};

const REQUEST_VERSION: i16 = 3;
const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;
const STREAM: &str = "kafka";

/// One requested topic's shape, for [`build_request`]. `assignments` is `None` for the null
/// list every client sends unless the caller named replicas; each present entry is one new
/// partition's replica list.
struct TopicSpec<'a> {
    name: &'a str,
    count: i32,
    assignments: Option<&'a [&'a [i32]]>,
}

impl<'a> TopicSpec<'a> {
    const fn new(name: &'a str, count: i32) -> Self {
        Self {
            name,
            count,
            assignments: None,
        }
    }
}

/// Builds a v3 flexible `CreatePartitions` request body for one or more topics.
fn build_request(topics: &[TopicSpec], validate_only: bool) -> Bytes {
    let mut enc = Encoder::with_capacity(256);
    enc.write_varint((topics.len() + 1) as u64);
    for topic in topics {
        enc.write_compact_nullable_string(Some(topic.name));
        enc.write_i32(topic.count);
        match topic.assignments {
            None => enc.write_varint(0),
            Some(assignments) => {
                enc.write_varint((assignments.len() + 1) as u64);
                for broker_ids in assignments {
                    enc.write_varint((broker_ids.len() + 1) as u64);
                    for &broker_id in *broker_ids {
                        enc.write_i32(broker_id);
                    }
                    enc.write_empty_tagged_fields();
                }
            }
        }
        enc.write_empty_tagged_fields(); // topic tagged fields
    }
    enc.write_i32(5_000); // timeout_ms
    enc.write_bool(validate_only);
    enc.write_empty_tagged_fields();
    enc.freeze()
}

/// Decodes every result in a v3 flexible `CreatePartitions` response into `(name, error_code,
/// error_message)`, in wire order.
fn decode_results(body: Bytes) -> Vec<(String, i16, Option<String>)> {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let results_plus_one = d.read_varint().expect("results array count");
    let mut results = Vec::new();
    for _ in 1..results_plus_one {
        let name = d
            .read_compact_nullable_string()
            .expect("topic name")
            .expect("topic name is not null");
        let error_code = d.read_i16().expect("error_code");
        let error_message = d.read_compact_nullable_string().expect("error_message");
        let _tagged = d.read_varint().expect("result tagged fields");
        results.push((name, error_code, error_message));
    }
    results
}

async fn send(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    topics: &[TopicSpec<'_>],
    validate_only: bool,
) -> Vec<(String, i16, Option<String>)> {
    let body = build_request(topics, validate_only);
    let outcome = create_partitions::handle(state, principal, REQUEST_VERSION, body).await;
    decode_results(outcome.expect_response("CreatePartitions request always answers"))
}

/// Grows `topics` without a principal, the SASL-off path, and returns `(name, error_code)`.
async fn grow(state: &GatewayState, topics: &[TopicSpec<'_>]) -> Vec<(String, i16)> {
    send(state, None, topics, false)
        .await
        .into_iter()
        .map(|(name, error_code, _)| (name, error_code))
        .collect()
}

async fn connected_state(server: &TestServer) -> GatewayState {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    GatewayState::new(
        Some(Arc::new(bridge)),
        TEST_MAX_FRAME_SIZE,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
    )
}

fn bridge(state: &GatewayState) -> &IggyBridge {
    state.bridge.as_deref().expect("bridge must be connected")
}

async fn create_topic(state: &GatewayState, name: &str, partitions: u32) {
    bridge(state)
        .create_kafka_topic(name, partitions)
        .await
        .expect("create the topic to grow");
}

/// The partition ids of `name` as Iggy itself reports them, read with a client of the test's
/// own rather than through the bridge under test.
async fn iggy_partition_ids(server: &TestServer, name: &str) -> Vec<u32> {
    let raw = raw_client(server).await;
    let topic = raw
        .get_topic(
            &Identifier::named(STREAM).expect("valid stream name"),
            &Identifier::named(name).expect("valid topic name"),
        )
        .await
        .expect("get_topic call")
        .expect("the topic exists");
    topic
        .partitions
        .iter()
        .map(|partition| partition.id)
        .collect()
}

fn principal(manage_topics: bool) -> AuthenticatedPrincipal {
    AuthenticatedPrincipal {
        username: "alice".to_string(),
        permissions: PrincipalPermissions {
            manage_topics,
            ..PrincipalPermissions::default()
        },
        permissions_known: true,
    }
}

#[tokio::test]
#[serial]
async fn create_partitions_grows_a_real_iggy_topic_by_appending_indices() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 1).await;

    let results = grow(&state, &[TopicSpec::new("orders", 3)]).await;
    assert_eq!(results, vec![("orders".to_string(), ERROR_NONE)]);

    // The new partitions follow the existing one, so partition 0 still names the same Iggy
    // partition and the Kafka index passes through unchanged after the grow.
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0, 1, 2]);
}

/// Metadata's "all topics" listing reads the same partition count a Kafka client sees, so a grow
/// must show there without any cache in between.
#[tokio::test]
#[serial]
async fn create_partitions_is_visible_through_metadatas_own_topic_listing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 2).await;

    let results = grow(&state, &[TopicSpec::new("orders", 5)]).await;
    assert_eq!(results, vec![("orders".to_string(), ERROR_NONE)]);

    let listed = bridge(&state)
        .list_kafka_topics()
        .await
        .expect("list the topics as Metadata does");
    let orders = listed
        .iter()
        .find(|topic| topic.kafka_topic == "orders")
        .expect("the grown topic is listed");
    assert_eq!(orders.partitions_count, 5);
}

#[tokio::test]
#[serial]
async fn create_partitions_with_an_equal_count_answers_invalid_partitions_and_changes_nothing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 3).await;

    let results = send(&state, None, &[TopicSpec::new("orders", 3)], false).await;
    assert_eq!(results.len(), 1);
    let (name, error_code, error_message) = &results[0];
    assert_eq!(name, "orders");
    assert_eq!(*error_code, ERROR_INVALID_PARTITIONS);
    assert_eq!(
        error_message.as_deref(),
        Some("Topic already has 3 partition(s).")
    );
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0, 1, 2]);
}

/// A smaller count answers `INVALID_PARTITIONS` (37).
#[tokio::test]
#[serial]
async fn create_partitions_with_a_smaller_count_answers_invalid_partitions_and_changes_nothing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 3).await;

    for count in [2, 0, -1] {
        let results = send(&state, None, &[TopicSpec::new("orders", count)], false).await;
        let (_, error_code, error_message) = &results[0];
        assert_eq!(*error_code, ERROR_INVALID_PARTITIONS, "count {count}");
        assert!(
            error_message
                .as_deref()
                .is_some_and(|message| message.contains("would not be an increase")),
            "count {count}: {error_message:?}"
        );
    }
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0, 1, 2]);
}

#[tokio::test]
#[serial]
async fn create_partitions_on_a_nonexistent_topic_answers_unknown_topic_or_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;

    let results = grow(&state, &[TopicSpec::new("never-existed", 2)]).await;
    assert_eq!(
        results,
        vec![(
            "never-existed".to_string(),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        )]
    );
}

/// `validate_only` runs every check and writes nothing: a valid request answers `NONE` and
/// leaves the count alone, and an invalid one answers what the real request would.
#[tokio::test]
#[serial]
async fn create_partitions_validate_only_answers_the_real_verdict_and_writes_nothing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 1).await;

    let valid = send(&state, None, &[TopicSpec::new("orders", 3)], true).await;
    assert_eq!(valid[0].1, ERROR_NONE);
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0]);

    let not_an_increase = send(&state, None, &[TopicSpec::new("orders", 1)], true).await;
    assert_eq!(not_an_increase[0].1, ERROR_INVALID_PARTITIONS);

    let missing = send(&state, None, &[TopicSpec::new("never-existed", 3)], true).await;
    assert_eq!(missing[0].1, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
}

/// A name repeated in the same request grows nothing. Like Kafka's `ControllerApis`, it gets one
/// `INVALID_REQUEST` (42) result with Kafka's text, however often it repeats, and the other
/// names in the request are still grown.
#[tokio::test]
#[serial]
async fn create_partitions_a_duplicate_name_gets_one_result_and_grows_nothing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 1).await;

    create_topic(&state, "payments", 1).await;

    let results = send(
        &state,
        None,
        &[
            TopicSpec::new("orders", 2),
            TopicSpec::new("payments", 2),
            TopicSpec::new("orders", 3),
        ],
        false,
    )
    .await;
    assert_eq!(
        results,
        vec![
            (
                "orders".to_string(),
                ERROR_INVALID_REQUEST,
                Some("Duplicate topic name.".to_string())
            ),
            ("payments".to_string(), ERROR_NONE, None),
        ]
    );
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0]);
    assert_eq!(iggy_partition_ids(&server, "payments").await, vec![0, 1]);
}

/// One missing topic in a batch must not block an unrelated, existing topic from growing.
#[tokio::test]
#[serial]
async fn create_partitions_grows_existing_topics_alongside_a_missing_one() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 1).await;

    let results = grow(
        &state,
        &[
            TopicSpec::new("never-existed", 2),
            TopicSpec::new("orders", 2),
        ],
    )
    .await;
    assert_eq!(
        results,
        vec![
            (
                "never-existed".to_string(),
                ERROR_UNKNOWN_TOPIC_OR_PARTITION
            ),
            ("orders".to_string(), ERROR_NONE),
        ]
    );
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0, 1]);
}

/// A manual assignment must hold one entry per new partition, each naming this gateway's single
/// broker exactly once. Anything else is `INVALID_REPLICA_ASSIGNMENT` (39) and grows nothing.
#[tokio::test]
#[serial]
async fn create_partitions_checks_a_manual_assignment_against_the_single_broker() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 1).await;

    let wrong_length: &[&[i32]] = &[&[1]];
    let other_broker: &[&[i32]] = &[&[1], &[2]];
    let empty_replicas: &[&[i32]] = &[&[1], &[]];
    for assignments in [wrong_length, other_broker, empty_replicas] {
        let topic = TopicSpec {
            name: "orders",
            count: 3,
            assignments: Some(assignments),
        };
        let results = grow(&state, &[topic]).await;
        assert_eq!(
            results,
            vec![("orders".to_string(), ERROR_INVALID_REPLICA_ASSIGNMENT)],
            "{assignments:?}"
        );
    }
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0]);

    let matching: &[&[i32]] = &[&[1], &[1]];
    let topic = TopicSpec {
        name: "orders",
        count: 3,
        assignments: Some(matching),
    };
    let results = grow(&state, &[topic]).await;
    assert_eq!(results, vec![("orders".to_string(), ERROR_NONE)]);
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0, 1, 2]);
}

/// Iggy adds at most 1000 partitions in one call. The handler answers the same `INVALID_REQUEST`
/// (42) the bridge would get back, before it calls Iggy, so `validate_only` sees it too.
#[tokio::test]
#[serial]
async fn create_partitions_adding_more_than_the_per_request_cap_answers_invalid_request() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 1).await;

    for validate_only in [true, false] {
        let results = send(
            &state,
            None,
            &[TopicSpec::new("orders", 1002)],
            validate_only,
        )
        .await;
        let (_, error_code, error_message) = &results[0];
        assert_eq!(
            *error_code, ERROR_INVALID_REQUEST,
            "validate_only {validate_only}"
        );
        assert!(
            error_message
                .as_deref()
                .is_some_and(|message| message.contains("1000")),
            "{error_message:?}"
        );
    }
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0]);
}

/// A request naming more than the topic cap is rejected wholesale (every entry,
/// `POLICY_VIOLATION`, nothing grown), the same contract `CreateTopics` and `DeleteTopics` hold.
#[tokio::test]
#[serial]
async fn create_partitions_rejects_more_than_the_topic_cap() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "topic-0", 1).await;

    let names: Vec<String> = (0..101).map(|i| format!("topic-{i}")).collect();
    let topics: Vec<TopicSpec> = names
        .iter()
        .map(|name| TopicSpec::new(name.as_str(), 2))
        .collect();
    let results = send(&state, None, &topics, false).await;
    assert_eq!(results.len(), 101);
    for (_, error_code, error_message) in &results {
        assert_eq!(*error_code, ERROR_POLICY_VIOLATION);
        assert!(
            error_message
                .as_deref()
                .is_some_and(|message| message.contains("100")),
            "error_message must name the actual cap, got {error_message:?}"
        );
    }
    assert_eq!(iggy_partition_ids(&server, "topic-0").await, vec![0]);
}

/// With SASL on, a principal without `manage_topics` is refused every topic with
/// `TOPIC_AUTHORIZATION_FAILED` (29) and nothing is written. One with it grows the topic.
#[tokio::test]
#[serial]
async fn create_partitions_requires_manage_topics_from_an_authenticated_principal() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 1).await;

    let denied = principal(false);
    let results = send(
        &state,
        Some(&denied),
        &[TopicSpec::new("orders", 2), TopicSpec::new("payments", 2)],
        false,
    )
    .await;
    assert_eq!(results.len(), 2);
    for (_, error_code, _) in &results {
        assert_eq!(*error_code, ERROR_TOPIC_AUTHORIZATION_FAILED);
    }
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0]);

    let allowed = principal(true);
    let results = send(
        &state,
        Some(&allowed),
        &[TopicSpec::new("orders", 2)],
        false,
    )
    .await;
    assert_eq!(results[0].1, ERROR_NONE);
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0, 1]);
}

/// Kafka answers a duplicated name before it authorizes the rest
/// (`ControllerApis.createPartitions`): a denied principal gets `INVALID_REQUEST` (42) once for
/// the duplicated name and `TOPIC_AUTHORIZATION_FAILED` (29) only for the others.
#[tokio::test]
#[serial]
async fn create_partitions_answers_a_duplicate_name_before_authorization() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 1).await;

    let denied = principal(false);
    let results = send(
        &state,
        Some(&denied),
        &[
            TopicSpec::new("orders", 2),
            TopicSpec::new("payments", 2),
            TopicSpec::new("orders", 3),
        ],
        false,
    )
    .await;
    let codes: Vec<(&str, i16)> = results
        .iter()
        .map(|(name, error_code, _)| (name.as_str(), *error_code))
        .collect();
    assert_eq!(
        codes,
        vec![
            ("orders", ERROR_INVALID_REQUEST),
            ("payments", ERROR_TOPIC_AUTHORIZATION_FAILED),
        ]
    );
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0]);
}

/// Two requests that overlap on one topic and ask for the same count: Kafka lets exactly one
/// succeed. Both read the count before either writes unless the gateway serializes them, and
/// then both would add and leave the topic past the count either asked for.
#[tokio::test]
#[serial]
async fn create_partitions_two_overlapping_requests_for_one_count_grow_the_topic_once() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 1).await;

    let request = [TopicSpec::new("orders", 3)];
    let (first, second) = tokio::join!(grow(&state, &request), grow(&state, &request));
    let mut error_codes = vec![first[0].1, second[0].1];
    error_codes.sort_unstable();
    assert_eq!(error_codes, vec![ERROR_NONE, ERROR_INVALID_PARTITIONS]);
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0, 1, 2]);
}

/// A topic created in Iggy with a fixed partition count refuses to grow. That is a rule on the
/// server, not a bad request, so it answers `POLICY_VIOLATION` (44) and grows nothing, and
/// `validate_only` answers the same.
#[tokio::test]
#[serial]
async fn create_partitions_on_a_fixed_partition_topic_answers_policy_violation() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    // Creates the mapped stream the fixed topic goes into.
    create_topic(&state, "orders", 1).await;
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        partition_resize_policy: Some(PartitionResizePolicy::Fixed),
        ..TopicCreateOptions::default()
    };
    raw_client(&server)
        .await
        .create_topic(
            &Identifier::named(STREAM).expect("valid stream name"),
            "fixed",
            &options,
        )
        .await
        .expect("create a fixed partition topic");

    for validate_only in [true, false] {
        let results = send(&state, None, &[TopicSpec::new("fixed", 2)], validate_only).await;
        assert_eq!(
            results,
            vec![(
                "fixed".to_string(),
                ERROR_POLICY_VIOLATION,
                Some("this topic was created with a fixed partition count".to_string())
            )],
            "validate_only {validate_only}"
        );
    }
    assert_eq!(iggy_partition_ids(&server, "fixed").await, vec![0]);
}

/// Builds a v0/v1 (non-flexible) request for one topic with a null assignment list.
fn build_legacy_request(name: &str, count: i32) -> Bytes {
    let mut enc = Encoder::with_capacity(64);
    enc.write_i32(1); // topics array length
    enc.write_nullable_string(Some(name))
        .expect("topic name fits");
    enc.write_i32(count);
    enc.write_i32(-1); // null assignments
    enc.write_i32(5_000); // timeout_ms
    enc.write_bool(false); // validate_only
    enc.freeze()
}

/// Decodes a v0/v1 response into `(name, error_code, error_message)`.
fn decode_legacy_results(body: Bytes) -> Vec<(String, i16, Option<String>)> {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let count = d.read_i32().expect("results array length");
    (0..count)
        .map(|_| {
            let name = d
                .read_nullable_string()
                .expect("topic name")
                .expect("topic name is not null");
            let error_code = d.read_i16().expect("error_code");
            let error_message = d.read_nullable_string().expect("error_message");
            (name, error_code, error_message)
        })
        .collect()
}

/// Older clients (Sarama, the Terraform provider) send v0 and v1. The bridge path answers them
/// the same way, error text included.
#[tokio::test]
#[serial]
async fn create_partitions_at_the_legacy_versions_grows_and_refuses_like_v3() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders", 1).await;

    for (version, count) in [(0, 2), (1, 3)] {
        let body = build_legacy_request("orders", count);
        let outcome = create_partitions::handle(&state, None, version, body).await;
        let results = decode_legacy_results(outcome.expect_response("answers"));
        assert_eq!(
            results,
            vec![("orders".to_string(), ERROR_NONE, None)],
            "v{version}"
        );
    }
    assert_eq!(iggy_partition_ids(&server, "orders").await, vec![0, 1, 2]);

    let outcome =
        create_partitions::handle(&state, None, 0, build_legacy_request("orders", 3)).await;
    assert_eq!(
        decode_legacy_results(outcome.expect_response("answers")),
        vec![(
            "orders".to_string(),
            ERROR_INVALID_PARTITIONS,
            Some("Topic already has 3 partition(s).".to_string())
        )]
    );
}
