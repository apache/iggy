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

//! Integration tests for `IggyBridge` against a real `iggy-server` process - not the
//! `KafkaGateway` under test elsewhere in this suite. `#3533` acceptance criteria this file
//! exercises directly: `ensure_stream_and_topic` idempotent on repeated calls, and the bridge
//! module invoked from a real (non-unit) test rather than only compiled.

use std::collections::HashMap;
use std::time::Duration;

use iggy::prelude::{
    Identifier, IggyMessage, MessageClient, Partitioning, StreamClient, TopicClient,
};
use secrecy::SecretString;
use serial_test::serial;

use iggy_gateway_kafka::bridge::{
    BridgeError, IggyBridge, IggyBridgeConfig, TopicMapping, TopicOverride,
};

#[path = "common/iggy_server.rs"]
mod iggy_server;

use iggy_server::{PortGuard, TestServer, raw_client};

#[tokio::test]
#[serial]
async fn ensure_stream_and_topic_is_idempotent_on_repeated_calls() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("first call creates the stream and topic");
    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("second call is a no-op against the now-existing stream and topic");
    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("third call is still a no-op");

    // Read back real state, not just three Oks: proves the second and third calls were actually
    // no-ops against the one topic the first call created, not e.g. three independent topics
    // that all happen to satisfy Ok(()) individually.
    let raw = raw_client(&server).await;
    let topics = raw
        .get_topics(&Identifier::named("kafka").expect("valid stream name"))
        .await
        .expect("get_topics call");
    assert_eq!(topics.len(), 1, "must be exactly one topic, not three");
    assert_eq!(topics[0].name, "orders");
    assert_eq!(topics[0].partitions_count, 3);
}

/// Regression test for coverage: nothing previously exercised `PartitionCountMismatch` on a real
/// server - only unit tests constructed the variant directly.
#[tokio::test]
#[serial]
async fn ensure_stream_and_topic_rejects_a_second_call_with_a_different_partition_count() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("first call creates the topic with 3 partitions");

    let err = bridge
        .ensure_stream_and_topic("orders", 5)
        .await
        .expect_err(
            "a different partition count against an existing topic must not silently succeed",
        );
    match err {
        BridgeError::PartitionCountMismatch {
            topic,
            existing,
            requested,
        } => {
            assert_eq!(topic, "orders", "must report the Kafka-side name");
            assert_eq!(existing, 3);
            assert_eq!(requested, 5);
        }
        other => panic!("expected PartitionCountMismatch, got {other:?}"),
    }
}

/// Regression test for coverage: every other test uses an empty `TopicMapping` (`HashMap::new()`),
/// so the Kafka-name-vs-Iggy-name distinction `ensure_topic`/`high_watermark` deliberately
/// maintain in their error paths (`kafka_topic`, not the resolved `topic_name`) can never actually
/// differ and so can never be caught wrong. This test maps a Kafka topic to a differently-named
/// Iggy stream/topic and checks both the happy path and the error paths report the Kafka-side
/// name a real caller (a future handler) would recognize.
#[tokio::test]
#[serial]
async fn bridge_operations_report_the_kafka_side_name_through_a_real_topic_mapping_override() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let mut config = server.test_config();
    let mut topics = HashMap::new();
    topics.insert(
        "orders".to_string(),
        TopicOverride {
            stream: "billing".to_string(),
            topic: "orders_v2".to_string(),
        },
    );
    config.topic_mapping = TopicMapping::new("kafka".to_string(), topics)
        .expect("valid mapping for this test's fixture data");
    let bridge = IggyBridge::connect(config)
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("must create the mapped Iggy stream/topic, not one named after the Kafka topic");

    // The Iggy-side resources are named differently from the Kafka topic - confirms the mapping
    // actually took effect, not that resolve() was a no-op that happened to pass either way.
    let raw = raw_client(&server).await;
    let billing_topics = raw
        .get_topics(&Identifier::named("billing").expect("valid stream name"))
        .await
        .expect("get_topics call");
    assert_eq!(billing_topics.len(), 1);
    assert_eq!(billing_topics[0].name, "orders_v2");

    let watermark = bridge
        .high_watermark("orders", 0)
        .await
        .expect("must resolve through the mapping, not fail looking for a stream named 'orders'");
    assert_eq!(watermark, 0);

    let out_of_range = bridge
        .high_watermark("orders", 5)
        .await
        .expect_err("partition 5 does not exist on a 1-partition topic");
    assert!(
        matches!(
            &out_of_range,
            BridgeError::PartitionOutOfRange { topic, .. } if topic == "orders"
        ),
        "error must quote the Kafka topic name 'orders', not the Iggy name 'orders_v2': \
         {out_of_range:?}"
    );

    let mismatch = bridge
        .ensure_stream_and_topic("orders", 2)
        .await
        .expect_err("different partition count against the mapped topic must not silently succeed");
    assert!(
        matches!(
            &mismatch,
            BridgeError::PartitionCountMismatch { topic, .. } if topic == "orders"
        ),
        "error must quote the Kafka topic name 'orders', not the Iggy name 'orders_v2': \
         {mismatch:?}"
    );
}

#[tokio::test]
#[serial]
async fn high_watermark_is_zero_for_a_fresh_empty_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("stream and topic must exist before checking the watermark");

    let watermark = bridge
        .high_watermark("orders", 0)
        .await
        .expect("fresh topic must report a watermark, not an error");
    assert_eq!(
        watermark, 0,
        "a freshly created, empty partition's high watermark must be 0"
    );
}

/// Pins the exact semantics of `Iggy::Partition::current_offset` (offset of the *last written*
/// message, not Kafka's "next offset to produce") against a real server - a test that only
/// checked the empty-topic case would pass under either interpretation and hide an off-by-one.
#[tokio::test]
#[serial]
async fn high_watermark_reflects_produced_messages() {
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
    let mut messages: Vec<IggyMessage> = (0..3)
        .map(|i| IggyMessage::from(format!("message-{i}")))
        .collect();
    let client = raw_client(&server).await;
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send 3 messages");

    let watermark = bridge
        .high_watermark("orders", 0)
        .await
        .expect("topic must report a watermark after producing");
    assert_eq!(
        watermark, 3,
        "high watermark after 3 messages (offsets 0, 1, 2) must be 3, not the last offset (2)"
    );
}

#[tokio::test]
#[serial]
async fn high_watermark_rejects_out_of_range_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("stream and topic must exist before checking the watermark");

    let err = bridge
        .high_watermark("orders", 5)
        .await
        .expect_err("partition 5 does not exist on a 1-partition topic");
    assert!(matches!(err, BridgeError::PartitionOutOfRange { .. }));
}

#[tokio::test]
#[serial]
async fn ensure_stream_and_topic_is_idempotent_for_a_numeric_topic_name() {
    // Regression test: Identifier::try_from/FromStr parses an all-digit string as a numeric ID,
    // not a name - a second call for the same numeric-named topic would look it up by the wrong
    // resource kind and fail with StreamIdNotFound/TopicIdNotFound despite the topic existing.
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("2024", 1)
        .await
        .expect("first call creates the numeric-named stream and topic");
    bridge
        .ensure_stream_and_topic("2024", 1)
        .await
        .expect("second call must still find the numeric-named topic by name, not by ID");
}

/// Regression test: `ensure_stream` used to hand `ensure_topic` the stream's *numeric* id
/// (`Identifier::numeric`), and streams are backed by a recycled slab (freed keys are reused by
/// the next created stream) - a stream deleted and recreated *between* `ensure_stream`'s own
/// `get_stream`/`create_stream` call and `ensure_topic`'s use of the id it returned would leave
/// `ensure_topic` writing into whatever stream now holds that recycled numeric key, not the one
/// `ensure_stream_and_topic`'s caller actually resolved. The fix threads the *named* `Identifier`
/// through instead.
///
/// What this test actually proves, and what it does not: `ensure_stream` and `ensure_topic` are
/// both private, called back-to-back inside one `ensure_stream_and_topic` invocation with no
/// `await` point this black-box test can land a delete-and-recreate inside - the exact race the
/// bug lived in is not reproducible from here, full stop, not just "hard." What this test does
/// verify is the weaker, but real, precondition: `ensure_stream_and_topic` has no bridge-level
/// cache, so its *second*, wholly separate invocation always resolves the stream fresh, by name.
/// A hypothetical regression back to `Identifier::numeric(...)` would still pass every assertion
/// below, because the numeric id that build would return comes from this call's own fresh,
/// by-name lookup - already correct by construction, not a stale value carried over from the
/// first call. Delete-and-recreate before the second call is not exercising the TOCTOU window at
/// all; it is here only to document (via the id-collision check right below) that the metadata
/// slab does in fact recycle freed keys, which is the premise the original bug depended on.
#[tokio::test]
#[serial]
async fn ensure_topic_targets_the_streams_live_incarnation_after_a_delete_and_recreate() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("first call creates the default stream and topic");

    let raw = raw_client(&server).await;
    let stream_name = Identifier::named("kafka").expect("valid stream name");
    let original = raw
        .get_stream(&stream_name)
        .await
        .expect("get_stream call")
        .expect("stream exists after ensure_stream_and_topic");
    raw.delete_stream(&Identifier::numeric(original.id).expect("numeric id"))
        .await
        .expect("delete the stream (and its topic with it)");
    let recreated = raw
        .create_stream("kafka")
        .await
        .expect("recreate a stream under the same name");
    if recreated.id == original.id {
        // Not guaranteed by any API contract, but the metadata slab does reuse freed low
        // indices - confirms this run actually exercised the recycled-key scenario, not just a
        // coincidentally-fresh one.
        eprintln!(
            "recreated stream reused the original numeric id {}",
            recreated.id
        );
    }

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("must create the topic under the live stream, not a stale numeric id");

    let topic = raw
        .get_topic(
            &stream_name,
            &Identifier::named("orders").expect("valid topic name"),
        )
        .await
        .expect("get_topic call")
        .expect("topic exists under the recreated stream");
    assert_eq!(topic.partitions_count, 1);
}

/// Finding: the SDK's connection-string parser splits on `@` then `:`, so a password containing
/// either character breaks unless credentials are passed as already-separated fields.
#[tokio::test]
#[serial]
async fn connect_succeeds_with_password_containing_special_characters() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn_with_password(data_dir.path(), "p@ss:word").await;

    IggyBridge::connect(server.test_config())
        .await
        .expect("bridge must connect with a password containing '@' and ':'");
}

/// Acceptance criterion: "no panics on Iggy unreachable at handler boundary." Connects to a port
/// nothing is listening on and asserts a plain `Err`, not a panic - the strongest way to fail this
/// assertion is exactly the failure mode being guarded against.
#[tokio::test]
async fn connect_to_unreachable_iggy_returns_err_not_panic() {
    let port_guard = PortGuard::acquire(); // locked but never bound - nothing listens on it
    let config = IggyBridgeConfig {
        address: format!("127.0.0.1:{}", port_guard.port),
        username: "iggy".to_string(),
        password: SecretString::from("iggy"),
        topic_mapping: TopicMapping::new("kafka".to_string(), HashMap::new())
            .expect("valid mapping for this test's fixture data"),
    };

    let result = IggyBridge::connect(config).await;
    assert!(matches!(result, Err(BridgeError::Iggy(_))));
}

/// Regression test for the missing dial timeout: `TcpClient::establish_bounded` only applies its
/// own `FAILOVER_DIAL_TIMEOUT` when at least two failover candidates are configured, which a
/// bridge (always exactly one address) never has, so the underlying `TcpStream::connect` had no
/// deadline at all against an address that drops packets instead of refusing them. 192.0.2.1 is
/// RFC 5737 TEST-NET-1 - reserved for documentation, routed nowhere, so the connect attempt hangs
/// on the kernel's own SYN-retry timeout (confirmed against this exact address before writing this
/// test: over the sandbox's real network stack, a bare `connect()` past 3s had not yet failed).
/// Before `REQUEST_TIMEOUT`, this test would have hung for minutes; now it must fail within a
/// bounded window.
#[tokio::test]
async fn connect_to_a_black_hole_address_times_out_instead_of_hanging() {
    let config = IggyBridgeConfig {
        address: "192.0.2.1:1234".to_string(),
        username: "iggy".to_string(),
        password: SecretString::from("iggy"),
        topic_mapping: TopicMapping::new("kafka".to_string(), HashMap::new())
            .expect("valid mapping for this test's fixture data"),
    };

    let start = tokio::time::Instant::now();
    // Outer safety net, not the behavior under test: if REQUEST_TIMEOUT regresses to "none"
    // again, this fails the test in bounded time instead of hanging the whole suite.
    let result = tokio::time::timeout(Duration::from_secs(30), IggyBridge::connect(config))
        .await
        .expect("IggyBridge::connect must return on its own, not hang past a generous margin");

    assert!(
        start.elapsed() < Duration::from_secs(20),
        "connect took {:?}, longer than REQUEST_TIMEOUT (15s) should allow",
        start.elapsed()
    );
    // BridgeError::Timeout, not BridgeError::Iggy: with_request_timeout's own elapsed branch
    // maps here (see that function's doc for why an unknown outcome must not borrow
    // CannotEstablishConnection's known-safe code).
    assert!(matches!(result, Err(BridgeError::Timeout)));
}

/// Regression test: `IggyClient::disconnect` (what `close` used to call) never touches
/// `heartbeat_handle`, so a bridge that called it kept heartbeating on a schedule and would
/// silently reconnect. `close` now calls `shutdown`, which this test only asserts succeeds against
/// a live connection - the heartbeat task's actual termination isn't observable from outside the
/// SDK, but a `close` that itself errored or hung would be a regression this catches directly.
#[tokio::test]
#[serial]
async fn close_succeeds_against_a_live_connection() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge.close().await.expect("close must succeed");
}

/// Kafka-client-observable regression test for the auth-mapping fix: a wrong bridge password is
/// the *bridge's own* misconfiguration (`IGGY_KAFKA_IGGY_PASSWORD`), not anything the Kafka client
/// did - the SDK's own sign-in path raises `InvalidPassword`/`InvalidCredentials` for this, not
/// `Unauthorized` (that one means a real, authenticated-but-forbidden ACL problem). Mapping this
/// to `TOPIC_AUTHORIZATION_FAILED` (29) would hand a real Kafka client library a fatal,
/// non-retriable "Not authorized to access topics" it has no way to act on. Asserts the actual
/// wire code a handler would send, not just the `BridgeError` variant - that number is what a real
/// Kafka client's error-handling logic branches on.
#[tokio::test]
#[serial]
async fn connect_with_wrong_password_maps_to_unknown_server_error_not_authorization_failed() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn_with_password(data_dir.path(), "the-real-password").await;

    let mut config = server.test_config();
    config.password = SecretString::from("a-completely-wrong-password");
    // `IggyBridge` derives no `Debug`, so `expect_err`/`unwrap_err` (which require `T: Debug` on
    // the `Ok` side too) don't apply here - match it out by hand instead.
    let Err(err) = IggyBridge::connect(config).await else {
        panic!("wrong password must not connect")
    };

    assert_eq!(
        err.to_kafka_error_code(),
        iggy_gateway_kafka::protocol::api::ERROR_UNKNOWN_SERVER_ERROR,
        "a bridge-side credential error must not surface as the Kafka client's own \
         TOPIC_AUTHORIZATION_FAILED (29): {err:?}"
    );
    assert_ne!(
        err.to_kafka_error_code(),
        iggy_gateway_kafka::protocol::api::ERROR_TOPIC_AUTHORIZATION_FAILED,
        "must not blame the Kafka client's ACLs for the bridge's own wrong password: {err:?}"
    );
}

/// End-to-end regression test for the batch high-watermark API: creates a 3-partition topic,
/// produces a different message count to each partition, and confirms one `high_watermarks` call
/// reports all three correctly and in the order requested - not just that a single-partition call
/// still works (that's `high_watermark_reflects_produced_messages`), but that the batching itself
/// keeps each partition's own count separate rather than conflating them.
#[tokio::test]
#[serial]
async fn high_watermarks_reports_every_requested_partition_from_one_round_trip() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("stream and topic must exist before producing");

    let stream_id = Identifier::named("kafka").expect("valid stream name");
    let topic_id = Identifier::named("orders").expect("valid topic name");
    let client = raw_client(&server).await;
    for (partition, count) in [(0u32, 1usize), (1, 3), (2, 0)] {
        if count == 0 {
            continue;
        }
        let mut messages: Vec<IggyMessage> = (0..count)
            .map(|i| IggyMessage::from(format!("partition-{partition}-message-{i}")))
            .collect();
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(partition),
                &mut messages,
            )
            .await
            .expect("send messages to this partition");
    }

    let watermarks: Vec<(u32, i64)> = bridge
        .high_watermarks("orders", &[0, 1, 2])
        .await
        .expect("all three partitions exist on this topic")
        .into_iter()
        .map(|(partition, watermark)| {
            (
                partition,
                watermark.expect("every requested partition exists on this topic"),
            )
        })
        .collect();

    assert_eq!(
        watermarks,
        vec![(0, 1), (1, 3), (2, 0)],
        "must report each partition's own watermark, in the order requested"
    );
}

/// Regression test: an earlier version of `high_watermarks` collected the whole batch into one
/// `Result`, so the first out-of-range partition discarded every watermark already resolved for
/// the call. Mixes a valid and an out-of-range partition in one call and asserts the valid one's
/// watermark still comes back.
#[tokio::test]
#[serial]
async fn high_watermarks_keeps_resolved_partitions_when_another_is_out_of_range() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("stream and topic must exist before checking watermarks");

    let results = bridge
        .high_watermarks("orders", &[0, 5])
        .await
        .expect("the call itself succeeds - only partition 5 is out of range, not the topic");

    assert_eq!(
        results.len(),
        2,
        "must report one entry per requested partition"
    );
    // BridgeError has no PartialEq (it wraps IggyError, itself not comparable), so the per-partition
    // Results are checked by hand rather than via assert_eq! on the whole tuple.
    let (partition0, watermark0) = &results[0];
    assert_eq!(*partition0, 0);
    assert_eq!(
        *watermark0
            .as_ref()
            .expect("partition 0 exists and must not be discarded by partition 5's error"),
        0
    );
    let (partition5, watermark5) = &results[1];
    assert_eq!(*partition5, 5);
    assert!(
        matches!(watermark5, Err(BridgeError::PartitionOutOfRange { .. })),
        "partition 5 does not exist on a 1-partition topic: {watermark5:?}"
    );
}

/// Kafka-client-observable regression test for topic-name validation: a whitespace-padded name is
/// not just unlikely for a real Kafka client to send, it's impossible - `Topic.legalChars` has no
/// space in it - so this exercises the defense-in-depth path a non-conformant client could still
/// reach, and asserts the wire code (`INVALID_TOPIC_EXCEPTION`, 17) a real client library would
/// recognize as "the topic name itself is the problem," not a generic server error.
#[tokio::test]
#[serial]
async fn ensure_stream_and_topic_rejects_a_padded_kafka_topic_name() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    let err = bridge
        .ensure_stream_and_topic(" orders ", 1)
        .await
        .expect_err("a padded Kafka topic name must not silently create a padded Iggy topic");

    assert!(
        matches!(err, BridgeError::InvalidKafkaTopicName { .. }),
        "expected InvalidKafkaTopicName, got {err:?}"
    );
    assert_eq!(
        err.to_kafka_error_code(),
        iggy_gateway_kafka::protocol::api::ERROR_INVALID_TOPIC_EXCEPTION
    );
}

#[tokio::test]
#[serial]
async fn high_watermark_rejects_a_padded_kafka_topic_name() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    let err = bridge
        .high_watermark(" orders ", 0)
        .await
        .expect_err("a padded Kafka topic name must be rejected before any Iggy lookup");
    assert!(matches!(err, BridgeError::InvalidKafkaTopicName { .. }));
}
