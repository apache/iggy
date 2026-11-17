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

//! `OffsetCommit` and `OffsetFetch` against a real `iggy-server` process, through the handlers
//! with a connected [`GatewayState`]. The offsets live in Iggy as external group offsets, so they
//! outlive the gateway. Responses are decoded by hand for the reason that
//! `create_topics_real_bridge_tests.rs` gives.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/wire.rs"]
mod wire;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::{
    Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier, Permissions, StreamClient,
    StreamPermissions, TopicClient, TopicPermissions,
};
use serial_test::serial;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::auth::{AuthenticatedPrincipal, TopicReads, UserKey};
use iggy_gateway_kafka::bridge::{IggyBridge, OFFSET_GROUP_PREFIX};
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::acl::PrincipalPermissions;
use iggy_gateway_kafka::protocol::api::{
    ConnectionState, ERROR_GROUP_AUTHORIZATION_FAILED, ERROR_NONE,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState,
};
use iggy_gateway_kafka::protocol::handlers::{offset_commit, offset_fetch};

use codec::Decoder;
use iggy_server::TestServer;
use wire::{
    build_offset_commit_request, build_offset_fetch_groups_request, build_offset_fetch_request,
};

const COMMIT_VERSION: i16 = 8;
const FETCH_VERSION: i16 = 8;
const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;
const GROUP: &str = "orders-app";
const UNKNOWN_OFFSET: i64 = -1;

/// One partition of an `OffsetFetch` answer: `(topic, index, offset, error)`.
type Fetched = (String, i32, i64, i16);

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

async fn seed_topic(server: &TestServer, topic: &str) {
    IggyBridge::connect(server.test_config())
        .await
        .expect("seed bridge should connect")
        .ensure_stream_and_topic(topic, 1)
        .await
        .expect("seed the topic");
}

/// Commits `offset` for partition 0 of `topic`, outside the group protocol, and returns its code.
async fn commit(state: &GatewayState, topic: &str, offset: i64) -> i16 {
    commit_as(state, GROUP, topic, offset).await
}

/// [`commit`] for `group`.
async fn commit_as(state: &GatewayState, group: &str, topic: &str, offset: i64) -> i16 {
    commit_by(state, None, group, topic, offset).await
}

/// [`commit_as`] on a connection that `principal` signed in on.
async fn commit_by(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    group: &str,
    topic: &str,
    offset: i64,
) -> i16 {
    let body = build_offset_commit_request(COMMIT_VERSION, group, topic, offset);
    let connection = ConnectionState::default();
    let response = offset_commit::handle(state, &connection, principal, COMMIT_VERSION, body)
        .await
        .expect_response("OffsetCommit answers");
    let codes = decode_commit_codes(response);
    assert_eq!(codes.len(), 1, "one partition was committed");
    codes[0]
}

/// A signed-in Iggy user that may poll, and read topics when `read_topics`.
fn principal(read_topics: bool) -> AuthenticatedPrincipal {
    AuthenticatedPrincipal {
        username: "reporting".to_string(),
        user: Some(UserKey {
            user_id: 9,
            created_at: 1,
        }),
        permissions: PrincipalPermissions {
            read_topics,
            poll_messages: true,
            ..PrincipalPermissions::default()
        },
        topic_reads: Arc::default(),
        permissions_known: true,
    }
}

/// The group error and partition 0's `(offset, error)` for `group` on `topic`.
async fn fetch(state: &GatewayState, group: &str, topic: &str) -> (i16, i64, i16) {
    let body = build_offset_fetch_request(FETCH_VERSION, group, Some(topic));
    let connection = ConnectionState::default();
    let response = offset_fetch::handle(state, &connection, FETCH_VERSION, body)
        .await
        .expect_response("OffsetFetch answers");
    let (group_error, partitions) = decode_fetch(response);
    assert_eq!(partitions.len(), 1, "partition 0 was asked for");
    let (_, index, offset, error) = &partitions[0];
    assert_eq!(*index, 0);
    (group_error, *offset, *error)
}

/// Decodes a v8 `OffsetCommit` response into one code per partition, in wire order.
fn decode_commit_codes(body: Bytes) -> Vec<i16> {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let mut codes = Vec::new();
    for _ in 1..d.read_varint().expect("topics") {
        let _name = d.read_compact_nullable_string().expect("topic name");
        for _ in 1..d.read_varint().expect("partitions") {
            let _index = d.read_i32().expect("partition_index");
            codes.push(d.read_i16().expect("error_code"));
            d.read_tagged_fields().expect("partition tagged fields");
        }
        d.read_tagged_fields().expect("topic tagged fields");
    }
    codes
}

/// Decodes a one-group v8 `OffsetFetch` response into the group error and every partition.
fn decode_fetch(body: Bytes) -> (i16, Vec<Fetched>) {
    let mut groups = decode_fetch_groups(body);
    assert_eq!(groups.len(), 1, "one group");
    let (_, group_error, partitions) = groups.remove(0);
    (group_error, partitions)
}

/// Decodes a v8 `OffsetFetch` response into `(group, error, partitions)` per group.
fn decode_fetch_groups(body: Bytes) -> Vec<(String, i16, Vec<Fetched>)> {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let mut groups = Vec::new();
    for _ in 1..d.read_varint().expect("groups") {
        let group_id = d
            .read_compact_nullable_string()
            .expect("group_id")
            .expect("a group id is never null");
        let mut partitions = Vec::new();
        for _ in 1..d.read_varint().expect("topics") {
            let name = d
                .read_compact_nullable_string()
                .expect("topic name")
                .expect("a topic name is never null");
            for _ in 1..d.read_varint().expect("partitions") {
                let index = d.read_i32().expect("partition_index");
                let offset = d.read_i64().expect("committed_offset");
                // Java takes any epoch but -1 for a real one. The gateway keeps neither field.
                let leader_epoch = d.read_i32().expect("committed_leader_epoch");
                assert_eq!(leader_epoch, -1, "no leader epoch");
                let metadata = d.read_compact_nullable_string().expect("metadata");
                assert_eq!(metadata.as_deref(), Some(""), "no metadata");
                let error = d.read_i16().expect("error_code");
                d.read_tagged_fields().expect("partition tagged fields");
                partitions.push((name.clone(), index, offset, error));
            }
            d.read_tagged_fields().expect("topic tagged fields");
        }
        let group_error = d.read_i16().expect("group error_code");
        d.read_tagged_fields().expect("group tagged fields");
        groups.push((group_id, group_error, partitions));
    }
    groups
}

fn iggy_id(name: &str) -> Identifier {
    Identifier::named(name).expect("valid Iggy name")
}

/// A Java consumer on defaults commits 0 on an empty partition, and a Kafka commit can sit past
/// the last message. Both are stored as sent.
#[tokio::test]
#[serial]
async fn given_an_empty_partition_when_a_group_commits_should_store_each_offset_as_sent() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;

    for offset in [0, 42, 1 << 40] {
        assert_eq!(commit(&state, "orders", offset).await, ERROR_NONE);
        assert_eq!(
            fetch(&state, GROUP, "orders").await,
            (ERROR_NONE, offset, ERROR_NONE)
        );
    }
}

/// The offset sits under the external group kind of Iggy group `kafka.cg.<group>`, apart from
/// that group's own offsets.
#[tokio::test]
#[serial]
async fn given_a_commit_the_offset_should_live_under_an_external_group_in_iggy() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;

    assert_eq!(commit(&state, "orders", 7).await, ERROR_NONE);

    let raw = iggy_server::raw_client(&server).await;
    let group = iggy_id(&format!("{OFFSET_GROUP_PREFIX}{GROUP}"));
    let (stream, topic) = (iggy_id("kafka"), iggy_id("orders"));
    assert!(
        raw.get_consumer_group(&stream, &topic, &group)
            .await
            .expect("read the group")
            .is_some(),
        "the first commit creates the Iggy group"
    );
    let stored = |consumer: Consumer| {
        let (raw, stream, topic) = (&raw, &stream, &topic);
        async move {
            raw.get_consumer_offset(&consumer, stream, topic, Some(0))
                .await
                .expect("read the offset")
                .map(|info| info.stored_offset)
        }
    };
    assert_eq!(
        stored(Consumer::external_group(group.clone())).await,
        Some(7)
    );
    assert_eq!(stored(Consumer::group(group)).await, None);
}

/// A principal that may poll but holds no read or manage permission on topics or streams commits
/// nothing, also to a group that exists, as Iggy would create no group for it. A listener that
/// authenticates no one commits as before.
#[tokio::test]
#[serial]
async fn given_a_principal_without_a_read_permission_should_commit_nothing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    let poller = principal(false);
    let raw = iggy_server::raw_client(&server).await;
    let (stream, topic) = (iggy_id("kafka"), iggy_id("orders"));
    let iggy_group = iggy_id(&format!("{OFFSET_GROUP_PREFIX}fresh"));

    assert_eq!(
        commit_by(&state, Some(&poller), "fresh", "orders", 7).await,
        ERROR_GROUP_AUTHORIZATION_FAILED
    );
    assert!(
        raw.get_consumer_group(&stream, &topic, &iggy_group)
            .await
            .expect("read the group")
            .is_none(),
        "the refused commit creates no Iggy group"
    );

    assert_eq!(
        commit_by(&state, Some(&principal(true)), "fresh", "orders", 8).await,
        ERROR_NONE
    );
    assert_eq!(
        commit_by(&state, Some(&poller), "fresh", "orders", 9).await,
        ERROR_GROUP_AUTHORIZATION_FAILED,
        "also once the group exists"
    );
    assert_eq!(
        fetch(&state, "fresh", "orders").await,
        (ERROR_NONE, 8, ERROR_NONE)
    );

    let anonymous = AuthenticatedPrincipal::anonymous();
    assert_eq!(
        commit_by(&state, Some(&anonymous), "open", "orders", 3).await,
        ERROR_NONE
    );
}

/// [`principal`] without a global read, holding the stream and topic grants of `permissions`.
fn scoped(permissions: &Permissions) -> AuthenticatedPrincipal {
    let mut principal = principal(false);
    principal.topic_reads = Arc::new(TopicReads::of(permissions));
    principal
}

/// `granted` on stream `stream`, and nothing else.
fn on_stream(stream: usize, granted: StreamPermissions) -> Permissions {
    Permissions {
        streams: Some(BTreeMap::from([(stream, granted)])),
        ..Permissions::default()
    }
}

/// Iggy creates a consumer group for a user with a read or manage grant on the topic's stream, or
/// on the topic, and not only for one with a global grant. A principal with such a grant commits
/// there, and nowhere else. The ids come from the change log, as the grants do.
#[tokio::test]
#[serial]
async fn given_a_stream_or_topic_grant_should_commit_only_where_it_may_read() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    seed_topic(&server, "refunds").await;
    let state = connected_state(&server).await;
    let bridge = state.bridge.as_ref().expect("a bridge");
    let shown = tokio::time::timeout(Duration::from_secs(10), async {
        while bridge.topic_shape("orders").is_none() || bridge.topic_shape("refunds").is_none() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(shown.is_ok(), "the change log shows both topics");
    let raw = iggy_server::raw_client(&server).await;
    let stream = raw
        .get_stream(&iggy_id("kafka"))
        .await
        .expect("read the stream")
        .expect("the stream exists");
    let orders = raw
        .get_topic(&iggy_id("kafka"), &iggy_id("orders"))
        .await
        .expect("read the topic")
        .expect("the topic exists");
    let stream_id = usize::try_from(stream.id).expect("a stream id fits usize");
    let orders_id = usize::try_from(orders.id).expect("a topic id fits usize");

    let whole_stream = scoped(&on_stream(
        stream_id,
        StreamPermissions {
            read_stream: true,
            ..StreamPermissions::default()
        },
    ));
    assert_eq!(
        commit_by(&state, Some(&whole_stream), "wide", "refunds", 4).await,
        ERROR_NONE,
        "a stream grant covers each topic of the stream"
    );

    let only_orders = scoped(&on_stream(
        stream_id,
        StreamPermissions {
            topics: Some(BTreeMap::from([(
                orders_id,
                TopicPermissions {
                    read_topic: true,
                    ..TopicPermissions::default()
                },
            )])),
            ..StreamPermissions::default()
        },
    ));
    assert_eq!(
        commit_by(&state, Some(&only_orders), "narrow", "orders", 5).await,
        ERROR_NONE
    );
    assert_eq!(
        commit_by(&state, Some(&only_orders), "narrow", "refunds", 6).await,
        ERROR_GROUP_AUTHORIZATION_FAILED,
        "the topic grant is on orders only"
    );
    assert!(
        raw.get_consumer_group(
            &iggy_id("kafka"),
            &iggy_id("refunds"),
            &iggy_id(&format!("{OFFSET_GROUP_PREFIX}narrow"))
        )
        .await
        .expect("read the group")
        .is_none(),
        "the refused commit creates no Iggy group"
    );

    let other_stream = scoped(&on_stream(
        stream_id + 1,
        StreamPermissions {
            read_stream: true,
            ..StreamPermissions::default()
        },
    ));
    assert_eq!(
        commit_by(&state, Some(&other_stream), "elsewhere", "orders", 7).await,
        ERROR_GROUP_AUTHORIZATION_FAILED,
        "a grant on another stream"
    );
    assert_eq!(
        commit_by(&state, Some(&only_orders), "narrow", "gone", 8).await,
        ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        "a topic the change log does not show, which clients retry"
    );
}

/// Kafka consumers read any negative offset as none, so a negative commit deletes the key. A key
/// or a group that is not there counts as deleted.
#[tokio::test]
#[serial]
async fn given_a_negative_commit_the_offset_should_be_deleted() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;

    assert_eq!(commit(&state, "orders", -1).await, ERROR_NONE);
    assert_eq!(commit(&state, "orders", 7).await, ERROR_NONE);
    assert_eq!(commit(&state, "orders", -1).await, ERROR_NONE);
    assert_eq!(
        fetch(&state, GROUP, "orders").await,
        (ERROR_NONE, UNKNOWN_OFFSET, ERROR_NONE)
    );
    assert_eq!(commit(&state, "orders", -1).await, ERROR_NONE);
}

/// No key, no group and no topic all read as -1 with no error, as Kafka answers them.
#[tokio::test]
#[serial]
async fn given_nothing_committed_fetch_should_answer_minus_one_without_error() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;

    assert_eq!(
        fetch(&state, "never-committed", "orders").await,
        (ERROR_NONE, UNKNOWN_OFFSET, ERROR_NONE)
    );
    assert_eq!(
        fetch(&state, GROUP, "no-such-topic").await,
        (ERROR_NONE, UNKNOWN_OFFSET, ERROR_NONE)
    );
}

/// The acceptance rule of #3542: committed offsets survive a gateway restart. A new gateway is a
/// new bridge with no memory of the old one.
#[tokio::test]
#[serial]
async fn given_a_restarted_gateway_fetch_should_return_the_committed_offset() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;

    let first = connected_state(&server).await;
    assert_eq!(commit(&first, "orders", 1_234).await, ERROR_NONE);
    drop(first);

    let restarted = connected_state(&server).await;
    assert_eq!(
        fetch(&restarted, GROUP, "orders").await,
        (ERROR_NONE, 1_234, ERROR_NONE)
    );
}

/// The Java `Admin.listConsumerGroupOffsets` asks for a whole group with a null topic list. Only
/// the topics that hold the group come back, with only their committed partitions.
#[tokio::test]
#[serial]
async fn given_a_null_topic_list_fetch_should_answer_every_committed_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    for topic in ["orders", "payments", "untouched"] {
        seed_topic(&server, topic).await;
    }
    let state = connected_state(&server).await;
    assert_eq!(commit(&state, "orders", 3).await, ERROR_NONE);
    assert_eq!(commit(&state, "payments", 5).await, ERROR_NONE);

    let body = build_offset_fetch_request(FETCH_VERSION, GROUP, None);
    let connection = ConnectionState::default();
    let response = offset_fetch::handle(&state, &connection, FETCH_VERSION, body)
        .await
        .expect_response("OffsetFetch answers");
    let (group_error, mut partitions) = decode_fetch(response);

    partitions.sort();
    assert_eq!(group_error, ERROR_NONE);
    assert_eq!(
        partitions,
        vec![
            ("orders".to_string(), 0, 3, ERROR_NONE),
            ("payments".to_string(), 0, 5, ERROR_NONE),
        ]
    );
}

/// A v8 request names several groups, and they read at once. Each answer is its own group's.
#[tokio::test]
#[serial]
async fn given_several_groups_in_one_request_fetch_should_answer_each_its_own_offset() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    assert_eq!(
        commit_as(&state, "first-app", "orders", 3).await,
        ERROR_NONE
    );
    assert_eq!(
        commit_as(&state, "second-app", "orders", 9).await,
        ERROR_NONE
    );

    let groups = ["first-app", "second-app", "never-committed"];
    let body = build_offset_fetch_groups_request(FETCH_VERSION, &groups, "orders");
    let connection = ConnectionState::default();
    let response = offset_fetch::handle(&state, &connection, FETCH_VERSION, body)
        .await
        .expect_response("OffsetFetch answers");

    let partition_zero = |offset| vec![("orders".to_string(), 0, offset, ERROR_NONE)];
    assert_eq!(
        decode_fetch_groups(response),
        vec![
            ("first-app".to_string(), ERROR_NONE, partition_zero(3)),
            ("second-app".to_string(), ERROR_NONE, partition_zero(9)),
            (
                "never-committed".to_string(),
                ERROR_NONE,
                partition_zero(UNKNOWN_OFFSET)
            ),
        ]
    );
}
