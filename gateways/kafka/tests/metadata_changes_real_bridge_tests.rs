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

//! The bridge follows Iggy's metadata change log, against a real `iggy-server` process. A group
//! that stores offsets through one gateway lists on another without a scan per topic, a bridge
//! user without `manage_servers` still lists groups by scanning, and an open connection takes the
//! permission changes of its Iggy user.

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
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use iggy::prelude::{
    GlobalPermissions, Identifier, IggyClient, Permissions, SystemClient, TopicClient, UserClient,
    UserCreateOptions, UserStatus,
};
use secrecy::SecretString;
use serial_test::serial;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::auth::IggyAuthenticator;
use iggy_gateway_kafka::bridge::{IggyBridge, IggyBridgeConfig, OffsetGroupSource, UserAccess};
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::listener::SecurityProtocol;
use iggy_gateway_kafka::protocol::api::{
    API_KEY_LIST_GROUPS, API_KEY_OFFSET_COMMIT, ERROR_COORDINATOR_LOAD_IN_PROGRESS, ERROR_NONE,
    GatewayState, handle_request_bounded,
};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

use codec::Decoder;
use iggy_server::TestServer;
use tcp::{ByteRead, build_request_frame, parse_response_payload, read_byte_with_timeout};
use wire::{build_list_groups_request, build_offset_commit_request};

const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;
const COMMIT_VERSION: i16 = 8;
const LIST_VERSION: i16 = 5;
/// How long a change takes to reach another gateway: the follower reads every 250 ms. The rest is
/// room for a slow test machine.
const ABOUT_A_SECOND: Duration = Duration::from_millis(1500);
const PASSWORD: &str = "gateway-secret";

async fn gateway(config: IggyBridgeConfig) -> GatewayState {
    let bridge = IggyBridge::connect(config)
        .await
        .expect("bridge should connect to a ready server");
    GatewayState::new(
        Some(Arc::new(bridge)),
        TEST_MAX_FRAME_SIZE,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
    )
}

/// A gateway whose bridge signs in as `username`.
async fn gateway_as(server: &TestServer, username: &str) -> GatewayState {
    gateway(IggyBridgeConfig {
        username: username.to_owned(),
        password: SecretString::from(PASSWORD),
        ..server.test_config()
    })
    .await
}

/// Creates Iggy user `username` with `global` permissions. Its id.
async fn create_user(raw: &IggyClient, username: &str, global: GlobalPermissions) -> u32 {
    raw.create_user(
        username,
        PASSWORD,
        UserStatus::Active,
        Some(Permissions {
            global,
            streams: None,
        }),
        &UserCreateOptions::default(),
    )
    .await
    .expect("create a user")
    .id
}

async fn seed_topic(server: &TestServer, topic: &str) {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("seed bridge should connect");
    bridge
        .ensure_stream_and_topic(topic, 1)
        .await
        .expect("seed the topic");
    bridge.close().await.expect("the seed bridge closes");
}

async fn commit(state: &GatewayState, group: &str, topic: &str) {
    let body = build_offset_commit_request(COMMIT_VERSION, group, topic, 1);
    let response = handle_request_bounded(state, API_KEY_OFFSET_COMMIT, COMMIT_VERSION, body)
        .await
        .expect_response("OffsetCommit answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    assert_eq!(d.read_varint().expect("topics"), 2, "one topic");
    let _name = d.read_compact_nullable_string().expect("topic name");
    assert_eq!(d.read_varint().expect("partitions"), 2, "one partition");
    let _index = d.read_i32().expect("partition_index");
    assert_eq!(d.read_i16().expect("error_code"), ERROR_NONE, "the commit");
}

/// The top-level error and the group ids of a v5 `ListGroups` answer.
async fn list(state: &GatewayState) -> (i16, Vec<String>) {
    let body = build_list_groups_request(LIST_VERSION, &[], &[]);
    let response = handle_request_bounded(state, API_KEY_LIST_GROUPS, LIST_VERSION, body)
        .await
        .expect_response("ListGroups answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let error = d.read_i16().expect("error_code");
    let mut groups = Vec::new();
    for _ in 1..d.read_varint().expect("groups") {
        let mut string = || {
            d.read_compact_nullable_string()
                .expect("a string")
                .expect("not null")
        };
        let (group_id, _protocol_type, _state, _group_type) =
            (string(), string(), string(), string());
        groups.push(group_id);
        d.read_tagged_fields().expect("group tagged fields");
    }
    d.read_tagged_fields().expect("tagged fields");
    assert_eq!(d.remaining(), 0, "ListGroups trailing bytes");
    (error, groups)
}

/// Lists until the groups satisfy `done`, for up to `limit`. The last groups, and how long it
/// took.
async fn list_until(
    state: &GatewayState,
    limit: Duration,
    done: impl Fn(&[String]) -> bool,
) -> (Vec<String>, Duration) {
    let started = Instant::now();
    loop {
        let (error, groups) = list(state).await;
        let settled = error == ERROR_NONE && done(&groups);
        if settled || started.elapsed() >= limit {
            assert!(
                error == ERROR_NONE || error == ERROR_COORDINATOR_LOAD_IN_PROGRESS,
                "ListGroups error {error}"
            );
            return (groups, started.elapsed());
        }
        sleep(Duration::from_millis(20)).await;
    }
}

/// The Iggy clients that user `user_id` holds.
async fn clients_of(raw: &IggyClient, user_id: u32) -> usize {
    raw.get_clients()
        .await
        .expect("list the clients")
        .iter()
        .filter(|client| client.user_id == Some(user_id))
        .count()
}

#[tokio::test]
#[serial]
async fn given_two_gateways_when_one_stores_a_first_offset_should_list_it_on_the_other_within_a_second()
 {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let raw = iggy_server::raw_client(&server).await;
    let follower_id = create_user(
        &raw,
        "gateway-b",
        GlobalPermissions {
            manage_servers: true,
            ..GlobalPermissions::default()
        },
    )
    .await;
    let first = gateway(server.test_config()).await;
    let second = gateway_as(&server, "gateway-b").await;
    let (before, _) = list_until(&second, ABOUT_A_SECOND, |_| true).await;

    commit(&first, "orders-app", "orders").await;
    let (after, took) = list_until(&second, ABOUT_A_SECOND * 4, |groups| {
        groups.iter().any(|group| group == "orders-app")
    })
    .await;

    assert!(before.is_empty(), "{before:?}");
    assert_eq!(after, ["orders-app"]);
    assert!(took <= ABOUT_A_SECOND, "the group listed after {took:?}");
    let bridge = second.bridge.as_deref().expect("a bridge");
    assert_eq!(bridge.offset_group_source(), OffsetGroupSource::ChangeLog);
    assert_eq!(
        clients_of(&raw, follower_id).await,
        2,
        "the shared client and the change log's, and no scan client"
    );
}

/// Iggy deletes the groups of a topic with it, and records no group change for them.
#[tokio::test]
#[serial]
async fn given_a_deleted_kafka_topic_when_following_should_drop_its_groups_from_the_list() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    for topic in ["orders", "payments"] {
        seed_topic(&server, topic).await;
    }
    let first = gateway(server.test_config()).await;
    let second = gateway(server.test_config()).await;
    commit(&first, "orders-app", "orders").await;
    commit(&first, "payments-app", "payments").await;
    let (both, _) = list_until(&second, ABOUT_A_SECOND * 4, |groups| groups.len() == 2).await;

    let raw = iggy_server::raw_client(&server).await;
    let iggy_id = |name: &str| Identifier::named(name).expect("valid Iggy name");
    raw.delete_topic(&iggy_id("kafka"), &iggy_id("orders"))
        .await
        .expect("delete the topic");
    let (left, took) = list_until(&second, ABOUT_A_SECOND * 4, |groups| groups.len() == 1).await;

    assert_eq!(both, ["orders-app", "payments-app"]);
    assert_eq!(left, ["payments-app"]);
    assert!(took <= ABOUT_A_SECOND, "the group left after {took:?}");
}

#[tokio::test]
#[serial]
async fn given_a_bridge_user_without_manage_servers_when_listing_groups_should_find_them_by_scanning()
 {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let raw = iggy_server::raw_client(&server).await;
    create_user(
        &raw,
        "gateway-c",
        GlobalPermissions {
            read_streams: true,
            read_topics: true,
            ..GlobalPermissions::default()
        },
    )
    .await;
    let first = gateway(server.test_config()).await;
    commit(&first, "orders-app", "orders").await;

    let scanning = gateway_as(&server, "gateway-c").await;
    let (groups, _) = list_until(&scanning, ABOUT_A_SECOND * 4, |groups| !groups.is_empty()).await;

    assert_eq!(groups, ["orders-app"]);
    let bridge = scanning.bridge.as_deref().expect("a bridge");
    assert_eq!(bridge.offset_group_source(), OffsetGroupSource::Scans);
}

#[tokio::test]
#[serial]
async fn given_changed_permissions_of_a_user_when_following_should_show_them_on_the_board() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let raw = iggy_server::raw_client(&server).await;
    let alice = create_user(&raw, "alice", GlobalPermissions::default()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect");
    let board = bridge.permission_board();
    let manages_topics = |access: Option<UserAccess>| match access {
        Some(UserAccess::Active(permissions)) => Some(permissions.manage_topics),
        _ => None,
    };
    let known = wait_for(|| manages_topics(board.access("alice")).is_some()).await;

    raw.update_permissions(
        &Identifier::numeric(alice).expect("a user id"),
        Some(Permissions {
            global: GlobalPermissions {
                manage_topics: true,
                ..GlobalPermissions::default()
            },
            streams: None,
        }),
    )
    .await
    .expect("grant manage_topics");
    let granted = wait_for(|| manages_topics(board.access("alice")) == Some(true)).await;
    raw.delete_user(&Identifier::numeric(alice).expect("a user id"))
        .await
        .expect("delete alice");
    let revoked = wait_for(|| board.access("alice") == Some(UserAccess::Revoked)).await;

    assert!(known && granted && revoked);
    bridge.close().await.expect("the bridge closes");
}

/// An open connection takes the permission changes of its Iggy user, and closes once the user is
/// deleted, as Iggy ends that user's sessions.
#[tokio::test]
#[serial]
async fn given_a_sasl_connection_when_its_user_changes_should_take_the_change_and_close_on_delete()
{
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let raw = iggy_server::raw_client(&server).await;
    let alice = create_user(&raw, "alice", GlobalPermissions::default()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect");
    let address = server.test_config().address;
    let config = GatewayConfig {
        listeners: vec![server::listener(SecurityProtocol::SaslPlaintext)],
        ..GatewayConfig::default()
    };
    let (addresses, shutdown) = server::spawn_gateway(
        |config| {
            KafkaGateway::new(config)
                .with_authenticator(Arc::new(IggyAuthenticator::new(address)))
                .with_bridge(Some(Arc::new(bridge)))
        },
        config,
    );
    let mut stream = TcpStream::connect(addresses[0]).await.expect("connect");
    authenticate(&mut stream, "alice").await;
    let before = acl_resources(&mut stream).await;

    raw.update_permissions(
        &Identifier::numeric(alice).expect("a user id"),
        Some(Permissions {
            global: GlobalPermissions {
                read_topics: true,
                ..GlobalPermissions::default()
            },
            streams: None,
        }),
    )
    .await
    .expect("grant read_topics");
    let started = Instant::now();
    let mut after = 0;
    while after == 0 && started.elapsed() < ABOUT_A_SECOND * 4 {
        sleep(Duration::from_millis(50)).await;
        after = acl_resources(&mut stream).await;
    }
    raw.delete_user(&Identifier::numeric(alice).expect("a user id"))
        .await
        .expect("delete alice");
    sleep(ABOUT_A_SECOND).await;
    let frame = build_request_frame(API_KEY_DESCRIBE_ACLS, 1, 9, None, &any_acl_filter_body());
    // The connection can close before this write lands, so a failed write is the same answer.
    let _ = stream.write_all(&frame).await;
    let read = read_byte_with_timeout(&mut stream, Duration::from_secs(5)).await;

    assert_eq!(before, 0, "alice holds nothing at first");
    assert!(after > 0, "the grant shows on the open connection");
    assert!(
        matches!(read, ByteRead::Closed),
        "the connection of a deleted user closes, got {read:?}"
    );
    drop(shutdown);
}

/// Polls `done` every 50 ms for up to four times [`ABOUT_A_SECOND`]. Whether it held.
async fn wait_for(done: impl Fn() -> bool) -> bool {
    let started = Instant::now();
    while !done() {
        if started.elapsed() >= ABOUT_A_SECOND * 4 {
            return false;
        }
        sleep(Duration::from_millis(50)).await;
    }
    true
}

const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;
const API_KEY_DESCRIBE_ACLS: i16 = 29;

/// Signs in on `stream` as `username` with SASL PLAIN.
async fn authenticate(stream: &mut TcpStream, username: &str) {
    let mut handshake = BytesMut::new();
    handshake.put_i16(5);
    handshake.extend_from_slice(b"PLAIN");
    let body = send(stream, API_KEY_SASL_HANDSHAKE, 1, 1, &handshake).await;
    assert_eq!(i16::from_be_bytes([body[0], body[1]]), ERROR_NONE, "PLAIN");

    let mut token = vec![0u8];
    token.extend_from_slice(username.as_bytes());
    token.push(0);
    token.extend_from_slice(PASSWORD.as_bytes());
    let mut authenticate = BytesMut::new();
    authenticate.put_i32(i32::try_from(token.len()).expect("a short token"));
    authenticate.extend_from_slice(&token);
    let body = send(stream, API_KEY_SASL_AUTHENTICATE, 1, 2, &authenticate).await;
    assert_eq!(i16::from_be_bytes([body[0], body[1]]), ERROR_NONE, "login");
}

/// The resources of a v1 `DescribeAcls` that matches every binding of the principal.
async fn acl_resources(stream: &mut TcpStream) -> i32 {
    let body = send(stream, API_KEY_DESCRIBE_ACLS, 1, 3, &any_acl_filter_body()).await;
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    assert_eq!(
        d.read_i16().expect("error_code"),
        ERROR_NONE,
        "DescribeAcls"
    );
    assert_eq!(d.read_i16().expect("error_message"), -1, "no error message");
    d.read_i32().expect("resources")
}

/// A v1 `DescribeAcls` filter that matches everything.
fn any_acl_filter_body() -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i8(1); // resource_type_filter = ANY
    buf.put_i16(-1); // resource_name_filter = null
    buf.put_i8(1); // pattern_type_filter = ANY
    buf.put_i16(-1); // principal_filter = null
    buf.put_i16(-1); // host_filter = null
    buf.put_i8(1); // operation = ANY
    buf.put_i8(1); // permission_type = ANY
    buf.freeze()
}

async fn send(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Bytes {
    let frame = build_request_frame(
        api_key,
        api_version,
        correlation_id,
        Some("metadata-changes-test"),
        body,
    );
    stream.write_all(&frame).await.expect("write request");
    let payload = tcp::read_response_frame(stream, TEST_MAX_FRAME_SIZE).await;
    let (echoed, response) = parse_response_payload(api_key, api_version, payload);
    assert_eq!(echoed, correlation_id, "correlation id must be echoed");
    response
}
