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

//! The group admin APIs against a real `iggy-server` process. `ListGroups` and `DescribeGroups`
//! report a group with no members on the coordinator whose offsets are in Iggy, as Kafka keeps
//! such a group and reports it as `Empty`. `DeleteGroups` and `OffsetDelete` delete those offsets.
//! Responses are decoded by hand for the reason that `create_topics_real_bridge_tests.rs` gives.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/wire.rs"]
mod wire;

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iggy::prelude::{
    Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier, Permissions, StreamClient,
    StreamPermissions, TopicClient, UserClient, UserCreateOptions, UserStatus,
};
use secrecy::SecretString;
use serial_test::serial;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::{IggyBridge, IggyBridgeConfig, TopicMapping, TopicOverride};
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{
    API_KEY_DELETE_GROUPS, API_KEY_DESCRIBE_GROUPS, API_KEY_JOIN_GROUP, API_KEY_LEAVE_GROUP,
    API_KEY_LIST_GROUPS, API_KEY_OFFSET_COMMIT, API_KEY_OFFSET_DELETE, API_KEY_OFFSET_FETCH,
    ERROR_COORDINATOR_LOAD_IN_PROGRESS, ERROR_GROUP_AUTHORIZATION_FAILED, ERROR_GROUP_ID_NOT_FOUND,
    ERROR_GROUP_SUBSCRIBED_TO_TOPIC, ERROR_NON_EMPTY_GROUP, ERROR_NONE,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, handle_request_bounded,
};

use codec::Decoder;
use iggy_server::TestServer;
use wire::{
    JoinGroupParams, build_delete_groups_request, build_describe_groups_request,
    build_join_group_request, build_leave_group_request, build_list_groups_request,
    build_offset_commit_partition_request, build_offset_commit_request,
    build_offset_delete_request, build_offset_fetch_request,
};

const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;
const COMMIT_VERSION: i16 = 8;
const FETCH_VERSION: i16 = 8;
/// The first flexible version.
const DELETE_GROUPS_VERSION: i16 = 2;
/// The first version with both filters.
const LIST_VERSION: i16 = 5;
/// The last version without `error_message`, so a missing group is `Dead` with error 0.
const DESCRIBE_VERSION: i16 = 5;
/// v3 admits a member without the `MEMBER_ID_REQUIRED` round trip.
const JOIN_VERSION: i16 = 3;
const LEAVE_VERSION: i16 = 1;
const GROUP: &str = "orders-app";

/// One `ListGroups` entry: `(group_id, protocol_type, state, group_type)`.
type Listed = (String, String, String, String);

/// One `DescribeGroups` entry with the fields an offsets-only group sets.
#[derive(Debug, PartialEq, Eq)]
struct Described {
    error: i16,
    error_message: Option<String>,
    group_id: String,
    state: String,
    protocol_type: String,
    protocol_data: String,
    members: usize,
}

async fn connected_state(server: &TestServer) -> GatewayState {
    state_with(server.test_config()).await
}

async fn state_with(config: IggyBridgeConfig) -> GatewayState {
    state_with_groups(config, groups_config()).await
}

/// A new group's first join completes at once, so a test can hold a live member.
fn groups_config() -> GroupCoordinatorConfig {
    GroupCoordinatorConfig {
        initial_rebalance_delay: Duration::ZERO,
        ..GroupCoordinatorConfig::default()
    }
}

async fn state_with_groups(
    config: IggyBridgeConfig,
    groups: GroupCoordinatorConfig,
) -> GatewayState {
    let bridge = IggyBridge::connect(config)
        .await
        .expect("bridge should connect to a ready server");
    GatewayState::new(
        Some(Arc::new(bridge)),
        TEST_MAX_FRAME_SIZE,
        0,
        GroupCoordinator::new(groups, CancellationToken::new()),
    )
}

/// A gateway whose bridge reaches `server` through `relay`. Its offset deadline is 500 ms, half
/// the shortest session, and so is the deadline of a deletion. A test outwaits it while the relay
/// holds a call.
async fn state_through(server: &TestServer, relay: &PausingRelay) -> GatewayState {
    let mut config = server.test_config();
    config.address.clone_from(&relay.address);
    let groups = GroupCoordinatorConfig {
        min_session_timeout: Duration::from_secs(1),
        deletion_hold: Duration::from_millis(500),
        ..groups_config()
    };
    state_with_groups(config, groups).await
}

/// Past the 30 s after which the SDK gives up on a call. A write that Iggy has not answered by
/// then ends in doubt: it can still land.
const PAST_SDK_DEADLINE: Duration = Duration::from_secs(31);

/// Past the 45 s for which a write waits for an earlier write in doubt on its client to settle.
const PAST_SLOT_LIMIT: Duration = Duration::from_secs(48);

/// A TCP relay to a server that numbers its connections in the order they open. It can hold what
/// the client sends on one of them, so a call there waits, unanswered, until it is released.
struct PausingRelay {
    address: String,
    holds: Arc<Mutex<Holds>>,
}

#[derive(Default)]
struct Holds {
    /// Per connection, in the order they opened: `true` holds what its client sends.
    switches: Vec<watch::Sender<bool>>,
    /// Whether a connection that opens now starts held.
    new: bool,
}

impl PausingRelay {
    async fn start(server: &TestServer) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the relay");
        let address = listener.local_addr().expect("relay address").to_string();
        let upstream = server.test_config().address;
        let holds = Arc::new(Mutex::new(Holds::default()));
        let connections = Arc::clone(&holds);
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                let paused = {
                    let mut holds = connections.lock().expect("relay connections");
                    let (switch, paused) = watch::channel(holds.new);
                    holds.switches.push(switch);
                    paused
                };
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    if let Ok(outbound) = TcpStream::connect(&upstream).await {
                        relay(inbound, outbound, paused).await;
                    }
                });
            }
        });
        Self { address, holds }
    }

    /// The connections opened so far.
    fn opened(&self) -> usize {
        self.holds.lock().expect("relay connections").switches.len()
    }

    /// Holds what the client of connection `index` sends from now on.
    fn hold(&self, index: usize) {
        self.holds.lock().expect("relay connections").switches[index].send_replace(true);
    }

    /// Sends on what connection `index` held.
    fn release(&self, index: usize) {
        self.holds.lock().expect("relay connections").switches[index].send_replace(false);
    }

    /// Holds what the client of each connection that opens from now on sends, so a client that
    /// reconnects cannot get past a hold.
    fn hold_new(&self) {
        self.holds.lock().expect("relay connections").new = true;
    }

    /// Sends on what every connection held, and holds no connection that opens from now on.
    fn release_all(&self) {
        let mut holds = self.holds.lock().expect("relay connections");
        holds.new = false;
        for switch in &holds.switches {
            switch.send_replace(false);
        }
    }
}

/// Copies both ways between a client and the server. What the client sends waits while `paused`
/// is `true`.
async fn relay(inbound: TcpStream, outbound: TcpStream, mut paused: watch::Receiver<bool>) {
    let (mut from_client, mut to_client) = inbound.into_split();
    let (mut from_server, mut to_server) = outbound.into_split();
    let requests = async move {
        let mut buf = vec![0; 64 * 1024];
        loop {
            let read = match from_client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            if paused.wait_for(|paused| !*paused).await.is_err()
                || to_server.write_all(&buf[..read]).await.is_err()
            {
                break;
            }
        }
    };
    let replies = async move {
        let _ = tokio::io::copy(&mut from_server, &mut to_client).await;
    };
    tokio::join!(requests, replies);
}

async fn seed_topic(server: &TestServer, topic: &str) {
    seed_topic_with(server, topic, 1).await;
}

async fn seed_topic_with(server: &TestServer, topic: &str, partitions: u32) {
    IggyBridge::connect(server.test_config())
        .await
        .expect("seed bridge should connect")
        .ensure_stream_and_topic(topic, partitions)
        .await
        .expect("seed the topic");
}

/// Runs `request` again while it answers `COORDINATOR_LOAD_IN_PROGRESS`, which clients retry, for
/// up to 30 s. The last answer.
async fn retried<T, F, R>(mut request: R, code_of: impl Fn(&T) -> i16) -> T
where
    F: Future<Output = T>,
    R: FnMut() -> F,
{
    let give_up = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let answer = request().await;
        if code_of(&answer) != ERROR_COORDINATOR_LOAD_IN_PROGRESS
            || tokio::time::Instant::now() >= give_up
        {
            return answer;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Commits `offset` for partition 0 of `topic` outside the group protocol, as admin tools do.
async fn commit(state: &GatewayState, group: &str, topic: &str, offset: i64) {
    let body = build_offset_commit_request(COMMIT_VERSION, group, topic, offset);
    assert_eq!(commit_body(state, body).await, ERROR_NONE, "the commit");
}

/// Commits `offset` for `partition` of `topic` outside the group protocol. The partition's code.
async fn commit_partition(
    state: &GatewayState,
    group: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) -> i16 {
    let body =
        build_offset_commit_partition_request(COMMIT_VERSION, group, topic, partition, offset);
    commit_body(state, body).await
}

async fn commit_body(state: &GatewayState, body: bytes::Bytes) -> i16 {
    let response = handle_request_bounded(state, API_KEY_OFFSET_COMMIT, COMMIT_VERSION, body)
        .await
        .expect_response("OffsetCommit answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    assert_eq!(d.read_varint().expect("topics"), 2, "one topic");
    let _name = d.read_compact_nullable_string().expect("topic name");
    assert_eq!(d.read_varint().expect("partitions"), 2, "one partition");
    let _index = d.read_i32().expect("partition_index");
    d.read_i16().expect("error_code")
}

/// Whether the Iggy group that holds the offsets of [`GROUP`] is on `topic`, asked of Iggy itself.
async fn offset_group_on(server: &TestServer, topic: &str) -> bool {
    let raw = iggy_server::raw_client(server).await;
    let iggy_id = |name: &str| Identifier::named(name).expect("valid Iggy name");
    raw.get_consumer_group(
        &iggy_id("kafka"),
        &iggy_id(topic),
        &iggy_id("kafka.cg.orders-app"),
    )
    .await
    .expect("get the Iggy group")
    .is_some()
}

/// The offset of [`GROUP`] for `partition` of `topic`, asked of Iggy itself. `None` when there is
/// no offset or no group.
async fn stored_offset(server: &TestServer, topic: &str, partition: u32) -> Option<u64> {
    let raw = iggy_server::raw_client(server).await;
    let iggy_id = |name: &str| Identifier::named(name).expect("valid Iggy name");
    let consumer = Consumer::external_group(iggy_id("kafka.cg.orders-app"));
    raw.get_consumer_offset(
        &consumer,
        &iggy_id("kafka"),
        &iggy_id(topic),
        Some(partition),
    )
    .await
    .ok()
    .flatten()
    .map(|offset| offset.stored_offset)
}

/// The top-level error and every group of a v5 `ListGroups` answer.
async fn list(state: &GatewayState, states: &[&str], group_types: &[&str]) -> (i16, Vec<Listed>) {
    let body = build_list_groups_request(LIST_VERSION, states, group_types);
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
        groups.push((string(), string(), string(), string()));
        d.read_tagged_fields().expect("group tagged fields");
    }
    d.read_tagged_fields().expect("tagged fields");
    assert_eq!(d.remaining(), 0, "ListGroups trailing bytes");
    (error, groups)
}

async fn describe(state: &GatewayState, version: i16, groups: &[&str]) -> Vec<Described> {
    let body = build_describe_groups_request(version, groups, false);
    let response = handle_request_bounded(state, API_KEY_DESCRIBE_GROUPS, version, body)
        .await
        .expect_response("DescribeGroups answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let mut described = Vec::new();
    for _ in 1..d.read_varint().expect("groups") {
        let error = d.read_i16().expect("error_code");
        let error_message = if version >= 6 {
            d.read_compact_nullable_string().expect("error_message")
        } else {
            None
        };
        let mut string = || {
            d.read_compact_nullable_string()
                .expect("a string")
                .expect("not null")
        };
        let (group_id, state, protocol_type, protocol_data) =
            (string(), string(), string(), string());
        let members = usize::try_from(d.read_varint().expect("members") - 1).expect("a count");
        assert_eq!(members, 0, "only groups with no members are described here");
        let _authorized_operations = d.read_i32().expect("authorized_operations");
        d.read_tagged_fields().expect("group tagged fields");
        described.push(Described {
            error,
            error_message,
            group_id,
            state,
            protocol_type,
            protocol_data,
            members,
        });
    }
    d.read_tagged_fields().expect("tagged fields");
    assert_eq!(d.remaining(), 0, "DescribeGroups trailing bytes");
    described
}

/// Joins a member to `group` and returns its id. The group is then `CompletingRebalance`.
async fn join(state: &GatewayState, group: &str) -> String {
    join_with(
        state,
        &JoinGroupParams {
            group_id: group,
            ..JoinGroupParams::default()
        },
    )
    .await
}

/// [`join`] with `params`.
async fn join_with(state: &GatewayState, params: &JoinGroupParams<'_>) -> String {
    let body = build_join_group_request(JOIN_VERSION, params);
    let response = handle_request_bounded(state, API_KEY_JOIN_GROUP, JOIN_VERSION, body)
        .await
        .expect_response("JoinGroup answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    assert_eq!(d.read_i16().expect("error_code"), ERROR_NONE, "the join");
    let _generation_id = d.read_i32().expect("generation_id");
    let _protocol_name = d.read_nullable_string().expect("protocol_name");
    let _leader = d.read_nullable_string().expect("leader");
    d.read_nullable_string()
        .expect("member_id")
        .expect("a member id")
}

async fn leave(state: &GatewayState, group: &str, member_id: &str) {
    let body = build_leave_group_request(LEAVE_VERSION, group, &[(member_id, None, None)]);
    let response = handle_request_bounded(state, API_KEY_LEAVE_GROUP, LEAVE_VERSION, body)
        .await
        .expect_response("LeaveGroup answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    assert_eq!(d.read_i16().expect("error_code"), ERROR_NONE, "the leave");
}

/// The committed offset of partition 0 of `topic`, -1 when there is none.
async fn committed(state: &GatewayState, group: &str, topic: &str) -> i64 {
    let body = build_offset_fetch_request(FETCH_VERSION, group, Some(topic));
    let response = handle_request_bounded(state, API_KEY_OFFSET_FETCH, FETCH_VERSION, body)
        .await
        .expect_response("OffsetFetch answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    assert_eq!(d.read_varint().expect("groups"), 2, "one group");
    let _group_id = d.read_compact_nullable_string().expect("group_id");
    assert_eq!(d.read_varint().expect("topics"), 2, "one topic");
    let _name = d.read_compact_nullable_string().expect("topic name");
    assert_eq!(d.read_varint().expect("partitions"), 2, "one partition");
    let _index = d.read_i32().expect("partition_index");
    let offset = d.read_i64().expect("committed_offset");
    let _leader_epoch = d.read_i32().expect("committed_leader_epoch");
    let _metadata = d.read_compact_nullable_string().expect("metadata");
    assert_eq!(
        d.read_i16().expect("error_code"),
        ERROR_NONE,
        "the partition"
    );
    d.read_tagged_fields().expect("partition tagged fields");
    d.read_tagged_fields().expect("topic tagged fields");
    assert_eq!(
        d.read_i16().expect("group error_code"),
        ERROR_NONE,
        "the group"
    );
    offset
}

/// `(group_id, error_code)` per group of a v2 `DeleteGroups` answer.
async fn delete_groups(state: &GatewayState, groups: &[&str]) -> Vec<(String, i16)> {
    let body = build_delete_groups_request(DELETE_GROUPS_VERSION, groups);
    let response =
        handle_request_bounded(state, API_KEY_DELETE_GROUPS, DELETE_GROUPS_VERSION, body)
            .await
            .expect_response("DeleteGroups answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let mut results = Vec::new();
    for _ in 1..d.read_varint().expect("results") {
        let group_id = d
            .read_compact_nullable_string()
            .expect("group_id")
            .expect("not null");
        results.push((group_id, d.read_i16().expect("error_code")));
        d.read_tagged_fields().expect("result tagged fields");
    }
    d.read_tagged_fields().expect("tagged fields");
    assert_eq!(d.remaining(), 0, "DeleteGroups trailing bytes");
    results
}

/// The top-level error and `(topic, partition, error_code)` per partition of an `OffsetDelete`.
async fn delete_offsets(
    state: &GatewayState,
    group: &str,
    topics: &[(&str, &[i32])],
) -> (i16, Vec<(String, i32, i16)>) {
    let body = build_offset_delete_request(group, topics);
    let response = handle_request_bounded(state, API_KEY_OFFSET_DELETE, 0, body)
        .await
        .expect_response("OffsetDelete answers");
    let mut d = Decoder::new(response);
    let error = d.read_i16().expect("error_code");
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let mut partitions = Vec::new();
    for _ in 0..d.read_i32().expect("topics") {
        let name = d
            .read_nullable_string()
            .expect("topic name")
            .expect("not null");
        for _ in 0..d.read_i32().expect("partitions") {
            let index = d.read_i32().expect("partition_index");
            partitions.push((name.clone(), index, d.read_i16().expect("error_code")));
        }
    }
    assert_eq!(d.remaining(), 0, "OffsetDelete trailing bytes");
    (error, partitions)
}

/// A consumer's subscription to `topics`, as the Java consumer sends it in `JoinGroup`.
fn subscription(topics: &[&str]) -> Vec<u8> {
    let mut bytes = 0_i16.to_be_bytes().to_vec();
    let count = i32::try_from(topics.len()).expect("a few topics");
    bytes.extend_from_slice(&count.to_be_bytes());
    for topic in topics {
        let len = i16::try_from(topic.len()).expect("a short name");
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(topic.as_bytes());
    }
    bytes.extend_from_slice(&(-1_i32).to_be_bytes()); // user_data: null
    bytes
}

fn listed(group: &str, state: &str) -> Listed {
    let group_type = "classic".to_string();
    (
        group.to_string(),
        "consumer".to_string(),
        state.to_string(),
        group_type,
    )
}

fn offsets_only(group: &str) -> Described {
    Described {
        error: ERROR_NONE,
        error_message: None,
        group_id: group.to_string(),
        state: "Empty".to_string(),
        protocol_type: "consumer".to_string(),
        protocol_data: String::new(),
        members: 0,
    }
}

/// A group whose consumers committed and left has no members here. Kafka still lists and
/// describes it, as `Empty`, while it holds offsets.
#[tokio::test]
#[serial]
async fn given_offsets_and_no_members_list_and_describe_should_report_an_empty_group() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 7).await;

    assert_eq!(
        list(&state, &[], &[]).await,
        (ERROR_NONE, vec![listed(GROUP, "Empty")])
    );
    assert_eq!(
        describe(&state, DESCRIBE_VERSION, &[GROUP, "never-committed"]).await,
        vec![
            offsets_only(GROUP),
            Described {
                state: "Dead".to_string(),
                protocol_type: String::new(),
                ..offsets_only("never-committed")
            },
        ]
    );
    assert_eq!(
        describe(&state, 6, &[GROUP, "never-committed"]).await,
        vec![
            offsets_only(GROUP),
            Described {
                error: ERROR_GROUP_ID_NOT_FOUND,
                error_message: Some("Group never-committed not found.".to_string()),
                state: "Dead".to_string(),
                protocol_type: String::new(),
                ..offsets_only("never-committed")
            },
        ]
    );
}

/// A live group is listed once, with its live state. After its last member leaves, the group is
/// `Empty`, because its offsets are still in Iggy.
#[tokio::test]
#[serial]
async fn given_a_live_group_with_offsets_should_list_it_once_and_empty_after_it_leaves() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 7).await;

    let member_id = join(&state, GROUP).await;
    assert_eq!(
        list(&state, &[], &[]).await,
        (ERROR_NONE, vec![listed(GROUP, "CompletingRebalance")])
    );

    leave(&state, GROUP, &member_id).await;
    assert_eq!(
        list(&state, &[], &[]).await,
        (ERROR_NONE, vec![listed(GROUP, "Empty")])
    );
    assert_eq!(
        describe(&state, DESCRIBE_VERSION, &[GROUP]).await,
        vec![offsets_only(GROUP)]
    );
}

/// The filters apply to a group that only holds offsets as to any other: its state is `Empty` and
/// its type is `classic`.
#[tokio::test]
#[serial]
async fn given_filters_should_keep_an_offsets_only_group_only_when_they_match() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 7).await;
    let empty = (ERROR_NONE, vec![listed(GROUP, "Empty")]);

    assert_eq!(list(&state, &[" empty "], &[]).await, empty);
    assert_eq!(list(&state, &[], &["Classic"]).await, empty);
    assert_eq!(list(&state, &["Stable"], &[]).await, (ERROR_NONE, vec![]));
    assert_eq!(list(&state, &[], &["consumer"]).await, (ERROR_NONE, vec![]));
}

/// Groups come from a scan that answers for a while. A commit through this gateway adds its group
/// at once, so a group does not wait for the next scan.
#[tokio::test]
#[serial]
async fn given_a_commit_after_the_last_scan_should_list_its_group_at_once() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, "first-app", "orders", 1).await;
    assert_eq!(
        list(&state, &[], &[]).await,
        (ERROR_NONE, vec![listed("first-app", "Empty")])
    );

    commit(&state, "second-app", "orders", 2).await;

    assert_eq!(
        list(&state, &[], &[]).await,
        (
            ERROR_NONE,
            vec![listed("first-app", "Empty"), listed("second-app", "Empty")]
        )
    );
}

/// A gateway that just started has no memory of the groups. Its scan finds the groups that another
/// gateway, or an earlier run, committed for, on every topic.
#[tokio::test]
#[serial]
async fn given_a_new_gateway_should_find_the_groups_of_every_topic() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    for topic in ["orders", "payments"] {
        seed_topic(&server, topic).await;
    }
    let first = connected_state(&server).await;
    commit(&first, "first-app", "orders", 1).await;
    commit(&first, "second-app", "payments", 2).await;
    commit(&first, "first-app", "payments", 3).await;
    drop(first);

    let restarted = connected_state(&server).await;

    assert_eq!(
        list(&restarted, &[], &[]).await,
        (
            ERROR_NONE,
            vec![listed("first-app", "Empty"), listed("second-app", "Empty")]
        )
    );
}

/// Only an Iggy group named `kafka.cg.<group>` holds Kafka offsets. A native Iggy group on the
/// same topic is not a Kafka group.
#[tokio::test]
#[serial]
async fn given_a_native_iggy_group_should_not_list_it() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let raw = iggy_server::raw_client(&server).await;
    let iggy_id = |name: &str| Identifier::named(name).expect("valid Iggy name");
    raw.create_consumer_group(&iggy_id("kafka"), &iggy_id("orders"), "native-app")
        .await
        .expect("create a native Iggy group");
    let state = connected_state(&server).await;

    assert_eq!(list(&state, &[], &[]).await, (ERROR_NONE, vec![]));
}

/// While the first scan runs, the gateway cannot tell `Empty` from `Dead`, so it answers
/// `COORDINATOR_LOAD_IN_PROGRESS`, which clients retry. A failed scan does not leave it there: the
/// groups it holds still answer, as before it knew of the others, so `--list` works with Iggy down.
#[tokio::test]
#[serial]
async fn given_iggy_unreachable_should_answer_from_the_live_groups_once_a_scan_failed() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    join(&state, "live-app").await;
    drop(server);

    assert_eq!(
        list(&state, &["Stable"], &[]).await,
        (ERROR_NONE, vec![]),
        "a filter that leaves out Empty needs no scan"
    );
    assert_eq!(
        retried(|| list(&state, &[], &[]), |(error, _)| *error).await,
        (ERROR_NONE, vec![listed("live-app", "CompletingRebalance")])
    );
    assert_eq!(
        describe(&state, DESCRIBE_VERSION, &[GROUP]).await,
        vec![Described {
            state: "Dead".to_string(),
            protocol_type: String::new(),
            ..offsets_only(GROUP)
        }],
        "a group that is not here is Dead, as without a bridge"
    );
}

/// A commit that its request gave up on runs on, and can still land. So `DeleteGroups` waits for
/// it. Deleting first would let the late commit find no Iggy group, create one, and store its
/// stale offset there, after `DeleteGroups` answered that the group is gone.
#[tokio::test]
#[serial]
async fn given_a_commit_past_its_deadline_delete_groups_should_wait_for_it_to_end() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic_with(&server, "orders", 2).await;
    let relay = PausingRelay::start(&server).await;
    let state = state_through(&server, &relay).await;
    let lane = relay.opened();
    // Partition 1 commits on a lane of its own. The deletion uses the lane of partition 0.
    assert_eq!(
        commit_partition(&state, GROUP, "orders", 1, 5).await,
        ERROR_NONE
    );
    assert_eq!(relay.opened(), lane + 1, "the commit lane connected");
    relay.hold(lane);

    assert_eq!(
        commit_partition(&state, GROUP, "orders", 1, 9).await,
        ERROR_COORDINATOR_LOAD_IN_PROGRESS,
        "the commit runs past its deadline"
    );
    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_COORDINATOR_LOAD_IN_PROGRESS)],
        "the late commit still runs"
    );

    relay.release(lane);
    assert_eq!(
        retried(|| delete_groups(&state, &[GROUP]), |answer| answer[0].1).await,
        vec![(GROUP.to_string(), ERROR_NONE)]
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !offset_group_on(&server, "orders").await,
        "no late commit brings the group back"
    );
}

/// A deletion that its request gave up on runs on, and Iggy can still delete the group. Until it
/// ends, the group takes no commit. A commit that landed first would be acknowledged, then deleted
/// with the group.
#[tokio::test]
#[serial]
async fn given_a_group_deletion_past_its_deadline_commits_should_wait_for_it_to_end() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic_with(&server, "orders", 2).await;
    let relay = PausingRelay::start(&server).await;
    let state = state_through(&server, &relay).await;
    let lane = relay.opened();
    // The deletion of the group on `orders` runs on the commit lane of partition 0.
    assert_eq!(
        commit_partition(&state, GROUP, "orders", 0, 5).await,
        ERROR_NONE
    );
    assert_eq!(relay.opened(), lane + 1, "the commit lane connected");
    relay.hold(lane);

    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_COORDINATOR_LOAD_IN_PROGRESS)],
        "the deletion runs past its deadline"
    );
    assert_eq!(
        commit_partition(&state, GROUP, "orders", 1, 7).await,
        ERROR_COORDINATOR_LOAD_IN_PROGRESS,
        "the deletion still runs"
    );

    relay.release(lane);
    assert_eq!(
        retried(
            || commit_partition(&state, GROUP, "orders", 1, 7),
            |code| *code
        )
        .await,
        ERROR_NONE
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        stored_offset(&server, "orders", 1).await,
        Some(7),
        "no late deletion takes the acknowledged offset"
    );
    assert_eq!(
        stored_offset(&server, "orders", 0).await,
        None,
        "the deletion took the older offset"
    );
}

/// A gateway with the scan client and the commit lanes of partitions 0 and 1 of `orders`
/// connected through `relay`, and the connection of each lane. The scan client lists the groups
/// to delete. A deletion runs on the lane of partition 0.
async fn lanes_through(server: &TestServer, relay: &PausingRelay) -> (GatewayState, usize, usize) {
    seed_topic_with(server, "orders", 2).await;
    let state = state_through(server, relay).await;
    let (error, _) = retried(|| list(&state, &[], &[]), |(error, _)| *error).await;
    assert_eq!(error, ERROR_NONE, "the scan client connected");
    let first = relay.opened();
    assert_eq!(
        commit_partition(&state, GROUP, "orders", 0, 5).await,
        ERROR_NONE
    );
    assert_eq!(
        commit_partition(&state, GROUP, "orders", 1, 6).await,
        ERROR_NONE
    );
    assert_eq!(relay.opened(), first + 2, "each lane connected");
    (state, first, first + 1)
}

/// A delete that Iggy has not answered by the SDK's deadline ends in doubt, and Iggy can still
/// run it. So the group takes no commit until a replay proves what the delete did. A commit
/// acknowledged before would go with the group.
#[tokio::test]
#[serial]
async fn given_a_group_deletion_in_doubt_commits_should_wait_until_its_outcome_is_proved() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let relay = PausingRelay::start(&server).await;
    let (state, deletion_lane, _) = lanes_through(&server, &relay).await;
    relay.hold(deletion_lane);
    relay.hold_new();

    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_COORDINATOR_LOAD_IN_PROGRESS)]
    );
    tokio::time::sleep(PAST_SDK_DEADLINE).await;
    assert_eq!(
        commit_partition(&state, GROUP, "orders", 1, 7).await,
        ERROR_COORDINATOR_LOAD_IN_PROGRESS,
        "the delete is in doubt"
    );

    relay.release_all();
    assert_eq!(
        retried(
            || commit_partition(&state, GROUP, "orders", 1, 7),
            |code| *code
        )
        .await,
        ERROR_NONE
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        stored_offset(&server, "orders", 1).await,
        Some(7),
        "no late delete takes the acknowledged offset"
    );
    assert_eq!(
        stored_offset(&server, "orders", 0).await,
        None,
        "the deletion took the older offset"
    );
}

/// A commit that Iggy has not answered by the SDK's deadline ends in doubt, and Iggy can still
/// run it. So `DeleteGroups` waits until a replay proves what the commit did. Deleting first would
/// let the late commit bring back an offset of the deleted group.
#[tokio::test]
#[serial]
async fn given_a_commit_in_doubt_delete_groups_should_wait_until_its_outcome_is_proved() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let relay = PausingRelay::start(&server).await;
    let (state, _, commit_lane) = lanes_through(&server, &relay).await;
    relay.hold(commit_lane);
    relay.hold_new();

    assert_eq!(
        commit_partition(&state, GROUP, "orders", 1, 9).await,
        ERROR_COORDINATOR_LOAD_IN_PROGRESS,
        "the commit runs past its deadline"
    );
    tokio::time::sleep(PAST_SDK_DEADLINE).await;
    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_COORDINATOR_LOAD_IN_PROGRESS)],
        "the commit is in doubt"
    );

    relay.release_all();
    assert_eq!(
        retried(|| delete_groups(&state, &[GROUP]), |answer| answer[0].1).await,
        vec![(GROUP.to_string(), ERROR_NONE)]
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !offset_group_on(&server, "orders").await,
        "no late commit brings the group back"
    );
}

/// A deletion that ends after its request answered 14 leaves nothing for the client's retry to
/// find. The retry answers 0, as Kafka answers the deletion the client asked for. A later request
/// answers `GROUP_ID_NOT_FOUND`, as in Kafka.
#[tokio::test]
#[serial]
async fn given_a_group_deletion_that_ended_after_its_request_answered_the_retry_should_answer_none()
{
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let relay = PausingRelay::start(&server).await;
    let (state, deletion_lane, _) = lanes_through(&server, &relay).await;
    relay.hold(deletion_lane);

    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_COORDINATOR_LOAD_IN_PROGRESS)]
    );
    relay.release(deletion_lane);
    let raw = iggy_server::raw_client(&server).await;
    let iggy_id = |name: &str| Identifier::named(name).expect("valid Iggy name");
    let gone = async {
        while raw
            .get_consumer_group(
                &iggy_id("kafka"),
                &iggy_id("orders"),
                &iggy_id("kafka.cg.orders-app"),
            )
            .await
            .expect("get the Iggy group")
            .is_some()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), gone)
        .await
        .expect("the deletion ends");
    // It notes the deletion right after it sees the group gone.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_NONE)],
        "the retry"
    );
    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_GROUP_ID_NOT_FOUND)],
        "a later request"
    );
}

/// A delete waits for an earlier delete in doubt on its client. When that one stays in doubt, the
/// later delete is never sent, so nothing of it can land, and its group takes commits again. A
/// replay of the earlier delete proves only that one.
#[tokio::test]
#[serial]
async fn given_a_delete_that_a_delete_in_doubt_kept_from_iggy_its_group_should_take_commits() {
    const OTHER: &str = "audit-app";
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let relay = PausingRelay::start(&server).await;
    let (state, deletion_lane, _) = lanes_through(&server, &relay).await;
    assert_eq!(
        commit_partition(&state, OTHER, "orders", 0, 3).await,
        ERROR_NONE
    );
    relay.hold(deletion_lane);
    relay.hold_new();

    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_COORDINATOR_LOAD_IN_PROGRESS)]
    );
    tokio::time::sleep(PAST_SDK_DEADLINE).await;
    assert_eq!(
        delete_groups(&state, &[OTHER]).await,
        vec![(OTHER.to_string(), ERROR_COORDINATOR_LOAD_IN_PROGRESS)],
        "the delete waits for the one in doubt"
    );
    tokio::time::sleep(PAST_SLOT_LIMIT).await;
    assert_eq!(
        commit_partition(&state, OTHER, "orders", 1, 4).await,
        ERROR_NONE,
        "the delete that was never sent let go of its group"
    );

    relay.release_all();
    assert_eq!(
        retried(|| delete_groups(&state, &[OTHER]), |answer| answer[0].1).await,
        vec![(OTHER.to_string(), ERROR_NONE)]
    );
}

/// Iggy never reuses a group id within a topic, but a topic deleted and created again numbers its
/// groups from the start. The id that a commit noted then names another group, which stays.
#[tokio::test]
#[serial]
async fn given_a_topic_created_again_delete_groups_should_keep_the_group_that_took_a_noted_id() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 5).await;
    let raw = iggy_server::raw_client(&server).await;
    let iggy_id = |name: &str| Identifier::named(name).expect("valid Iggy name");
    let group_id = async |name: &str| {
        raw.get_consumer_group(&iggy_id("kafka"), &iggy_id("orders"), &iggy_id(name))
            .await
            .expect("get the Iggy group")
            .map(|group| group.id)
    };
    let noted = group_id("kafka.cg.orders-app").await;

    raw.delete_topic(&iggy_id("kafka"), &iggy_id("orders"))
        .await
        .expect("delete the topic");
    seed_topic(&server, "orders").await;
    raw.create_consumer_group(&iggy_id("kafka"), &iggy_id("orders"), "analytics")
        .await
        .expect("create a native Iggy group");
    assert_eq!(group_id("analytics").await, noted, "it took the noted id");

    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_GROUP_ID_NOT_FOUND)]
    );
    assert_eq!(group_id("analytics").await, noted, "it stays");
}

/// Kafka refuses to delete a group while it has members. After they leave, the group goes with
/// its offsets, and the admin APIs no longer find it.
#[tokio::test]
#[serial]
async fn given_a_group_with_members_delete_should_refuse_it_and_delete_it_after_they_leave() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 7).await;
    let member_id = join(&state, GROUP).await;

    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_NON_EMPTY_GROUP)]
    );
    assert_eq!(committed(&state, GROUP, "orders").await, 7);

    leave(&state, GROUP, &member_id).await;
    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_NONE)]
    );
    assert_eq!(committed(&state, GROUP, "orders").await, -1);
    assert_eq!(list(&state, &[], &[]).await, (ERROR_NONE, vec![]));
    assert_eq!(
        describe(&state, DESCRIBE_VERSION, &[GROUP]).await,
        vec![Described {
            state: "Dead".to_string(),
            protocol_type: String::new(),
            ..offsets_only(GROUP)
        }]
    );
    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_GROUP_ID_NOT_FOUND)]
    );
}

/// A group goes from every topic that holds it. Other groups on those topics stay.
#[tokio::test]
#[serial]
async fn given_a_group_on_several_topics_delete_should_remove_it_from_each_and_keep_the_others() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    for topic in ["orders", "payments"] {
        seed_topic(&server, topic).await;
    }
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 1).await;
    commit(&state, GROUP, "payments", 2).await;
    commit(&state, "other-app", "orders", 3).await;

    assert_eq!(
        delete_groups(&state, &[GROUP, "never-committed"]).await,
        vec![
            (GROUP.to_string(), ERROR_NONE),
            ("never-committed".to_string(), ERROR_GROUP_ID_NOT_FOUND),
        ]
    );

    assert_eq!(committed(&state, GROUP, "orders").await, -1);
    assert_eq!(committed(&state, GROUP, "payments").await, -1);
    assert_eq!(committed(&state, "other-app", "orders").await, 3);
    assert_eq!(
        list(&state, &[], &[]).await,
        (ERROR_NONE, vec![listed("other-app", "Empty")])
    );
    let raw = iggy_server::raw_client(&server).await;
    let iggy_id = |name: &str| Identifier::named(name).expect("valid Iggy name");
    for topic in ["orders", "payments"] {
        let found = raw
            .get_consumer_group(
                &iggy_id("kafka"),
                &iggy_id(topic),
                &iggy_id("kafka.cg.orders-app"),
            )
            .await
            .expect("get the Iggy group");
        assert!(found.is_none(), "the Iggy group is gone from {topic}");
    }
}

/// A group with no members here exists while it holds offsets. Another gateway committed for this
/// one after the last scan here, so only asking the topic finds it.
#[tokio::test]
#[serial]
async fn given_a_group_committed_through_another_gateway_since_the_last_scan_delete_offsets_should_find_it()
 {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let here = connected_state(&server).await;
    let other = connected_state(&server).await;
    assert_eq!(
        list(&here, &[], &[]).await,
        (ERROR_NONE, vec![]),
        "the scan here finds no group"
    );
    commit(&other, GROUP, "orders", 5).await;

    assert_eq!(
        delete_offsets(&here, GROUP, &[("orders", &[0])]).await,
        (ERROR_NONE, vec![("orders".to_string(), 0, ERROR_NONE)])
    );
    assert_eq!(committed(&other, GROUP, "orders").await, -1);
}

/// Kafka keeps the offsets of a topic that a member subscribes to, and deletes the others, also
/// for a partition that held none.
#[tokio::test]
#[serial]
async fn given_a_member_subscribed_to_one_topic_delete_offsets_should_delete_only_the_others() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    for topic in ["orders", "payments"] {
        seed_topic(&server, topic).await;
    }
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 5).await;
    commit(&state, GROUP, "payments", 6).await;
    let orders = subscription(&["orders"]);
    let protocols: &[(&str, &[u8])] = &[("range", &orders)];
    let member_id = join_with(
        &state,
        &JoinGroupParams {
            group_id: GROUP,
            protocols,
            ..JoinGroupParams::default()
        },
    )
    .await;

    assert_eq!(
        delete_offsets(
            &state,
            GROUP,
            &[("orders", &[0]), ("payments", &[0, 1]), ("missing", &[0])],
        )
        .await,
        (
            ERROR_NONE,
            vec![
                ("orders".to_string(), 0, ERROR_GROUP_SUBSCRIBED_TO_TOPIC),
                ("payments".to_string(), 0, ERROR_NONE),
                ("payments".to_string(), 1, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
                ("missing".to_string(), 0, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
            ]
        )
    );
    assert_eq!(committed(&state, GROUP, "orders").await, 5);
    assert_eq!(committed(&state, GROUP, "payments").await, -1);

    leave(&state, GROUP, &member_id).await;
    assert_eq!(
        delete_offsets(&state, GROUP, &[("orders", &[0])]).await,
        (ERROR_NONE, vec![("orders".to_string(), 0, ERROR_NONE)])
    );
    assert_eq!(committed(&state, GROUP, "orders").await, -1);
    assert_eq!(
        delete_offsets(&state, "never-committed", &[("orders", &[0])]).await,
        (ERROR_GROUP_ID_NOT_FOUND, vec![])
    );
}

/// A subscription that does not decode keeps every topic, as in Kafka. A group of another
/// protocol keeps every offset while it has members.
#[tokio::test]
#[serial]
async fn given_members_kafka_cannot_read_delete_offsets_should_keep_every_offset() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 5).await;
    let _member_id = join(&state, GROUP).await;
    let _worker_id = join_with(
        &state,
        &JoinGroupParams {
            group_id: "connect-cluster",
            protocol_type: "connect",
            ..JoinGroupParams::default()
        },
    )
    .await;

    assert_eq!(
        delete_offsets(&state, GROUP, &[("orders", &[0])]).await,
        (
            ERROR_NONE,
            vec![("orders".to_string(), 0, ERROR_GROUP_SUBSCRIBED_TO_TOPIC)]
        )
    );
    assert_eq!(committed(&state, GROUP, "orders").await, 5);
    assert_eq!(
        delete_offsets(&state, "connect-cluster", &[("orders", &[0])]).await,
        (ERROR_NON_EMPTY_GROUP, vec![])
    );
}

/// The bridge user may not read an override topic, so `Metadata` leaves it out. A group can hold
/// offsets there from before the bridge user lost access. The gateway cannot delete them, so it
/// deletes nothing and does not answer success.
#[tokio::test]
#[serial]
async fn given_an_override_topic_the_bridge_user_may_not_read_delete_should_refuse_and_keep_every_offset()
 {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let mapping = || {
        let audit = TopicOverride {
            stream: "hidden".to_string(),
            topic: "audit".to_string(),
        };
        TopicMapping::new(
            "kafka".to_string(),
            HashMap::from([("audit".to_string(), audit)]),
        )
        .expect("a valid mapping")
    };
    let root = IggyBridgeConfig {
        topic_mapping: mapping(),
        ..server.test_config()
    };
    IggyBridge::connect(root)
        .await
        .expect("root bridge should connect")
        .ensure_stream_and_topic("audit", 1)
        .await
        .expect("create hidden/audit");
    let raw = iggy_server::raw_client(&server).await;
    let iggy_id = |name: &str| Identifier::named(name).expect("valid Iggy name");
    raw.create_consumer_group(&iggy_id("hidden"), &iggy_id("audit"), "kafka.cg.orders-app")
        .await
        .expect("an Iggy group with offsets on hidden/audit");
    let kafka_stream = raw
        .get_stream(&iggy_id("kafka"))
        .await
        .expect("get the stream")
        .expect("the stream exists")
        .id;
    let read_kafka = StreamPermissions {
        read_stream: true,
        ..StreamPermissions::default()
    };
    raw.create_user(
        "kafka-bridge",
        "kafka-bridge-secret",
        UserStatus::Active,
        Some(Permissions {
            streams: Some(BTreeMap::from([(
                usize::try_from(kafka_stream).expect("a small stream id"),
                read_kafka,
            )])),
            ..Permissions::default()
        }),
        &UserCreateOptions::default(),
    )
    .await
    .expect("create a bridge user that may read only the stream kafka");
    let state = state_with(IggyBridgeConfig {
        username: "kafka-bridge".to_string(),
        password: SecretString::from("kafka-bridge-secret"),
        topic_mapping: mapping(),
        ..server.test_config()
    })
    .await;
    commit(&state, GROUP, "orders", 4).await;

    assert_eq!(
        delete_groups(&state, &[GROUP]).await,
        vec![(GROUP.to_string(), ERROR_GROUP_AUTHORIZATION_FAILED)]
    );

    assert_eq!(
        committed(&state, GROUP, "orders").await,
        4,
        "nothing is deleted"
    );
    let hidden = raw
        .get_consumer_group(
            &iggy_id("hidden"),
            &iggy_id("audit"),
            &iggy_id("kafka.cg.orders-app"),
        )
        .await
        .expect("get the Iggy group");
    assert!(hidden.is_some(), "the offsets on hidden/audit stay");
}
