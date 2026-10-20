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

//! Metadata and partition consensus groups choose primaries independently.
//! New partitions must inherit the metadata view, while existing partitions
//! can retain a different primary after a metadata-only election. SDK clients
//! settle on the metadata leader, so both writes to new partitions and group
//! polls of existing partitions must reach an owner that can commit progress.

use std::str::FromStr;
use std::time::Duration;

use super::register_forwarding::connect_without_login;
use bytes::Bytes;
use futures::StreamExt;
use iggy::prelude::*;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, GET_CONSUMER_GROUP_CODE, GET_PARTITION_ROUTE_CODE, POLL_MESSAGES_CODE,
    STORE_CONSUMER_OFFSET_CODE,
};
use iggy_binary_protocol::consensus::{RequestHeader, result_code, result_section_len};
use iggy_binary_protocol::requests::consumer_groups::GetConsumerGroupRequest;
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::requests::messages::{GetPartitionRouteRequest, PollMessagesRequest};
use iggy_binary_protocol::requests::system::BindSessionRequest;
use iggy_binary_protocol::requests::users::login_register::BindSecret;
use iggy_binary_protocol::responses::consumer_groups::ConsumerGroupDetailsResponse;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{
    ClientVersionInfo, IGGY_PROTOCOL_VERSION, WireDecode, WireEncode, WireName,
};
use iggy_common::locking::IggyRwLockFn;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::wire_conversions::{
    consumer_to_wire, identifier_to_wire, polling_strategy_to_wire,
};
use iggy_common::{
    BinaryTransport, ConsumerOffsetInfo, IggyMessagesBatch, RESYNC_REQUIRED_PARTITION_SENTINEL,
    VsrSessionControl,
};
use integration::harness::disk::{
    leader_node_index_via, read_metadata_superblock_state, read_partition_superblock_state,
};
use integration::harness::{TestBinary, TestHarness};
use integration::iggy_harness;
use journal::superblock::{PingPongSuperblock, SuperblockStore};
use reqwest::StatusCode;
use reqwest::header::RETRY_AFTER;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};

use crate::server::http_client::{HttpClient, applied_op, node_url, until_primary_resolved};
use crate::server::raw_tcp;

const STREAM_NAME: &str = "partition-routing-stream";
const TOPIC_NAME: &str = "partition-routing-topic";
const PARTITION_ID: u32 = 0;
const GROUP_NAME: &str = "partition-routing-group";
const OFFSET_GROUP_NAME: &str = "offset-routing-group";
const GROUP_PAYLOADS: [&str; 2] = ["group-poll-first", "group-poll-second"];
const RELAY_CONSUMER_ID: u32 = 7;
const RELAYED_PAYLOADS: [&str; 2] = ["relayed-acked", "relayed-unacked"];
/// The marker and the route of an HTTP request that one node relays to another.
const FORWARDED_HEADER: &str = "iggy-forwarded";
const PARTITION_ROUTE_HEADER: &str = "iggy-partition-route";
/// Partition 0 at incarnation 1, owner generation 0 and metadata op 0, with
/// 5 s left. Every replica applied op 0, so the metadata hold ends at once.
const ROUTE_AT_FIRST_OP: &str = "0,1,0,0,5000";
const GROUP_POLL_BUDGET: Duration = Duration::from_secs(20);
// QUIC may use the full 30-second response budget before a dead data connection
// reports an uncertain outcome; recovery then needs a fresh routing attempt.
const DATA_FAILOVER_BUDGET: Duration = Duration::from_secs(60);
const INITIAL_VIEW: u32 = 0;
const METADATA_CHECKPOINT_REQUESTS: usize = 256;
// Keep handcrafted requests distinct from the parent SDK session's low IDs.
const RAW_REQUEST_START: u64 = 1 << 63;

/// Long enough for the backups to miss `cluster.heartbeat_timeout` (5s by
/// default) and conclude an election.
const ELECTION_SETTLE: Duration = Duration::from_secs(15);
/// Long enough for the restarted node 0 to rejoin at the new view.
const REJOIN_SETTLE: Duration = Duration::from_secs(10);
/// Under the SDK's own `RESPONSE_READ_TIMEOUT` (30s), so this fires first and
/// names the failure. Above it the SDK's timeout always wins and the budget is
/// dead code.
const SEND_BUDGET: Duration = Duration::from_secs(20);
/// How long the metadata plane gets to settle on a leader that is not node 0,
/// the state this test needs before it can observe anything.
const PRECONDITION_BUDGET: Duration = Duration::from_secs(20);
const PRECONDITION_POLL: Duration = Duration::from_millis(500);

fn message(payload: &str) -> IggyMessage {
    IggyMessage::from_str(payload).expect("build message")
}

fn send_body(payload: &str) -> SendMessages {
    let messages = vec![message(payload)];
    SendMessages {
        metadata_length: 0,
        stream_id: Identifier::default(),
        topic_id: Identifier::default(),
        partitioning: Partitioning::partition_id(PARTITION_ID),
        batch: IggyMessagesBatch::from(&messages),
        context: None,
    }
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_metadata_view_moved_when_producing_to_a_fresh_topic_should_reach_the_advertised_leader(
    harness: &mut TestHarness,
) {
    // Kill node 0: it is the view-0 primary of BOTH planes, so the metadata
    // plane must elect someone else. Nothing has been written yet, so no
    // partition group exists to move with it. Fixed waits rather than polling:
    // dialing a leaderless cluster blocks for the SDK's own budget, and a poll
    // loop that opens a fresh connection each round never converges.
    harness.kill_node(0).expect("kill node 0");
    sleep(ELECTION_SETTLE).await;
    harness.restart_node(0).expect("restart node 0");
    sleep(REJOIN_SETTLE).await;

    // Read through node 1: node 0 has only just restarted, and the roster read
    // is auth-gated, so it needs a node that can complete a login now.
    //
    // A SETUP PRECONDITION, not an invariant of the system. `primary_index` is
    // `view % replica_count` with no `Status::Normal` gate, so a cluster that
    // elected three times is back to advertising node 0 while perfectly
    // healthy. Polled rather than asserted once: the split this test is about
    // is only observable while the planes disagree, and one kill normally
    // lands view 1 immediately.
    let leader = {
        let deadline = Instant::now() + PRECONDITION_BUDGET;
        loop {
            let index = leader_node_index_via(harness, 1).await;
            if index != 0 {
                break index;
            }
            assert!(
                Instant::now() < deadline,
                "the metadata plane never settled on a leader other than node 0 within \
                 {PRECONDITION_BUDGET:?}; with the leader at node 0 both planes agree and \
                 the split this test is about cannot show"
            );
            sleep(PRECONDITION_POLL).await;
        }
    };

    // A brand-new topic. Its partition group is seeded from the metadata view,
    // so its primary is the advertised leader; left at view 0 it would be
    // replica 0, the node that was just killed and restarted.
    let setup = harness
        .root_client_for_node(leader)
        .await
        .expect("root client on the metadata leader");
    setup
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    let stream_id = Identifier::named(STREAM_NAME).expect("stream identifier");
    setup
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                durability: Durability::Persisted,
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    let topic_id = Identifier::named(TOPIC_NAME).expect("topic identifier");
    let partitioning = Partitioning::partition_id(PARTITION_ID);

    // Where a client asking for node 0 actually ends up.
    let leader_address = harness
        .node(leader)
        .tcp_addr()
        .expect("the leader exposes a TCP endpoint")
        .to_string();
    let on_node_zero = harness
        .root_client_for_node(0)
        .await
        .expect("root client on node 0");
    let landed_on = on_node_zero.get_connection_info().await.server_address;

    // Asserted, not printed. `root_client_for_node` signs in, and sign-in ends
    // in the SDK's leader check, so this client is on the LEADER whatever node
    // it dialed. Pinning that down is what stops the send below from being
    // read as "node 0 accepted it": nothing here ever reaches node 0, and a
    // reader who assumes otherwise draws the opposite conclusion from a pass.
    assert_eq!(
        landed_on, leader_address,
        "a signed-in client follows the roster's leader, so one dialing node 0 must settle on \
         node {leader}; landing anywhere else means the redirect did not run and the send below \
         is testing a different node than this test claims"
    );

    // The contract: the node the roster advertises accepts a partition write.
    // Seeded from the metadata view the group's primary IS that node; left at
    // view 0 it would be replica 0, and every client would be steered away
    // from the only node that could accept.
    let accepted_by_leader = send_once(&on_node_zero, &stream_id, &topic_id, &partitioning).await;
    assert!(
        accepted_by_leader.is_ok(),
        "node {leader} is advertised as the cluster leader, so a partition write sent there must \
         be accepted (or forwarded), got {accepted_by_leader:?}"
    );
}

/// Any node answers a partition route, so a client that reaches one node
/// learns where every partition is served. The nodes agree on the partition's
/// incarnation, count and primary. Send or poll permission is enough, and a
/// user with neither learns nothing.
#[iggy_harness(cluster_nodes = 3)]
async fn given_any_node_when_routing_a_partition_should_name_the_same_primary(
    harness: &mut TestHarness,
) {
    const PARTITIONS_COUNT: u32 = 2;
    const PASSWORD: &str = "route-password";
    const SENDER: &str = "route-sender";
    const POLLER: &str = "route-poller";
    const STRANGER: &str = "route-stranger";
    let leader = harness.node(0).tcp_addr().unwrap();
    let root = connect_without_login(leader).await;
    root.login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    root.create_stream(STREAM_NAME).await.unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    root.create_topic(
        &stream,
        TOPIC_NAME,
        &TopicCreateOptions {
            partitions_count: Some(PARTITIONS_COUNT),
            message_expiry: Some(IggyExpiry::NeverExpire),
            ..TopicCreateOptions::default()
        },
    )
    .await
    .unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    for (name, global) in [
        (
            SENDER,
            GlobalPermissions {
                send_messages: true,
                ..GlobalPermissions::default()
            },
        ),
        (
            POLLER,
            GlobalPermissions {
                poll_messages: true,
                ..GlobalPermissions::default()
            },
        ),
        (STRANGER, GlobalPermissions::default()),
    ] {
        root.create_user(
            name,
            PASSWORD,
            UserStatus::Active,
            Some(Permissions {
                global,
                streams: None,
            }),
        )
        .await
        .unwrap();
    }
    let route_request = GetPartitionRouteRequest {
        stream_id: identifier_to_wire(&stream).unwrap(),
        topic_id: identifier_to_wire(&topic).unwrap(),
        partition_id: PARTITION_ID,
    }
    .to_bytes();

    let stranger = connect_without_login(leader).await;
    stranger.login_user(STRANGER, PASSWORD).await.unwrap();
    let refused = stranger
        .send_raw_with_response(GET_PARTITION_ROUTE_CODE, route_request.clone())
        .await;
    assert!(
        matches!(refused, Err(IggyError::Unauthorized)),
        "a user without send or poll permission must not learn the route, got {refused:?}"
    );

    let mut routes = Vec::new();
    for name in [SENDER, POLLER] {
        let member = connect_without_login(leader).await;
        member.login_user(name, PASSWORD).await.unwrap();
        let binding = BindSessionRequest {
            version_info: ClientVersionInfo {
                protocol_version: IGGY_PROTOCOL_VERSION,
                sdk_name: WireName::new("routing-test").unwrap(),
                sdk_version: WireName::new("1.0.0").unwrap(),
            },
            identity: member.session_identity().await.unwrap(),
            bind_secret: member.session_bind_secret().await.unwrap(),
        };
        let mut request = RequestHeader {
            client: binding.identity.client_id,
            session: binding.identity.session,
            request: RAW_REQUEST_START,
            ..Default::default()
        };
        for node in 0..harness.cluster_size() {
            let mut data = raw_tcp::connect_to(harness.node(node).tcp_addr().unwrap()).await;
            data_request(
                &mut data,
                &mut request,
                BIND_SESSION_CODE,
                &binding.to_bytes(),
                None,
            )
            .await
            .unwrap();
            // A node creates its replica of a new partition after it applies
            // the topic, and answers transient until then.
            let route = timeout(PRECONDITION_BUDGET, async {
                loop {
                    match data_request(
                        &mut data,
                        &mut request,
                        GET_PARTITION_ROUTE_CODE,
                        &route_request,
                        None,
                    )
                    .await
                    {
                        Ok(reply) => break PartitionRouteResponse::decode_from(&reply).unwrap(),
                        Err(IggyError::TransientNotAccepted) => sleep(PRECONDITION_POLL).await,
                        Err(error) => panic!("node {node} refused the route for {name}: {error:?}"),
                    }
                }
            })
            .await
            .unwrap_or_else(|_| panic!("node {node} never routed the partition for {name}"));
            routes.push((node, name, route));
        }
    }

    let (_, _, first) = &routes[0];
    assert_ne!(
        first.incarnation, 0,
        "a created partition has an incarnation"
    );
    assert_eq!(first.partitions_count, PARTITIONS_COUNT);
    assert!(usize::from(first.primary_replica) < harness.cluster_size());
    for (node, name, route) in &routes {
        assert_eq!(
            (
                route.incarnation,
                route.partitions_count,
                route.view,
                route.primary_replica
            ),
            (
                first.incarnation,
                first.partitions_count,
                first.view,
                first.primary_replica
            ),
            "node {node} answered {name} with a different route"
        );
        assert!(
            route.metadata_op > 0,
            "node {node} applied the topic, so its route reports a metadata op"
        );
    }
}

#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    consumer_group.rebalancing_timeout = "1s",
))]
async fn given_different_metadata_and_partition_primaries_when_group_auto_commits_should_return_messages(
    harness: &mut TestHarness,
) {
    assert_group_auto_commit_routing(harness, TransportProtocol::Tcp).await;
}

#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    consumer_group.rebalancing_timeout = "1s",
))]
async fn given_different_metadata_and_partition_primaries_when_quic_group_auto_commits_should_return_messages(
    harness: &mut TestHarness,
) {
    assert_group_auto_commit_routing(harness, TransportProtocol::Quic).await;
}

#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    consumer_group.rebalancing_timeout = "1s",
))]
async fn given_different_metadata_and_partition_primaries_when_websocket_group_auto_commits_should_return_messages(
    harness: &mut TestHarness,
) {
    assert_group_auto_commit_routing(harness, TransportProtocol::WebSocket).await;
}

#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_split_primaries_when_http_auto_commits_on_a_backup_should_replicate_offsets(
    harness: &mut TestHarness,
) {
    let (partition_primary, metadata_primary, _) = seed_split_primaries(harness).await;
    assert_ne!(partition_primary, metadata_primary);
    let client = harness
        .node(metadata_primary)
        .http_client()
        .unwrap()
        .with_root_login()
        .connect()
        .await
        .unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let consumer = Consumer::default();
    let response = timeout(
        GROUP_POLL_BUDGET,
        client.poll_messages(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::first(),
            GROUP_PAYLOADS.len() as u32,
            true,
        ),
    )
    .await
    .expect("HTTP auto-commit must forward to the partition primary")
    .unwrap();
    assert_eq!(response.messages.len(), GROUP_PAYLOADS.len());
    for (message, expected) in response.messages.iter().zip(GROUP_PAYLOADS) {
        assert_eq!(message.payload.as_ref(), expected.as_bytes());
    }
    assert_routed_offset_from_each_node(
        harness,
        &consumer,
        Some((GROUP_PAYLOADS.len() - 1) as u64),
    )
    .await;
}

/// Every partition request an HTTP caller sends to a backup runs on the
/// partition primary: the backup chooses the partition and relays the request.
#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_split_primaries_when_http_calls_a_partition_backup_should_serve_on_the_primary(
    harness: &mut TestHarness,
) {
    let (partition_primary, backup, _) = seed_split_primaries(harness).await;
    assert_ne!(partition_primary, backup);
    let http = HttpClient::login_root_no_redirect(node_url(harness, backup)).await;
    let messages = http.url(&format!(
        "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages"
    ));
    // The relay copies the partition primary's applied op onto each response.
    let mut primary_applied_op = 0;
    for (payload, query, status) in [
        (RELAYED_PAYLOADS[0], "", StatusCode::CREATED),
        (RELAYED_PAYLOADS[1], "?ack=none", StatusCode::ACCEPTED),
    ] {
        let response = http
            .client
            .post(format!("{messages}{query}"))
            .bearer_auth(&http.token)
            .json(&send_body(payload))
            .send()
            .await
            .expect("send request");
        assert_eq!(response.status(), status, "send{query} on a backup");
        primary_applied_op = primary_applied_op.max(applied_op(&response));
    }

    let expected: Vec<&str> = GROUP_PAYLOADS.into_iter().chain(RELAYED_PAYLOADS).collect();
    let poll = format!(
        "{messages}?consumer_id={RELAY_CONSUMER_ID}&partition_id={PARTITION_ID}\
         &kind=offset&value=0&count={}&auto_commit=false",
        expected.len() + 1
    );
    let polled = timeout(GROUP_POLL_BUDGET, async {
        loop {
            let response = http
                .client
                .get(&poll)
                .bearer_auth(&http.token)
                .send()
                .await
                .expect("poll request");
            assert_eq!(response.status(), StatusCode::OK, "poll on a backup");
            let polled: PolledMessages = response.json().await.expect("decode PolledMessages");
            if polled.messages.len() >= expected.len() {
                break polled;
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the unacknowledged send must commit on the partition primary");
    let payloads: Vec<&[u8]> = polled
        .messages
        .iter()
        .map(|message| message.payload.as_ref())
        .collect();
    let expected: Vec<&[u8]> = expected.iter().map(|payload| payload.as_bytes()).collect();
    assert_eq!(payloads, expected);
    assert!(
        polled.context.metadata_op >= primary_applied_op,
        "the relayed poll context {:?} must cover the primary's applied op {primary_applied_op}",
        polled.context
    );

    let offsets = http.url(&format!(
        "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/consumer-offsets"
    ));
    let offset = format!("{offsets}?consumer_id={RELAY_CONSUMER_ID}&partition_id={PARTITION_ID}");
    let store = StoreConsumerOffset {
        context: None,
        consumer: Consumer::new(Identifier::numeric(RELAY_CONSUMER_ID).unwrap()),
        partition_id: Some(PARTITION_ID),
        offset: 1,
    };
    let response = http
        .client
        .put(&offsets)
        .bearer_auth(&http.token)
        .json(&store)
        .send()
        .await
        .expect("store offset request");
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "store on a backup"
    );
    pause_node(harness, partition_primary, true);
    let (response, poll_response, ()) = tokio::join!(
        http.client.get(&offset).bearer_auth(&http.token).send(),
        http.client.get(&poll).bearer_auth(&http.token).send(),
        async {
            wait_for_queued_relays(harness, partition_primary, 2).await;
            pause_node(harness, partition_primary, false);
        },
    );
    let poll_response = poll_response.expect("relayed poll request");
    assert_eq!(poll_response.status(), StatusCode::OK);
    let relayed: PolledMessages = poll_response.json().await.expect("decode relayed poll");
    assert_eq!(relayed.messages.len(), expected.len());
    let response = response.expect("relayed get offset request");
    assert_eq!(response.status(), StatusCode::OK, "get on a backup");
    let info: ConsumerOffsetInfo = response.json().await.expect("decode ConsumerOffsetInfo");
    assert_eq!(info.stored_offset, store.offset);
    let response = http
        .client
        .delete(format!(
            "{offsets}/{RELAY_CONSUMER_ID}?partition_id={PARTITION_ID}"
        ))
        .bearer_auth(&http.token)
        .send()
        .await
        .expect("delete offset request");
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "delete on a backup"
    );
    let response = http
        .client
        .get(&offset)
        .bearer_auth(&http.token)
        .send()
        .await
        .expect("get offset request after delete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND, "deleted offset");
}

/// A relayed request carries the partition the contacted node chose. A backup
/// refuses it with `NotPrimary` instead of relaying it again, a malformed route
/// is the caller's error, and a route without the relay marker is ignored.
#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_split_primaries_when_a_relayed_request_reaches_a_backup_should_refuse_not_primary(
    harness: &mut TestHarness,
) {
    let (partition_primary, backup, _) = seed_split_primaries(harness).await;
    assert_ne!(partition_primary, backup);
    let http = HttpClient::login_root_no_redirect(node_url(harness, backup)).await;
    let messages = http.url(&format!(
        "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages"
    ));
    let poll = format!(
        "{messages}?consumer_id={RELAY_CONSUMER_ID}&partition_id={PARTITION_ID}\
         &kind=offset&value=0&count=1&auto_commit=false"
    );
    let send = |route: &'static str| {
        http.client
            .post(&messages)
            .bearer_auth(&http.token)
            .header(PARTITION_ROUTE_HEADER, route)
            .json(&send_body(RELAYED_PAYLOADS[0]))
    };

    for (request, name) in [
        (
            send(ROUTE_AT_FIRST_OP).header(FORWARDED_HEADER, "1"),
            "send",
        ),
        (
            http.client
                .get(&poll)
                .bearer_auth(&http.token)
                .header(FORWARDED_HEADER, "1")
                .header(PARTITION_ROUTE_HEADER, ROUTE_AT_FIRST_OP),
            "poll",
        ),
    ] {
        let response = request.send().await.expect("relayed request");
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "relayed {name} on a backup"
        );
        assert!(response.headers().contains_key(RETRY_AFTER));
        let body: serde_json::Value = response.json().await.expect("decode the error body");
        assert_eq!(
            body["id"],
            IggyError::NotPrimary.as_code(),
            "relayed {name}"
        );
    }

    let response = send("0,1")
        .header(FORWARDED_HEADER, "1")
        .send()
        .await
        .expect("malformed relayed send");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = send(ROUTE_AT_FIRST_OP)
        .send()
        .await
        .expect("send with a route and no relay marker");
    assert_eq!(response.status(), StatusCode::CREATED);
}

/// A read changes no state, so a backup resends a relayed read whose partition
/// primary died before it answered, and the caller gets the answer of the next
/// primary. A poll with `auto_commit` may have moved its offset, so its outcome
/// stays unknown.
#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_split_primaries_when_a_relayed_read_loses_its_primary_should_resend_it(
    harness: &mut TestHarness,
) {
    let (partition_primary, backup, _) = seed_split_primaries(harness).await;
    assert_ne!(partition_primary, backup);
    let http = HttpClient::login_root_no_redirect(node_url(harness, backup)).await;
    let offsets = http.url(&format!(
        "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/consumer-offsets"
    ));
    let store = StoreConsumerOffset {
        context: None,
        consumer: Consumer::new(Identifier::numeric(RELAY_CONSUMER_ID).unwrap()),
        partition_id: Some(PARTITION_ID),
        offset: 1,
    };
    let response = http
        .client
        .put(&offsets)
        .bearer_auth(&http.token)
        .json(&store)
        .send()
        .await
        .expect("store offset request");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let messages = http.url(&format!(
        "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages"
    ));
    let poll = |auto_commit: bool| {
        format!(
            "{messages}?consumer_id={RELAY_CONSUMER_ID}&partition_id={PARTITION_ID}\
             &kind=offset&value=0&count={}&auto_commit={auto_commit}",
            GROUP_PAYLOADS.len()
        )
    };
    let get = |url: String| http.client.get(url).bearer_auth(&http.token).send();

    // A frozen primary takes the relayed requests and never answers them.
    pause_node(harness, partition_primary, true);
    let offset = format!("{offsets}?consumer_id={RELAY_CONSUMER_ID}&partition_id={PARTITION_ID}");
    let (offset_read, read, auto_commit, ()) = tokio::join!(
        get(offset.clone()),
        get(poll(false)),
        get(poll(true)),
        async {
            wait_for_queued_relays(harness, partition_primary, 3).await;
            harness
                .kill_node(partition_primary)
                .expect("kill the frozen partition primary");
        },
    );

    let response = read.expect("poll without auto_commit");
    let response = if response.status() == StatusCode::SERVICE_UNAVAILABLE {
        until_primary_resolved(|| async { get(poll(false)).await.expect("read-only poll") }).await
    } else {
        response
    };
    let (status, body) = status_and_body(response).await;
    assert_eq!(status, StatusCode::OK, "poll without auto_commit: {body}");
    let polled: PolledMessages = serde_json::from_str(&body).expect("decode PolledMessages");
    assert_eq!(polled.messages.len(), GROUP_PAYLOADS.len());
    let (status, body) = status_and_body(auto_commit.expect("poll with auto_commit")).await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "poll with auto_commit: {body}"
    );
    let body: serde_json::Value = serde_json::from_str(&body).expect("decode the error body");
    assert_eq!(body["code"], "forward_failed");
    let (status, body) = status_and_body(offset_read.expect("offset read")).await;
    // A new primary refuses to read a stored offset until it applied its view
    // start. That refusal admits nothing, so the caller resends the read.
    let (status, body) = if status == StatusCode::SERVICE_UNAVAILABLE {
        status_and_body(
            until_primary_resolved(|| async { get(offset.clone()).await.expect("offset read") })
                .await,
        )
        .await
    } else {
        (status, body)
    };
    assert_eq!(status, StatusCode::OK, "offset read: {body}");
    let info: ConsumerOffsetInfo = serde_json::from_str(&body).expect("decode ConsumerOffsetInfo");
    assert_eq!(info.stored_offset, store.offset);
}

fn pause_node(harness: &TestHarness, node: usize, paused: bool) {
    let pid = harness.node(node).pid().expect("the node is running");
    let signal = if paused { libc::SIGSTOP } else { libc::SIGCONT };
    // SAFETY: the signal targets a child this harness owns and has not reaped.
    assert_eq!(unsafe { libc::kill(pid as libc::pid_t, signal) }, 0);
}

/// The stopped HTTP listener cannot consume bytes. Nonempty receive queues
/// prove the relay requests reached it before it is resumed or killed.
async fn wait_for_queued_relays(harness: &TestHarness, node: usize, expected: usize) {
    const TCP_ESTABLISHED: &str = "01";
    let port = harness
        .node(node)
        .http_addr()
        .expect("node HTTP address")
        .port();
    timeout(PRECONDITION_BUDGET, async {
        loop {
            let queued = ["/proc/net/tcp", "/proc/net/tcp6"]
                .into_iter()
                .map(|path| {
                    std::fs::read_to_string(path)
                        .expect("read the kernel TCP connection table")
                        .lines()
                        .skip(1)
                        .filter(|line| {
                            let fields: Vec<_> = line.split_whitespace().collect();
                            fields.get(3) == Some(&TCP_ESTABLISHED)
                                && fields
                                    .get(1)
                                    .and_then(|address| address.rsplit_once(':'))
                                    .and_then(|(_, port)| u16::from_str_radix(port, 16).ok())
                                    == Some(port)
                                && fields
                                    .get(4)
                                    .and_then(|queues| queues.split_once(':'))
                                    .and_then(|(_, received)| {
                                        usize::from_str_radix(received, 16).ok()
                                    })
                                    .is_some_and(|received| received > 0)
                        })
                        .count()
                })
                .sum::<usize>();
            if queued >= expected {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("every HTTP relay must reach the paused partition primary");
}

async fn status_and_body(response: reqwest::Response) -> (StatusCode, String) {
    let status = response.status();
    (status, response.text().await.expect("response body"))
}

#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
#[ignore = "requires Go; run this test explicitly with --ignored"]
async fn given_split_primaries_when_go_group_auto_commits_should_preserve_membership(
    harness: &mut TestHarness,
) {
    run_go_split_primary_test(
        harness,
        "^TestE2E_SplitPrimaryPollsPreserveCoordinatorMembership$",
    )
    .await;
}

#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
#[ignore = "requires Go; run this test explicitly with --ignored"]
async fn given_split_primaries_when_go_group_commits_manually_should_preserve_membership(
    harness: &mut TestHarness,
) {
    run_go_split_primary_test(
        harness,
        "^TestE2E_SplitPrimaryManualCommitPreservesMembership$",
    )
    .await;
}

async fn run_go_split_primary_test(harness: &mut TestHarness, test: &str) {
    let (_, metadata_primary, _) = seed_split_primaries(harness).await;
    let output = tokio::process::Command::new("go")
        .args(["test", "./tests", "-run", test, "-count=1", "-v"])
        .current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../foreign/go"))
        .env(
            "IGGY_TCP_ADDRESS",
            harness
                .node(metadata_primary)
                .tcp_addr()
                .unwrap()
                .to_string(),
        )
        .env("IGGY_POLL_ROUTING_STREAM", STREAM_NAME)
        .env("IGGY_POLL_ROUTING_TOPIC", TOPIC_NAME)
        .env(
            "IGGY_POLL_ROUTING_MESSAGES_PER_PARTITION",
            GROUP_PAYLOADS.len().to_string(),
        )
        .kill_on_drop(true)
        .output()
        .await
        .expect("run Go SDK test with the seeded cluster");
    assert!(
        output.status.success(),
        "Go SDK routing regression failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn assert_group_offset_routing(
    harness: &TestHarness,
    member: &IggyClient,
    metadata_address: &str,
) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let group = Identifier::named(OFFSET_GROUP_NAME).unwrap();
    let consumer = Consumer::group(group.clone());
    member
        .create_consumer_group(&stream, &topic, OFFSET_GROUP_NAME)
        .await
        .unwrap();
    member
        .join_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    integration::harness::wait_for_consumer_group_assignment(
        member,
        &stream,
        &topic,
        &group,
        1,
        PRECONDITION_BUDGET,
    )
    .await;
    let before = member.get_me().await.unwrap();
    for offset in 0..GROUP_PAYLOADS.len() as u64 {
        timeout(
            GROUP_POLL_BUDGET,
            member.store_consumer_offset(&consumer, &stream, &topic, Some(PARTITION_ID), offset),
        )
        .await
        .expect("manual group commit must reach the partition primary")
        .expect("manual group commit must preserve its coordinator membership");
        assert_eq!(
            member.get_connection_info().await.server_address,
            metadata_address
        );
        assert_eq!(member.get_me().await.unwrap().client_id, before.client_id);
    }
    let expected_offset = (GROUP_PAYLOADS.len() - 1) as u64;
    assert_routed_offset_from_each_node(harness, &consumer, Some(expected_offset)).await;
    member
        .delete_consumer_offset(&consumer, &stream, &topic, Some(PARTITION_ID))
        .await
        .unwrap();
    assert_eq!(
        member.get_connection_info().await.server_address,
        metadata_address
    );
    let membership = member
        .get_consumer_group(&stream, &topic, &group)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(membership.members_count, 1);
    assert_eq!(membership.members[0].partitions, [PARTITION_ID]);
    member
        .leave_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    assert_routed_offset_from_each_node(harness, &consumer, None).await;
    for (group_name, policy) in [
        (
            "interval-offset-group",
            AutoCommit::Interval(NonZeroIggyDuration::ONE_SECOND),
        ),
        (
            "default-offset-group",
            AutoCommit::IntervalOrWhen(
                NonZeroIggyDuration::ONE_SECOND,
                AutoCommitWhen::PollingMessages,
            ),
        ),
    ] {
        let mut interval_consumer = member
            .consumer_group(group_name, STREAM_NAME, TOPIC_NAME)
            .unwrap()
            .batch_length(GROUP_PAYLOADS.len() as u32)
            .auto_commit(policy)
            .build();
        interval_consumer.init().await.unwrap();
        for _ in GROUP_PAYLOADS {
            timeout(GROUP_POLL_BUDGET, interval_consumer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        let consumer = Consumer::group(Identifier::named(group_name).unwrap());
        assert_routed_offset_from_each_node(harness, &consumer, Some(expected_offset)).await;
        assert_eq!(
            member.get_connection_info().await.server_address,
            metadata_address
        );
        assert_eq!(member.get_me().await.unwrap().client_id, before.client_id);
        interval_consumer.shutdown().await.unwrap();
    }
}

async fn assert_routed_offset_from_each_node(
    harness: &TestHarness,
    consumer: &Consumer,
    expected: Option<u64>,
) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    for node in 0..harness.cluster_size() {
        let address = harness.node(node).tcp_addr().unwrap();
        let replica = connect_without_login(address).await;
        replica
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        timeout(PRECONDITION_BUDGET, async {
            loop {
                let offset = replica
                    .get_consumer_offset(consumer, &stream, &topic, Some(PARTITION_ID))
                    .await
                    .unwrap();
                if offset.map(|offset| offset.stored_offset) == expected {
                    break;
                }
                sleep(PRECONDITION_POLL).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "a client starting on node {node} must read offset {expected:?} from the primary"
            )
        });
    }
}

async fn seed_split_primaries(harness: &mut TestHarness) -> (usize, usize, u32) {
    let partition_primary = leader_node_index_via(harness, 0).await;
    let producer = harness
        .root_client_for_node(partition_primary)
        .await
        .unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    producer.create_stream(STREAM_NAME).await.unwrap();
    producer
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                messages_required_to_save: Some(1),
                durability: Durability::Persisted,
                consumer_offset_durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let mut messages: Vec<_> = GROUP_PAYLOADS.into_iter().map(message).collect();
    producer
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("seed messages before separating the consensus views");
    let partition_view =
        read_partition_superblock_state(&harness.node(partition_primary).data_path())
            .map_or(INITIAL_VIEW, |state| state.view);
    assert_eq!(
        partition_view as usize % harness.cluster_size(),
        partition_primary,
        "the seed producer must be on the partition primary"
    );

    // Checkpoint before editing the stopped backup's view, so the fixture
    // preserves a real durable state instead of fabricating its other fields.
    for index in 0..METADATA_CHECKPOINT_REQUESTS {
        producer
            .create_stream(&format!("{STREAM_NAME}-{index}"))
            .await
            .expect("fill the metadata journal to trigger its checkpoint");
    }
    let backup = (partition_primary + 1) % harness.cluster_size();
    timeout(PRECONDITION_BUDGET, async {
        while read_metadata_superblock_state(&harness.node(backup).data_path()).is_none() {
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the backup must checkpoint before the metadata-only view change");
    advance_backup_metadata_view(harness, backup);
    let metadata_primary = timeout(PRECONDITION_BUDGET, async {
        loop {
            let leader = leader_node_index_via(harness, partition_primary).await;
            if leader != partition_primary {
                break leader;
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the metadata-only election must move leadership off the live partition primary");

    (partition_primary, metadata_primary, partition_view)
}

async fn assert_group_auto_commit_routing(harness: &mut TestHarness, transport: TransportProtocol) {
    let (partition_primary, metadata_primary, partition_view) = seed_split_primaries(harness).await;
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let metadata_node = harness.node(metadata_primary);
    let (builder, address) = match transport {
        TransportProtocol::Tcp => (metadata_node.tcp_client(), metadata_node.tcp_addr()),
        TransportProtocol::Quic => (metadata_node.quic_client(), metadata_node.quic_addr()),
        TransportProtocol::WebSocket => (
            metadata_node.websocket_client(),
            metadata_node.websocket_addr(),
        ),
        TransportProtocol::Http => panic!("this test requires a binary transport"),
    };
    let member = builder
        .unwrap()
        .with_reconnecting_root_login()
        .connect()
        .await
        .unwrap();
    let metadata_address = address.unwrap().to_string();
    timeout(PRECONDITION_BUDGET, async {
        loop {
            let polled = member
                .poll_messages(
                    &stream,
                    &topic,
                    Some(PARTITION_ID),
                    &Consumer::default(),
                    &PollingStrategy::first(),
                    u32::try_from(GROUP_PAYLOADS.len()).unwrap(),
                    false,
                )
                .await
                .expect("the metadata leader can read the backup without committing offsets");
            if polled.messages.len() == GROUP_PAYLOADS.len() {
                break;
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the metadata leader's backup must hold both seeded messages");
    assert_group_offset_routing(harness, &member, &metadata_address).await;
    member
        .create_consumer_group(&stream, &topic, GROUP_NAME)
        .await
        .unwrap();
    let group = Identifier::named(GROUP_NAME).unwrap();
    member
        .join_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    integration::harness::wait_for_consumer_group_assignment(
        &member,
        &stream,
        &topic,
        &group,
        1,
        PRECONDITION_BUDGET,
    )
    .await;
    let membership = member
        .get_consumer_group(&stream, &topic, &group)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(membership.members_count, 1);
    assert_eq!(membership.members[0].partitions, [PARTITION_ID]);
    assert_eq!(
        member.get_connection_info().await.server_address,
        metadata_address
    );
    let backup_view = read_partition_superblock_state(&harness.node(metadata_primary).data_path())
        .map_or(INITIAL_VIEW, |state| state.view);
    assert_eq!(
        backup_view, partition_view,
        "the partition view must not follow the metadata-only election"
    );

    let consumer = Consumer::group(group);
    for expected in GROUP_PAYLOADS {
        let result = timeout(
            GROUP_POLL_BUDGET,
            member.poll_messages(
                &stream,
                &topic,
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true,
            ),
        )
        .await
        .expect("an auto-commit group poll must complete within its routing budget");
        let endpoint = member.get_connection_info().await.server_address;
        let polled = result.unwrap_or_else(|error| {
            panic!(
                "a joined group must poll across different primaries: {error:?}; \
                 metadata primary={metadata_primary}, partition primary={partition_primary}, \
                 connection before={metadata_address}, connection after={endpoint}"
            )
        });
        assert_eq!(
            polled.messages.len(),
            1,
            "the assigned partition has unread messages"
        );
        assert_eq!(polled.messages[0].payload.as_ref(), expected.as_bytes());
        assert_eq!(
            endpoint, metadata_address,
            "a data poll must retain the metadata connection"
        );
        let current_membership = member
            .get_consumer_group(&stream, &topic, &consumer.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current_membership.members_count, 1);
        assert_eq!(current_membership.members[0].id, membership.members[0].id);
    }

    let expected_offset = u64::try_from(GROUP_PAYLOADS.len() - 1).unwrap();
    assert_routed_offset_from_each_node(harness, &consumer, Some(expected_offset)).await;
    let partition_primary = assert_cached_route_after_primary_loss(
        harness,
        &member,
        partition_primary,
        &stream,
        &topic,
        &consumer,
    )
    .await;
    assert_data_session_fences(
        harness,
        &member,
        partition_primary,
        &stream,
        &topic,
        &consumer,
    )
    .await;
}

async fn assert_cached_route_after_primary_loss(
    harness: &mut TestHarness,
    member: &IggyClient,
    previous_primary: usize,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
) -> usize {
    const PAYLOAD: &str = "message-after-partition-failover";
    let mut receiver = member
        .consumer_group(GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(1)
        .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
        .build();
    receiver.init().await.unwrap();
    assert!(
        member
            .poll_messages(
                stream,
                topic,
                None,
                consumer,
                &PollingStrategy::next(),
                1,
                true
            )
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    let membership = member
        .get_consumer_group(stream, topic, &consumer.id)
        .await
        .unwrap()
        .unwrap();
    let coordinator = member.get_connection_info().await.server_address;
    harness.kill_node(previous_primary).unwrap();
    let route_request = GetPartitionRouteRequest {
        stream_id: identifier_to_wire(stream).unwrap(),
        topic_id: identifier_to_wire(topic).unwrap(),
        partition_id: PARTITION_ID,
    }
    .to_bytes();
    let primary = timeout(PRECONDITION_BUDGET, async {
        loop {
            match member
                .send_binary_request(GET_PARTITION_ROUTE_CODE, route_request.clone())
                .await
            {
                Ok(response) => {
                    let route = PartitionRouteResponse::decode_from(&response).unwrap();
                    let node = usize::from(route.primary_replica);
                    assert!(node < harness.cluster_size());
                    if node != previous_primary {
                        break node;
                    }
                }
                Err(IggyError::TransientNotAccepted) => {}
                Err(error) => panic!("route discovery failed during partition election: {error:?}"),
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the live metadata coordinator must discover the new partition primary");
    let producer = connect_without_login(harness.node(primary).tcp_addr().unwrap()).await;
    producer
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    producer
        .send_messages(
            stream,
            topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut [message(PAYLOAD)],
        )
        .await
        .unwrap();
    let received = timeout(DATA_FAILOVER_BUDGET, async {
        loop {
            match receiver.next().await.expect("the consumer must stay open") {
                Ok(message) => break message,
                // The fixture killed the old owner before polling, so the
                // uncertain reply cannot hide admission of this new message.
                Err(IggyError::TransientNotCommitted) => {}
                Err(error) => panic!("poll could not recover its cached data route: {error:?}"),
            }
        }
    })
    .await
    .expect("IggyConsumer must resume after data loss without coordinator reconnect events");
    assert_eq!(received.message.payload.as_ref(), PAYLOAD.as_bytes());
    assert_eq!(
        member.get_connection_info().await.server_address,
        coordinator
    );
    let current = member
        .get_consumer_group(stream, topic, &consumer.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.members_count, 1);
    assert_eq!(current.members[0].id, membership.members[0].id);
    primary
}

async fn assert_data_session_fences(
    harness: &TestHarness,
    member: &IggyClient,
    partition_primary: usize,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
) {
    const OTHER_USER: &str = "routing-other-user";
    const OTHER_PASSWORD: &str = "routing-other-password";
    let poll = PollMessagesRequest {
        consumer: consumer_to_wire(consumer).unwrap(),
        stream_id: identifier_to_wire(stream).unwrap(),
        topic_id: identifier_to_wire(topic).unwrap(),
        partition_id: Some(PARTITION_ID),
        strategy: polling_strategy_to_wire(&PollingStrategy::next()),
        count: 1,
        auto_commit: true,
    }
    .to_bytes();
    let route_request = GetPartitionRouteRequest {
        stream_id: identifier_to_wire(stream).unwrap(),
        topic_id: identifier_to_wire(topic).unwrap(),
        partition_id: PARTITION_ID,
    }
    .to_bytes();
    let response = member
        .send_binary_request(GET_PARTITION_ROUTE_CODE, route_request)
        .await
        .unwrap();
    let route = PartitionRouteResponse::decode_from(&response).unwrap();
    let address = harness.node(partition_primary).tcp_addr().unwrap();
    let mut data = raw_tcp::connect_to(address).await;
    let mut binding = {
        let client = member.client();
        let client = client.read().await;
        let (identity, bind_secret) = match &*client {
            ClientWrapper::Tcp(client) => (
                client.session_identity().await.unwrap(),
                client.session_bind_secret().await,
            ),
            ClientWrapper::Quic(client) => (
                client.session_identity().await.unwrap(),
                client.session_bind_secret().await,
            ),
            ClientWrapper::WebSocket(client) => (
                client.session_identity().await.unwrap(),
                client.session_bind_secret().await,
            ),
            ClientWrapper::Http(_) | ClientWrapper::Iggy(_) => {
                panic!("fixture requires a binary transport");
            }
        };
        BindSessionRequest {
            version_info: ClientVersionInfo {
                protocol_version: IGGY_PROTOCOL_VERSION,
                sdk_name: WireName::new("routing-test").unwrap(),
                sdk_version: WireName::new("1.0.0").unwrap(),
            },
            identity,
            bind_secret: bind_secret.unwrap(),
        }
    };
    let mut request = RequestHeader {
        client: binding.identity.client_id,
        session: binding.identity.session,
        request: RAW_REQUEST_START,
        ..Default::default()
    };
    member
        .create_user(OTHER_USER, OTHER_PASSWORD, UserStatus::Active, None)
        .await
        .unwrap();
    let other_user = Identifier::named(OTHER_USER).unwrap();
    let observer = harness
        .root_client_for_node(partition_primary)
        .await
        .unwrap();
    timeout(PRECONDITION_BUDGET, async {
        while observer.get_user(&other_user).await.unwrap().is_none() {
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the metadata observer must see the new user before login");
    let other = connect_without_login(address).await;
    other.login_user(OTHER_USER, OTHER_PASSWORD).await.unwrap();
    assert!(
        matches!(
            other
                .send_raw_with_response(BIND_SESSION_CODE, binding.to_bytes())
                .await,
            Err(IggyError::AlreadyAuthenticated)
        ),
        "authentication as another user must not authorize attachment"
    );
    let anonymous = connect_without_login(address).await;
    let mut wrong_secret = binding.clone();
    const WRONG_BIND_SECRET: [u8; 32] = [0; 32];
    wrong_secret.bind_secret = BindSecret::new(Box::new(WRONG_BIND_SECRET));
    assert!(matches!(
        anonymous
            .send_raw_with_response(BIND_SESSION_CODE, wrong_secret.to_bytes())
            .await,
        Err(IggyError::Unauthenticated)
    ));
    let mut wrong_epoch = binding.clone();
    wrong_epoch.identity.session += 1;
    assert!(
        matches!(
            data_request(
                &mut data,
                &mut request,
                BIND_SESSION_CODE,
                &wrong_epoch.to_bytes(),
                None
            )
            .await,
            Err(IggyError::InvalidSession(_))
        ),
        "attachment must require the exact parent epoch"
    );
    data_request(
        &mut data,
        &mut request,
        BIND_SESSION_CODE,
        &binding.to_bytes(),
        None,
    )
    .await
    .unwrap();
    let first_poll = data_request(
        &mut data,
        &mut request,
        POLL_MESSAGES_CODE,
        &poll,
        Some(route.context()),
    )
    .await
    .unwrap();
    let context = PolledMessages::from_bytes(first_poll).unwrap().context;
    assert_ne!(context.owner_generation, 0);
    // A caller without the owner generation sends 0, and the primary binds it
    // to the owner it installed for this session.
    let mut unbound = context;
    unbound.owner_generation = 0;
    let response = data_request(
        &mut data,
        &mut request,
        POLL_MESSAGES_CODE,
        &poll,
        Some(unbound),
    )
    .await
    .expect("owner 0 must bind to the owner installed for this session");
    assert_eq!(
        PolledMessages::from_bytes(response).unwrap().partition_id,
        PARTITION_ID
    );

    member
        .leave_consumer_group(stream, topic, &consumer.id)
        .await
        .unwrap();
    let membership_query = GetConsumerGroupRequest {
        stream_id: identifier_to_wire(stream).unwrap(),
        topic_id: identifier_to_wire(topic).unwrap(),
        group_id: identifier_to_wire(&consumer.id).unwrap(),
    }
    .to_bytes();
    timeout(PRECONDITION_BUDGET, async {
        loop {
            let response = data_request(
                &mut data,
                &mut request,
                GET_CONSUMER_GROUP_CODE,
                &membership_query,
                None,
            )
            .await
            .unwrap();
            let membership = ConsumerGroupDetailsResponse::decode_from(&response).unwrap();
            if membership.group.members_count == 0 {
                break;
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the data node must observe the member leaving");
    let response = data_request(
        &mut data,
        &mut request,
        POLL_MESSAGES_CODE,
        &poll,
        Some(context),
    )
    .await
    .unwrap();
    let polled = PolledMessages::from_bytes(response).unwrap();
    assert_eq!(
        polled.partition_id, RESYNC_REQUIRED_PARTITION_SENTINEL,
        "the attached session must respect the member leaving"
    );
    assert!(polled.messages.is_empty());
    let response = data_request(
        &mut data,
        &mut request,
        POLL_MESSAGES_CODE,
        &poll,
        Some(unbound),
    )
    .await
    .unwrap();
    assert_eq!(
        PolledMessages::from_bytes(response).unwrap().partition_id,
        RESYNC_REQUIRED_PARTITION_SENTINEL,
        "owner 0 must not outlive the membership"
    );
    let offset_write = StoreConsumerOffsetRequest {
        consumer: consumer_to_wire(consumer).unwrap(),
        stream_id: identifier_to_wire(stream).unwrap(),
        topic_id: identifier_to_wire(topic).unwrap(),
        partition_id: Some(PARTITION_ID),
        offset: 0,
        ack: iggy_binary_protocol::AckLevel::Quorum,
    }
    .to_bytes();
    assert!(
        matches!(
            data_request(
                &mut data,
                &mut request,
                STORE_CONSUMER_OFFSET_CODE,
                &offset_write,
                Some(context)
            )
            .await,
            Err(IggyError::ConsumerGroupPartitionNotOwned(..))
        ),
        "a departed member must not commit through its attachment"
    );

    member
        .join_consumer_group(stream, topic, &consumer.id)
        .await
        .unwrap();
    integration::harness::wait_for_consumer_group_assignment(
        member,
        stream,
        topic,
        &consumer.id,
        1,
        PRECONDITION_BUDGET,
    )
    .await;
    let replacement_context = timeout(PRECONDITION_BUDGET, async {
        loop {
            match member
                .poll_messages(
                    stream,
                    topic,
                    Some(PARTITION_ID),
                    consumer,
                    &PollingStrategy::first(),
                    1,
                    false,
                )
                .await
            {
                Ok(polled) if polled.context.owner_generation > context.owner_generation => {
                    break polled.context;
                }
                Ok(_)
                | Err(
                    IggyError::ConsumerGroupPartitionNotOwned(..) | IggyError::TransientNotAccepted,
                ) => {}
                Err(error) => panic!("poll failed after rejoining: {error:?}"),
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("rejoining must install a pollable owner within the routing budget");
    let context = replacement_context;
    binding.identity.metadata_watermark =
        binding.identity.metadata_watermark.max(context.metadata_op);
    data_request(
        &mut data,
        &mut request,
        BIND_SESSION_CODE,
        &binding.to_bytes(),
        None,
    )
    .await
    .unwrap();
    member.logout_user().await.unwrap();
    timeout(PRECONDITION_BUDGET, async {
        loop {
            let result = data_request(
                &mut data,
                &mut request,
                POLL_MESSAGES_CODE,
                &poll,
                Some(context),
            )
            .await;
            if matches!(result, Err(IggyError::Unauthenticated)) {
                break;
            }
            assert!(
                result.is_ok() || matches!(result, Err(IggyError::TransientNotAccepted)),
                "unexpected response while logout replicates: {result:?}"
            );
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("parent logout must fence the independent data session");
    assert_eq!(data.peer_addr().unwrap(), address);
}

fn advance_backup_metadata_view(harness: &mut TestHarness, backup: usize) {
    harness.stop_node(backup).expect("stop only a backup");
    let data_path = harness.node(backup).data_path();
    let mut state = read_metadata_superblock_state(&data_path).expect("backup metadata state");
    // Model a crash after persisting a metadata view change. Keeping log_view
    // and every partition file intact makes the two consensus planes diverge.
    state.view += 1;
    std::thread::spawn(move || {
        compio::runtime::Runtime::new()
            .expect("superblock I/O runtime")
            .block_on(async move {
                PingPongSuperblock::open(data_path.join("metadata"))
                    .await
                    .expect("open the stopped backup's metadata superblock")
                    .write(&state.to_bytes())
                    .await
                    .expect("persist the metadata-only view change");
            });
    })
    .join()
    .expect("superblock writer thread");
    harness.restart_node(backup).expect("restart the backup");
}

/// One send, bounded. The SDK replays `TransientNotAccepted` and then hands the
/// request to its failover path, which re-reads the same roster and returns to
/// the same wrong node, so with the defect present the send burns its whole
/// budget. The timeout fires before the SDK's own and names which it was.
async fn send_once(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partitioning: &Partitioning,
) -> Result<(), String> {
    let mut messages = vec![message("probe")];
    match tokio::time::timeout(
        SEND_BUDGET,
        client.send_messages(stream_id, topic_id, partitioning, &mut messages),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(format!("{error:?}")),
        Err(_) => Err(format!(
            "no answer within {SEND_BUDGET:?} (client livelocked)"
        )),
    }
}

// Session-fence probes must reach the selected socket without client routing or retries.
async fn data_request(
    data: &mut TcpStream,
    request: &mut RequestHeader,
    code: u32,
    body: &[u8],
    context: Option<PartitionContext>,
) -> Result<Bytes, IggyError> {
    *request = RequestHeader::for_request(
        code,
        request.client,
        request.request.checked_add(1).unwrap(),
        request.session,
        body,
    )
    .unwrap();
    if let Some(context) = context {
        context.stamp(request);
    }
    let (reply, body) = raw_tcp::exchange(data, request, body).await;
    let status = raw_tcp::reply_status(&reply);
    if status != 0 {
        return Err(IggyError::from_code(status));
    }
    let body = Bytes::from(body);
    if request.operation.is_result_framed() {
        match result_code(&body).expect("complete result section") {
            0 => Ok(body.slice(result_section_len(&body).unwrap()..)),
            code => Err(IggyError::from_code(code)),
        }
    } else {
        Ok(body)
    }
}
