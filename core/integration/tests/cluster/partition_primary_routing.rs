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
//! New partitions use their committed placement views, while existing partitions
//! can retain a different primary after a metadata-only election. SDK clients
//! settle on the metadata leader, so both writes to new partitions and group
//! polls of existing partitions must reach an owner that can commit progress.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::register_forwarding::connect_without_login;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::future::join_all;
use iggy::prelude::*;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, GET_CONSUMER_GROUP_CODE, GET_PARTITION_ROUTE_CODE, POLL_MESSAGES_CODE,
    SEND_MESSAGES_CODE, STORE_CONSUMER_OFFSET_CODE,
};
use iggy_binary_protocol::consensus::{RequestHeader, result_code, result_section_len};
use iggy_binary_protocol::requests::consumer_groups::GetConsumerGroupRequest;
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::requests::messages::send_messages::{RawMessage, SendMessagesEncoder};
use iggy_binary_protocol::requests::messages::{GetPartitionRouteRequest, PollMessagesRequest};
use iggy_binary_protocol::requests::system::BindSessionRequest;
use iggy_binary_protocol::requests::users::login_register::BindSecret;
use iggy_binary_protocol::responses::consumer_groups::ConsumerGroupDetailsResponse;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{
    ClientVersionInfo, IGGY_PROTOCOL_VERSION, WireDecode, WireEncode, WireIdentifier, WireName,
    WirePartitioning,
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime;
use tokio::time::{Instant, sleep, timeout};

use crate::server::http_client::{
    HttpClient, access_token, applied_op, node_url, until_primary_resolved,
};
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
/// The `<view>,<replica>` a hop target's `NotPrimary` names as its primary.
const PARTITION_PRIMARY_HEADER: &str = "iggy-partition-primary";
/// Partition 0 at incarnation 1, owner generation 0 and metadata op 0, with
/// 5 s left. Every replica applied op 0, so the metadata hold ends at once.
const ROUTE_AT_FIRST_OP: &str = "0,1,0,0,5000";
/// The hop target's budget for the relays it serves, and each credential's
/// share of it, as the server sizes them.
const RELAY_TARGET_BUDGET: usize = 128;
const RELAY_TARGET_SHARE: usize = 32;
const SPREAD_PARTITIONS: u32 = 3;
const RELAY_TARGET_NODE: usize = 1;
const RELAY_TARGET_PARTITION: u32 = 1;
/// The longest budget a hop target grants a relay: its own request deadline.
const RELAY_BUDGET: Duration = Duration::from_secs(10);
/// Declared by a stalled upload, which never sends it in full.
const STALLED_BODY_LENGTH: usize = 1024;
/// How late a stalled upload may end on a loaded machine.
const DEADLINE_SLACK: Duration = Duration::from_secs(10);
/// A node's outbound relay slots, shared by its metadata and partition relays,
/// and the longest it relays a metadata request, as the server sizes them.
const FORWARD_SLOTS: u64 = 128;
const FORWARD_DEADLINE: Duration = Duration::from_secs(30);
const REQUESTS_METRIC: &str = "http_requests_total";
const RELAY_PROBE_INTERVAL: Duration = Duration::from_millis(100);
/// A node proves its relay fields under a key derived from the cluster key
/// material, here the `http.jwt` secret, with the context the server freezes.
const RELAY_PROOF_HEADER: &str = "iggy-relay-proof";
const RELAY_KEY_MATERIAL: &str = "0123456789abcdef0123456789abcdef";
const RELAY_KEY_CONTEXT: &str = "apache-iggy server http-relay v1 key->hmac-sha256-key";
const RELAY_PROOF_LIFETIME_MICROS: u64 = 10_000_000;
const RELAY_FORGER: &str = "relay-forger";
const RELAY_FORGER_PASSWORD: &str = "relay-forger-password";
/// The node that relays the caller's request to the hop target.
const RELAY_ENTRY_NODE: usize = 2;
/// Lets forged relays reach the hop target before the genuine ones.
const FORGED_HEAD_START: Duration = Duration::from_secs(1);
/// How long a relay for an op the cluster has not committed must stay held.
const HOLD_PROBE: Duration = Duration::from_secs(1);
const HELD_PAYLOAD: &str = "held-for-a-fresh-op";
const PROBES_METRIC: &str = "http_partition_primary_probes_total";
const RELAYS_METRIC: &str = "http_partition_relays_total";
const FLOOR_READS_METRIC: &str = "http_metadata_floor_reads_total";
const SESSION_REGISTERS_METRIC: &str = "http_session_registers_total";
const METADATA_FLOOR_PATH: &str = "/cluster/metadata-floor";
const FLOOR_STREAM: &str = "moves-the-commit-floor";
/// Created to commit the op a held relay waits for.
const FRONTIER_STREAM: &str = "moves-the-metadata-frontier";
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
const REBALANCE_BUDGET: Duration = Duration::from_secs(120);
/// `cluster.heartbeat_timeout` at its default.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long quorum evidence keeps partition admission open past
/// `HEARTBEAT_TIMEOUT`, as the server sets it.
const FRESHNESS_MARGIN: Duration = Duration::from_secs(3);
/// How long a source primary drains a granted move before it gives the grant
/// up, as the server sets it.
const GRANT_TIMEOUT: Duration = Duration::from_secs(1);
/// The longest pause the SDK takes between two attempts of one call.
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(1);
/// A send that meets a move waits out the source's drain and a retry pause. A
/// handover that stalls ends in an election after a heartbeat timeout.
const MOVE_STALL_BOUND: Duration = GRANT_TIMEOUT
    .saturating_add(HEARTBEAT_TIMEOUT)
    .saturating_add(RETRY_BACKOFF_MAX);
/// Logged by a source primary when it hands its partition to the move target.
const TRANSFER_MARKER: &str = "rebalancing partition primary after target readiness confirmation";
const READBACK_BATCH: u32 = 1_000;
/// How late an isolated primary may close admission on a loaded machine.
const FENCE_SLACK: Duration = Duration::from_secs(2);
const FENCE_BOUND: Duration = HEARTBEAT_TIMEOUT
    .saturating_add(FRESHNESS_MARGIN)
    .saturating_add(FENCE_SLACK);
const FENCE_PROBE_INTERVAL: Duration = Duration::from_millis(100);

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

pub(super) async fn partition_route(
    client: &TcpClient,
    stream: &Identifier,
    topic: &Identifier,
    partition_id: u32,
) -> PartitionRouteResponse {
    let request = GetPartitionRouteRequest {
        stream_id: identifier_to_wire(stream).unwrap(),
        topic_id: identifier_to_wire(topic).unwrap(),
        partition_id,
    }
    .to_bytes();
    timeout(PRECONDITION_BUDGET, async {
        loop {
            match client
                .send_raw_with_response(GET_PARTITION_ROUTE_CODE, request.clone())
                .await
            {
                Ok(reply) => break PartitionRouteResponse::decode_from(&reply).unwrap(),
                Err(IggyError::TransientNotAccepted) => sleep(PRECONDITION_POLL).await,
                Err(error) => panic!("route {stream}/{topic}/{partition_id}: {error}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("partition {stream}/{topic}/{partition_id} never became routable"))
}

#[iggy_harness(cluster_nodes = 3, server(sharding.cpu_allocation = "2"))]
async fn given_concurrent_creates_when_growing_partitions_should_spread_and_serve_every_partition(
    harness: &mut TestHarness,
) {
    const FIRST_TOPIC: &str = "spread-first";
    const SECOND_TOPIC: &str = "spread-second";
    const THIRD_TOPIC: &str = "spread-third";
    const INITIAL_PARTITIONS: u32 = 3;
    const ADDED_PARTITIONS: u32 = 3;
    const SECOND_PARTITIONS: u32 = 2;
    const THIRD_PARTITIONS: u32 = 4;

    let first = connect_without_login(harness.node(0).tcp_addr().unwrap()).await;
    first
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let second = connect_without_login(harness.node(0).tcp_addr().unwrap()).await;
    second
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    first.create_stream(STREAM_NAME).await.unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let options = TopicCreateOptions {
        partitions_count: Some(INITIAL_PARTITIONS),
        durability: Durability::Persisted,
        message_expiry: Some(IggyExpiry::NeverExpire),
        ..TopicCreateOptions::default()
    };
    first
        .create_topic(&stream, FIRST_TOPIC, &options)
        .await
        .unwrap();
    let first_topic = Identifier::named(FIRST_TOPIC).unwrap();
    for partition_id in 0..INITIAL_PARTITIONS {
        let route = partition_route(&first, &stream, &first_topic, partition_id).await;
        assert_eq!(u32::from(route.primary_replica), partition_id);
        assert_eq!(route.view, partition_id);
    }

    let second_options = TopicCreateOptions {
        partitions_count: Some(SECOND_PARTITIONS),
        ..options.clone()
    };
    let third_options = TopicCreateOptions {
        partitions_count: Some(THIRD_PARTITIONS),
        ..options
    };
    let (second_result, third_result) = futures::join!(
        first.create_topic(&stream, SECOND_TOPIC, &second_options),
        second.create_topic(&stream, THIRD_TOPIC, &third_options),
    );
    second_result.unwrap();
    third_result.unwrap();
    let mut topics = [
        (FIRST_TOPIC, INITIAL_PARTITIONS),
        (SECOND_TOPIC, SECOND_PARTITIONS),
        (THIRD_TOPIC, THIRD_PARTITIONS),
    ];
    assert_initial_spread(&first, &stream, &topics, harness.cluster_size()).await;
    first
        .create_partitions(&stream, &first_topic, ADDED_PARTITIONS)
        .await
        .unwrap();
    topics[0].1 += ADDED_PARTITIONS;
    assert_initial_spread(&first, &stream, &topics, harness.cluster_size()).await;

    let mut original_routes = Vec::new();
    for (name, count) in topics {
        let topic = Identifier::named(name).unwrap();
        for partition_id in 0..count {
            let route = partition_route(&first, &stream, &topic, partition_id).await;
            original_routes.push((name, partition_id, route));
            let payload = format!("{name}-{partition_id}");
            let mut messages = vec![message(&payload)];
            first
                .send_messages(
                    &stream,
                    &topic,
                    &Partitioning::partition_id(partition_id),
                    &mut messages,
                )
                .await
                .unwrap();
            assert_partition_payload(&second, &stream, &topic, partition_id, &payload).await;
        }
    }
    drop(first);
    drop(second);
    for node in 0..harness.cluster_size() {
        harness.stop_node(node).unwrap();
    }
    for node in 0..harness.cluster_size() {
        harness.restart_node(node).unwrap();
    }
    let recovered = connect_without_login(harness.node(0).tcp_addr().unwrap()).await;
    recovered
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    for (name, partition_id, original) in original_routes {
        let topic = Identifier::named(name).unwrap();
        let route = partition_route(&recovered, &stream, &topic, partition_id).await;
        assert_eq!(route.incarnation, original.incarnation);
        assert_eq!(route.partitions_count, original.partitions_count);
        assert!(route.view >= original.view, "{name}/{partition_id}");
        let payload = format!("{name}-{partition_id}");
        assert_partition_payload(&recovered, &stream, &topic, partition_id, &payload).await;
    }
}

async fn assert_initial_spread(
    client: &TcpClient,
    stream: &Identifier,
    topics: &[(&str, u32)],
    replicas: usize,
) {
    let mut primary_counts = vec![0_u32; replicas];
    for &(name, count) in topics {
        let topic = Identifier::named(name).unwrap();
        for partition_id in 0..count {
            let route = partition_route(client, stream, &topic, partition_id).await;
            assert_eq!(route.view, u32::from(route.primary_replica));
            primary_counts[usize::from(route.primary_replica)] += 1;
        }
    }
    let total: u32 = topics.iter().map(|(_, count)| count).sum();
    assert_eq!(
        primary_counts,
        vec![total / u32::try_from(replicas).unwrap(); replicas],
        "create batches and growth must count already admitted assignments"
    );
}

async fn assert_partition_payload(
    client: &TcpClient,
    stream: &Identifier,
    topic: &Identifier,
    partition_id: u32,
    expected: &str,
) {
    let polled = client
        .poll_messages(
            stream,
            topic,
            Some(partition_id),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .unwrap();
    assert_eq!(polled.messages.len(), 1, "{topic}/{partition_id}");
    assert_eq!(polled.messages[0].payload.as_ref(), expected.as_bytes());
}

#[iggy_harness(cluster_nodes = 5, server(sharding.cpu_allocation = "2"))]
async fn given_balanced_primaries_when_a_replica_fails_and_returns_should_rebalance_without_data_loss(
    harness: &mut TestHarness,
) {
    const TOPIC_COUNT: usize = 100;
    const PHASES: [&str; 3] = ["before-failure", "while-absent", "after-return"];

    let client = connect_without_login(harness.node(1).tcp_addr().unwrap()).await;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let identity = client.session_identity().await.unwrap();
    client.create_stream(STREAM_NAME).await.unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        durability: Durability::Persisted,
        message_expiry: Some(IggyExpiry::NeverExpire),
        ..TopicCreateOptions::default()
    };
    let mut topics = Vec::with_capacity(TOPIC_COUNT);
    for index in 0..TOPIC_COUNT {
        let name = format!("rebalance-{index}");
        client.create_topic(&stream, &name, &options).await.unwrap();
        topics.push(Identifier::named(&name).unwrap());
    }
    let mut request = RequestHeader {
        client: identity.client_id,
        session: identity.session,
        request: RAW_REQUEST_START,
        ..Default::default()
    };
    let initial =
        wait_for_balanced_routes(harness, &client, &stream, &topics, &[20; 5], &mut request).await;
    append_and_assert_partition_sentinels(&client, &stream, &topics, &PHASES[..1]).await;

    harness.kill_node(0).unwrap();
    let surviving = wait_for_balanced_routes(
        harness,
        &client,
        &stream,
        &topics,
        &[0, 25, 25, 25, 25],
        &mut request,
    )
    .await;
    append_and_assert_partition_sentinels(&client, &stream, &topics, &PHASES[..2]).await;

    harness.restart_node(0).unwrap();
    let returned =
        wait_for_balanced_routes(harness, &client, &stream, &topics, &[20; 5], &mut request).await;
    for (index, original) in initial.iter().enumerate() {
        assert_ne!(original.incarnation, 0);
        for current in [&surviving[index], &returned[index]] {
            assert_eq!(
                current.incarnation, original.incarnation,
                "{}",
                topics[index]
            );
            assert_eq!(current.partitions_count, 1, "{}", topics[index]);
            assert!(current.view >= original.view, "{}", topics[index]);
        }
        assert!(
            returned[index].view >= surviving[index].view,
            "{}",
            topics[index]
        );
    }
    append_and_assert_partition_sentinels(&client, &stream, &topics, &PHASES).await;
    let current_identity = client.session_identity().await.unwrap();
    assert_eq!(current_identity.client_id, identity.client_id);
    assert_eq!(current_identity.session, identity.session);
}

async fn wait_for_balanced_routes(
    harness: &TestHarness,
    client: &TcpClient,
    stream: &Identifier,
    topics: &[Identifier],
    expected_counts: &[usize],
    request: &mut RequestHeader,
) -> Vec<PartitionRouteResponse> {
    let mut last_counts = vec![vec![0; expected_counts.len()]; expected_counts.len()];
    timeout(REBALANCE_BUDGET, async {
        let binding = BindSessionRequest {
            version_info: ClientVersionInfo {
                protocol_version: IGGY_PROTOCOL_VERSION,
                sdk_name: WireName::new("routing-test").unwrap(),
                sdk_version: WireName::new("1.0.0").unwrap(),
            },
            identity: client.session_identity().await.unwrap(),
            bind_secret: client.session_bind_secret().await.unwrap(),
        }
        .to_bytes();
        let route_requests: Vec<_> = topics
            .iter()
            .map(|topic| {
                GetPartitionRouteRequest {
                    stream_id: identifier_to_wire(stream).unwrap(),
                    topic_id: identifier_to_wire(topic).unwrap(),
                    partition_id: PARTITION_ID,
                }
                .to_bytes()
            })
            .collect();
        let mut observers = Vec::with_capacity(expected_counts.len());
        for (node, count) in expected_counts.iter().enumerate() {
            if *count == 0 {
                continue;
            }
            let address = harness.node(node).tcp_addr().unwrap();
            let mut socket = raw_tcp::connect_to(address).await;
            loop {
                match data_request(&mut socket, request, BIND_SESSION_CODE, &binding, None).await {
                    Ok(_) => break,
                    Err(IggyError::TransientNotAccepted) => sleep(PRECONDITION_POLL).await,
                    Err(error) => panic!("node {node} refused the existing session: {error:?}"),
                }
            }
            assert_eq!(socket.peer_addr().unwrap(), address);
            observers.push((node, socket));
        }
        loop {
            let mut snapshots = Vec::with_capacity(observers.len());
            for (node, socket) in &mut observers {
                let mut routes = Vec::with_capacity(topics.len());
                for body in &route_requests {
                    match data_request(socket, request, GET_PARTITION_ROUTE_CODE, body, None).await {
                        Ok(body) => routes.push(PartitionRouteResponse::decode_from(&body).unwrap()),
                        Err(IggyError::TransientNotAccepted) => break,
                        Err(error) => panic!("node {node} refused a partition route: {error:?}"),
                    }
                }
                if routes.len() != topics.len() {
                    break;
                }
                let mut counts = vec![0; expected_counts.len()];
                for route in &routes {
                    counts[usize::from(route.primary_replica)] += 1;
                }
                last_counts[*node].clone_from(&counts);
                if counts != expected_counts {
                    break;
                }
                snapshots.push(routes);
            }
            if snapshots.len() == observers.len()
                && snapshots.windows(2).all(|pair| {
                    pair[0].iter().zip(&pair[1]).all(|(left, right)| {
                        (left.incarnation, left.partitions_count, left.view, left.primary_replica)
                            == (right.incarnation, right.partitions_count, right.view, right.primary_replica)
                    })
                })
            {
                return snapshots.remove(0);
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "partition primaries did not converge to {expected_counts:?} within {REBALANCE_BUDGET:?}; \
             physical-node observations: {last_counts:?}"
        )
    })
}

async fn append_and_assert_partition_sentinels(
    client: &TcpClient,
    stream: &Identifier,
    topics: &[Identifier],
    phases: &[&str],
) {
    timeout(REBALANCE_BUDGET, async {
        for topic in topics {
            let payload = format!("{topic}:{}", phases.last().unwrap());
            client
                .send_messages(
                    stream,
                    topic,
                    &Partitioning::partition_id(PARTITION_ID),
                    &mut [message(&payload)],
                )
                .await
                .unwrap();
            let polled = client
                .poll_messages(
                    stream,
                    topic,
                    Some(PARTITION_ID),
                    &Consumer::default(),
                    &PollingStrategy::offset(0),
                    u32::try_from(phases.len() + 1).unwrap(),
                    false,
                )
                .await
                .unwrap();
            assert_eq!(polled.messages.len(), phases.len(), "{topic}");
            for (message, phase) in polled.messages.iter().zip(phases) {
                assert_eq!(
                    message.payload.as_ref(),
                    format!("{topic}:{phase}").as_bytes()
                );
            }
        }
    })
    .await
    .expect("every partition must preserve and serve its history across rebalancing");
}

/// Producers write every partition back to back while the rebalancer moves
/// primaries to a returning replica. Every acknowledged send stays readable in
/// send order. A send that ends uncertain is never sent again, so no message
/// may appear twice. Each send ends acknowledged, uncertain or with the refusal
/// the SDK retries, and none waits longer than a move can fence its partition.
#[iggy_harness(cluster_nodes = 3, server(sharding.cpu_allocation = "2"))]
async fn given_producers_on_every_partition_when_primaries_move_should_keep_acknowledged_order_without_stalls(
    harness: &mut TestHarness,
) {
    const TOPIC_COUNT: usize = 12;
    const RETURNING_MOVES: usize = 4;

    let client = connect_without_login(harness.node(1).tcp_addr().unwrap()).await;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let identity = client.session_identity().await.unwrap();
    client.create_stream(STREAM_NAME).await.unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        durability: Durability::Persisted,
        message_expiry: Some(IggyExpiry::NeverExpire),
        ..TopicCreateOptions::default()
    };
    let mut topics = Vec::with_capacity(TOPIC_COUNT);
    for index in 0..TOPIC_COUNT {
        let name = format!("moving-{index}");
        client.create_topic(&stream, &name, &options).await.unwrap();
        topics.push(Identifier::named(&name).unwrap());
    }
    let mut request = RequestHeader {
        client: identity.client_id,
        session: identity.session,
        request: RAW_REQUEST_START,
        ..Default::default()
    };
    wait_for_balanced_routes(harness, &client, &stream, &topics, &[4; 3], &mut request).await;
    harness.kill_node(0).unwrap();
    wait_for_balanced_routes(harness, &client, &stream, &topics, &[0, 6, 6], &mut request).await;

    let producing = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let producers = spawn_producers(
        harness.node(1).tcp_addr().unwrap(),
        topics.clone(),
        Arc::clone(&producing),
        Arc::clone(&stop),
    );
    timeout(PRECONDITION_BUDGET, async {
        while producing.load(Ordering::Acquire) < topics.len() {
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("every producer must acknowledge a send before the replica returns");
    let transfers_before = transfers_started(harness);
    harness.restart_node(0).unwrap();
    wait_for_balanced_routes(harness, &client, &stream, &topics, &[4; 3], &mut request).await;
    let transfers = transfers_started(harness) - transfers_before;
    stop.store(true, Ordering::Release);
    let logs = producers.join().expect("producer thread");

    let mut latencies: Vec<Duration> = logs
        .iter()
        .flat_map(|log| log.latencies.iter().copied())
        .collect();
    latencies.sort_unstable();
    println!(
        "{} sends across {transfers} primary moves: {} acknowledged, {} uncertain, {} refused, \
         {} unexpected; latency p50 {:?}, p99 {:?}, max {:?}",
        latencies.len(),
        logs.iter().map(|log| log.acknowledged.len()).sum::<usize>(),
        logs.iter().map(|log| log.uncertain.len()).sum::<usize>(),
        logs.iter().map(|log| log.refused.len()).sum::<usize>(),
        logs.iter().map(|log| log.unexpected.len()).sum::<usize>(),
        latencies[latencies.len() / 2],
        latencies[latencies.len() * 99 / 100],
        latencies[latencies.len() - 1],
    );
    assert!(
        transfers >= RETURNING_MOVES,
        "the producers ran across {transfers} moves, expected at least {RETURNING_MOVES}"
    );
    for (topic, log) in topics.iter().zip(&logs) {
        let sequences = read_sequences(&client, &stream, topic).await;
        assert_produced_history(topic, log, &sequences);
    }
}

/// What one producer saw of its sends. Sequence `n` is the `n`th send.
#[derive(Default)]
struct ProducerLog {
    acknowledged: Vec<u64>,
    uncertain: Vec<u64>,
    refused: Vec<u64>,
    unexpected: Vec<(u64, IggyError)>,
    latencies: Vec<Duration>,
}

/// The producers run on a thread of their own, so a blocking harness call on
/// the test thread does not stretch their latencies.
fn spawn_producers(
    address: SocketAddr,
    topics: Vec<Identifier>,
    producing: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<Vec<ProducerLog>> {
    thread::spawn(move || {
        runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("producer runtime")
            .block_on(join_all(topics.iter().map(|topic| {
                produce_until_stopped(address, topic, &producing, &stop)
            })))
    })
}

async fn produce_until_stopped(
    address: SocketAddr,
    topic: &Identifier,
    producing: &AtomicUsize,
    stop: &AtomicBool,
) -> ProducerLog {
    let client = connect_without_login(address).await;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let partitioning = Partitioning::partition_id(PARTITION_ID);
    let mut log = ProducerLog::default();
    let mut sequence = 0;
    while !stop.load(Ordering::Acquire) {
        let payload = format!("{topic}:{sequence}");
        let started = Instant::now();
        let outcome = client
            .send_messages(&stream, topic, &partitioning, &mut [message(&payload)])
            .await;
        log.latencies.push(started.elapsed());
        match outcome {
            Ok(_) => {
                if log.acknowledged.is_empty() {
                    producing.fetch_add(1, Ordering::Release);
                }
                log.acknowledged.push(sequence);
            }
            Err(IggyError::OperationUncertain(operation)) => {
                client
                    .abandon_operation(operation)
                    .await
                    .expect("free the lane of an uncertain send");
                log.uncertain.push(sequence);
            }
            Err(IggyError::TransientNotAccepted) => log.refused.push(sequence),
            Err(error) => log.unexpected.push((sequence, error)),
        }
        sequence += 1;
    }
    log
}

fn transfers_started(harness: &TestHarness) -> usize {
    // Node 0 restarts, which truncates its log, and it never moves a primary away.
    (1..harness.cluster_size())
        .map(|node| harness.node(node).stdout_occurrences(TRANSFER_MARKER))
        .sum()
}

async fn read_sequences(client: &TcpClient, stream: &Identifier, topic: &Identifier) -> Vec<u64> {
    let mut sequences = Vec::new();
    let mut offset = 0;
    loop {
        let polled = client
            .poll_messages(
                stream,
                topic,
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::offset(offset),
                READBACK_BATCH,
                false,
            )
            .await
            .unwrap();
        let Some(last) = polled.messages.last() else {
            return sequences;
        };
        offset = last.header.offset + 1;
        for message in &polled.messages {
            let payload = std::str::from_utf8(&message.payload).expect("a produced payload");
            let (name, sequence) = payload.rsplit_once(':').expect("a produced payload");
            assert_eq!(name, topic.to_string());
            sequences.push(sequence.parse().expect("a produced sequence"));
        }
    }
}

fn assert_produced_history(topic: &Identifier, log: &ProducerLog, sequences: &[u64]) {
    assert!(
        log.unexpected.is_empty(),
        "{topic}: sends ended with errors the SDK does not retry: {:?}",
        log.unexpected
    );
    let stalled: Vec<_> = log
        .latencies
        .iter()
        .enumerate()
        .filter(|(_, latency)| **latency > MOVE_STALL_BOUND)
        .collect();
    assert!(
        stalled.is_empty(),
        "{topic}: sends stalled past {MOVE_STALL_BOUND:?}: {stalled:?}"
    );
    if let Some(pair) = sequences.windows(2).find(|pair| pair[0] >= pair[1]) {
        panic!(
            "{topic}: sequence {} follows {} in the log",
            pair[1], pair[0]
        );
    }
    let read: HashSet<u64> = sequences.iter().copied().collect();
    let lost: Vec<_> = log
        .acknowledged
        .iter()
        .filter(|sequence| !read.contains(sequence))
        .collect();
    assert!(
        lost.is_empty(),
        "{topic}: acknowledged sends lost: {lost:?}"
    );
    let sent: HashSet<u64> = log
        .acknowledged
        .iter()
        .chain(&log.uncertain)
        .copied()
        .collect();
    let refused: Vec<_> = sequences
        .iter()
        .filter(|sequence| !sent.contains(sequence))
        .collect();
    assert!(
        refused.is_empty(),
        "{topic}: refused sends reached the log: {refused:?}"
    );
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_partition_primary_leading_metadata_when_its_quorum_freezes_should_refuse_writes_until_it_resumes(
    harness: &mut TestHarness,
) {
    assert_isolated_primary_fence(harness, true).await;
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_partition_primary_backing_up_metadata_when_its_quorum_freezes_should_refuse_writes_until_it_resumes(
    harness: &mut TestHarness,
) {
    assert_isolated_primary_fence(harness, false).await;
}

/// A partition primary whose quorum peers are frozen gets no fresh metadata
/// quorum evidence, whether it credits answered probes as the metadata primary
/// or probes as a backup. It must refuse partition writes with 58 once that
/// evidence is `heartbeat_timeout + 3s` old, admit nothing it refused, and
/// serve the partition again after the peers resume.
async fn assert_isolated_primary_fence(harness: &TestHarness, leads_metadata: bool) {
    const ADMITTED: [&str; 2] = ["before-isolation", "after-resume"];
    const REFUSED: &str = "refused-while-isolated";

    let client = connect_without_login(harness.node(0).tcp_addr().unwrap()).await;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client.create_stream(STREAM_NAME).await.unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let partitions = u32::try_from(harness.cluster_size()).unwrap();
    let options = TopicCreateOptions {
        partitions_count: Some(partitions),
        ..TopicCreateOptions::default()
    };
    client
        .create_topic(&stream, TOPIC_NAME, &options)
        .await
        .unwrap();
    let leader = leader_node_index_via(harness, 0).await;
    let mut routes = Vec::with_capacity(harness.cluster_size());
    for partition_id in 0..partitions {
        routes.push(partition_route(&client, &stream, &topic, partition_id).await);
    }
    let (partition_id, route) = (0..partitions)
        .zip(routes)
        .find(|(_, route)| (usize::from(route.primary_replica) == leader) == leads_metadata)
        .expect("one partition primary on every node, the metadata primary among them");
    let primary = usize::from(route.primary_replica);
    let address = harness.node(primary).tcp_addr().unwrap();
    let partitioning = Partitioning::partition_id(partition_id);
    client
        .send_messages(&stream, &topic, &partitioning, &mut [message(ADMITTED[0])])
        .await
        .unwrap();

    let probe = connect_without_login(address).await;
    probe
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let identity = probe.session_identity().await.unwrap();
    let binding = BindSessionRequest {
        version_info: ClientVersionInfo {
            protocol_version: IGGY_PROTOCOL_VERSION,
            sdk_name: WireName::new("routing-test").unwrap(),
            sdk_version: WireName::new("1.0.0").unwrap(),
        },
        identity,
        bind_secret: probe.session_bind_secret().await.unwrap(),
    }
    .to_bytes();
    let mut request = RequestHeader {
        client: identity.client_id,
        session: identity.session,
        request: RAW_REQUEST_START,
        ..Default::default()
    };
    let mut socket = raw_tcp::connect_to(address).await;
    data_request(&mut socket, &mut request, BIND_SESSION_CODE, &binding, None)
        .await
        .expect("bind the probe session on the partition primary");
    let poll = PollMessagesRequest {
        consumer: consumer_to_wire(&Consumer::default()).unwrap(),
        stream_id: identifier_to_wire(&stream).unwrap(),
        topic_id: identifier_to_wire(&topic).unwrap(),
        partition_id: Some(partition_id),
        strategy: polling_strategy_to_wire(&PollingStrategy::offset(0)),
        count: 1,
        auto_commit: false,
    }
    .to_bytes();
    data_request(
        &mut socket,
        &mut request,
        POLL_MESSAGES_CODE,
        &poll,
        Some(route.context()),
    )
    .await
    .expect("a primary with its quorum must serve the partition");

    let peers: Vec<usize> = (0..harness.cluster_size())
        .filter(|node| *node != primary)
        .collect();
    let isolated_at = Instant::now();
    for &peer in &peers {
        pause_node(harness, peer, true);
    }
    let fenced = timeout(FENCE_BOUND, async {
        loop {
            match data_request(
                &mut socket,
                &mut request,
                POLL_MESSAGES_CODE,
                &poll,
                Some(route.context()),
            )
            .await
            {
                Ok(_) => sleep(FENCE_PROBE_INTERVAL).await,
                Err(error) => break error,
            }
        }
    })
    .await;
    let write = data_request(
        &mut socket,
        &mut request,
        SEND_MESSAGES_CODE,
        &raw_send_body(partition_id, REFUSED),
        Some(route.context()),
    )
    .await;
    let refused_after = isolated_at.elapsed();
    for &peer in &peers {
        pause_node(harness, peer, false);
    }
    println!(
        "node {primary}, metadata leader {leader}, refused a write to partition {partition_id} \
         {refused_after:?} after isolation"
    );
    assert_eq!(
        fenced,
        Ok(IggyError::TransientNotAccepted),
        "the isolated primary must close partition admission within {FENCE_BOUND:?}"
    );
    assert_eq!(
        write.err(),
        Some(IggyError::TransientNotAccepted),
        "the isolated primary must refuse a write with 58"
    );
    assert!(
        refused_after <= FENCE_BOUND,
        "the write was refused {refused_after:?} after isolation, past {FENCE_BOUND:?}"
    );

    timeout(
        DATA_FAILOVER_BUDGET,
        client.send_messages(&stream, &topic, &partitioning, &mut [message(ADMITTED[1])]),
    )
    .await
    .expect("the cluster must answer a write after the peers resume")
    .expect("the cluster must accept a write after the peers resume");
    let polled = client
        .poll_messages(
            &stream,
            &topic,
            Some(partition_id),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            u32::try_from(ADMITTED.len() + 1).unwrap(),
            false,
        )
        .await
        .unwrap();
    let payloads: Vec<&[u8]> = polled
        .messages
        .iter()
        .map(|message| message.payload.as_ref())
        .collect();
    assert_eq!(
        payloads,
        ADMITTED.map(str::as_bytes),
        "a write refused with 58 must not reach the log"
    );
}

fn raw_send_body(partition_id: u32, payload: &str) -> Bytes {
    let stream_id = WireIdentifier::named(STREAM_NAME).unwrap();
    let topic_id = WireIdentifier::named(TOPIC_NAME).unwrap();
    let partitioning = WirePartitioning::PartitionId(partition_id);
    let messages = [RawMessage {
        id: 0,
        origin_timestamp: 0,
        headers: None,
        payload: payload.as_bytes(),
    }];
    let mut body = BytesMut::with_capacity(SendMessagesEncoder::encoded_size(
        &stream_id,
        &topic_id,
        &partitioning,
        &messages,
    ));
    SendMessagesEncoder::encode(&mut body, &stream_id, &topic_id, &partitioning, &messages)
        .unwrap();
    body.freeze()
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_metadata_view_moved_when_creating_topic_should_route_to_its_assigned_primary(
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

    // With no existing partitions, the placement tie goes to replica 0,
    // independently of the metadata primary elected above.
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
    let route_client = connect_without_login(harness.node(leader).tcp_addr().unwrap()).await;
    route_client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let route = partition_route(&route_client, &stream_id, &topic_id, PARTITION_ID).await;
    assert_eq!(route.primary_replica, 0);
    assert_eq!(route.view, INITIAL_VIEW);

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

    // The session connection follows metadata leadership, while the write
    // must reach the independently assigned partition primary.
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

/// The node that receives a partition request serves it through its owner's
/// primary gate first. The partition primary serves every request without a
/// relay, and looks itself up only once, before the caller's first write
/// Registers a session there. A backup looks the primary up and relays once,
/// a read after its owner refused and a write before it Registers anything,
/// and the hop target serves the relay without a lookup, under the one
/// session it Registers for the caller.
#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_split_primaries_when_http_calls_each_node_should_relay_only_from_a_backup(
    harness: &mut TestHarness,
) {
    let (partition_primary, backup, _) = seed_split_primaries(harness).await;
    assert_ne!(partition_primary, backup);
    let primary_http =
        HttpClient::login_root_no_redirect(node_url(harness, partition_primary)).await;
    let backup_http = HttpClient::login_root_no_redirect(node_url(harness, backup)).await;

    let before = RoutingCost::of(&primary_http).await;
    let requests = call_every_partition_route(&primary_http).await;
    assert_eq!(
        RoutingCost::of(&primary_http).await.since(before),
        RoutingCost {
            probes: 1,
            session_registers: 1,
            ..RoutingCost::default()
        },
        "{requests} requests on the partition primary"
    );

    let target_before = RoutingCost::of(&primary_http).await;
    let before = RoutingCost::of(&backup_http).await;
    let requests = call_every_partition_route(&backup_http).await;
    assert_eq!(
        RoutingCost::of(&backup_http).await.since(before),
        RoutingCost {
            probes: requests,
            relays: requests,
            ..RoutingCost::default()
        },
        "{requests} requests on a backup: one lookup and one relay each, and no Register"
    );
    assert_eq!(
        RoutingCost::of(&primary_http).await.since(target_before),
        RoutingCost {
            session_registers: 1,
            ..RoutingCost::default()
        },
        "the hop target serves each relay without a lookup"
    );
}

/// A linearizable poll on a node that is not the metadata primary is served at
/// or above the metadata primary's commit floor instead of redirected to it.
/// The node misses the commit while it is paused, so only its hold for the
/// floor makes the answer cover the commit. The partition primary serves the
/// poll itself and another backup relays it once. Only a cluster node reads
/// the floor.
#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_split_primaries_when_polling_linearizable_off_the_metadata_primary_should_serve_at_the_commit_floor(
    harness: &mut TestHarness,
) {
    let (partition_primary, metadata_primary, _) = seed_split_primaries(harness).await;
    let other_backup = (0..harness.cluster_size())
        .find(|node| ![partition_primary, metadata_primary].contains(node))
        .expect("a node that is neither primary");
    let leader = HttpClient::login_root_no_redirect(node_url(harness, metadata_primary)).await;
    let poll = format!(
        "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages?consumer_id={RELAY_CONSUMER_ID}\
         &partition_id={PARTITION_ID}&kind=offset&value=0&count={}&auto_commit=false\
         &consistency=linearizable",
        GROUP_PAYLOADS.len()
    );
    let mut committed = 0;
    for (node, relays) in [(partition_primary, 0), (other_backup, 1)] {
        let http = HttpClient::login_root_no_redirect(node_url(harness, node)).await;
        let before = RoutingCost::of(&http).await;
        pause_node(harness, node, true);
        let response = leader
            .post_json(
                "/streams",
                &serde_json::json!({ "name": format!("{FLOOR_STREAM}-{node}") }),
            )
            .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "commit an op node {node} has not applied"
        );
        committed = applied_op(&response);
        let (response, ()) = tokio::join!(
            http.client
                .get(http.url(&poll))
                .bearer_auth(&http.token)
                .send(),
            async {
                wait_for_queued_relays(harness, node, 1).await;
                pause_node(harness, node, false);
            },
        );
        let (status, body) = status_and_body(response.expect("linearizable poll")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "linearizable poll on node {node}: {body}"
        );
        let polled: PolledMessages = serde_json::from_str(&body).expect("decode PolledMessages");
        assert_eq!(polled.messages.len(), GROUP_PAYLOADS.len());
        assert!(
            polled.context.metadata_op >= committed,
            "the poll on node {node} served at {:?}, below the committed op {committed}",
            polled.context
        );
        assert_eq!(
            RoutingCost::of(&http).await.since(before),
            RoutingCost {
                probes: relays,
                relays,
                floor_reads: 1,
                session_registers: 0,
            },
            "linearizable poll on node {node}"
        );
    }

    let before = RoutingCost::of(&leader).await;
    let response = leader.get(&poll).await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "linearizable poll on the leader"
    );
    assert_eq!(
        RoutingCost::of(&leader).await.since(before),
        RoutingCost {
            probes: 1,
            relays: 1,
            ..RoutingCost::default()
        },
        "the metadata primary reads no floor"
    );

    let floor = leader.url(METADATA_FLOOR_PATH);
    let response = leader
        .client
        .get(&floor)
        .bearer_auth(&leader.token)
        .send()
        .await
        .expect("unproven floor read");
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "unproven floor read"
    );
    let response = leader
        .client
        .get(&floor)
        .header(
            RELAY_PROOF_HEADER,
            fresh_relay_proof("GET", METADATA_FLOOR_PATH, ""),
        )
        .send()
        .await
        .expect("proven floor read");
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "proven floor read"
    );
    assert!(applied_op(&response) >= committed);
}

/// What routing its HTTP partition requests cost one node, from its counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RoutingCost {
    probes: u64,
    relays: u64,
    floor_reads: u64,
    session_registers: u64,
}

impl RoutingCost {
    async fn of(http: &HttpClient) -> Self {
        let metrics = metrics(http).await;
        Self {
            probes: counter(&metrics, PROBES_METRIC),
            relays: counter(&metrics, RELAYS_METRIC),
            floor_reads: counter(&metrics, FLOOR_READS_METRIC),
            session_registers: counter(&metrics, SESSION_REGISTERS_METRIC),
        }
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            probes: self.probes - earlier.probes,
            relays: self.relays - earlier.relays,
            floor_reads: self.floor_reads - earlier.floor_reads,
            session_registers: self.session_registers - earlier.session_registers,
        }
    }
}

async fn metrics(http: &HttpClient) -> String {
    let response = http.get("/metrics").await;
    assert_eq!(response.status(), StatusCode::OK, "metrics scrape");
    response.text().await.expect("metrics text")
}

fn counter(metrics: &str, name: &str) -> u64 {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
        .unwrap_or_else(|| panic!("{name} missing from the exposition:\n{metrics}"))
}

/// Send, poll, and store, read and delete an offset through `http`, each
/// answered as the partition primary answers it. Returns the request count.
async fn call_every_partition_route(http: &HttpClient) -> u64 {
    let messages = http.url(&format!(
        "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages"
    ));
    let offsets = http.url(&format!(
        "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/consumer-offsets"
    ));
    let store = StoreConsumerOffset {
        context: None,
        consumer: Consumer::new(Identifier::numeric(RELAY_CONSUMER_ID).unwrap()),
        partition_id: Some(PARTITION_ID),
        offset: 0,
    };
    let requests = [
        (
            http.client
                .post(&messages)
                .json(&send_body(RELAYED_PAYLOADS[0])),
            StatusCode::CREATED,
        ),
        (
            http.client.get(format!(
                "{messages}?consumer_id={RELAY_CONSUMER_ID}&partition_id={PARTITION_ID}\
                 &kind=offset&value=0&count=1&auto_commit=false"
            )),
            StatusCode::OK,
        ),
        (
            http.client.put(&offsets).json(&store),
            StatusCode::NO_CONTENT,
        ),
        (
            http.client.get(format!(
                "{offsets}?consumer_id={RELAY_CONSUMER_ID}&partition_id={PARTITION_ID}"
            )),
            StatusCode::OK,
        ),
        (
            http.client.delete(format!(
                "{offsets}/{RELAY_CONSUMER_ID}?partition_id={PARTITION_ID}"
            )),
            StatusCode::NO_CONTENT,
        ),
    ];
    let count = requests.len() as u64;
    for (request, expected) in requests {
        let response = request
            .bearer_auth(&http.token)
            .send()
            .await
            .expect("partition request");
        let (status, body) = status_and_body(response).await;
        assert_eq!(status, expected, "partition request: {body}");
    }
    count
}

/// A relayed request carries the partition the contacted node chose and the
/// node's proof of it. A backup refuses a proven relay with `NotPrimary`
/// instead of relaying it again, naming the primary of its view so that a
/// contacted node with an older view reaches it in one more hop. It refuses
/// relay fields without a valid, unexpired proof, takes a malformed proven
/// route as the caller's error, and ignores a route without the relay marker.
#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_split_primaries_when_a_relayed_request_reaches_a_backup_should_refuse_not_primary(
    harness: &mut TestHarness,
) {
    let (partition_primary, backup, partition_view) = seed_split_primaries(harness).await;
    assert_ne!(partition_primary, backup);
    let http = HttpClient::login_root_no_redirect(node_url(harness, backup)).await;
    let send_path = format!("/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages");
    let poll_path = format!(
        "{send_path}?consumer_id={RELAY_CONSUMER_ID}&partition_id={PARTITION_ID}\
         &kind=offset&value=0&count=1&auto_commit=false"
    );
    let send = |route: &str| {
        http.client
            .post(http.url(&send_path))
            .bearer_auth(&http.token)
            .header(PARTITION_ROUTE_HEADER, route)
            .json(&send_body(RELAYED_PAYLOADS[0]))
    };
    let relayed_send = |route: &str, proof: String| {
        send(route)
            .header(FORWARDED_HEADER, "1")
            .header(RELAY_PROOF_HEADER, proof)
    };

    for (request, name) in [
        (
            relayed_send(
                ROUTE_AT_FIRST_OP,
                fresh_relay_proof("POST", &send_path, ROUTE_AT_FIRST_OP),
            ),
            "send",
        ),
        (
            http.client
                .get(http.url(&poll_path))
                .bearer_auth(&http.token)
                .header(FORWARDED_HEADER, "1")
                .header(PARTITION_ROUTE_HEADER, ROUTE_AT_FIRST_OP)
                .header(
                    RELAY_PROOF_HEADER,
                    fresh_relay_proof("GET", &poll_path, ROUTE_AT_FIRST_OP),
                ),
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
        assert_eq!(
            response
                .headers()
                .get(PARTITION_PRIMARY_HEADER)
                .map(|value| value.to_str().expect("an ASCII primary")),
            Some(format!("{partition_view},{partition_primary}").as_str()),
            "relayed {name} names the primary of the backup's view"
        );
        let body: serde_json::Value = response.json().await.expect("decode the error body");
        assert_eq!(
            body["id"],
            IggyError::NotPrimary.as_code(),
            "relayed {name}"
        );
    }

    let unreachable_frontier = format!("0,1,0,{},5000", u64::MAX);
    let expired = relay_proof(
        "POST",
        &send_path,
        ROUTE_AT_FIRST_OP,
        IggyTimestamp::now().as_micros() - RELAY_PROOF_LIFETIME_MICROS,
    );
    for (request, name) in [
        (
            send(ROUTE_AT_FIRST_OP).header(FORWARDED_HEADER, "1"),
            "no proof",
        ),
        (
            relayed_send(
                &unreachable_frontier,
                fresh_relay_proof("POST", &send_path, ROUTE_AT_FIRST_OP),
            ),
            "a forged frontier",
        ),
        (relayed_send(ROUTE_AT_FIRST_OP, expired), "an expired proof"),
    ] {
        let response = request.send().await.expect("unproven relay");
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "relay with {name}"
        );
    }

    let response = relayed_send("0,1", fresh_relay_proof("POST", &send_path, "0,1"))
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

pub(super) fn pause_node(harness: &TestHarness, node: usize, paused: bool) {
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
        "TestE2E_SplitPrimaryPollsPreserveCoordinatorMembership",
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
        "TestE2E_SplitPrimaryManualCommitPreservesMembership",
    )
    .await;
}

async fn run_go_split_primary_test(harness: &mut TestHarness, test: &str) {
    let (_, metadata_primary, _) = seed_split_primaries(harness).await;
    let output = tokio::process::Command::new("go")
        .args([
            "test",
            "./tests",
            "-run",
            &format!("^{test}$"),
            "-count=1",
            "-v",
        ])
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
    // `go test` also succeeds when the test skips or no test matches.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains(&format!("--- PASS: {test} (")),
        "Go SDK routing regression {test} failed or did not run:\n{stdout}\n{}",
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
    // The restarted backup refuses partition requests with 58 until a probe of
    // the new metadata primary credits it, and callers send it raw requests.
    timeout(PRECONDITION_BUDGET, async {
        while !harness.node(backup).partition_admission_open() {
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the restarted backup must admit partition requests");

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

/// A hop target serves relays from a budget of their own. Stalled relayed
/// uploads that hold every slot leave the node's own relays flowing, a caller
/// that hangs up frees its slots at once, and the rest end at the deadline.
#[iggy_harness(cluster_nodes = 3, server(
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_stalled_http_relay_uploads_when_every_target_slot_is_held_should_keep_relaying_and_free_them(
    harness: &mut TestHarness,
) {
    let route = relay_route(spread_relay_target(harness).await, 0);
    let http = HttpClient::login_root_no_redirect(node_url(harness, RELAY_TARGET_NODE)).await;
    let mut tokens = Vec::new();
    for _ in 0..=RELAY_TARGET_BUDGET / RELAY_TARGET_SHARE {
        let session = http
            .login(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await;
        tokens.push(session.token);
    }
    let (fresh, stalling) = tokens
        .split_last()
        .expect("one credential per share, plus one");
    let (cancelled, waiting) = stalling.split_last().expect("credentials that stall");
    let address = harness
        .node(RELAY_TARGET_NODE)
        .http_addr()
        .expect("hop target HTTP address");
    // The credential that hangs up opens last, so its deadline comes last too.
    let opened = Instant::now();
    let mut uploads = Vec::with_capacity(RELAY_TARGET_BUDGET);
    for token in waiting.iter().chain([cancelled]) {
        for _ in 0..RELAY_TARGET_SHARE {
            uploads.push(stalled_relay_upload(address, token, &route).await);
        }
    }

    timeout(PRECONDITION_BUDGET, async {
        loop {
            let (status, body) = relayed_poll(&http, fresh, &route).await;
            if status == StatusCode::SERVICE_UNAVAILABLE && body.contains("relay_target_busy") {
                break;
            }
            assert_eq!(
                status,
                StatusCode::OK,
                "relayed poll before the budget fills: {body}"
            );
            sleep(RELAY_PROBE_INTERVAL).await;
        }
    })
    .await
    .expect("the stalled uploads must hold every hop-target slot");
    let (status, body) = relayed_poll(&http, cancelled, &route).await;
    assert_eq!(
        status,
        StatusCode::TOO_MANY_REQUESTS,
        "saturated credential: {body}"
    );
    assert!(body.contains("too_many_relayed_requests"), "{body}");

    let response = http
        .post_json(
            "/streams",
            &serde_json::json!({ "name": "created-while-every-target-slot-is-held" }),
        )
        .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "control-plane relay from the hop target"
    );
    let response = http
        .client
        .post(http.url(&format!(
            "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages"
        )))
        .bearer_auth(&http.token)
        .json(&send_body(RELAYED_PAYLOADS[0]))
        .send()
        .await
        .expect("partition relay from the hop target");
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "partition relay from the hop target"
    );

    drop(uploads.split_off(uploads.len() - RELAY_TARGET_SHARE));
    timeout(PRECONDITION_BUDGET, async {
        loop {
            let (status, body) = relayed_poll(&http, cancelled, &route).await;
            if status == StatusCode::OK {
                break;
            }
            assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
            sleep(RELAY_PROBE_INTERVAL).await;
        }
    })
    .await
    .expect("a caller hanging up mid-body must free its slots");
    let mut unread = [0; 1];
    assert!(
        matches!(
            uploads[0].try_read(&mut unread),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ),
        "the slots must be freed by the hang-up, before any deadline"
    );

    for upload in uploads {
        let answer = timeout(
            (opened + RELAY_BUDGET + DEADLINE_SLACK).saturating_duration_since(Instant::now()),
            stalled_answer(upload),
        )
        .await
        .expect("a stalled upload must end at its deadline");
        assert!(answer.starts_with("HTTP/1.1 503"), "{answer}");
        assert!(
            answer.contains(&format!(
                "\"id\":{}",
                IggyError::TransientNotAccepted.as_code()
            )),
            "{answer}"
        );
    }
    let (status, body) = relayed_poll(&http, fresh, &route).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "every slot must be free again: {body}"
    );
}

/// A metadata backup relays control-plane writes and partition requests from
/// one pool of outbound slots. Control-plane uploads that stall mid-body take
/// no slot, so the node's partition relays keep flowing, and each stalled
/// upload ends at the relay deadline.
#[iggy_harness(cluster_nodes = 3, server(
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_stalled_http_metadata_uploads_when_they_match_the_forward_slots_should_keep_relaying_and_end_them(
    harness: &mut TestHarness,
) {
    spread_relay_target(harness).await;
    let http = HttpClient::login_root_no_redirect(node_url(harness, RELAY_TARGET_NODE)).await;
    let address = harness
        .node(RELAY_TARGET_NODE)
        .http_addr()
        .expect("metadata backup HTTP address");
    let counted = counter(&metrics(&http).await, REQUESTS_METRIC);
    let opened = Instant::now();
    let mut uploads = Vec::new();
    for _ in 0..FORWARD_SLOTS {
        uploads.push(
            stalled_upload(
                address,
                "/streams",
                &format!("authorization: Bearer {}\r\n", http.token),
            )
            .await,
        );
    }
    // Each scrape counts itself too.
    timeout(PRECONDITION_BUDGET, async {
        for scrapes in 1.. {
            if counter(&metrics(&http).await, REQUESTS_METRIC) >= counted + FORWARD_SLOTS + scrapes
            {
                break;
            }
            sleep(RELAY_PROBE_INTERVAL).await;
        }
    })
    .await
    .expect("the metadata backup must take the head of every stalled upload");

    let response = http
        .client
        .post(http.url(&format!(
            "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages"
        )))
        .bearer_auth(&http.token)
        .json(&send_body(RELAYED_PAYLOADS[0]))
        .send()
        .await
        .expect("partition relay from the metadata backup");
    let (status, body) = status_and_body(response).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "partition relay while control-plane uploads stall: {body}"
    );

    for upload in uploads {
        let answer = timeout(
            (opened + FORWARD_DEADLINE + DEADLINE_SLACK).saturating_duration_since(Instant::now()),
            stalled_answer(upload),
        )
        .await
        .expect("a stalled upload must end at its deadline");
        assert!(answer.starts_with("HTTP/1.1 503"), "{answer}");
        assert!(
            answer.contains(&format!(
                "\"id\":{}",
                IggyError::TransientNotAccepted.as_code()
            )),
            "{answer}"
        );
    }
}

/// A client cannot pose as a relay. Forged relay fields that ask the hop
/// target to hold for a metadata op the cluster never reaches take no slot and
/// no hold, so the relays to and from the node keep flowing. A genuine relay
/// for an op the cluster has not committed yet still holds until the hop
/// target applies it, then is served.
#[iggy_harness(cluster_nodes = 3, server(
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_forged_http_relays_when_flooding_a_partition_primary_should_refuse_them_without_a_hold(
    harness: &mut TestHarness,
) {
    let incarnation = spread_relay_target(harness).await;
    let admin = connect_without_login(harness.node(0).tcp_addr().unwrap()).await;
    admin
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    admin
        .create_user(
            RELAY_FORGER,
            RELAY_FORGER_PASSWORD,
            UserStatus::Active,
            None,
        )
        .await
        .expect("create a user without permissions");
    let root = HttpClient::login_root_no_redirect(node_url(harness, RELAY_TARGET_NODE)).await;
    // The hop target verifies the forger's bearer only once it applied the user.
    let forger = timeout(PRECONDITION_BUDGET, async {
        loop {
            let response = root
                .client
                .post(root.url("/users/login"))
                .json(&serde_json::json!({
                    "username": RELAY_FORGER,
                    "password": RELAY_FORGER_PASSWORD,
                }))
                .send()
                .await
                .expect("login request");
            if response.status().is_success() {
                break root.with_token(access_token(response).await);
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the hop target must apply the new user");
    let entry = HttpClient::login_root_no_redirect(node_url(harness, RELAY_ENTRY_NODE)).await;
    let send_path = format!("/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages");
    let unreachable_frontier = relay_route(incarnation, u64::MAX);
    let proof_of_another_route =
        fresh_relay_proof("POST", &send_path, &relay_route(incarnation, 0));
    let forged = (0..RELAY_TARGET_BUDGET).map(|index| {
        let request = forger
            .client
            .post(forger.url(&send_path))
            .bearer_auth(&forger.token)
            .header(FORWARDED_HEADER, "1")
            .header(PARTITION_ROUTE_HEADER, &unreachable_frontier)
            .json(&send_body_to(RELAY_TARGET_PARTITION, "forged"));
        if index % 2 == 0 {
            request.send()
        } else {
            request
                .header(RELAY_PROOF_HEADER, &proof_of_another_route)
                .send()
        }
    });

    let started = Instant::now();
    let (forged, (to_target, from_target)) =
        tokio::join!(futures::future::join_all(forged), async {
            sleep(FORGED_HEAD_START).await;
            let to_target = entry
                .client
                .post(entry.url(&send_path))
                .bearer_auth(&entry.token)
                .json(&send_body_to(RELAY_TARGET_PARTITION, RELAYED_PAYLOADS[0]))
                .send()
                .await
                .expect("relay to the hop target");
            let from_target = root
                .client
                .post(root.url(&send_path))
                .bearer_auth(&root.token)
                .json(&send_body(RELAYED_PAYLOADS[1]))
                .send()
                .await
                .expect("relay from the hop target");
            (to_target, from_target)
        },);
    for response in forged {
        let (status, body) = status_and_body(response.expect("forged relay")).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "forged relay: {body}");
    }
    assert!(
        started.elapsed() < RELAY_BUDGET,
        "no forged relay may hold for its metadata op"
    );
    let (status, body) = status_and_body(to_target).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "relay to the hop target: {body}"
    );
    let (status, body) = status_and_body(from_target).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "relay from the hop target: {body}"
    );

    let leader = leader_node_index_via(harness, RELAY_TARGET_NODE).await;
    let leader_http = HttpClient::login_root_no_redirect(node_url(harness, leader)).await;
    let committed = applied_op(&leader_http.get("/streams").await);
    let next_op = relay_route(incarnation, committed + 1);
    let held = tokio::spawn(
        root.client
            .post(root.url(&send_path))
            .bearer_auth(&root.token)
            .header(FORWARDED_HEADER, "1")
            .header(PARTITION_ROUTE_HEADER, &next_op)
            .header(
                RELAY_PROOF_HEADER,
                fresh_relay_proof("POST", &send_path, &next_op),
            )
            .json(&send_body_to(RELAY_TARGET_PARTITION, HELD_PAYLOAD))
            .send(),
    );
    sleep(HOLD_PROBE).await;
    assert!(
        !held.is_finished(),
        "a genuine relay must hold for an op the cluster has not committed"
    );
    let response = leader_http
        .post_json("/streams", &serde_json::json!({ "name": FRONTIER_STREAM }))
        .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "commit the op the held relay waits for"
    );
    let response = held.await.expect("held relay task").expect("held relay");
    let served_at = response
        .status()
        .is_success()
        .then(|| applied_op(&response));
    let (status, body) = status_and_body(response).await;
    assert_eq!(status, StatusCode::CREATED, "held relay: {body}");
    assert!(
        served_at > Some(committed),
        "the hop target must serve the held relay only after applying its op"
    );
}

/// A caller can name any metadata op in the context of its request, and a hop
/// target holds a relay until it applied the op of its route. The contacted
/// node relays only an op it applied, so requests that name an op the cluster
/// never reaches wait there, relay nothing and take no hop-target slot: the
/// hop target still admits a relay of the same credential.
#[iggy_harness(cluster_nodes = 3, server(
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_contexts_naming_an_unreached_op_when_relaying_should_hold_them_on_the_contacted_node(
    harness: &mut TestHarness,
) {
    let incarnation = spread_relay_target(harness).await;
    let entry = HttpClient::login_root_no_redirect(node_url(harness, RELAY_ENTRY_NODE)).await;
    let send_url = entry.url(&format!(
        "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages"
    ));
    let unreached_body = SendMessages {
        context: Some(PartitionContext {
            incarnation,
            owner_generation: 0,
            metadata_op: u64::MAX,
        }),
        ..send_body_to(RELAY_TARGET_PARTITION, "names-an-unreached-op")
    };
    let send = |body: &SendMessages| {
        entry
            .client
            .post(&send_url)
            .bearer_auth(&entry.token)
            .json(body)
            .send()
    };

    let before = RoutingCost::of(&entry).await;
    let (unreached, admitted) = tokio::join!(
        futures::future::join_all((0..RELAY_TARGET_SHARE).map(|_| send(&unreached_body))),
        async {
            sleep(FORGED_HEAD_START).await;
            send(&send_body_to(RELAY_TARGET_PARTITION, RELAYED_PAYLOADS[0]))
                .await
                .expect("relay of the same credential")
        },
    );
    let (status, body) = status_and_body(admitted).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "relay of the same credential: {body}"
    );
    for response in unreached {
        let (status, body) = status_and_body(response.expect("send naming an unreached op")).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "send naming an unreached op: {body}"
        );
        let error: serde_json::Value = serde_json::from_str(&body).expect("decode the error body");
        assert_eq!(error["id"], IggyError::TransientNotAccepted.as_code());
    }
    assert_eq!(
        RoutingCost::of(&entry).await.since(before).relays,
        1,
        "the contacted node must not relay an op it has not applied"
    );
}

/// A three-partition topic places partition N on node N while node 0 stays
/// the metadata primary, so node 1 serves partition 1 and relays partition 0
/// and every control-plane write. Returns the incarnation of partition 1.
async fn spread_relay_target(harness: &TestHarness) -> u64 {
    let client = connect_without_login(harness.node(0).tcp_addr().unwrap()).await;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client.create_stream(STREAM_NAME).await.unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let options = TopicCreateOptions {
        partitions_count: Some(SPREAD_PARTITIONS),
        message_expiry: Some(IggyExpiry::NeverExpire),
        ..TopicCreateOptions::default()
    };
    client
        .create_topic(&stream, TOPIC_NAME, &options)
        .await
        .unwrap();
    let relayed = partition_route(&client, &stream, &topic, PARTITION_ID).await;
    assert_eq!(relayed.primary_replica, 0);
    let served = partition_route(&client, &stream, &topic, RELAY_TARGET_PARTITION).await;
    assert_eq!(usize::from(served.primary_replica), RELAY_TARGET_NODE);
    assert_ne!(
        leader_node_index_via(harness, RELAY_TARGET_NODE).await,
        RELAY_TARGET_NODE
    );
    served.incarnation
}

/// The route a relay of partition 1 carries, with the longest budget a hop
/// target grants.
fn relay_route(incarnation: u64, metadata_op: u64) -> String {
    format!(
        "{RELAY_TARGET_PARTITION},{incarnation},0,{metadata_op},{}",
        RELAY_BUDGET.as_millis()
    )
}

fn send_body_to(partition_id: u32, payload: &str) -> SendMessages {
    SendMessages {
        partitioning: Partitioning::partition_id(partition_id),
        ..send_body(payload)
    }
}

/// Prove relay fields the way a cluster node does: HMAC-SHA256 under the key
/// BLAKE3 derives from the cluster key material, over the length-prefixed
/// method, path and query, and route, then the expiry in unix microseconds.
fn relay_proof(method: &str, path_and_query: &str, route: &str, expires_at: u64) -> String {
    let key = ring::hmac::Key::new(
        ring::hmac::HMAC_SHA256,
        &blake3::derive_key(RELAY_KEY_CONTEXT, RELAY_KEY_MATERIAL.as_bytes()),
    );
    let mut signed = Vec::new();
    for field in [method, path_and_query, route] {
        signed.extend_from_slice(&(field.len() as u64).to_le_bytes());
        signed.extend_from_slice(field.as_bytes());
    }
    signed.extend_from_slice(&expires_at.to_le_bytes());
    format!(
        "{expires_at},{}",
        hex::encode(ring::hmac::sign(&key, &signed))
    )
}

fn fresh_relay_proof(method: &str, path_and_query: &str, route: &str) -> String {
    relay_proof(
        method,
        path_and_query,
        route,
        IggyTimestamp::now().as_micros() + RELAY_PROOF_LIFETIME_MICROS,
    )
}

/// Poll partition 1 on node 1 as a relay carrying `route`.
async fn relayed_poll(http: &HttpClient, token: &str, route: &str) -> (StatusCode, String) {
    let path_and_query = format!(
        "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages?consumer_id={RELAY_CONSUMER_ID}\
         &partition_id={RELAY_TARGET_PARTITION}&kind=offset&value=0&count=1&auto_commit=false"
    );
    let response = http
        .client
        .get(http.url(&path_and_query))
        .bearer_auth(token)
        .header(FORWARDED_HEADER, "1")
        .header(PARTITION_ROUTE_HEADER, route)
        .header(
            RELAY_PROOF_HEADER,
            fresh_relay_proof("GET", &path_and_query, route),
        )
        .send()
        .await
        .expect("relayed poll");
    status_and_body(response).await
}

/// Send the head of a relayed send and the first byte of its body, then stall.
async fn stalled_relay_upload(address: SocketAddr, token: &str, route: &str) -> TcpStream {
    let path = format!("/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages");
    let proof = fresh_relay_proof("POST", &path, route);
    stalled_upload(
        address,
        &path,
        &format!(
            "authorization: Bearer {token}\r\n{FORWARDED_HEADER}: 1\r\n\
             {PARTITION_ROUTE_HEADER}: {route}\r\n{RELAY_PROOF_HEADER}: {proof}\r\n"
        ),
    )
    .await
}

/// Send the head of a POST to `path` with `headers`, each line ended by CRLF,
/// and the first byte of its body, then stall.
async fn stalled_upload(address: SocketAddr, path: &str, headers: &str) -> TcpStream {
    let mut upload = TcpStream::connect(address)
        .await
        .expect("connect to the node");
    let head = format!(
        "POST {path} HTTP/1.1\r\nhost: {address}\r\n{headers}\
         content-type: application/json\r\ncontent-length: {STALLED_BODY_LENGTH}\r\n\r\n{{"
    );
    upload
        .write_all(head.as_bytes())
        .await
        .expect("send the head of an upload");
    upload
}

/// The hop target's answer to a stalled upload. It closes the connection after
/// answering, because the rest of the body never comes; a reset after the
/// answer leaves what was read.
async fn stalled_answer(mut upload: TcpStream) -> String {
    let mut answer = Vec::new();
    let _ = upload.read_to_end(&mut answer).await;
    String::from_utf8_lossy(&answer).into_owned()
}
