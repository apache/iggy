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

//! In-process gateways that share their groups through the coordinator log, and the group
//! requests the tests send them - compiled into each integration test binary via `#[path]`.
//!
//! Callers must also declare the `codec`, `tcp` and `wire` modules at their crate root, as for
//! `tcp.rs`.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    FindCoordinatorResponse, GroupId, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse, SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use iggy_gateway_kafka::bridge::{IggyBridge, IggyBridgeConfig};
use iggy_gateway_kafka::group::GroupCoordinatorConfig;
use iggy_gateway_kafka::listener::{BrokerEntry, ListenerConfig, SecurityProtocol, parse_brokers};
use iggy_gateway_kafka::protocol::api::{
    API_KEY_FIND_COORDINATOR, API_KEY_HEARTBEAT, API_KEY_JOIN_GROUP, API_KEY_OFFSET_COMMIT,
    API_KEY_OFFSET_FETCH, API_KEY_SYNC_GROUP, ERROR_NONE,
};
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

use super::{tcp, wire};

/// How long the gateways may take to share out the coordinator partitions.
pub const SETTLE: Duration = Duration::from_secs(30);

/// The topic the groups commit offsets for.
pub const TOPIC: &str = "orders";

const MAX_FRAME: usize = 8 * 1024 * 1024;

/// One gateway of a test, until it stops.
pub struct Gateway {
    pub node_id: u16,
    pub addr: SocketAddr,
    shutdown: broadcast::Sender<()>,
    task: JoinHandle<()>,
}

impl Gateway {
    pub async fn stop(self) {
        let _ = self.shutdown.send(());
        tokio::time::timeout(Duration::from_secs(20), self.task)
            .await
            .expect("the gateway stops within its drain")
            .expect("the gateway task does not panic");
    }
}

/// One gateway per entry of `bridges`, nodes 1 to its length, that list each other and share their
/// groups through the coordinator log. Gateway `n` reaches Iggy through `bridges[n - 1]`. A first
/// join completes at once.
pub async fn start_gateways(bridges: &[IggyBridgeConfig]) -> Vec<Gateway> {
    let sockets: Vec<_> = bridges
        .iter()
        .map(|_| bind_listener("127.0.0.1:0").expect("bind ephemeral port"))
        .collect();
    let addrs: Vec<SocketAddr> = sockets
        .iter()
        .map(|socket| socket.local_addr().expect("local addr"))
        .collect();
    let brokers = listed(&addrs);
    let mut gateways = Vec::with_capacity(bridges.len());
    for ((node_id, socket), (addr, bridge)) in
        (1..).zip(sockets).zip(addrs.into_iter().zip(bridges))
    {
        let config = gateway_config(node_id, brokers.clone());
        gateways.push(start_gateway(bridge.clone(), config, socket, addr).await);
    }
    gateways
}

/// `IGGY_KAFKA_BROKERS` for gateways 1 to the length of `addrs`.
pub fn listed(addrs: &[SocketAddr]) -> Vec<BrokerEntry> {
    let listed: Vec<String> = (1..)
        .zip(addrs)
        .map(|(node_id, addr): (u16, _)| format!("{node_id}@{addr}"))
        .collect();
    parse_brokers(&listed.join(",")).expect("listed gateways")
}

pub fn gateway_config(node_id: u16, brokers: Vec<BrokerEntry>) -> GatewayConfig {
    GatewayConfig {
        node_id,
        brokers,
        coordinator_log: true,
        shutdown_drain_timeout: Duration::from_secs(1),
        group: GroupCoordinatorConfig {
            initial_rebalance_delay: Duration::ZERO,
            ..GroupCoordinatorConfig::default()
        },
        ..GatewayConfig::default()
    }
}

pub async fn start_gateway(
    bridge: IggyBridgeConfig,
    config: GatewayConfig,
    socket: TcpListener,
    addr: SocketAddr,
) -> Gateway {
    let node_id = config.node_id;
    let listener = BoundListener::new(
        ListenerConfig::new(SecurityProtocol::Plaintext, addr.to_string()),
        socket,
    );
    let bridge = Arc::new(connect(bridge).await);
    let gateway = KafkaGateway::new(config).with_bridge(Some(bridge));
    let (shutdown, receiver) = broadcast::channel(1);
    let task = tokio::spawn(async move {
        if let Err(error) = gateway.run(vec![listener], receiver).await {
            panic!("gateway {node_id} failed: {error}");
        }
    });
    Gateway {
        node_id,
        addr,
        shutdown,
        task,
    }
}

/// A bridge, retried while a new cluster elects its metadata primary.
pub async fn connect(config: IggyBridgeConfig) -> IggyBridge {
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        match IggyBridge::connect(config.clone()).await {
            Ok(bridge) => return bridge,
            Err(error) => assert!(
                tokio::time::Instant::now() < deadline,
                "no bridge within {SETTLE:?}: {error}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Sends `request` on a connection of its own and decodes the answer. Encodes before it returns,
/// so the future holds no reference to the request.
pub fn call<Q: Encodable, A: Decodable>(
    addr: SocketAddr,
    key: i16,
    version: i16,
    request: &Q,
) -> impl Future<Output = A> {
    send(addr, key, version, wire::encode(request, version))
}

pub async fn send<A: Decodable>(addr: SocketAddr, key: i16, version: i16, body: Bytes) -> A {
    try_send(addr, key, version, body)
        .await
        .expect("connect to the gateway")
}

/// [`send`], or `None` when nothing accepts at `addr`, as for a gateway that stopped.
pub async fn try_send<A: Decodable>(
    addr: SocketAddr,
    key: i16,
    version: i16,
    body: Bytes,
) -> Option<A> {
    let frame = tcp::build_request_frame(key, version, 7, Some("coordinator-test"), &body);
    let mut stream = TcpStream::connect(addr).await.ok()?;
    stream.write_all(&frame).await.expect("send the request");
    let payload = tcp::read_response_frame(&mut stream, MAX_FRAME).await;
    let (_, body) = tcp::parse_response_payload(key, version, payload);
    Some(wire::decode(body, version))
}

/// Polls `check` until it returns a value or `limit` passes.
pub async fn eventually<T, F: Future<Output = Option<T>>>(
    limit: Duration,
    what: &str,
    mut check: impl FnMut() -> F,
) -> T {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what} did not happen within {limit:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The node id each group's coordinator has according to the gateway at `addr`, or `None` when
/// the gateway names none for one of them.
pub async fn coordinators(addr: SocketAddr, groups: &[String]) -> Option<Vec<i32>> {
    let request = FindCoordinatorRequest::default().with_coordinator_keys(
        groups
            .iter()
            .map(|group| StrBytes::from_string(group.clone()))
            .collect(),
    );
    let body = wire::encode(&request, 4);
    let response: FindCoordinatorResponse =
        try_send(addr, API_KEY_FIND_COORDINATOR, 4, body).await?;
    response
        .coordinators
        .iter()
        .map(|coordinator| (coordinator.error_code == ERROR_NONE).then_some(coordinator.node_id.0))
        .collect()
}

/// The coordinator of `group` once every gateway of `gateways` names the same one.
pub async fn agreed_coordinator(gateways: &[&Gateway], group: &str) -> u16 {
    let groups = [group.to_string()];
    eventually(SETTLE, "agreement on the coordinator", || async {
        let mut named = BTreeSet::new();
        for gateway in gateways {
            named.insert(coordinators(gateway.addr, &groups).await?[0]);
        }
        let node = *named.first()?;
        (named.len() == 1).then(|| u16::try_from(node).expect("a node id"))
    })
    .await
}

pub fn gateway_of(gateways: &[Gateway], node_id: u16) -> &Gateway {
    gateways
        .iter()
        .find(|gateway| gateway.node_id == node_id)
        .expect("a listed gateway")
}

/// One member joins `group` at `addr` and syncs as its leader. Returns its member id and
/// generation.
pub async fn join_and_sync(addr: SocketAddr, group: &str) -> (StrBytes, i32) {
    let join = JoinGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_session_timeout_ms(30_000)
        .with_rebalance_timeout_ms(30_000)
        .with_member_id(StrBytes::new())
        .with_protocol_type(StrBytes::from_static_str("consumer"))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(b"subscription")),
        ]);
    let joined: JoinGroupResponse = call(addr, API_KEY_JOIN_GROUP, 3, &join).await;
    assert_eq!(joined.error_code, ERROR_NONE, "join");
    let sync = SyncGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_generation_id(joined.generation_id)
        .with_member_id(joined.member_id.clone())
        .with_assignments(vec![
            SyncGroupRequestAssignment::default()
                .with_member_id(joined.member_id.clone())
                .with_assignment(Bytes::from_static(b"assignment")),
        ]);
    let synced: SyncGroupResponse = call(addr, API_KEY_SYNC_GROUP, 2, &sync).await;
    assert_eq!(synced.error_code, ERROR_NONE, "sync");
    assert_eq!(synced.assignment, Bytes::from_static(b"assignment"));
    (joined.member_id, joined.generation_id)
}

pub async fn heartbeat(
    addr: SocketAddr,
    group: &str,
    member_id: &StrBytes,
    generation: i32,
) -> i16 {
    try_heartbeat(addr, group, member_id, generation)
        .await
        .expect("connect to the gateway")
}

/// [`heartbeat`], or `None` when nothing accepts at `addr`.
pub async fn try_heartbeat(
    addr: SocketAddr,
    group: &str,
    member_id: &StrBytes,
    generation: i32,
) -> Option<i16> {
    let request = HeartbeatRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_generation_id(generation)
        .with_member_id(member_id.clone());
    let body = wire::encode(&request, 2);
    let response: HeartbeatResponse = try_send(addr, API_KEY_HEARTBEAT, 2, body).await?;
    Some(response.error_code)
}

pub async fn commit(
    addr: SocketAddr,
    group: &str,
    member_id: &StrBytes,
    generation: i32,
    offset: i64,
    metadata: &str,
) -> i16 {
    let request = OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_generation_id_or_member_epoch(generation)
        .with_member_id(member_id.clone())
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(offset)
                        .with_committed_leader_epoch(-1)
                        .with_committed_metadata(Some(StrBytes::from_string(metadata.to_string()))),
                ]),
        ]);
    let response: OffsetCommitResponse = call(addr, API_KEY_OFFSET_COMMIT, 7, &request).await;
    response.topics[0].partitions[0].error_code
}

/// The group error, and the offset and metadata of partition 0.
pub async fn fetch(addr: SocketAddr, group: &str) -> (i16, i64, String) {
    try_fetch(addr, group)
        .await
        .expect("connect to the gateway")
}

/// [`fetch`], or `None` when nothing accepts at `addr`.
pub async fn try_fetch(addr: SocketAddr, group: &str) -> Option<(i16, i64, String)> {
    let request = OffsetFetchRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_topics(Some(vec![
            OffsetFetchRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_indexes(vec![0]),
        ]));
    let body = wire::encode(&request, 6);
    let response: OffsetFetchResponse = try_send(addr, API_KEY_OFFSET_FETCH, 6, body).await?;
    let Some(partition) = response
        .topics
        .first()
        .and_then(|topic| topic.partitions.first())
    else {
        return Some((response.error_code, -1, String::new()));
    };
    Some((
        response.error_code,
        partition.committed_offset,
        partition
            .metadata
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
    ))
}

/// Creates the topic the groups commit for.
pub async fn create_topic(config: IggyBridgeConfig) {
    let bridge = connect(config).await;
    bridge
        .create_kafka_topic(TOPIC, 2)
        .await
        .expect("create the topic");
    bridge.close().await.expect("close the bridge");
}
