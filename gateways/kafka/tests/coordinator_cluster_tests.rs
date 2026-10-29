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

//! Three gateways beside a three-node Iggy cluster, one beside each node, sharing their consumer
//! groups through the coordinator log. A gateway or a node stops, and the groups go on.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/gateways.rs"]
mod gateways;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::net::SocketAddr;
use std::time::Duration;

use kafka_protocol::protocol::StrBytes;
use serial_test::serial;

use iggy_gateway_kafka::protocol::api::{
    ERROR_COORDINATOR_LOAD_IN_PROGRESS, ERROR_COORDINATOR_NOT_AVAILABLE, ERROR_NONE,
    ERROR_NOT_COORDINATOR,
};
use iggy_gateway_kafka::server::bind_listener;

use gateways::{
    Gateway, commit, coordinators, create_topic, eventually, gateway_config, join_and_sync, listed,
    start_gateway, start_gateways, try_fetch, try_heartbeat,
};
use iggy_server::TestCluster;

const NODES: usize = 3;

/// How long a group may take to come back after a node stops: a view change, new sessions, and
/// the coordinator partitions moving.
const RECOVERY: Duration = Duration::from_mins(1);

/// The codes a member retries while its group moves to another gateway.
const MOVING: [i16; 3] = [
    ERROR_NOT_COORDINATOR,
    ERROR_COORDINATOR_LOAD_IN_PROGRESS,
    ERROR_COORDINATOR_NOT_AVAILABLE,
];

/// A cluster and a gateway beside each node, gateway `n + 1` beside node `n`.
async fn start(data_dir: &std::path::Path) -> (TestCluster, Vec<Gateway>) {
    let cluster = TestCluster::spawn(data_dir, NODES).await;
    create_topic(cluster.bridge_config(0)).await;
    let bridges: Vec<_> = (0..NODES).map(|node| cluster.bridge_config(node)).collect();
    let gateways = start_gateways(&bridges).await;
    (cluster, gateways)
}

/// The gateway that coordinates `group`, as the first gateway of `addrs` that names one says.
async fn find(addrs: &[SocketAddr], group: &str) -> Option<SocketAddr> {
    let groups = [group.to_string()];
    for addr in addrs {
        if let Some(named) = coordinators(*addr, &groups).await {
            let node = usize::try_from(named[0]).ok()?;
            return addrs.get(node.checked_sub(1)?).copied();
        }
    }
    None
}

/// A heartbeat of the member, sent where `FindCoordinator` points, as a Kafka consumer does.
/// Retries while the group moves. Fails at once on an answer that would make the member rejoin.
async fn heartbeat_through(
    addrs: &[SocketAddr],
    group: &str,
    member_id: &StrBytes,
    generation: i32,
) {
    eventually(
        RECOVERY,
        "a heartbeat at the group's coordinator",
        || async {
            let coordinator = find(addrs, group).await?;
            // A client of a stopped gateway connects again, as a Kafka consumer does.
            let code = try_heartbeat(coordinator, group, member_id, generation).await?;
            assert!(
                code == ERROR_NONE || MOVING.contains(&code),
                "the member must keep its id and generation, got {code}"
            );
            (code == ERROR_NONE).then_some(())
        },
    )
    .await;
}

/// The group error, offset and metadata of partition 0, read where `FindCoordinator` points.
/// Retries while the group moves, as a Kafka consumer does, so the answer is the coordinator's.
async fn fetch_through(addrs: &[SocketAddr], group: &str) -> (i16, i64, String) {
    eventually(
        RECOVERY,
        "an offset read at the group's coordinator",
        || async {
            let coordinator = find(addrs, group).await?;
            let answer = try_fetch(coordinator, group).await?;
            (!MOVING.contains(&answer.0)).then_some(answer)
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_gateway_beside_each_node_when_the_coordinator_gateway_stops_should_move_its_groups()
 {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let (_cluster, mut gateways) = start(data_dir.path()).await;
    let group = "orders-cluster";
    let addrs: Vec<SocketAddr> = gateways.iter().map(|gateway| gateway.addr).collect();

    let coordinator = eventually(RECOVERY, "a coordinator", || find(&addrs, group)).await;
    let (member_id, generation) = join_and_sync(coordinator, group).await;
    assert_eq!(
        commit(coordinator, group, &member_id, generation, 5, "before").await,
        ERROR_NONE
    );

    let at = gateways
        .iter()
        .position(|gateway| gateway.addr == coordinator)
        .expect("the coordinator is one of the gateways");
    gateways.remove(at).stop().await;
    heartbeat_through(&addrs, group, &member_id, generation).await;
    assert_eq!(
        fetch_through(&addrs, group).await,
        (ERROR_NONE, 5, "before".to_string())
    );

    for gateway in gateways {
        gateway.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_gateway_beside_each_node_when_a_node_stops_should_keep_serving_the_group() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let (mut cluster, gateways) = start(data_dir.path()).await;
    let group = "payments-cluster";
    let addrs: Vec<SocketAddr> = gateways.iter().map(|gateway| gateway.addr).collect();

    let coordinator = eventually(RECOVERY, "a coordinator", || find(&addrs, group)).await;
    let (member_id, generation) = join_and_sync(coordinator, group).await;
    assert_eq!(
        commit(coordinator, group, &member_id, generation, 9, "before").await,
        ERROR_NONE
    );

    // The node that started first holds the metadata primary, so its stop forces a view change.
    cluster.stop(0);
    heartbeat_through(&addrs, group, &member_id, generation).await;
    eventually(RECOVERY, "a commit after the node stopped", || async {
        let coordinator = find(&addrs, group).await?;
        let code = commit(coordinator, group, &member_id, generation, 10, "after").await;
        (code == ERROR_NONE).then_some(())
    })
    .await;
    assert_eq!(
        fetch_through(&addrs, group).await,
        (ERROR_NONE, 10, "after".to_string())
    );

    for gateway in gateways {
        gateway.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_gateway_beside_each_node_when_each_restarts_in_turn_should_keep_the_generation() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let (cluster, mut gateways) = start(data_dir.path()).await;
    let group = "rolling";
    let addrs: Vec<SocketAddr> = gateways.iter().map(|gateway| gateway.addr).collect();
    let brokers = listed(&addrs);

    let coordinator = eventually(RECOVERY, "a coordinator", || find(&addrs, group)).await;
    let (member_id, generation) = join_and_sync(coordinator, group).await;

    for index in 0..gateways.len() {
        let gateway = gateways.remove(index);
        let (node_id, addr) = (gateway.node_id, gateway.addr);
        gateway.stop().await;
        heartbeat_through(&addrs, group, &member_id, generation).await;
        let socket = bind_listener(&addr.to_string()).expect("bind the same port again");
        let bridge = cluster.bridge_config(usize::from(node_id) - 1);
        let config = gateway_config(node_id, brokers.clone());
        gateways.insert(index, start_gateway(bridge, config, socket, addr).await);
        heartbeat_through(&addrs, group, &member_id, generation).await;
    }

    for gateway in gateways {
        gateway.stop().await;
    }
}
