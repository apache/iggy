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

//! Two nodes of four shards each on one runtime, every shard its own bus
//! over the owner table of its node. The hops the server sends through a
//! shard inbox (frame forwards, close reports, close requests) run in a
//! spawned task here; fd delegation is a direct install on the owner bus.

mod common;

use common::{header_only, loopback, set_replica_ctx};
use iggy_binary_protocol::Command;
use message_bus::config::MessageBusConfig;
use message_bus::connector::start as start_connector;
use message_bus::replica::listener::{self, MessageHandler, bind, run};
use message_bus::{
    AcceptedReplicaFn, ConnectionInstaller, DialedReplicaFn, IggyMessageBus, LinkLayout,
    MessageBus, ReplicaLinkId, ReplicaOwnerTable, installer,
};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CLUSTER: u128 = 0x4B4C;
const NODES: u8 = 2;
const SHARDS: u16 = 4;
const LINKS: u16 = 2;
const SWEEP: Duration = Duration::from_millis(50);
const WAIT: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(10);
const SHUTDOWN: Duration = Duration::from_secs(2);
// 4 shards, 2 links: range 0 = shards {0, 1}, range 1 = {2, 3}. The skip
// flag keeps link 0 off shard 0, and the only peer has slot 0.
const LINK_OWNERS: [u16; 2] = [1, 2];

type Shards = Rc<Vec<Rc<IggyMessageBus>>>;
type Received = Rc<RefCell<Vec<(u16, Command)>>>;

struct Node {
    id: u8,
    table: Arc<ReplicaOwnerTable>,
    shards: Shards,
    received: Received,
    addr: SocketAddr,
}

#[compio::test]
#[allow(clippy::future_not_send)]
async fn links_route_by_shard_range_and_reform_after_one_closes() {
    let nodes = [start_node(0, LINKS).await, start_node(1, LINKS).await];
    connect(&nodes).await;
    wait_until("both bundles up", || all_up(&nodes)).await;
    for node in &nodes {
        let owners = [0, 1].map(|link| node.table.claim(peer_of(node), link).map(|c| c.owner));
        assert_eq!(owners, LINK_OWNERS.map(Some), "node {}", node.id);
    }

    // Shard 3 sends a group frame over link 1, through its owner shard 2,
    // and a metadata frame over link 0 whatever its range.
    send(&nodes[0], 3, Command::Prepare).await;
    send(&nodes[0], 3, Command::ForwardRegister).await;
    send(&nodes[1], LINK_OWNERS[1], Command::PrepareOk).await;
    wait_until("frames delivered", || received(&nodes) == [1, 2]).await;
    assert_received(&nodes[1], (LINK_OWNERS[1], Command::Prepare));
    assert_received(&nodes[1], (LINK_OWNERS[0], Command::ForwardRegister));
    assert_received(&nodes[0], (LINK_OWNERS[1], Command::PrepareOk));

    let epochs_before = nodes.each_ref().map(link_epochs);
    let claim = nodes[1].table.claim(0, 1).expect("link 1 installed");
    nodes[1].shards[usize::from(claim.owner)].close_replica_link(ReplicaLinkId {
        replica: 0,
        link: 1,
        epoch: claim.epoch,
    });
    wait_until("both bundles re-formed in a new epoch", || {
        all_up(&nodes)
            && nodes.iter().zip(&epochs_before).all(|(node, before)| {
                link_epochs(node)
                    .iter()
                    .zip(before)
                    .all(|(now, before)| now > before)
            })
    })
    .await;

    send(&nodes[0], 3, Command::Commit).await;
    wait_until("frame delivered after the re-formation", || {
        received(&nodes) == [1, 3]
    })
    .await;
    assert_received(&nodes[1], (LINK_OWNERS[1], Command::Commit));
    for node in &nodes {
        let stats = node.shards[0].take_replica_link_stats();
        assert!(stats.bundle_teardowns >= 1, "node {}: {stats:?}", node.id);
        assert_eq!(stats.bundles_up, 1, "node {}", node.id);
    }

    shut_down(&nodes).await;
}

#[compio::test]
#[allow(clippy::future_not_send)]
async fn nodes_with_other_link_counts_never_connect() {
    let nodes = [start_node(0, LINKS).await, start_node(1, 1).await];
    connect(&nodes).await;
    compio::time::sleep(SWEEP * 6).await;

    assert!(!nodes[0].table.is_up(1));
    assert!(!nodes[1].table.is_up(0));
    // Node 1 refuses link 1 at the preamble and link 0 at the hello.
    let refusals = nodes[1].shards[0]
        .take_replica_link_stats()
        .preamble_refusals;
    assert!(refusals >= 1, "preamble refusals: {refusals}");
    let failures: u64 = nodes[1]
        .shards
        .iter()
        .map(|bus| bus.take_replica_link_stats().handshake_failures)
        .sum();
    assert!(failures >= 1, "handshake failures: {failures}");

    shut_down(&nodes).await;
}

#[allow(clippy::future_not_send)]
async fn start_node(id: u8, links: u16) -> Node {
    let layout =
        LinkLayout::new(SHARDS, links, true, usize::from(NODES - 1)).expect("valid layout");
    let table = Arc::new(ReplicaOwnerTable::with_layout(layout));
    let config = MessageBusConfig {
        reconnect_period: SWEEP,
        mesh_expected_peers: usize::from(NODES - 1),
        ..MessageBusConfig::default()
    };
    let shards: Shards = Rc::new(
        (0..SHARDS)
            .map(|shard| {
                Rc::new(IggyMessageBus::with_tunables_and_owner_table(
                    shard,
                    config.clone(),
                    Arc::clone(&table),
                ))
            })
            .collect(),
    );
    for bus in shards.iter() {
        set_replica_ctx(bus, CLUSTER, id, NODES, None);
        wire_shard(bus, &Rc::downgrade(&shards));
    }
    let close_on = Rc::downgrade(&shards);
    shards[0].set_replica_link_close_fn(Box::new(move |owner, link| {
        if let Some(shards) = close_on.upgrade() {
            let owner = Rc::clone(&shards[usize::from(owner)]);
            compio::runtime::spawn(async move { owner.close_replica_link(link) }).detach();
        }
    }));

    let received = Received::default();
    let (replica_listener, addr) = bind(loopback()).await.expect("bind replica listener");
    let token = shards[0].token();
    let accepted = accept_fn(&shards, &received);
    let handle = compio::runtime::spawn(async move {
        run(replica_listener, token, accepted).await;
    });
    shards[0].track_background(handle);
    Node {
        id,
        table,
        shards,
        received,
        addr,
    }
}

/// The server sends both hops through the shard inboxes.
fn wire_shard(bus: &IggyMessageBus, shards: &Weak<Vec<Rc<IggyMessageBus>>>) {
    let forward_to = Weak::clone(shards);
    bus.set_replica_forward_fn(Box::new(move |replica, owner, message| {
        if let Some(shards) = forward_to.upgrade() {
            let owner = Rc::clone(&shards[usize::from(owner)]);
            compio::runtime::spawn(async move {
                let _ = owner.send_to_replica(replica, message).await;
            })
            .detach();
        }
        Ok(())
    }));
    let report_to = Weak::clone(shards);
    bus.set_connection_lost_fn(Rc::new(move |link| {
        if let Some(shards) = report_to.upgrade() {
            let shard_zero = Rc::clone(&shards[0]);
            compio::runtime::spawn(async move { shard_zero.replica_link_closed(link) }).detach();
        }
    }));
}

fn accept_fn(shards: &Shards, received: &Received) -> AcceptedReplicaFn {
    let shards = Rc::downgrade(shards);
    let received = Rc::clone(received);
    Rc::new(move |stream| {
        let Some(shards) = shards.upgrade() else {
            return;
        };
        let received = Rc::clone(&received);
        let shard_zero = Rc::clone(&shards[0]);
        listener::admit_inbound(&shard_zero, stream, move |stream, link, owner, slot| {
            let ack_to = Rc::clone(&shards[0]);
            installer::install_replica_inbound(
                &shards[usize::from(owner)],
                stream,
                link,
                on_message(&received, owner),
                Box::new(move || ack_to.replica_inbound_handshake_done(slot, link)),
            );
        });
    })
}

fn dial_fn(node: &Node) -> DialedReplicaFn {
    let shards = Rc::downgrade(&node.shards);
    let received = Rc::clone(&node.received);
    Rc::new(move |stream, link, owner| {
        let Some(shards) = shards.upgrade() else {
            return;
        };
        let ack_to = Rc::clone(&shards[0]);
        ack_to.mark_dial_pending(link.replica, link.link);
        installer::install_replica_outbound(
            &shards[usize::from(owner)],
            link,
            stream,
            on_message(&received, owner),
            Box::new(move || ack_to.replica_outbound_handshake_done(link)),
        );
    })
}

fn on_message(received: &Received, shard: u16) -> MessageHandler {
    let received = Rc::clone(received);
    Rc::new(move |_, message| {
        received
            .borrow_mut()
            .push((shard, message.header().command));
    })
}

#[allow(clippy::future_not_send)]
async fn connect(nodes: &[Node]) {
    for node in nodes {
        let peers = nodes
            .iter()
            .filter(|peer| peer.id != node.id)
            .map(|peer| (peer.id, peer.addr))
            .collect();
        start_connector(&node.shards[0], node.id, peers, dial_fn(node), SWEEP).await;
    }
}

#[allow(clippy::future_not_send)]
async fn send(node: &Node, shard: u16, command: Command) {
    node.shards[usize::from(shard)]
        .send_to_replica(
            peer_of(node),
            header_only(command, CLUSTER, node.id).into_frozen(),
        )
        .await
        .expect("bundle is up");
}

#[allow(clippy::future_not_send)]
async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        compio::time::sleep(POLL).await;
    }
}

#[allow(clippy::future_not_send)]
async fn shut_down(nodes: &[Node]) {
    for node in nodes {
        for bus in node.shards.iter() {
            bus.shutdown(SHUTDOWN).await;
        }
    }
}

const fn peer_of(node: &Node) -> u8 {
    NODES - 1 - node.id
}

fn all_up(nodes: &[Node]) -> bool {
    nodes.iter().all(|node| node.table.is_up(peer_of(node)))
}

fn link_epochs(node: &Node) -> Vec<Option<u32>> {
    (0..LINKS)
        .map(|link| node.table.claim(peer_of(node), link).map(|c| c.epoch))
        .collect()
}

fn received(nodes: &[Node]) -> Vec<usize> {
    nodes
        .iter()
        .map(|node| node.received.borrow().len())
        .collect()
}

fn assert_received(node: &Node, frame: (u16, Command)) {
    let received = node.received.borrow();
    assert!(received.contains(&frame), "node {}: {received:?}", node.id);
}
