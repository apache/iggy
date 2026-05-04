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

//! Outbound replica connector.
//!
//! Runs only on shard 0. For each peer replica with `peer_id > self_id` the
//! connector dials at startup and re-dials on a periodic sweep. After a
//! successful TCP connect the connector sends a plaintext `Ping` frame
//! announcing this replica's id and `cluster_id` and hands the stream
//! to the `on_dialed` callback supplied by the shard bootstrap, which
//! duplicates the fd and ships it to the owning shard via the
//! inter-shard channel
//! (see `shard::coordinator::ShardZeroCoordinator`).
//!
//! No transport-level authentication: the future `LOGIN_REPLICA`
//! command in the caller (`server-ng`) carries the cluster shared
//! secret post-connect.

use crate::IggyMessageBus;
use crate::framing;
use crate::lifecycle::ShutdownToken;
use crate::{AcceptedReplicaFn, GenericHeader, Message};
use compio::net::TcpStream;
use iggy_binary_protocol::{Command2, HEADER_SIZE};
use std::mem::size_of;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Default reconnect sweep period.
///
/// Equivalent to `MessageBusConfig::default().reconnect_period`; exposed
/// as a named const for test / bench ergonomics. Kept in sync with the
/// `MessageBusConfig::default` impl. Remove once the configs-crate
/// migration lands and bootstrap always reads the period from
/// `ServerConfig`.
pub const DEFAULT_RECONNECT_PERIOD: Duration = Duration::from_secs(5);

/// Dial every peer with `peer_id > self_id` once, then launch a periodic
/// sweep in the background. The periodic task handle is tracked on the bus
/// so graceful shutdown can await it.
#[allow(clippy::future_not_send)]
pub async fn start(
    bus: &Rc<IggyMessageBus>,
    cluster_id: u128,
    self_id: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_dialed: AcceptedReplicaFn,
    reconnect_period: Duration,
) {
    connect_all(bus, cluster_id, self_id, &peers, &on_dialed).await;

    let handler = on_dialed.clone();
    let token = bus.token();
    let bus_for_task = Rc::clone(bus);
    let handle = compio::runtime::spawn(async move {
        periodic_reconnect(
            &bus_for_task,
            cluster_id,
            self_id,
            peers,
            handler,
            reconnect_period,
            token,
        )
        .await;
    });
    bus.track_background(handle);
}

#[allow(clippy::future_not_send)]
async fn connect_all(
    bus: &Rc<IggyMessageBus>,
    cluster_id: u128,
    self_id: u8,
    peers: &[(u8, SocketAddr)],
    on_dialed: &AcceptedReplicaFn,
) {
    for &(peer_id, addr) in peers {
        if peer_id <= self_id {
            continue;
        }
        // Skip peers that already have a live mapping on this cluster.
        // `replicas().contains` covers single-shard deployments where the
        // connection lives on shard 0; `owning_shard` covers multi-shard
        // deployments where the fd was delegated to a peer shard but the
        // mapping broadcast reached shard 0. Either hit means a previous
        // sweep (or the inbound listener) already installed this peer.
        // Redialing wastes a TCP round-trip; the live entry stays intact
        // (the loser of the registry insert race in `install_replica_conn`
        // gets `RejectedRegistration` back and tears DOWN ITS OWN orphan
        // tasks via `drain_rejected_registration`, never the winner's).
        if bus.replicas().contains(peer_id) || bus.owning_shard(peer_id).is_some() {
            debug!(
                replica = peer_id,
                "skip reconnect: peer already registered on cluster"
            );
            continue;
        }
        connect_one(cluster_id, self_id, peer_id, addr, on_dialed).await;
    }
}

#[allow(clippy::future_not_send)]
async fn periodic_reconnect(
    bus: &Rc<IggyMessageBus>,
    cluster_id: u128,
    self_id: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_dialed: AcceptedReplicaFn,
    period: Duration,
    token: ShutdownToken,
) {
    while token.sleep_or_shutdown(period).await {
        connect_all(bus, cluster_id, self_id, &peers, &on_dialed).await;
    }
    debug!("replica reconnect periodic task exiting");
}

/// Dial a single peer, send the plaintext `Ping` frame, and hand the
/// stream to `on_dialed` on success. Dial / write failures are logged
/// and swallowed; VSR tolerates missing peers and the periodic sweep
/// retries.
#[allow(clippy::future_not_send)]
async fn connect_one(
    cluster_id: u128,
    self_id: u8,
    peer_id: u8,
    addr: SocketAddr,
    on_dialed: &AcceptedReplicaFn,
) {
    let mut stream = match TcpStream::connect(addr).await {
        Ok(s) => s,
        Err(e) => {
            debug!(replica = peer_id, %addr, "connect failed: {e}");
            return;
        }
    };
    if let Err(e) = stream.set_nodelay(true) {
        debug!(replica = peer_id, %addr, "set_nodelay failed: {e}");
    }

    let ping = build_ping_message(cluster_id, self_id);
    if let Err(e) = framing::write_message(&mut stream, ping).await {
        warn!(replica = peer_id, %addr, "handshake write failed: {e}");
        return;
    }

    info!(replica = peer_id, %addr, "dialed peer replica, delegating fd");
    on_dialed(stream, peer_id);
}

/// Build a plaintext `Ping` frame announcing this replica's id and
/// `cluster_id`. No nonce, no timestamp, no MAC: cluster-secret
/// validation moves to the future `LOGIN_REPLICA` command in the caller.
fn build_ping_message(cluster_id: u128, replica_id: u8) -> Message<GenericHeader> {
    #[allow(clippy::cast_possible_truncation)]
    Message::<GenericHeader>::new(size_of::<GenericHeader>()).transmute_header(
        |_, h: &mut GenericHeader| {
            h.command = Command2::Ping;
            h.cluster = cluster_id;
            h.replica = replica_id;
            h.size = HEADER_SIZE as u32;
        },
    )
}
