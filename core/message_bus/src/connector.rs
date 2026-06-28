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
//! successful TCP connect the connector hands the raw stream to the
//! `on_dialed` callback supplied by the shard bootstrap, which duplicates
//! the fd and ships it to the owning shard via the inter-shard channel
//! (see `shard::coordinator::ShardZeroCoordinator`). No byte is written
//! here: the dialer half of the handshake (plaintext `ReplicaHello`, or
//! the 3-message mutual MAC exchange when `cluster.auth.enabled` is set -
//! see [`crate::replica::handshake`]) runs on the owning shard.
//!
//! Because delegation returns before the handshake completes, the sweep
//! consults the shard-0 pending-dial set
//! (`IggyMessageBus::check_dial_pending`) in addition to the owner table:
//! the callback marks a peer pending after a successful delegation and
//! the owning shard's handshake-outcome ack clears it, with a
//! deadline-based expiry as the lost-ack backstop. Without it the next
//! sweep would redial a peer whose handshake is still in flight.

use crate::DialedReplicaFn;
use crate::IggyMessageBus;
use crate::lifecycle::ShutdownToken;
use compio::net::TcpStream;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, info};

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
    self_id: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_dialed: DialedReplicaFn,
    reconnect_period: Duration,
) {
    connect_all(bus, self_id, &peers, &on_dialed).await;

    let handler = on_dialed.clone();
    let token = bus.token();
    let bus_for_task = Rc::clone(bus);
    let handle = compio::runtime::spawn(async move {
        periodic_reconnect(
            &bus_for_task,
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
    self_id: u8,
    peers: &[(u8, SocketAddr)],
    on_dialed: &DialedReplicaFn,
) {
    let dials = peers.iter().filter_map(|&(peer_id, addr)| {
        if peer_id <= self_id {
            return None;
        }
        // Skip peers that already have a live mapping on this cluster.
        // `replicas().contains` covers single-shard deployments where the
        // connection lives on shard 0; `owning_shard` covers multi-shard
        // deployments where the fd was delegated to a peer shard.
        // `check_dial_pending` covers the window between a successful
        // delegation and the owning shard's handshake outcome, during
        // which neither table has an entry yet - redialing there would
        // open a second connection to the same peer whose install CAS
        // winners can differ per side, churning both. Either hit means
        // this peer needs no dial this sweep. The per-peer
        // `owning_shard` CAS in `install_replica_conn` arbitrates any
        // remaining cross-shard inbound install race.
        if bus.replicas().contains(peer_id)
            || bus.owning_shard(peer_id).is_some()
            || bus.check_dial_pending(peer_id)
        {
            debug!(
                replica = peer_id,
                "skip reconnect: peer already registered or handshake pending"
            );
            return None;
        }
        Some(connect_one(peer_id, addr, on_dialed))
    });
    // Dial concurrently so one unreachable peer's connect latency does not
    // stall the rest. The futures share one task, so the `on_dialed`
    // delegations never overlap. Inbound vs outbound never collide for
    // the same peer (directionality: we dial only higher ids, accept only
    // lower).
    futures::future::join_all(dials).await;
}

#[allow(clippy::future_not_send)]
async fn periodic_reconnect(
    bus: &Rc<IggyMessageBus>,
    self_id: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_dialed: DialedReplicaFn,
    period: Duration,
    token: ShutdownToken,
) {
    while token.sleep_or_shutdown(period).await {
        connect_all(bus, self_id, &peers, &on_dialed).await;
    }
    debug!("replica reconnect periodic task exiting");
}

/// Dial a single peer and hand the raw stream to `on_dialed` on success.
///
/// Connect failures are logged and swallowed; VSR tolerates missing
/// peers and the periodic sweep retries. The handshake (and its
/// `handshake_grace` bound) runs on the owning shard after delegation.
#[allow(clippy::future_not_send)]
async fn connect_one(peer_id: u8, addr: SocketAddr, on_dialed: &DialedReplicaFn) {
    let stream = match TcpStream::connect(addr).await {
        Ok(s) => s,
        Err(e) => {
            debug!(replica = peer_id, %addr, "connect failed: {e}");
            return;
        }
    };
    if let Err(e) = stream.set_nodelay(true) {
        debug!(replica = peer_id, %addr, "set_nodelay failed: {e}");
    }
    info!(
        replica = peer_id,
        %addr,
        "dialed peer replica, delegating fd"
    );
    on_dialed(stream, peer_id);
}
