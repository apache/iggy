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
//! connector dials every missing link of the peer's bundle at startup and
//! re-dials on a periodic sweep. After a successful TCP connect the
//! connector hands the raw stream to the `on_dialed` callback supplied by
//! the shard bootstrap, with the link and the shard that owns it; the
//! callback duplicates the fd and ships it to that shard via the
//! inter-shard channel (see `shard::coordinator::ShardZeroCoordinator`).
//! No byte is written here: the placement preamble and the dialer half of
//! the handshake (plaintext `ReplicaHello`, or the 3-message mutual MAC
//! exchange when `cluster.auth.enabled` is set - see
//! [`crate::replica::handshake`]) run on the owning shard.
//!
//! Each sweep first refreshes the bundle of every peer, in both roles, so
//! a bundle that stays incomplete past `handshake_grace` tears down, and a
//! link of an old epoch gets its close request again (see
//! [`crate::replica::bundle`]).
//!
//! Because delegation returns before the handshake completes, the sweep
//! consults the shard-0 pending-dial set
//! (`IggyMessageBus::check_dial_pending`) in addition to the owner table:
//! the callback marks a link pending after a successful delegation and
//! the owning shard's handshake-outcome ack clears it, with a
//! deadline-based expiry as the lost-ack backstop. Without it the next
//! sweep would redial a link whose handshake is still in flight.

use crate::IggyMessageBus;
use crate::lifecycle::ShutdownToken;
use crate::replica::links::peer_slot;
use crate::{DialedReplicaFn, ReplicaLinkId};
use compio::net::TcpStream;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, info};

/// Dial every link to every peer with `peer_id > self_id` once, then launch
/// a periodic sweep in the background. The periodic task handle is tracked
/// on the bus so graceful shutdown can await it.
#[allow(clippy::future_not_send)]
pub async fn start(
    bus: &Rc<IggyMessageBus>,
    self_id: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_dialed: DialedReplicaFn,
    reconnect_period: Duration,
) {
    connect_all(bus, self_id, &peers, &on_dialed, reconnect_period).await;

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
    bus: &IggyMessageBus,
    self_id: u8,
    peers: &[(u8, SocketAddr)],
    on_dialed: &DialedReplicaFn,
    connect_timeout: Duration,
) {
    for &(peer_id, _) in peers {
        bus.refresh_replica_bundle(peer_id);
    }
    // `replica_links_to_dial` skips links that are installed or whose
    // delegated handshake is still in flight (`check_dial_pending`):
    // redialing there would open a second connection for the same link.
    // The owner-table claim in `install_replica_conn` refuses any that
    // still gets through.
    let dials = peers
        .iter()
        .filter(|&&(peer_id, _)| peer_id > self_id)
        .flat_map(|&(peer_id, addr)| {
            bus.replica_links_to_dial(peer_id)
                .into_iter()
                .map(move |link| (link, addr))
        })
        .map(|(link, addr)| connect_one(bus, self_id, link, addr, on_dialed, connect_timeout));
    // Dial concurrently so one unreachable peer's connect latency does not
    // stall the rest. The futures share one task, so the `on_dialed`
    // delegations never overlap. Inbound vs outbound never collide for
    // the same peer (directionality: we dial only higher ids, accept only
    // lower).
    futures::future::join_all(dials).await;
}

#[allow(clippy::future_not_send)]
async fn periodic_reconnect(
    bus: &IggyMessageBus,
    self_id: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_dialed: DialedReplicaFn,
    period: Duration,
    token: ShutdownToken,
) {
    while token.sleep_or_shutdown(period).await {
        connect_all(bus, self_id, &peers, &on_dialed, period).await;
    }
    debug!("replica reconnect periodic task exiting");
}

/// Dial one link and hand the raw stream to `on_dialed` on success.
///
/// Connect failures are logged and swallowed; VSR tolerates missing
/// peers and the periodic sweep retries. The handshake (and its
/// `handshake_grace` bound) runs on the owning shard after delegation.
///
/// The dial is bounded by `connect_timeout`: the sweep joins every dial,
/// so a peer that drops SYNs would otherwise hold the whole sweep for the
/// kernel connect timeout (about two minutes on Linux) and starve the
/// retries the heartbeat timeout depends on.
#[allow(clippy::future_not_send)]
async fn connect_one(
    bus: &IggyMessageBus,
    self_id: u8,
    link: ReplicaLinkId,
    addr: SocketAddr,
    on_dialed: &DialedReplicaFn,
    connect_timeout: Duration,
) {
    let peer_id = link.replica;
    let stream = match compio::time::timeout(connect_timeout, TcpStream::connect(addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            debug!(replica = peer_id, link = link.link, %addr, "connect failed: {e}");
            return;
        }
        Err(_) => {
            debug!(
                replica = peer_id,
                link = link.link,
                %addr,
                ?connect_timeout,
                "connect timed out"
            );
            return;
        }
    };
    // A bundle that tore down during the connect wants this link in a new
    // epoch; the next sweep dials it.
    if !bus.wants_replica_dial(link) {
        debug!(
            replica = peer_id,
            link = link.link,
            epoch = link.epoch,
            "bundle moved on during the connect, dropping the dialed link"
        );
        return;
    }
    if let Err(e) = stream.set_nodelay(true) {
        debug!(replica = peer_id, %addr, "set_nodelay failed: {e}");
    }
    let owner = bus
        .link_layout()
        .link_owner(link.link, peer_slot(self_id, peer_id));
    info!(
        replica = peer_id,
        link = link.link,
        epoch = link.epoch,
        owner,
        %addr,
        "dialed peer replica link, delegating fd"
    );
    on_dialed(stream, link, owner);
}
