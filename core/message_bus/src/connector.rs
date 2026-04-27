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
//! connector dials at startup and re-dials on a periodic sweep. On a
//! successful `Ping` handshake the accepted stream is handed to the
//! `on_dialed` callback supplied by the shard bootstrap, which duplicates
//! the fd and ships it to the owning shard via the inter-shard channel
//! (see `shard::coordinator::ShardZeroCoordinator`).

use crate::IggyMessageBus;
use crate::auth::{self, LABEL_REPLICA, TokenSource};
use crate::framing;
use crate::lifecycle::ShutdownToken;
use crate::socket_opts::apply_keepalive_for_connection;
use crate::{AcceptedReplicaFn, GenericHeader, Message};
use compio::net::TcpStream;
use iggy_binary_protocol::{Command2, HEADER_SIZE};
use iggy_common::IggyTimestamp;
use rand::Rng;
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
    token_source: Rc<dyn TokenSource>,
) {
    connect_all(bus, cluster_id, self_id, &peers, &on_dialed, &token_source).await;

    let handler = on_dialed.clone();
    let token = bus.token();
    let bus_for_task = Rc::clone(bus);
    let token_source_for_task = Rc::clone(&token_source);
    let handle = compio::runtime::spawn(async move {
        periodic_reconnect(
            &bus_for_task,
            cluster_id,
            self_id,
            peers,
            handler,
            reconnect_period,
            token,
            token_source_for_task,
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
    token_source: &Rc<dyn TokenSource>,
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
        connect_one(
            bus,
            cluster_id,
            self_id,
            peer_id,
            addr,
            on_dialed,
            token_source,
        )
        .await;
    }
}

#[allow(clippy::future_not_send)]
#[allow(clippy::too_many_arguments)]
async fn periodic_reconnect(
    bus: &Rc<IggyMessageBus>,
    cluster_id: u128,
    self_id: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_dialed: AcceptedReplicaFn,
    period: Duration,
    token: ShutdownToken,
    token_source: Rc<dyn TokenSource>,
) {
    while token.sleep_or_shutdown(period).await {
        connect_all(bus, cluster_id, self_id, &peers, &on_dialed, &token_source).await;
    }
    debug!("replica reconnect periodic task exiting");
}

/// Dial a single peer, send the `Ping` handshake, and hand the stream to
/// `on_dialed` on success. Dial / handshake failures are logged and
/// swallowed; VSR tolerates missing peers and the periodic sweep retries.
#[allow(clippy::future_not_send)]
#[allow(clippy::too_many_arguments)]
async fn connect_one(
    bus: &Rc<IggyMessageBus>,
    cluster_id: u128,
    self_id: u8,
    peer_id: u8,
    addr: SocketAddr,
    on_dialed: &AcceptedReplicaFn,
    token_source: &Rc<dyn TokenSource>,
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
    let cfg = bus.config();
    if let Err(e) = apply_keepalive_for_connection(
        &stream,
        cfg.keepalive_idle,
        cfg.keepalive_interval,
        cfg.keepalive_retries,
    ) {
        warn!(
            replica = peer_id, %addr,
            "failed to configure TCP keepalive on outbound socket: {e}"
        );
    }

    let ping = build_ping_message(cluster_id, self_id, token_source.as_ref());
    if let Err(e) = framing::write_message(&mut stream, ping).await {
        warn!(replica = peer_id, %addr, "handshake write failed: {e}");
        return;
    }

    info!(replica = peer_id, %addr, "dialed peer replica, delegating fd");
    on_dialed(stream, peer_id);
}

/// Build a Ping handshake message identifying our replica id and carrying
/// a fresh auth envelope stamped by `token_source`.
fn build_ping_message(
    cluster_id: u128,
    replica_id: u8,
    token_source: &dyn TokenSource,
) -> Message<GenericHeader> {
    let now_ns = IggyTimestamp::now().as_nanos();
    debug_assert!(
        now_ns <= u128::from(u64::MAX),
        "auth_timestamp overflows u64 ns (year ~2554) - envelope TIMESTAMP_RANGE is 8 bytes by design"
    );
    #[allow(clippy::cast_possible_truncation)]
    let timestamp_ns = now_ns as u64;
    let nonce = {
        let mut buf = [0u8; 16];
        rand::rng().fill_bytes(&mut buf);
        u128::from_le_bytes(buf)
    };

    #[allow(clippy::cast_possible_truncation)]
    Message::<GenericHeader>::new(size_of::<GenericHeader>()).transmute_header(
        |_, h: &mut GenericHeader| {
            h.command = Command2::Ping;
            h.cluster = cluster_id;
            h.replica = replica_id;
            h.size = HEADER_SIZE as u32;
            let challenge = auth::AuthChallenge {
                cluster: cluster_id,
                peer_id: u128::from(replica_id),
                timestamp_ns,
                release: h.release,
                nonce,
                label: LABEL_REPLICA,
            };
            auth::encode_envelope(&mut h.reserved_command, token_source, &challenge);
        },
    )
}
