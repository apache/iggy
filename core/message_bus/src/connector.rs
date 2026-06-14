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
//! successful TCP connect the connector sends a plaintext `ReplicaHello` frame
//! announcing this replica's id and `cluster_id` and hands the stream
//! to the `on_dialed` callback supplied by the shard bootstrap, which
//! duplicates the fd and ships it to the owning shard via the
//! inter-shard channel
//! (see `shard::coordinator::ShardZeroCoordinator`).
//!
//! When `cluster.auth.enabled` is set the dialer runs the dialer
//! half of the 3-message mutual BLAKE3 keyed-MAC handshake (see
//! [`crate::replica::auth`]): it sends `nonce_d` in the `ReplicaHello`, verifies
//! the acceptor's `ReplicaChallenge` MAC, and sends a `ReplicaFinish` frame
//! carrying its own MAC before delegating the fd. With no secret configured it
//! sends a plain `ReplicaHello` exactly as before.

use crate::IggyMessageBus;
use crate::framing;
use crate::lifecycle::ShutdownToken;
use crate::replica::auth::{self, ReplicaAuth, Transcript};
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
#[allow(clippy::too_many_arguments)]
pub async fn start(
    bus: &Rc<IggyMessageBus>,
    cluster_id: u128,
    self_id: u8,
    peers: Vec<(u8, SocketAddr)>,
    auth: Option<ReplicaAuth>,
    handshake_grace: Duration,
    on_dialed: AcceptedReplicaFn,
    reconnect_period: Duration,
) {
    connect_all(
        bus,
        cluster_id,
        self_id,
        &peers,
        auth.as_ref(),
        handshake_grace,
        &on_dialed,
    )
    .await;

    let handler = on_dialed.clone();
    let token = bus.token();
    let bus_for_task = Rc::clone(bus);
    let handle = compio::runtime::spawn(async move {
        periodic_reconnect(
            &bus_for_task,
            cluster_id,
            self_id,
            peers,
            auth,
            handshake_grace,
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
    auth: Option<&ReplicaAuth>,
    handshake_grace: Duration,
    on_dialed: &AcceptedReplicaFn,
) {
    let max_message_size = bus.config().max_message_size;
    let dials = peers.iter().filter_map(|&(peer_id, addr)| {
        if peer_id <= self_id {
            return None;
        }
        // Skip peers that already have a live mapping on this cluster.
        // `replicas().contains` covers single-shard deployments where the
        // connection lives on shard 0; `owning_shard` covers multi-shard
        // deployments where the fd was delegated to a peer shard but the
        // mapping broadcast reached shard 0. Either hit means a previous
        // sweep (or the inbound listener) already installed this peer,
        // and redialing wastes a TCP round-trip. The same `owning_shard`
        // CAS is also taken inside `install_replica_conn` before any
        // task is spawned, so a concurrent inbound install for the same
        // peer on a different shard loses the race there and drops the
        // fd; the live entry stays intact on the winner.
        if bus.replicas().contains(peer_id) || bus.owning_shard(peer_id).is_some() {
            debug!(
                replica = peer_id,
                "skip reconnect: peer already registered on cluster"
            );
            return None;
        }
        Some(connect_one(
            cluster_id,
            self_id,
            peer_id,
            addr,
            auth,
            max_message_size,
            handshake_grace,
            on_dialed,
        ))
    });
    // Dial concurrently: an accept-but-silent peer must not stall the rest
    // behind its handshake-grace read. The futures share one task, so the
    // `on_dialed` installs never overlap. Inbound vs outbound never collide for
    // the same peer (directionality: we dial only higher ids, accept only
    // lower); the per-peer `owning_shard` CAS in `install_replica_conn`
    // arbitrates the cross-shard inbound install race.
    futures::future::join_all(dials).await;
}

#[allow(clippy::future_not_send, clippy::too_many_arguments)]
async fn periodic_reconnect(
    bus: &Rc<IggyMessageBus>,
    cluster_id: u128,
    self_id: u8,
    peers: Vec<(u8, SocketAddr)>,
    auth: Option<ReplicaAuth>,
    handshake_grace: Duration,
    on_dialed: AcceptedReplicaFn,
    period: Duration,
    token: ShutdownToken,
) {
    while token.sleep_or_shutdown(period).await {
        connect_all(
            bus,
            cluster_id,
            self_id,
            &peers,
            auth.as_ref(),
            handshake_grace,
            &on_dialed,
        )
        .await;
    }
    debug!("replica reconnect periodic task exiting");
}

/// Dial a single peer and hand the stream to `on_dialed` on success.
///
/// With `auth = None` this sends a plaintext `ReplicaHello` exactly as before.
/// With `auth = Some` it runs the dialer half of the mutual MAC handshake
/// (`ReplicaHello`+`nonce_d` -> verify `ReplicaChallenge` -> `ReplicaFinish`+`mac_d`)
/// before delegating. The dialer holds the strictly lower id, the acceptor the
/// higher; the transcript binds `dialer_id = self_id`, `acceptor_id = peer_id`.
/// Dial, write, read, timeout, and MAC failures are logged and swallowed; VSR
/// tolerates missing peers and the periodic sweep retries. If the acceptor
/// answers with a nonzero `ReplicaChallenge` status (a reject), the reason is
/// logged and the dial is abandoned.
#[allow(
    clippy::future_not_send,
    clippy::similar_names,
    clippy::too_many_arguments
)]
async fn connect_one(
    cluster_id: u128,
    self_id: u8,
    peer_id: u8,
    addr: SocketAddr,
    auth: Option<&ReplicaAuth>,
    max_message_size: usize,
    handshake_grace: Duration,
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

    // Bound the whole handshake leg (every write plus the ReplicaChallenge read) under one
    // grace. A peer that accepts then stops reading would otherwise park a
    // dialer write forever and wedge the inline shard-0 reconnect sweep.
    // Mirrors the acceptor's handshake_read timeout; the OS connect stays
    // outside, like the acceptor's accept.
    match compio::time::timeout(
        handshake_grace,
        dial_handshake(
            &mut stream,
            cluster_id,
            self_id,
            peer_id,
            addr,
            auth,
            max_message_size,
        ),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(())) => return,
        Err(_) => {
            warn!(replica = peer_id, %addr, grace = ?handshake_grace, "handshake exceeded grace");
            return;
        }
    }

    info!(
        replica = peer_id,
        %addr,
        authenticated = auth.is_some(),
        "dialed peer replica, delegating fd"
    );
    on_dialed(stream, peer_id);
}

/// Run the dialer handshake on an established stream. Logs and returns `Err(())`
/// on any failure; the caller abandons the dial and the periodic sweep retries.
/// With `auth = None` it sends a single plaintext `ReplicaHello`, exactly as before.
#[allow(clippy::future_not_send, clippy::similar_names)]
async fn dial_handshake(
    stream: &mut TcpStream,
    cluster_id: u128,
    self_id: u8,
    peer_id: u8,
    addr: SocketAddr,
    auth: Option<&ReplicaAuth>,
    max_message_size: usize,
) -> Result<(), ()> {
    let Some(auth) = auth else {
        let hello = build_hello_message(cluster_id, self_id, None);
        if let Err(e) = framing::write_message(stream, hello).await {
            warn!(replica = peer_id, %addr, "handshake write failed: {e}");
            return Err(());
        }
        return Ok(());
    };

    let nonce_d = match auth::random_nonce() {
        Ok(nonce) => nonce,
        Err(e) => {
            warn!(replica = peer_id, %addr, "nonce generation failed: {e}");
            return Err(());
        }
    };
    let hello = build_hello_message(cluster_id, self_id, Some(&nonce_d));
    if let Err(e) = framing::write_message(stream, hello).await {
        warn!(replica = peer_id, %addr, "handshake write failed: {e}");
        return Err(());
    }

    let challenge = match framing::read_message(stream, max_message_size).await {
        Ok(msg) => msg,
        Err(e) => {
            warn!(replica = peer_id, %addr, "handshake read failed: {e}");
            return Err(());
        }
    };
    if challenge.header().command != Command2::ReplicaChallenge {
        warn!(
            replica = peer_id,
            %addr,
            command = ?challenge.header().command,
            "unexpected replica handshake response command"
        );
        return Err(());
    }
    // Read command + status before nonce/MAC: a reject (or garbage) status
    // carries no valid nonce/MAC, so parsing them would be meaningless.
    let status = auth::read_status(&challenge.header().reserved_command);
    if status != auth::HandshakeStatus::Ok {
        warn!(
            replica = peer_id,
            %addr,
            reason = status.as_str(),
            "peer rejected replica handshake"
        );
        return Err(());
    }
    let nonce_a = auth::read_nonce(&challenge.header().reserved_command);
    let mac_a = auth::read_mac(&challenge.header().reserved_command);
    let transcript = Transcript {
        cluster_id,
        dialer_id: self_id,
        acceptor_id: peer_id,
        nonce_d,
        nonce_a,
    };
    if !auth.verify_acceptor_mac(&transcript, &mac_a) {
        warn!(replica = peer_id, %addr, "replica handshake MAC verification failed");
        return Err(());
    }
    let mac_d = auth.dialer_mac(&transcript);
    if let Err(e) =
        framing::write_message(stream, build_finish_message(cluster_id, self_id, &mac_d)).await
    {
        warn!(replica = peer_id, %addr, "handshake finish write failed: {e}");
        return Err(());
    }
    Ok(())
}

/// Build a `ReplicaHello` frame announcing this replica's id and `cluster_id`.
/// When `nonce_d` is set it is placed in `reserved_command[0..32]` to open
/// the authenticated handshake; otherwise the frame is the legacy plaintext
/// announce.
fn build_hello_message(
    cluster_id: u128,
    replica_id: u8,
    nonce_d: Option<&[u8; auth::NONCE_LEN]>,
) -> Message<GenericHeader> {
    #[allow(clippy::cast_possible_truncation)]
    Message::<GenericHeader>::new(size_of::<GenericHeader>()).transmute_header(
        |_, h: &mut GenericHeader| {
            h.command = Command2::ReplicaHello;
            h.cluster = cluster_id;
            h.replica = replica_id;
            h.size = HEADER_SIZE as u32;
            if let Some(nonce) = nonce_d {
                h.reserved_command[..auth::NONCE_LEN].copy_from_slice(nonce);
            }
        },
    )
}

/// Build the dialer's `ReplicaFinish` frame carrying `mac_d` in
/// `reserved_command[32..64]`.
fn build_finish_message(
    cluster_id: u128,
    replica_id: u8,
    mac_d: &[u8; auth::MAC_LEN],
) -> Message<GenericHeader> {
    #[allow(clippy::cast_possible_truncation)]
    Message::<GenericHeader>::new(size_of::<GenericHeader>()).transmute_header(
        |_, h: &mut GenericHeader| {
            h.command = Command2::ReplicaFinish;
            h.cluster = cluster_id;
            h.replica = replica_id;
            h.size = HEADER_SIZE as u32;
            h.reserved_command[auth::NONCE_LEN..auth::NONCE_LEN + auth::MAC_LEN]
                .copy_from_slice(mac_d);
        },
    )
}
