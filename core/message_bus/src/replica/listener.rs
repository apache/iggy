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

//! Inbound TCP listener for replica-to-replica consensus traffic.
//!
//! # Replica plane = TCP only
//!
//! This module is TCP-only by design, not by omission. Do NOT add WS,
//! QUIC, or HTTP paths here. The VSR chain-hash safety proof
//! (`last_prepare_checksum` in `core/consensus/src/plane_helpers.rs`),
//! the fd-delegation path (`F_DUPFD_CLOEXEC` needs a dupable plaintext
//! fd), the `writev` batching inside the TCP transport, and the single-digit
//! RTT assumptions used by the view-change timer all rest on a FIFO
//! byte stream between a bounded set of replicas on a trusted LAN. A
//! datagram-oriented or gateway-terminated transport violates one or
//! more of those assumptions.
//!
//! Runs only on shard 0. On every accepted `ReplicaHello` frame the listener
//! hands the accepted `TcpStream` to an `on_accepted` callback provided
//! by the shard bootstrap, which dup-and-ships the fd to the owning
//! shard via the inter-shard channel
//! (see `shard::coordinator::ShardZeroCoordinator`). This module no
//! longer installs writer / reader tasks itself.
//!
//! Duplicate connections are eliminated by directionality: each replica
//! only dials peers with strictly greater ids and only accepts inbound
//! from peers with strictly lower ids. No race, no tiebreaker.
//!
//! # Trust boundary (PSK handshake)
//!
//! When `cluster.auth.enabled` is set the listener runs the acceptor half of a
//! 3-message mutual BLAKE3 keyed-MAC handshake over the `reserved_command`
//! bytes (see [`crate::replica::auth`]) and REJECTS any peer that does not
//! complete it. The MAC proves the peer possesses the cluster PSK: it
//! authenticates cluster MEMBERSHIP, not per-replica identity. The peer id that
//! keys the registry is the announced `ReplicaHello` header byte. It is folded
//! into the keyed MAC (a dialer cannot MAC one id and announce another), but the
//! MAC key is a single cluster-wide PSK-derived subkey, not a per-replica key,
//! so any PSK holder can mint a valid MAC for any (smaller) replica id - there
//! is no anti-Sybil guarantee. With auth disabled the listener stays
//! in legacy mode: the `ReplicaHello` id is trusted unverified.
//!
//! Enabling auth is a coordinated-restart change, and not the only one: the
//! consensus `cluster_id` is derived from the cluster name unconditionally
//! (`auth::cluster_domain_id`, was a constant), so a mixed-version roster fails
//! to connect regardless of the auth setting. Flip every node in one restart.
//!
//! Because the PSK gates membership and not identity, an on-wire attacker who
//! learns the cluster id (auth off) OR an insider key-holder (auth on) can
//! register as any peer with a smaller replica id, so operators MUST deploy the
//! replica port on a trusted L2 boundary (cluster-local VPC, dedicated private
//! subnet, encrypted overlay such as `WireGuard`, or an air-gapped management
//! network).
//!
//! TLS / encryption is NOT provided here: the handshake authenticates the
//! peer but the data stream stays plaintext (so the fd remains dupable for
//! cross-shard delegation). On-wire confidentiality, if required, comes from
//! out-of-band link encryption, and on-wire frame integrity from the
//! follow-up per-frame session-key MAC.
//
// TODO(hubcio): follow-ups - dual-key rotation acceptance window, and the
// per-frame session-key MAC over installed frame headers that closes the
// MITM-inject / panic-DoS vector.

use crate::framing;
use crate::lifecycle::ShutdownToken;
use crate::replica::auth::{self, HandshakeStatus, ReplicaAuth, Transcript};
use crate::socket_opts::bind_reusable_tcp_listener;
use crate::{AcceptedReplicaFn, GenericHeader, Message};
use compio::net::{TcpListener, TcpStream};
use futures::FutureExt;
use iggy_binary_protocol::{Command2, HEADER_SIZE};
use iggy_common::IggyError;
use std::cell::Cell;
use std::mem::size_of;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Handler for inbound replica consensus messages.
///
/// Preserved for callers (tests, simulator-facing glue) that want to install
/// a connection locally without going through the coordinator. The shard-0
/// production path uses [`AcceptedReplicaFn`] instead.
pub type MessageHandler = Rc<dyn Fn(u8, Message<GenericHeader>)>;

/// Bind the replica listener and return the bound address.
///
/// # Errors
///
/// Returns [`IggyError::CannotBindToSocket`] if the bind fails.
#[allow(clippy::future_not_send)]
pub async fn bind(addr: SocketAddr) -> Result<(TcpListener, SocketAddr), IggyError> {
    let listener = bind_reusable_tcp_listener(addr)
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;
    let actual = listener
        .local_addr()
        .map_err(|e| IggyError::IoError(e.to_string()))?;
    Ok((listener, actual))
}

/// Hard cap on concurrently in-flight inbound handshakes. A legitimate dialer
/// set is bounded by `self_id` (< `replica_count` <= 255), so the cap only
/// sheds load under a hostile flood; it bounds fd / memory without ever
/// stalling the accept loop.
const MAX_INFLIGHT_HANDSHAKES: usize = 256;

/// Run the inbound replica listener accept loop until the shutdown token fires.
///
/// With `auth = None`, every accepted connection that passes the plaintext
/// `ReplicaHello` frame check (command, cluster, directional rule) fires the
/// `on_accepted` callback. With `auth = Some` the connection must additionally
/// complete the mutual MAC handshake; a missing nonce, a non-`ReplicaFinish`
/// finish frame, a failed dialer-MAC, or a grace timeout drops it without
/// firing the callback. Once fired, the callback owns the accepted stream.
///
/// Each [`TcpStream`] returned by `accept()` is handed to its own
/// spawned task that runs the `handshake_read` step under
/// [`compio::time::timeout(handshake_grace, ...)`]. Spawning per-incoming
/// is load-bearing: a single hostile or merely slow peer would otherwise
/// hold the entire replica accept loop behind its handshake `read` until
/// peer EOF, and subsequent peers would not be served. Mirrors the
/// client-plane defense in [`crate::client_listener::quic`].
///
/// `handshake_grace` bounds the wall-clock time each spawned handshake
/// task may take before it is cancelled and the connection dropped;
/// threaded from [`crate::MessageBusConfig::handshake_grace`] by the
/// bootstrap caller.
///
/// Each spawned handshake task is fire-and-forget (`detach`): compio 0.19's
/// `JoinHandle` exposes no `is_finished`, so there is no handle vector to
/// reap. Two bounds keep it safe: each task is capped by `handshake_grace`,
/// and the number of concurrently in-flight tasks is capped by
/// [`MAX_INFLIGHT_HANDSHAKES`] (excess inbound is dropped). On shutdown the
/// accept loop exits; each detached task owns its accepted stream (not the
/// listener), so it self-terminates within `handshake_grace`, or is torn down
/// with the runtime if still in flight.
#[allow(clippy::future_not_send, clippy::too_many_arguments)]
pub async fn run(
    listener: TcpListener,
    token: ShutdownToken,
    cluster_id: u128,
    self_id: u8,
    replica_count: u8,
    auth: Option<ReplicaAuth>,
    on_accepted: AcceptedReplicaFn,
    max_message_size: usize,
    handshake_grace: Duration,
) {
    info!(
        "Replica listener accepting on {:?}",
        listener.local_addr().ok()
    );
    let inflight = Rc::new(Cell::new(0usize));
    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!("Replica listener shutting down");
                break;
            }
            result = listener.accept().fuse() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        if inflight.get() >= MAX_INFLIGHT_HANDSHAKES {
                            warn!(
                                %peer_addr,
                                cap = MAX_INFLIGHT_HANDSHAKES,
                                "replica handshake in-flight cap reached; dropping inbound"
                            );
                            continue;
                        }
                        let on_accepted = on_accepted.clone();
                        let auth = auth.clone();
                        // Fire-and-forget: compio 0.19's `JoinHandle` has no
                        // `is_finished`, so there is no handle vector to reap.
                        // `detach()` lets each handshake task self-manage; the
                        // `InflightGuard` decrements the shared counter on every
                        // exit path, keeping the `MAX_INFLIGHT_HANDSHAKES` cap
                        // accurate. Detached tasks own their accepted stream, not
                        // the listener: each is bounded by `handshake_grace` and
                        // torn down with the runtime if still in flight.
                        inflight.set(inflight.get() + 1);
                        let guard = InflightGuard(Rc::clone(&inflight));
                        compio::runtime::spawn(async move {
                            let _guard = guard;
                            let mut stream = stream;
                            let res = compio::time::timeout(
                                handshake_grace,
                                handshake_read(
                                    &mut stream,
                                    cluster_id,
                                    self_id,
                                    replica_count,
                                    max_message_size,
                                    auth.as_ref(),
                                ),
                            )
                            .await;
                            match res {
                                Ok(Ok(peer_id)) => {
                                    on_accepted(stream, peer_id);
                                }
                                Ok(Err(e)) => {
                                    warn!(%peer_addr, "replica handshake failed: {e}");
                                }
                                Err(_) => {
                                    warn!(
                                        %peer_addr,
                                        grace = ?handshake_grace,
                                        "replica handshake exceeded handshake_grace; closing connection"
                                    );
                                }
                            }
                        })
                        .detach();
                    }
                    Err(e) => {
                        error!("Replica listener accept failed: {e}");
                    }
                }
            }
        }
    }
}

/// Decrements the shared in-flight handshake counter when the spawned task
/// ends, on any exit path (success, error, or grace timeout). Pairs with the
/// pre-spawn increment in [`run`] to keep [`MAX_INFLIGHT_HANDSHAKES`] accurate.
struct InflightGuard(Rc<Cell<usize>>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

/// Read the 256 B `ReplicaHello` frame, enforce command + cluster match and the
/// directional rule, then (when `auth` is configured) run the acceptor half
/// of the mutual MAC handshake and return the cryptographically verified
/// peer id. With `auth = None` the listener stays in legacy mode and returns
/// the announced id unverified.
///
/// The dialer (the peer) holds the strictly lower id; this acceptor holds the
/// higher id (see the directional rule). The transcript therefore binds
/// `dialer_id = peer_id`, `acceptor_id = self_id`.
///
/// On a rejection an authenticated, still-waiting dialer is answered with a
/// nonzero-status [`build_challenge_message`] (see [`reject`]) so it learns the
/// cause from its own logs rather than seeing a bare connection close.
#[allow(clippy::future_not_send, clippy::similar_names)]
async fn handshake_read(
    stream: &mut TcpStream,
    our_cluster: u128,
    self_id: u8,
    replica_count: u8,
    max_message_size: usize,
    auth: Option<&ReplicaAuth>,
) -> Result<u8, IggyError> {
    let msg = framing::read_message(stream, max_message_size).await?;
    let header = msg.header();
    let peer_id = header.replica;
    let has_nonce = auth::has_nonce(&header.reserved_command);
    // Only a peer that sent a nonce speaks the authenticated protocol and is
    // waiting to read our response; a legacy (no-nonce) dialer delegates its
    // fd without reading, so a reject frame would land in its VSR reader instead.
    let nackable = auth.is_some() && has_nonce;

    if header.command != Command2::ReplicaHello {
        return reject(
            stream,
            our_cluster,
            self_id,
            peer_id,
            HandshakeStatus::UnknownCommand,
            nackable,
        )
        .await;
    }
    if header.cluster != our_cluster {
        return reject(
            stream,
            our_cluster,
            self_id,
            peer_id,
            HandshakeStatus::ClusterMismatch,
            nackable,
        )
        .await;
    }
    // Directional rule: a replica only accepts inbound from peers with
    // strictly lower ids. The peer is responsible for not dialing us if
    // it has the higher id; this is just defensive.
    if peer_id >= replica_count || peer_id >= self_id {
        return reject(
            stream,
            our_cluster,
            self_id,
            peer_id,
            HandshakeStatus::DirectionalRule,
            nackable,
        )
        .await;
    }

    let Some(auth) = auth else {
        return Ok(peer_id);
    };

    if !has_nonce {
        // Auth is enabled and enforced: a legacy (no-nonce) peer is rejected.
        // No reject frame: a legacy dialer delegates its fd without reading a
        // response.
        return reject(
            stream,
            our_cluster,
            self_id,
            peer_id,
            HandshakeStatus::AuthRequired,
            false,
        )
        .await;
    }

    let nonce_d = auth::read_nonce(&header.reserved_command);
    let nonce_a = auth::random_nonce()?;
    let transcript = Transcript {
        cluster_id: our_cluster,
        dialer_id: peer_id,
        acceptor_id: self_id,
        nonce_d,
        nonce_a,
    };
    let mac_a = auth.acceptor_mac(&transcript);
    framing::write_message(
        stream,
        build_challenge_message(
            our_cluster,
            self_id,
            HandshakeStatus::Ok,
            Some((&nonce_a, &mac_a)),
        ),
    )
    .await?;

    let finish = framing::read_message(stream, max_message_size).await?;
    // Past this point the dialer has delegated its fd (or bailed on `mac_a`) and
    // is no longer reading, so every reject here is log-only (no frame).
    // Check the command before the MAC: the finish frame is identified by its
    // own discriminant, not by handshake position.
    if finish.header().command != Command2::ReplicaFinish {
        return reject(
            stream,
            our_cluster,
            self_id,
            peer_id,
            HandshakeStatus::UnknownCommand,
            false,
        )
        .await;
    }
    let mac_d = auth::read_mac(&finish.header().reserved_command);
    if !auth.verify_dialer_mac(&transcript, &mac_d) {
        return reject(
            stream,
            our_cluster,
            self_id,
            peer_id,
            HandshakeStatus::MacMismatch,
            false,
        )
        .await;
    }
    Ok(peer_id)
}

/// Log a rejected handshake with its `reason` and, when `nack` is set, send a
/// best-effort reject [`build_challenge_message`] (a `ReplicaChallenge` with a
/// nonzero status and no nonce/MAC) so an authenticated, still-waiting dialer
/// learns the cause. `nack` is false for legacy (no-nonce) and post-challenge
/// rejects, whose peer is not reading a response (a frame would reach its VSR
/// reader instead). Always returns `Err` so callers `return`.
#[allow(clippy::future_not_send, clippy::similar_names)]
async fn reject(
    stream: &mut TcpStream,
    cluster_id: u128,
    self_id: u8,
    peer_id: u8,
    reason: HandshakeStatus,
    nack: bool,
) -> Result<u8, IggyError> {
    warn!(
        replica = peer_id,
        reason = reason.as_str(),
        "rejecting replica handshake"
    );
    if nack
        && let Err(e) = framing::write_message(
            stream,
            build_challenge_message(cluster_id, self_id, reason, None),
        )
        .await
    {
        debug!(
            replica = peer_id,
            "failed to send replica handshake reject: {e}"
        );
    }
    Err(IggyError::InvalidCommand)
}

/// Build the acceptor's `ReplicaChallenge` frame. `status` goes at
/// [`auth::STATUS_OFFSET`] ([`HandshakeStatus::Ok`] writes the implicit zero).
/// On success pass `Some((nonce_a, mac_a))`, placed at `reserved_command[0..32]`
/// and `[32..64]`; on a reject pass `None` (no nonce/MAC). The three disjoint
/// regions (nonce / MAC / status) never overlap.
fn build_challenge_message(
    cluster_id: u128,
    replica_id: u8,
    status: HandshakeStatus,
    nonce_mac: Option<(&[u8; auth::NONCE_LEN], &[u8; auth::MAC_LEN])>,
) -> Message<GenericHeader> {
    #[allow(clippy::cast_possible_truncation)]
    Message::<GenericHeader>::new(size_of::<GenericHeader>()).transmute_header(
        |_, h: &mut GenericHeader| {
            h.command = Command2::ReplicaChallenge;
            h.cluster = cluster_id;
            h.replica = replica_id;
            h.size = HEADER_SIZE as u32;
            h.reserved_command[auth::STATUS_OFFSET] = status as u8;
            if let Some((nonce_a, mac_a)) = nonce_mac {
                h.reserved_command[..auth::NONCE_LEN].copy_from_slice(nonce_a);
                h.reserved_command[auth::NONCE_LEN..auth::NONCE_LEN + auth::MAC_LEN]
                    .copy_from_slice(mac_a);
            }
        },
    )
}
