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
//! # Replica plane = TCP forever (load-bearing invariant)
//!
//! This module is TCP-only by design, not by omission. Do NOT add WS,
//! QUIC, or HTTP paths here. The VSR chain-hash safety proof
//! (`last_prepare_checksum` in `core/consensus/src/plane_helpers.rs`),
//! the fd-delegation path (`F_DUPFD_CLOEXEC` needs a dupable plaintext
//! fd), the `writev` batching in `writer_task.rs`, and the single-digit
//! RTT assumptions used by the view-change timer all rest on a FIFO
//! byte stream between a bounded set of replicas on a trusted LAN. A
//! datagram-oriented or gateway-terminated transport violates one or
//! more of those assumptions. See
//! `Documents/silverhand/iggy/message_bus/transport-plan/INVARIANTS.md`
//! (I1) for the debate context.
//!
//! Runs only on shard 0. On every successful `Ping` handshake the listener
//! hands the accepted `TcpStream` to an `on_accepted` callback provided by
//! the shard bootstrap, which dup-and-ships the fd to the owning shard via
//! the inter-shard channel (see `shard::coordinator::ShardZeroCoordinator`).
//! This module no longer installs writer / reader tasks itself.
//!
//! Duplicate connections are eliminated by directionality: each replica
//! only dials peers with strictly greater ids and only accepts inbound
//! from peers with strictly lower ids. No race, no tiebreaker.
//!
//! # Security
//!
//! The `Ping` handshake validates `cluster_id`, the directional bound,
//! `replica_count`, and a BLAKE3-keyed MAC carried in
//! `GenericHeader.reserved_command[0..57]` (see [`crate::auth`]). Peers
//! that cannot produce a valid tag are rejected. Even so, TLS/encryption is
//! NOT provided here: operators still need to deploy the replica port on a
//! trusted network boundary (cluster-local VPC, private subnet, overlay)
//! if wire confidentiality is required.
//!
//! # Trust model
//!
//! The cluster ships with one shared 32 B secret across all replicas
//! (see [`crate::auth_config`]). Any holder of the secret can mint a
//! valid MAC for ANY `peer_id` in either direction. The dialer-side
//! directional rule (`peer_id > self_id` only dials) plus the
//! acceptor-side check (`parsed.replica < self_id`) blocks UPWARD
//! impersonation - a low-id peer cannot pretend to be a high-id peer
//! during dial - but cross-impersonation in the OTHER direction is open
//! by design under a single shared secret. A compromised replica B can
//! authenticate as any replica A with `A < B`. Mitigation comes from
//! the trusted-LAN deployment assumption + the fact that the
//! authenticated identity is only used for VSR consensus framing on the
//! same plane.
//!
//! Per-peer secrets (each replica gets its own key, the acceptor looks
//! up the right one for the announced `peer_id`) is the documented
//! Phase 2+ hardening. Until then, the cluster operator must treat
//! "secret compromise" as equivalent to "any-replica compromise."

use crate::auth::{self, LABEL_REPLICA, TokenSource};
use crate::framing;
use crate::lifecycle::ShutdownToken;
use crate::{AcceptedReplicaFn, GenericHeader, Message, ReplicaNonceStore};
use compio::net::{SocketOpts, TcpListener, TcpStream};
use futures::FutureExt;
use iggy_binary_protocol::Command2;
use iggy_common::{IggyError, IggyTimestamp};
use std::net::SocketAddr;
use std::rc::Rc;
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
    // `SO_REUSEPORT` intentionally not set: only shard 0 binds the replica
    // listener. Kernel-level accept distribution would fight the shard-0
    // coordinator's explicit round-robin allocation.
    let opts = SocketOpts::new().nodelay(true).keepalive(true);
    let listener = TcpListener::bind_with_options(addr, &opts)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;
    let actual = listener
        .local_addr()
        .map_err(|e| IggyError::IoError(e.to_string()))?;
    Ok((listener, actual))
}

/// Run the inbound replica listener accept loop until the shutdown token
/// fires. Every successful handshake fires the `on_accepted` callback; the
/// callback owns the accepted stream from that point on.
///
/// `nonces` is the bus-resident per-peer nonce dedup store. The listener
/// reaches into it via `borrow_mut().entry(peer).or_default()` for each
/// handshake; the borrow is held only across the synchronous
/// `handshake_verify`, never across an `await`, so the `RefCell` is
/// safe under the single-threaded compio runtime.
#[allow(clippy::future_not_send)]
#[allow(clippy::too_many_arguments)]
pub async fn run(
    listener: TcpListener,
    token: ShutdownToken,
    cluster_id: u128,
    self_id: u8,
    replica_count: u8,
    on_accepted: AcceptedReplicaFn,
    max_message_size: usize,
    token_source: Rc<dyn TokenSource>,
    nonces: Rc<ReplicaNonceStore>,
) {
    info!(
        "Replica listener accepting on {:?}",
        listener.local_addr().ok()
    );
    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!("Replica listener shutting down");
                break;
            }
            result = listener.accept().fuse() => {
                match result {
                    Ok((mut stream, peer_addr)) => {
                        let read = handshake_read(&mut stream, cluster_id, max_message_size).await;
                        let outcome = match read {
                            Ok(parsed) => {
                                let now_ns = IggyTimestamp::now().as_nanos();
                                let mut store = nonces.borrow_mut();
                                let ring = store.entry(parsed.replica).or_default();
                                handshake_verify(
                                    &parsed,
                                    token_source.as_ref(),
                                    ring,
                                    now_ns,
                                    self_id,
                                    replica_count,
                                )
                            }
                            Err(e) => Err(e),
                        };
                        match outcome {
                            Ok(peer_id) => {
                                on_accepted(stream, peer_id);
                            }
                            Err(e) => {
                                warn!(%peer_addr, "replica handshake failed: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        error!("Replica listener accept failed: {e}");
                    }
                }
            }
        }
    }
}

/// Parsed-but-unverified handshake captured from the wire. Held across
/// the await boundary between I/O and the sync verify step.
struct ParsedHandshake {
    replica: u8,
    decoded: auth::DecodedEnvelope,
    challenge: auth::AuthChallenge,
}

/// I/O-only portion of the handshake: read the 256 B `Ping` frame, enforce
/// command + cluster match, and parse the auth envelope's bytes. Does NOT
/// touch the nonce ring or verify the tag.
#[allow(clippy::future_not_send)]
async fn handshake_read(
    stream: &mut TcpStream,
    our_cluster: u128,
    max_message_size: usize,
) -> Result<ParsedHandshake, IggyError> {
    let msg = framing::read_message(stream, max_message_size).await?;
    let header = msg.header();
    if header.command != Command2::Ping {
        return Err(IggyError::InvalidCommand);
    }
    if header.cluster != our_cluster {
        return Err(IggyError::InvalidCommand);
    }
    let decoded = auth::decode_envelope(&header.reserved_command).map_err(auth::to_iggy_error)?;
    let challenge = auth::AuthChallenge {
        cluster: header.cluster,
        peer_id: u128::from(header.replica),
        timestamp_ns: decoded.timestamp_ns,
        release: header.release,
        nonce: decoded.nonce,
        label: LABEL_REPLICA,
    };
    Ok(ParsedHandshake {
        replica: header.replica,
        decoded,
        challenge,
    })
}

/// Synchronous verifier: tag + replay, then the directional tiebreak.
/// Runs outside any await so the `&mut ReplicaNonceRing` borrow cannot
/// span I/O.
fn handshake_verify(
    parsed: &ParsedHandshake,
    token_source: &dyn TokenSource,
    nonces: &mut auth::ReplicaNonceRing,
    now_ns: u128,
    self_id: u8,
    replica_count: u8,
) -> Result<u8, IggyError> {
    auth::verify_envelope(
        token_source,
        &parsed.challenge,
        &parsed.decoded,
        now_ns,
        nonces,
    )
    .map_err(auth::to_iggy_error)?;

    // Directional rule: a replica only accepts inbound from peers with
    // strictly lower ids. The peer is responsible for not dialing us if
    // it has the higher id; this is just defensive.
    if parsed.replica >= replica_count || parsed.replica >= self_id {
        return Err(IggyError::InvalidCommand);
    }
    Ok(parsed.replica)
}
