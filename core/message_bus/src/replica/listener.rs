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
//! Runs only on shard 0. On every accepted `Ping` frame the listener
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
//! # Auth
//!
//! The `Ping` frame announces the dialing replica's id in plaintext;
//! the bus uses it to key the registry but does NOT validate replica
//! identity at the transport layer. Mirrors the SDK-client plane: the
//! connection always succeeds, identity is established post-handshake
//! by the caller (`server-ng`) via the future `LOGIN_REPLICA` command
//! that carries the cluster's shared secret. Until that command
//! succeeds the caller MUST treat the replica as unauthenticated and
//! refuse to honor consensus messages from it.
//!
//! TLS / encryption is NOT provided here: operators still need to
//! deploy the replica port on a trusted network boundary
//! (cluster-local VPC, private subnet, overlay) if wire confidentiality
//! is required.
//
// TODO(F3-auth): commit c9f5aeb4a deleted the prior replica-plane
// authn (BLAKE3-keyed MAC over `GenericHeader.reserved_command[0..57]`
// + per-peer nonce ring under `auth/`, ~900 LOC) without mention in
// the commit body. Pre-PR the inbound `Ping` was MAC-verified before
// the registry insert; post-PR only `cluster_id` + directional rule
// gate the connection. `LOGIN_REPLICA` (referenced above as the
// promised mitigation) does not exist anywhere in the tree. Until it
// lands, an attacker on the trusted-LAN boundary can forge `Ping`
// frames that key the registry and drive consensus traffic. Restore
// transport-layer MAC, or land `LOGIN_REPLICA`, before relying on
// network-boundary trust alone.

use crate::framing;
use crate::lifecycle::ShutdownToken;
use crate::{AcceptedReplicaFn, GenericHeader, Message};
use compio::net::{SocketOpts, TcpListener, TcpStream};
use compio::runtime::JoinHandle;
use futures::FutureExt;
use iggy_binary_protocol::Command2;
use iggy_common::IggyError;
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
    // `SO_REUSEPORT` intentionally not set: only shard 0 binds the replica
    // listener. Kernel-level accept distribution would fight the shard-0
    // coordinator's explicit round-robin allocation.
    let opts = SocketOpts::new().nodelay(true);
    let listener = TcpListener::bind_with_options(addr, &opts)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;
    let actual = listener
        .local_addr()
        .map_err(|e| IggyError::IoError(e.to_string()))?;
    Ok((listener, actual))
}

/// Run the inbound replica listener accept loop until the shutdown token fires.
///
/// Every accepted connection that passes the plaintext `Ping` frame
/// check fires the `on_accepted` callback; the callback owns the
/// accepted stream from that point on.
///
/// Each [`TcpStream`] returned by `accept()` is handed to its own
/// spawned task that runs the [`handshake_read`] under
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
/// Completed handshake handles are reaped opportunistically at the top
/// of the loop so the in-flight handle vector stays bounded for the
/// listener's lifetime; remaining handles are awaited on shutdown so
/// in-progress handshakes get a chance to finish (or hit the grace
/// timeout) before the listener fully exits.
#[allow(clippy::future_not_send, clippy::too_many_arguments)]
pub async fn run(
    listener: TcpListener,
    token: ShutdownToken,
    cluster_id: u128,
    self_id: u8,
    replica_count: u8,
    on_accepted: AcceptedReplicaFn,
    max_message_size: usize,
    handshake_grace: Duration,
) {
    info!(
        "Replica listener accepting on {:?}",
        listener.local_addr().ok()
    );
    let mut handshake_handles: Vec<JoinHandle<()>> = Vec::new();
    loop {
        // Drop completed handles before blocking on the next accept so the
        // vector stays bounded under sustained inbound traffic. Dropping
        // an already-finished `Task` is a no-op (no in-flight work to
        // cancel).
        handshake_handles.retain(|h| !h.is_finished());

        futures::select! {
            () = token.wait().fuse() => {
                debug!("Replica listener shutting down");
                break;
            }
            result = listener.accept().fuse() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        let on_accepted = on_accepted.clone();
                        let handle = compio::runtime::spawn(async move {
                            let mut stream = stream;
                            let res = compio::time::timeout(
                                handshake_grace,
                                handshake_read(
                                    &mut stream,
                                    cluster_id,
                                    self_id,
                                    replica_count,
                                    max_message_size,
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
                        });
                        handshake_handles.push(handle);
                    }
                    Err(e) => {
                        error!("Replica listener accept failed: {e}");
                    }
                }
            }
        }
    }

    for handle in handshake_handles {
        let _ = handle.await;
    }
}

/// Read the 256 B `Ping` frame, enforce command + cluster match and the
/// directional rule, return the announced replica id. No transport-level
/// authentication.
#[allow(clippy::future_not_send)]
async fn handshake_read(
    stream: &mut TcpStream,
    our_cluster: u128,
    self_id: u8,
    replica_count: u8,
    max_message_size: usize,
) -> Result<u8, IggyError> {
    let msg = framing::read_message(stream, max_message_size).await?;
    let header = msg.header();
    if header.command != Command2::Ping {
        return Err(IggyError::InvalidCommand);
    }
    if header.cluster != our_cluster {
        return Err(IggyError::InvalidCommand);
    }
    // Directional rule: a replica only accepts inbound from peers with
    // strictly lower ids. The peer is responsible for not dialing us if
    // it has the higher id; this is just defensive.
    if header.replica >= replica_count || header.replica >= self_id {
        return Err(IggyError::InvalidCommand);
    }
    Ok(header.replica)
}
