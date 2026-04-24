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

use crate::auth::{self, NonceRing, TokenSource};
use crate::framing;
use crate::lifecycle::ShutdownToken;
use crate::{AcceptedReplicaFn, GenericHeader, Message};
use compio::net::{SocketOpts, TcpListener, TcpStream};
use futures::FutureExt;
use iggy_binary_protocol::Command2;
use iggy_common::{IggyError, IggyTimestamp};
use std::cell::RefCell;
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
) {
    info!(
        "Replica listener accepting on {:?}",
        listener.local_addr().ok()
    );
    let nonces = RefCell::new(NonceRing::default());
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
                                let mut ring = nonces.borrow_mut();
                                handshake_verify(
                                    &parsed,
                                    token_source.as_ref(),
                                    &mut ring,
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
    };
    Ok(ParsedHandshake {
        replica: header.replica,
        decoded,
        challenge,
    })
}

/// Synchronous verifier: tag + replay, then the directional tiebreak.
/// Runs outside any await so the `&mut NonceRing` borrow cannot span I/O.
fn handshake_verify(
    parsed: &ParsedHandshake,
    token_source: &dyn TokenSource,
    nonces: &mut NonceRing,
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
