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

//! Replica handshake halves, run on the OWNING shard.
//!
//! Shard 0 accepts / dials raw TCP and delegates the fd before any byte
//! is read or written (blind delegation); the owning shard then runs one
//! of the two halves here. The acceptor half reads the `ReplicaHello`,
//! enforces command / cluster / directional checks, and (when auth is
//! configured) the acceptor side of the 3-message mutual BLAKE3
//! keyed-MAC handshake over the `reserved_command` bytes (see
//! [`crate::replica::auth`]). The dialer half announces this replica's
//! id and runs the dialer side of the same exchange.
//!
//! # Trust boundary (PSK handshake)
//!
//! When `cluster.auth.enabled` is set the acceptor REJECTS any peer that
//! does not complete the handshake. The MAC proves the peer possesses
//! the cluster PSK: it authenticates cluster MEMBERSHIP, not per-replica
//! identity. The peer id that keys the registry is the announced
//! `ReplicaHello` header byte. It is folded into the keyed MAC (a dialer
//! cannot MAC one id and announce another), but the MAC key is a single
//! cluster-wide PSK-derived subkey, not a per-replica key, so any PSK
//! holder can mint a valid MAC for any (smaller) replica id - there is
//! no anti-Sybil guarantee. With auth disabled the acceptor stays in
//! legacy mode: the `ReplicaHello` id is trusted unverified.
//!
//! Enabling auth is a coordinated-restart change, and not the only one:
//! the consensus `cluster_id` is derived from the cluster name
//! unconditionally (`auth::cluster_domain_id`, was a constant), so a
//! mixed-version roster fails to connect regardless of the auth setting.
//! Flip every node in one restart.
//!
//! Because the PSK gates membership and not identity, an on-wire
//! attacker who learns the cluster id (auth off) OR an insider
//! key-holder (auth on) can register as any peer with a smaller replica
//! id, so operators MUST either enable `cluster.tls` (the owning shard
//! wraps the delegated stream in TLS 1.3 before any handshake frame
//! flows; see [`ReplicaTlsCtx`]) or deploy the replica port on a
//! trusted L2 boundary (cluster-local VPC, dedicated private subnet,
//! encrypted overlay such as `WireGuard`, or an air-gapped management
//! network).
//
// TODO(hubcio): follow-up - dual-key rotation acceptance window.

use crate::framing;
use crate::replica::auth::{self, ChannelBinding, HandshakeStatus, ReplicaAuth, Transcript};
use crate::{GenericHeader, Message};
use compio::io::{AsyncRead, AsyncWrite};
use iggy_binary_protocol::{Command2, HEADER_SIZE};
use iggy_common::IggyError;
use rustls::pki_types::ServerName;
use std::mem::size_of;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{debug, warn};

/// Identity and auth parameters the owning shard needs to run a
/// delegated replica handshake. Built once at bootstrap and installed on
/// every shard's bus via `IggyMessageBus::set_replica_handshake_ctx`.
///
/// `auth` carries the PSK-derived MAC keys; `tls` the rustls material
/// for wrapping the delegated stream before the handshake frames flow.
/// `Rc`-shared clones are cheap.
#[derive(Clone)]
pub struct ReplicaHandshakeCtx {
    pub cluster_id: u128,
    pub self_id: u8,
    pub replica_count: u8,
    pub auth: Option<Rc<ReplicaAuth>>,
    pub tls: Option<Rc<ReplicaTlsCtx>>,
}

/// TLS material for the replica plane.
///
/// `server` drives the acceptor (TLS server) role on a delegated inbound
/// connection; `client` drives the dialer role on a delegated outbound
/// one. `peer_names` maps a replica id to the name the dialer presents
/// in SNI and verifies the peer's certificate against; in self-signed
/// mode the client config's verifier accepts any certificate and only
/// the PSK handshake authenticates the peer, so the name only feeds SNI.
///
/// Both configs are TLS 1.3 only with the `iggy-replica` ALPN
/// ([`crate::transports::tls::REPLICA_ALPN`]); a client-plane TLS
/// endpoint mistakenly dialed into the replica port fails the ALPN
/// negotiation instead of reaching the replica handshake.
pub struct ReplicaTlsCtx {
    pub server: Arc<rustls::ServerConfig>,
    pub client: Arc<rustls::ClientConfig>,
    /// Indexed by replica id; same length as the roster.
    pub peer_names: Vec<ServerName<'static>>,
}

/// Run the acceptor half on a delegated inbound stream and return the
/// verified peer id.
///
/// Reads the 256 B `ReplicaHello` frame, enforces command + cluster
/// match and the directional rule, then (when `auth` is configured)
/// runs the acceptor side of the mutual MAC handshake with `binding`
/// folded into every MAC (the TLS exporter on a TLS link, see
/// [`ChannelBinding`]). With `auth = None` the acceptor stays in legacy
/// mode and returns the announced id unverified.
///
/// The dialer (the peer) holds the strictly lower id; this acceptor
/// holds the higher id (see the directional rule). The transcript
/// therefore binds `dialer_id = peer_id`, `acceptor_id = self_id`.
///
/// On a rejection an authenticated, still-waiting dialer is answered
/// with a nonzero-status [`build_challenge_message`] (see [`reject`]) so
/// it learns the cause from its own logs rather than seeing a bare
/// connection close.
///
/// # Errors
///
/// Returns an error on any framing failure or rejected handshake; the
/// caller drops the stream.
#[allow(clippy::future_not_send, clippy::similar_names, clippy::too_many_lines)]
pub async fn acceptor_handshake<S: AsyncRead + AsyncWrite>(
    stream: &mut S,
    ctx: &ReplicaHandshakeCtx,
    binding: ChannelBinding,
    max_message_size: usize,
) -> Result<u8, IggyError> {
    let our_cluster = ctx.cluster_id;
    let self_id = ctx.self_id;
    let auth = ctx.auth.as_deref();
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
    if peer_id >= ctx.replica_count || peer_id >= self_id {
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
        binding,
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
    // Past this point the dialer has been installed on its owning shard (or
    // bailed on `mac_a`) and is no longer reading handshake frames, so every
    // reject here is log-only (no frame).
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

/// Run the dialer half on a delegated outbound stream.
///
/// Logs and returns `Err(())` on any failure; the caller drops the
/// stream and the shard-0 periodic sweep retries once the pending-dial
/// entry clears. With `auth = None` it sends a single plaintext
/// `ReplicaHello`. `binding` is folded into every MAC; both ends must
/// derive it from the same channel (see [`ChannelBinding`]).
///
/// The dialer holds the strictly lower id, the acceptor the higher; the
/// transcript binds `dialer_id = self_id`, `acceptor_id = peer_id`. If
/// the acceptor answers with a nonzero `ReplicaChallenge` status (a
/// reject), the reason is logged and the dial is abandoned.
///
/// # Errors
///
/// Returns `Err(())` on any write, read, command, status, or MAC
/// failure (already logged).
#[allow(clippy::future_not_send, clippy::similar_names)]
pub async fn dialer_handshake<S: AsyncRead + AsyncWrite>(
    stream: &mut S,
    ctx: &ReplicaHandshakeCtx,
    peer_id: u8,
    binding: ChannelBinding,
    max_message_size: usize,
) -> Result<(), ()> {
    let cluster_id = ctx.cluster_id;
    let self_id = ctx.self_id;
    let Some(auth) = ctx.auth.as_deref() else {
        let hello = build_hello_message(cluster_id, self_id, None);
        if let Err(e) = framing::write_message(stream, hello).await {
            warn!(replica = peer_id, "handshake write failed: {e}");
            return Err(());
        }
        return Ok(());
    };

    let nonce_d = match auth::random_nonce() {
        Ok(nonce) => nonce,
        Err(e) => {
            warn!(replica = peer_id, "nonce generation failed: {e}");
            return Err(());
        }
    };
    let hello = build_hello_message(cluster_id, self_id, Some(&nonce_d));
    if let Err(e) = framing::write_message(stream, hello).await {
        warn!(replica = peer_id, "handshake write failed: {e}");
        return Err(());
    }

    let challenge = match framing::read_message(stream, max_message_size).await {
        Ok(msg) => msg,
        Err(e) => {
            warn!(replica = peer_id, "handshake read failed: {e}");
            return Err(());
        }
    };
    if challenge.header().command != Command2::ReplicaChallenge {
        warn!(
            replica = peer_id,
            command = ?challenge.header().command,
            "unexpected replica handshake response command"
        );
        return Err(());
    }
    // Read command + status before nonce/MAC: a reject (or garbage) status
    // carries no valid nonce/MAC, so parsing them would be meaningless.
    let status = auth::read_status(&challenge.header().reserved_command);
    if status != HandshakeStatus::Ok {
        warn!(
            replica = peer_id,
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
        binding,
    };
    if !auth.verify_acceptor_mac(&transcript, &mac_a) {
        warn!(
            replica = peer_id,
            "replica handshake MAC verification failed"
        );
        return Err(());
    }
    let mac_d = auth.dialer_mac(&transcript);
    if let Err(e) =
        framing::write_message(stream, build_finish_message(cluster_id, self_id, &mac_d)).await
    {
        warn!(replica = peer_id, "handshake finish write failed: {e}");
        return Err(());
    }
    Ok(())
}

/// Log a rejected handshake with its `reason` and, when `nack` is set, send a
/// best-effort reject [`build_challenge_message`] (a `ReplicaChallenge` with a
/// nonzero status and no nonce/MAC) so an authenticated, still-waiting dialer
/// learns the cause. `nack` is false for legacy (no-nonce) and post-challenge
/// rejects, whose peer is not reading a response (a frame would reach its VSR
/// reader instead). Always returns `Err` so callers `return`.
#[allow(clippy::future_not_send, clippy::similar_names)]
async fn reject<S: AsyncRead + AsyncWrite>(
    stream: &mut S,
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
