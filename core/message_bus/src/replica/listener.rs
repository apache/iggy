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
//! the fd-delegation path (`F_DUPFD_CLOEXEC` dups the raw TCP fd before
//! any protocol state exists), and the single-digit RTT assumptions
//! used by the view-change timer all rest on a FIFO byte stream between
//! a bounded set of replicas. A datagram-oriented or gateway-terminated
//! transport violates one or more of those assumptions.
//!
//! Runs only on shard 0. The listener reads NOTHING from an accepted
//! connection: every accepted `TcpStream` is handed as-is to the
//! `on_accepted` callback provided by the shard bootstrap. The callback
//! reads only the 8-byte placement preamble
//! (`IggyMessageBus::place_replica_inbound`), which names the dialer and
//! the link, and dup-and-ships the raw fd to the shard that owns that
//! link via the inter-shard channel. The owning shard runs the full
//! handshake (see [`crate::replica::handshake`]) and installs the
//! connection locally, so no TLS or handshake state ever crosses shards.
//!
//! Admission control also lives in the callback: it acquires a slot
//! from the shard-0-global in-flight handshake cap
//! (`IggyMessageBus::try_acquire_replica_handshake_slot`) before the
//! preamble read and drops the connection when the cap is reached. The
//! slot is released by the owning shard's handshake-outcome ack, with a
//! deadline-based expiry as the lost-ack backstop.
//!
//! Duplicate connections are eliminated by directionality: each replica
//! only dials peers with strictly greater ids and only accepts inbound
//! from peers with strictly lower ids. No race, no tiebreaker. The
//! directional check itself runs on the owning shard, inside the
//! handshake.

use crate::accept::pause_after_accept_error;
use crate::lifecycle::ShutdownToken;
use crate::socket_opts::bind_reusable_tcp_listener;
use crate::{
    AcceptedReplicaFn, GenericHeader, IggyMessageBus, MAX_INFLIGHT_REPLICA_HANDSHAKES, Message,
    ReplicaLinkId,
};
use compio::net::{TcpListener, TcpStream};
use futures::FutureExt;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Instant;
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

/// Shard 0 side of an accepted replica socket, for the `on_accepted`
/// callback.
///
/// Takes a slot in the in-flight handshake cap, then reads the placement
/// preamble in a task tracked on `bus`, so a slow dialer never stalls the
/// accept loop. A placed socket goes to `delegate` with its link, the shard
/// that owns the link, the slot and the admission deadline: one
/// `handshake_grace` after the slot was taken. The preamble read and the
/// owner shard's handshake both end by that deadline, so a live handshake
/// never outlasts its slot. From there the slot belongs to `delegate`: the
/// owner shard's handshake ack releases it, and a failed delegation must
/// release it at once. A full cap or a refused preamble closes the socket.
pub fn admit_inbound(
    bus: &Rc<IggyMessageBus>,
    stream: TcpStream,
    delegate: impl FnOnce(TcpStream, ReplicaLinkId, u16, u64, Instant) + 'static,
) {
    let Some(slot) = bus.try_acquire_replica_handshake_slot() else {
        warn!(
            cap = MAX_INFLIGHT_REPLICA_HANDSHAKES,
            "replica handshake in-flight cap reached; dropping inbound"
        );
        return;
    };
    let deadline = Instant::now() + bus.config().handshake_grace;
    let task_bus = Rc::clone(bus);
    let handle = compio::runtime::spawn(async move {
        let mut stream = stream;
        match task_bus.place_replica_inbound(&mut stream, deadline).await {
            Ok((link, owner)) => delegate(stream, link, owner, slot, deadline),
            Err(error) => {
                task_bus.release_replica_handshake_slot(slot);
                warn!(
                    peer = ?stream.peer_addr().ok(),
                    "refusing inbound replica connection: {error}"
                );
            }
        }
    });
    bus.track_background(handle);
}

/// Run the inbound replica listener accept loop until the shutdown token
/// fires.
///
/// Every accepted connection fires `on_accepted` immediately; no byte is
/// read here. The callback owns the accepted stream and is responsible
/// for the in-flight cap check, the preamble read and the fd delegation
/// (see [`admit_inbound`] and the module doc). The loop awaits only
/// `accept()` and, after an error that says a resource is used up, the
/// pause of [`pause_after_accept_error`]. Neither waits on what a peer
/// sends, so a hostile or slow peer cannot stall admission of subsequent
/// peers.
#[allow(clippy::future_not_send)]
pub async fn run(listener: TcpListener, token: ShutdownToken, on_accepted: AcceptedReplicaFn) {
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
                    Ok((stream, _peer_addr)) => {
                        on_accepted(stream);
                    }
                    Err(e) => {
                        error!("Replica listener accept failed: {e}");
                        pause_after_accept_error(&e, &token).await;
                    }
                }
            }
        }
    }
}
