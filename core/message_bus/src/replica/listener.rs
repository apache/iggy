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
//! `on_accepted` callback provided by the shard bootstrap, which
//! dup-and-ships the raw fd to the owning shard via the inter-shard
//! channel (blind delegation - the peer id is unknown until the owning
//! shard reads the `ReplicaHello`). The owning shard runs the full
//! handshake (see [`crate::replica::handshake`]) and installs the
//! connection locally, so no connection state ever crosses shards.
//!
//! Admission control also lives in the callback: it acquires a slot
//! from the shard-0-global in-flight handshake cap
//! (`IggyMessageBus::try_acquire_replica_handshake_slot`) and drops the
//! connection when the cap is reached. The slot is released by the
//! owning shard's handshake-outcome ack, with a deadline-based expiry
//! as the lost-ack backstop.
//!
//! Duplicate connections are eliminated by directionality: each replica
//! only dials peers with strictly greater ids and only accepts inbound
//! from peers with strictly lower ids. No race, no tiebreaker. The
//! directional check itself runs on the owning shard, inside the
//! handshake.

use crate::lifecycle::ShutdownToken;
use crate::socket_opts::bind_reusable_tcp_listener;
use crate::{AcceptedReplicaFn, GenericHeader, Message};
use compio::net::TcpListener;
use futures::FutureExt;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::rc::Rc;
use tracing::{debug, error, info};

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

/// Run the inbound replica listener accept loop until the shutdown token
/// fires.
///
/// Every accepted connection fires `on_accepted` immediately; no byte is
/// read here. The callback owns the accepted stream and is responsible
/// for the in-flight cap check and the fd delegation (see the module
/// doc). The loop never awaits anything but `accept()`, so a hostile or
/// slow peer cannot stall admission of subsequent peers.
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
                    }
                }
            }
        }
    }
}
