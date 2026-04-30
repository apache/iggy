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

//! Pre-upgrade TCP listener for WebSocket SDK clients.
//!
//! Runs only on shard 0. The accept loop performs no protocol work
//! beyond `TcpListener::accept`: every accepted stream is handed
//! verbatim to the supplied callback, which dups the fd and ships a
//! `ShardFramePayload::ClientWsConnectionSetup` frame to the
//! round-robin-selected target shard. The HTTP-Upgrade handshake runs
//! on the owning shard inside
//! [`crate::ConnectionInstaller::install_client_ws_fd`]; no
//! subprotocol negotiation is performed at the bus layer.
//!
//! The fd at ship-time is plain TCP, so fd-delegation (TCP-only) is
//! well-defined. The WS state machine only materialises after the
//! upgrade, on the owning shard, where it can stay non-`Send`.
//!
//! `ShardFramePayload::ClientWsConnectionSetup` is defined in
//! `core/shard/src/lib.rs`; the rustdoc cannot intra-link across crates
//! without pulling `shard` in as a doc-only dep.

use crate::AcceptedWsClientFn;
use crate::lifecycle::ShutdownToken;
use compio::net::{SocketOpts, TcpListener};
use futures::FutureExt;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::rc::Rc;
use tracing::{debug, error, info};

/// Bind the WS pre-upgrade TCP listener and return the bound address.
///
/// Mirrors [`crate::client_listener::tcp::bind`] in shape (`TCP_NODELAY` +
/// `SO_KEEPALIVE` on by default). The receiving shard re-applies socket
/// options on the dup'd fd via the existing client-install path, so
/// kernel-level options propagate end-to-end.
///
/// # Errors
///
/// Returns [`IggyError::CannotBindToSocket`] if the bind fails.
#[allow(clippy::future_not_send)]
pub async fn bind(addr: SocketAddr) -> Result<(TcpListener, SocketAddr), IggyError> {
    // `SO_REUSEPORT` intentionally not set: only shard 0 binds the WS
    // listener. The shard-0 coordinator round-robins accepts to owning
    // shards via `ShardFramePayload::ClientWsConnectionSetup`.
    let opts = SocketOpts::new().nodelay(true);
    let listener = TcpListener::bind_with_options(addr, &opts)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;
    let actual = listener
        .local_addr()
        .map_err(|e| IggyError::IoError(e.to_string()))?;
    Ok((listener, actual))
}

/// Run the WS pre-upgrade listener accept loop until the shutdown
/// token fires.
///
/// Each accepted [`compio::net::TcpStream`] is handed verbatim to
/// `on_accepted` (no upgrade attempted here). The callback owns the
/// stream from that point on.
#[allow(clippy::future_not_send)]
pub async fn run(listener: TcpListener, token: ShutdownToken, on_accepted: AcceptedWsClientFn) {
    info!(
        "Client listener (WS, pre-upgrade) accepting on {:?}",
        listener.local_addr().ok()
    );

    let on_accepted: Rc<_> = on_accepted;
    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!("Client listener (WS) shutting down");
                break;
            }
            result = listener.accept().fuse() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        debug!(%peer_addr, "WS client TCP accepted, delegating fd pre-upgrade");
                        on_accepted(stream);
                    }
                    Err(e) => {
                        error!("Client listener (WS) accept failed: {e}");
                    }
                }
            }
        }
    }
}
