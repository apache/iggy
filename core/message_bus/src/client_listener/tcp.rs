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

//! TCP listener for SDK clients.
//!
//! Runs only on shard 0. Every accepted connection is handed to an
//! `on_accepted` callback supplied by the shard bootstrap, which mints a
//! client id (encoding the target shard in the top 16 bits), duplicates
//! the fd, and ships the setup frame to the owning shard.
//!
//! Clients send `RequestHeader` messages and receive `ReplyHeader` messages.
//! The owning shard's router handler wraps the duplicated fd and installs
//! writer + reader tasks via [`crate::installer`].

use crate::AcceptedClientFn;
use crate::lifecycle::ShutdownToken;
use compio::net::{SocketOpts, TcpListener};
use futures::FutureExt;
use iggy_common::IggyError;
use std::net::SocketAddr;
use tracing::{debug, error, info};

/// Bind the listener and return the bound address without starting the
/// accept loop. Useful for tests that need the port before driving traffic.
///
/// # Errors
///
/// Returns [`IggyError::CannotBindToSocket`] if the bind fails.
#[allow(clippy::future_not_send)]
pub async fn bind(addr: SocketAddr) -> Result<(TcpListener, SocketAddr), IggyError> {
    // `SO_REUSEPORT` intentionally not set: only shard 0 binds the client
    // listener. The shard-0 coordinator round-robins accepts to owning
    // shards via `ShardFramePayload::ClientConnectionSetup`.
    let opts = SocketOpts::new().nodelay(true);
    let listener = TcpListener::bind_with_options(addr, &opts)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;
    let actual = listener
        .local_addr()
        .map_err(|e| IggyError::IoError(e.to_string()))?;
    Ok((listener, actual))
}

/// Run the client listener accept loop until the shutdown token fires. The
/// `on_accepted` callback owns the accepted stream from that point on.
///
/// # Security
///
/// This listener has no authentication on accept. Deploy behind an
/// authenticating reverse proxy or a trusted-network boundary.
#[allow(clippy::future_not_send)]
pub async fn run(listener: TcpListener, token: ShutdownToken, on_accepted: AcceptedClientFn) {
    info!(
        "Client listener (TCP) accepting on {:?}",
        listener.local_addr().ok()
    );

    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!("Client listener (TCP) shutting down");
                break;
            }
            result = listener.accept().fuse() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        debug!(%peer_addr, "client accepted, delegating fd");
                        on_accepted(stream);
                    }
                    Err(e) => {
                        error!("Client listener (TCP) accept failed: {e}");
                    }
                }
            }
        }
    }
}
