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

use std::io;
use thiserror::Error;

/// Transport-level error for `MessageBus::send_to_*` operations.
///
/// All variants are non-fatal from the consensus perspective.
/// VSR handles message loss via timeout-driven retransmission from the WAL.
///
/// The client- and replica-keyed variants separate three physically
/// distinct failure modes so operators can tell apart routing bugs from
/// transient disconnects:
/// - `*NotFound` / `*NotConnected` — the key is unknown to this shard's
///   local registry (genuine peer state, often recoverable by reconnect).
/// - `*RouteMissing` — the bus cannot forward to the owning shard
///   because no forward fn was installed (bootstrap ordering bug).
/// - `*ForwardFailed` — the forward fn rejected the frame (inter-shard
///   queue full, shutdown, etc.).
#[derive(Debug, Error)]
pub enum SendError {
    #[error("client {0} not found in local registry")]
    ClientNotFound(u128),

    #[error("client {0}: no inter-shard forward fn installed")]
    ClientRouteMissing(u128),

    #[error("client {0}: inter-shard forward failed")]
    ClientForwardFailed(u128),

    #[error("replica {0} not connected on this shard")]
    ReplicaNotConnected(u8),

    #[error("replica {0}: no inter-shard forward fn installed")]
    ReplicaRouteMissing(u8),

    #[error("replica {0}: inter-shard forward failed")]
    ReplicaForwardFailed(u8),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("bus is shutting down")]
    BusShuttingDown,

    #[error("queue full, message dropped")]
    Backpressure,

    #[error("inter-shard routing to shard {0} failed")]
    RoutingFailed(u16),

    #[error("fd duplication failed: {0}")]
    DupFailed(#[source] io::Error),
}
