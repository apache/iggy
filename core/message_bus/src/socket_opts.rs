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

//! Per-connection socket options.
//!
//! `compio::net::SocketOpts` exposes `keepalive(bool)` only - it does not
//! let callers tune `TCP_KEEPIDLE`, `TCP_KEEPINTVL`, or `TCP_KEEPCNT`. We
//! reach through to `socket2::SockRef` for the finer knobs so the bus can
//! detect a silently dead peer within a predictable bound without waiting
//! for the kernel defaults (2 hours idle on Linux).

use compio::net::TcpStream;
use socket2::{SockRef, TcpKeepalive};
use std::io;
use std::time::Duration;

/// Configure TCP keepalive on a freshly accepted or freshly dialed
/// connection. Uses `socket2` directly because compio's `SocketOpts` only
/// exposes the on/off toggle; we need the idle/interval/count tuning to get
/// a bounded detection window on a half-dead peer.
///
/// `idle` maps to `TCP_KEEPIDLE`, `interval` to `TCP_KEEPINTVL`, `retries`
/// to `TCP_KEEPCNT`. Total detection window is roughly
/// `idle + retries * interval`; keep it comfortably shorter than the VSR
/// view-change timeout so replicas can observe a dead peer ahead of the
/// consensus-level decision.
///
/// # Errors
///
/// Returns the underlying `io::Error` if the kernel rejects any of the
/// setsockopt calls. Callers log-and-continue rather than tear down the
/// connection, because a missing keepalive is a soft failure.
pub fn apply_keepalive_for_connection(
    stream: &TcpStream,
    idle: Duration,
    interval: Duration,
    retries: u32,
) -> io::Result<()> {
    let sock = SockRef::from(stream);
    let params = TcpKeepalive::new()
        .with_time(idle)
        .with_interval(interval)
        .with_retries(retries);
    sock.set_tcp_keepalive(&params)
}

/// Disable Nagle on a per-connection socket.
///
/// Linux does not propagate `TCP_NODELAY` from a listener socket to the fd
/// returned by `accept(2)`, so the bus must toggle it per-connection on both
/// outbound dials and freshly accepted streams. Matching behaviour on both
/// halves keeps small consensus frames (PrepareOk/Commit/SVC/DVC/SV) from
/// getting held by the 40 ms Nagle coalescer.
///
/// # Errors
///
/// Returns the underlying `io::Error` if the kernel rejects the setsockopt
/// call. Callers log-and-continue; VSR's prepare timeout absorbs a stray
/// Nagle-delayed frame on the soft-failure path.
pub fn apply_nodelay_for_connection(stream: &TcpStream) -> io::Result<()> {
    SockRef::from(stream).set_tcp_nodelay(true)
}
