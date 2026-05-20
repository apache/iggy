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
//! Keepalive is intentionally NOT configured here: SDK clients manage
//! their own keepalive policy at the application layer, and
//! replica<->replica liveness is observed by VSR heartbeats rather
//! than by `SO_KEEPALIVE`. Only `TCP_NODELAY` toggling lives in this
//! module today.

use compio::net::TcpStream;
use socket2::SockRef;
use std::io;

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
