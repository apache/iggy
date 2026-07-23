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

use compio::net::{TcpListener, TcpStream};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use std::io;
use std::net::SocketAddr;

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

/// Build a TCP listener that tolerates another socket already holding the port.
///
/// Only shard 0 binds the real listener, so nothing here needs the load
/// balancing `SO_REUSEPORT` exists for; it only widens what the bind accepts.
///
/// TODO: work out whether these listeners need `SO_REUSEPORT` at all. Dropping
/// it would make a stale process holding the port a loud bind failure instead
/// of two live listeners silently splitting the accept queue.
pub fn bind_reusable_tcp_listener(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(all(
        unix,
        not(any(target_os = "illumos", target_os = "solaris", target_os = "cygwin"))
    ))]
    socket.set_reuse_port(true)?;
    socket.bind(&addr.into())?;
    socket.listen(libc::SOMAXCONN)?;
    socket.set_nonblocking(true)?;

    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

#[cfg(test)]
mod tests {
    use super::bind_reusable_tcp_listener;
    use compio::runtime::Runtime;
    use socket2::{Domain, Protocol, Socket, Type};
    use std::net::{Ipv4Addr, SocketAddr};

    #[test]
    fn reusable_tcp_listener_can_bind_over_reserved_port() {
        let reserve_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let socket = Socket::new(
            Domain::for_address(reserve_addr),
            Type::STREAM,
            Some(Protocol::TCP),
        )
        .expect("reserve socket");
        socket.set_reuse_address(true).expect("reserve reuseaddr");
        #[cfg(all(
            unix,
            not(any(target_os = "illumos", target_os = "solaris", target_os = "cygwin"))
        ))]
        socket.set_reuse_port(true).expect("reserve reuseport");
        // Bound, not listening: a port held without joining the accept group,
        // which is what the reuse flags have to tolerate and what a plain bind
        // would reject.
        socket.bind(&reserve_addr.into()).expect("reserve bind");
        let addr = socket
            .local_addr()
            .expect("reserve local addr")
            .as_socket()
            .expect("socket addr");

        let runtime = Runtime::new().expect("runtime");
        runtime.enter(|| {
            let listener = bind_reusable_tcp_listener(addr)
                .expect("second listener should bind on reserved port");
            assert_eq!(listener.local_addr().expect("listener addr"), addr);
        });
    }
}
