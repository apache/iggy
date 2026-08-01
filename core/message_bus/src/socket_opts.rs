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

/// Build a TCP listener that tolerates `TIME_WAIT` leftovers and bound-but-idle
/// reservation sockets, but refuses a live listener on the same port.
///
/// `SO_REUSEADDR` is all that takes: it rebinds over `TIME_WAIT` remnants of a
/// previous process's connections and over a bound-not-listening reservation
/// socket, while a second bind against an already-LISTENing socket still fails
/// with `EADDRINUSE`. `SO_REUSEPORT` is deliberately NOT set: only shard 0
/// binds the real listener, so nothing here needs the load balancing it
/// exists for, and setting it would let a stale process holding the port
/// silently split the accept queue with this one instead of failing the bind
/// loudly.
pub fn bind_reusable_tcp_listener(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
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

    fn reserve_socket() -> (Socket, SocketAddr) {
        let reserve_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let socket = Socket::new(
            Domain::for_address(reserve_addr),
            Type::STREAM,
            Some(Protocol::TCP),
        )
        .expect("reserve socket");
        socket.set_reuse_address(true).expect("reserve reuseaddr");
        socket.bind(&reserve_addr.into()).expect("reserve bind");
        let addr = socket
            .local_addr()
            .expect("reserve local addr")
            .as_socket()
            .expect("socket addr");
        (socket, addr)
    }

    #[test]
    fn reusable_tcp_listener_can_bind_over_reserved_port() {
        // Bound, not listening: a port held without joining the accept group,
        // which `SO_REUSEADDR` has to tolerate and a plain bind would reject.
        let (_reserve, addr) = reserve_socket();

        let runtime = Runtime::new().expect("runtime");
        runtime.enter(|| {
            let listener = bind_reusable_tcp_listener(addr)
                .expect("second listener should bind on reserved port");
            assert_eq!(listener.local_addr().expect("listener addr"), addr);
        });
    }

    #[test]
    fn reusable_tcp_listener_refuses_port_with_live_listener() {
        // A LISTENing socket on the port is a stale process still serving it.
        // Without `SO_REUSEPORT` the bind must fail loudly instead of joining
        // the accept group and silently splitting inbound connections.
        let (listening, addr) = reserve_socket();
        listening.listen(1).expect("reserve listen");

        let runtime = Runtime::new().expect("runtime");
        runtime.enter(|| {
            let result = bind_reusable_tcp_listener(addr);
            assert!(
                result.is_err(),
                "bind over a live listener must fail with EADDRINUSE"
            );
        });
    }
}
