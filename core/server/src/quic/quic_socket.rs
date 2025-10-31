/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use std::num::TryFromIntError;

use crate::configs::quic::QuicSocketConfig;

/// Build a UDP socket for the given address and configure the options that are
/// required by the server
pub fn build(addr: &SocketAddr, config: &QuicSocketConfig) -> Socket {
    // Choose the correct address family based on the target address
    let socket = Socket::new(Domain::for_address(*addr), Type::DGRAM, Some(Protocol::UDP))
        .expect("Unable to create a UDP socket");

    // Allow multiple sockets (shards) to bind to the same address
    socket
        .set_reuse_address(true)
        .expect("Unable to set SO_REUSEADDR on socket");

    // SO_REUSEPORT is only available on Unix-like systems
    #[cfg(unix)]
    socket
        .set_reuse_port(true)
        .expect("Unable to set SO_REUSEPORT on socket");

    // Configure socket buffer sizes and keepalive if override is enabled
    if config.override_defaults {
        config
            .recv_buffer_size
            .as_bytes_u64()
            .try_into()
            .map_err(|e: TryFromIntError| std::io::Error::other(e.to_string()))
            .and_then(|size| socket.set_recv_buffer_size(size))
            .expect("Unable to set SO_RCVBUF on socket");

        config
            .send_buffer_size
            .as_bytes_u64()
            .try_into()
            .map_err(|e: TryFromIntError| std::io::Error::other(e.to_string()))
            .and_then(|size| socket.set_send_buffer_size(size))
            .expect("Unable to set SO_SNDBUF on socket");

        socket
            .set_keepalive(config.keepalive)
            .expect("Unable to set SO_KEEPALIVE on socket");
    }

    socket
}
