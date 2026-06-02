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

pub mod connection_handler;
pub mod tcp_listener;
pub mod tcp_server;
pub mod tcp_socket;
pub mod tcp_tls_listener;

pub const COMPONENT: &str = "TCP";

/// Bind a `TcpListener` via the compio 0.19 `TcpSocket` builder.
///
/// compio 0.19 removed `TcpListener::bind_with_options(addr, SocketOpts)`;
/// this is the shared replacement for every legacy listener bind.
/// `SO_REUSEPORT` is always set (thread-per-core: many sockets bind the
/// same addr+port); `reuseaddr` is opt-in. When `tuning` is `Some` and
/// `override_defaults` is set, the socket buffers / keepalive / nodelay
/// are applied (a zero linger maps to `set_zero_linger`; a non-zero
/// linger has no compio-0.19 successor and is dropped).
pub(crate) async fn bind_reuseport_listener(
    addr: std::net::SocketAddr,
    reuseaddr: bool,
    tuning: Option<&crate::configs::tcp::TcpSocketConfig>,
) -> std::io::Result<compio::net::TcpListener> {
    use compio::net::TcpSocket;

    let socket = match addr {
        std::net::SocketAddr::V4(_) => TcpSocket::new_v4().await?,
        std::net::SocketAddr::V6(_) => TcpSocket::new_v6().await?,
    };
    socket.set_reuseport(true)?;
    if reuseaddr {
        socket.set_reuseaddr(true)?;
    }
    if let Some(config) = tuning
        && config.override_defaults
    {
        let recv_buffer_size = config
            .recv_buffer_size
            .as_bytes_u64()
            .try_into()
            .expect("Failed to parse recv_buffer_size for TCP socket");
        let send_buffer_size = config
            .send_buffer_size
            .as_bytes_u64()
            .try_into()
            .expect("Failed to parse send_buffer_size for TCP socket");
        socket.set_recv_buffer_size(recv_buffer_size)?;
        socket.set_send_buffer_size(send_buffer_size)?;
        socket.set_keepalive(config.keepalive)?;
        if config.linger.get_duration().is_zero() {
            socket.set_zero_linger()?;
        }
        socket.set_nodelay(config.nodelay)?;
    }
    socket.bind(addr).await?;
    socket.listen(1024).await
}
