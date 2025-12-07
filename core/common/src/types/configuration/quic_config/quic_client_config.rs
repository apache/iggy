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

use crate::{
    AutoLogin, ConnectionString, ConnectionStringOptions, IggyDuration,
    QuicClientReconnectionConfig, QuicConnectionStringOptions,
};
use std::str::FromStr;

/// Configuration for the QUIC client.
#[derive(Debug, Clone)]
pub struct QuicClientConfig {
    /// The address to bind the QUIC client to.
    pub client_address: String,
    /// The address of the QUIC server to connect to.
    pub server_address: String,
    /// The server name to use.
    pub server_name: String,
    /// Whether to automatically login user after establishing connection.
    pub auto_login: AutoLogin,
    // Whether to automatically reconnect when disconnected.
    pub reconnection: QuicClientReconnectionConfig,
    /// The size of the response buffer.
    pub response_buffer_size: u64,
    /// The maximum number of concurrent bidirectional streams.
    pub max_concurrent_bidi_streams: u64,
    /// The size of the datagram send buffer.
    pub datagram_send_buffer_size: u64,
    /// The initial MTU.
    pub initial_mtu: u16,
    /// The send window.
    pub send_window: u64,
    /// The receive window.
    pub receive_window: u64,
    /// The keep alive interval.
    pub keep_alive_interval: u64,
    /// The maximum idle timeout.
    pub max_idle_timeout: u64,
    /// Whether to validate the server certificate.
    pub validate_certificate: bool,
    /// Interval of heartbeats sent by the client
    pub heartbeat_interval: IggyDuration,
}

impl Default for QuicClientConfig {
    fn default() -> QuicClientConfig {
        QuicClientConfig {
            client_address: "127.0.0.1:0".to_string(),
            server_address: "127.0.0.1:8080".to_string(),
            server_name: "localhost".to_string(),
            auto_login: AutoLogin::Disabled,
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
            reconnection: QuicClientReconnectionConfig::default(),
            response_buffer_size: 1000 * 1000 * 10,
            max_concurrent_bidi_streams: 10000,
            datagram_send_buffer_size: 100_000,
            initial_mtu: 1200,
            send_window: 100_000,
            receive_window: 100_000,
            keep_alive_interval: 5000,
            max_idle_timeout: 10000,
            validate_certificate: false,
        }
    }
}

impl From<ConnectionString<QuicConnectionStringOptions>> for QuicClientConfig {
    fn from(connection_string: ConnectionString<QuicConnectionStringOptions>) -> Self {
        QuicClientConfig {
            client_address: "127.0.0.1:0".to_string(),
            server_address: connection_string.server_address().into(),
            server_name: "localhost".to_string(),
            auto_login: connection_string.auto_login().to_owned(),
            reconnection: connection_string.options().reconnection().to_owned(),
            response_buffer_size: connection_string.options().response_buffer_size(),
            max_concurrent_bidi_streams: connection_string.options().max_concurrent_bidi_streams(),
            datagram_send_buffer_size: connection_string.options().datagram_send_buffer_size(),
            initial_mtu: connection_string.options().initial_mtu(),
            send_window: connection_string.options().send_window(),
            receive_window: connection_string.options().receive_window(),
            keep_alive_interval: connection_string.options().keep_alive_interval(),
            max_idle_timeout: connection_string.options().max_idle_timeout(),
            validate_certificate: connection_string.options().validate_certificate(),
            heartbeat_interval: connection_string.options().heartbeat_interval(),
        }
    }
}
