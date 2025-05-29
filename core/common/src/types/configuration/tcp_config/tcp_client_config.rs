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
use crate::types::configuration::auth_config::connection_string::ConnectionString;
use crate::types::configuration::auth_config::connection_string_options::ConnectionStringOptions;
use crate::types::configuration::tcp_config::tcp_connection_string_options::TcpConnectionStringOptions;
use crate::{AutoLogin, IggyDuration, TcpClientReconnectionConfig};
use std::str::FromStr;

/// Configuration for the TCP client.
#[derive(Debug, Clone)]
pub struct TcpClientConfig {
    /// The address of the Iggy server.
    pub server_address: String,
    /// Whether to use TLS when connecting to the server.
    pub tls_enabled: bool,
    /// The domain to use for TLS when connecting to the server.
    pub tls_domain: String,
    /// The path to the CA file for TLS.
    pub tls_ca_file: Option<String>,
    /// Whether to automatically login user after establishing connection.
    pub auto_login: AutoLogin,
    /// Whether to automatically reconnect when disconnected.
    pub reconnection: TcpClientReconnectionConfig,
    /// Interval of heartbeats sent by the client
    pub heartbeat_interval: IggyDuration,
    /// Disable Nagle algorithm for the TCP socket.
    pub nodelay: bool,
}

impl Default for TcpClientConfig {
    fn default() -> TcpClientConfig {
        TcpClientConfig {
            server_address: "127.0.0.1:8090".to_string(),
            tls_enabled: false,
            tls_domain: "localhost".to_string(),
            tls_ca_file: None,
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
            auto_login: AutoLogin::Disabled,
            reconnection: TcpClientReconnectionConfig::default(),
            nodelay: false,
        }
    }
}

impl From<ConnectionString<TcpConnectionStringOptions>> for TcpClientConfig {
    fn from(connection_string: ConnectionString<TcpConnectionStringOptions>) -> Self {
        TcpClientConfig {
            server_address: connection_string.server_address().into(),
            auto_login: connection_string.auto_login().to_owned(),
            tls_enabled: connection_string.options().tls_enabled(),
            tls_domain: connection_string.options().tls_domain().into(),
            tls_ca_file: connection_string.options().tls_ca_file().to_owned(),
            reconnection: connection_string.options().reconnection().to_owned(),
            heartbeat_interval: connection_string.options().heartbeat_interval(),
            nodelay: connection_string.options().nodelay(),
        }
    }
}
