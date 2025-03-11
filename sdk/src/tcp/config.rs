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

use crate::client::AutoLogin;
use crate::utils::duration::IggyDuration;
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

#[derive(Debug, Clone)]
pub struct TcpClientReconnectionConfig {
    pub enabled: bool,
    pub max_retries: Option<u32>,
    pub interval: IggyDuration,
    pub reestablish_after: IggyDuration,
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

impl Default for TcpClientReconnectionConfig {
    fn default() -> TcpClientReconnectionConfig {
        TcpClientReconnectionConfig {
            enabled: true,
            max_retries: None,
            interval: IggyDuration::from_str("1s").unwrap(),
            reestablish_after: IggyDuration::from_str("5s").unwrap(),
        }
    }
}

/// Builder for the TCP client configuration.
/// Allows configuring the TCP client with custom settings or using defaults:
/// - `server_address`: Default is "127.0.0.1:8090"
/// - `auto_login`: Default is AutoLogin::Disabled.
/// - `reconnection`: Default is enabled unlimited retries and 1 second interval.
/// - `tls_enabled`: Default is false.
/// - `tls_domain`: Default is "localhost".
/// - `tls_ca_file`: Default is None.
#[derive(Debug, Default)]
pub struct TcpClientConfigBuilder {
    config: TcpClientConfig,
}

impl TcpClientConfigBuilder {
    pub fn new() -> Self {
        TcpClientConfigBuilder::default()
    }

    /// Sets the server address for the TCP client.
    pub fn with_server_address(mut self, server_address: String) -> Self {
        self.config.server_address = server_address;
        self
    }

    /// Sets the auto sign in during connection.
    pub fn with_auto_sign_in(mut self, auto_sign_in: AutoLogin) -> Self {
        self.config.auto_login = auto_sign_in;
        self
    }

    pub fn with_enabled_reconnection(mut self) -> Self {
        self.config.reconnection.enabled = true;
        self
    }

    /// Sets the number of retries when connecting to the server.
    pub fn with_reconnection_max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.config.reconnection.max_retries = max_retries;
        self
    }

    /// Sets the interval between retries when connecting to the server.
    pub fn with_reconnection_interval(mut self, interval: IggyDuration) -> Self {
        self.config.reconnection.interval = interval;
        self
    }

    /// Sets whether to use TLS when connecting to the server.
    pub fn with_tls_enabled(mut self, tls_enabled: bool) -> Self {
        self.config.tls_enabled = tls_enabled;
        self
    }

    /// Sets the domain to use for TLS when connecting to the server.
    pub fn with_tls_domain(mut self, tls_domain: String) -> Self {
        self.config.tls_domain = tls_domain;
        self
    }

    /// Sets the path to the CA file for TLS.
    pub fn with_tls_ca_file(mut self, tls_ca_file: String) -> Self {
        self.config.tls_ca_file = Some(tls_ca_file);
        self
    }

    /// Sets the nodelay option for the TCP socket.
    pub fn with_no_delay(mut self) -> Self {
        self.config.nodelay = true;
        self
    }

    /// Builds the TCP client configuration.
    pub fn build(self) -> TcpClientConfig {
        self.config
    }
}
