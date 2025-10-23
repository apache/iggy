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

use crate::{AutoLogin, IggyDuration, IggyError, WebSocketClientConfig};
use std::net::SocketAddr;

/// Builder for the WebSocket client configuration.
/// Allows configuring the WebSocket client with custom settings or using defaults:
/// - `server_address`: Default is "127.0.0.1:8092"
/// - `auto_login`: Default is AutoLogin::Disabled.
/// - `reconnection`: Default is enabled unlimited retries and 1 second interval.
/// - `heartbeat_interval`: Default is 5 seconds.
/// - WebSocket-specific options with sensible defaults from tungstenite.
#[derive(Debug, Default)]
pub struct WebSocketClientConfigBuilder {
    config: WebSocketClientConfig,
}

impl WebSocketClientConfigBuilder {
    pub fn new() -> Self {
        WebSocketClientConfigBuilder::default()
    }

    /// Sets the server address for the WebSocket client.
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

    /// Sets the time to wait before attempting to reestablish connection.
    pub fn with_reestablish_after(mut self, reestablish_after: IggyDuration) -> Self {
        self.config.reconnection.reestablish_after = reestablish_after;
        self
    }

    /// Sets the heartbeat interval.
    pub fn with_heartbeat_interval(mut self, heartbeat_interval: IggyDuration) -> Self {
        self.config.heartbeat_interval = heartbeat_interval;
        self
    }

    /// Sets the read buffer size.
    pub fn with_read_buffer_size(mut self, size: usize) -> Self {
        self.config.ws_config.read_buffer_size = Some(size);
        self
    }

    /// Sets the write buffer size.
    pub fn with_write_buffer_size(mut self, size: usize) -> Self {
        self.config.ws_config.write_buffer_size = Some(size);
        self
    }

    /// Sets the maximum write buffer size.
    pub fn with_max_write_buffer_size(mut self, size: usize) -> Self {
        self.config.ws_config.max_write_buffer_size = Some(size);
        self
    }

    /// Sets the maximum message size.
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.config.ws_config.max_message_size = Some(size);
        self
    }

    /// Sets the maximum frame size.
    pub fn with_max_frame_size(mut self, size: usize) -> Self {
        self.config.ws_config.max_frame_size = Some(size);
        self
    }

    /// Sets whether to accept unmasked frames.
    /// Note: Clients should typically keep this as false for RFC compliance.
    pub fn with_accept_unmasked_frames(mut self, accept: bool) -> Self {
        self.config.ws_config.accept_unmasked_frames = accept;
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

    /// Sets whether to validate the TLS certificate.
    pub fn with_tls_validate_certificate(mut self, tls_validate_certificate: bool) -> Self {
        self.config.tls_validate_certificate = tls_validate_certificate;
        self
    }

    /// Builds the WebSocket client configuration.
    pub fn build(self) -> Result<WebSocketClientConfig, IggyError> {
        let addr = self.config.server_address.trim();

        // Check if it's a valid socket address or host:port format
        if addr.parse::<SocketAddr>().is_err() {
            let (host, port) = addr.rsplit_once(':').unwrap_or((addr, ""));
            if port.is_empty() || port.parse::<u16>().is_err() {
                return Err(IggyError::InvalidIpAddress(
                    host.to_owned(),
                    port.to_owned(),
                ));
            }
        }

        Ok(self.config)
    }
}
