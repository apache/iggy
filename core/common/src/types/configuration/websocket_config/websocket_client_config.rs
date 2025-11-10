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
use crate::types::configuration::websocket_config::websocket_connection_string_options::WebSocketConnectionStringOptions;
use crate::{AutoLogin, IggyDuration, WebSocketClientReconnectionConfig};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use tungstenite::protocol::WebSocketConfig as TungsteniteConfig;

/// Configuration for the WebSocket client.
#[derive(Debug, Clone)]
pub struct WebSocketClientConfig {
    /// The address of the Iggy server.
    pub server_address: String,
    /// Whether to automatically login user after establishing connection.
    pub auto_login: AutoLogin,
    /// Whether to automatically reconnect when disconnected.
    pub reconnection: WebSocketClientReconnectionConfig,
    /// Interval of heartbeats sent by the client
    pub heartbeat_interval: IggyDuration,
    /// WebSocket-specific configuration.
    pub ws_config: WebSocketConfig,
    /// Whether tls is enabled
    pub tls_enabled: bool,
    /// The domain to use for TLS
    pub tls_domain: String,
    /// The path to the CA file for TLS
    pub tls_ca_file: Option<String>,
    /// Whether to validate the TLS certificate
    pub tls_validate_certificate: bool,
}

/// WebSocket-specific configuration that maps to tungstenite options.
/// Uses same structure as server-side WebSocketConfig but for client.
#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    /// Read buffer size.
    pub read_buffer_size: Option<usize>,
    /// Write buffer size.
    pub write_buffer_size: Option<usize>,
    /// Maximum write buffer size.
    pub max_write_buffer_size: Option<usize>,
    /// Maximum message size.
    pub max_message_size: Option<usize>,
    /// Maximum frame size.
    pub max_frame_size: Option<usize>,
    /// Accept unmasked frames (client should typically keep as false).
    pub accept_unmasked_frames: bool,
}

impl Default for WebSocketClientConfig {
    fn default() -> Self {
        WebSocketClientConfig {
            server_address: "127.0.0.1:8092".to_string(),
            auto_login: AutoLogin::Disabled,
            reconnection: WebSocketClientReconnectionConfig::default(),
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
            ws_config: WebSocketConfig::default(),
            tls_enabled: false,
            tls_domain: "localhost".to_string(),
            tls_ca_file: None,
            tls_validate_certificate: false,
        }
    }
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        // Use tungstenite defaults
        let tungstenite_config = TungsteniteConfig::default();
        WebSocketConfig {
            read_buffer_size: Some(tungstenite_config.read_buffer_size),
            write_buffer_size: Some(tungstenite_config.write_buffer_size),
            max_write_buffer_size: Some(tungstenite_config.max_write_buffer_size),
            max_message_size: tungstenite_config.max_message_size,
            max_frame_size: tungstenite_config.max_frame_size,
            accept_unmasked_frames: false,
        }
    }
}

impl WebSocketConfig {
    /// Convert to tungstenite WebSocketConfig so we can use tungstenite defaults
    pub fn to_tungstenite_config(&self) -> TungsteniteConfig {
        let mut config = TungsteniteConfig::default();

        if let Some(read_buf_size) = self.read_buffer_size {
            config = config.read_buffer_size(read_buf_size);
        }

        if let Some(write_buf_size) = self.write_buffer_size {
            config = config.write_buffer_size(write_buf_size);
        }

        if let Some(max_write_buf_size) = self.max_write_buffer_size {
            config = config.max_write_buffer_size(max_write_buf_size);
        }

        if let Some(max_msg_size) = self.max_message_size {
            config = config.max_message_size(Some(max_msg_size));
        }

        if let Some(max_frame_size) = self.max_frame_size {
            config = config.max_frame_size(Some(max_frame_size));
        }

        config = config.accept_unmasked_frames(self.accept_unmasked_frames);

        config
    }
}

impl From<ConnectionString<WebSocketConnectionStringOptions>> for WebSocketClientConfig {
    fn from(connection_string: ConnectionString<WebSocketConnectionStringOptions>) -> Self {
        let options = connection_string.options();

        let mut ws_config = WebSocketConfig::default();

        // Apply connection string options to WebSocket config
        if let Some(read_buf_size) = options.read_buffer_size() {
            ws_config.read_buffer_size = Some(read_buf_size);
        }
        if let Some(write_buf_size) = options.write_buffer_size() {
            ws_config.write_buffer_size = Some(write_buf_size);
        }
        if let Some(max_write_buf_size) = options.max_write_buffer_size() {
            ws_config.max_write_buffer_size = Some(max_write_buf_size);
        }
        if let Some(max_msg_size) = options.max_message_size() {
            ws_config.max_message_size = Some(max_msg_size);
        }
        if let Some(max_frame_size) = options.max_frame_size() {
            ws_config.max_frame_size = Some(max_frame_size);
        }
        if let Some(accept_unmasked) = options.accept_unmasked_frames() {
            ws_config.accept_unmasked_frames = accept_unmasked;
        }

        WebSocketClientConfig {
            server_address: connection_string.server_address().into(),
            auto_login: connection_string.auto_login().to_owned(),
            reconnection: connection_string.options().reconnection().to_owned(),
            heartbeat_interval: connection_string.options().heartbeat_interval(),
            ws_config,
            tls_enabled: options.tls_enabled(),
            tls_domain: options.tls_domain().into(),
            tls_ca_file: options.tls_ca_file().map(|s| s.to_string()),
            tls_validate_certificate: options.tls_validate_certificate(),
        }
    }
}

impl Display for WebSocketClientConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ server_address: {}, reconnection: {}, heartbeat_interval: {}, ws_config: {} }}",
            self.server_address, self.reconnection, self.heartbeat_interval, self.ws_config
        )
    }
}

impl Display for WebSocketConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ read_buffer_size: {:?}, write_buffer_size: {:?}, max_write_buffer_size: {:?}, max_message_size: {:?}, max_frame_size: {:?}, accept_unmasked_frames: {} }}",
            self.read_buffer_size,
            self.write_buffer_size,
            self.max_write_buffer_size,
            self.max_message_size,
            self.max_frame_size,
            self.accept_unmasked_frames
        )
    }
}
