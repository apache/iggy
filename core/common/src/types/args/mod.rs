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

use crate::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use clap::Parser;
use serde::{Deserialize, Serialize};

/// The arguments used by the `ClientProviderConfig` to create a client.
/// We are not using default values here because we want to be able to
/// distinguish between the default value and the value that was not
/// provided by the user. This allows us to fallback to values provided
/// in contexts.toml.
#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
#[command(author, version, about, long_about = None)]
pub struct ArgsOptional {
    /// The transport to use. Valid values are `quic`, `http`, `tcp` and `ws`
    ///
    /// [default: tcp]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,

    /// Optional encryption key for the message payload used by the client
    ///
    /// (default: none)
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_key: Option<String>,

    /// Optional username for initial login
    ///
    /// [default: DEFAULT_ROOT_USERNAME]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_username: Option<String>,

    /// Optional password for initial login
    ///
    /// [default: DEFAULT_ROOT_PASSWORD]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_password: Option<String>,

    /// The optional API URL for the HTTP transport
    ///
    /// [default: http://localhost:3000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_api_url: Option<String>,

    /// The optional number of retries for the HTTP transport
    ///
    /// [default: 3]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_retries: Option<u32>,

    /// The optional client address for the TCP transport
    ///
    /// [default: 127.0.0.1:8090]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_server_address: Option<String>,

    /// The optional number of max reconnect retries for the TCP transport
    ///
    /// [default: 3]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_reconnection_max_retries: Option<u32>,

    /// The optional reconnect interval for the TCP transport
    ///
    /// [default: "1s"]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_reconnection_interval: Option<String>,

    /// Flag to enable TLS for the TCP transport
    #[arg(long, default_missing_value(Some("true")), num_args(0..1))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_tls_enabled: Option<bool>,

    /// The optional TLS domain for the TCP transport
    ///
    /// [default: localhost]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_tls_domain: Option<String>,

    /// The optional client address for the QUIC transport
    ///
    /// [default: 127.0.0.1:0]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_client_address: Option<String>,

    /// The optional server address for the QUIC transport
    ///
    /// [default: 127.0.0.1:8080]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_server_address: Option<String>,

    /// The optional server name for the QUIC transport
    ///
    /// [default: localhost]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_server_name: Option<String>,

    /// The optional number of max reconnect retries for the QUIC transport
    ///
    /// [default: 3]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_reconnection_max_retries: Option<u32>,

    /// The optional reconnect interval for the QUIC transport
    ///
    /// [default: "1s"]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_reconnection_interval: Option<String>,

    /// The optional maximum number of concurrent bidirectional streams for QUIC
    ///
    /// [default: 10000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_max_concurrent_bidi_streams: Option<u64>,

    /// The optional datagram send buffer size for QUIC
    ///
    /// [default: 100000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_datagram_send_buffer_size: Option<u64>,

    /// The optional initial MTU for QUIC
    ///
    /// [default: 1200]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_initial_mtu: Option<u16>,

    /// The optional send window for QUIC
    ///
    /// [default: 100000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_send_window: Option<u64>,

    /// The optional receive window for QUIC
    ///
    /// [default: 100000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_receive_window: Option<u64>,

    /// The optional response buffer size for QUIC
    ///
    /// [default: 1048576]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_response_buffer_size: Option<u64>,

    /// The optional keep alive interval for QUIC
    ///
    /// [default: 5000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_keep_alive_interval: Option<u64>,

    /// The optional maximum idle timeout for QUIC
    ///
    /// [default: 10000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_max_idle_timeout: Option<u64>,

    /// Flag to enable certificate validation for QUIC
    #[arg(long, default_missing_value(Some("true")), num_args(0..1))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic_validate_certificate: Option<bool>,

    /// The optional server address for the WebSocket transport
    ///
    /// [default: 127.0.0.1:8092]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub websocket_server_address: Option<String>,

    /// The optional number of max reconnect retries for the WebSocket transport
    ///
    /// [default: 3]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub websocket_reconnection_max_retries: Option<u32>,

    /// The optional reconnect interval for the WebSocket transport
    ///
    /// [default: "1s"]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub websocket_reconnection_interval: Option<String>,
}

/// The arguments used by the `ClientProviderConfig` to create a client.
#[derive(Debug, Clone)]
pub struct Args {
    /// The transport to use. Valid values are `quic`, `http`, `tcp` and `ws`
    pub transport: String,

    /// Optional encryption key for the message payload used by the client
    pub encryption_key: String,

    /// The optional API URL for the HTTP transport
    pub http_api_url: String,

    /// The optional number of retries for the HTTP transport
    pub http_retries: u32,

    // The optional username for initial login
    pub username: String,

    // The optional password for initial login
    pub password: String,

    /// The optional client address for the TCP transport
    pub tcp_server_address: String,

    /// The optional number of maximum reconnect retries for the TCP transport
    pub tcp_reconnection_enabled: bool,

    /// The optional number of maximum reconnect retries for the TCP transport
    pub tcp_reconnection_max_retries: Option<u32>,

    /// The optional reconnect interval for the TCP transport
    pub tcp_reconnection_interval: String,

    /// The optional re-establish after last connection interval for TCP
    pub tcp_reconnection_reestablish_after: String,

    /// The optional heartbeat interval for the TCP transport
    pub tcp_heartbeat_interval: String,

    /// Flag to enable TLS for the TCP transport
    pub tcp_tls_enabled: bool,

    /// The optional TLS domain for the TCP transport
    pub tcp_tls_domain: String,

    /// The optional CA file for the TCP transport
    pub tcp_tls_ca_file: Option<String>,

    /// Disable nodelay for the TCP transport
    pub tcp_nodelay: bool,

    /// The optional client address for the QUIC transport
    pub quic_client_address: String,

    /// The optional server address for the QUIC transport
    pub quic_server_address: String,

    /// The optional server name for the QUIC transport
    pub quic_server_name: String,

    /// The optional number of maximum reconnect retries for the QUIC transport
    pub quic_reconnection_enabled: bool,

    /// The optional number of maximum reconnect retries for the QUIC transport
    pub quic_reconnection_max_retries: Option<u32>,

    /// The optional reconnect interval for the QUIC transport
    pub quic_reconnection_interval: String,

    /// The optional re-establish after last connection interval for QUIC
    pub quic_reconnection_reestablish_after: String,

    /// The optional maximum number of concurrent bidirectional streams for QUIC
    pub quic_max_concurrent_bidi_streams: u64,

    /// The optional datagram send buffer size for QUIC
    pub quic_datagram_send_buffer_size: u64,

    /// The optional initial MTU for QUIC
    pub quic_initial_mtu: u16,

    /// The optional send window for QUIC
    pub quic_send_window: u64,

    /// The optional receive window for QUIC
    pub quic_receive_window: u64,

    /// The optional response buffer size for QUIC
    pub quic_response_buffer_size: u64,

    /// The optional keep alive interval for QUIC
    pub quic_keep_alive_interval: u64,

    /// The optional maximum idle timeout for QUIC
    pub quic_max_idle_timeout: u64,

    /// Flag to enable certificate validation for QUIC
    pub quic_validate_certificate: bool,

    /// The optional heartbeat interval for the QUIC transport
    pub quic_heartbeat_interval: String,

    /// The optional server address for the WebSocket transport
    pub websocket_server_address: String,

    /// The optional number of maximum reconnect retries for the WebSocket transport
    pub websocket_reconnection_enabled: bool,

    /// The optional number of maximum reconnect retries for the WebSocket transport
    pub websocket_reconnection_max_retries: Option<u32>,

    /// The optional reconnect interval for the WebSocket transport
    pub websocket_reconnection_interval: String,

    /// The optional re-establish after last connection interval for WebSocket
    pub websocket_reconnection_reestablish_after: String,

    /// The optional heartbeat interval for the WebSocket transport
    pub websocket_heartbeat_interval: String,

    /// The optional TLS enabled for the WebSocket transport
    pub websocket_tls_enabled: bool,

    /// The optional TLS domain for the WebSocket transport
    pub websocket_tls_domain: String,

    /// The optional TLS CA file for the WebSocket transport
    pub websocket_tls_ca_file: Option<String>,

    /// The optional TLS validate certificate for the WebSocket transport
    pub websocket_tls_validate_certificate: bool,
}

const QUIC_TRANSPORT: &str = "quic";
const HTTP_TRANSPORT: &str = "http";
const TCP_TRANSPORT: &str = "tcp";
const WEBSOCKET_TRANSPORT: &str = "websocket";

impl Args {
    pub fn get_server_address(&self) -> Option<String> {
        match self.transport.as_str() {
            QUIC_TRANSPORT => Some(self.quic_server_address.replace("localhost", "127.0.0.1")),
            HTTP_TRANSPORT => Some(
                self.http_api_url
                    .clone()
                    .replace("http://", "")
                    .replace("localhost", "127.0.0.1"),
            ),
            TCP_TRANSPORT => Some(self.tcp_server_address.replace("localhost", "127.0.0.1")),
            WEBSOCKET_TRANSPORT => Some(
                self.websocket_server_address
                    .replace("localhost", "127.0.0.1"),
            ),
            _ => None,
        }
    }
}

impl Default for Args {
    fn default() -> Self {
        Self {
            transport: "tcp".to_string(),
            encryption_key: "".to_string(),
            http_api_url: "http://localhost:3000".to_string(),
            http_retries: 3,
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
            tcp_server_address: "127.0.0.1:8090".to_string(),
            tcp_reconnection_enabled: true,
            tcp_reconnection_max_retries: None,
            tcp_reconnection_interval: "1s".to_string(),
            tcp_reconnection_reestablish_after: "5s".to_string(),
            tcp_heartbeat_interval: "5s".to_string(),
            tcp_tls_enabled: false,
            tcp_tls_domain: "localhost".to_string(),
            tcp_tls_ca_file: None,
            tcp_nodelay: false,
            quic_client_address: "127.0.0.1:0".to_string(),
            quic_server_address: "127.0.0.1:8080".to_string(),
            quic_server_name: "localhost".to_string(),
            quic_reconnection_enabled: true,
            quic_reconnection_max_retries: None,
            quic_reconnection_interval: "1s".to_string(),
            quic_reconnection_reestablish_after: "5s".to_string(),
            quic_max_concurrent_bidi_streams: 10000,
            quic_datagram_send_buffer_size: 100000,
            quic_initial_mtu: 1200,
            quic_send_window: 1000000,
            quic_receive_window: 1000000,
            quic_response_buffer_size: 1048576,
            quic_keep_alive_interval: 5000,
            quic_max_idle_timeout: 100000,
            quic_validate_certificate: false,
            quic_heartbeat_interval: "5s".to_string(),
            websocket_server_address: "127.0.0.1:8092".to_string(),
            websocket_reconnection_enabled: true,
            websocket_reconnection_max_retries: None,
            websocket_reconnection_interval: "1s".to_string(),
            websocket_reconnection_reestablish_after: "5s".to_string(),
            websocket_heartbeat_interval: "5s".to_string(),
            websocket_tls_enabled: false,
            websocket_tls_domain: "localhost".to_string(),
            websocket_tls_ca_file: None,
            websocket_tls_validate_certificate: false,
        }
    }
}

impl From<Vec<ArgsOptional>> for Args {
    fn from(args_set: Vec<ArgsOptional>) -> Self {
        let mut args = Args::default();

        for optional_args in args_set {
            if let Some(transport) = optional_args.transport {
                args.transport = transport;
            }
            if let Some(encryption_key) = optional_args.encryption_key {
                args.encryption_key = encryption_key;
            }
            if let Some(username) = optional_args.credentials_username {
                args.username = username;
            }
            if let Some(password) = optional_args.credentials_password {
                args.password = password;
            }
            if let Some(http_api_url) = optional_args.http_api_url {
                args.http_api_url = http_api_url;
            }
            if let Some(http_retries) = optional_args.http_retries {
                args.http_retries = http_retries;
            }
            if let Some(tcp_server_address) = optional_args.tcp_server_address {
                args.tcp_server_address = tcp_server_address;
            }
            if let Some(tcp_reconnection_retries) = optional_args.tcp_reconnection_max_retries {
                args.tcp_reconnection_max_retries = Some(tcp_reconnection_retries);
            }
            if let Some(tcp_reconnection_interval) = optional_args.tcp_reconnection_interval {
                args.tcp_reconnection_interval = tcp_reconnection_interval;
            }
            if let Some(tcp_tls_enabled) = optional_args.tcp_tls_enabled {
                args.tcp_tls_enabled = tcp_tls_enabled;
            }
            if let Some(tcp_tls_domain) = optional_args.tcp_tls_domain {
                args.tcp_tls_domain = tcp_tls_domain;
            }
            if let Some(quic_client_address) = optional_args.quic_client_address {
                args.quic_client_address = quic_client_address;
            }
            if let Some(quic_server_address) = optional_args.quic_server_address {
                args.quic_server_address = quic_server_address;
            }
            if let Some(quic_server_name) = optional_args.quic_server_name {
                args.quic_server_name = quic_server_name;
            }
            if let Some(quic_reconnection_retries) = optional_args.quic_reconnection_max_retries {
                args.quic_reconnection_max_retries = Some(quic_reconnection_retries);
            }
            if let Some(quic_reconnection_interval) = optional_args.quic_reconnection_interval {
                args.quic_reconnection_interval = quic_reconnection_interval;
            }
            if let Some(quic_max_concurrent_bidi_streams) =
                optional_args.quic_max_concurrent_bidi_streams
            {
                args.quic_max_concurrent_bidi_streams = quic_max_concurrent_bidi_streams;
            }
            if let Some(quic_datagram_send_buffer_size) =
                optional_args.quic_datagram_send_buffer_size
            {
                args.quic_datagram_send_buffer_size = quic_datagram_send_buffer_size;
            }
            if let Some(quic_initial_mtu) = optional_args.quic_initial_mtu {
                args.quic_initial_mtu = quic_initial_mtu;
            }
            if let Some(quic_send_window) = optional_args.quic_send_window {
                args.quic_send_window = quic_send_window;
            }
            if let Some(quic_receive_window) = optional_args.quic_receive_window {
                args.quic_receive_window = quic_receive_window;
            }
            if let Some(quic_response_buffer_size) = optional_args.quic_response_buffer_size {
                args.quic_response_buffer_size = quic_response_buffer_size;
            }
            if let Some(quic_keep_alive_interval) = optional_args.quic_keep_alive_interval {
                args.quic_keep_alive_interval = quic_keep_alive_interval;
            }
            if let Some(quic_max_idle_timeout) = optional_args.quic_max_idle_timeout {
                args.quic_max_idle_timeout = quic_max_idle_timeout;
            }
            if let Some(quic_validate_certificate) = optional_args.quic_validate_certificate {
                args.quic_validate_certificate = quic_validate_certificate;
            }
            if let Some(websocket_server_address) = optional_args.websocket_server_address {
                args.websocket_server_address = websocket_server_address;
            }
            if let Some(websocket_reconnection_retries) =
                optional_args.websocket_reconnection_max_retries
            {
                args.websocket_reconnection_max_retries = Some(websocket_reconnection_retries);
            }
            if let Some(websocket_reconnection_interval) =
                optional_args.websocket_reconnection_interval
            {
                args.websocket_reconnection_interval = websocket_reconnection_interval;
            }
        }

        args
    }
}
