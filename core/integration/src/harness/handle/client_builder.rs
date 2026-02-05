/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

//! Chainable client builder for test harness.
//!
//! Provides a fluent API for creating and configuring clients connected to test servers.
//!
//! # Examples
//!
//! ```ignore
//! // Simple unauthenticated client
//! let client = harness.server().tcp_client().connect().await?;
//!
//! // With root login
//! let client = harness.server().tcp_client().with_root_login().connect().await?;
//!
//! // Custom login
//! let client = harness.server().http_client()
//!     .with_login("user", "pass")
//!     .connect().await?;
//! ```

use crate::harness::config::{AutoLoginConfig, TlsConfig};
use crate::harness::error::TestBinaryError;
use iggy::http::http_client::HttpClient;
use iggy::prelude::{
    Client, HttpClientConfig, IggyClient, QuicClientConfig, TcpClient, TcpClientConfig, UserClient,
    WebSocketClientConfig,
};
use iggy::quic::quic_client::QuicClient;
use iggy::websocket::websocket_client::WebSocketClient;
use iggy_common::TransportProtocol;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

/// Server connection details needed by `ClientBuilder`.
#[derive(Debug, Clone)]
pub struct ServerConnection {
    pub tcp_addr: Option<SocketAddr>,
    pub http_addr: Option<SocketAddr>,
    pub quic_addr: Option<SocketAddr>,
    pub websocket_addr: Option<SocketAddr>,
    pub tls: Option<TlsConfig>,
    pub websocket_tls: Option<TlsConfig>,
    pub tls_ca_cert_path: Option<PathBuf>,
}

/// Chainable client builder.
///
/// Created via `ServerHandle::tcp_client()`, `http_client()`, etc.
/// Configuration is chainable, connection happens on `connect()`.
#[must_use = "ClientBuilder does nothing until .connect() is called"]
pub struct ClientBuilder {
    transport: TransportProtocol,
    connection: ServerConnection,
    auto_login: Option<AutoLoginConfig>,
    tcp_nodelay: bool,
}

impl ClientBuilder {
    pub(crate) fn new(transport: TransportProtocol, connection: ServerConnection) -> Self {
        Self {
            transport,
            connection,
            auto_login: None,
            tcp_nodelay: false,
        }
    }

    /// Enable automatic login as root user after connection.
    pub fn with_root_login(mut self) -> Self {
        self.auto_login = Some(AutoLoginConfig::root());
        self
    }

    /// Enable automatic login with custom credentials after connection.
    pub fn with_login(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auto_login = Some(AutoLoginConfig::new(username, password));
        self
    }

    /// Enable TCP_NODELAY (only affects TCP transport).
    pub fn with_nodelay(mut self) -> Self {
        self.tcp_nodelay = true;
        self
    }

    /// Connect to the server and optionally perform auto-login.
    pub async fn connect(self) -> Result<IggyClient, TestBinaryError> {
        let client = match self.transport {
            TransportProtocol::Tcp => self.create_tcp_client().await?,
            TransportProtocol::Http => self.create_http_client().await?,
            TransportProtocol::Quic => self.create_quic_client().await?,
            TransportProtocol::WebSocket => self.create_websocket_client().await?,
        };

        if let Some(ref login) = self.auto_login {
            client
                .login_user(&login.username, &login.password)
                .await
                .map_err(|e| TestBinaryError::ClientConnection {
                    transport: self.transport.to_string(),
                    address: self.get_address_string(),
                    source: format!("login failed: {e}"),
                })?;
        }

        Ok(client)
    }

    async fn create_tcp_client(&self) -> Result<IggyClient, TestBinaryError> {
        let addr = self
            .connection
            .tcp_addr
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "TCP transport not available".to_string(),
            })?;

        let tls_enabled = self.connection.tls.is_some();
        let tls_validate = self.connection.tls.as_ref().is_some_and(|t| !t.self_signed);

        let config = TcpClientConfig {
            server_address: addr.to_string(),
            nodelay: self.tcp_nodelay,
            tls_enabled,
            tls_domain: "localhost".to_string(),
            tls_ca_file: self
                .connection
                .tls_ca_cert_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string()),
            tls_validate_certificate: tls_validate,
            ..TcpClientConfig::default()
        };

        let client =
            TcpClient::create(Arc::new(config)).map_err(|e| TestBinaryError::ClientCreation {
                transport: "TCP".to_string(),
                address: addr.to_string(),
                source: e.to_string(),
            })?;

        Client::connect(&client)
            .await
            .map_err(|e| TestBinaryError::ClientConnection {
                transport: "TCP".to_string(),
                address: addr.to_string(),
                source: e.to_string(),
            })?;

        Ok(IggyClient::create(
            iggy::prelude::ClientWrapper::Tcp(client),
            None,
            None,
        ))
    }

    async fn create_http_client(&self) -> Result<IggyClient, TestBinaryError> {
        let addr = self
            .connection
            .http_addr
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "HTTP transport not available".to_string(),
            })?;

        let config = HttpClientConfig {
            api_url: format!("http://{}", addr),
            ..HttpClientConfig::default()
        };

        let client =
            HttpClient::create(Arc::new(config)).map_err(|e| TestBinaryError::ClientCreation {
                transport: "HTTP".to_string(),
                address: addr.to_string(),
                source: e.to_string(),
            })?;

        Ok(IggyClient::create(
            iggy::prelude::ClientWrapper::Http(client),
            None,
            None,
        ))
    }

    async fn create_quic_client(&self) -> Result<IggyClient, TestBinaryError> {
        let addr = self
            .connection
            .quic_addr
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "QUIC transport not available".to_string(),
            })?;

        let config = QuicClientConfig {
            server_address: addr.to_string(),
            max_idle_timeout: 2_000_000,
            ..QuicClientConfig::default()
        };

        let client =
            QuicClient::create(Arc::new(config)).map_err(|e| TestBinaryError::ClientCreation {
                transport: "QUIC".to_string(),
                address: addr.to_string(),
                source: e.to_string(),
            })?;

        Client::connect(&client)
            .await
            .map_err(|e| TestBinaryError::ClientConnection {
                transport: "QUIC".to_string(),
                address: addr.to_string(),
                source: e.to_string(),
            })?;

        Ok(IggyClient::create(
            iggy::prelude::ClientWrapper::Quic(client),
            None,
            None,
        ))
    }

    async fn create_websocket_client(&self) -> Result<IggyClient, TestBinaryError> {
        let addr = self
            .connection
            .websocket_addr
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "WebSocket transport not available".to_string(),
            })?;

        let tls_enabled = self.connection.websocket_tls.is_some();
        let tls_validate = self
            .connection
            .websocket_tls
            .as_ref()
            .is_some_and(|t| !t.self_signed);

        let config = WebSocketClientConfig {
            server_address: addr.to_string(),
            tls_enabled,
            tls_domain: "localhost".to_string(),
            tls_ca_file: self
                .connection
                .tls_ca_cert_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string()),
            tls_validate_certificate: tls_validate,
            ..WebSocketClientConfig::default()
        };

        let client = WebSocketClient::create(Arc::new(config)).map_err(|e| {
            TestBinaryError::ClientCreation {
                transport: "WebSocket".to_string(),
                address: addr.to_string(),
                source: e.to_string(),
            }
        })?;

        Client::connect(&client)
            .await
            .map_err(|e| TestBinaryError::ClientConnection {
                transport: "WebSocket".to_string(),
                address: addr.to_string(),
                source: e.to_string(),
            })?;

        Ok(IggyClient::create(
            iggy::prelude::ClientWrapper::WebSocket(client),
            None,
            None,
        ))
    }

    fn get_address_string(&self) -> String {
        match self.transport {
            TransportProtocol::Tcp => self
                .connection
                .tcp_addr
                .map(|a| a.to_string())
                .unwrap_or_default(),
            TransportProtocol::Http => self
                .connection
                .http_addr
                .map(|a| a.to_string())
                .unwrap_or_default(),
            TransportProtocol::Quic => self
                .connection
                .quic_addr
                .map(|a| a.to_string())
                .unwrap_or_default(),
            TransportProtocol::WebSocket => self
                .connection
                .websocket_addr
                .map(|a| a.to_string())
                .unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_connection() -> ServerConnection {
        ServerConnection {
            tcp_addr: Some("127.0.0.1:8080".parse().unwrap()),
            http_addr: Some("127.0.0.1:8081".parse().unwrap()),
            quic_addr: Some("127.0.0.1:8082".parse().unwrap()),
            websocket_addr: Some("127.0.0.1:8083".parse().unwrap()),
            tls: None,
            websocket_tls: None,
            tls_ca_cert_path: None,
        }
    }

    #[test]
    fn test_builder_chainable() {
        let builder = ClientBuilder::new(TransportProtocol::Tcp, dummy_connection())
            .with_root_login()
            .with_nodelay();

        assert!(builder.auto_login.is_some());
        assert!(builder.tcp_nodelay);
    }

    #[test]
    fn test_with_login() {
        let builder = ClientBuilder::new(TransportProtocol::Http, dummy_connection())
            .with_login("user", "pass");

        let login = builder.auto_login.unwrap();
        assert_eq!(login.username, "user");
        assert_eq!(login.password, "pass");
    }
}
