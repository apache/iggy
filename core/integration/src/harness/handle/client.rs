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

use crate::harness::config::ClientConfig;
use crate::harness::error::TestBinaryError;
use iggy::http::http_client::HttpClient;
use iggy::prelude::{
    Client, ClientWrapper, HttpClientConfig, QuicClientConfig, TcpClient, TcpClientConfig,
    UserClient, WebSocketClientConfig,
};
use iggy::quic::quic_client::QuicClient;
use iggy::websocket::websocket_client::WebSocketClient;
use iggy_common::TransportProtocol;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

pub struct ClientHandle {
    config: ClientConfig,
    address: SocketAddr,
    client: Option<ClientWrapper>,
}

impl std::fmt::Debug for ClientHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientHandle")
            .field("transport", &self.config.transport)
            .field("address", &self.address)
            .field("connected", &self.client.is_some())
            .finish()
    }
}

impl ClientHandle {
    pub fn new(config: ClientConfig, address: SocketAddr) -> Self {
        Self {
            config,
            address,
            client: None,
        }
    }

    pub async fn connect(&mut self) -> Result<(), TestBinaryError> {
        let client = self.create_client().await?;

        if let Some(ref login) = self.config.auto_login {
            let _: iggy::prelude::IdentityInfo = client
                .login_user(&login.username, &login.password)
                .await
                .map_err(|e| TestBinaryError::ClientConnection {
                    transport: format!("{:?}", self.config.transport),
                    address: self.address.to_string(),
                    source: format!("Auto-login failed: {}", e),
                })?;
        }

        self.client = Some(client);
        Ok(())
    }

    pub async fn reconnect(&mut self) -> Result<(), TestBinaryError> {
        self.disconnect().await;
        self.connect().await
    }

    pub async fn disconnect(&mut self) {
        if let Some(ref client) = self.client {
            let _ = client.disconnect().await;
        }
        self.client = None;
    }

    pub fn update_address(&mut self, address: SocketAddr) {
        self.address = address;
    }

    pub fn transport(&self) -> TransportProtocol {
        self.config.transport
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn inner(&self) -> Option<&ClientWrapper> {
        self.client.as_ref()
    }

    async fn create_client(&self) -> Result<ClientWrapper, TestBinaryError> {
        match self.config.transport {
            TransportProtocol::Tcp => self.create_tcp_client().await,
            TransportProtocol::Quic => self.create_quic_client().await,
            TransportProtocol::Http => self.create_http_client().await,
            TransportProtocol::WebSocket => self.create_websocket_client().await,
        }
    }

    async fn create_tcp_client(&self) -> Result<ClientWrapper, TestBinaryError> {
        let config = TcpClientConfig {
            server_address: self.address.to_string(),
            nodelay: self.config.tcp_nodelay,
            tls_enabled: self.config.tls_enabled,
            tls_domain: self.config.tls_domain.clone().unwrap_or_default(),
            tls_ca_file: self
                .config
                .tls_ca_file
                .as_ref()
                .map(|p| p.display().to_string()),
            tls_validate_certificate: self.config.tls_validate_certificate,
            ..TcpClientConfig::default()
        };

        let client =
            TcpClient::create(Arc::new(config)).map_err(|e| TestBinaryError::ClientCreation {
                transport: "TCP".to_string(),
                address: self.address.to_string(),
                source: e.to_string(),
            })?;

        Client::connect(&client)
            .await
            .map_err(|e| TestBinaryError::ClientConnection {
                transport: "TCP".to_string(),
                address: self.address.to_string(),
                source: e.to_string(),
            })?;

        Ok(ClientWrapper::Tcp(client))
    }

    async fn create_quic_client(&self) -> Result<ClientWrapper, TestBinaryError> {
        let config = QuicClientConfig {
            server_address: self.address.to_string(),
            max_idle_timeout: 2_000_000,
            ..QuicClientConfig::default()
        };

        let client =
            QuicClient::create(Arc::new(config)).map_err(|e| TestBinaryError::ClientCreation {
                transport: "QUIC".to_string(),
                address: self.address.to_string(),
                source: e.to_string(),
            })?;

        Client::connect(&client)
            .await
            .map_err(|e| TestBinaryError::ClientConnection {
                transport: "QUIC".to_string(),
                address: self.address.to_string(),
                source: e.to_string(),
            })?;

        Ok(ClientWrapper::Quic(client))
    }

    async fn create_http_client(&self) -> Result<ClientWrapper, TestBinaryError> {
        let config = HttpClientConfig {
            api_url: format!("http://{}", self.address),
            ..HttpClientConfig::default()
        };

        let client =
            HttpClient::create(Arc::new(config)).map_err(|e| TestBinaryError::ClientCreation {
                transport: "HTTP".to_string(),
                address: self.address.to_string(),
                source: e.to_string(),
            })?;

        Ok(ClientWrapper::Http(client))
    }

    async fn create_websocket_client(&self) -> Result<ClientWrapper, TestBinaryError> {
        let config = WebSocketClientConfig {
            server_address: self.address.to_string(),
            tls_enabled: self.config.tls_enabled,
            tls_domain: self.config.tls_domain.clone().unwrap_or_default(),
            tls_ca_file: self
                .config
                .tls_ca_file
                .as_ref()
                .map(|p| p.display().to_string()),
            tls_validate_certificate: self.config.tls_validate_certificate,
            ..WebSocketClientConfig::default()
        };

        let client = WebSocketClient::create(Arc::new(config)).map_err(|e| {
            TestBinaryError::ClientCreation {
                transport: "WebSocket".to_string(),
                address: self.address.to_string(),
                source: e.to_string(),
            }
        })?;

        Client::connect(&client)
            .await
            .map_err(|e| TestBinaryError::ClientConnection {
                transport: "WebSocket".to_string(),
                address: self.address.to_string(),
                source: e.to_string(),
            })?;

        Ok(ClientWrapper::WebSocket(client))
    }
}

impl Deref for ClientHandle {
    type Target = ClientWrapper;

    fn deref(&self) -> &Self::Target {
        self.client
            .as_ref()
            .expect("Client not connected. Call connect() first.")
    }
}
