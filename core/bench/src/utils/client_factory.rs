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

use crate::args::common::IggyBenchArgs;
use crate::args::transport::BenchmarkTransportCommand;
use async_trait::async_trait;
use iggy::http::http_client::HttpClient;
use iggy::prelude::{
    Client, ClientWrapper, HttpClientConfig, IdentityInfo, IggyClient, QuicClientConfig, TcpClient,
    TcpClientConfig, TransportProtocol, UserClient, WebSocketClientConfig,
};
use iggy::quic::quic_client::QuicClient;
use iggy::websocket::websocket_client::WebSocketClient;
use std::sync::Arc;

#[async_trait]
pub trait ClientFactory: Sync + Send {
    async fn create_client(&self) -> ClientWrapper;
    fn transport(&self) -> TransportProtocol;
    fn server_addr(&self) -> String;
    fn username(&self) -> &str;
    fn password(&self) -> &str;
}

pub async fn authenticate(client: &IggyClient, username: &str, password: &str) -> IdentityInfo {
    client.login_user(username, password).await.unwrap()
}

#[derive(Debug, Clone)]
pub struct HttpClientFactory {
    pub server_addr: String,
    pub username: String,
    pub password: String,
}

#[async_trait]
impl ClientFactory for HttpClientFactory {
    async fn create_client(&self) -> ClientWrapper {
        let config = HttpClientConfig {
            api_url: format!("http://{}", self.server_addr.clone()),
            ..HttpClientConfig::default()
        };
        let client = HttpClient::create(Arc::new(config)).unwrap();
        ClientWrapper::Http(client)
    }

    fn transport(&self) -> TransportProtocol {
        TransportProtocol::Http
    }

    fn server_addr(&self) -> String {
        self.server_addr.clone()
    }

    fn username(&self) -> &str {
        &self.username
    }

    fn password(&self) -> &str {
        &self.password
    }
}

#[derive(Debug, Clone, Default)]
pub struct TcpClientFactory {
    pub server_addr: String,
    pub nodelay: bool,
    pub tls_enabled: bool,
    pub tls_domain: String,
    pub tls_ca_file: Option<String>,
    pub tls_validate_certificate: bool,
    pub username: String,
    pub password: String,
}

#[async_trait]
impl ClientFactory for TcpClientFactory {
    async fn create_client(&self) -> ClientWrapper {
        let config = TcpClientConfig {
            server_address: self.server_addr.clone(),
            nodelay: self.nodelay,
            tls_enabled: self.tls_enabled,
            tls_domain: self.tls_domain.clone(),
            tls_ca_file: self.tls_ca_file.clone(),
            tls_validate_certificate: self.tls_validate_certificate,
            ..TcpClientConfig::default()
        };
        let client = TcpClient::create(Arc::new(config)).unwrap_or_else(|e| {
            panic!(
                "Failed to create TcpClient, iggy-server has address {}, error: {:?}",
                self.server_addr, e
            )
        });
        Client::connect(&client).await.unwrap_or_else(|e| {
            if self.tls_enabled {
                panic!(
                    "Failed to connect to iggy-server at {} with TLS enabled, error: {:?}\n\
                    Hint: Make sure the server is started with TLS enabled and self-signed certificate:\n\
                    IGGY_TCP_TLS_ENABLED=true IGGY_TCP_TLS_SELF_SIGNED=true\n
                    or start iggy-bench with relevant tcp tls arguments: --tls --tls-domain <domain> --tls-ca-file <ca_file>\n",
                    self.server_addr, e
                )
            } else {
                panic!(
                    "Failed to connect to iggy-server at {}, error: {:?}",
                    self.server_addr, e
                )
            }
        });
        ClientWrapper::Tcp(client)
    }

    fn transport(&self) -> TransportProtocol {
        TransportProtocol::Tcp
    }

    fn server_addr(&self) -> String {
        self.server_addr.clone()
    }

    fn username(&self) -> &str {
        &self.username
    }

    fn password(&self) -> &str {
        &self.password
    }
}

#[derive(Debug, Clone)]
pub struct QuicClientFactory {
    pub server_addr: String,
    pub username: String,
    pub password: String,
}

#[async_trait]
impl ClientFactory for QuicClientFactory {
    async fn create_client(&self) -> ClientWrapper {
        let config = QuicClientConfig {
            server_address: self.server_addr.clone(),
            max_idle_timeout: 2_000_000,
            ..QuicClientConfig::default()
        };
        let client = QuicClient::create(Arc::new(config)).unwrap();
        Client::connect(&client).await.unwrap();
        ClientWrapper::Quic(client)
    }

    fn transport(&self) -> TransportProtocol {
        TransportProtocol::Quic
    }

    fn server_addr(&self) -> String {
        self.server_addr.clone()
    }

    fn username(&self) -> &str {
        &self.username
    }

    fn password(&self) -> &str {
        &self.password
    }
}

#[derive(Debug, Clone)]
pub struct WebSocketClientFactory {
    pub server_addr: String,
    pub username: String,
    pub password: String,
}

#[async_trait]
impl ClientFactory for WebSocketClientFactory {
    async fn create_client(&self) -> ClientWrapper {
        let config = WebSocketClientConfig {
            server_address: self.server_addr.clone(),
            ..WebSocketClientConfig::default()
        };
        let client = WebSocketClient::create(Arc::new(config)).unwrap();
        Client::connect(&client).await.unwrap();
        ClientWrapper::WebSocket(client)
    }

    fn transport(&self) -> TransportProtocol {
        TransportProtocol::WebSocket
    }

    fn server_addr(&self) -> String {
        self.server_addr.clone()
    }

    fn username(&self) -> &str {
        &self.username
    }

    fn password(&self) -> &str {
        &self.password
    }
}

pub fn create_client_factory(args: &IggyBenchArgs) -> Arc<dyn ClientFactory> {
    let username = args.username().to_owned();
    let password = args.password().to_owned();

    match &args.transport() {
        TransportProtocol::Http => Arc::new(HttpClientFactory {
            server_addr: args.server_address().to_owned(),
            username,
            password,
        }),
        TransportProtocol::Tcp => {
            let transport_command = args.transport_command();
            if let BenchmarkTransportCommand::Tcp(tcp_args) = transport_command {
                Arc::new(TcpClientFactory {
                    server_addr: args.server_address().to_owned(),
                    nodelay: args.nodelay(),
                    tls_enabled: tcp_args.tls,
                    tls_domain: tcp_args.tls_domain.clone(),
                    tls_ca_file: tcp_args.tls_ca_file.clone(),
                    tls_validate_certificate: tcp_args.tls_validate_certificate,
                    username,
                    password,
                })
            } else {
                unreachable!("Transport is TCP but transport command is not TcpArgs")
            }
        }
        TransportProtocol::Quic => Arc::new(QuicClientFactory {
            server_addr: args.server_address().to_owned(),
            username,
            password,
        }),
        TransportProtocol::WebSocket => Arc::new(WebSocketClientFactory {
            server_addr: args.server_address().to_owned(),
            username,
            password,
        }),
    }
}
