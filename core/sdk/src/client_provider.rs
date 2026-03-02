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

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::client::IggyClient;
#[cfg(feature = "http")]
use crate::http::http_client::HttpClient;
use crate::prelude::ClientError;
#[cfg(feature = "http")]
use crate::prelude::HttpClientConfig;
#[cfg(any(feature = "quic", feature = "tcp", feature = "websocket"))]
use crate::prelude::IggyDuration;
#[cfg(feature = "websocket")]
use crate::prelude::WebSocketClient;
#[cfg(feature = "quic")]
use crate::prelude::{QuicClientConfig, QuicClientReconnectionConfig};
#[cfg(feature = "tcp")]
use crate::prelude::{TcpClientConfig, TcpClientReconnectionConfig};
#[cfg(feature = "quic")]
use crate::quic::quic_client::QuicClient;
#[cfg(feature = "tcp")]
use crate::tcp::tcp_client::TcpClient;
#[cfg(any(feature = "tcp", feature = "quic", feature = "websocket"))]
use iggy_binary_protocol::Client;
#[cfg(any(feature = "quic", feature = "tcp", feature = "websocket"))]
use iggy_common::AutoLogin;
#[cfg(any(feature = "quic", feature = "tcp", feature = "websocket"))]
use iggy_common::Credentials;
use iggy_common::TransportProtocol;
#[cfg(feature = "websocket")]
use iggy_common::{WebSocketClientConfig, WebSocketClientReconnectionConfig, WebSocketConfig};
use std::str::FromStr;
use std::sync::Arc;

/// Configuration for the `ClientProvider`.
/// It consists of the following fields:
/// - `transport`: the transport protocol to use (TCP, QUIC, or HTTP).
/// - `http`: the optional configuration for the HTTP transport.
/// - `quic`: the optional configuration for the QUIC transport.
/// - `tcp`: the optional configuration for the TCP transport.
#[derive(Debug)]
pub struct ClientProviderConfig {
    /// The transport protocol to use.
    pub transport: TransportProtocol,
    /// The optional configuration for the HTTP transport.
    #[cfg(feature = "http")]
    pub http: Option<Arc<HttpClientConfig>>,
    /// The optional configuration for the QUIC transport.
    #[cfg(feature = "quic")]
    pub quic: Option<Arc<QuicClientConfig>>,
    /// The optional configuration for the TCP transport.
    #[cfg(feature = "tcp")]
    pub tcp: Option<Arc<TcpClientConfig>>,
    /// The optional configuration for the WebSocket transport.
    #[cfg(feature = "websocket")]
    pub websocket: Option<Arc<WebSocketClientConfig>>,
}

impl Default for ClientProviderConfig {
    fn default() -> ClientProviderConfig {
        ClientProviderConfig {
            #[cfg(feature = "tcp")]
            transport: TransportProtocol::Tcp,
            #[cfg(all(not(feature = "tcp"), feature = "quic"))]
            transport: TransportProtocol::Quic,
            #[cfg(all(not(feature = "tcp"), not(feature = "quic"), feature = "http"))]
            transport: TransportProtocol::Http,
            #[cfg(all(
                not(feature = "tcp"),
                not(feature = "quic"),
                not(feature = "http"),
                feature = "websocket"
            ))]
            transport: TransportProtocol::WebSocket,
            #[cfg(feature = "http")]
            http: Some(Arc::new(HttpClientConfig::default())),
            #[cfg(feature = "quic")]
            quic: Some(Arc::new(QuicClientConfig::default())),
            #[cfg(feature = "tcp")]
            tcp: Some(Arc::new(TcpClientConfig::default())),
            #[cfg(feature = "websocket")]
            websocket: Some(Arc::new(WebSocketClientConfig::default())),
        }
    }
}

impl ClientProviderConfig {
    /// Create a new `ClientProviderConfig` from the provided `Args`.
    pub fn from_args(args: crate::prelude::Args) -> Result<Self, ClientError> {
        Self::from_args_set_autologin(args, true)
    }

    /// Create a new `ClientProviderConfig` from the provided `Args` with possibility to enable or disable
    /// auto login option for TCP or QUIC protocols.
    pub fn from_args_set_autologin(
        args: crate::prelude::Args,
        auto_login: bool,
    ) -> Result<Self, ClientError> {
        let _ = &auto_login;
        let transport = TransportProtocol::from_str(&args.transport)
            .map_err(|_| ClientError::InvalidTransport(args.transport.clone()))?;
        let mut config = Self {
            transport,
            #[cfg(feature = "http")]
            http: None,
            #[cfg(feature = "quic")]
            quic: None,
            #[cfg(feature = "tcp")]
            tcp: None,
            #[cfg(feature = "websocket")]
            websocket: None,
        };
        match config.transport {
            #[cfg(feature = "quic")]
            TransportProtocol::Quic => {
                config.quic = Some(Arc::new(QuicClientConfig {
                    client_address: args.quic_client_address,
                    server_address: args.quic_server_address,
                    server_name: args.quic_server_name,
                    heartbeat_interval: IggyDuration::from_str(&args.quic_heartbeat_interval)
                        .unwrap(),
                    reconnection: QuicClientReconnectionConfig {
                        enabled: args.quic_reconnection_enabled,
                        max_retries: args.quic_reconnection_max_retries,
                        interval: IggyDuration::from_str(&args.quic_reconnection_interval).unwrap(),
                        reestablish_after: IggyDuration::from_str(
                            &args.quic_reconnection_reestablish_after,
                        )
                        .unwrap(),
                    },
                    auto_login: if auto_login {
                        AutoLogin::Enabled(Credentials::UsernamePassword(
                            args.username,
                            args.password,
                        ))
                    } else {
                        AutoLogin::Disabled
                    },
                    response_buffer_size: args.quic_response_buffer_size,
                    max_concurrent_bidi_streams: args.quic_max_concurrent_bidi_streams,
                    datagram_send_buffer_size: args.quic_datagram_send_buffer_size,
                    initial_mtu: args.quic_initial_mtu,
                    send_window: args.quic_send_window,
                    receive_window: args.quic_receive_window,
                    keep_alive_interval: args.quic_keep_alive_interval,
                    max_idle_timeout: args.quic_max_idle_timeout,
                    validate_certificate: args.quic_validate_certificate,
                }));
            }
            #[cfg(feature = "http")]
            TransportProtocol::Http => {
                config.http = Some(Arc::new(HttpClientConfig {
                    api_url: args.http_api_url,
                    retries: args.http_retries,
                }));
            }
            #[cfg(feature = "tcp")]
            TransportProtocol::Tcp => {
                config.tcp = Some(Arc::new(TcpClientConfig {
                    server_address: args.tcp_server_address,
                    tls_enabled: args.tcp_tls_enabled,
                    tls_domain: args.tcp_tls_domain,
                    tls_ca_file: args.tcp_tls_ca_file,
                    tls_validate_certificate: true,
                    nodelay: args.tcp_nodelay,
                    heartbeat_interval: IggyDuration::from_str(&args.tcp_heartbeat_interval)
                        .unwrap(),
                    reconnection: TcpClientReconnectionConfig {
                        enabled: args.tcp_reconnection_enabled,
                        max_retries: args.tcp_reconnection_max_retries,
                        interval: IggyDuration::from_str(&args.tcp_reconnection_interval).unwrap(),
                        reestablish_after: IggyDuration::from_str(
                            &args.tcp_reconnection_reestablish_after,
                        )
                        .unwrap(),
                    },
                    auto_login: if auto_login {
                        AutoLogin::Enabled(Credentials::UsernamePassword(
                            args.username,
                            args.password,
                        ))
                    } else {
                        AutoLogin::Disabled
                    },
                }));
            }
            #[cfg(feature = "websocket")]
            TransportProtocol::WebSocket => {
                config.websocket = Some(Arc::new(WebSocketClientConfig {
                    server_address: args.websocket_server_address,
                    heartbeat_interval: IggyDuration::from_str(&args.websocket_heartbeat_interval)
                        .unwrap(),
                    reconnection: WebSocketClientReconnectionConfig {
                        enabled: args.websocket_reconnection_enabled,
                        max_retries: args.websocket_reconnection_max_retries,
                        interval: IggyDuration::from_str(&args.websocket_reconnection_interval)
                            .unwrap(),
                        reestablish_after: IggyDuration::from_str(
                            &args.websocket_reconnection_reestablish_after,
                        )
                        .unwrap(),
                    },
                    auto_login: if auto_login {
                        AutoLogin::Enabled(Credentials::UsernamePassword(
                            args.username,
                            args.password,
                        ))
                    } else {
                        AutoLogin::Disabled
                    },
                    ws_config: WebSocketConfig::default(),
                    tls_enabled: args.websocket_tls_enabled,
                    tls_domain: args.websocket_tls_domain,
                    tls_ca_file: args.websocket_tls_ca_file,
                    tls_validate_certificate: args.websocket_tls_validate_certificate,
                }));
            }
            #[allow(unreachable_patterns)]
            _ => return Err(ClientError::InvalidTransport(args.transport)),
        }

        Ok(config)
    }
}

/// Create a default `IggyClient` with the default configuration.
pub async fn get_default_client_() -> Result<IggyClient, ClientError> {
    get_client(Arc::new(ClientProviderConfig::default())).await
}

/// Create a `IggyClient` for the specific transport based on the provided configuration.
pub async fn get_client(config: Arc<ClientProviderConfig>) -> Result<IggyClient, ClientError> {
    let client = get_raw_connected_client(config).await?;
    Ok(IggyClient::builder().with_client(client).build()?)
}

/// Create a `Client` for the specific transport based on the provided configuration.
pub async fn get_raw_connected_client(
    config: Arc<ClientProviderConfig>,
) -> Result<ClientWrapper, ClientError> {
    get_raw_client(config, true).await
}

/// Create a `Client` for the specific transport based on the provided configuration.
pub async fn get_raw_client(
    config: Arc<ClientProviderConfig>,
    establish_connection: bool,
) -> Result<ClientWrapper, ClientError> {
    let _ = &establish_connection;
    match config.transport {
        #[cfg(feature = "quic")]
        TransportProtocol::Quic => {
            let quic_config = config.quic.as_ref().unwrap();
            let client = QuicClient::create(quic_config.clone())?;
            if establish_connection {
                Client::connect(&client).await?
            };
            Ok(ClientWrapper::Quic(client))
        }
        #[cfg(feature = "http")]
        TransportProtocol::Http => {
            let http_config = config.http.as_ref().unwrap();
            let client = HttpClient::create(http_config.clone())?;
            Ok(ClientWrapper::Http(client))
        }
        #[cfg(feature = "tcp")]
        TransportProtocol::Tcp => {
            let tcp_config = config.tcp.as_ref().unwrap();
            let client = TcpClient::create(tcp_config.clone())?;
            if establish_connection {
                Client::connect(&client).await?
            };
            Ok(ClientWrapper::Tcp(client))
        }
        #[cfg(feature = "websocket")]
        TransportProtocol::WebSocket => {
            let websocket_config = config.websocket.as_ref().unwrap();
            let client = WebSocketClient::create(websocket_config.clone())?;
            if establish_connection {
                Client::connect(&client).await?
            };
            Ok(ClientWrapper::WebSocket(client))
        }
        #[allow(unreachable_patterns)]
        _ => Err(ClientError::InvalidTransport(config.transport.to_string())),
    }
}
