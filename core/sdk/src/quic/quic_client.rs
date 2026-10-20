// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::binary::impl_binary_client;
use crate::driver::OperationDriver;
use crate::links::{Dialer, ReconnectPolicy, ReplicaLinks};
use crate::quic::skip_server_verification::SkipServerVerification;
use crate::session_owner::SessionOwner;
use iggy_common::{
    AutoLogin, ConnectionString, ConnectionStringUtils, QuicConnectionStringOptions,
    TransportProtocol, validate_server_address,
};
use iggy_common::{IggyError, QuicClientConfig};
use quinn::crypto::rustls::QuicClientConfig as QuinnQuicClientConfig;
use quinn::{ClientConfig, IdleTimeout, VarInt};
use rustls::crypto::CryptoProvider;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tracing::{error, warn};

/// QUIC client whose replica links share one session and operation driver.
#[derive(Debug)]
pub struct QuicClient {
    pub(crate) config: Arc<QuicClientConfig>,
    pub(crate) driver: OperationDriver,
}

impl Default for QuicClient {
    fn default() -> Self {
        Self::create(Arc::new(QuicClientConfig::default())).unwrap()
    }
}

impl_binary_client!(QuicClient);

impl QuicClient {
    pub fn new(
        client_address: &str,
        server_address: &str,
        server_name: &str,
        validate_certificate: bool,
        auto_sign_in: AutoLogin,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(QuicClientConfig {
            client_address: client_address.to_string(),
            server_address: server_address.to_string(),
            server_name: server_name.to_string(),
            validate_certificate,
            auto_login: auto_sign_in,
            ..Default::default()
        }))
    }

    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        if ConnectionStringUtils::parse_protocol(connection_string)? != TransportProtocol::Quic {
            return Err(IggyError::InvalidConnectionString);
        }

        Self::create(Arc::new(
            ConnectionString::<QuicConnectionStringOptions>::from_str(connection_string)?.into(),
        ))
    }

    /// Creates the client without spawning tasks or opening sockets.
    pub fn create(config: Arc<QuicClientConfig>) -> Result<Self, IggyError> {
        validate_server_address(&config.server_address)?;
        config
            .client_address
            .parse::<SocketAddr>()
            .map_err(|_| IggyError::InvalidClientAddress)?;
        let client_config = configure(&config)?;
        let session = Arc::new(SessionOwner::with_pending_operations(
            config.auto_login.clone(),
            config.pending_operations,
        ));
        let links = ReplicaLinks::new(
            Dialer::Quic {
                config: Arc::clone(&config),
                endpoint: OnceCell::new(),
                client_config,
            },
            config.server_address.clone(),
            ReconnectPolicy::from(&config.reconnection),
            session,
        );
        Ok(Self {
            config,
            driver: OperationDriver::new(links),
        })
    }
}

fn configure(config: &QuicClientConfig) -> Result<ClientConfig, IggyError> {
    let max_concurrent_bidi_streams = VarInt::try_from(config.max_concurrent_bidi_streams);
    if max_concurrent_bidi_streams.is_err() {
        error!(
            "Invalid 'max_concurrent_bidi_streams': {}",
            config.max_concurrent_bidi_streams
        );
        return Err(IggyError::InvalidConfiguration);
    }

    let receive_window = VarInt::try_from(config.receive_window);
    if receive_window.is_err() {
        error!("Invalid 'receive_window': {}", config.receive_window);
        return Err(IggyError::InvalidConfiguration);
    }

    let mut transport = quinn::TransportConfig::default();
    transport.initial_mtu(config.initial_mtu);
    transport.send_window(config.send_window);
    transport.receive_window(receive_window.unwrap());
    transport.datagram_send_buffer_size(config.datagram_send_buffer_size as usize);
    transport.max_concurrent_bidi_streams(max_concurrent_bidi_streams.unwrap());
    if config.keep_alive_interval > 0 {
        transport.keep_alive_interval(Some(Duration::from_millis(config.keep_alive_interval)));
    }
    if config.max_idle_timeout > 0 {
        let max_idle_timeout =
            IdleTimeout::try_from(Duration::from_millis(config.max_idle_timeout));
        if max_idle_timeout.is_err() {
            error!("Invalid 'max_idle_timeout': {}", config.max_idle_timeout);
            return Err(IggyError::InvalidConfiguration);
        }
        transport.max_idle_timeout(Some(max_idle_timeout.unwrap()));
    }

    if CryptoProvider::get_default().is_none()
        && let Err(e) = rustls::crypto::ring::default_provider().install_default()
    {
        warn!(
            "Failed to install rustls crypto provider. Error: {:?}. This may be normal if another thread installed it first.",
            e
        );
    }
    let mut client_config = match config.validate_certificate {
        true => ClientConfig::try_with_platform_verifier().map_err(|error| {
            error!("Failed to create QUIC client configuration: {error}");
            IggyError::InvalidConfiguration
        })?,
        false => {
            match QuinnQuicClientConfig::try_from(
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(SkipServerVerification::new())
                    .with_no_client_auth(),
            ) {
                Ok(config) => ClientConfig::new(Arc::new(config)),
                Err(error) => {
                    error!("Failed to create QUIC client configuration: {error}");
                    return Err(IggyError::InvalidConfiguration);
                }
            }
        }
    };
    client_config.transport_config(Arc::new(transport));
    Ok(client_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{
        BinaryTransport, Client, ClientState, Credentials, DiagnosticEvent, IggyDuration,
        NonZeroIggyDuration,
    };
    use secrecy::ExposeSecret;

    #[tokio::test]
    async fn given_a_cancelled_shutdown_when_called_again_should_finish_cleanup_once() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let config = quinn::ServerConfig::with_single_cert(
            vec![certified.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der())
                .into(),
        )
        .unwrap();
        let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let client = QuicClient::create(Arc::new(QuicClientConfig {
            server_address: endpoint.local_addr().unwrap().to_string(),
            ..Default::default()
        }))
        .unwrap();
        let (peer, connected) = tokio::join!(
            async { endpoint.accept().await.unwrap().await.unwrap() },
            client.connect()
        );
        connected.unwrap();
        let mut events = client.subscribe_events().await;
        {
            let mut shutdown = Box::pin(client.shutdown());
            assert!(futures::poll!(&mut shutdown).is_pending());
        }

        client.shutdown().await.unwrap();
        client.shutdown().await.unwrap();

        assert_eq!(client.get_state().await, ClientState::Shutdown);
        assert_eq!(
            client.connect().await.unwrap_err(),
            IggyError::ClientShutdown
        );
        assert!(peer.close_reason().is_some());
        let mut shutdowns = 0;
        while let Ok(event) = events.try_recv() {
            if event == DiagnosticEvent::Shutdown {
                shutdowns += 1;
            }
        }
        assert_eq!(shutdowns, 1);
    }

    #[tokio::test]
    async fn should_fail_with_a_zero_heartbeat_interval() {
        let value = "iggy+quic://user:secret@127.0.0.1:1234?heartbeat_interval=none";

        let error = QuicClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

    #[tokio::test]
    async fn should_fail_with_a_zero_reconnection_interval() {
        let value = "iggy+quic://user:secret@127.0.0.1:1234?reconnection_interval=0";

        let error = QuicClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

    #[tokio::test]
    async fn should_fail_with_empty_connection_string() {
        let value = "";
        let quic_client = QuicClient::from_connection_string(value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_without_username() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_without_password() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_without_server_address() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_without_port() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_with_invalid_prefix() {
        let connection_string_prefix = "invalid+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_with_unmatch_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_with_default_prefix() {
        let default_connection_string_prefix = "iggy://";
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{default_connection_string_prefix}{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_with_invalid_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?invalid_option=invalid"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_succeed_without_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_ok());

        let quic_client_config = quic_client.unwrap().config;
        assert_eq!(
            quic_client_config.server_address,
            format!("{server_address}:{port}")
        );
        match &quic_client_config.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(u, p)) => {
                assert_eq!(u, &username.to_string());
                assert_eq!(p.expose_secret(), password);
            }
            other => panic!("expected UsernamePassword auto_login, got {other:?}"),
        }

        assert_eq!(quic_client_config.response_buffer_size, 10_000_000);
        assert_eq!(quic_client_config.max_concurrent_bidi_streams, 10_000);
        assert_eq!(quic_client_config.datagram_send_buffer_size, 100_000);
        assert_eq!(quic_client_config.initial_mtu, 1200);
        assert_eq!(quic_client_config.send_window, 100_000);
        assert_eq!(quic_client_config.receive_window, 100_000);
        assert_eq!(quic_client_config.keep_alive_interval, 5000);
        assert_eq!(quic_client_config.max_idle_timeout, 10_000);
        assert!(!quic_client_config.validate_certificate);
        assert_eq!(
            quic_client_config.heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
        );

        assert!(quic_client_config.reconnection.enabled);
        assert!(quic_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            quic_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            quic_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[tokio::test]
    async fn should_succeed_with_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let initial_mtu = "3000";
        let reconnection_interval = "5s";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?initial_mtu={initial_mtu}&reconnection_interval={reconnection_interval}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_ok());

        let quic_client_config = quic_client.unwrap().config;
        assert_eq!(
            quic_client_config.server_address,
            format!("{server_address}:{port}")
        );
        match &quic_client_config.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(u, p)) => {
                assert_eq!(u, &username.to_string());
                assert_eq!(p.expose_secret(), password);
            }
            other => panic!("expected UsernamePassword auto_login, got {other:?}"),
        }

        assert_eq!(quic_client_config.response_buffer_size, 10_000_000);
        assert_eq!(quic_client_config.max_concurrent_bidi_streams, 10_000);
        assert_eq!(quic_client_config.datagram_send_buffer_size, 100_000);
        assert_eq!(
            quic_client_config.initial_mtu,
            initial_mtu.parse::<u16>().unwrap()
        );
        assert_eq!(quic_client_config.send_window, 100_000);
        assert_eq!(quic_client_config.receive_window, 100_000);
        assert_eq!(quic_client_config.keep_alive_interval, 5000);
        assert_eq!(quic_client_config.max_idle_timeout, 10_000);
        assert!(!quic_client_config.validate_certificate);
        assert_eq!(
            quic_client_config.heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
        );

        assert!(quic_client_config.reconnection.enabled);
        assert!(quic_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            quic_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str(reconnection_interval).unwrap()
        );
        assert_eq!(
            quic_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[tokio::test]
    async fn should_succeed_with_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_ok());

        let quic_client_config = quic_client.unwrap().config;
        assert_eq!(
            quic_client_config.server_address,
            format!("{server_address}:{port}")
        );
        match &quic_client_config.auto_login {
            AutoLogin::Enabled(Credentials::PersonalAccessToken(t)) => {
                assert_eq!(t.expose_secret(), pat);
            }
            other => panic!("expected PersonalAccessToken auto_login, got {other:?}"),
        }

        assert_eq!(quic_client_config.response_buffer_size, 10_000_000);
        assert_eq!(quic_client_config.max_concurrent_bidi_streams, 10_000);
        assert_eq!(quic_client_config.datagram_send_buffer_size, 100_000);
        assert_eq!(quic_client_config.initial_mtu, 1200);
        assert_eq!(quic_client_config.send_window, 100_000);
        assert_eq!(quic_client_config.receive_window, 100_000);
        assert_eq!(quic_client_config.keep_alive_interval, 5000);
        assert_eq!(quic_client_config.max_idle_timeout, 10_000);
        assert!(!quic_client_config.validate_certificate);
        assert_eq!(
            quic_client_config.heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
        );

        assert!(quic_client_config.reconnection.enabled);
        assert!(quic_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            quic_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            quic_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[tokio::test]
    async fn should_create_with_hostname_address() {
        let config = QuicClientConfig {
            server_address: "localhost:8080".to_string(),
            ..Default::default()
        };
        let client = QuicClient::create(Arc::new(config));
        assert!(client.is_ok(), "Expected Ok, got: {:?}", client.err());
    }

    #[tokio::test]
    async fn should_create_with_fqdn_address() {
        let config = QuicClientConfig {
            server_address: "my-server.example.com:8080".to_string(),
            ..Default::default()
        };
        let client = QuicClient::create(Arc::new(config));
        assert!(client.is_ok(), "Expected Ok, got: {:?}", client.err());
    }

    #[tokio::test]
    async fn should_store_raw_hostname_in_current_server_address() {
        let hostname = "localhost:8080";
        let config = QuicClientConfig {
            server_address: hostname.to_string(),
            ..Default::default()
        };
        let client = QuicClient::create(Arc::new(config)).unwrap();
        assert_eq!(client.connection_address(), hostname);
    }

    #[tokio::test]
    async fn should_succeed_from_connection_string_with_hostname() {
        let connection_string = "iggy+quic://user:secret@localhost:1234";
        let client = QuicClient::from_connection_string(connection_string);
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.config.server_address, "localhost:1234");
    }

    #[test]
    fn should_fail_create_with_invalid_server_address_even_without_builder() {
        let config = Arc::new(QuicClientConfig {
            server_address: "127.0.0.1".to_string(),
            ..Default::default()
        });

        let client = QuicClient::create(config);
        assert!(client.is_err());
    }
}
