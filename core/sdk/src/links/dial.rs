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

//! Opens a link to one endpoint over the transport of the client.

use super::Link;
use super::mux::MuxLink;
use super::quic::QuicLink;
use crate::tcp::tcp_tls_verifier::NoServerVerification;
use iggy_common::{
    IggyError, QuicClientConfig, TcpClientConfig, TransportProtocol, WebSocketClientConfig,
};
use quinn::{ConnectError, Endpoint};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use std::net::{Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpStream, lookup_host};
use tokio::sync::OnceCell;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::{Connector, client_async_tls_with_config};
use tracing::error;

/// A replica that drops the SYN or never finishes a handshake fails its dial
/// after this long, so it cannot hold up the dials of other replicas.
pub(crate) const DIAL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub(crate) enum Dialer {
    Tcp(Arc<TcpClientConfig>),
    /// Every QUIC link of one client shares `endpoint`.
    Quic {
        config: Arc<QuicClientConfig>,
        endpoint: OnceCell<Endpoint>,
        client_config: quinn::ClientConfig,
    },
    WebSocket(Arc<WebSocketClientConfig>),
    #[cfg(test)]
    Scripted(test_support::ScriptedDialer),
}

impl Dialer {
    pub(crate) fn protocol(&self) -> TransportProtocol {
        match self {
            Self::Tcp(_) => TransportProtocol::Tcp,
            Self::Quic { .. } => TransportProtocol::Quic,
            Self::WebSocket(_) => TransportProtocol::WebSocket,
            #[cfg(test)]
            Self::Scripted(_) => TransportProtocol::Tcp,
        }
    }

    /// Opens a link to `endpoint` within `DIAL_TIMEOUT`.
    ///
    /// `CannotEstablishConnection` means that this endpoint failed, and
    /// another endpoint can still work. Any other error is a configuration
    /// fault that no endpoint can satisfy.
    pub(crate) async fn dial(&self, endpoint: &str) -> Result<Link, IggyError> {
        let dial = async {
            match self {
                Self::Tcp(config) => dial_tcp(config, endpoint).await,
                Self::Quic {
                    config,
                    endpoint: quic,
                    client_config,
                } => dial_quic(config, quic, client_config, endpoint).await,
                Self::WebSocket(config) => dial_websocket(config, endpoint).await,
                #[cfg(test)]
                Self::Scripted(dialer) => dialer.dial(endpoint).await,
            }
        };
        timeout(DIAL_TIMEOUT, dial).await.unwrap_or_else(|_| {
            error!("Connecting to {endpoint} took longer than {DIAL_TIMEOUT:?}.");
            Err(IggyError::CannotEstablishConnection)
        })
    }

    /// Waits until every QUIC connection of the client is gone.
    pub(crate) async fn shutdown(&self) {
        if let Self::Quic { endpoint, .. } = self
            && let Some(endpoint) = endpoint.get()
        {
            endpoint.wait_idle().await;
        }
    }
}

async fn dial_tcp(config: &TcpClientConfig, endpoint: &str) -> Result<Link, IggyError> {
    let stream = TcpStream::connect(endpoint).await.map_err(|error| {
        error!("Failed to connect to {endpoint}: {error}");
        IggyError::CannotEstablishConnection
    })?;
    let peer = stream.peer_addr().map_err(|error| {
        error!("Failed to read the address of {endpoint}: {error}");
        IggyError::CannotEstablishConnection
    })?;
    if let Err(error) = stream.set_nodelay(config.nodelay) {
        error!("Failed to set the nodelay option for {endpoint}: {error}, continuing...");
    }
    if !config.tls_enabled {
        let (reader, writer) = stream.into_split();
        return Ok(Link::Mux(MuxLink::tcp(reader, writer, peer)));
    }

    let tls = tls_client_config(
        config.tls_validate_certificate,
        config.tls_ca_file.as_deref(),
    )?;
    let domain = if config.tls_domain.is_empty() {
        tls_server_name(endpoint)
    } else {
        config.tls_domain.clone()
    };
    let domain = ServerName::try_from(domain).map_err(|error| {
        error!("Failed to create a server name from the domain. {error}");
        IggyError::InvalidTlsDomain
    })?;
    let stream = TlsConnector::from(Arc::new(tls))
        .connect(domain, stream)
        .await
        .map_err(|error| {
            // The verdict describes the peer, not this client: a certificate
            // that names another host, or a peer that answers a ClientHello
            // with something else. Other endpoints can still be fine.
            error!("Failed to establish a TLS connection to {endpoint}: {error}");
            IggyError::CannotEstablishConnection
        })?;
    let (reader, writer) = tokio::io::split(stream);
    Ok(Link::Mux(MuxLink::tcp(reader, writer, peer)))
}

async fn dial_quic(
    config: &QuicClientConfig,
    quic: &OnceCell<Endpoint>,
    client_config: &quinn::ClientConfig,
    endpoint: &str,
) -> Result<Link, IggyError> {
    let address = resolve(endpoint).await?;
    let quic = quic
        .get_or_try_init(|| async {
            let client_address = if address.is_ipv6()
                && config.client_address == QuicClientConfig::default().client_address
            {
                SocketAddr::from((Ipv6Addr::LOCALHOST, 0))
            } else {
                config
                    .client_address
                    .parse::<SocketAddr>()
                    .map_err(|_| IggyError::InvalidClientAddress)?
            };
            let mut endpoint =
                Endpoint::client(client_address).map_err(|_| IggyError::CannotCreateEndpoint)?;
            endpoint.set_default_client_config(client_config.clone());
            Ok::<_, IggyError>(endpoint)
        })
        .await?;
    let connecting = quic
        .connect(address, &config.server_name)
        .map_err(|error| {
            error!("Failed to start a QUIC connection to {endpoint}: {error}");
            match error {
                ConnectError::InvalidServerName(_) => IggyError::InvalidTlsDomain,
                _ => IggyError::CannotEstablishConnection,
            }
        })?;
    let connection = connecting.await.map_err(|error| {
        error!("Failed to establish a QUIC connection to {endpoint}: {error}");
        IggyError::CannotEstablishConnection
    })?;
    Ok(Link::Quic(QuicLink::new(
        connection,
        config.response_buffer_size as usize,
    )))
}

async fn dial_websocket(config: &WebSocketClientConfig, endpoint: &str) -> Result<Link, IggyError> {
    let address = resolve(endpoint).await?;
    let stream = TcpStream::connect(address).await.map_err(|error| {
        error!("Failed to connect to {endpoint}: {error}");
        IggyError::CannotEstablishConnection
    })?;
    let (url, connector) = if config.tls_enabled {
        let tls = tls_client_config(
            config.tls_validate_certificate,
            config.tls_ca_file.as_deref(),
        )?;
        let domain = if config.tls_domain.is_empty() {
            address.ip().to_string()
        } else {
            config.tls_domain.clone()
        };
        let server_name = domain
            .strip_prefix('[')
            .and_then(|name| name.strip_suffix(']'))
            .unwrap_or(&domain);
        ServerName::try_from(server_name).map_err(|error| {
            error!("Failed to create a server name from the domain. {error}");
            IggyError::InvalidTlsDomain
        })?;
        let host = if domain.contains(':') && !domain.starts_with('[') {
            format!("[{domain}]")
        } else {
            domain
        };
        (
            format!("wss://{host}:{}", address.port()),
            Connector::Rustls(Arc::new(tls)),
        )
    } else {
        (format!("ws://{address}"), Connector::Plain)
    };
    let (websocket, _) = client_async_tls_with_config(
        url,
        stream,
        Some(config.ws_config.to_tungstenite_config()),
        Some(connector),
    )
    .await
    .map_err(|error| {
        error!("The WebSocket handshake with {endpoint} failed: {error}");
        IggyError::CannotEstablishConnection
    })?;
    Ok(Link::Mux(MuxLink::websocket(websocket, address)))
}

/// The first address of `endpoint`. A name that does not resolve now can
/// resolve on the next attempt, so it fails only this dial.
async fn resolve(endpoint: &str) -> Result<SocketAddr, IggyError> {
    match lookup_host(endpoint).await {
        Ok(mut addresses) => addresses.next().ok_or_else(|| {
            error!("No address resolved for {endpoint}.");
            IggyError::CannotEstablishConnection
        }),
        Err(error) => {
            error!("Failed to resolve {endpoint}: {error}");
            Err(IggyError::CannotEstablishConnection)
        }
    }
}

fn tls_client_config(
    validate_certificate: bool,
    ca_file: Option<&str>,
) -> Result<rustls::ClientConfig, IggyError> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    if !validate_certificate {
        return Ok(rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoServerVerification))
            .with_no_client_auth());
    }

    let mut roots = rustls::RootCertStore::empty();
    match ca_file {
        Some(path) => {
            let certificates = CertificateDer::pem_file_iter(path).map_err(|error| {
                error!("Failed to read the CA file: {path}. {error}");
                IggyError::InvalidTlsCertificatePath
            })?;
            for certificate in certificates {
                let certificate = certificate.map_err(|error| {
                    error!("Failed to read a certificate from the CA file: {path}. {error}");
                    IggyError::InvalidTlsCertificate
                })?;
                roots.add(certificate).map_err(|error| {
                    error!("Failed to add a certificate to the root certificate store. {error}");
                    IggyError::InvalidTlsCertificate
                })?;
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

/// The host part of `endpoint`, for the SNI of a TLS handshake.
fn tls_server_name(endpoint: &str) -> String {
    if let Ok(address) = SocketAddr::from_str(endpoint) {
        return address.ip().to_string();
    }
    endpoint
        .rsplit_once(':')
        .map_or(endpoint, |(host, _port)| host)
        .to_owned()
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::Link;
    use crate::links::mux::MuxLink;
    use crate::links::test_support::LOCALHOST;
    use futures::future::BoxFuture;
    use iggy_common::IggyError;
    use std::fmt;
    use tokio::io::DuplexStream;

    type Accept = dyn Fn(&str) -> BoxFuture<'static, Result<DuplexStream, IggyError>> + Send + Sync;

    /// Dials in-memory connections. The test serves the other end of each
    /// one, or fails the dial.
    pub(crate) struct ScriptedDialer(Box<Accept>);

    impl ScriptedDialer {
        pub(crate) fn new(
            accept: impl Fn(&str) -> BoxFuture<'static, Result<DuplexStream, IggyError>>
            + Send
            + Sync
            + 'static,
        ) -> Self {
            Self(Box::new(accept))
        }

        pub(crate) async fn dial(&self, endpoint: &str) -> Result<Link, IggyError> {
            let stream = (self.0)(endpoint).await?;
            let (reader, writer) = tokio::io::split(stream);
            Ok(Link::Mux(MuxLink::tcp(reader, writer, LOCALHOST)))
        }
    }

    impl fmt::Debug for ScriptedDialer {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("ScriptedDialer")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::links::test_support::{LOCALHOST, SERVER_NAME, quic_server};
    use rustls::RootCertStore;
    use tokio::net::TcpListener;

    const INVALID_TLS_DOMAIN: &str = "invalid server name";
    const INVALID_CA_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml/ca.pem");

    #[tokio::test]
    async fn given_invalid_endpoint_when_dialing_should_report_endpoint_failure() {
        let dialers = [
            Dialer::Tcp(Arc::new(TcpClientConfig::default())),
            quic_dialer(QuicClientConfig::default()),
            Dialer::WebSocket(Arc::new(WebSocketClientConfig::default())),
        ];
        for dialer in dialers {
            assert_eq!(
                dialer.dial(&LOCALHOST.to_string()).await.err(),
                Some(IggyError::CannotEstablishConnection),
                "{:?} must allow another endpoint after port zero fails",
                dialer.protocol()
            );
        }
    }

    #[tokio::test]
    async fn given_invalid_tls_domain_when_dialing_should_report_configuration_failure() {
        let listener = TcpListener::bind(LOCALHOST).await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let dialers = [
            Dialer::Tcp(Arc::new(TcpClientConfig {
                tls_enabled: true,
                tls_domain: INVALID_TLS_DOMAIN.into(),
                ..Default::default()
            })),
            quic_dialer(QuicClientConfig {
                server_name: INVALID_TLS_DOMAIN.into(),
                ..Default::default()
            }),
            Dialer::WebSocket(Arc::new(WebSocketClientConfig {
                tls_enabled: true,
                tls_domain: INVALID_TLS_DOMAIN.into(),
                ..Default::default()
            })),
        ];
        for dialer in dialers {
            assert_eq!(
                dialer.dial(&endpoint).await.err(),
                Some(IggyError::InvalidTlsDomain),
                "{:?} must not retry an invalid client domain against another endpoint",
                dialer.protocol()
            );
        }
    }

    #[tokio::test]
    async fn given_invalid_ca_path_when_dialing_should_report_configuration_failure() {
        let listener = TcpListener::bind(LOCALHOST).await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let dialers = [
            Dialer::Tcp(Arc::new(TcpClientConfig {
                tls_enabled: true,
                tls_ca_file: Some(INVALID_CA_PATH.into()),
                ..Default::default()
            })),
            Dialer::WebSocket(Arc::new(WebSocketClientConfig {
                tls_enabled: true,
                tls_validate_certificate: true,
                tls_ca_file: Some(INVALID_CA_PATH.into()),
                ..Default::default()
            })),
        ];
        for dialer in dialers {
            assert_eq!(
                dialer.dial(&endpoint).await.err(),
                Some(IggyError::InvalidTlsCertificatePath),
                "{:?} must preserve the local CA configuration error",
                dialer.protocol()
            );
        }
    }

    #[tokio::test]
    async fn given_peer_closes_handshake_when_dialing_should_report_endpoint_failure() {
        let dialers = [
            Dialer::Tcp(Arc::new(TcpClientConfig {
                tls_enabled: true,
                ..Default::default()
            })),
            Dialer::WebSocket(Arc::new(WebSocketClientConfig::default())),
            Dialer::WebSocket(Arc::new(WebSocketClientConfig {
                tls_enabled: true,
                tls_domain: "[::1]".into(),
                ..Default::default()
            })),
        ];
        for dialer in dialers {
            let listener = TcpListener::bind(LOCALHOST).await.unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let (result, ()) = tokio::join!(dialer.dial(&endpoint), async {
                drop(listener.accept().await.unwrap());
            });
            assert_eq!(
                result.err(),
                Some(IggyError::CannotEstablishConnection),
                "{:?} must not classify a failed peer handshake as invalid client config",
                dialer.protocol()
            );
        }
    }

    #[tokio::test]
    async fn given_untrusted_quic_peer_when_dialing_should_report_endpoint_failure() {
        let (server, _certificate) = quic_server();
        let endpoint = server.local_addr().unwrap().to_string();
        let dialer = quic_dialer(QuicClientConfig {
            server_name: SERVER_NAME.into(),
            ..Default::default()
        });
        let (result, accepted) = tokio::join!(dialer.dial(&endpoint), async {
            server.accept().await.unwrap().await
        });
        assert!(accepted.is_err(), "the untrusted certificate must fail TLS");
        assert_eq!(result.err(), Some(IggyError::CannotEstablishConnection));
    }

    #[test]
    fn given_dns_ipv4_and_ipv6_endpoints_when_naming_the_tls_server_should_drop_the_port() {
        assert_eq!(tls_server_name("iggy-1:8090"), "iggy-1");
        assert_eq!(tls_server_name("127.0.0.1:8090"), "127.0.0.1");
        assert_eq!(tls_server_name("[fd00::1]:8090"), "fd00::1");
    }

    fn quic_dialer(config: QuicClientConfig) -> Dialer {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut endpoint = Endpoint::client(LOCALHOST).unwrap();
        let client_config = quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        endpoint.set_default_client_config(client_config.clone());
        Dialer::Quic {
            config: Arc::new(config),
            endpoint: OnceCell::new_with(Some(endpoint)),
            client_config,
        }
    }
}
