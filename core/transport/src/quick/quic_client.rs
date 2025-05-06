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

use async_broadcast::{broadcast, Receiver, Sender};
use bytes::Bytes;
use iggy_common::{
    AutoLogin, ClientState, Credentials, DiagnosticEvent, IggyDuration, IggyError, IggyTimestamp,
    QuicClientConfig,
};
use quinn::crypto::rustls::QuicClientConfig as QuinnQuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, RecvStream, VarInt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error, SignatureScheme};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, trace, warn};

const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;

/// QUIC client for interacting with the Iggy API.
#[derive(Debug)]
pub struct QuicClient {
    endpoint: Endpoint,
    connection: Mutex<Option<Connection>>,
    config: Arc<QuicClientConfig>,
    server_address: SocketAddr,
    state: Mutex<ClientState>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    connected_at: Mutex<Option<IggyTimestamp>>,
}

unsafe impl Send for QuicClient {}
unsafe impl Sync for QuicClient {}

impl Default for QuicClient {
    fn default() -> Self {
        QuicClient::create(Arc::new(QuicClientConfig::default())).unwrap()
    }
}

impl QuicClient {
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn connection(&self) -> &Mutex<Option<Connection>> {
        &self.connection
    }

    pub fn config(&self) -> &Arc<QuicClientConfig> {
        &self.config
    }

    pub fn server_address(&self) -> SocketAddr {
        self.server_address
    }

    pub fn state(&self) -> &Mutex<ClientState> {
        &self.state
    }

    pub fn events(&self) -> &(Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>) {
        &self.events
    }

    pub fn connected_at(&self) -> &Mutex<Option<IggyTimestamp>> {
        &self.connected_at
    }

    pub fn set_state(&mut self, state: Mutex<ClientState>) {
        self.state = state;
    }
}

impl QuicClient {
    /// Creates a new QUIC client for the provided client and server addresses.
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

    /// Create a new QUIC client for the provided configuration.
    pub fn create(config: Arc<QuicClientConfig>) -> Result<Self, IggyError> {
        let server_address = config
            .server_address
            .parse::<SocketAddr>()
            .map_err(|error| {
                error!("Invalid server address: {error}");
                IggyError::InvalidServerAddress
            })?;
        let client_address = if server_address.is_ipv6()
            && config.client_address == QuicClientConfig::default().client_address
        {
            "[::1]:0"
        } else {
            &config.client_address
        }
        .parse::<SocketAddr>()
        .map_err(|error| {
            error!("Invalid client address: {error}");
            IggyError::InvalidClientAddress
        })?;

        let quic_config = configure(&config)?;
        let endpoint = Endpoint::client(client_address);
        if endpoint.is_err() {
            error!("Cannot create client endpoint");
            return Err(IggyError::CannotCreateEndpoint);
        }

        let mut endpoint = endpoint.unwrap();
        endpoint.set_default_client_config(quic_config);

        Ok(Self {
            config,
            endpoint,
            server_address,
            connection: Mutex::new(None),
            state: Mutex::new(ClientState::Disconnected),
            events: broadcast(1000),
            connected_at: Mutex::new(None),
        })
    }

    async fn handle_response(&self, recv: &mut RecvStream) -> Result<Bytes, IggyError> {
        let buffer = recv
            .read_to_end(self.config.response_buffer_size as usize)
            .await
            .map_err(|error| {
                error!("Failed to read response data: {error}");
                IggyError::QuicError
            })?;
        if buffer.is_empty() {
            return Err(IggyError::EmptyResponse);
        }

        let status = u32::from_le_bytes(
            buffer[..4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        if status != 0 {
            error!(
                "Received an invalid response with status: {} ({}).",
                status,
                IggyError::from_code_as_string(status)
            );

            return Err(IggyError::from_code(status));
        }

        let length = u32::from_le_bytes(
            buffer[4..RESPONSE_INITIAL_BYTES_LENGTH]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        trace!("Status: OK. Response length: {}", length);
        if length <= 1 {
            return Ok(Bytes::new());
        }

        Ok(Bytes::copy_from_slice(
            &buffer[RESPONSE_INITIAL_BYTES_LENGTH..RESPONSE_INITIAL_BYTES_LENGTH + length as usize],
        ))
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

    if CryptoProvider::get_default().is_none() {
        if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
            warn!("Failed to install rustls crypto provider. Error: {:?}. This may be normal if another thread installed it first.", e);
        }
    }
    let mut client_config = match config.validate_certificate {
        true => ClientConfig::with_platform_verifier(),
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

#[derive(Debug)]
struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Signature used by the server to sign self-signed certificate (using rcgen)
        vec![SignatureScheme::ECDSA_NISTP256_SHA256]
    }
}
