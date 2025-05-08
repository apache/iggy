use crate::prelude::Client;
use crate::tcp::tcp_config::TcpClientConfig;
use crate::tcp::tcp_connection_stream_kind::ConnectionStreamKind;
use async_broadcast::{broadcast, Receiver, Sender};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::{BinaryClient, BinaryTransport};
use iggy_common::{
    AutoLogin, ClientState, Command, ConnectionString, DiagnosticEvent, IggyDuration, IggyError,
    IggyErrorDiscriminants, IggyTimestamp,
};
use rustls::pki_types::ServerName;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_rustls::{TlsConnector, TlsStream};
use tracing::{error, info, trace, warn};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const NAME: &str = "Iggy";

/// TCP client for interacting with the Iggy API.
/// It requires a valid server address.
#[derive(Debug)]
pub struct TcpClient {
    pub(crate) stream: Mutex<Option<ConnectionStreamKind>>,
    pub(crate) config: Arc<TcpClientConfig>,
    pub(crate) state: Mutex<ClientState>,
    client_address: Mutex<Option<SocketAddr>>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    connected_at: Mutex<Option<IggyTimestamp>>,
}

impl Default for TcpClient {
    fn default() -> Self {
        TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap()
    }
}

#[async_trait]
impl Client for TcpClient {
    async fn connect(&self) -> Result<(), IggyError> {
        TcpClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        TcpClient::disconnect(self).await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        TcpClient::shutdown(self).await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[async_trait]
impl BinaryTransport for TcpClient {
    async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }

    async fn send_with_response<T: Command>(&self, command: &T) -> Result<Bytes, IggyError> {
        command.validate()?;
        self.send_raw_with_response(command.code(), command.to_bytes())
            .await
    }

    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        let result = self.send_raw(code, payload.clone()).await;
        if result.is_ok() {
            return result;
        }

        let error = result.unwrap_err();
        if !matches!(
            error,
            IggyError::Disconnected
                | IggyError::EmptyResponse
                | IggyError::Unauthenticated
                | IggyError::StaleClient
        ) {
            return Err(error);
        }

        if !self.config.reconnection.enabled {
            return Err(IggyError::Disconnected);
        }

        self.disconnect().await?;

        {
            let client_address = self.get_client_address_value().await;
            info!(
                "Reconnecting to the server: {} by client: {client_address}...",
                self.config.server_address
            );
        }

        self.connect().await?;
        self.send_raw(code, payload).await
    }

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.broadcast(event).await {
            error!("Failed to send a TCP diagnostic event: {error}");
        }
    }

    fn get_heartbeat_interval(&self) -> IggyDuration {
        self.config.heartbeat_interval
    }
}

impl BinaryClient for TcpClient {}

impl TcpClient {
    /// Create a new TCP client for the provided server address.
    pub fn new(
        server_address: &str,
        auto_sign_in: AutoLogin,
        heartbeat_interval: IggyDuration,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(TcpClientConfig {
            heartbeat_interval,
            server_address: server_address.to_string(),
            auto_login: auto_sign_in,
            ..Default::default()
        }))
    }

    /// Create a new TCP client for the provided server address using TLS.
    pub fn new_tls(
        server_address: &str,
        domain: &str,
        auto_sign_in: AutoLogin,
        heartbeat_interval: IggyDuration,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(TcpClientConfig {
            heartbeat_interval,
            server_address: server_address.to_string(),
            tls_enabled: true,
            tls_domain: domain.to_string(),
            auto_login: auto_sign_in,
            ..Default::default()
        }))
    }

    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        Self::create(Arc::new(
            ConnectionString::from_str(connection_string)?.into(),
        ))
    }

    /// Create a new TCP client based on the provided configuration.
    pub fn create(config: Arc<TcpClientConfig>) -> Result<Self, IggyError> {
        Ok(Self {
            config,
            client_address: Mutex::new(None),
            stream: Mutex::new(None),
            state: Mutex::new(ClientState::Disconnected),
            events: broadcast(1000),
            connected_at: Mutex::new(None),
        })
    }

    async fn handle_response(
        &self,
        status: u32,
        length: u32,
        stream: &mut ConnectionStreamKind,
    ) -> Result<Bytes, IggyError> {
        if status != 0 {
            // TEMP: See https://github.com/apache/iggy/pull/604 for context.
            if status == IggyErrorDiscriminants::TopicIdAlreadyExists as u32
                || status == IggyErrorDiscriminants::TopicNameAlreadyExists as u32
                || status == IggyErrorDiscriminants::StreamIdAlreadyExists as u32
                || status == IggyErrorDiscriminants::StreamNameAlreadyExists as u32
                || status == IggyErrorDiscriminants::UserAlreadyExists as u32
                || status == IggyErrorDiscriminants::PersonalAccessTokenAlreadyExists as u32
                || status == IggyErrorDiscriminants::ConsumerGroupIdAlreadyExists as u32
                || status == IggyErrorDiscriminants::ConsumerGroupNameAlreadyExists as u32
            {
                tracing::debug!(
                    "Received a server resource already exists response: {} ({})",
                    status,
                    IggyError::from_code_as_string(status)
                )
            } else {
                error!(
                    "Received an invalid response with status: {} ({}).",
                    status,
                    IggyError::from_code_as_string(status),
                );
            }

            return Err(IggyError::from_code(status));
        }

        trace!("Status: OK. Response length: {}", length);
        if length <= 1 {
            return Ok(Bytes::new());
        }

        let mut response_buffer = BytesMut::with_capacity(length as usize);
        response_buffer.put_bytes(0, length as usize);
        stream.read(&mut response_buffer).await?;
        Ok(response_buffer.freeze())
    }
    async fn get_client_address_value(&self) -> String {
        let client_address = self.client_address.lock().await;
        if let Some(client_address) = &*client_address {
            client_address.to_string()
        } else {
            "unknown".to_string()
        }
    }
}
