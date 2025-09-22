use std::{
    fmt::Debug,
    io::{IoSlice, Read, Write},
    mem::MaybeUninit,
    net::SocketAddr,
    ops::DerefMut,
    str::FromStr,
    sync::{Arc, Mutex},
};

use async_broadcast::{self, Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::{BinaryClient, BinaryTransport, Client};
use iggy_common::{
    AutoLogin, ClientState, Command, ConnectionString, ConnectionStringUtils, 
    DiagnosticEvent, IggyDuration, IggyError, IggyTimestamp, TcpClientConfig, TcpConnectionStringOptions,
    TransportProtocol,
};
use tracing::{debug, error, trace};

use crate::{
    connection::transport::{ClientConfig, Transport, TcpTransport, TcpTlsTransport},
    protocol::core::{ControlAction, ProtocolCore, ProtocolCoreConfig, TxBuf},
};

#[derive(Debug)]
pub struct TcpClientSync<T>
where
    T: Transport + Debug,
    T::Config: ClientConfig,
{
    pub(crate) stream_factory: Arc<T>,
    pub(crate) config: Arc<T::Config>,
    inner: Mutex<ProtocolCore>,
    stream: Mutex<Option<T::Stream>>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>), // TODO change async_broadcast to sync crate via feature flag
    recv_buffer: Mutex<BytesMut>,
    client_address: Mutex<Option<SocketAddr>>,
    connected_at: Mutex<Option<IggyTimestamp>>,
}

impl<T> TcpClientSync<T>
where
    T: Transport + Debug,
    T::Config: ClientConfig,
{
    pub fn create(stream_factory: Arc<T>, config: Arc<T::Config>) -> Result<Self, IggyError> {
        let proto_config = ProtocolCoreConfig {
            auto_login: config.auto_login(),
            reestablish_after: config.reconnection_reestablish_after(),
            max_retries: config.reconnection_max_retries(),
            reconnection_enabled: config.reconnection_enabled(),
            reconnection_interval: config.reconnection_interval(),
        };
        let (tx, rx) = broadcast(1000);

        Ok(Self {
            stream_factory,
            config,
            inner: Mutex::new(ProtocolCore::new(proto_config)),
            stream: Mutex::new(None),
            recv_buffer: Mutex::new(BytesMut::with_capacity(16 * 1024)),
            events: (tx, rx),
            client_address: Mutex::new(None),
            connected_at: Mutex::new(None),
        })
    }

    fn get_client_address_value(&self) -> String {
        let client_address = self.client_address.lock().unwrap();
        if let Some(client_address) = &*client_address {
            client_address.to_string()
        } else {
            "unknown".to_string()
        }
    }
}

#[maybe_async::sync_impl]
#[async_trait]
impl<T> Client for TcpClientSync<T>
where
    T: Transport + Send + Sync + 'static + Debug,
    T::Config: Send + Sync + Debug,
    T::Stream: Send + Sync + Debug,
{
    fn connect(&self) -> Result<(), IggyError> {
        let address = self.config.server_address();
        let config = self.config.clone();
        
        let stream = {
            let mut core = self.inner.lock().unwrap();
            let mut recv_buf = self.recv_buffer.lock().unwrap();
            Self::connect(&mut core, address, config, &mut recv_buf)?
        };

        if let Some(stream) = stream {
            *self.stream.lock().unwrap() = Some(stream);
            
            let now = IggyTimestamp::now();
            *self.connected_at.lock().unwrap() = Some(now);
            
            self.publish_event(DiagnosticEvent::Connected);
            
            let client_address = self.get_client_address_value();
            debug!("TcpClientSync client: {client_address} has connected to server at: {now}");
        }

        Ok(())
    }

    fn disconnect(&self) -> Result<(), IggyError> {
        if self.get_state() == ClientState::Disconnected {
            return Ok(());
        }

        let client_address = self.get_client_address_value();
        debug!("TcpClientSync client: {client_address} is disconnecting from server...");
        
        {
            let mut core = self.inner.lock().unwrap();
            core.disconnect();
            *self.stream.lock().unwrap() = None;
        }
        
        self.publish_event(DiagnosticEvent::Disconnected);
        
        let now = IggyTimestamp::now();
        debug!("TcpClientSync client: {client_address} has disconnected from server at: {now}.");
        Ok(())
    }

    fn shutdown(&self) -> Result<(), IggyError> {
        if self.get_state() == ClientState::Shutdown {
            return Ok(());
        }

        {
            let mut core = self.inner.lock().unwrap();
            let stream = self.stream.lock().unwrap().take();
            if let Some(mut stream) = stream {
                if let Err(e) = T::shutdown(&mut stream) {
                    error!("Failed to shutdown stream gracefully: {}", e);
                }
            }
            core.shutdown();
        }
        
        self.publish_event(DiagnosticEvent::Shutdown);
        Ok(())
    }

    fn subscribe_events(&self) -> async_broadcast::Receiver<iggy_common::DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[maybe_async::sync_impl]
#[async_trait]
impl<T> BinaryTransport for TcpClientSync<T>
where
    T: Transport + Send + Sync + 'static + Debug,
    T::Config: Send + Sync + Debug,
    T::Stream: Send + Sync + Debug,
{
    fn get_state(&self) -> ClientState {
        self.inner.lock().unwrap().state
    }

    fn set_state(&self, client_state: ClientState) {
        let mut core = self.inner.lock().unwrap();
        core.state = client_state
    }

    fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.try_broadcast(event) {
            error!("Failed to send a TCP diagnostic event: {error}");
        }
    }

    fn get_heartbeat_interval(&self) -> IggyDuration {
        self.config.heartbeat_interval()
    }

    fn send_with_response<C: Command>(&self, command: &C) -> Result<Bytes, IggyError> {
        command.validate()?;
        self.send_raw_with_response(command.code(), command.to_bytes())
    }

    fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        let result = self.send_raw(code, payload.clone());
        if result.is_ok() {
            return result;
        }

        let error = result.unwrap_err();
        
        let should_reconnect = {
            let core = self.inner.lock().unwrap();
            core.should_reconnect_for_error(&error)
        };

        if !should_reconnect {
            return Err(error);
        }

        let server_address = self.config.server_address();
        {
            let mut core = self.inner.lock().unwrap();
            core.initiate_reconnection(server_address)?;
        }

        self.connect()?;
        self.send_raw(code, payload)
    }
}

impl<T> BinaryClient for TcpClientSync<T>
where
    T: Transport + Send + Sync + 'static + Debug,
    T::Config: Send + Sync + Debug,
    T::Stream: Send + Sync + Debug,
{
}

impl<T> TcpClientSync<T>
where
    T: Transport + Debug,
{
    fn connect(
        core: &mut ProtocolCore,
        address: SocketAddr,
        config: Arc<T::Config>,
        recv_buf: &mut BytesMut,
    ) -> Result<Option<T::Stream>, IggyError> {
        let current_state = core.state;
        if matches!(
            current_state,
            ClientState::Connected | ClientState::Authenticating | ClientState::Authenticated
        ) {
            debug!(
                "TcpClientSync: Already connected (state: {:?}), completing waiter immediately",
                current_state
            );
            return Ok(None);
        }
        if matches!(current_state, ClientState::Connecting) {
            debug!("TcpClientSync: Already connecting, adding to waiters");
            return Ok(None);
        }

        core.desire_connect(address).map_err(|e| {
            error!("TcpClientSync: desire_connect failed: {}", e.as_string());
            e
        })?;

        let mut stream;
        loop {
            match core.poll() {
                ControlAction::Connect(addr) => match T::connect(config.clone(), addr) {
                    Ok(s) => {
                        core.on_connected()?;
                        stream = s;
                        break;
                    }
                    Err(_) => {
                        core.disconnect();
                        return Err(IggyError::CannotEstablishConnection)
                    }
                },
                ControlAction::Wait(duration) => {
                    std::thread::sleep(duration.get_duration());
                }
                ControlAction::Error(err) => return Err(err),
                ControlAction::Noop | ControlAction::Authenticate { .. } => return Ok(None),
            }
        }
        debug!("TcpClientSync: Connection established");

        if !core.should_wait_auth() {
            return Ok(Some(stream));
        }

        let tx = core
            .poll_transmit()
            .ok_or(IggyError::IncorrectConnectionState)?;
        write::<T>(&mut stream, tx)?;

        trace!("Sent AUTH, waiting for a response...");

        let _ = read::<T>(&mut stream, core, recv_buf);
        let _ = match core.take_auth_result() {
            Some(res) => res,
            None => Err(IggyError::IncorrectConnectionState),
        }?;

        Ok(Some(stream))
    }

    fn send_raw(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        let mut core = self.inner.lock().unwrap();
        let mut stream = self.stream.lock().unwrap();
        let mut recv_buf = self.recv_buffer.lock().unwrap();
        let s = match stream.deref_mut() {
            Some(s) => s,
            // TODO add error trace
            None => return Err(IggyError::IncorrectConnectionState),
        };

        core.send(code, payload)?;
        if let Some(tx) = core.poll_transmit() {
            write::<T>(s, tx)?;
            return read::<T>(s, core.deref_mut(), &mut recv_buf);
        }
        Ok(Bytes::new())
    }
}

fn write<T: Transport>(stream: &mut T::Stream, tx: TxBuf) -> Result<(), IggyError> {
    let mut off = 0usize;
    let total = tx.total_len();

    while off < total {
        let mut slices = [IoSlice::new(&[]), IoSlice::new(&[])];
        let iov = if off < 8 {
            slices[0] = IoSlice::new(&tx.header[off..]);
            if !tx.payload.is_empty() {
                slices[1] = IoSlice::new(&tx.payload);
                &slices[..2]
            } else {
                &slices[..1]
            }
        } else {
            let body_off = off - 8;
            slices[0] = IoSlice::new(&tx.payload[body_off..]);
            &slices[..1]
        };

        let n = stream
            .write_vectored(iov)
            .map_err(|_| IggyError::CannotEstablishConnection)?;
        if n == 0 {
            return Err(IggyError::CannotEstablishConnection);
        }
        off += n;
    }

    stream
        .flush()
        .map_err(|_| IggyError::CannotEstablishConnection)?;
    Ok(())
}

fn read<T: Transport>(
    stream: &mut T::Stream,
    core: &mut ProtocolCore,
    recv_buf: &mut BytesMut,
) -> Result<Bytes, IggyError> {
    let mut result: Option<Result<Bytes, IggyError>> = None;
    loop {
        if recv_buf.spare_capacity_mut().is_empty() {
            recv_buf.reserve(8192);
        }

        let spare: &mut [MaybeUninit<u8>] = recv_buf.spare_capacity_mut();

        let buf: &mut [u8] = unsafe { &mut *(spare as *mut [MaybeUninit<u8>] as *mut [u8]) };

        let n = match stream.read(buf) {
            Ok(0) => {
                core.disconnect();
                return Err(IggyError::CannotEstablishConnection);
            }
            Ok(n) => n,
            Err(e) => {
                error!("TcpClientSync: read error: {}", e);
                return Err(IggyError::CannotEstablishConnection);
            }
        };

        unsafe {
            recv_buf.advance_mut(n);
        }

        core.process_incoming_with(recv_buf, |_, status: u32, payload| {
            if status == 0 {
                result = Some(Ok(payload));
            } else {
                result = Some(Err(IggyError::from_code(status)));
            }
        });
        if result.is_some() {
            return result.unwrap();
        }
    }
}

impl TcpClientSync<TcpTransport> {
    pub fn new(
        server_address: &str,
        auto_sign_in: AutoLogin,
        heartbeat_interval: IggyDuration,
    ) -> Result<Self, IggyError> {
        Self::create(
            Arc::new(TcpTransport),
            Arc::new(TcpClientConfig {
                heartbeat_interval,
                server_address: server_address.to_string(),
                auto_login: auto_sign_in,
                ..Default::default()
            }),
        )
    }

    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        if ConnectionStringUtils::parse_protocol(connection_string)? != TransportProtocol::Tcp {
            return Err(IggyError::InvalidConnectionString);
        }

        Self::create(
            Arc::new(TcpTransport),
            Arc::new(
                ConnectionString::<TcpConnectionStringOptions>::from_str(connection_string)?.into(),
            ),
        )
    }

    pub fn create_tcp(config: Arc<TcpClientConfig>) -> Result<Self, IggyError> {
        Self::create(Arc::new(TcpTransport), config)
    }
}

// Type aliases for convenience
pub type TcpClientSyncTcp = TcpClientSync<TcpTransport>;
pub type TcpClientSyncTls = TcpClientSync<TcpTlsTransport>;

impl TcpClientSyncTls {
    pub fn from_connection_string_tls(connection_string: &str) -> Result<Self, IggyError> {
        if ConnectionStringUtils::parse_protocol(connection_string)? != TransportProtocol::Tcp {
            return Err(IggyError::InvalidConnectionString);
        }

        Self::create(
            Arc::new(TcpTlsTransport),
            Arc::new(
                ConnectionString::<TcpConnectionStringOptions>::from_str(connection_string)?.into(),
            ),
        )
    }

    pub fn create_tcp_tls(config: Arc<TcpClientConfig>) -> Result<Self, IggyError> {
        Self::create(Arc::new(TcpTlsTransport), config)
    }

    pub fn new_tls(
        server_address: &str,
        domain: &str,
        auto_sign_in: AutoLogin,
        heartbeat_interval: IggyDuration,
    ) -> Result<Self, IggyError> {
        Self::create(
            Arc::new(TcpTlsTransport),
            Arc::new(TcpClientConfig {
                heartbeat_interval,
                server_address: server_address.to_string(),
                tls_enabled: true,
                tls_domain: domain.to_string(),
                auto_login: auto_sign_in,
                ..Default::default()
            }),
        )
    }
}

impl Default for TcpClientSync<TcpTransport> {
    fn default() -> Self {
        TcpClientSync::create_tcp(Arc::new(TcpClientConfig::default())).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::Credentials;

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_without_username() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_without_port() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_with_invalid_prefix() {
        let connection_string_prefix = "invalid+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_with_unmatch_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_succeed_with_default_prefix() {
        let default_connection_string_prefix = "iggy://";
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{default_connection_string_prefix}{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_ok());
    }

    #[test]
    fn should_fail_with_invalid_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?invalid_option=invalid"
        );
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_succeed_without_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_ok());

        let tcp_client_sync = tcp_client.unwrap();
        assert_eq!(
            tcp_client_sync.config.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            tcp_client_sync.config.auto_login,
            AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string()
            ))
        );

        assert!(!tcp_client_sync.config.tls_enabled);
        assert!(tcp_client_sync.config.tls_domain.is_empty());
        assert!(tcp_client_sync.config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_sync.config.heartbeat_interval,
            IggyDuration::from_str("5s").unwrap()
        );

        assert!(tcp_client_sync.config.reconnection.enabled);
        assert!(tcp_client_sync.config.reconnection.max_retries.is_none());
        assert_eq!(
            tcp_client_sync.config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_sync.config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[test]
    fn should_succeed_with_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let heartbeat_interval = "10s";
        let reconnection_retries = "10";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?heartbeat_interval={heartbeat_interval}&reconnection_retries={reconnection_retries}"
        );
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_ok());

        let tcp_client_sync = tcp_client.unwrap();
        assert_eq!(
            tcp_client_sync.config.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            tcp_client_sync.config.auto_login,
            AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string()
            ))
        );

        assert!(!tcp_client_sync.config.tls_enabled);
        assert!(tcp_client_sync.config.tls_domain.is_empty());
        assert!(tcp_client_sync.config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_sync.config.heartbeat_interval,
            IggyDuration::from_str(heartbeat_interval).unwrap()
        );

        assert!(tcp_client_sync.config.reconnection.enabled);
        assert_eq!(
            tcp_client_sync.config.reconnection.max_retries.unwrap(),
            reconnection_retries.parse::<u32>().unwrap()
        );
        assert_eq!(
            tcp_client_sync.config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_sync.config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[test]
    fn should_succeed_with_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let tcp_client = TcpClientSync::<TcpTransport>::from_connection_string(&value);
        assert!(tcp_client.is_ok());

        let tcp_client_sync = tcp_client.unwrap();
        assert_eq!(
            tcp_client_sync.config.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            tcp_client_sync.config.auto_login,
            AutoLogin::Enabled(Credentials::PersonalAccessToken(pat.to_string()))
        );

        assert!(!tcp_client_sync.config.tls_enabled);
        assert!(tcp_client_sync.config.tls_domain.is_empty());
        assert!(tcp_client_sync.config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_sync.config.heartbeat_interval,
            IggyDuration::from_str("5s").unwrap()
        );

        assert!(tcp_client_sync.config.reconnection.enabled);
        assert!(tcp_client_sync.config.reconnection.max_retries.is_none());
        assert_eq!(
            tcp_client_sync.config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_sync.config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[test]
    fn should_create_tcp_client_with_new() {
        let server_address = "127.0.0.1:8080";
        let auto_login = AutoLogin::Enabled(Credentials::UsernamePassword(
            "user".to_string(),
            "pass".to_string(),
        ));
        let heartbeat_interval = IggyDuration::from_str("10s").unwrap();

        let tcp_client = TcpClientSync::<TcpTransport>::new(
            server_address,
            auto_login.clone(),
            heartbeat_interval,
        );
        assert!(tcp_client.is_ok());

        let client = tcp_client.unwrap();
        assert_eq!(client.config.server_address, server_address);
        assert_eq!(client.config.auto_login, auto_login);
        assert_eq!(client.config.heartbeat_interval, heartbeat_interval);
        assert!(!client.config.tls_enabled);
    }

    #[test]
    fn should_create_tls_client_with_new_tls() {
        let server_address = "127.0.0.1:8080";
        let domain = "localhost";
        let auto_login = AutoLogin::Enabled(Credentials::UsernamePassword(
            "user".to_string(),
            "pass".to_string(),
        ));
        let heartbeat_interval = IggyDuration::from_str("10s").unwrap();

        let tcp_client = TcpClientSyncTls::new_tls(
            server_address,
            domain,
            auto_login.clone(),
            heartbeat_interval,
        );
        assert!(tcp_client.is_ok());

        let client = tcp_client.unwrap();
        assert_eq!(client.config.server_address, server_address);
        assert_eq!(client.config.auto_login, auto_login);
        assert_eq!(client.config.heartbeat_interval, heartbeat_interval);
        assert!(client.config.tls_enabled);
        assert_eq!(client.config.tls_domain, domain);
    }

    #[test]
    fn should_create_tcp_client_with_config() {
        let config = Arc::new(TcpClientConfig {
            server_address: "127.0.0.1:8080".to_string(),
            auto_login: AutoLogin::Enabled(Credentials::PersonalAccessToken("token".to_string())),
            tls_enabled: false,
            ..Default::default()
        });

        let tcp_client = TcpClientSync::<TcpTransport>::create_tcp(config.clone());
        assert!(tcp_client.is_ok());

        let client = tcp_client.unwrap();
        assert_eq!(client.config.server_address, config.server_address);
        assert_eq!(client.config.auto_login, config.auto_login);
        assert!(!client.config.tls_enabled);
    }

    #[test]
    fn should_create_tls_client_with_config() {
        let config = Arc::new(TcpClientConfig {
            server_address: "127.0.0.1:8080".to_string(),
            auto_login: AutoLogin::Enabled(Credentials::PersonalAccessToken("token".to_string())),
            tls_enabled: true,
            tls_domain: "localhost".to_string(),
            ..Default::default()
        });

        let tcp_client = TcpClientSyncTls::create_tcp_tls(config.clone());
        assert!(tcp_client.is_ok());

        let client = tcp_client.unwrap();
        assert_eq!(client.config.server_address, config.server_address);
        assert_eq!(client.config.auto_login, config.auto_login);
        assert!(client.config.tls_enabled);
        assert_eq!(client.config.tls_domain, config.tls_domain);
    }

    #[test]
    fn should_handle_default_client() {
        let client = TcpClientSync::<TcpTransport>::default();
        let default_config = TcpClientConfig::default();
        
        assert_eq!(client.config.server_address, default_config.server_address);
        assert_eq!(client.config.auto_login, default_config.auto_login);
        assert_eq!(client.config.heartbeat_interval, default_config.heartbeat_interval);
        assert_eq!(client.config.tls_enabled, default_config.tls_enabled);
    }
}
