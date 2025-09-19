use std::{
    collections::VecDeque,
    net::SocketAddr,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy_common::{AutoLogin, ClientState, Credentials, IggyDuration, IggyError, IggyErrorDiscriminants, IggyTimestamp};
use tracing::{debug, error, info, warn};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const REQUEST_HEADER_BYTES: usize = 8;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
#[derive(Debug)]
pub struct ProtocolCoreConfig {
    pub auto_login: AutoLogin,
    pub reestablish_after: IggyDuration,
    pub max_retries: Option<u32>,
    pub reconnection_enabled: bool,
    pub reconnection_interval: IggyDuration,
}

#[derive(Debug)]
pub enum ControlAction {
    Connect(SocketAddr),
    Wait(IggyDuration),
    Authenticate { username: String, password: String },
    Noop,
    Error(IggyError),
}

pub struct TxBuf {
    pub header: [u8; 8],
    pub payload: Bytes,
    pub request_id: u64,
}

impl TxBuf {
    #[inline]
    pub fn total_len(&self) -> usize {
        REQUEST_HEADER_BYTES + self.payload.len()
    }
}

#[derive(Debug)]
pub struct ProtocolCore {
    pub state: ClientState,
    config: ProtocolCoreConfig,
    last_connect_attempt: Option<IggyTimestamp>,
    last_reconnection_attempt: Option<IggyTimestamp>,
    pub retry_count: u32,
    pub connection_retry_count: u32,
    next_request_id: u64,
    pending_sends: VecDeque<(u32, Bytes, u64)>,
    sent_order: VecDeque<u64>,
    auth_pending: bool,
    auth_request_id: Option<u64>,
    server_address: Option<SocketAddr>,
    last_auth_result: Option<Result<(), IggyError>>,
    connection_established: bool,
}

impl ProtocolCore {
    pub fn new(config: ProtocolCoreConfig) -> Self {
        Self {
            state: ClientState::Disconnected,
            config,
            last_connect_attempt: None,
            last_reconnection_attempt: None,
            retry_count: 0,
            connection_retry_count: 0,
            next_request_id: 1,
            pending_sends: VecDeque::new(),
            sent_order: VecDeque::new(),
            auth_pending: false,
            auth_request_id: None,
            server_address: None,
            last_auth_result: None,
            connection_established: false,
        }
    }

    pub fn poll_transmit(&mut self) -> Option<TxBuf> {
        if let Some((code, payload, request_id)) = self.pending_sends.pop_front() {
            let total_len = (payload.len() + REQUEST_INITIAL_BYTES_LENGTH) as u32;
            self.sent_order.push_back(request_id);

            Some(TxBuf {
                payload,
                header: make_header(total_len, code),
                request_id,
            })
        } else {
            None
        }
    }

    pub fn send(&mut self, code: u32, payload: Bytes) -> Result<u64, IggyError> {
        match self.state {
            ClientState::Shutdown => Err(IggyError::ClientShutdown),
            ClientState::Disconnected | ClientState::Connecting => Err(IggyError::NotConnected),
            ClientState::Connected | ClientState::Authenticating | ClientState::Authenticated => {
                Ok(self.queue_send(code, payload))
            }
        }
    }

    fn queue_send(&mut self, code: u32, payload: Bytes) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        self.pending_sends.push_back((code, payload, request_id));
        request_id
    }

    pub fn process_incoming_with<F: FnMut(u64, u32, Bytes)>(
        &mut self,
        buf: &mut BytesMut,
        mut f: F,
    ) {
        loop {
            if buf.len() < RESPONSE_INITIAL_BYTES_LENGTH {
                break;
            }
            let status = u32::from_le_bytes(buf[..4].try_into().unwrap());
            let length = u32::from_le_bytes(buf[4..8].try_into().unwrap());
            let total = RESPONSE_INITIAL_BYTES_LENGTH + length as usize;
            if buf.len() < total {
                break;
            }

            buf.advance(RESPONSE_INITIAL_BYTES_LENGTH);
            let payload = if length <= 1 {
                Bytes::new()
            } else {
                buf.split_to(length as usize).freeze()
            };
            
            // Handle response status logging
            if status != 0 {
                self.handle_error_response(status);
            }
            
            if let Some(id) = self.on_response(status) {
                f(id, status, payload);
            }
        }
    }

    /// Handle error response logging based on status code
    fn handle_error_response(&self, status: u32) {
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
            debug!(
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
    }

    pub fn on_response(&mut self, status: u32) -> Option<u64> {
        let request_id = self.sent_order.pop_front()?;

        if Some(request_id) == self.auth_request_id {
            if status == 0 {
                debug!("Authentication successful");
                self.state = ClientState::Authenticated;
                self.auth_pending = false;
                self.last_auth_result = Some(Ok(()));
            } else {
                warn!("Authentication failed with status: {}", status);
                self.state = ClientState::Connected;
                self.auth_pending = false;
                self.last_auth_result = Some(Err(IggyError::Unauthenticated));
            }
            self.auth_request_id = None;
        }

        Some(request_id)
    }

    pub fn poll(&mut self) -> ControlAction {
        match self.state {
            ClientState::Shutdown => ControlAction::Error(IggyError::ClientShutdown),
            ClientState::Disconnected => ControlAction::Noop,
            ClientState::Authenticated | ClientState::Authenticating | ClientState::Connected => {
                ControlAction::Noop
            }
            ClientState::Connecting => {
                let server_address = match self.server_address {
                    Some(addr) => addr,
                    None => return ControlAction::Error(IggyError::ConnectionMissedSocket),
                };

                // Check if reconnection is enabled
                if !self.config.reconnection_enabled {
                    if self.connection_retry_count > 0 {
                        warn!("Automatic reconnection is disabled.");
                        return ControlAction::Error(IggyError::CannotEstablishConnection);
                    }
                }

                // Handle timing for reestablish_after (initial connect timing)
                if let Some(last_reconnection) = self.last_reconnection_attempt {
                    let now = IggyTimestamp::now();
                    let elapsed = now.as_micros().saturating_sub(last_reconnection.as_micros());
                    if elapsed < self.config.reestablish_after.as_micros() {
                        let remaining = IggyDuration::from(self.config.reestablish_after.as_micros() - elapsed);
                        info!(
                            "Trying to connect to the server in: {remaining}",
                            remaining = remaining.as_human_time_string()
                        );
                        return ControlAction::Wait(remaining);
                    }
                }

                // Handle timing for reconnection interval (between retries)
                if let Some(last_attempt) = self.last_connect_attempt {
                    let now = IggyTimestamp::now();
                    let elapsed = now.as_micros().saturating_sub(last_attempt.as_micros());
                    if elapsed < self.config.reconnection_interval.as_micros() {
                        let remaining = IggyDuration::from(self.config.reconnection_interval.as_micros() - elapsed);
                        return ControlAction::Wait(remaining);
                    }
                }

                // Check retry limits
                let unlimited_retries = self.config.max_retries.is_none();
                let max_retries = self.config.max_retries.unwrap_or_default();
                
                if !unlimited_retries && self.connection_retry_count >= max_retries {
                    error!(
                        "Maximum retry attempts ({}) exceeded for server: {:?}",
                        max_retries, server_address
                    );
                    return ControlAction::Error(IggyError::MaxRetriesExceeded);
                }

                // Increment retry count and log attempt
                self.connection_retry_count += 1;
                self.last_connect_attempt = Some(IggyTimestamp::now());

                let max_retries_str = if let Some(max_retries) = self.config.max_retries {
                    max_retries.to_string()
                } else {
                    "unlimited".to_string()
                };

                if self.connection_retry_count > 1 {
                    let interval_str = self.config.reconnection_interval.as_human_time_string();
                    info!(
                        "Retrying to connect to server ({}/{max_retries_str}): {server_address:?} in: {interval_str}",
                        self.connection_retry_count,
                        max_retries_str = max_retries_str,
                        server_address = server_address,
                        interval_str = interval_str
                    );
                } else {
                    info!(
                        "Iggy client is connecting to server: {server_address:?}...",
                        server_address = server_address
                    );
                }

                ControlAction::Connect(server_address)
            }
        }
    }

    pub fn desire_connect(&mut self, server_address: SocketAddr) -> Result<(), IggyError> {
        match self.state {
            ClientState::Shutdown => return Err(IggyError::ClientShutdown),
            ClientState::Connecting => return Ok(()),
            ClientState::Connected | ClientState::Authenticating | ClientState::Authenticated => {
                return Ok(());
            }
            _ => {
                self.state = ClientState::Connecting;
                self.server_address = Some(server_address);
            }
        }

        Ok(())
    }

    pub fn on_connected(&mut self) -> Result<(), IggyError> {
        debug!("Transport connected");
        if self.state != ClientState::Connecting {
            return Err(IggyError::IncorrectConnectionState);
        }
        self.state = ClientState::Connected;
        self.connection_established = true;
        // Reset retry counters on successful connection
        self.retry_count = 0;
        self.connection_retry_count = 0;

        match &self.config.auto_login {
            AutoLogin::Disabled => {
                info!("Automatic sign-in is disabled.");
            }
            AutoLogin::Enabled(credentials) => {
                if !self.auth_pending {
                    self.state = ClientState::Authenticating;
                    self.auth_pending = true;

                    match credentials {
                        Credentials::UsernamePassword(username, password) => {
                            let auth_payload = encode_auth(&username, &password);
                            let auth_id = self.queue_send(0x0A, auth_payload);
                            self.auth_request_id = Some(auth_id);
                        }
                        Credentials::PersonalAccessToken(token) => {
                            let auth_payload = encode_pat_auth(token);
                            let auth_id = self.queue_send(0x0B, auth_payload); // Different command code for PAT
                            self.auth_request_id = Some(auth_id);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn disconnect(&mut self) {
        debug!("Transport disconnected");
        self.state = ClientState::Disconnected;
        self.auth_pending = false;
        self.auth_request_id = None;
        self.sent_order.clear();
        
        // Set last reconnection attempt time if we had an established connection
        if self.connection_established {
            self.last_reconnection_attempt = Some(IggyTimestamp::now());
            self.connection_established = false;
        }
    }

    pub fn shutdown(&mut self) {
        self.state = ClientState::Shutdown;
        self.auth_pending = false;
        self.auth_request_id = None;
        self.sent_order.clear();
    }

    pub fn should_wait_auth(&self) -> bool {
        matches!(self.config.auto_login, AutoLogin::Enabled(_)) && self.auth_pending
    }

    pub fn take_auth_result(&mut self) -> Option<Result<(), IggyError>> {
        self.last_auth_result.take()
    }

    /// Check if we should attempt reconnection for the given error
    pub fn should_reconnect_for_error(&self, error: &IggyError) -> bool {
        if !self.config.reconnection_enabled {
            return false;
        }

        matches!(
            error,
            IggyError::Disconnected
                | IggyError::EmptyResponse
                | IggyError::Unauthenticated
                | IggyError::StaleClient
        )
    }

    /// Initiate reconnection process
    pub fn initiate_reconnection(&mut self, server_address: SocketAddr) -> Result<(), IggyError> {
        info!(
            "Reconnecting to the server: {server_address:?} by client...",
            server_address = server_address
        );
        
        self.disconnect();
        self.desire_connect(server_address)
    }

    /// Get current retry information for logging
    pub fn get_retry_info(&self) -> (u32, String, String) {
        let max_retries_str = if let Some(max_retries) = self.config.max_retries {
            max_retries.to_string()
        } else {
            "unlimited".to_string()
        };
        
        let interval_str = self.config.reconnection_interval.as_human_time_string();
        
        (self.connection_retry_count, max_retries_str, interval_str)
    }
}

fn encode_auth(username: &str, password: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u32_le(username.len() as u32);
    buf.put_slice(username.as_bytes());
    buf.put_u32_le(password.len() as u32);
    buf.put_slice(password.as_bytes());
    buf.freeze()
}

fn encode_pat_auth(token: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u32_le(token.len() as u32);
    buf.put_slice(token.as_bytes());
    buf.freeze()
}

fn make_header(total_len: u32, code: u32) -> [u8; 8] {
    let mut h = [0u8; 8];
    h[..4].copy_from_slice(&total_len.to_le_bytes());
    h[4..].copy_from_slice(&code.to_le_bytes());
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn create_test_config() -> ProtocolCoreConfig {
        ProtocolCoreConfig {
            auto_login: AutoLogin::Disabled,
            reestablish_after: IggyDuration::from_str("5s").unwrap(),
            max_retries: Some(3),
            reconnection_enabled: true,
            reconnection_interval: IggyDuration::from_str("1s").unwrap(),
        }
    }

    #[test]
    fn test_protocol_core_creation() {
        let config = create_test_config();
        let core = ProtocolCore::new(config);
        
        assert_eq!(core.state, ClientState::Disconnected);
        assert_eq!(core.retry_count, 0);
        assert_eq!(core.connection_retry_count, 0);
        assert_eq!(core.next_request_id, 1);
        assert!(core.pending_sends.is_empty());
        assert!(core.sent_order.is_empty());
        assert!(!core.auth_pending);
        assert!(core.auth_request_id.is_none());
        assert!(core.server_address.is_none());
        assert!(core.last_auth_result.is_none());
        assert!(!core.connection_established);
    }

    #[test]
    fn test_state_transitions_connecting() {
        let config = create_test_config();
        let mut core = ProtocolCore::new(config);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        // Test desire_connect
        core.desire_connect(addr).unwrap();
        assert_eq!(core.state, ClientState::Connecting);
        assert_eq!(core.server_address, Some(addr));
    }

    #[test]
    fn test_on_connected_success() {
        let config = create_test_config();
        let mut core = ProtocolCore::new(config);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        core.desire_connect(addr).unwrap();
        core.on_connected().unwrap();
        
        assert_eq!(core.state, ClientState::Connected);
        assert_eq!(core.retry_count, 0);
        assert_eq!(core.connection_retry_count, 0);
        assert!(core.connection_established);
    }

    #[test]
    fn test_on_connected_wrong_state() {
        let config = create_test_config();
        let mut core = ProtocolCore::new(config);
        
        // Try to connect without being in Connecting state
        let result = core.on_connected();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), IggyError::IncorrectConnectionState);
    }

    #[test]
    fn test_disconnect() {
        let config = create_test_config();
        let mut core = ProtocolCore::new(config);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        core.desire_connect(addr).unwrap();
        core.on_connected().unwrap();
        
        core.disconnect();
        
        assert_eq!(core.state, ClientState::Disconnected);
        assert!(!core.auth_pending);
        assert!(core.auth_request_id.is_none());
        assert!(core.sent_order.is_empty());
        assert!(!core.connection_established);
        assert!(core.last_reconnection_attempt.is_some());
    }

    #[test]
    fn test_shutdown() {
        let config = create_test_config();
        let mut core = ProtocolCore::new(config);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        core.desire_connect(addr).unwrap();
        core.on_connected().unwrap();
        
        core.shutdown();
        
        assert_eq!(core.state, ClientState::Shutdown);
        assert!(!core.auth_pending);
        assert!(core.auth_request_id.is_none());
    }

    #[test]
    fn test_retry_logic_with_limits() {
        let config = ProtocolCoreConfig {
            auto_login: AutoLogin::Disabled,
            reestablish_after: IggyDuration::from_str("0ms").unwrap(), // No wait
            max_retries: Some(2),
            reconnection_enabled: true,
            reconnection_interval: IggyDuration::from_str("0ms").unwrap(), // No wait
        };
        let mut core = ProtocolCore::new(config);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        core.desire_connect(addr).unwrap();
        
        // First poll should return Connect
        let action = core.poll();
        assert!(matches!(action, ControlAction::Connect(_)));
        assert_eq!(core.connection_retry_count, 1);
        
        // Simulate connection failure
        core.disconnect();
        core.desire_connect(addr).unwrap();
        
        // Second poll should return Connect
        let action = core.poll();
        assert!(matches!(action, ControlAction::Connect(_)));
        assert_eq!(core.connection_retry_count, 2);
        
        // Simulate connection failure
        core.disconnect();
        core.desire_connect(addr).unwrap();
        
        // Third poll should return Error due to max retries exceeded
        let action = core.poll();
        assert!(matches!(action, ControlAction::Error(IggyError::MaxRetriesExceeded)));
    }

    #[test]
    fn test_retry_logic_unlimited() {
        let config = ProtocolCoreConfig {
            auto_login: AutoLogin::Disabled,
            reestablish_after: IggyDuration::from_str("0ms").unwrap(),
            max_retries: None, // Unlimited
            reconnection_enabled: true,
            reconnection_interval: IggyDuration::from_str("0ms").unwrap(),
        };
        let mut core = ProtocolCore::new(config);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        core.desire_connect(addr).unwrap();
        
        // Should always allow retries
        for i in 1..=10 {
            let action = core.poll();
            assert!(matches!(action, ControlAction::Connect(_)));
            assert_eq!(core.connection_retry_count, i);
            
            core.disconnect();
            core.desire_connect(addr).unwrap();
        }
    }

    #[test]
    fn test_reconnection_disabled() {
        let config = ProtocolCoreConfig {
            auto_login: AutoLogin::Disabled,
            reestablish_after: IggyDuration::from_str("0ms").unwrap(),
            max_retries: Some(3),
            reconnection_enabled: false,
            reconnection_interval: IggyDuration::from_str("1s").unwrap(),
        };
        let mut core = ProtocolCore::new(config);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        core.desire_connect(addr).unwrap();
        
        // First connection attempt should work
        let action = core.poll();
        assert!(matches!(action, ControlAction::Connect(_)));
        
        // Simulate failure and retry
        core.disconnect();
        core.desire_connect(addr).unwrap();
        
        // Should fail immediately when reconnection is disabled
        let action = core.poll();
        assert!(matches!(action, ControlAction::Error(IggyError::CannotEstablishConnection)));
    }

    #[test]
    fn test_should_reconnect_for_error() {
        let config = create_test_config();
        let core = ProtocolCore::new(config);
        
        // Should reconnect for these errors
        assert!(core.should_reconnect_for_error(&IggyError::Disconnected));
        assert!(core.should_reconnect_for_error(&IggyError::EmptyResponse));
        assert!(core.should_reconnect_for_error(&IggyError::Unauthenticated));
        assert!(core.should_reconnect_for_error(&IggyError::StaleClient));
        
        // Should not reconnect for other errors
        assert!(!core.should_reconnect_for_error(&IggyError::InvalidCommand));
        assert!(!core.should_reconnect_for_error(&IggyError::StreamIdNotFound(1)));
    }

    #[test]
    fn test_should_reconnect_disabled() {
        let config = ProtocolCoreConfig {
            auto_login: AutoLogin::Disabled,
            reestablish_after: IggyDuration::from_str("5s").unwrap(),
            max_retries: Some(3),
            reconnection_enabled: false,
            reconnection_interval: IggyDuration::from_str("1s").unwrap(),
        };
        let core = ProtocolCore::new(config);
        
        // Should not reconnect when reconnection is disabled
        assert!(!core.should_reconnect_for_error(&IggyError::Disconnected));
        assert!(!core.should_reconnect_for_error(&IggyError::EmptyResponse));
    }

    #[test]
    fn test_initiate_reconnection() {
        let config = create_test_config();
        let mut core = ProtocolCore::new(config);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        // First establish connection
        core.desire_connect(addr).unwrap();
        core.on_connected().unwrap();
        
        // Initiate reconnection
        core.initiate_reconnection(addr).unwrap();
        
        assert_eq!(core.state, ClientState::Connecting);
        assert_eq!(core.server_address, Some(addr));
    }

    #[test]
    fn test_queue_send_and_request_id() {
        let config = create_test_config();
        let mut core = ProtocolCore::new(config);
        
        let payload = Bytes::from("test message");
        let request_id = core.queue_send(0x10, payload.clone());
        
        assert_eq!(request_id, 1);
        assert_eq!(core.next_request_id, 2);
        assert_eq!(core.pending_sends.len(), 1);
        
        let (code, queued_payload, queued_id) = &core.pending_sends[0];
        assert_eq!(*code, 0x10);
        assert_eq!(*queued_payload, payload);
        assert_eq!(*queued_id, request_id);
    }

    #[test]
    fn test_poll_transmit() {
        let config = create_test_config();
        let mut core = ProtocolCore::new(config);
        
        // Queue a message
        let payload = Bytes::from("test");
        core.queue_send(0x10, payload);
        
        // Poll should return the queued message
        let tx_buf = core.poll_transmit();
        assert!(tx_buf.is_some());
        
        let tx = tx_buf.unwrap();
        // Extract command code from header (last 4 bytes)
        let command_code = u32::from_le_bytes(tx.header[4..8].try_into().unwrap());
        assert_eq!(command_code, 0x10);
        assert_eq!(tx.payload, Bytes::from("test"));
        
        // Should be empty after polling
        assert!(core.poll_transmit().is_none());
    }

    #[test] 
    fn test_auth_with_username_password() {
        let config = ProtocolCoreConfig {
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                "user".to_string(),
                "pass".to_string(),
            )),
            reestablish_after: IggyDuration::from_str("5s").unwrap(),
            max_retries: Some(3),
            reconnection_enabled: true,
            reconnection_interval: IggyDuration::from_str("1s").unwrap(),
        };
        let mut core = ProtocolCore::new(config);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        core.desire_connect(addr).unwrap();
        core.on_connected().unwrap();
        
        assert_eq!(core.state, ClientState::Authenticating);
        assert!(core.auth_pending);
        assert!(core.auth_request_id.is_some());
        assert!(!core.pending_sends.is_empty());
    }

    #[test]
    fn test_auth_with_pat() {
        let config = ProtocolCoreConfig {
            auto_login: AutoLogin::Enabled(Credentials::PersonalAccessToken(
                "test-token".to_string(),
            )),
            reestablish_after: IggyDuration::from_str("5s").unwrap(),
            max_retries: Some(3),
            reconnection_enabled: true,
            reconnection_interval: IggyDuration::from_str("1s").unwrap(),
        };
        let mut core = ProtocolCore::new(config);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        core.desire_connect(addr).unwrap();
        core.on_connected().unwrap();
        
        assert_eq!(core.state, ClientState::Authenticating);
        assert!(core.auth_pending);
        assert!(core.auth_request_id.is_some());
        assert!(!core.pending_sends.is_empty());
    }

    #[test]
    fn test_get_retry_info() {
        let config = ProtocolCoreConfig {
            auto_login: AutoLogin::Disabled,
            reestablish_after: IggyDuration::from_str("5s").unwrap(),
            max_retries: Some(5),
            reconnection_enabled: true,
            reconnection_interval: IggyDuration::from_str("2s").unwrap(),
        };
        let mut core = ProtocolCore::new(config);
        
        core.connection_retry_count = 3;
        let (count, max_str, interval_str) = core.get_retry_info();
        
        assert_eq!(count, 3);
        assert_eq!(max_str, "5");
        assert_eq!(interval_str, "2s");
    }

    #[test]
    fn test_get_retry_info_unlimited() {
        let config = ProtocolCoreConfig {
            auto_login: AutoLogin::Disabled,
            reestablish_after: IggyDuration::from_str("5s").unwrap(),
            max_retries: None,
            reconnection_enabled: true,
            reconnection_interval: IggyDuration::from_str("1s").unwrap(),
        };
        let core = ProtocolCore::new(config);
        
        let (_, max_str, _) = core.get_retry_info();
        assert_eq!(max_str, "unlimited");
    }
}
