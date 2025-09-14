use std::{
    collections::VecDeque,
    net::SocketAddr,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy_common::{AutoLogin, ClientState, Credentials, IggyDuration, IggyError, IggyTimestamp};
use tracing::{debug, info, warn};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const REQUEST_HEADER_BYTES: usize = 8;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
#[derive(Debug)]
pub struct ProtocolCoreConfig {
    pub auto_login: AutoLogin,
    pub reestablish_after: IggyDuration,
    pub max_retries: Option<u32>,
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
    pub retry_count: u32,
    next_request_id: u64,
    pending_sends: VecDeque<(u32, Bytes, u64)>,
    sent_order: VecDeque<u64>,
    auth_pending: bool,
    auth_request_id: Option<u64>,
    server_address: Option<SocketAddr>,
    last_auth_result: Option<Result<(), IggyError>>,
}

impl ProtocolCore {
    pub fn new(config: ProtocolCoreConfig) -> Self {
        Self {
            state: ClientState::Disconnected,
            config,
            last_connect_attempt: None,
            retry_count: 0,
            next_request_id: 1,
            pending_sends: VecDeque::new(),
            sent_order: VecDeque::new(),
            auth_pending: false,
            auth_request_id: None,
            server_address: None,
            last_auth_result: None,
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
            if let Some(id) = self.on_response(status) {
                f(id, status, payload);
            }
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

                if let Some(last) = self.last_connect_attempt {
                    let now = IggyTimestamp::now();
                    let elapsed = now.as_micros().saturating_sub(last.as_micros());
                    if elapsed < self.config.reestablish_after.as_micros() {
                        let remaining =
                            IggyDuration::from(self.config.reestablish_after.as_micros() - elapsed);
                        return ControlAction::Wait(remaining);
                    }
                }

                if let Some(max_retries) = self.config.max_retries {
                    if self.retry_count >= max_retries {
                        return ControlAction::Error(IggyError::MaxRetriesExceeded);
                    }
                }

                self.retry_count += 1;
                self.last_connect_attempt = Some(IggyTimestamp::now());

                return ControlAction::Connect(server_address);
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
        self.retry_count = 0;

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
                        _ => {
                            todo!("add PersonalAccessToken")
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
}

fn encode_auth(username: &str, password: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u32_le(username.len() as u32);
    buf.put_slice(username.as_bytes());
    buf.put_u32_le(password.len() as u32);
    buf.put_slice(password.as_bytes());
    buf.freeze()
}

fn make_header(total_len: u32, code: u32) -> [u8; 8] {
    let mut h = [0u8; 8];
    h[..4].copy_from_slice(&total_len.to_le_bytes());
    h[4..].copy_from_slice(&code.to_le_bytes());
    h
}
