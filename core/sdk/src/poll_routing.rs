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

//! Auto-commit polls use persistent data connections while the coordinator
//! connection retains group membership. Only explicit non-admission is retried:
//! a poll has no deduplication key, even when retried on the same connection.
//! Cluster routing requires servers supporting the routing and attachment commands.

use crate::leader_aware::{node_address, transport_port};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::codes::{
    ATTACH_CONSUMER_SESSION_CODE, GET_POLL_ROUTING_CODE, PING_CODE, POLL_MESSAGES_ON_PRIMARY_CODE,
};
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::system::AttachConsumerSessionRequest;
use iggy_binary_protocol::responses::messages::PollRoutingResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::{
    BinaryClient, ClusterNode, Credentials, IdKind, Identifier, IggyError, TransportProtocol,
};
use secrecy::SecretString;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{Instant, sleep, timeout_at};

const MAX_CACHED_ROUTES: usize = 4096;
const MAX_DATA_CONNECTIONS: usize = 256;
const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const ROUTING_RETRY_INTERVAL: Duration = Duration::from_millis(50);

#[async_trait]
pub(crate) trait PollTransport: BinaryClient + Send + Sync + Sized {
    const PROTOCOL: TransportProtocol;

    async fn connect_poll_client(&self, endpoint: &str) -> Result<Self, IggyError>;

    /// One exchange on this connection, with no node movement or automatic
    /// replay of an ambiguous poll outcome.
    async fn send_poll_request(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError>;

    async fn send_poll_control(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        let result = self.send_poll_request(code, payload.clone()).await;
        if result.as_ref().is_err_and(poll_connection_failed) {
            self.send_raw_with_response(PING_CODE, Bytes::new()).await?;
            return self.send_poll_request(code, payload).await;
        }
        result
    }
}

#[derive(Debug)]
struct PollRoute {
    endpoint: String,
    consumer_session: AttachConsumerSessionRequest,
}

#[derive(Debug)]
struct PollConnection<T> {
    client: T,
    consumer_session: Option<AttachConsumerSessionRequest>,
    usable: bool,
}

type ConnectionSlot<T> = Arc<AsyncMutex<Option<PollConnection<T>>>>;

#[derive(Debug)]
pub(crate) struct PollRouter<T> {
    pub(crate) metadata_watermark: Arc<AtomicU64>,
    routes: Mutex<HashMap<Bytes, Arc<PollRoute>>>,
    connections: Mutex<HashMap<String, ConnectionSlot<T>>>,
    credentials: Mutex<Option<(Credentials, u32)>>,
    next_heartbeat: Mutex<Option<Instant>>,
}

impl<T> Default for PollRouter<T> {
    fn default() -> Self {
        Self {
            metadata_watermark: Arc::default(),
            routes: Mutex::default(),
            connections: Mutex::default(),
            credentials: Mutex::default(),
            next_heartbeat: Mutex::default(),
        }
    }
}

impl<T> PollRouter<T> {
    pub(crate) fn clear_session(&self) {
        self.next_heartbeat
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    pub(crate) fn remember_credentials(&self, credentials: Credentials, user_id: u32) {
        *self
            .credentials
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((credentials, user_id));
    }

    pub(crate) fn forget_credentials(&self) {
        self.credentials
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    pub(crate) fn credentials(&self) -> Option<Credentials> {
        self.credentials
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|(credentials, _)| credentials.clone())
    }

    pub(crate) fn refresh_password(&self, user: &Identifier, new_password: &str) {
        let mut credentials = self
            .credentials
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some((Credentials::UsernamePassword(username, password), user_id)) =
            credentials.as_mut()
        else {
            return;
        };
        if matches_session_user(user, *user_id, username) {
            *password = SecretString::from(new_password.to_owned());
        }
    }

    pub(crate) fn refresh_username(&self, user: &Identifier, new_username: &str) {
        let mut credentials = self
            .credentials
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some((Credentials::UsernamePassword(username, _), user_id)) = credentials.as_mut()
        else {
            return;
        };
        if matches_session_user(user, *user_id, username) {
            *username = new_username.to_owned();
        }
    }
}

impl<T: PollTransport> PollRouter<T> {
    pub(crate) async fn poll(
        &self,
        coordinator: &T,
        request: &PollMessagesRequest,
    ) -> Result<Bytes, IggyError> {
        let payload = request.to_bytes();
        let parameters_size = request.strategy.encoded_size() + size_of::<u32>() + size_of::<u8>();
        let key = payload.slice(..payload.len() - parameters_size);
        let now = Instant::now();
        let deadline = now + POLL_TIMEOUT;
        let result = timeout_at(deadline, async {
            let heartbeat_due = {
                let mut next = self
                    .next_heartbeat
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let due = next.is_some_and(|next| now >= next);
                if next.is_none() || due {
                    *next = Some(now + coordinator.get_heartbeat_interval().get_duration());
                }
                due
            };
            if heartbeat_due {
                coordinator
                    .send_poll_control(PING_CODE, Bytes::new())
                    .await?;
            }
            loop {
                let result = self.poll_once(coordinator, &key, &payload).await;
                if !matches!(result, Err(IggyError::TransientNotAccepted)) {
                    return result;
                }
                self.routes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&key);
                if Instant::now() + ROUTING_RETRY_INTERVAL >= deadline {
                    return Err(IggyError::TransientNotAccepted);
                }
                sleep(ROUTING_RETRY_INTERVAL).await;
            }
        })
        .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                self.routes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&key);
                Err(IggyError::TransientNotCommitted)
            }
        }
    }

    async fn poll_once(
        &self,
        coordinator: &T,
        key: &Bytes,
        payload: &Bytes,
    ) -> Result<Bytes, IggyError> {
        let route = self.route(coordinator, key, payload).await?;
        let slot = {
            let mut connections = self
                .connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(slot) = connections.get(&route.endpoint) {
                Arc::clone(slot)
            } else {
                if connections.len() >= MAX_DATA_CONNECTIONS {
                    return Err(IggyError::InvalidConfiguration);
                }
                let slot = Arc::default();
                connections.insert(route.endpoint.clone(), Arc::clone(&slot));
                slot
            }
        };
        let mut connection = slot.lock().await;
        if connection
            .as_ref()
            .is_some_and(|connection| !connection.usable)
        {
            connection.take();
        }
        if connection.is_none() {
            let client = match coordinator.connect_poll_client(&route.endpoint).await {
                Ok(client) => client,
                Err(error) if poll_connection_failed(&error) => {
                    return Err(IggyError::TransientNotAccepted);
                }
                Err(error) => return Err(error),
            };
            client
                .send_poll_request(
                    ATTACH_CONSUMER_SESSION_CODE,
                    route.consumer_session.to_bytes(),
                )
                .await
                .map_err(unaccepted_data_error)?;
            *connection = Some(PollConnection {
                client,
                consumer_session: Some(route.consumer_session),
                usable: true,
            });
        }
        let Some(connection) = connection.as_mut() else {
            return Err(IggyError::NotConnected);
        };
        // A canceled exchange must not leave a pooled connection reusable while
        // its detached transport task can still be reading the previous reply.
        connection.usable = false;
        if connection.consumer_session.is_none_or(|session| {
            session.client_id != route.consumer_session.client_id
                || session.session != route.consumer_session.session
                || session.metadata_watermark < route.consumer_session.metadata_watermark
        }) {
            connection
                .client
                .send_poll_request(
                    ATTACH_CONSUMER_SESSION_CODE,
                    route.consumer_session.to_bytes(),
                )
                .await
                .map_err(unaccepted_data_error)?;
            connection.consumer_session = Some(route.consumer_session);
        }
        let result = connection
            .client
            .send_poll_request(POLL_MESSAGES_ON_PRIMARY_CODE, payload.clone())
            .await;
        connection.usable = matches!(result, Ok(_) | Err(IggyError::TransientNotAccepted));
        if matches!(result, Err(IggyError::TransientNotAccepted)) {
            connection.consumer_session = None;
        }
        if !connection.usable {
            self.routes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(key);
        }
        result.map_err(|error| {
            if poll_connection_failed(&error) {
                IggyError::TransientNotCommitted
            } else {
                error
            }
        })
    }

    async fn route(
        &self,
        coordinator: &T,
        key: &Bytes,
        payload: &Bytes,
    ) -> Result<Arc<PollRoute>, IggyError> {
        if let Some(route) = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .filter(|route| {
                route.consumer_session.metadata_watermark
                    >= self.metadata_watermark.load(Ordering::Acquire)
            })
        {
            return Ok(Arc::clone(route));
        }
        let response = coordinator
            .send_poll_control(GET_POLL_ROUTING_CODE, payload.clone())
            .await?;
        let mut response =
            PollRoutingResponse::decode_from(&response).map_err(|_| IggyError::InvalidCommand)?;
        response.consumer_session.metadata_watermark = response
            .consumer_session
            .metadata_watermark
            .max(self.metadata_watermark.load(Ordering::Acquire));
        let node = ClusterNode::try_from(response.primary)?;
        let port = transport_port(&node, T::PROTOCOL);
        if port == 0 {
            return Err(IggyError::FeatureUnavailable);
        }
        let route = Arc::new(PollRoute {
            endpoint: node_address(&node, port),
            consumer_session: response.consumer_session,
        });
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if routes.len() >= MAX_CACHED_ROUTES {
            routes.clear();
        }
        routes.insert(key.clone(), Arc::clone(&route));
        Ok(route)
    }
}

pub(crate) fn matches_session_user(user: &Identifier, user_id: u32, username: &str) -> bool {
    match user.kind {
        IdKind::Numeric => user.get_u32_value().is_ok_and(|id| id == user_id),
        IdKind::String => user
            .get_cow_str_value()
            .is_ok_and(|name| name.as_ref() == username),
    }
}

fn poll_connection_failed(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::Disconnected
            | IggyError::NotConnected
            | IggyError::CannotEstablishConnection
            | IggyError::EmptyResponse
            | IggyError::TcpError
            | IggyError::QuicError
            | IggyError::ConnectionClosed
            | IggyError::WebSocketSendError
            | IggyError::WebSocketReceiveError
            | IggyError::Unauthenticated
            | IggyError::StaleClient
    )
}

fn unaccepted_data_error(error: IggyError) -> IggyError {
    if poll_connection_failed(&error) {
        IggyError::TransientNotAccepted
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::responses::system::get_cluster_metadata::ClusterNodeResponse;
    use iggy_binary_protocol::{
        Command, HEADER_SIZE, Operation, ReplyHeader, WireConsumer, WireIdentifier,
        WirePollingStrategy,
    };
    use iggy_common::{
        BinaryTransport, Client, ClientState, ConsumerGroupClientState, DiagnosticEvent,
        NonZeroIggyDuration, VsrSessionControl, VsrSessionSealed,
    };
    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Channel {
        Coordinator,
        Data,
        Recovery,
    }

    type Exchange = (Channel, u32, Result<Bytes, IggyError>);

    #[derive(Debug, Default)]
    struct Script {
        exchanges: Mutex<VecDeque<Exchange>>,
        attachments: Mutex<Vec<u64>>,
        connections: AtomicUsize,
    }

    #[derive(Debug)]
    struct Transport {
        channel: Channel,
        script: Arc<Script>,
    }

    impl Transport {
        fn exchange(
            &self,
            channel: Channel,
            code: u32,
            payload: &Bytes,
        ) -> Result<Bytes, IggyError> {
            let (expected_channel, expected_code, result) = self
                .script
                .exchanges
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request");
            assert_eq!((channel, code), (expected_channel, expected_code));
            if code == ATTACH_CONSUMER_SESSION_CODE {
                self.script.attachments.lock().unwrap().push(
                    AttachConsumerSessionRequest::decode_from(payload)
                        .unwrap()
                        .metadata_watermark,
                );
            }
            result
        }
    }

    #[async_trait]
    impl Client for Transport {
        async fn connect(&self) -> Result<(), IggyError> {
            Ok(())
        }
        async fn disconnect(&self) -> Result<(), IggyError> {
            Ok(())
        }
        async fn shutdown(&self) -> Result<(), IggyError> {
            Ok(())
        }
        async fn subscribe_events(&self) -> async_broadcast::Receiver<DiagnosticEvent> {
            async_broadcast::broadcast(1).1
        }
    }

    impl BinaryClient for Transport {}
    impl VsrSessionSealed for Transport {}

    #[async_trait]
    impl VsrSessionControl for Transport {
        async fn bind_vsr_session(&self, _session: u64) -> Result<(), IggyError> {
            Ok(())
        }
        async fn reset_vsr_session(&self) -> Result<(), IggyError> {
            Ok(())
        }
        fn sdk_version(&self) -> &'static str {
            crate::SDK_VERSION
        }
    }

    #[async_trait]
    impl BinaryTransport for Transport {
        async fn get_state(&self) -> ClientState {
            ClientState::Authenticated
        }
        async fn set_state(&self, _state: ClientState) {}
        async fn publish_event(&self, _event: DiagnosticEvent) {}
        async fn send_raw_with_response(
            &self,
            code: u32,
            payload: Bytes,
        ) -> Result<Bytes, IggyError> {
            self.exchange(Channel::Recovery, code, &payload)
        }
        fn get_heartbeat_interval(&self) -> NonZeroIggyDuration {
            NonZeroIggyDuration::from_str("5s").unwrap()
        }
        fn consumer_group_state(&self) -> Arc<ConsumerGroupClientState> {
            Arc::default()
        }
    }

    #[async_trait]
    impl PollTransport for Transport {
        const PROTOCOL: TransportProtocol = TransportProtocol::Tcp;
        async fn connect_poll_client(&self, _endpoint: &str) -> Result<Self, IggyError> {
            self.script.connections.fetch_add(1, Ordering::Relaxed);
            Ok(Self {
                channel: Channel::Data,
                script: Arc::clone(&self.script),
            })
        }
        async fn send_poll_request(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
            self.exchange(self.channel, code, &payload)
        }
    }

    #[tokio::test]
    async fn acknowledged_metadata_changes_refresh_cached_routes_and_attachments() {
        let (router, coordinator, request) = fixture([
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (
                Channel::Data,
                ATTACH_CONSUMER_SESSION_CODE,
                Ok(Bytes::new()),
            ),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Ok(Bytes::from_static(b"first")),
            ),
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (
                Channel::Data,
                ATTACH_CONSUMER_SESSION_CODE,
                Ok(Bytes::new()),
            ),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Ok(Bytes::from_static(b"after metadata")),
            ),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Ok(Bytes::from_static(b"warm")),
            ),
        ]);
        assert_eq!(router.poll(&coordinator, &request).await.unwrap(), "first");
        let metadata_reply = ReplyHeader {
            command: Command::Reply,
            operation: Operation::PurgeTopic,
            size: u32::try_from(HEADER_SIZE).unwrap(),
            commit: 11,
            ..Default::default()
        };
        crate::vsr::observe_metadata_reply(
            &router.metadata_watermark,
            bytemuck::bytes_of(&metadata_reply).try_into().unwrap(),
        );
        assert_eq!(
            router.poll(&coordinator, &request).await.unwrap(),
            "after metadata"
        );
        assert_eq!(router.poll(&coordinator, &request).await.unwrap(), "warm");
        assert_eq!(*coordinator.script.attachments.lock().unwrap(), [1, 11]);
        assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 1);
        assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn broken_control_recovers_and_lost_data_reply_returns_without_replaying_the_poll() {
        let (router, coordinator, request) = fixture([
            (
                Channel::Coordinator,
                GET_POLL_ROUTING_CODE,
                Err(IggyError::Disconnected),
            ),
            (Channel::Recovery, PING_CODE, Ok(Bytes::new())),
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (
                Channel::Data,
                ATTACH_CONSUMER_SESSION_CODE,
                Ok(Bytes::new()),
            ),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Err(IggyError::Disconnected),
            ),
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (
                Channel::Data,
                ATTACH_CONSUMER_SESSION_CODE,
                Ok(Bytes::new()),
            ),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Ok(Bytes::from_static(b"resumed")),
            ),
        ]);
        assert!(matches!(
            router.poll(&coordinator, &request).await,
            Err(IggyError::TransientNotCommitted)
        ));
        assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 1);
        assert_eq!(
            router.poll(&coordinator, &request).await.unwrap(),
            "resumed"
        );
        assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 2);
        assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
    }

    fn fixture(
        exchanges: impl IntoIterator<Item = Exchange>,
    ) -> (PollRouter<Transport>, Transport, PollMessagesRequest) {
        let script = Arc::new(Script {
            exchanges: Mutex::new(exchanges.into_iter().collect()),
            ..Default::default()
        });
        (
            PollRouter::default(),
            Transport {
                channel: Channel::Coordinator,
                script,
            },
            PollMessagesRequest {
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::numeric(1),
                partition_id: Some(0),
                consumer: WireConsumer::consumer_group(WireIdentifier::numeric(7)),
                strategy: WirePollingStrategy::next(),
                count: 1,
                auto_commit: true,
            },
        )
    }

    fn routing() -> Bytes {
        PollRoutingResponse {
            consumer_session: AttachConsumerSessionRequest {
                client_id: 7,
                session: 1,
                metadata_watermark: 1,
            },
            primary: ClusterNodeResponse {
                name: "primary".to_owned(),
                ip: "127.0.0.1".to_owned(),
                tcp_port: 8090,
                quic_port: 0,
                http_port: 0,
                websocket_port: 0,
                role: 1,
                status: 1,
            },
        }
        .to_bytes()
    }
}
