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

//! Auto-commit polls and offset writes use persistent data connections while
//! the coordinator retains group membership. Only explicit non-admission permits
//! rerouting. Replicated writes keep the data session's own deduplication identity.
//! Routes and attachments are fenced by the coordinator's session generation.
//! Cluster routing requires servers supporting the routing and attachment commands.

use crate::leader_aware::{node_address, transport_port};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, GET_CLUSTER_METADATA_CODE, GET_CONSUMER_OFFSET_ROUTING_CODE,
    GET_POLL_ROUTING_CODE, PING_CODE, POLL_MESSAGES_ON_PRIMARY_CODE,
};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::requests::consumer_offsets::GetConsumerOffsetRequest;
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::system::{BindSessionRequest, SessionIdentity};
use iggy_binary_protocol::responses::messages::PollRoutingResponse;
use iggy_binary_protocol::responses::system::get_cluster_metadata::ClusterMetadataResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::ClientState;
use iggy_common::{
    BinaryClient, ClusterNode, Credentials, IdKind, Identifier, IggyError, TransportProtocol,
};
use secrecy::SecretString;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{Instant, sleep, timeout, timeout_at};
use tracing::error;

const MAX_CACHED_ROUTES: usize = 4096;
const MAX_DATA_CONNECTIONS: usize = 256;
const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const ROUTING_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const ROUTING_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const ROSTER_READ_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) const fn is_poll_routing_code(code: u32) -> bool {
    matches!(
        code,
        BIND_SESSION_CODE
            | GET_POLL_ROUTING_CODE
            | POLL_MESSAGES_ON_PRIMARY_CODE
            | GET_CONSUMER_OFFSET_ROUTING_CODE
    )
}

#[async_trait]
pub(crate) trait PollTransport: BinaryClient + Send + Sync + Sized {
    const PROTOCOL: TransportProtocol;

    async fn connect_poll_client(&self, endpoint: &str) -> Result<Self, IggyError>;

    /// One exchange on this connection, with no node movement or automatic
    /// replay of an ambiguous outcome, including replicated offset writes.
    async fn send_poll_request(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        self.send_poll_request_with_context(code, payload, PartitionContext::default())
            .await
    }

    async fn send_poll_request_with_context(
        &self,
        code: u32,
        payload: Bytes,
        context: PartitionContext,
    ) -> Result<Bytes, IggyError>;

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
    generation: u64,
    endpoint: String,
    consumer_session: SessionIdentity,
    context: PartitionContext,
}

#[derive(Debug)]
struct PollConnection<T> {
    client: T,
    consumer_session: Option<SessionIdentity>,
    usable: bool,
}

type ConnectionSlot<T> = Arc<AsyncMutex<Option<PollConnection<T>>>>;
type RouteKey = (u32, Bytes);

#[derive(Debug)]
pub(crate) struct PollRouter<T> {
    pub(crate) metadata_watermark: Arc<AtomicU64>,
    /// Zero means no successful topology read, not a standalone server.
    pub(crate) roster_size: AtomicUsize,
    session_generation: AtomicU64,
    routes: Mutex<HashMap<RouteKey, Arc<PollRoute>>>,
    connections: Mutex<HashMap<String, ConnectionSlot<T>>>,
    credentials: Mutex<Option<(Credentials, u32)>>,
    next_heartbeat: Mutex<Option<Instant>>,
}

impl<T> Default for PollRouter<T> {
    fn default() -> Self {
        Self {
            metadata_watermark: Arc::default(),
            roster_size: AtomicUsize::new(0),
            session_generation: AtomicU64::new(0),
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
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut connections = self
            .connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Serialize invalidation with route and connection publication.
        self.session_generation.fetch_add(1, Ordering::AcqRel);
        routes.clear();
        connections.clear();
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

    pub(crate) fn remembered_credentials(&self) -> Option<Credentials> {
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
        context: Option<PartitionContext>,
    ) -> Result<Bytes, IggyError> {
        let payload = request.to_bytes();
        let parameters_size = request.strategy.encoded_size() + size_of::<u32>() + size_of::<u8>();
        let key = (
            GET_POLL_ROUTING_CODE,
            payload.slice(..payload.len() - parameters_size),
        );
        self.send_routed(
            coordinator,
            POLL_MESSAGES_ON_PRIMARY_CODE,
            key,
            payload,
            context,
        )
        .await
    }

    pub(crate) async fn write_offset(
        &self,
        coordinator: &T,
        code: u32,
        payload: Bytes,
        context: Option<PartitionContext>,
    ) -> Result<Bytes, IggyError> {
        let (_, route_size) =
            GetConsumerOffsetRequest::decode(&payload).map_err(|_| IggyError::InvalidCommand)?;
        let key = (
            GET_CONSUMER_OFFSET_ROUTING_CODE,
            payload.slice(..route_size),
        );
        self.send_routed(coordinator, code, key, payload, context)
            .await
    }

    pub(crate) async fn is_clustered(&self, coordinator: &T) -> Result<bool, IggyError> {
        if self.roster_size.load(Ordering::Acquire) == 0 {
            let response = timeout(
                ROSTER_READ_TIMEOUT,
                coordinator.send_poll_control(GET_CLUSTER_METADATA_CODE, Bytes::new()),
            )
            .await
            .map_err(|_| IggyError::TransientNotAccepted)??;
            let metadata = ClusterMetadataResponse::decode_from(&response)
                .map_err(|_| IggyError::InvalidCommand)?;
            if metadata.nodes.is_empty() {
                return Err(IggyError::TransientNotAccepted);
            }
            self.roster_size
                .store(metadata.nodes.len(), Ordering::Release);
        }
        Ok(self.roster_size.load(Ordering::Acquire) > 1)
    }

    async fn send_routed(
        &self,
        coordinator: &T,
        code: u32,
        key: RouteKey,
        payload: Bytes,
        mut context: Option<PartitionContext>,
    ) -> Result<Bytes, IggyError> {
        let clustered = self.is_clustered(coordinator).await?;
        let now = Instant::now();
        let deadline = now + POLL_TIMEOUT;
        let result = timeout_at(deadline, async {
            // A standalone server serves the exchange on the coordinator connection itself.
            let heartbeat_due = clustered && {
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
            let mut retry_interval = ROUTING_RETRY_INTERVAL;
            loop {
                let result = if clustered {
                    self.poll_once(coordinator, code, &key, &payload, &mut context)
                        .await
                } else {
                    self.send_to_coordinator(coordinator, code, &key, &payload, &mut context)
                        .await
                };
                if !matches!(result, Err(IggyError::TransientNotAccepted)) {
                    return result;
                }
                self.routes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&key);
                if Instant::now() + retry_interval >= deadline {
                    return Err(IggyError::TransientNotAccepted);
                }
                sleep(retry_interval).await;
                retry_interval = (retry_interval * 2).min(ROUTING_RETRY_MAX_INTERVAL);
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

    async fn send_to_coordinator(
        &self,
        coordinator: &T,
        code: u32,
        key: &RouteKey,
        payload: &Bytes,
        context: &mut Option<PartitionContext>,
    ) -> Result<Bytes, IggyError> {
        let route = self.route(coordinator, key, payload).await?;
        let captured = *context.get_or_insert(route.context);
        let result = coordinator
            .send_raw_with_context(code, payload.clone(), captured)
            .await;
        if result.is_err() {
            self.routes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(key);
        }
        result
    }

    async fn poll_once(
        &self,
        coordinator: &T,
        code: u32,
        key: &RouteKey,
        payload: &Bytes,
        context: &mut Option<PartitionContext>,
    ) -> Result<Bytes, IggyError> {
        let route = self.route(coordinator, key, payload).await?;
        let captured = *context.get_or_insert(route.context);
        let slot = {
            let mut connections = self
                .connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.validate_route(&route)?;
            if let Some(slot) = connections.get(&route.endpoint) {
                Arc::clone(slot)
            } else {
                if connections.len() >= MAX_DATA_CONNECTIONS {
                    error!(endpoint = route.endpoint, protocol = %T::PROTOCOL, limit = MAX_DATA_CONNECTIONS, "primary poll connection pool is full");
                    return Err(IggyError::InvalidConfiguration);
                }
                let slot = Arc::default();
                connections.insert(route.endpoint.clone(), Arc::clone(&slot));
                slot
            }
        };
        let mut connection = slot.lock().await;
        self.validate_route(&route)?;
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
            self.validate_route(&route)?;
            *connection = Some(PollConnection {
                client,
                consumer_session: None,
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
            let response = connection
                .client
                .send_poll_request(
                    BIND_SESSION_CODE,
                    BindSessionRequest {
                        version_info: iggy_common::rust_sdk_version_info(
                            coordinator.sdk_version(),
                        )?,
                        identity: route.consumer_session,
                        bind_secret: coordinator.session_bind_secret().await?,
                    }
                    .to_bytes(),
                )
                .await
                .map_err(unaccepted_data_error)?;
            coordinator.decode_session_binding(route.consumer_session, &response)?;
            connection
                .client
                .set_state(ClientState::Authenticated)
                .await;
            connection.consumer_session = Some(route.consumer_session);
        }
        self.validate_route(&route)?;
        let result = connection
            .client
            .send_poll_request_with_context(code, payload.clone(), captured)
            .await;
        connection.usable = !result.as_ref().is_err_and(poll_connection_failed);
        if matches!(result, Err(IggyError::TransientNotAccepted)) {
            connection.consumer_session = None;
        }
        if result.is_err() {
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
        key: &RouteKey,
        payload: &Bytes,
    ) -> Result<Arc<PollRoute>, IggyError> {
        let generation = self.session_generation.load(Ordering::Acquire);
        if let Some(route) = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .filter(|route| {
                route.generation == generation
                    && route.consumer_session.metadata_watermark
                        >= self.metadata_watermark.load(Ordering::Acquire)
            })
        {
            return Ok(Arc::clone(route));
        }
        let response = coordinator
            .send_poll_control(
                key.0,
                if key.0 == GET_CONSUMER_OFFSET_ROUTING_CODE {
                    key.1.clone()
                } else {
                    payload.clone()
                },
            )
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
            error!(node = node.name, protocol = %T::PROTOCOL, "partition primary does not expose the polling transport");
            return Err(IggyError::FeatureUnavailable);
        }
        let route = Arc::new(PollRoute {
            generation,
            endpoint: node_address(&node, port),
            consumer_session: response.consumer_session,
            context: response.context,
        });
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.validate_route(&route)?;
        if routes.len() >= MAX_CACHED_ROUTES {
            routes.clear();
        }
        routes.insert(key.clone(), Arc::clone(&route));
        Ok(route)
    }

    fn validate_route(&self, route: &PollRoute) -> Result<(), IggyError> {
        if route.generation != self.session_generation.load(Ordering::Acquire)
            || route.consumer_session.metadata_watermark
                < self.metadata_watermark.load(Ordering::Acquire)
        {
            return Err(IggyError::TransientNotAccepted);
        }
        Ok(())
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
    use iggy_binary_protocol::AckLevel;
    use iggy_binary_protocol::codes::{
        DELETE_CONSUMER_OFFSET_CODE, GET_SEND_CONTEXT_CODE, SEND_MESSAGES_CODE,
        STORE_CONSUMER_OFFSET_CODE,
    };
    use iggy_binary_protocol::requests::consumer_offsets::{
        DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
    };
    use iggy_binary_protocol::responses::system::get_cluster_metadata::ClusterNodeResponse;
    use iggy_binary_protocol::{
        Command, HEADER_SIZE, Operation, ReplyHeader, WireConsumer, WireIdentifier, WireName,
        WirePollingStrategy,
    };
    use iggy_common::{
        BinaryTransport, Client, ClientState, ConsumerGroupClientState, DiagnosticEvent,
        Identifier, IggyMessage, MessageClient, NonZeroIggyDuration, Partitioning,
        VsrSessionControl, VsrSessionSealed,
    };
    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Notify;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Channel {
        Coordinator,
        Data,
        Recovery,
    }

    type Exchange = (Channel, u32, Result<Bytes, IggyError>);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum PausePoint {
        Connect,
        Reply(u32),
    }

    #[derive(Debug)]
    struct Pause {
        point: PausePoint,
        reached: Notify,
        resume: Notify,
    }

    #[derive(Debug, Default)]
    struct Script {
        exchanges: Mutex<VecDeque<Exchange>>,
        route_queries: AtomicUsize,
        attachments: Mutex<Vec<SessionIdentity>>,
        contexts: Mutex<Vec<(u32, PartitionContext)>>,
        send_payloads: Mutex<Vec<Bytes>>,
        state: Arc<ConsumerGroupClientState>,
        connections: AtomicUsize,
        pause: Mutex<Option<Arc<Pause>>>,
    }

    impl Script {
        async fn pause_at(&self, point: PausePoint) {
            let pause = {
                let mut pause = self.pause.lock().unwrap();
                if pause.as_ref().is_some_and(|pause| pause.point == point) {
                    pause.take()
                } else {
                    None
                }
            };
            if let Some(pause) = pause {
                pause.reached.notify_one();
                pause.resume.notified().await;
            }
        }
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
            if matches!(
                code,
                GET_POLL_ROUTING_CODE | GET_CONSUMER_OFFSET_ROUTING_CODE
            ) {
                self.script.route_queries.fetch_add(1, Ordering::Relaxed);
            }
            if code == BIND_SESSION_CODE {
                let identity = BindSessionRequest::decode_from(payload).unwrap().identity;
                self.script.attachments.lock().unwrap().push(identity);
                return result.map(|bytes| {
                    assert!(
                        bytes.is_empty(),
                        "scripted binding supplies its typed result"
                    );
                    iggy_binary_protocol::responses::users::LoginRegisterResponse {
                        user_id: 1,
                        session: identity.session,
                        server_protocol_version: iggy_binary_protocol::IGGY_PROTOCOL_VERSION,
                        server_version: WireName::new("0.11.0").unwrap(),
                    }
                    .to_bytes()
                });
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
        async fn session_bind_secret(
            &self,
        ) -> Result<iggy_binary_protocol::requests::users::login_register::BindSecret, IggyError>
        {
            Ok(
                iggy_binary_protocol::requests::users::login_register::BindSecret::new(Box::new(
                    [0x5a; 32],
                )),
            )
        }
        async fn session_identity(&self) -> Result<SessionIdentity, IggyError> {
            Ok(SessionIdentity {
                client_id: 7,
                session: 11,
                metadata_watermark: 11,
            })
        }
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
        async fn send_offset_write_with_response(
            &self,
            code: u32,
            payload: Bytes,
            context: Option<PartitionContext>,
        ) -> Result<Bytes, IggyError> {
            PollRouter::default()
                .write_offset(self, code, payload, context)
                .await
        }

        async fn send_poll_with_response(
            &self,
            request: &PollMessagesRequest,
            context: Option<PartitionContext>,
        ) -> Result<Bytes, IggyError> {
            PollRouter::default().poll(self, request, context).await
        }

        async fn send_raw_with_context(
            &self,
            code: u32,
            payload: Bytes,
            context: PartitionContext,
        ) -> Result<Bytes, IggyError> {
            self.script.contexts.lock().unwrap().push((code, context));
            if code == SEND_MESSAGES_CODE {
                self.script
                    .send_payloads
                    .lock()
                    .unwrap()
                    .push(payload.clone());
            }
            self.send_raw_with_response(code, payload).await
        }

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
            self.script.state.clone()
        }
    }

    #[async_trait]
    impl PollTransport for Transport {
        const PROTOCOL: TransportProtocol = TransportProtocol::Tcp;
        async fn connect_poll_client(&self, _endpoint: &str) -> Result<Self, IggyError> {
            self.script.connections.fetch_add(1, Ordering::Relaxed);
            self.script.pause_at(PausePoint::Connect).await;
            Ok(Self {
                channel: Channel::Data,
                script: Arc::clone(&self.script),
            })
        }
        async fn send_poll_request_with_context(
            &self,
            code: u32,
            payload: Bytes,
            context: PartitionContext,
        ) -> Result<Bytes, IggyError> {
            self.script.contexts.lock().unwrap().push((code, context));
            let result = self.exchange(self.channel, code, &payload);
            self.script.pause_at(PausePoint::Reply(code)).await;
            result
        }
    }

    #[tokio::test]
    async fn reconnect_fences_in_flight_routes_connections_and_attachments() {
        for code in [
            POLL_MESSAGES_ON_PRIMARY_CODE,
            STORE_CONSUMER_OFFSET_CODE,
            DELETE_CONSUMER_OFFSET_CODE,
        ] {
            let routing_code = if code == POLL_MESSAGES_ON_PRIMARY_CODE {
                GET_POLL_ROUTING_CODE
            } else {
                GET_CONSUMER_OFFSET_ROUTING_CODE
            };
            for point in [
                PausePoint::Reply(routing_code),
                PausePoint::Connect,
                PausePoint::Reply(BIND_SESSION_CODE),
            ] {
                let mut replacement = PollRoutingResponse::decode_from(&routing()).unwrap();
                replacement.consumer_session.session = 2;
                let mut exchanges = vec![(Channel::Coordinator, routing_code, Ok(routing()))];
                if point == PausePoint::Reply(BIND_SESSION_CODE) {
                    exchanges.push((Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())));
                }
                exchanges.extend([
                    (
                        Channel::Coordinator,
                        routing_code,
                        Ok(replacement.to_bytes()),
                    ),
                    (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
                    (Channel::Data, code, Ok(Bytes::from_static(b"resumed"))),
                    (Channel::Data, code, Ok(Bytes::from_static(b"warm"))),
                ]);
                let (router, coordinator, request) = fixture(exchanges);
                let pause = Arc::new(Pause {
                    point,
                    reached: Notify::new(),
                    resume: Notify::new(),
                });
                *coordinator.script.pause.lock().unwrap() = Some(Arc::clone(&pause));
                let pending = routed_request(&router, &coordinator, &request, code);
                tokio::pin!(pending);
                tokio::select! {
                    result = &mut pending => panic!("request finished before reconnect: {result:?}"),
                    () = pause.reached.notified() => {}
                }
                router.clear_session();
                router.clear_session();
                router.metadata_watermark.store(11, Ordering::Release);
                pause.resume.notify_one();

                assert_eq!(pending.await.unwrap(), "resumed", "{code} at {point:?}");
                assert_eq!(
                    routed_request(&router, &coordinator, &request, code)
                        .await
                        .unwrap(),
                    "warm"
                );
                let routes = router.routes.lock().unwrap();
                assert_eq!(routes.len(), 1);
                assert!(routes.iter().all(|((command, _), route)| {
                    *command == routing_code && route.consumer_session.session == 2
                }));
                replacement.consumer_session.metadata_watermark = 11;
                assert_eq!(
                    *coordinator.script.attachments.lock().unwrap(),
                    if point == PausePoint::Reply(BIND_SESSION_CODE) {
                        vec![
                            PollRoutingResponse::decode_from(&routing())
                                .unwrap()
                                .consumer_session,
                            replacement.consumer_session,
                        ]
                    } else {
                        vec![replacement.consumer_session]
                    }
                );
                assert_eq!(
                    coordinator.script.connections.load(Ordering::Relaxed),
                    if point == PausePoint::Reply(routing_code) {
                        1
                    } else {
                        2
                    }
                );
                assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn reconnect_fences_queued_requests_without_replaying_in_flight_data() {
        for code in [
            POLL_MESSAGES_ON_PRIMARY_CODE,
            STORE_CONSUMER_OFFSET_CODE,
            DELETE_CONSUMER_OFFSET_CODE,
        ] {
            let routing_code = if code == POLL_MESSAGES_ON_PRIMARY_CODE {
                GET_POLL_ROUTING_CODE
            } else {
                GET_CONSUMER_OFFSET_ROUTING_CODE
            };
            let mut replacement = PollRoutingResponse::decode_from(&routing()).unwrap();
            replacement.consumer_session.session = 2;
            let (router, coordinator, request) = fixture([
                (Channel::Coordinator, routing_code, Ok(routing())),
                (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
                (Channel::Data, code, Ok(Bytes::from_static(b"in flight"))),
                (
                    Channel::Coordinator,
                    routing_code,
                    Ok(replacement.to_bytes()),
                ),
                (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
                (Channel::Data, code, Ok(Bytes::from_static(b"queued"))),
            ]);
            let pause = Arc::new(Pause {
                point: PausePoint::Reply(code),
                reached: Notify::new(),
                resume: Notify::new(),
            });
            *coordinator.script.pause.lock().unwrap() = Some(Arc::clone(&pause));
            let in_flight = routed_request(&router, &coordinator, &request, code);
            tokio::pin!(in_flight);
            tokio::select! {
                result = &mut in_flight => panic!("data reply was not paused: {result:?}"),
                () = pause.reached.notified() => {}
            }
            let queued = routed_request(&router, &coordinator, &request, code);
            tokio::pin!(queued);
            assert!(futures::poll!(&mut queued).is_pending());
            router.clear_session();
            router.clear_session();
            pause.resume.notify_one();

            let (in_flight, queued) = tokio::join!(in_flight, queued);
            assert_eq!(in_flight.unwrap(), "in flight");
            assert_eq!(queued.unwrap(), "queued");
            assert_eq!(
                coordinator.script.attachments.lock().unwrap().last(),
                Some(&replacement.consumer_session)
            );
            assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 2);
            assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn given_new_route_when_retrying_a_continuation_should_preserve_its_incarnation_and_owner()
     {
        let context = PartitionContext {
            incarnation: 7,
            owner_generation: 5,
            metadata_op: 11,
        };
        let mut original_route = PollRoutingResponse::decode_from(&routing()).unwrap();
        original_route.context = context;
        let mut newer_route = original_route.clone();
        newer_route.context.incarnation += 1;
        newer_route.context.owner_generation += 1;
        newer_route.context.metadata_op += 1;
        let (router, coordinator, request) = fixture([
            (
                Channel::Coordinator,
                GET_POLL_ROUTING_CODE,
                Ok(original_route.to_bytes()),
            ),
            (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Err(IggyError::TransientNotAccepted),
            ),
            (
                Channel::Coordinator,
                GET_POLL_ROUTING_CODE,
                Ok(newer_route.to_bytes()),
            ),
            (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Err(IggyError::HistoryUnavailable),
            ),
        ]);
        assert!(matches!(
            router.poll(&coordinator, &request, Some(context)).await,
            Err(IggyError::HistoryUnavailable)
        ));
        let contexts: Vec<_> = coordinator
            .script
            .contexts
            .lock()
            .unwrap()
            .iter()
            .filter(|(code, _)| *code == POLL_MESSAGES_ON_PRIMARY_CODE)
            .map(|(_, context)| *context)
            .collect();
        assert_eq!(contexts, vec![context, context]);
        assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn offset_writes_reuse_the_data_session_with_their_own_route_fence() {
        let (router, coordinator, request) = fixture([
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Ok(Bytes::new()),
            ),
            (
                Channel::Coordinator,
                GET_CONSUMER_OFFSET_ROUTING_CODE,
                Ok(routing()),
            ),
            (
                Channel::Data,
                STORE_CONSUMER_OFFSET_CODE,
                Err(IggyError::TransientNotAccepted),
            ),
            (
                Channel::Coordinator,
                GET_CONSUMER_OFFSET_ROUTING_CODE,
                Ok(routing()),
            ),
            (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
            (Channel::Data, STORE_CONSUMER_OFFSET_CODE, Ok(Bytes::new())),
            (Channel::Data, DELETE_CONSUMER_OFFSET_CODE, Ok(Bytes::new())),
        ]);
        router.poll(&coordinator, &request, None).await.unwrap();
        let store = StoreConsumerOffsetRequest {
            consumer: request.consumer.clone(),
            stream_id: request.stream_id.clone(),
            topic_id: request.topic_id.clone(),
            partition_id: request.partition_id,
            offset: 10,
            ack: AckLevel::Quorum,
        }
        .to_bytes();
        router
            .write_offset(&coordinator, STORE_CONSUMER_OFFSET_CODE, store, None)
            .await
            .unwrap();
        let delete = DeleteConsumerOffsetRequest {
            consumer: request.consumer,
            stream_id: request.stream_id,
            topic_id: request.topic_id,
            partition_id: request.partition_id,
            ack: AckLevel::Quorum,
        }
        .to_bytes();
        router
            .write_offset(&coordinator, DELETE_CONSUMER_OFFSET_CODE, delete, None)
            .await
            .unwrap();
        assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.script.attachments.lock().unwrap().len(), 2);
        assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ambiguous_offset_writes_do_not_recover_or_replay() {
        for error in [IggyError::Disconnected, IggyError::TransientNotCommitted] {
            let (router, coordinator, request) = fixture([
                (
                    Channel::Coordinator,
                    GET_CONSUMER_OFFSET_ROUTING_CODE,
                    Ok(routing()),
                ),
                (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
                (Channel::Data, STORE_CONSUMER_OFFSET_CODE, Err(error)),
            ]);
            let store = StoreConsumerOffsetRequest {
                consumer: request.consumer,
                stream_id: request.stream_id,
                topic_id: request.topic_id,
                partition_id: request.partition_id,
                offset: 10,
                ack: AckLevel::Quorum,
            }
            .to_bytes();
            assert!(matches!(
                router
                    .write_offset(&coordinator, STORE_CONSUMER_OFFSET_CODE, store, None)
                    .await,
                Err(IggyError::TransientNotCommitted)
            ));
            assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 1);
            assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn given_terminal_offset_errors_when_routed_should_not_recover_or_replay() {
        for (code, error) in [
            (STORE_CONSUMER_OFFSET_CODE, IggyError::InvalidOffset(10)),
            (
                DELETE_CONSUMER_OFFSET_CODE,
                IggyError::ConsumerOffsetNotFound(7),
            ),
        ] {
            let expected_code = error.as_code();
            let (router, coordinator, request) = fixture([
                (
                    Channel::Coordinator,
                    GET_CONSUMER_OFFSET_ROUTING_CODE,
                    Ok(routing()),
                ),
                (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
                (Channel::Data, code, Err(error)),
            ]);
            let error = routed_request(&router, &coordinator, &request, code)
                .await
                .expect_err("terminal offset errors must be returned to the caller");
            assert_eq!(error.as_code(), expected_code);
            assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 1);
            assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn acknowledged_metadata_changes_refresh_cached_routes_and_attachments() {
        let (router, coordinator, request) = fixture([
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Ok(Bytes::from_static(b"first")),
            ),
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
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
        assert_eq!(
            router.poll(&coordinator, &request, None).await.unwrap(),
            "first"
        );
        let metadata_reply = ReplyHeader {
            command: Command::Reply,
            operation: Operation::DeleteTopic,
            size: u32::try_from(HEADER_SIZE).unwrap(),
            commit: 11,
            ..Default::default()
        };
        crate::vsr::observe_metadata_reply(
            &router.metadata_watermark,
            bytemuck::bytes_of(&metadata_reply).try_into().unwrap(),
        );
        assert_eq!(
            router.poll(&coordinator, &request, None).await.unwrap(),
            "after metadata"
        );
        assert_eq!(
            router.poll(&coordinator, &request, None).await.unwrap(),
            "warm"
        );
        assert_eq!(
            coordinator
                .script
                .attachments
                .lock()
                .unwrap()
                .iter()
                .map(|session| session.metadata_watermark)
                .collect::<Vec<_>>(),
            [1, 11]
        );
        assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 1);
        assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_topology_cannot_fall_back_to_a_coordinator_auto_commit() {
        let primary = PollRoutingResponse::decode_from(&routing())
            .unwrap()
            .primary;
        let metadata = ClusterMetadataResponse {
            name: "standalone".to_owned(),
            nodes: vec![primary],
        }
        .to_bytes();
        let (router, coordinator, request) = fixture([
            (
                Channel::Coordinator,
                GET_CLUSTER_METADATA_CODE,
                Err(IggyError::InvalidCommand),
            ),
            (
                Channel::Coordinator,
                GET_CLUSTER_METADATA_CODE,
                Ok(metadata),
            ),
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (
                Channel::Recovery,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Ok(Bytes::from_static(b"standalone")),
            ),
            (
                Channel::Recovery,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Ok(Bytes::from_static(b"warm")),
            ),
        ]);
        router.roster_size.store(0, Ordering::Release);
        assert!(matches!(
            router.poll(&coordinator, &request, None).await,
            Err(IggyError::InvalidCommand)
        ));
        assert_eq!(router.roster_size.load(Ordering::Acquire), 0);
        assert_eq!(
            router.poll(&coordinator, &request, None).await.unwrap(),
            "standalone"
        );
        assert_eq!(
            router.poll(&coordinator, &request, None).await.unwrap(),
            "warm"
        );
        assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 0);
        assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn given_standalone_server_when_not_accepted_should_retry_with_the_first_context() {
        let mut first_route = PollRoutingResponse::decode_from(&routing()).unwrap();
        first_route.context.incarnation = 7;
        let mut newer_route = first_route.clone();
        newer_route.context.incarnation += 1;
        for code in [POLL_MESSAGES_ON_PRIMARY_CODE, STORE_CONSUMER_OFFSET_CODE] {
            let routing_code = if code == POLL_MESSAGES_ON_PRIMARY_CODE {
                GET_POLL_ROUTING_CODE
            } else {
                GET_CONSUMER_OFFSET_ROUTING_CODE
            };
            let (router, coordinator, request) = fixture([
                (
                    Channel::Coordinator,
                    routing_code,
                    Err(IggyError::TransientNotAccepted),
                ),
                (
                    Channel::Coordinator,
                    routing_code,
                    Ok(first_route.to_bytes()),
                ),
                (
                    Channel::Recovery,
                    code,
                    Err(IggyError::TransientNotAccepted),
                ),
                (
                    Channel::Coordinator,
                    routing_code,
                    Ok(newer_route.to_bytes()),
                ),
                (Channel::Recovery, code, Ok(Bytes::from_static(b"accepted"))),
            ]);
            router.roster_size.store(1, Ordering::Release);

            assert_eq!(
                routed_request(&router, &coordinator, &request, code)
                    .await
                    .unwrap(),
                "accepted",
                "{code}"
            );
            let contexts: Vec<_> = coordinator
                .script
                .contexts
                .lock()
                .unwrap()
                .iter()
                .filter(|(sent, _)| *sent == code)
                .map(|(_, context)| *context)
                .collect();
            assert_eq!(contexts, [first_route.context, first_route.context]);
            assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sustained_refusals_bound_route_queries_and_keep_the_non_admission_result() {
        const SCRIPTED_ATTEMPTS: usize = 1000;
        const MAX_ROUTE_QUERIES: usize = 40;
        for refuse_routing in [true, false] {
            let exchanges = (0..SCRIPTED_ATTEMPTS).flat_map(|_| {
                if refuse_routing {
                    return vec![(
                        Channel::Coordinator,
                        GET_POLL_ROUTING_CODE,
                        Err(IggyError::TransientNotAccepted),
                    )];
                }
                vec![
                    (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
                    (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
                    (
                        Channel::Data,
                        POLL_MESSAGES_ON_PRIMARY_CODE,
                        Err(IggyError::TransientNotAccepted),
                    ),
                ]
            });
            let (router, coordinator, request) = fixture(exchanges);
            let started = Instant::now();
            assert!(matches!(
                router.poll(&coordinator, &request, None).await,
                Err(IggyError::TransientNotAccepted)
            ));
            let queries = coordinator.script.route_queries.load(Ordering::Relaxed);
            assert!(
                queries <= MAX_ROUTE_QUERIES,
                "sustained refusal overloaded the coordinator with {queries} route queries"
            );
            assert!(started.elapsed() <= POLL_TIMEOUT);
            assert!(started.elapsed() >= POLL_TIMEOUT - Duration::from_secs(1));
            assert_eq!(
                coordinator.script.connections.load(Ordering::Relaxed),
                usize::from(!refuse_routing)
            );
        }
    }

    #[tokio::test]
    async fn complete_error_replies_preserve_the_attached_connection() {
        let (router, coordinator, request) = fixture([
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Err(IggyError::TooManyConsumerOffsets),
            ),
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Ok(Bytes::from_static(b"resumed")),
            ),
        ]);
        assert!(matches!(
            router.poll(&coordinator, &request, None).await,
            Err(IggyError::TooManyConsumerOffsets)
        ));
        assert_eq!(
            router.poll(&coordinator, &request, None).await.unwrap(),
            "resumed"
        );
        assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 1);
        assert_eq!(
            coordinator
                .script
                .attachments
                .lock()
                .unwrap()
                .iter()
                .map(|session| session.metadata_watermark)
                .collect::<Vec<_>>(),
            [1]
        );
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
            (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Err(IggyError::Disconnected),
            ),
            (Channel::Coordinator, GET_POLL_ROUTING_CODE, Ok(routing())),
            (Channel::Data, BIND_SESSION_CODE, Ok(Bytes::new())),
            (
                Channel::Data,
                POLL_MESSAGES_ON_PRIMARY_CODE,
                Ok(Bytes::from_static(b"resumed")),
            ),
        ]);
        assert!(matches!(
            router.poll(&coordinator, &request, None).await,
            Err(IggyError::TransientNotCommitted)
        ));
        assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 1);
        assert_eq!(
            router.poll(&coordinator, &request, None).await.unwrap(),
            "resumed"
        );
        assert_eq!(coordinator.script.connections.load(Ordering::Relaxed), 2);
        assert!(coordinator.script.exchanges.lock().unwrap().is_empty());
    }

    async fn routed_request(
        router: &PollRouter<Transport>,
        coordinator: &Transport,
        request: &PollMessagesRequest,
        code: u32,
    ) -> Result<Bytes, IggyError> {
        let payload = match code {
            POLL_MESSAGES_ON_PRIMARY_CODE => return router.poll(coordinator, request, None).await,
            STORE_CONSUMER_OFFSET_CODE => StoreConsumerOffsetRequest {
                consumer: request.consumer.clone(),
                stream_id: request.stream_id.clone(),
                topic_id: request.topic_id.clone(),
                partition_id: request.partition_id,
                offset: 10,
                ack: AckLevel::Quorum,
            }
            .to_bytes(),
            DELETE_CONSUMER_OFFSET_CODE => DeleteConsumerOffsetRequest {
                consumer: request.consumer.clone(),
                stream_id: request.stream_id.clone(),
                topic_id: request.topic_id.clone(),
                partition_id: request.partition_id,
                ack: AckLevel::Quorum,
            }
            .to_bytes(),
            _ => panic!("unexpected routed command: {code}"),
        };
        router.write_offset(coordinator, code, payload, None).await
    }

    #[tokio::test]
    async fn given_stale_send_context_when_refused_should_refresh_once_and_preserve_batch() {
        let old = PartitionContext::default();
        let current = PartitionContext {
            metadata_op: 42,
            ..old
        };
        let (_, transport, _) = fixture([
            (
                Channel::Recovery,
                SEND_MESSAGES_CODE,
                Err(IggyError::HistoryUnavailable),
            ),
            (
                Channel::Recovery,
                GET_SEND_CONTEXT_CODE,
                Ok(current.to_bytes()),
            ),
            (Channel::Recovery, SEND_MESSAGES_CODE, Ok(Bytes::new())),
        ]);
        let stream = Identifier::numeric(1).unwrap();
        let topic = Identifier::numeric(2).unwrap();
        transport
            .script
            .state
            .set_partition_context(&stream, &topic, 0, old);
        let mut messages = [IggyMessage::from("payload")];
        transport
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .unwrap();
        assert_ne!(messages[0].header.id, 0);
        assert_eq!(
            *transport.script.contexts.lock().unwrap(),
            vec![(SEND_MESSAGES_CODE, old), (SEND_MESSAGES_CODE, current)]
        );
        let payloads = transport.script.send_payloads.lock().unwrap();
        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0], payloads[1]);
        assert!(transport.script.exchanges.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn given_send_refusals_when_repeated_or_uncertain_should_stop_refreshing() {
        for uncertain in [false, true] {
            let current = PartitionContext::default();
            let expected = if uncertain {
                IggyError::TransientNotCommitted
            } else {
                IggyError::HistoryUnavailable
            };
            let mut exchanges =
                vec![(Channel::Recovery, SEND_MESSAGES_CODE, Err(expected.clone()))];
            if !uncertain {
                exchanges.extend([
                    (
                        Channel::Recovery,
                        GET_SEND_CONTEXT_CODE,
                        Ok(current.to_bytes()),
                    ),
                    (Channel::Recovery, SEND_MESSAGES_CODE, Err(expected.clone())),
                ]);
            }
            let (_, transport, _) = fixture(exchanges);
            let stream = Identifier::numeric(1).unwrap();
            let topic = Identifier::numeric(2).unwrap();
            transport
                .script
                .state
                .set_partition_context(&stream, &topic, 0, current);
            let mut messages = [IggyMessage::from("payload")];
            let result = transport
                .send_messages(
                    &stream,
                    &topic,
                    &Partitioning::partition_id(0),
                    &mut messages,
                )
                .await;
            assert_eq!(result.unwrap_err(), expected);
            assert_eq!(
                transport.script.contexts.lock().unwrap().len(),
                if uncertain { 1 } else { 2 }
            );
            assert!(transport.script.exchanges.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn given_malformed_send_context_when_decoded_should_report_invalid_format() {
        let (_, transport, _) = fixture([(
            Channel::Recovery,
            GET_SEND_CONTEXT_CODE,
            Ok(Bytes::from_static(b"bad")),
        )]);
        let mut messages = [IggyMessage::from("payload")];
        let result = transport
            .send_messages(
                &Identifier::numeric(1).unwrap(),
                &Identifier::numeric(2).unwrap(),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await;
        assert_eq!(result.unwrap_err(), IggyError::InvalidFormat);
        assert!(transport.script.contexts.lock().unwrap().is_empty());
    }

    fn fixture(
        exchanges: impl IntoIterator<Item = Exchange>,
    ) -> (PollRouter<Transport>, Transport, PollMessagesRequest) {
        let script = Arc::new(Script {
            exchanges: Mutex::new(exchanges.into_iter().collect()),
            ..Default::default()
        });
        (
            PollRouter {
                roster_size: AtomicUsize::new(2),
                ..PollRouter::default()
            },
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
            context: Default::default(),
            consumer_session: SessionIdentity {
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
