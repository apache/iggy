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

use crate::{
    ClientState, Credentials, DiagnosticEvent, Identifier, IggyError, NonZeroIggyDuration,
    PartitionContext,
};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::WireDecode;
use iggy_binary_protocol::WireEncode;
use iggy_binary_protocol::codes::{
    DELETE_CONSUMER_OFFSET_CODE, POLL_MESSAGES_CODE, STORE_CONSUMER_OFFSET_CODE,
};
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::system::{BindSessionRequest, SessionIdentity};
use iggy_binary_protocol::requests::users::login_register::BindSecret;
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Default budget shared by a binary call's lookups, admission and retries.
#[doc(hidden)]
pub const BINARY_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

tokio::task_local! {
    static OPERATION_DEADLINE: Instant;
}

#[doc(hidden)]
pub async fn with_binary_deadline<F: Future>(deadline: Instant, future: F) -> F::Output {
    let deadline = OPERATION_DEADLINE
        .try_with(|current| deadline.min(*current))
        .unwrap_or(deadline);
    OPERATION_DEADLINE.scope(deadline, future).await
}

#[doc(hidden)]
pub fn binary_operation_deadline() -> Instant {
    OPERATION_DEADLINE
        .try_with(|deadline| *deadline)
        .unwrap_or_else(|_| Instant::now() + BINARY_OPERATION_TIMEOUT)
}

#[async_trait]
pub trait BinaryTransport {
    /// Gets the state of the client.
    async fn get_state(&self) -> ClientState;
    /// Sets the state of the client.
    async fn set_state(&self, state: ClientState);
    async fn publish_event(&self, event: DiagnosticEvent);
    /// Low-level raw API: send `code` with an encoded `payload` and return the
    /// raw reply body.
    ///
    /// The transport routes partition commands to their primary and captures
    /// context when the caller supplies none. Raw `SendMessages` requires an
    /// explicit partition id; raw `Balanced` and `MessagesKey` sends return
    /// [`IggyError::FeatureUnavailable`]. Use [`MessageClient::send_messages`](crate::MessageClient::send_messages)
    /// for balanced or keyed selection.
    ///
    /// Retries keep their request identity and first context. A definitive
    /// history refusal lets a send capture once again; an uncertain operation
    /// stays retained under its original context for explicit recovery.
    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError>
    where
        Self: Sync,
    {
        match code {
            POLL_MESSAGES_CODE => {
                let request = PollMessagesRequest::decode_from(&payload)
                    .map_err(|_| IggyError::InvalidCommand)?;
                self.send_poll_with_response(&request, None).await
            }
            STORE_CONSUMER_OFFSET_CODE | DELETE_CONSUMER_OFFSET_CODE => {
                self.send_offset_write_with_response(code, payload, None)
                    .await
            }
            _ => {
                self.send_raw_with_context(code, payload, PartitionContext::default())
                    .await
            }
        }
    }
    /// Send with a captured partition incarnation and owner. Every retry must retain it.
    /// A history refusal after an uncertain attempt returns `OperationUncertain`,
    /// so callers cannot refresh and repeat that write under a new identity.
    async fn send_raw_with_context(
        &self,
        code: u32,
        payload: Bytes,
        context: PartitionContext,
    ) -> Result<Bytes, IggyError>;
    /// Route a store or delete offset request while retaining the membership connection.
    /// Every retry keeps a captured `context`. Without one, the request takes the context
    /// its route reports.
    async fn send_offset_write_with_response(
        &self,
        code: u32,
        payload: Bytes,
        context: Option<PartitionContext>,
    ) -> Result<Bytes, IggyError>
    where
        Self: Sync;

    /// Transports may route an auto-commit poll without moving the connection
    /// that owns consumer-group membership.
    async fn send_poll_with_response(
        &self,
        request: &PollMessagesRequest,
        context: Option<PartitionContext>,
    ) -> Result<Bytes, IggyError>
    where
        Self: Sync;
    /// The partition count from the newest usable route for this topic.
    #[doc(hidden)]
    fn routed_partition_count(
        &self,
        _stream_id: &Identifier,
        _topic_id: &Identifier,
    ) -> Option<u32> {
        None
    }

    fn get_heartbeat_interval(&self) -> NonZeroIggyDuration;

    /// Per-transport consumer-group + partitioning cache used to resolve
    /// partitioning client-side under VSR. Shared via `Arc` so a refresh task can hold it.
    fn consumer_group_state(&self) -> Arc<crate::ConsumerGroupClientState>;
}

/// Separate opt-in marker for session control. Exported as `VsrSessionSealed`
/// so the SDK crate and external transport implementations can implement it;
/// this does not restrict implementations to this crate.
mod vsr_session_sealed {
    pub trait Sealed {}
}

/// VSR-internal session control. Distinct from [`BinaryTransport`] so
/// `&dyn BinaryTransport` cannot reach `bind`/`reset` -- mid-session
/// mutation corrupts the dedup counter or silently breaks at-most-once.
#[async_trait]
pub trait VsrSessionControl: vsr_session_sealed::Sealed + BinaryTransport {
    /// Establish a connection for explicit credentials before capturing the
    /// sign-in fence. Binary SDK transports suppress automatic sign-in here.
    #[doc(hidden)]
    async fn prepare_sign_in(&self) -> Result<(), IggyError>
    where
        Self: crate::Client,
    {
        if matches!(
            self.get_state().await,
            ClientState::Disconnected | ClientState::Connecting
        ) {
            self.connect().await?;
        }
        Ok(())
    }

    /// Internal bearer proof. Never log or expose this value to application users.
    #[doc(hidden)]
    async fn session_bind_secret(&self) -> Result<BindSecret, IggyError>;
    async fn session_identity(&self) -> Result<SessionIdentity, IggyError>;
    #[doc(hidden)]
    fn decode_session_binding(
        &self,
        identity: SessionIdentity,
        response: &[u8],
    ) -> Result<LoginRegisterResponse, IggyError> {
        decode_session_binding(&identity, response)
    }
    async fn resume_vsr_session(&self) -> Result<(), IggyError>
    where
        Self: Sync,
    {
        let fence = self.session_fence();
        let identity = self.session_identity().await?;
        let request = BindSessionRequest {
            version_info: crate::rust_sdk_version_info(self.sdk_version())?,
            identity,
            bind_secret: self.session_bind_secret().await?,
        };
        let response = self
            .send_raw_with_response(
                iggy_binary_protocol::codes::BIND_SESSION_CODE,
                request.to_bytes(),
            )
            .await?;
        let bound = self.decode_session_binding(identity, &response)?;
        self.bind_vsr_session_at(fence, bound.session).await?;
        self.set_state(ClientState::Authenticated).await;
        self.publish_event(DiagnosticEvent::SignedIn).await;
        Ok(())
    }
    async fn bind_vsr_session(&self, session: u64) -> Result<(), IggyError>;
    /// The fence of the current session. A sign-in reads it before it reads
    /// the session it signs in for, and binds its reply with
    /// [`Self::bind_vsr_session_at`].
    #[doc(hidden)]
    fn session_fence(&self) -> u64 {
        0
    }
    /// [`Self::bind_vsr_session`], but only while the session fence is still
    /// `fence`. A reply that arrives after a reset, such as the reset of an
    /// explicit disconnect, must not bind the session that replaced it.
    #[doc(hidden)]
    async fn bind_vsr_session_at(&self, _fence: u64, session: u64) -> Result<(), IggyError> {
        self.bind_vsr_session(session).await
    }
    async fn reset_vsr_session(&self) -> Result<(), IggyError>;
    /// Keep the credentials a sign-in succeeded with, so a transport that
    /// loses its connection can re-establish the session -- on this node or,
    /// after failing over, on another one. A caller that signs in by hand is
    /// otherwise less reconnectable than one that configures `AutoLogin`,
    /// which is a surprising difference between two ways of doing the same
    /// thing. Transports that cannot reconnect leave this a no-op.
    async fn remember_session_credentials(&self, _credentials: Credentials, _user_id: u32) {}
    /// Drop them: after an explicit logout there is no session to restore,
    /// and a reconnect must not resurrect one.
    async fn forget_session_credentials(&self) {}
    /// SDK crate version sent in the login-register version prefix.
    /// Implemented by the transports so the value is the SDK crate's own
    /// `CARGO_PKG_VERSION` (`iggy` for Rust), not `iggy_common`'s.
    fn sdk_version(&self) -> &'static str;
}

pub use vsr_session_sealed::Sealed as VsrSessionSealed;

/// Decodes the reply to a session bind. The reply must name the session that
/// `identity` asked for.
#[doc(hidden)]
pub fn decode_session_binding(
    identity: &SessionIdentity,
    response: &[u8],
) -> Result<LoginRegisterResponse, IggyError> {
    let bound =
        LoginRegisterResponse::decode_from(response).map_err(|_| IggyError::InvalidFormat)?;
    if bound.session == 0 {
        return Err(IggyError::InvalidSession(bound.session));
    }
    if identity.session != 0 && bound.session != identity.session {
        return Err(IggyError::SessionMismatch(identity.session, bound.session));
    }
    Ok(bound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConsumerGroupClientState;
    use bytes::BytesMut;
    use iggy_binary_protocol::codes::{GET_PARTITION_ROUTE_CODE, PING_CODE, SEND_MESSAGES_CODE};
    use iggy_binary_protocol::requests::consumer_offsets::{
        DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
    };
    use iggy_binary_protocol::requests::messages::{RawMessage, SendMessagesEncoder};
    use iggy_binary_protocol::{
        AckLevel, WireConsumer, WireIdentifier, WirePartitioning, WirePollingStrategy,
    };
    use std::sync::Mutex;

    const STREAM: u32 = 1;
    const TOPIC: &str = "orders";
    const PARTITION: u32 = 3;
    const RECEIPT: &[u8] = b"receipt";
    #[derive(Debug, PartialEq)]
    enum Call {
        Raw(u32, Bytes, PartitionContext),
        Poll(PollMessagesRequest, Option<PartitionContext>),
        OffsetWrite(u32, Bytes, Option<PartitionContext>),
    }

    /// Keeps the provided `send_raw_with_response`, the behavior under test.
    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<Call>>,
        state: Arc<ConsumerGroupClientState>,
    }

    impl Recorder {
        fn record(&self, call: Call) {
            self.calls.lock().unwrap().push(call);
        }

        fn take_calls(&self) -> Vec<Call> {
            std::mem::take(&mut self.calls.lock().unwrap())
        }
    }

    #[async_trait]
    impl BinaryTransport for Recorder {
        async fn get_state(&self) -> ClientState {
            ClientState::Authenticated
        }

        async fn set_state(&self, _state: ClientState) {}

        async fn publish_event(&self, _event: DiagnosticEvent) {}

        async fn send_raw_with_context(
            &self,
            code: u32,
            payload: Bytes,
            context: PartitionContext,
        ) -> Result<Bytes, IggyError> {
            self.record(Call::Raw(code, payload, context));
            Ok(Bytes::from_static(RECEIPT))
        }

        async fn send_offset_write_with_response(
            &self,
            code: u32,
            payload: Bytes,
            context: Option<PartitionContext>,
        ) -> Result<Bytes, IggyError> {
            self.record(Call::OffsetWrite(code, payload, context));
            Ok(Bytes::from_static(RECEIPT))
        }

        async fn send_poll_with_response(
            &self,
            request: &PollMessagesRequest,
            context: Option<PartitionContext>,
        ) -> Result<Bytes, IggyError> {
            self.record(Call::Poll(request.clone(), context));
            Ok(Bytes::from_static(RECEIPT))
        }

        fn get_heartbeat_interval(&self) -> NonZeroIggyDuration {
            NonZeroIggyDuration::ONE_SECOND
        }

        fn consumer_group_state(&self) -> Arc<ConsumerGroupClientState> {
            Arc::clone(&self.state)
        }
    }

    fn wire_topic() -> WireIdentifier {
        WireIdentifier::named(TOPIC).unwrap()
    }

    fn consumer() -> WireConsumer {
        WireConsumer::consumer(WireIdentifier::numeric(1))
    }

    fn send_payload(partitioning: &WirePartitioning) -> Bytes {
        let messages = [RawMessage {
            id: 1,
            origin_timestamp: 0,
            headers: None,
            payload: b"payload",
        }];
        let mut payload = BytesMut::new();
        SendMessagesEncoder::encode(
            &mut payload,
            &WireIdentifier::numeric(STREAM),
            &wire_topic(),
            partitioning,
            &messages,
        )
        .unwrap();
        payload.freeze()
    }

    #[tokio::test]
    async fn given_raw_send_to_partition_id_when_sent_should_leave_routing_to_transport() {
        let transport = Recorder::default();
        let payload = send_payload(&WirePartitioning::PartitionId(PARTITION));
        let response = transport
            .send_raw_with_response(SEND_MESSAGES_CODE, payload.clone())
            .await
            .unwrap();
        assert_eq!(response, Bytes::from_static(RECEIPT));
        assert_eq!(
            transport.take_calls(),
            vec![Call::Raw(
                SEND_MESSAGES_CODE,
                payload,
                PartitionContext::default()
            )]
        );
    }

    #[tokio::test]
    async fn given_raw_poll_when_sent_should_route_like_a_typed_poll() {
        let transport = Recorder::default();
        let request = PollMessagesRequest {
            consumer: consumer(),
            stream_id: WireIdentifier::numeric(STREAM),
            topic_id: wire_topic(),
            partition_id: Some(PARTITION),
            strategy: WirePollingStrategy::offset(0),
            count: 1,
            auto_commit: false,
        };
        transport
            .send_raw_with_response(POLL_MESSAGES_CODE, request.to_bytes())
            .await
            .unwrap();
        assert_eq!(transport.take_calls(), vec![Call::Poll(request, None)]);
    }

    #[tokio::test]
    async fn given_raw_offset_write_when_sent_should_route_to_the_partition_primary() {
        let transport = Recorder::default();
        let store = StoreConsumerOffsetRequest {
            consumer: consumer(),
            stream_id: WireIdentifier::numeric(STREAM),
            topic_id: wire_topic(),
            partition_id: Some(PARTITION),
            offset: 10,
            ack: AckLevel::Quorum,
        };
        let delete = DeleteConsumerOffsetRequest {
            consumer: consumer(),
            stream_id: WireIdentifier::numeric(STREAM),
            topic_id: wire_topic(),
            partition_id: Some(PARTITION),
            ack: AckLevel::Quorum,
        };
        for (code, payload) in [
            (STORE_CONSUMER_OFFSET_CODE, store.to_bytes()),
            (DELETE_CONSUMER_OFFSET_CODE, delete.to_bytes()),
        ] {
            transport
                .send_raw_with_response(code, payload.clone())
                .await
                .unwrap();
            assert_eq!(
                transport.take_calls(),
                vec![Call::OffsetWrite(code, payload, None)]
            );
        }
    }

    #[tokio::test]
    async fn given_non_partition_code_when_sent_should_pass_through_with_default_context() {
        let transport = Recorder::default();
        let payload = Bytes::from_static(b"opaque");
        for code in [PING_CODE, GET_PARTITION_ROUTE_CODE] {
            transport
                .send_raw_with_response(code, payload.clone())
                .await
                .unwrap();
            assert_eq!(
                transport.take_calls(),
                vec![Call::Raw(
                    code,
                    payload.clone(),
                    PartitionContext::default()
                )]
            );
        }
    }

    #[tokio::test]
    async fn given_undecodable_partition_payload_when_sent_should_fail_before_sending() {
        let transport = Recorder::default();
        for (code, payload) in [(POLL_MESSAGES_CODE, Bytes::from_static(b"lost-poll"))] {
            let result = transport.send_raw_with_response(code, payload).await;
            assert_eq!(result, Err(IggyError::InvalidCommand), "code {code}");
        }
        assert!(transport.take_calls().is_empty());
    }
}
