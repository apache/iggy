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
use iggy_binary_protocol::requests::messages::{FetchMessagesRequest, PollMessagesRequest};
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
    static OPERATION_LINEAGE: Option<BinaryOperationLineage>;
}

/// The caller sign-in shared by every request of one public operation.
/// The owner is stable across session recovery and distinguishes nested clients.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryOperationLineage {
    owner: u128,
    lineage: u64,
}

impl BinaryOperationLineage {
    pub fn new(owner: u128, lineage: u64) -> Self {
        Self { owner, lineage }
    }

    pub fn capture(owner: u128, lineage: u64) -> Self {
        binary_operation_lineage()
            .filter(|current| current.owner == owner)
            .unwrap_or(Self { owner, lineage })
    }

    pub fn lineage(self) -> u64 {
        self.lineage
    }
}

#[doc(hidden)]
pub fn binary_operation_lineage() -> Option<BinaryOperationLineage> {
    OPERATION_LINEAGE.try_get().ok().flatten()
}

#[doc(hidden)]
pub async fn with_binary_lineage<F: Future>(
    lineage: Option<BinaryOperationLineage>,
    future: F,
) -> F::Output {
    OPERATION_LINEAGE.scope(lineage, future).await
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
    #[doc(hidden)]
    fn operation_lineage(&self) -> Option<BinaryOperationLineage> {
        None
    }

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
    /// Send a non-replicated command `code` with an encoded `payload` to the
    /// replica `replica_id` and return its raw reply body.
    ///
    /// The request carries the session's metadata watermark, so the replica
    /// answers once it applied the metadata the session saw. The reply is the
    /// chosen replica's own: a refusal such as [`IggyError::NotPrimary`] is
    /// returned, never retried on another replica. A replicated command, a poll
    /// or a consumer-offset read returns [`IggyError::InvalidCommand`] before
    /// anything is sent. A replica the roster does not name, or names without
    /// an endpoint for this transport, returns [`IggyError::InvalidIdentifier`].
    /// A link lost after the request was written makes the outcome unknown, so
    /// a call that no later attempt answers returns
    /// [`IggyError::TransientNotCommitted`]. Transports without replica links
    /// return [`IggyError::FeatureUnavailable`].
    async fn send_raw_to_replica(
        &self,
        _code: u32,
        _payload: Bytes,
        _replica_id: u8,
    ) -> Result<Bytes, IggyError> {
        Err(IggyError::FeatureUnavailable)
    }
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
    /// Route a poll with bounded server wait and response size.
    async fn send_poll_with_response_and_options(
        &self,
        request: &PollMessagesRequest,
        context: Option<PartitionContext>,
        options: Option<crate::DeferredPollOptions>,
    ) -> Result<Bytes, IggyError>
    where
        Self: Sync,
    {
        if options.is_none() {
            self.send_poll_with_response(request, context).await
        } else {
            Err(IggyError::FeatureUnavailable)
        }
    }

    /// A fetch is a deferred request to the primary of its targets. A target
    /// with incarnation zero takes the incarnation of its route, because the
    /// server refuses that incarnation.
    /// A raw partial reply preserves wire statuses. If a committing section's
    /// progress is unknown, a later progress call on that target returns
    /// `PartitionLaneBlocked` with the local ID to abandon. Typed callers use
    /// [`Self::send_fetch_sections`] to receive that ID in the section error.
    #[doc(hidden)]
    async fn send_fetch_with_response(
        &self,
        _request: &FetchMessagesRequest,
    ) -> Result<Bytes, IggyError> {
        Err(IggyError::FeatureUnavailable)
    }

    /// Typed fetch sections retain local recovery identities for unknown
    /// committing progress. Raw callers retain the original wire statuses.
    #[doc(hidden)]
    async fn send_fetch_sections(
        &self,
        request: &FetchMessagesRequest,
    ) -> Result<Vec<crate::FetchedSection>, IggyError> {
        crate::FetchedSection::from_reply(&self.send_fetch_with_response(request).await?)
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
    /// Runs one explicit attempt, including cleanup if the caller cancels it.
    #[doc(hidden)]
    async fn sign_in_with_credentials(
        &self,
        credentials: Credentials,
    ) -> Result<crate::IdentityInfo, IggyError>
    where
        Self: crate::BinaryClient + Sized,
    {
        crate::binary_sign_in(self, credentials).await
    }

    #[doc(hidden)]
    async fn finish_sign_in(
        &self,
        credentials: Credentials,
        user_id: u32,
    ) -> Result<(), IggyError> {
        self.set_state(ClientState::Authenticated).await;
        self.remember_session_credentials(credentials, user_id)
            .await;
        self.publish_event(DiagnosticEvent::SignedIn).await;
        Ok(())
    }

    /// Completes only the captured caller's Logout. Preparatory Logout keeps its lineage.
    #[doc(hidden)]
    async fn finish_sign_out(&self, _signing_in: bool) -> Result<(), IggyError> {
        self.forget_session_credentials().await;
        self.reset_vsr_session().await?;
        self.set_state(ClientState::Connected).await;
        self.publish_event(DiagnosticEvent::SignedOut).await;
        Ok(())
    }

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
    use crate::{
        Client, Consumer, ConsumerGroupClientState, DeferredPollOptions,
        MIN_DEFERRED_POLL_REPLY_MARGIN_US, MessageClient, PollOptions, PollingKind,
        PollingStrategy, ServerCommitPolicy, fetch_binary_messages, sync_group_assignment,
    };
    use bytes::BytesMut;
    use iggy_binary_protocol::codes::SYNC_CONSUMER_GROUP_CODE;
    use iggy_binary_protocol::codes::{GET_PARTITION_ROUTE_CODE, PING_CODE, SEND_MESSAGES_CODE};
    use iggy_binary_protocol::requests::consumer_offsets::{
        DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
    };
    use iggy_binary_protocol::requests::messages::{
        FetchMessagesTarget, RawMessage, SendMessagesEncoder,
    };
    use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
    use iggy_binary_protocol::responses::messages::{
        FetchMessagesResponse, FetchMessagesSection, FetchMessagesSectionHeader,
        PollMessagesResponseHeader,
    };
    use iggy_binary_protocol::{
        AckLevel, STATUS_OK, WireConsumer, WireIdentifier, WirePartitioning, WirePollingStrategy,
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
        DeferredPoll(
            PollMessagesRequest,
            Option<PartitionContext>,
            DeferredPollOptions,
        ),
        OffsetWrite(u32, Bytes, Option<PartitionContext>),
        Fetch(FetchMessagesRequest),
    }

    /// Keeps the provided `send_raw_with_response`, the behavior under test.
    #[derive(Debug, Default)]
    struct Recorder {
        calls: Mutex<Vec<Call>>,
        state: Arc<ConsumerGroupClientState>,
        sync: Mutex<Option<SyncPublication>>,
    }

    #[derive(Debug)]
    struct SyncPublication {
        caller_changed: bool,
        empty: bool,
        replaced: bool,
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
            if code == SYNC_CONSUMER_GROUP_CODE {
                let mut scenario = self.sync.lock().unwrap();
                let scenario = scenario.as_mut().unwrap();
                if scenario.replaced {
                    assert!(self.state.is_registered("1|orders|3"));
                    assert_eq!(self.state.assignment_snapshot("1|orders|3").1, [7]);
                    if scenario.caller_changed {
                        return Err(IggyError::Unauthenticated);
                    }
                } else {
                    // The transport has accepted the reply; replacement wins before common cache publication.
                    scenario.replaced = true;
                    self.state.clear_session_scoped();
                    self.state.register_group(
                        "1|orders|3".to_owned(),
                        Identifier::numeric(STREAM).unwrap(),
                        Identifier::named(TOPIC).unwrap(),
                        Identifier::numeric(3).unwrap(),
                    );
                    self.state
                        .set_assignment("1|orders|3".to_owned(), 9, vec![7]);
                    return Ok(if scenario.empty {
                        Bytes::new()
                    } else {
                        SyncConsumerGroupResponse {
                            generation: 1,
                            partitions: vec![2],
                        }
                        .to_bytes()
                    });
                }
                return Ok(SyncConsumerGroupResponse {
                    generation: 9,
                    partitions: vec![7],
                }
                .to_bytes());
            }
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

        /// Answers a deferred poll with an empty reply of its partition.
        async fn send_poll_with_response_and_options(
            &self,
            request: &PollMessagesRequest,
            context: Option<PartitionContext>,
            options: Option<DeferredPollOptions>,
        ) -> Result<Bytes, IggyError> {
            let Some(options) = options else {
                return self.send_poll_with_response(request, context).await;
            };
            self.record(Call::DeferredPoll(request.clone(), context, options));
            Ok(PollMessagesResponseHeader {
                partition_id: request.partition_id.unwrap(),
                current_offset: 0,
                messages_count: 0,
                context: Default::default(),
            }
            .to_bytes())
        }

        /// Serves an empty section of `PARTITION` and refuses the other
        /// targets as moved.
        async fn send_fetch_with_response(
            &self,
            request: &FetchMessagesRequest,
        ) -> Result<Bytes, IggyError> {
            self.record(Call::Fetch(request.clone()));
            let sections = request
                .targets
                .iter()
                .map(|target| FetchMessagesSection {
                    header: FetchMessagesSectionHeader {
                        partition_id: target.partition_id,
                        status: if target.partition_id == PARTITION {
                            STATUS_OK
                        } else {
                            IggyError::NotPrimary.as_code()
                        },
                        current_offset: 0,
                        messages_count: 0,
                        context: PartitionContext::default(),
                        next_offset: 0,
                        records_len: 0,
                    },
                    records: &[],
                })
                .collect();
            Ok(FetchMessagesResponse { sections }.to_bytes().unwrap())
        }

        fn get_heartbeat_interval(&self) -> NonZeroIggyDuration {
            NonZeroIggyDuration::ONE_SECOND
        }

        fn consumer_group_state(&self) -> Arc<ConsumerGroupClientState> {
            Arc::clone(&self.state)
        }
    }

    impl VsrSessionSealed for Recorder {}
    impl crate::BinaryClient for Recorder {}

    #[async_trait]
    impl VsrSessionControl for Recorder {
        async fn session_bind_secret(&self) -> Result<BindSecret, IggyError> {
            Err(IggyError::FeatureUnavailable)
        }
        async fn session_identity(&self) -> Result<SessionIdentity, IggyError> {
            Err(IggyError::FeatureUnavailable)
        }
        async fn bind_vsr_session(&self, _session: u64) -> Result<(), IggyError> {
            Err(IggyError::FeatureUnavailable)
        }
        async fn reset_vsr_session(&self) -> Result<(), IggyError> {
            Err(IggyError::FeatureUnavailable)
        }
        fn sdk_version(&self) -> &'static str {
            "0.0.0"
        }
    }

    #[async_trait]
    impl Client for Recorder {
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

    async fn given_a_sync_reply_when_session_changes_before_publication_should_keep_new_membership(
        caller_changed: bool,
        empty: bool,
    ) {
        let transport = Recorder {
            sync: Mutex::new(Some(SyncPublication {
                caller_changed,
                empty,
                replaced: false,
            })),
            ..Default::default()
        };
        let result = sync_group_assignment(
            &transport,
            &Identifier::numeric(STREAM).unwrap(),
            &Identifier::named(TOPIC).unwrap(),
            &Identifier::numeric(3).unwrap(),
        )
        .await;
        if caller_changed {
            assert_eq!(result.unwrap_err(), IggyError::Unauthenticated);
        } else {
            assert!(
                result.is_ok(),
                "automatic recovery must re-sync: {result:?}"
            );
        }
        assert!(transport.state.is_registered("1|orders|3"));
        assert_eq!(transport.state.assignment_snapshot("1|orders|3").1, [7]);
        let calls = transport.take_calls();
        assert_eq!(calls.len(), 2, "{calls:?}");
        assert!(
            calls
                .iter()
                .all(|call| matches!(call, Call::Raw(SYNC_CONSUMER_GROUP_CODE, ..))),
            "{calls:?}"
        );
    }

    #[tokio::test]
    async fn given_a_sync_reply_when_session_changes_before_publication_should_keep_new_membership_recovery_assignment()
     {
        given_a_sync_reply_when_session_changes_before_publication_should_keep_new_membership(
            false, false,
        )
        .await;
    }

    #[tokio::test]
    async fn given_a_sync_reply_when_session_changes_before_publication_should_keep_new_membership_recovery_empty()
     {
        given_a_sync_reply_when_session_changes_before_publication_should_keep_new_membership(
            false, true,
        )
        .await;
    }

    #[tokio::test]
    async fn given_a_sync_reply_when_session_changes_before_publication_should_keep_new_membership_caller_assignment()
     {
        given_a_sync_reply_when_session_changes_before_publication_should_keep_new_membership(
            true, false,
        )
        .await;
    }

    #[tokio::test]
    async fn given_a_sync_reply_when_session_changes_before_publication_should_keep_new_membership_caller_empty()
     {
        given_a_sync_reply_when_session_changes_before_publication_should_keep_new_membership(
            true, true,
        )
        .await;
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
    async fn given_a_fetch_when_sent_should_encode_every_target_and_return_a_section_of_each() {
        const MOVED: u32 = 5;
        const COUNT: u32 = 10;
        const OFFSET: u64 = 7;
        let fence = PartitionContext {
            incarnation: 4,
            owner_generation: 2,
            metadata_op: 9,
        };
        let options = DeferredPollOptions::default();
        let transport = Recorder::default();

        let sections = fetch_binary_messages(
            &transport,
            &Identifier::numeric(STREAM).unwrap(),
            &Identifier::named(TOPIC).unwrap(),
            &[
                (PARTITION, PollingStrategy::next()),
                (
                    MOVED,
                    PollingStrategy::continuation(MOVED, PollingKind::Offset, OFFSET, fence),
                ),
            ],
            &Consumer::new(Identifier::numeric(1).unwrap()),
            COUNT,
            true,
            options,
        )
        .await
        .unwrap();

        let calls = transport.take_calls();
        let [Call::Fetch(request)] = calls.as_slice() else {
            panic!("expected one fetch: {calls:?}");
        };
        assert_eq!(
            (&request.consumer, &request.stream_id, &request.topic_id),
            (&consumer(), &WireIdentifier::numeric(STREAM), &wire_topic())
        );
        assert_eq!(
            (
                request.wait_us,
                request.request_timeout_us,
                request.min_count,
                request.count,
                request.max_bytes,
                request.auto_commit
            ),
            (
                options.max_wait.as_micros(),
                options.request_timeout.as_micros(),
                options.min_count,
                COUNT,
                options.max_bytes,
                true
            )
        );
        assert_eq!(
            request.targets,
            [
                FetchMessagesTarget {
                    partition_id: PARTITION,
                    strategy: WirePollingStrategy::next(),
                    max_bytes: options.max_bytes,
                    incarnation: 0,
                    owner_generation: 0,
                },
                FetchMessagesTarget {
                    partition_id: MOVED,
                    strategy: WirePollingStrategy::offset(OFFSET),
                    max_bytes: options.max_bytes,
                    incarnation: fence.incarnation,
                    owner_generation: fence.owner_generation,
                },
            ]
        );
        let statuses: Vec<_> = sections
            .iter()
            .map(|section| (section.partition_id, section.status.clone()))
            .collect();
        assert_eq!(
            statuses,
            [(PARTITION, Ok(())), (MOVED, Err(IggyError::NotPrimary))]
        );
    }

    #[tokio::test]
    async fn given_an_invalid_fetch_when_sent_should_fail_before_the_transport() {
        let transport = Recorder::default();
        let next = (PARTITION, PollingStrategy::next());
        let other_partition = (
            PARTITION,
            PollingStrategy::continuation(
                PARTITION + 1,
                PollingKind::Offset,
                0,
                PartitionContext::default(),
            ),
        );
        let options = DeferredPollOptions::default();
        // Unlike a deferred poll, a fetch needs the reply margin also
        // without a wait.
        let no_margin = DeferredPollOptions {
            max_wait: Duration::ZERO.into(),
            request_timeout: Duration::from_micros(MIN_DEFERRED_POLL_REPLY_MARGIN_US - 1).into(),
            ..options
        };
        let cases = [
            (vec![], 1, options, IggyError::InvalidCommand),
            (vec![next, next], 1, options, IggyError::InvalidCommand),
            (
                vec![(PARTITION, PollingStrategy::first())],
                1,
                options,
                IggyError::InvalidCommand,
            ),
            (vec![other_partition], 1, options, IggyError::InvalidCommand),
            (vec![next], 0, options, IggyError::InvalidMessagesCount),
            (vec![next], 1, no_margin, IggyError::InvalidCommand),
            (
                vec![next],
                1,
                DeferredPollOptions {
                    request_timeout: Duration::from_micros(MIN_DEFERRED_POLL_REPLY_MARGIN_US)
                        .into(),
                    ..no_margin
                },
                IggyError::InvalidCommand,
            ),
            (
                vec![next],
                1,
                DeferredPollOptions {
                    request_timeout: (Duration::from_micros(MIN_DEFERRED_POLL_REPLY_MARGIN_US)
                        + Duration::from_nanos(999))
                    .into(),
                    ..no_margin
                },
                IggyError::InvalidCommand,
            ),
        ];

        for (targets, count, options, error) in cases {
            let fetched = fetch_binary_messages(
                &transport,
                &Identifier::numeric(STREAM).unwrap(),
                &Identifier::named(TOPIC).unwrap(),
                &targets,
                &Consumer::new(Identifier::numeric(1).unwrap()),
                count,
                false,
                options,
            )
            .await;
            assert_eq!(fetched.unwrap_err(), error, "{targets:?}");
        }
        assert!(transport.take_calls().is_empty());
    }

    #[tokio::test]
    async fn given_a_poll_when_sent_should_send_one_deferred_poll_of_its_partition() {
        const COUNT: u32 = 5;
        const OFFSET: u64 = 7;
        let context = PartitionContext {
            incarnation: 4,
            owner_generation: 2,
            metadata_op: 9,
        };
        let options = PollOptions {
            count: COUNT,
            server_commit: ServerCommitPolicy::OnRead,
            ..PollOptions::default()
        };
        let transport = Recorder::default();

        let polled = transport
            .poll_messages(
                &Identifier::numeric(STREAM).unwrap(),
                &Identifier::named(TOPIC).unwrap(),
                PARTITION,
                &Consumer::new(Identifier::numeric(1).unwrap()),
                &PollingStrategy::continuation(PARTITION, PollingKind::Offset, OFFSET, context),
                options,
            )
            .await
            .unwrap();

        assert_eq!(
            transport.take_calls(),
            vec![Call::DeferredPoll(
                PollMessagesRequest {
                    consumer: consumer(),
                    stream_id: WireIdentifier::numeric(STREAM),
                    topic_id: wire_topic(),
                    partition_id: Some(PARTITION),
                    strategy: WirePollingStrategy::offset(OFFSET),
                    count: COUNT,
                    auto_commit: true,
                },
                Some(context),
                DeferredPollOptions::from(options),
            )]
        );
        assert_eq!(
            polled.continuation(),
            PollingStrategy::continuation(PARTITION, PollingKind::Offset, OFFSET, context),
            "an empty reply keeps the start of its request"
        );
    }

    #[tokio::test]
    async fn given_an_invalid_poll_when_sent_should_fail_before_the_transport() {
        let transport = Recorder::default();
        let other_partition = PollingStrategy::continuation(
            PARTITION + 1,
            PollingKind::Offset,
            0,
            PartitionContext::default(),
        );
        let cases = [
            (
                PollingStrategy::next(),
                PollOptions {
                    count: 0,
                    ..PollOptions::default()
                },
                IggyError::InvalidMessagesCount,
            ),
            (
                other_partition,
                PollOptions::default(),
                IggyError::InvalidCommand,
            ),
        ];

        for (strategy, options, error) in cases {
            let polled = transport
                .poll_messages(
                    &Identifier::numeric(STREAM).unwrap(),
                    &Identifier::named(TOPIC).unwrap(),
                    PARTITION,
                    &Consumer::new(Identifier::numeric(1).unwrap()),
                    &strategy,
                    options,
                )
                .await;
            assert_eq!(polled.unwrap_err(), error, "{strategy:?}");
        }
        assert!(transport.take_calls().is_empty());
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

        let result = transport
            .send_raw_with_response(POLL_MESSAGES_CODE, Bytes::from_static(b"lost-poll"))
            .await;

        assert_eq!(result, Err(IggyError::InvalidCommand));
        assert!(transport.take_calls().is_empty());
    }

    #[tokio::test]
    async fn given_nested_clients_when_capturing_lineage_should_preserve_each_owner() {
        let outer = BinaryOperationLineage::capture(1, 10);
        with_binary_lineage(Some(outer), async {
            assert_eq!(BinaryOperationLineage::capture(1, 11), outer);
            let inner = BinaryOperationLineage::capture(2, 20);
            assert_ne!(inner, outer);
            with_binary_lineage(Some(inner), async {
                assert_eq!(BinaryOperationLineage::capture(2, 21), inner);
            })
            .await;
            assert_eq!(BinaryOperationLineage::capture(1, 12), outer);
        })
        .await;
        assert!(binary_operation_lineage().is_none());
    }
}
