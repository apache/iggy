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

//! The operation driver coordinates a request across replica links. One deadline
//! bounds the lane wait, the lookups, the attempts and the pauses of a call.
//! A frame that can have taken effect is never frozen again, so a retry never
//! applies a request twice.

use crate::links::{Frame, Link, ReplicaLinks, Target, lost_link, pause_before_retry, read_reply};
use crate::session_owner::{LaneKey, LaneTicket, RetainedOperation, SessionEpoch, SessionOwner};
use crate::vsr::decode_reply_for;
use bytes::Bytes;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, CHANGE_PASSWORD_CODE, DELETE_CONSUMER_OFFSET_CODE, GET_CONSUMER_OFFSET_CODE,
    GET_PARTITION_ROUTE_CODE, LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE,
    POLL_MESSAGES_CODE, SEND_MESSAGES_CODE, STORE_CONSUMER_OFFSET_CODE, UPDATE_RESOURCE_CODE,
};
use iggy_binary_protocol::consensus::{Operation, operation_for_code};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, GetConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::{
    GetPartitionRouteRequest, PollMessagesRequest, SendMessagesHeader,
};
use iggy_binary_protocol::requests::system::{ResourceTarget, UpdateResourceRequest};
use iggy_binary_protocol::requests::users::ChangePasswordRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::{
    WireDecode, WireEncode, WireOptions, WirePartitioning, WirePollingStrategy,
};
use iggy_common::{
    BinaryOperationLineage, ClientState, Credentials, DiagnosticEvent, HeaderKind, Identifier,
    IggyError, OptionKey,
};
use route_cache::RouteCache;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::time::{Instant, timeout_at};
use tracing::debug;

mod route_cache;

pub(crate) use route_cache::PartitionKey;

pub(crate) use iggy_common::{
    binary_operation_deadline as operation_deadline, with_binary_deadline as with_deadline,
};

/// Where a binary client sends one request.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RequestTarget<'a> {
    Metadata,
    Partition(PartitionTarget<'a>),
}

/// A partition request as its caller spelled the partition.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PartitionTarget<'a> {
    pub(crate) stream_id: &'a Identifier,
    pub(crate) topic_id: &'a Identifier,
    pub(crate) partition_id: u32,
    /// `None` captures the route context. Retries retain the first context sent.
    pub(crate) context: Option<PartitionContext>,
}

/// Sends the requests of one client over its replica links.
#[derive(Debug)]
pub(crate) struct OperationDriver {
    session: Arc<SessionOwner>,
    links: ReplicaLinks,
    routes: Mutex<RouteCache>,
}

impl OperationDriver {
    /// The driver shares the session owner of `links`.
    pub(crate) fn new(links: ReplicaLinks) -> Self {
        Self {
            session: Arc::clone(links.session()),
            links,
            routes: Mutex::default(),
        }
    }

    pub(crate) fn links(&self) -> &ReplicaLinks {
        &self.links
    }

    pub(crate) fn session(&self) -> &Arc<SessionOwner> {
        &self.session
    }

    pub(crate) fn partition_count(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Option<u32> {
        self.routes().partition_count(
            stream_id,
            topic_id,
            self.session.fence(),
            self.session.metadata_watermark(),
        )
    }

    pub(crate) fn begin_sign_in(&self) -> Result<ExplicitSignIn<'_>, IggyError> {
        let (lineage, owed) = self.session.begin_sign_in()?;
        if owed {
            self.session
                .in_lineage(lineage.lineage(), || self.links.disconnect());
        }
        Ok(ExplicitSignIn {
            driver: self,
            lineage,
            complete: false,
        })
    }

    pub(crate) async fn finish_sign_in(
        &self,
        credentials: Credentials,
        user_id: u32,
    ) -> Result<(), IggyError> {
        let lineage = self.session.operation_lineage().lineage();
        if let Ok(link) = self.links.link(Target::MetadataPrimary)
            && let Err(error) = self.links.refresh_roster(&link).await
        {
            debug!("Failed to refresh replicas after sign-in: {error}");
        }
        self.session
            .finish_sign_in(lineage, credentials, user_id, || {
                self.links.set_state(ClientState::Authenticated);
                self.links.publish(DiagnosticEvent::SignedIn);
            })
    }

    pub(crate) async fn poll(
        &self,
        request: &PollMessagesRequest,
        context: Option<PartitionContext>,
    ) -> Result<Bytes, IggyError> {
        let stream_id = Identifier::try_from(&request.stream_id)?;
        let topic_id = Identifier::try_from(&request.topic_id)?;
        let partition_id = match request.partition_id {
            Some(partition) => partition,
            None if request.consumer.kind
                == iggy_binary_protocol::primitives::consumer::KIND_CONSUMER =>
            {
                0
            }
            None => return Err(IggyError::InvalidIdentifier),
        };
        let call = Call::new(
            POLL_MESSAGES_CODE,
            request.to_bytes(),
            RequestTarget::Partition(PartitionTarget {
                stream_id: &stream_id,
                topic_id: &topic_id,
                partition_id,
                context,
            }),
            operation_deadline(),
        );
        self.drive(Call {
            progress_poll: request.auto_commit
                || request.strategy.kind == WirePollingStrategy::next().kind,
            ..call
        })
        .await
    }

    pub(crate) async fn send_raw(
        &self,
        code: u32,
        payload: Bytes,
        context: Option<PartitionContext>,
    ) -> Result<Bytes, IggyError> {
        let (stream, topic, partition_id) = match code {
            SEND_MESSAGES_CODE => {
                let (metadata_length, body) = payload
                    .split_first_chunk()
                    .ok_or(IggyError::InvalidCommand)?;
                let request = body
                    .get(..u32::from_le_bytes(*metadata_length) as usize)
                    .and_then(|metadata| SendMessagesHeader::decode_from(metadata).ok())
                    .ok_or(IggyError::InvalidCommand)?;
                let WirePartitioning::PartitionId(partition_id) = request.partitioning else {
                    return Err(IggyError::FeatureUnavailable);
                };
                (request.stream_id, request.topic_id, partition_id)
            }
            POLL_MESSAGES_CODE => {
                let request = PollMessagesRequest::decode_from(&payload)
                    .map_err(|_| IggyError::InvalidCommand)?;
                return self.poll(&request, context).await;
            }
            GET_CONSUMER_OFFSET_CODE => {
                let request = GetConsumerOffsetRequest::decode_from(&payload)
                    .map_err(|_| IggyError::InvalidCommand)?;
                (
                    request.stream_id,
                    request.topic_id,
                    request.partition_id.unwrap_or(0),
                )
            }
            STORE_CONSUMER_OFFSET_CODE => {
                let request = StoreConsumerOffsetRequest::decode_from(&payload)
                    .map_err(|_| IggyError::InvalidCommand)?;
                (
                    request.stream_id,
                    request.topic_id,
                    request.partition_id.ok_or(IggyError::InvalidIdentifier)?,
                )
            }
            DELETE_CONSUMER_OFFSET_CODE => {
                let request = DeleteConsumerOffsetRequest::decode_from(&payload)
                    .map_err(|_| IggyError::InvalidCommand)?;
                (
                    request.stream_id,
                    request.topic_id,
                    request.partition_id.ok_or(IggyError::InvalidIdentifier)?,
                )
            }
            _ => return self.send(code, payload, RequestTarget::Metadata).await,
        };
        let stream_id = Identifier::try_from(&stream)?;
        let topic_id = Identifier::try_from(&topic)?;
        self.send(
            code,
            payload,
            RequestTarget::Partition(PartitionTarget {
                stream_id: &stream_id,
                topic_id: &topic_id,
                partition_id,
                context,
            }),
        )
        .await
    }

    /// Sends one request and returns the body of its reply. A poll goes
    /// through [`Self::poll`], which knows whether the poll moves its consumer.
    pub(crate) async fn send(
        &self,
        code: u32,
        payload: Bytes,
        target: RequestTarget<'_>,
    ) -> Result<Bytes, IggyError> {
        debug_assert_ne!(code, POLL_MESSAGES_CODE, "a poll goes through Self::poll");
        let deadline = operation_deadline();
        self.drive(Call::new(code, payload, target, deadline)).await
    }

    /// Replays the retained operation `op` until it has an outcome or
    /// `deadline` passes. A proved outcome frees the lane. An operation that
    /// is no longer retained returns the outcome that a replay proved for it.
    pub(crate) async fn resolve(&self, op: u64, deadline: Instant) -> Result<(), IggyError> {
        let (ticket, retained) = match self.session.resume(op, deadline).await {
            Err(IggyError::OperationNotRetained(_)) => return self.session.proved_outcome(op),
            resumed => resumed?,
        };
        self.drive(Call::resumed(ticket, retained, deadline))
            .await
            .map(|_| ())
    }

    /// Frees the lane of the retained operation `op`. Its outcome stays
    /// unknown.
    pub(crate) fn abandon(&self, op: u64) -> Result<(), IggyError> {
        self.session.abandon(op)
    }

    async fn drive(&self, mut call: Call) -> Result<Bytes, IggyError> {
        let lineage = self.session.operation_lineage().lineage();
        let started = SessionEpoch {
            lineage,
            ..self.session.epoch()
        };
        loop {
            let next = match self.attempt(&mut call, started).await {
                Ok(body) => {
                    self.refresh_sign_in(&call);
                    call.complete(Ok(()));
                    return Ok(body);
                }
                Err(next) => next,
            };
            call.settle_sent();
            match next {
                Next::Again if Instant::now() < call.deadline => {}
                Next::Again => return Err(call.expire()),
                Next::Pause => {
                    if !call.pause().await {
                        return Err(call.expire());
                    }
                }
                Next::Fail(error) => return Err(call.end(error)),
                Next::Committed(error) => {
                    call.complete(Err(error.clone()));
                    return Err(error);
                }
            }
        }
    }

    /// Applies a committed password change or rename to the stored sign-in,
    /// so a later sign-in does not replay the old credentials. Typed, raw and
    /// resolved calls all succeed here, and only the request carries the new
    /// credentials.
    fn refresh_sign_in(&self, call: &Call) {
        let Some(frame) = &call.frame else {
            return;
        };
        match call.code {
            CHANGE_PASSWORD_CODE => {
                if let Ok(request) = ChangePasswordRequest::decode_from(&frame.body)
                    && let Ok(user) = Identifier::try_from(&request.user_id)
                {
                    self.session.refresh_password(&user, &request.new_password);
                }
            }
            UPDATE_RESOURCE_CODE => {
                if let Ok(UpdateResourceRequest {
                    target: ResourceTarget::User { user_id },
                    options,
                    ..
                }) = UpdateResourceRequest::decode_from(&frame.body)
                    && let Some(username) = patched_username(&options)
                    && let Ok(user) = Identifier::try_from(&user_id)
                {
                    self.session.refresh_username(&user, username);
                }
            }
            _ => {}
        }
    }

    /// One attempt of `call`, from the choice of its replica to its reply.
    /// The call started in the session epoch `started`.
    async fn attempt(&self, call: &mut Call, started: SessionEpoch) -> Result<Bytes, Next> {
        match self.stopped() {
            Some(IggyError::ClientShutdown) => return Err(Next::Fail(IggyError::ClientShutdown)),
            // The caller signed in, signed out or disconnected since the call
            // started, and only a recovery by the client continues a call.
            _ if self.session.epoch().lineage != started.lineage => {
                return Err(Next::Fail(IggyError::Unauthenticated));
            }
            Some(error) => return Err(Next::Fail(error)),
            None => {}
        }
        if call.frame.is_some() && call.fence() != Some(self.session.fence()) {
            // A replaced session can never apply the frame. An uncertain call
            // ends with its uncertainty, any other call freezes again.
            if call.uncertain {
                return Err(Next::Fail(IggyError::Unauthenticated));
            }
            call.frame = None;
        }
        if call.frame.is_none() {
            self.wait_for_session(call, started).await?;
        }
        let route = self.route_for(call, started.lineage).await?;
        let context = self.context(call, route.as_ref());
        // The lane comes first, so a blocked lane ends the call at once, also
        // while no link is ready.
        let mut ticket = self.ticket(call, &context).await?;
        let body_len = call
            .frame
            .as_ref()
            .map_or(call.payload.len(), |frame| frame.body.len());
        if let Err(error) = ticket.reserve(call.code, body_len, call.deadline).await {
            call.ticket = Some(ticket);
            return Err(Next::Fail(error));
        }
        let hop = match self.hop(call, route).await {
            Ok(hop) => hop,
            Err(next) => {
                call.ticket = Some(ticket);
                return Err(next);
            }
        };
        let deadline = call.deadline;
        let (frame, ticket) = call.frame(ticket, context, started.lineage)?;
        let request = frame.header;
        let result = hop.link.send(frame, deadline, ticket.sent_flag()).await;
        let sent = *ticket.sent_flag();
        let result = match result {
            Ok(reply) => match decode_reply_for(&request, &reply.header, reply.body) {
                Ok(outcome) => {
                    if request.operation == Operation::NonReplicated
                        && self.session.epoch().lineage != started.lineage
                    {
                        // Progress polls can move a cursor without a replayable receipt.
                        call.uncertain |= call.progress_poll
                            && matches!(
                                &outcome.result,
                                Ok(_) | Err(IggyError::TransientNotCommitted)
                            );
                        return Err(Next::Fail(IggyError::Unauthenticated));
                    }
                    self.session.observe_metadata_reply(&reply.header);
                    let bound = self.session.epoch().bound;
                    close_if_detached(&hop.link, &outcome.result, bound);
                    if outcome.committed && call.uncertain {
                        return match outcome.result {
                            Ok(body) => self.signed_in(call, &hop, body),
                            Err(error) => Err(Next::Committed(error)),
                        };
                    }
                    if bound
                        && request.operation == Operation::NonReplicated
                        && !is_sign_in(call.code)
                        && outcome.result == Err(IggyError::Unauthenticated)
                    {
                        // A detached link refused the read before admission.
                        Err(IggyError::TransientNotAccepted)
                    } else if call.uncertain
                        && !matches!(
                            request.operation,
                            Operation::NonReplicated | Operation::Register | Operation::Logout
                        )
                        && outcome.result.is_ok()
                    {
                        Err(IggyError::TransientNotCommitted)
                    } else {
                        outcome.result
                    }
                }
                Err(_) => {
                    hop.link.close();
                    Err(IggyError::Disconnected)
                }
            },
            // A detached link can still belong to a live session. Its next bind
            // decides whether the frozen request can be replayed safely.
            Err(IggyError::Unauthenticated)
                if request.session != 0 && hop.link.no_session_eviction() =>
            {
                Err(IggyError::Disconnected)
            }
            Err(error) => {
                call.uncertain |= sent
                    && !lost_link(&error)
                    && (call.progress_poll
                        || LaneKey::of(call.code, &context, call.partition_id()).is_some());
                Err(error)
            }
        };
        self.settle(call, &hop, result, sent).await
    }

    /// The error of a call on a client without a link or a retrying slot.
    /// Only a keepalive or a connect brings it back, so the call fails now.
    fn stopped(&self) -> Option<IggyError> {
        match self.links.state() {
            ClientState::Shutdown => Some(IggyError::ClientShutdown),
            ClientState::Disconnected => Some(IggyError::NotConnected),
            _ => None,
        }
    }

    /// A call that started with a session must not continue unbound after
    /// automatic sign-in is rejected, and no call continues in a session
    /// that the caller signed in to after the call started. A wait that
    /// reaches the deadline sent nothing, and the sign-in can still finish,
    /// so it ends with `TransientNotAccepted`.
    async fn wait_for_session(&self, call: &Call, started: SessionEpoch) -> Result<(), Next> {
        if is_sign_in(call.code) || self.session.epoch().bound {
            return Ok(());
        }
        if self.session.sign_in_blocked() {
            return Err(Next::Fail(IggyError::Unauthenticated));
        }
        let requires_session = started.bound || started.sign_in_owed;
        if self.session.sign_in_credentials().is_none() {
            return if requires_session {
                Err(Next::Fail(IggyError::Unauthenticated))
            } else {
                Ok(())
            };
        }
        if !requires_session
            && !matches!(
                self.links.state(),
                ClientState::Connecting | ClientState::Authenticating
            )
        {
            return Err(Next::Fail(IggyError::Unauthenticated));
        }
        let mut epoch = self.session.subscribe_epoch();
        let ready = epoch.wait_for(|epoch| {
            epoch.lineage != started.lineage
                || epoch.bound
                || (requires_session && !epoch.sign_in_owed)
        });
        match timeout_at(call.deadline, ready).await {
            Ok(Ok(epoch)) if epoch.bound && epoch.lineage == started.lineage => Ok(()),
            // The old caller lineage ended or its automatic sign-in was refused.
            Ok(_) => Err(Next::Fail(IggyError::Unauthenticated)),
            Err(_) => Err(Next::Fail(IggyError::TransientNotAccepted)),
        }
    }

    /// The route of a partition call, `None` for any other call. A partition
    /// call looks up its route unless the cached one is usable. The lookup
    /// belongs to the call, so it runs only in the call's `lineage`.
    async fn route_for(
        &self,
        call: &mut Call,
        lineage: u64,
    ) -> Result<Option<PartitionRouteResponse>, Next> {
        let Some(partition) = &call.partition else {
            return Ok(None);
        };
        match self.route(&partition.key, lineage, call.deadline).await {
            Ok(route) => Ok(Some(route)),
            Err(error) if retries_lookup(&error) => {
                call.refusal = IggyError::TransientNotAccepted;
                Err(Next::Pause)
            }
            Err(error) => Err(Next::Fail(error)),
        }
    }

    /// The context that a new frame of `call` carries. Every attempt and
    /// every replay keeps the context of the first frame.
    fn context(&self, call: &Call, route: Option<&PartitionRouteResponse>) -> PartitionContext {
        match (&call.partition, route) {
            (Some(partition), Some(route)) => partition.context.unwrap_or_else(|| route.context()),
            _ if operation_for_code(call.code) == Operation::NonReplicated => self.read_context(),
            _ => PartitionContext::default(),
        }
    }

    /// A read without a partition context waits until its replica applied
    /// the metadata that the session saw.
    fn read_context(&self) -> PartitionContext {
        PartitionContext {
            metadata_op: self.session.metadata_watermark(),
            ..PartitionContext::default()
        }
    }

    /// The lane ticket of `call`. The call enters its lane on its first
    /// attempt and keeps the ticket until it ends.
    async fn ticket(
        &self,
        call: &mut Call,
        context: &PartitionContext,
    ) -> Result<LaneTicket, Next> {
        match call.ticket.take() {
            Some(ticket) => Ok(ticket),
            None => {
                let key = if call.progress_poll {
                    Some(LaneKey::Partition {
                        incarnation: context.incarnation,
                        partition_id: call.partition_id(),
                    })
                } else {
                    LaneKey::of(call.code, context, call.partition_id())
                };
                self.session
                    .enter_lane(key, call.deadline)
                    .await
                    .map_err(Next::Fail)
            }
        }
    }

    /// The link of the next attempt: to the primary of the route, else to
    /// the metadata primary.
    async fn hop(
        &self,
        call: &mut Call,
        route: Option<PartitionRouteResponse>,
    ) -> Result<Hop, Next> {
        let target = route.map_or(Target::MetadataPrimary, |route| {
            Target::Replica(route.primary_replica)
        });
        if let Ok(link) = self.links.link(target) {
            return Ok(Hop { link, route });
        }
        call.refusal = IggyError::TransientNotAccepted;
        match (&call.partition, route) {
            (Some(partition), Some(route)) => {
                self.routes()
                    .failed(&route, partition.key.partition_id, route.primary_replica);
            }
            _ => self.find_metadata_primary(call.deadline).await,
        }
        Err(Next::Pause)
    }

    /// Reads the outcome of one attempt. `sent` tells that this or an
    /// earlier attempt can have taken effect.
    async fn settle(
        &self,
        call: &mut Call,
        hop: &Hop,
        result: Result<Bytes, IggyError>,
        sent: bool,
    ) -> Result<Bytes, Next> {
        let error = match result {
            Ok(body) => return self.signed_in(call, hop, body),
            Err(error) => error,
        };
        if call.progress_poll
            && (error == IggyError::TransientNotCommitted || (sent && lost_link(&error)))
        {
            call.uncertain = true;
            return Err(Next::Fail(IggyError::TransientNotCommitted));
        }
        let repeated = std::mem::replace(
            &mut call.not_primary,
            matches!(error, IggyError::NotPrimary),
        );
        match error {
            IggyError::TransientNotCommitted => {
                call.uncertain = true;
                Err(Next::Pause)
            }
            IggyError::TransientNotAccepted => {
                call.refusal = error;
                call.settle_sent();
                self.refresh(call, hop, Failure::Refused).await;
                Err(Next::Pause)
            }
            IggyError::NotPrimary => {
                // Neither was admitted, so the deadline reports both as 58.
                call.refusal = IggyError::TransientNotAccepted;
                call.settle_sent();
                self.refresh(call, hop, Failure::NotPrimary).await;
                Err(if repeated { Next::Pause } else { Next::Again })
            }
            error if lost_link(&error) => {
                call.uncertain |= sent;
                call.refusal = IggyError::TransientNotAccepted;
                self.refresh(call, hop, Failure::LostLink).await;
                Err(Next::Pause)
            }
            // The refusal spent the request id, so the call starts a new
            // operation in the same lane.
            IggyError::LifecycleBusy if !call.uncertain => {
                call.refusal = error;
                call.frame = None;
                Err(Next::Pause)
            }
            IggyError::HistoryUnavailable => {
                self.invalidate(call, hop);
                if call.restarts_on_new_history() {
                    call.restart();
                    return Err(Next::Again);
                }
                Err(Next::Fail(error))
            }
            IggyError::ConsumerGroupPartitionNotOwned(..) => {
                self.invalidate(call, hop);
                Err(Next::Fail(error))
            }
            error if is_namespace_miss(&error) => {
                call.settle_sent();
                self.invalidate(call, hop);
                let Some(route) = hop.route else {
                    return Err(Next::Fail(error));
                };
                if self
                    .is_metadata_primary(&hop.link, route.primary_replica, call.deadline)
                    .await
                {
                    return Err(Next::Fail(error));
                }
                // A backup can miss metadata that the metadata primary
                // committed. It denied the frame before admission, so the
                // frame goes out again.
                call.refusal = IggyError::TransientNotAccepted;
                Err(Next::Pause)
            }
            error => Err(Next::Fail(error)),
        }
    }

    /// Binds the session of a successful sign-in to the link that carried it,
    /// at the fence that the sign-in started at.
    fn signed_in(&self, call: &Call, hop: &Hop, body: Bytes) -> Result<Bytes, Next> {
        let Some(fence) = call.fence() else {
            return Ok(body);
        };
        match call.code {
            code if is_sign_in(code) => {
                let registered = LoginRegisterResponse::decode_from(&body)
                    .map_err(|_| Next::Fail(IggyError::InvalidFormat))?;
                self.links.mark_bound(&hop.link, fence);
                self.session
                    .bind_at(fence, registered.session)
                    .map_err(Next::Fail)?;
            }
            BIND_SESSION_CODE => self.links.mark_bound(&hop.link, fence),
            _ => {}
        }
        Ok(body)
    }

    /// Learns where the next attempt goes after `hop` failed one.
    async fn refresh(&self, call: &Call, hop: &Hop, failure: Failure) {
        if let (Some(partition), Some(route)) = (&call.partition, &hop.route) {
            let partition_id = partition.key.partition_id;
            let replica = route.primary_replica;
            let mut routes = self.routes();
            match failure {
                Failure::NotPrimary => routes.not_primary(route, partition_id, replica),
                Failure::Refused | Failure::LostLink => {
                    routes.failed(route, partition_id, replica);
                }
            }
        } else if matches!(failure, Failure::LostLink) {
            self.find_metadata_primary(call.deadline).await;
        } else {
            self.read_roster(&hop.link, call.deadline).await;
        }
    }

    fn invalidate(&self, call: &Call, hop: &Hop) {
        if let (Some(partition), Some(route)) = (&call.partition, &hop.route) {
            self.routes().invalidate(route, partition.key.partition_id);
        }
    }

    /// The route of `key`: the cached one if it is usable, else a lookup.
    async fn route(
        &self,
        key: &PartitionKey,
        lineage: u64,
        deadline: Instant,
    ) -> Result<PartitionRouteResponse, IggyError> {
        let fence = self.session.fence();
        let cached = self
            .routes()
            .usable(key, fence, self.session.metadata_watermark());
        match cached {
            Some(route) => Ok(route),
            None => self.lookup(key, fence, lineage, deadline).await,
        }
    }

    /// Asks one ready replica at a time for the route of `key`, in id order.
    /// The first one is the replica that a refusal named, else the metadata
    /// primary. An answer older than the cached route goes to the next one.
    /// A lookup that no replica answers usably ends with 58, which the caller
    /// retries until its deadline.
    async fn lookup(
        &self,
        key: &PartitionKey,
        fence: u64,
        lineage: u64,
        deadline: Instant,
    ) -> Result<PartitionRouteResponse, IggyError> {
        let body = GetPartitionRouteRequest::try_from(key)?.to_bytes();
        let hinted = self.routes().ask_first(key);
        let metadata_primary = self.links.metadata_primary();
        let first = hinted.or(metadata_primary);
        let mut replicas = self.links.ready_replicas();
        let start = first.map_or(0, |first| replicas.partition_point(|id| *id < first));
        replicas.rotate_left(start);
        for replica in replicas {
            let Ok(link) = self.links.link(Target::Replica(replica)) else {
                continue;
            };
            let answer = self
                .read(
                    &link,
                    GET_PARTITION_ROUTE_CODE,
                    body.clone(),
                    lineage,
                    deadline,
                )
                .await
                .and_then(|answer| {
                    PartitionRouteResponse::decode_from(&answer)
                        .map_err(|_| IggyError::InvalidFormat)
                });
            match answer {
                Ok(answer) => {
                    if let Some(route) = self.routes().merge(key, answer, fence) {
                        return Ok(route);
                    }
                }
                Err(error) if retries_lookup(&error) => {}
                Err(error) if is_namespace_miss(&error) => {
                    // A backup can miss metadata that another client just created.
                    // Only the metadata primary can make this miss terminal.
                    if self.is_metadata_primary(&link, replica, deadline).await {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(IggyError::TransientNotAccepted)
    }

    /// Whether `replica` is the metadata primary. The belief can predate a
    /// view change, so the roster read on `link` of that replica must still
    /// name it. No lookup reaches a believed metadata primary without a
    /// link, so then the roster of `replica` is read to update the belief.
    async fn is_metadata_primary(&self, link: &Arc<Link>, replica: u8, deadline: Instant) -> bool {
        let believed = self.links.metadata_primary();
        let unreachable =
            believed.is_none_or(|primary| self.links.link(Target::Replica(primary)).is_err());
        (believed == Some(replica) || unreachable)
            && self.read_roster(link, deadline).await
            && self.links.metadata_primary() == Some(replica)
    }

    /// One read on `link` in `lineage`, without a retry.
    async fn read(
        &self,
        link: &Link,
        code: u32,
        body: Bytes,
        lineage: u64,
        deadline: Instant,
    ) -> Result<Bytes, IggyError> {
        let mut ticket = self.session.enter_lane(None, deadline).await?;
        let frame = ticket.freeze_in(lineage, code, body, self.read_context(), None)?;
        let reply = link.send(frame, deadline, ticket.sent_flag()).await?;
        let result = read_reply(&self.session, reply);
        let bound = self.session.epoch().bound;
        close_if_detached(link, &result, bound);
        if bound && result == Err(IggyError::Unauthenticated) {
            return Err(IggyError::TransientNotAccepted);
        }
        result
    }

    /// Reads the roster on a ready link other than the metadata primary,
    /// after the link to the metadata primary went away.
    async fn find_metadata_primary(&self, deadline: Instant) {
        let primary = self.links.metadata_primary();
        let other = self
            .links
            .ready_replicas()
            .into_iter()
            .find(|replica| Some(*replica) != primary);
        if let Some(link) = other.and_then(|replica| self.links.link(Target::Replica(replica)).ok())
        {
            self.read_roster(&link, deadline).await;
        }
    }

    /// Reads the roster on `link`, so the next attempt finds the metadata
    /// primary that the roster names. Returns whether the read succeeded.
    async fn read_roster(&self, link: &Arc<Link>, deadline: Instant) -> bool {
        match timeout_at(deadline, self.links.refresh_roster(link)).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                debug!("A roster read failed: {error}");
                false
            }
            Err(_) => false,
        }
    }

    fn routes(&self) -> MutexGuard<'_, RouteCache> {
        self.routes.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One call and what its attempts saw so far.
struct Call {
    code: u32,
    payload: Bytes,
    partition: Option<PartitionCall>,
    deadline: Instant,
    ticket: Option<LaneTicket>,
    frame: Option<Frame>,
    /// Replaying a progress poll can advance its offset again without delivering the first result.
    progress_poll: bool,
    /// An attempt of the frame can have taken effect, so the call never
    /// freezes again.
    uncertain: bool,
    /// The error at the deadline, unless an attempt can have taken effect.
    /// A call that waited for a link or a primary until then ends with 58.
    refusal: IggyError,
    retries: u32,
    /// The last attempt got `NotPrimary`. Another one in a row pauses first.
    not_primary: bool,
    /// A send that got `HistoryUnavailable` started over once.
    restarted: bool,
}

struct PartitionCall {
    key: PartitionKey,
    /// The context of the poll reply that the call continues. `None` takes
    /// the context of the route.
    context: Option<PartitionContext>,
}

/// The replica of one attempt.
struct Hop {
    link: Arc<Link>,
    /// The route that chose the replica, `None` for the metadata primary.
    route: Option<PartitionRouteResponse>,
}

/// Cancellation discards only the session owned by this explicit attempt.
pub(crate) struct ExplicitSignIn<'a> {
    driver: &'a OperationDriver,
    lineage: BinaryOperationLineage,
    complete: bool,
}

impl ExplicitSignIn<'_> {
    pub(crate) fn lineage(&self) -> BinaryOperationLineage {
        self.lineage
    }

    pub(crate) fn complete(mut self) {
        self.complete = true;
    }
}

impl Drop for ExplicitSignIn<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.driver
                .session
                .fail_sign_in(self.lineage.lineage(), || self.driver.links.disconnect());
        }
    }
}

/// What a call does after an attempt that did not end it.
enum Next {
    Again,
    Pause,
    Fail(IggyError),
    Committed(IggyError),
}

/// How an attempt failed before a definitive outcome.
enum Failure {
    Refused,
    NotPrimary,
    LostLink,
}

impl Call {
    fn new(code: u32, payload: Bytes, target: RequestTarget<'_>, deadline: Instant) -> Self {
        let partition = match target {
            RequestTarget::Metadata => None,
            RequestTarget::Partition(target) => Some(PartitionCall {
                key: PartitionKey::from(&target),
                context: target.context,
            }),
        };
        Self {
            code,
            payload,
            partition,
            deadline,
            ticket: None,
            frame: None,
            progress_poll: false,
            uncertain: false,
            refusal: IggyError::TransientNotAccepted,
            retries: 0,
            not_primary: false,
            restarted: false,
        }
    }

    /// A replay of a retained operation, uncertain from its first attempt.
    fn resumed(ticket: LaneTicket, retained: RetainedOperation, deadline: Instant) -> Self {
        let RetainedOperation {
            code,
            frame,
            partition,
            ..
        } = retained;
        Self {
            partition: partition.map(|key| PartitionCall { key, context: None }),
            ticket: Some(ticket),
            frame: Some(frame),
            uncertain: true,
            ..Self::new(code, Bytes::new(), RequestTarget::Metadata, deadline)
        }
    }

    /// The fence of the frame, if the call froze one.
    fn fence(&self) -> Option<u64> {
        self.ticket.as_ref().map(LaneTicket::fence)
    }

    fn partition_id(&self) -> u32 {
        self.partition
            .as_ref()
            .map_or(0, |partition| partition.key.partition_id)
    }

    /// The frame of the call and its ticket. The call freezes again only
    /// after it started over, and only in the `lineage` that it started in.
    fn frame(
        &mut self,
        ticket: LaneTicket,
        context: PartitionContext,
        lineage: u64,
    ) -> Result<(Frame, &mut LaneTicket), Next> {
        let ticket = self.ticket.insert(ticket);
        let frame = match &self.frame {
            Some(frame) => {
                if !ticket.revalidate(lineage).map_err(Next::Fail)? {
                    return Err(Next::Again);
                }
                frame.clone()
            }
            None => {
                let partition = self
                    .partition
                    .as_ref()
                    .map(|partition| partition.key.clone());
                let frame = ticket
                    .freeze_in(lineage, self.code, self.payload.clone(), context, partition)
                    .map_err(Next::Fail)?;
                self.frame = Some(frame.clone());
                frame
            }
        };
        Ok((frame, ticket))
    }

    /// Whether a send that got `HistoryUnavailable` starts over under the
    /// next route. Only a send without a captured context starts over, and
    /// only once.
    fn restarts_on_new_history(&self) -> bool {
        self.code == SEND_MESSAGES_CODE
            && self
                .partition
                .as_ref()
                .is_some_and(|partition| partition.context.is_none())
            && !self.uncertain
            && !self.restarted
    }

    /// Starts the call over as a new operation, in the lane of the next
    /// route.
    fn restart(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            ticket.complete();
        }
        self.frame = None;
        self.restarted = true;
    }

    /// Between attempts, the ticket counts the frame as sent only while an
    /// attempt can have taken effect. So a call dropped during a pause
    /// retains only such a frame.
    fn settle_sent(&mut self) {
        let uncertain = self.uncertain;
        if let Some(ticket) = &mut self.ticket {
            *ticket.sent_flag() = uncertain;
        }
    }

    /// Sleeps before the next attempt. Returns false instead if the attempt
    /// cannot start before the deadline.
    async fn pause(&mut self) -> bool {
        let retry = self.retries;
        self.retries = retry.saturating_add(1);
        pause_before_retry(retry, self.deadline, rand::random()).await
    }

    /// Ends the call with the outcome that it proved.
    fn complete(&mut self, outcome: Result<(), IggyError>) {
        if let Some(ticket) = self.ticket.take() {
            ticket.prove(outcome);
        }
    }

    /// Ends the call with `error`. A call that can have taken effect ends
    /// with its uncertainty instead.
    fn end(mut self, error: IggyError) -> IggyError {
        match self.ticket.take() {
            Some(ticket) if self.uncertain => ticket.retain(),
            Some(ticket) => {
                ticket.complete();
                error
            }
            None => error,
        }
    }

    /// Ends the call at its deadline with its last refusal. A sign-in that no
    /// primary answered was not accepted, also after it was sent.
    fn expire(mut self) -> IggyError {
        let refusal = std::mem::replace(&mut self.refusal, IggyError::TransientNotAccepted);
        if is_sign_in(self.code) {
            self.uncertain = false;
        }
        self.end(refusal)
    }
}

/// The new username a user patch sets, if it sets one.
fn patched_username(options: &WireOptions) -> Option<&str> {
    let key = <&str>::from(OptionKey::Username).as_bytes();
    options
        .into_iter()
        .find(|entry| entry.key == key && entry.value_kind.0 == HeaderKind::String.as_code())
        .and_then(|entry| std::str::from_utf8(entry.value).ok())
}

fn is_sign_in(code: u32) -> bool {
    matches!(code, LOGIN_REGISTER_CODE | LOGIN_REGISTER_WITH_PAT_CODE)
}

/// Whether another replica, or the same one later, can answer a lookup that
/// failed with `error`.
fn retries_lookup(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::TransientNotAccepted | IggyError::NotPrimary
    ) || lost_link(error)
}

/// Whether `error` tells that the metadata of the replica has no such
/// stream, topic or partition.
fn is_namespace_miss(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::StreamIdNotFound(_)
            | IggyError::StreamNameNotFound(_)
            | IggyError::TopicIdNotFound(_, _)
            | IggyError::TopicNameNotFound(_, _)
            | IggyError::PartitionNotFound(_, _, _)
    )
}

/// State transfer can detach a link while its session remains live.
/// Rebinding distinguishes that case from an ended session that needs sign-in.
fn close_if_detached(link: &Link, result: &Result<Bytes, IggyError>, bound: bool) {
    if matches!(result, Err(IggyError::StaleClient))
        || (bound && matches!(result, Err(IggyError::Unauthenticated)))
    {
        link.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::links::OPERATION_TIMEOUT;
    use crate::links::RETRY_BACKOFF_MAX;
    use crate::links::fake_cluster::{
        Answer, FIRST_SESSION, FakeCluster, Received, TEST_BUDGET, UNANSWERED_CODE, connected,
        endpoint, eventually, links, policy, topic_body,
    };
    use crate::tcp::tcp_client::TcpClient;
    use bytes::BytesMut;
    use iggy_binary_protocol::codes::{
        CREATE_STREAM_CODE, GET_CLUSTER_METADATA_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE,
        STORE_CONSUMER_OFFSET_CODE,
    };
    use iggy_binary_protocol::consensus::EvictionReason;
    use iggy_binary_protocol::requests::messages::{RawMessage, SendMessagesEncoder};
    use iggy_binary_protocol::{WireConsumer, WireIdentifier};
    use iggy_common::{
        AutoLogin, Client, Credentials, DiagnosticEvent, IggyMessage, MessageClient, Partitioning,
        PendingOperationsConfig, UserClient,
    };
    use secrecy::SecretString;
    use std::iter::repeat_n;
    use std::num::NonZeroUsize;
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    const REPLICAS: u8 = 3;
    const PRIMARY: u8 = 1;
    const PARTITION_ID: u32 = 1;
    const PAYLOAD: &[u8] = b"payload";
    const COMMIT: u64 = 20;
    /// Long enough for a frame to reach a replica, far below the deadline.
    const CALLER_TIMEOUT: Duration = Duration::from_secs(1);
    /// More answers than one call meets before its deadline.
    const ENDLESS: usize = 1_000;
    /// The second call starts while the first one holds the lane.
    const SECOND_CALL_DELAY: Duration = Duration::from_secs(10);
    /// A user that the caller signs in as instead of the configured one.
    const OTHER_USER: &str = "other";
    const OTHER_PASSWORD: &str = "password";
    /// The user id of a remembered sign-in.
    const USER_ID: u32 = 1;
    /// Far below the deadline, so a call that waits for the deadline fails
    /// the test.
    const RECOVERY_TIMEOUT: Duration = Duration::from_secs(5);
    const ROUTE: PartitionRouteResponse = PartitionRouteResponse {
        incarnation: 7,
        metadata_op: 10,
        view: 1,
        partitions_count: 3,
        primary_replica: PRIMARY,
    };
    const MOVED: PartitionRouteResponse = PartitionRouteResponse {
        view: 2,
        primary_replica: 2,
        ..ROUTE
    };
    const RECREATED: PartitionRouteResponse = PartitionRouteResponse {
        incarnation: 8,
        metadata_op: 30,
        ..ROUTE
    };

    struct Fixture {
        cluster: Arc<FakeCluster>,
        driver: OperationDriver,
        stream_id: Identifier,
        topic_id: Identifier,
    }

    impl Fixture {
        /// A bound client of three replicas, each of which routes the
        /// partition by `ROUTE`.
        async fn new() -> Self {
            let cluster = FakeCluster::new(REPLICAS);
            *cluster.route.lock().unwrap() = Some(ROUTE);
            let links = connected(&cluster, usize::from(REPLICAS)).await;
            Self {
                cluster,
                driver: OperationDriver::new(links),
                stream_id: Identifier::numeric(1).unwrap(),
                topic_id: Identifier::numeric(2).unwrap(),
            }
        }

        fn partition(&self, context: Option<PartitionContext>) -> RequestTarget<'_> {
            RequestTarget::Partition(PartitionTarget {
                stream_id: &self.stream_id,
                topic_id: &self.topic_id,
                partition_id: PARTITION_ID,
                context,
            })
        }

        async fn call(&self, code: u32, target: RequestTarget<'_>) -> Result<Bytes, IggyError> {
            self.driver
                .send(code, Bytes::from_static(PAYLOAD), target)
                .await
        }

        fn poll(&self, auto_commit: bool, strategy: WirePollingStrategy) -> PollMessagesRequest {
            PollMessagesRequest {
                consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::numeric(2),
                partition_id: Some(PARTITION_ID),
                strategy,
                count: 1,
                auto_commit,
            }
        }

        async fn send_messages(&self) -> Result<Bytes, IggyError> {
            self.call(SEND_MESSAGES_CODE, self.partition(None)).await
        }

        /// Queues answers of the partition primary to the next sends.
        fn script_sends(&self, answers: impl IntoIterator<Item = Answer>) {
            self.cluster
                .script(&endpoint(PRIMARY), SEND_MESSAGES_CODE, answers);
        }

        fn sends(&self) -> Vec<Received> {
            self.cluster.received(SEND_MESSAGES_CODE)
        }

        /// Ends a send with an unknown outcome and returns its operation.
        async fn uncertain_send(&self) -> u64 {
            self.script_sends([
                Answer::Refuse(IggyError::TransientNotCommitted),
                Answer::Refuse(IggyError::RequestTooOld),
            ]);
            let result = self.send_messages().await;
            let op = self.sends()[0].header.request;
            assert_eq!(result.unwrap_err(), IggyError::OperationUncertain(op));
            op
        }

        /// A call of `code` to the target of its kind. A poll moves the
        /// consumer offset.
        async fn call_to(&self, code: u32) -> Result<Bytes, IggyError> {
            match code {
                SEND_MESSAGES_CODE => self.send_messages().await,
                POLL_MESSAGES_CODE => {
                    self.driver
                        .poll(&self.poll(true, WirePollingStrategy::first()), None)
                        .await
                }
                _ => self.call(code, RequestTarget::Metadata).await,
            }
        }

        /// Waits until the client dropped the session of `fence` and every
        /// slot serves the next one.
        async fn next_session(&self, fence: u64) {
            eventually("every slot serves a new session", || {
                self.driver.session.fence() > fence
                    && self.driver.links.ready_replicas().len() == usize::from(REPLICAS)
            })
            .await;
        }

        /// Routes the partition to the replica that the client believes is
        /// the metadata primary, and returns that replica.
        fn route_to_metadata_primary(&self) -> u8 {
            let metadata_primary = self.driver.links.metadata_primary().unwrap();
            *self.cluster.route.lock().unwrap() = Some(PartitionRouteResponse {
                primary_replica: metadata_primary,
                ..ROUTE
            });
            metadata_primary
        }

        fn missing_topic(&self) -> IggyError {
            IggyError::TopicIdNotFound(self.topic_id.clone(), self.stream_id.clone())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_raw_send_metadata_when_validating_should_reject_malformed_and_unresolved_before_wire()
     {
        let f = Fixture::new().await;
        for partitioning in [
            WirePartitioning::Balanced,
            WirePartitioning::MessagesKey(b"key".to_vec()),
        ] {
            let payload = encoded_send(&partitioning);
            assert_eq!(
                f.driver.send_raw(SEND_MESSAGES_CODE, payload, None).await,
                Err(IggyError::FeatureUnavailable)
            );
        }
        let mut invalid = encoded_send(&WirePartitioning::PartitionId(PARTITION_ID)).to_vec();
        invalid[..size_of::<u32>()].copy_from_slice(&u32::MAX.to_le_bytes());
        for payload in [Bytes::new(), Bytes::from(invalid)] {
            assert_eq!(
                f.driver.send_raw(SEND_MESSAGES_CODE, payload, None).await,
                Err(IggyError::InvalidCommand)
            );
        }
        assert!(f.sends().is_empty());
        assert_eq!(f.cluster.count(GET_PARTITION_ROUTE_CODE), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_raw_send_with_length_prefixed_metadata_when_sent_should_route_the_unchanged_frame()
     {
        let f = Fixture::new().await;
        let payload = encoded_send(&WirePartitioning::PartitionId(PARTITION_ID));
        f.driver
            .send_raw(SEND_MESSAGES_CODE, payload.clone(), None)
            .await
            .unwrap();
        let sends = f.sends();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].endpoint, endpoint(PRIMARY));
        assert_eq!(sends[0].body, payload);
        assert_eq!(sends[0].header.partition_incarnation, ROUTE.incarnation);
    }

    #[tokio::test(start_paused = true)]
    async fn given_balanced_send_when_partition_disappears_should_refresh_count_and_reselect_only_once()
     {
        for rejected_again in [false, true] {
            let f = Fixture::new().await;
            let route = PartitionRouteResponse {
                partitions_count: 2,
                ..ROUTE
            };
            *f.cluster.route.lock().unwrap() = Some(route);
            f.cluster.script(
                &endpoint(0),
                GET_TOPIC_CODE,
                [Answer::Body(topic_body(2)), Answer::Body(topic_body(1))],
            );
            let missing = IggyError::PartitionNotFound(1, f.topic_id.clone(), f.stream_id.clone());
            // The metadata primary confirms each miss of the partition primary.
            f.cluster.script(
                &endpoint(0),
                GET_PARTITION_ROUTE_CODE,
                [
                    route_body(route),
                    route_body(route),
                    Answer::Refuse(missing.clone()),
                    Answer::Refuse(missing.clone()),
                ],
            );
            let final_answer = if rejected_again {
                Answer::Refuse(missing.clone())
            } else {
                Answer::Commit(COMMIT)
            };
            f.script_sends([
                Answer::Commit(COMMIT),
                Answer::Refuse(missing.clone()),
                final_answer,
            ]);
            let client = TcpClient::from_links(f.driver.links);
            let mut messages = [IggyMessage::from_str("payload").unwrap()];
            client
                .send_messages(
                    &f.stream_id,
                    &f.topic_id,
                    &Partitioning::balanced(),
                    &mut messages,
                )
                .await
                .unwrap();
            let result = client
                .send_messages(
                    &f.stream_id,
                    &f.topic_id,
                    &Partitioning::balanced(),
                    &mut messages,
                )
                .await;
            if rejected_again {
                assert_eq!(result.unwrap_err(), missing);
            } else {
                result.unwrap();
            }
            let sent = f.cluster.received(SEND_MESSAGES_CODE);
            assert_eq!(
                sent.iter().map(sent_partition).collect::<Vec<_>>(),
                [0, 1, 0]
            );
            assert_ne!(sent[1].header.request, sent[2].header.request);
            assert_eq!(f.cluster.count(GET_TOPIC_CODE), 2);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_explicit_or_uncertain_send_when_partition_is_missing_should_never_choose_another()
     {
        for uncertain in [false, true] {
            let f = Fixture::new().await;
            f.cluster
                .script(&endpoint(0), GET_TOPIC_CODE, [Answer::Body(topic_body(2))]);
            let missing = IggyError::PartitionNotFound(1, f.topic_id.clone(), f.stream_id.clone());
            let mut answers = Vec::new();
            if uncertain {
                answers.push(Answer::Refuse(IggyError::TransientNotCommitted));
            }
            answers.push(Answer::Refuse(missing.clone()));
            f.script_sends(answers);
            // The metadata primary confirms the miss of the partition primary.
            f.cluster.script(
                &endpoint(0),
                GET_PARTITION_ROUTE_CODE,
                [route_body(ROUTE), Answer::Refuse(missing.clone())],
            );
            let client = TcpClient::from_links(f.driver.links);
            let partitioning = if uncertain {
                Partitioning::balanced()
            } else {
                Partitioning::partition_id(PARTITION_ID)
            };
            let mut messages = [IggyMessage::from_str("payload").unwrap()];
            let result = client
                .send_messages(&f.stream_id, &f.topic_id, &partitioning, &mut messages)
                .await;
            let sent = f.cluster.received(SEND_MESSAGES_CODE);
            if uncertain {
                assert_eq!(
                    result.unwrap_err(),
                    IggyError::OperationUncertain(sent[0].header.request)
                );
                assert_eq!(sent.len(), 2);
                assert_eq!(
                    bytemuck::bytes_of(&sent[0].header),
                    bytemuck::bytes_of(&sent[1].header)
                );
                assert_eq!(f.cluster.count(GET_TOPIC_CODE), 1);
            } else {
                assert_eq!(result.unwrap_err(), missing);
                assert_eq!(sent.len(), 1);
                assert_eq!(f.cluster.count(GET_TOPIC_CODE), 0);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_balanced_send_when_its_topic_is_gone_at_the_refresh_should_fail_with_topic_not_found()
     {
        let f = Fixture::new().await;
        f.cluster.script(
            &endpoint(0),
            GET_TOPIC_CODE,
            [Answer::Body(topic_body(2)), Answer::Body(Vec::new())],
        );
        let missing = IggyError::PartitionNotFound(0, f.topic_id.clone(), f.stream_id.clone());
        // The metadata primary confirms the miss of the partition primary.
        f.cluster.script(
            &endpoint(0),
            GET_PARTITION_ROUTE_CODE,
            [route_body(ROUTE), Answer::Refuse(missing.clone())],
        );
        f.script_sends([Answer::Refuse(missing)]);
        let client = TcpClient::from_links(f.driver.links);
        let mut messages = [IggyMessage::from_str("payload").unwrap()];

        let result = client
            .send_messages(
                &f.stream_id,
                &f.topic_id,
                &Partitioning::balanced(),
                &mut messages,
            )
            .await;

        assert_eq!(
            result.unwrap_err(),
            IggyError::TopicIdNotFound(f.topic_id.clone(), f.stream_id.clone())
        );
        assert_eq!(f.cluster.count(GET_TOPIC_CODE), 2);
        assert_eq!(f.cluster.count(SEND_MESSAGES_CODE), 1);
    }

    fn sent_partition(sent: &Received) -> u32 {
        let (length, body) = sent.body.split_first_chunk().unwrap();
        let header =
            SendMessagesHeader::decode_from(&body[..u32::from_le_bytes(*length) as usize]).unwrap();
        let WirePartitioning::PartitionId(partition) = header.partitioning else {
            panic!("wire send must name a partition");
        };
        partition
    }

    fn encoded_send(partitioning: &WirePartitioning) -> Bytes {
        let mut payload = BytesMut::new();
        SendMessagesEncoder::encode(
            &mut payload,
            &WireIdentifier::numeric(1),
            &WireIdentifier::numeric(2),
            partitioning,
            &[RawMessage {
                id: 1,
                origin_timestamp: 0,
                headers: None,
                payload: PAYLOAD,
            }],
        )
        .unwrap();
        payload.freeze()
    }

    fn route_body(route: PartitionRouteResponse) -> Answer {
        Answer::Body(route.to_bytes().to_vec())
    }

    fn endpoints(received: &[Received]) -> Vec<String> {
        received
            .iter()
            .map(|request| request.endpoint.clone())
            .collect()
    }

    fn ops(received: &[Received]) -> Vec<u64> {
        received
            .iter()
            .map(|request| request.header.request)
            .collect()
    }

    fn stamped_context(request: &Received) -> PartitionContext {
        PartitionContext {
            incarnation: request.header.partition_incarnation,
            owner_generation: request.header.owner_generation,
            metadata_op: request.header.minimum_metadata_op,
        }
    }

    async fn given_a_frozen_retry_when_session_changes_during_lookup_should_fence_the_frame_and_keep_its_id(
        caller_changed: bool,
        uncertain: bool,
    ) {
        let fixture = Fixture::new().await;
        fixture.cluster.hold(SEND_MESSAGES_CODE);
        fixture.script_sends([
            Answer::Refuse(if uncertain {
                IggyError::TransientNotCommitted
            } else {
                IggyError::TransientNotAccepted
            }),
            Answer::Refuse(IggyError::RequestTooOld),
        ]);
        let (result, ()) = tokio::join!(fixture.send_messages(), async {
            eventually("first send freezes the retry", || {
                fixture.cluster.count(SEND_MESSAGES_CODE) == 1
            })
            .await;
            fixture.driver.routes().invalidate(&ROUTE, PARTITION_ID);
            fixture.cluster.hold(GET_PARTITION_ROUTE_CODE);
            fixture.cluster.release(SEND_MESSAGES_CODE);
            eventually("retry waits for a fresh route", || {
                fixture.cluster.count(GET_PARTITION_ROUTE_CODE) == 2
            })
            .await;
            let links: Vec<_> = fixture
                .driver
                .links
                .ready_replicas()
                .into_iter()
                .map(|replica| fixture.driver.links.link(Target::Replica(replica)).unwrap())
                .collect();
            let fence = fixture.driver.session.fence();
            fixture
                .driver
                .session
                .reset_if_fence(fence, !caller_changed)
                .unwrap();
            fixture.driver.session.bind(FIRST_SESSION + 1).unwrap();
            *fixture.cluster.session.lock().unwrap() = Some(FIRST_SESSION + 1);
            for link in links {
                fixture
                    .driver
                    .links
                    .mark_bound(&link, fixture.driver.session.fence());
            }
            fixture.cluster.release(GET_PARTITION_ROUTE_CODE);
        });
        let sends = fixture.sends();
        let operation = sends[0].header.request;
        if uncertain {
            assert_eq!(
                result,
                Err(IggyError::OperationUncertain(operation)),
                "caller_changed={caller_changed}"
            );
            assert_eq!(
                sends.len(),
                1,
                "an uncertain old frame cannot enter the new session"
            );
        } else if caller_changed {
            assert_eq!(result, Err(IggyError::Unauthenticated));
            assert_eq!(sends.len(), 1);
        } else {
            assert_eq!(result, Err(IggyError::RequestTooOld));
            assert_eq!(sends.len(), 2);
            assert_eq!(sends[1].header.session, FIRST_SESSION + 1);
            assert_ne!(sends[1].header.request, operation);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_frozen_retry_when_session_changes_during_lookup_should_fence_the_frame_and_keep_its_id_recovery_unsent()
     {
        given_a_frozen_retry_when_session_changes_during_lookup_should_fence_the_frame_and_keep_its_id(false, false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_frozen_retry_when_session_changes_during_lookup_should_fence_the_frame_and_keep_its_id_recovery_uncertain()
     {
        given_a_frozen_retry_when_session_changes_during_lookup_should_fence_the_frame_and_keep_its_id(false, true).await;
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_frozen_retry_when_session_changes_during_lookup_should_fence_the_frame_and_keep_its_id_caller_unsent()
     {
        given_a_frozen_retry_when_session_changes_during_lookup_should_fence_the_frame_and_keep_its_id(true, false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_frozen_retry_when_session_changes_during_lookup_should_fence_the_frame_and_keep_its_id_caller_uncertain()
     {
        given_a_frozen_retry_when_session_changes_during_lookup_should_fence_the_frame_and_keep_its_id(true, true).await;
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_refused_metadata_write_when_cancelled_during_roster_refresh_should_retain_only_prior_uncertainty()
     {
        for uncertain in [false, true] {
            for refusal in [IggyError::TransientNotAccepted, IggyError::NotPrimary] {
                let f = Fixture::new().await;
                let mut answers = Vec::new();
                if uncertain {
                    answers.push(Answer::Refuse(IggyError::TransientNotCommitted));
                }
                answers.push(Answer::Refuse(refusal));
                f.cluster.script(&endpoint(0), CREATE_STREAM_CODE, answers);
                f.cluster
                    .script(&endpoint(0), GET_CLUSTER_METADATA_CODE, [Answer::Silence]);
                let roster_count = f.cluster.count(GET_CLUSTER_METADATA_CODE);

                assert!(
                    timeout(
                        CALLER_TIMEOUT,
                        f.call(CREATE_STREAM_CODE, RequestTarget::Metadata)
                    )
                    .await
                    .is_err()
                );

                assert_eq!(f.cluster.count(GET_CLUSTER_METADATA_CODE), roster_count + 1);
                let op = f.cluster.received(CREATE_STREAM_CODE)[0].header.request;
                assert_eq!(f.driver.session.retains_operation(op), uncertain);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_58_89_and_a_dropped_link_when_retrying_should_resend_the_same_header_and_body() {
        let f = Fixture::new().await;
        f.script_sends([
            Answer::Refuse(IggyError::TransientNotAccepted),
            Answer::HangUp,
            Answer::Refuse(IggyError::NotPrimary),
        ]);
        f.cluster.script(
            &endpoint(PRIMARY),
            GET_PARTITION_ROUTE_CODE,
            [route_body(MOVED)],
        );

        f.send_messages().await.unwrap();

        let sends = f.sends();
        assert_eq!(
            endpoints(&sends),
            [endpoint(1), endpoint(1), endpoint(1), endpoint(2)]
        );
        for send in &sends {
            assert_eq!(
                bytemuck::bytes_of(&send.header),
                bytemuck::bytes_of(&sends[0].header)
            );
            assert_eq!(send.body, PAYLOAD);
        }
        assert_eq!(stamped_context(&sends[0]), ROUTE.context());
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_57_when_later_attempts_fail_should_return_operation_uncertain() {
        // 58 and 89 keep the call going until its deadline. Every other
        // refusal ends it at once.
        let refusals = [
            IggyError::TransientNotAccepted,
            IggyError::NotPrimary,
            IggyError::HistoryUnavailable,
            IggyError::ConsumerGroupPartitionNotOwned(1, 1),
            IggyError::InvalidCredentials,
            IggyError::RequestTooOld,
            IggyError::LifecycleBusy,
        ];
        for refusal in refusals {
            let f = Fixture::new().await;
            f.script_sends([Answer::Refuse(IggyError::TransientNotCommitted)]);
            f.script_sends(repeat_n(Answer::Refuse(refusal.clone()), ENDLESS));

            let result = f.send_messages().await;

            let op = f.sends()[0].header.request;
            assert!(ops(&f.sends()).iter().all(|sent| *sent == op), "{refusal}");
            assert_eq!(
                result.unwrap_err(),
                IggyError::OperationUncertain(op),
                "{refusal}"
            );
            assert!(f.driver.abandon(op).is_ok(), "{refusal}: not retained");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_silent_partition_primary_when_the_deadline_passes_should_retain_the_sent_operation()
     {
        let f = Fixture::new().await;
        f.script_sends([Answer::Silence]);
        let started = Instant::now();

        let result = f.send_messages().await;

        let op = f.sends()[0].header.request;
        assert_eq!(result.unwrap_err(), IggyError::OperationUncertain(op));
        assert!(started.elapsed() <= OPERATION_TIMEOUT);
        assert_eq!(
            f.send_messages().await.unwrap_err(),
            IggyError::LaneBlocked(op)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_silent_replica_when_a_lookup_reaches_the_deadline_should_send_no_late_frame() {
        let f = Fixture::new().await;
        f.cluster
            .script(&endpoint(0), GET_PARTITION_ROUTE_CODE, [Answer::Silence]);

        let result = f.send_messages().await;

        assert_eq!(result.unwrap_err(), IggyError::TransientNotAccepted);
        // Lets a frame that left after the deadline reach its replica.
        sleep(CALLER_TIMEOUT).await;
        assert_eq!(f.cluster.count(GET_PARTITION_ROUTE_CODE), 1);
        assert!(f.sends().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_read_past_its_send_point_when_the_deadline_passes_should_return_57() {
        let f = Fixture::new().await;
        let started = Instant::now();

        let result = f.call(UNANSWERED_CODE, RequestTarget::Metadata).await;

        assert_eq!(result.unwrap_err(), IggyError::TransientNotCommitted);
        assert!(started.elapsed() <= OPERATION_TIMEOUT);
        assert_eq!(f.cluster.count(UNANSWERED_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_89_on_a_metadata_request_when_retrying_should_read_12_on_that_link_and_keep_the_identity()
     {
        let f = Fixture::new().await;
        f.cluster.elect(1);
        f.cluster.script(
            &endpoint(0),
            CREATE_STREAM_CODE,
            [Answer::Refuse(IggyError::NotPrimary)],
        );
        f.cluster
            .script(&endpoint(1), CREATE_STREAM_CODE, [Answer::Commit(COMMIT)]);

        f.call(CREATE_STREAM_CODE, RequestTarget::Metadata)
            .await
            .unwrap();

        let creates = f.cluster.received(CREATE_STREAM_CODE);
        assert_eq!(endpoints(&creates), [endpoint(0), endpoint(1)]);
        assert_eq!(ops(&creates), [creates[0].header.request; 2]);
        assert!(
            f.cluster
                .codes(&endpoint(0))
                .ends_with(&[CREATE_STREAM_CODE, GET_CLUSTER_METADATA_CODE])
        );
        assert_eq!(f.driver.links.metadata_primary(), Some(1));
        assert_eq!(f.driver.session.metadata_watermark(), COMMIT);
    }

    #[tokio::test(start_paused = true)]
    async fn given_89_from_a_backup_when_looking_up_should_ask_that_backup_first_and_raise_the_view_floor()
     {
        let f = Fixture::new().await;
        f.script_sends([Answer::Refuse(IggyError::NotPrimary)]);
        f.cluster
            .script(&endpoint(2), GET_PARTITION_ROUTE_CODE, [route_body(MOVED)]);

        f.send_messages().await.unwrap();

        let lookups = f.cluster.received(GET_PARTITION_ROUTE_CODE);
        assert_eq!(endpoints(&lookups), [endpoint(0), endpoint(1), endpoint(2)]);
        assert_eq!(endpoints(&f.sends()), [endpoint(1), endpoint(2)]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_88_before_uncertainty_when_retrying_should_start_a_new_operation() {
        let f = Fixture::new().await;
        f.script_sends([Answer::Refuse(IggyError::LifecycleBusy)]);

        f.send_messages().await.unwrap();

        let sends = f.sends();
        assert_eq!(sends.len(), 2);
        assert_ne!(sends[0].header.request, sends[1].header.request);
        assert_eq!(sends[1].body, sends[0].body);
        assert_eq!(stamped_context(&sends[1]), stamped_context(&sends[0]));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_committed_lifecycle_refusal_when_retrying_should_restart_only_before_uncertainty()
     {
        for uncertain in [false, true] {
            let fixture = Fixture::new().await;
            let mut answers = Vec::new();
            if uncertain {
                answers.push(Answer::Refuse(IggyError::TransientNotCommitted));
            }
            answers.push(Answer::CommitRejected(COMMIT, IggyError::LifecycleBusy));
            answers.push(Answer::Commit(COMMIT + 1));
            fixture
                .cluster
                .script(&endpoint(0), CREATE_STREAM_CODE, answers);

            let result = fixture
                .call(CREATE_STREAM_CODE, RequestTarget::Metadata)
                .await;

            if uncertain {
                assert_eq!(result, Err(IggyError::LifecycleBusy));
            } else {
                result.unwrap();
            }
            let requests = fixture.cluster.received(CREATE_STREAM_CODE);
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests[0].header.request == requests[1].header.request,
                uncertain
            );
            assert_eq!(requests[0].body, requests[1].body);
            fixture
                .call(CREATE_STREAM_CODE, RequestTarget::Metadata)
                .await
                .unwrap();
            let next = fixture.cluster.received(CREATE_STREAM_CODE);
            assert_ne!(next[2].header.request, requests[1].header.request);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_87_on_a_send_when_refreshed_should_start_one_new_operation_under_the_new_incarnation()
     {
        let f = Fixture::new().await;
        *f.cluster.route.lock().unwrap() = Some(RECREATED);
        f.cluster
            .script(&endpoint(0), GET_PARTITION_ROUTE_CODE, [route_body(ROUTE)]);
        f.script_sends([Answer::Refuse(IggyError::HistoryUnavailable)]);

        f.send_messages().await.unwrap();

        let sends = f.sends();
        assert_eq!(sends.len(), 2);
        assert_eq!(stamped_context(&sends[0]), ROUTE.context());
        assert_eq!(stamped_context(&sends[1]), RECREATED.context());
        assert_ne!(sends[0].header.request, sends[1].header.request);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_send_that_cannot_start_over_when_its_history_is_gone_should_fail_with_history_unavailable()
     {
        // Without a context, a send starts over once. A captured context
        // never starts over.
        let cases = [(None, 2), (Some(ROUTE.context()), 1)];
        for (context, frames) in cases {
            let f = Fixture::new().await;
            f.script_sends([
                Answer::Refuse(IggyError::HistoryUnavailable),
                Answer::Refuse(IggyError::HistoryUnavailable),
            ]);

            let result = f.call(SEND_MESSAGES_CODE, f.partition(context)).await;

            assert_eq!(result.unwrap_err(), IggyError::HistoryUnavailable);
            assert_eq!(f.sends().len(), frames, "{context:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_store_without_position_when_5009_should_return_without_retry() {
        let f = Fixture::new().await;
        f.cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Refuse(IggyError::ConsumerGroupPartitionNotOwned(
                1, 1,
            ))],
        );

        let refused = f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None)).await;
        f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
            .await
            .unwrap();

        assert!(matches!(
            refused,
            Err(IggyError::ConsumerGroupPartitionNotOwned(..))
        ));
        assert_eq!(f.cluster.count(STORE_CONSUMER_OFFSET_CODE), 2);
        assert_eq!(f.cluster.count(GET_PARTITION_ROUTE_CODE), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_first_polls_and_positions_when_sent_should_stamp_owner_0_or_the_poll_reply_owner()
     {
        let f = Fixture::new().await;
        let polled = PartitionContext {
            owner_generation: 42,
            metadata_op: 15,
            ..ROUTE.context()
        };

        f.driver
            .poll(&f.poll(false, WirePollingStrategy::first()), None)
            .await
            .unwrap();
        f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(Some(polled)))
            .await
            .unwrap();

        let polls = f.cluster.received(POLL_MESSAGES_CODE);
        let stores = f.cluster.received(STORE_CONSUMER_OFFSET_CODE);
        assert_eq!(ROUTE.context().owner_generation, 0);
        assert_eq!(stamped_context(&polls[0]), ROUTE.context());
        assert_eq!(stamped_context(&stores[0]), polled);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_retained_op_when_resolved_after_commit_should_return_ok_and_free_the_lane() {
        let f = Fixture::new().await;
        let op = f.uncertain_send().await;

        f.driver
            .resolve(op, Instant::now() + OPERATION_TIMEOUT)
            .await
            .unwrap();
        f.send_messages().await.unwrap();

        let sends = f.sends();
        assert_eq!(ops(&sends[..3]), [op; 3]);
        assert_eq!(sends[2].body, PAYLOAD);
        assert_ne!(sends[3].header.request, op);
        let deadline = Instant::now() + OPERATION_TIMEOUT;
        assert_eq!(f.driver.resolve(op, deadline).await, Ok(()));
        let never_uncertain = sends[3].header.request;
        assert_eq!(
            f.driver.resolve(never_uncertain, deadline).await,
            Err(IggyError::OperationNotRetained(never_uncertain))
        );
        assert_eq!(f.sends().len(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_full_mutation_budget_when_resolving_should_reuse_the_retained_reservation() {
        let mut fixture = Fixture::new().await;
        let session = Arc::new(SessionOwner::with_pending_operations(
            AutoLogin::Enabled(Credentials::UsernamePassword(
                "iggy".to_owned(),
                SecretString::from("iggy"),
            )),
            PendingOperationsConfig {
                max_operations: NonZeroUsize::MIN,
                max_bytes: NonZeroUsize::new(iggy_binary_protocol::HEADER_SIZE + PAYLOAD.len())
                    .unwrap(),
            },
        ));
        fixture.driver = OperationDriver::new(ReplicaLinks::new(
            fixture.cluster.dialer(),
            endpoint(0),
            policy(),
            session,
        ));
        fixture.driver.links.connect().await.unwrap();
        let operation = fixture.uncertain_send().await;

        fixture
            .driver
            .resolve(operation, Instant::now() + OPERATION_TIMEOUT)
            .await
            .unwrap();
        fixture.send_messages().await.unwrap();

        let sends = fixture.sends();
        assert_eq!(ops(&sends[..3]), [operation; 3]);
        assert_eq!(sends[2].body, PAYLOAD);
        assert_ne!(sends[3].header.request, operation);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_store_when_a_committed_rejection_replays_should_return_it_and_free_the_lane()
     {
        let f = Fixture::new().await;
        f.cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [
                Answer::Refuse(IggyError::TransientNotCommitted),
                Answer::Refuse(IggyError::RequestTooOld),
                Answer::CommitRejected(COMMIT, IggyError::InvalidOffset(0)),
            ],
        );
        let failed = f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None)).await;
        let op = f.cluster.received(STORE_CONSUMER_OFFSET_CODE)[0]
            .header
            .request;
        assert_eq!(failed, Err(IggyError::OperationUncertain(op)));

        for _ in 0..2 {
            let resolved = f
                .driver
                .resolve(op, Instant::now() + OPERATION_TIMEOUT)
                .await;
            assert_eq!(resolved, Err(IggyError::InvalidOffset(0)));
        }

        assert_eq!(
            f.driver.abandon(op),
            Err(IggyError::OperationNotRetained(op))
        );
        f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
            .await
            .unwrap();
        let stores = f.cluster.received(STORE_CONSUMER_OFFSET_CODE);
        assert_eq!(stores.len(), 4);
        assert_eq!(ops(&stores[..3]), [op; 3]);
        assert_ne!(stores[3].header.request, op);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_send_when_an_unproved_reply_arrives_should_keep_its_lane() {
        for answer in [
            Answer::Refuse(IggyError::Unauthorized),
            Answer::Body(Vec::new()),
        ] {
            let f = Fixture::new().await;
            let op = f.uncertain_send().await;
            f.script_sends(repeat_n(answer, ENDLESS));

            let resolved = f.driver.resolve(op, Instant::now() + CALLER_TIMEOUT).await;

            assert_eq!(resolved, Err(IggyError::OperationUncertain(op)));
            assert_eq!(f.send_messages().await, Err(IggyError::LaneBlocked(op)));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_uncertain_poll_progress_when_a_reply_is_lost_should_end_without_replay() {
        for (auto_commit, strategy, progress) in [
            (true, WirePollingStrategy::first(), true),
            (false, WirePollingStrategy::next(), true),
            (false, WirePollingStrategy::first(), false),
        ] {
            for (failure, uncertain) in [
                (Answer::Refuse(IggyError::TransientNotCommitted), true),
                (Answer::HangUp, true),
                (Answer::Refuse(IggyError::TransientNotAccepted), false),
            ] {
                let f = Fixture::new().await;
                f.cluster.script(
                    &endpoint(PRIMARY),
                    POLL_MESSAGES_CODE,
                    [failure, Answer::Body(Vec::new())],
                );
                let result = f
                    .driver
                    .poll(&f.poll(auto_commit, strategy.clone()), None)
                    .await;
                let polls = f.cluster.received(POLL_MESSAGES_CODE);
                if progress && uncertain {
                    assert_eq!(result, Err(IggyError::TransientNotCommitted));
                    assert_eq!(polls.len(), 1, "uncertain progress must not be replayed");
                    let operation_id = polls[0].header.request;
                    assert_eq!(
                        f.driver.abandon(operation_id),
                        Err(IggyError::OperationNotRetained(operation_id))
                    );
                    f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
                        .await
                        .unwrap();
                } else {
                    assert_eq!(result, Ok(Bytes::new()));
                    assert_eq!(polls.len(), 2, "pure reads and unaccepted polls may retry");
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_progress_poll_when_cancelled_should_release_the_barrier_without_retention() {
        for (auto_commit, strategy) in [
            (true, WirePollingStrategy::first()),
            (false, WirePollingStrategy::next()),
        ] {
            let f = Fixture::new().await;
            f.cluster
                .script(&endpoint(PRIMARY), POLL_MESSAGES_CODE, [Answer::Silence]);
            let request = f.poll(auto_commit, strategy);
            let poll = f.driver.poll(&request, None);
            let store = f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None));
            tokio::pin!(store);
            let expired = timeout(CALLER_TIMEOUT, async {
                tokio::pin!(poll);
                tokio::select! {
                    _ = &mut poll => panic!("the unanswered poll returned"),
                    () = eventually("the progress poll arrived", || f.cluster.count(POLL_MESSAGES_CODE) == 1) => {}
                }
                tokio::select! {
                    _ = &mut poll => panic!("the unanswered poll returned"),
                    _ = &mut store => panic!("the store bypassed the progress barrier"),
                }
            }).await;
            assert!(expired.is_err());
            assert_eq!(f.cluster.count(STORE_CONSUMER_OFFSET_CODE), 0);
            let poll_id = f.cluster.received(POLL_MESSAGES_CODE)[0].header.request;
            assert_eq!(
                f.driver.abandon(poll_id),
                Err(IggyError::OperationNotRetained(poll_id))
            );

            store.await.unwrap();
            assert_eq!(f.cluster.count(STORE_CONSUMER_OFFSET_CODE), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_retained_mutation_when_progress_poll_enters_should_return_its_blocking_id() {
        let f = Fixture::new().await;
        let op = f.uncertain_send().await;

        let polled = f
            .driver
            .poll(&f.poll(false, WirePollingStrategy::next()), None)
            .await;

        assert_eq!(polled, Err(IggyError::LaneBlocked(op)));
        assert_eq!(f.cluster.count(POLL_MESSAGES_CODE), 0);
        f.driver
            .poll(&f.poll(false, WirePollingStrategy::first()), None)
            .await
            .expect("a poll that moves no consumer takes no lane");
        assert_eq!(f.cluster.count(POLL_MESSAGES_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_shared_deadline_when_a_nested_call_runs_should_keep_the_earliest_deadline() {
        let f = Fixture::new().await;
        let started = Instant::now();
        let deadline = started + CALLER_TIMEOUT;

        let result = with_deadline(
            deadline,
            with_deadline(
                started + OPERATION_TIMEOUT,
                f.call(UNANSWERED_CODE, RequestTarget::Metadata),
            ),
        )
        .await;

        assert_eq!(result, Err(IggyError::TransientNotCommitted));
        assert_eq!(Instant::now(), deadline);
        assert!(operation_deadline() > deadline);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_operation_when_abandoned_should_free_the_lane_without_a_replay() {
        let f = Fixture::new().await;
        let op = f.uncertain_send().await;

        f.driver.abandon(op).unwrap();
        f.send_messages().await.unwrap();

        let sends = f.sends();
        assert_eq!(sends.len(), 3);
        assert_ne!(sends[2].header.request, op);
        assert_eq!(
            f.driver.abandon(op).unwrap_err(),
            IggyError::OperationNotRetained(op)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_replaced_session_when_resolving_an_uncertain_operation_should_report_it_not_retained()
     {
        let f = Fixture::new().await;
        let op = f.uncertain_send().await;
        let fence = f.driver.session.fence();

        *f.cluster.session.lock().unwrap() = None;
        for id in 0..REPLICAS {
            f.cluster.drop_connections(&endpoint(id));
        }
        eventually("every slot serves a new session", || {
            f.driver.session.fence() > fence
                && f.driver.links.ready_replicas().len() == usize::from(REPLICAS)
        })
        .await;
        let resolved = f
            .driver
            .resolve(op, Instant::now() + OPERATION_TIMEOUT)
            .await;

        assert_eq!(resolved.unwrap_err(), IggyError::OperationNotRetained(op));
        f.send_messages().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_future_dropped_after_write_when_the_next_op_enters_should_fail_with_lane_blocked()
     {
        let f = Fixture::new().await;
        f.script_sends([Answer::Silence]);

        let gave_up = timeout(CALLER_TIMEOUT, f.send_messages()).await;

        assert!(gave_up.is_err());
        let op = f.sends()[0].header.request;
        assert_eq!(
            f.send_messages().await.unwrap_err(),
            IggyError::LaneBlocked(op)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_caller_that_gave_up_after_a_refusal_when_entering_the_lane_should_send_a_new_operation()
     {
        let f = Fixture::new().await;
        f.script_sends([Answer::Refuse(IggyError::TransientNotAccepted)]);
        // The lookup after the refusal never ends, so the caller gives up
        // there.
        f.cluster
            .script(&endpoint(2), GET_PARTITION_ROUTE_CODE, [Answer::Silence]);

        let gave_up = timeout(CALLER_TIMEOUT, f.send_messages()).await;
        f.send_messages().await.unwrap();

        assert!(gave_up.is_err());
        let sends = f.sends();
        assert_eq!(sends.len(), 2);
        assert_ne!(sends[0].header.request, sends[1].header.request);
    }

    #[tokio::test(start_paused = true)]
    async fn given_replicas_that_refuse_every_lookup_when_the_deadline_passes_should_fail_with_58()
    {
        for refusal in [IggyError::TransientNotAccepted, IggyError::NotPrimary] {
            let f = Fixture::new().await;
            for replica in 0..REPLICAS {
                f.cluster.script(
                    &endpoint(replica),
                    GET_PARTITION_ROUTE_CODE,
                    repeat_n(Answer::Refuse(refusal.clone()), ENDLESS),
                );
            }
            let started = Instant::now();

            let result = f.send_messages().await;

            assert_eq!(
                result.unwrap_err(),
                IggyError::TransientNotAccepted,
                "{refusal}"
            );
            let elapsed = started.elapsed();
            assert!(elapsed <= OPERATION_TIMEOUT, "{refusal}: {elapsed:?}");
            assert!(
                elapsed >= OPERATION_TIMEOUT - RETRY_BACKOFF_MAX,
                "{refusal}: {elapsed:?}"
            );
            assert!(f.sends().is_empty(), "{refusal}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_metadata_commit_above_a_cached_route_when_sending_should_look_up_again_at_the_new_watermark()
     {
        let f = Fixture::new().await;
        f.send_messages().await.unwrap();
        f.cluster
            .script(&endpoint(0), CREATE_STREAM_CODE, [Answer::Commit(COMMIT)]);
        f.call(CREATE_STREAM_CODE, RequestTarget::Metadata)
            .await
            .unwrap();
        let current = PartitionRouteResponse {
            metadata_op: COMMIT,
            ..ROUTE
        };
        *f.cluster.route.lock().unwrap() = Some(current);

        f.send_messages().await.unwrap();
        f.call(GET_STREAMS_CODE, RequestTarget::Metadata)
            .await
            .unwrap();

        let lookups = f.cluster.received(GET_PARTITION_ROUTE_CODE);
        let waits: Vec<u64> = lookups
            .iter()
            .map(|lookup| lookup.header.minimum_metadata_op)
            .collect();
        assert_eq!(waits, [0, COMMIT]);
        assert_eq!(stamped_context(&f.sends()[1]), current.context());
        let reads = f.cluster.received(GET_STREAMS_CODE);
        assert_eq!(reads[0].header.minimum_metadata_op, COMMIT);
    }

    #[tokio::test(start_paused = true)]
    async fn given_many_cached_routes_when_an_unrelated_metadata_write_commits_should_look_up_each_route_once_more()
     {
        const TOPICS: u32 = 4;
        const PARTITIONS_PER_TOPIC: u32 = 16;
        let routes = usize::try_from(TOPICS * PARTITIONS_PER_TOPIC).unwrap();
        let f = Fixture::new().await;
        let topics: Vec<Identifier> = (1..=TOPICS)
            .map(|topic_id| Identifier::numeric(topic_id).unwrap())
            .collect();
        let send_to_each_partition = async || {
            for topic_id in &topics {
                for partition_id in 0..PARTITIONS_PER_TOPIC {
                    let target = RequestTarget::Partition(PartitionTarget {
                        stream_id: &f.stream_id,
                        topic_id,
                        partition_id,
                        context: None,
                    });
                    f.call(SEND_MESSAGES_CODE, target).await.unwrap();
                }
            }
        };
        let lookups = || f.cluster.count(GET_PARTITION_ROUTE_CODE);
        send_to_each_partition().await;
        assert_eq!(lookups(), routes);

        f.cluster
            .script(&endpoint(0), CREATE_STREAM_CODE, [Answer::Commit(COMMIT)]);
        f.call(CREATE_STREAM_CODE, RequestTarget::Metadata)
            .await
            .unwrap();
        *f.cluster.route.lock().unwrap() = Some(PartitionRouteResponse {
            metadata_op: COMMIT,
            ..ROUTE
        });
        send_to_each_partition().await;
        assert_eq!(lookups(), 2 * routes, "one lookup more per cached route");

        send_to_each_partition().await;
        assert_eq!(
            lookups(),
            2 * routes,
            "the routes looked up again serve later sends"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lagging_backup_when_the_metadata_primary_is_materialising_should_retry_the_route()
     {
        let fixture = Fixture::new().await;
        let metadata_primary = fixture.driver.links.metadata_primary().unwrap();
        fixture.cluster.script(
            &endpoint(metadata_primary),
            GET_PARTITION_ROUTE_CODE,
            [Answer::Refuse(IggyError::TransientNotAccepted)],
        );
        for replica in 0..REPLICAS {
            if replica != metadata_primary {
                fixture.cluster.script(
                    &endpoint(replica),
                    GET_PARTITION_ROUTE_CODE,
                    [Answer::Refuse(IggyError::TopicIdNotFound(
                        fixture.topic_id.clone(),
                        fixture.stream_id.clone(),
                    ))],
                );
            }
        }

        fixture.send_messages().await.unwrap();

        let mut queried: Vec<_> = (0..REPLICAS).map(endpoint).collect();
        queried.push(endpoint(metadata_primary));
        assert_eq!(
            endpoints(&fixture.cluster.received(GET_PARTITION_ROUTE_CODE)),
            queried
        );
        assert_eq!(fixture.sends().len(), 1);
        assert_eq!(endpoints(&fixture.sends()), [endpoint(PRIMARY)]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_missing_topic_on_the_metadata_primary_when_looking_up_should_return_not_found()
    {
        let fixture = Fixture::new().await;
        let metadata_primary = fixture.driver.links.metadata_primary().unwrap();
        let missing =
            IggyError::TopicIdNotFound(fixture.topic_id.clone(), fixture.stream_id.clone());
        fixture.cluster.script(
            &endpoint(metadata_primary),
            GET_PARTITION_ROUTE_CODE,
            [Answer::Refuse(missing.clone())],
        );

        let error = fixture.send_messages().await.unwrap_err();

        assert_eq!(error.as_code(), missing.as_code());
        assert_eq!(
            endpoints(&fixture.cluster.received(GET_PARTITION_ROUTE_CODE)),
            [endpoint(metadata_primary)]
        );
        assert!(fixture.sends().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_namespace_miss_from_a_backup_primary_when_the_metadata_primary_refuses_the_lookup_should_end_with_that_refusal()
     {
        let f = Fixture::new().await;
        let metadata_primary = f.driver.links.metadata_primary().unwrap();
        f.script_sends([Answer::Refuse(f.missing_topic())]);
        f.cluster.script(
            &endpoint(metadata_primary),
            GET_PARTITION_ROUTE_CODE,
            [route_body(ROUTE), Answer::Refuse(IggyError::Unauthorized)],
        );

        let result = f.send_messages().await;

        assert_eq!(result.unwrap_err(), IggyError::Unauthorized);
        assert_eq!(endpoints(&f.sends()), [endpoint(PRIMARY)]);
        assert_eq!(
            endpoints(&f.cluster.received(GET_PARTITION_ROUTE_CODE)),
            vec![endpoint(metadata_primary); 2]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_namespace_miss_from_the_metadata_primary_when_its_roster_still_names_it_should_fail_at_once()
     {
        let f = Fixture::new().await;
        let metadata_primary = f.route_to_metadata_primary();
        f.cluster.script(
            &endpoint(metadata_primary),
            SEND_MESSAGES_CODE,
            [Answer::Refuse(f.missing_topic())],
        );

        let result = f.send_messages().await;

        assert_eq!(result.unwrap_err(), f.missing_topic());
        assert_eq!(f.sends().len(), 1);
        assert_eq!(f.cluster.count(GET_PARTITION_ROUTE_CODE), 1);
        assert!(
            f.cluster
                .codes(&endpoint(metadata_primary))
                .ends_with(&[SEND_MESSAGES_CODE, GET_CLUSTER_METADATA_CODE])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_namespace_miss_from_a_deposed_metadata_primary_when_its_roster_names_another_should_ask_another_replica()
     {
        for code in [GET_PARTITION_ROUTE_CODE, SEND_MESSAGES_CODE] {
            let f = Fixture::new().await;
            let deposed = f.route_to_metadata_primary();
            let elected = PRIMARY;
            f.cluster.elect(elected);
            f.cluster.script(
                &endpoint(deposed),
                code,
                [Answer::Refuse(f.missing_topic())],
            );

            f.send_messages().await.unwrap();

            assert_eq!(f.driver.links.metadata_primary(), Some(elected), "{code}");
            assert_eq!(
                endpoints(&f.cluster.received(GET_PARTITION_ROUTE_CODE)),
                [endpoint(deposed), endpoint(elected)],
                "{code}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_backup_primary_that_keeps_missing_the_namespace_when_the_deadline_passes_should_return_58()
     {
        let f = Fixture::new().await;
        f.script_sends(repeat_n(Answer::Refuse(f.missing_topic()), ENDLESS));
        let started = Instant::now();

        let result = f.send_messages().await;

        assert_eq!(result.unwrap_err(), IggyError::TransientNotAccepted);
        assert!(started.elapsed() <= OPERATION_TIMEOUT);
        let sends = f.sends();
        assert!(sends.len() > 1);
        for send in &sends {
            assert_eq!(
                bytemuck::bytes_of(&send.header),
                bytemuck::bytes_of(&sends[0].header)
            );
        }
        let op = sends[0].header.request;
        assert_eq!(
            f.driver.abandon(op),
            Err(IggyError::OperationNotRetained(op))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_namespace_miss_when_cancelled_during_its_roster_read_should_retain_no_operation()
     {
        let f = Fixture::new().await;
        let metadata_primary = f.route_to_metadata_primary();
        f.cluster.script(
            &endpoint(metadata_primary),
            SEND_MESSAGES_CODE,
            [Answer::Refuse(f.missing_topic())],
        );
        f.cluster.script(
            &endpoint(metadata_primary),
            GET_CLUSTER_METADATA_CODE,
            [Answer::Silence],
        );

        assert!(timeout(CALLER_TIMEOUT, f.send_messages()).await.is_err());

        let op = f.sends()[0].header.request;
        assert!(!f.driver.session.retains_operation(op));
        assert_eq!(
            f.cluster.codes(&endpoint(metadata_primary)).last(),
            Some(&GET_CLUSTER_METADATA_CODE)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_believed_metadata_primary_without_a_link_when_the_elected_one_misses_the_namespace_should_fail_with_that_miss()
     {
        for cached in [false, true] {
            let f = Fixture::new().await;
            if cached {
                f.send_messages().await.unwrap();
            }
            let deposed = f.driver.links.metadata_primary().unwrap();
            f.cluster.down.lock().unwrap().insert(endpoint(deposed));
            f.cluster.drop_connections(&endpoint(deposed));
            eventually("the deposed metadata primary has no link", || {
                !f.driver.links.ready_replicas().contains(&deposed)
            })
            .await;
            f.cluster.elect(PRIMARY);
            f.script_sends(repeat_n(Answer::Refuse(f.missing_topic()), ENDLESS));
            for replica in (0..REPLICAS).filter(|replica| *replica != deposed) {
                f.cluster.script(
                    &endpoint(replica),
                    GET_PARTITION_ROUTE_CODE,
                    repeat_n(Answer::Refuse(f.missing_topic()), ENDLESS),
                );
            }
            let started = Instant::now();

            let result = f.send_messages().await;

            assert_eq!(result.unwrap_err(), f.missing_topic(), "cached {cached}");
            assert!(
                started.elapsed() < RETRY_BACKOFF_MAX,
                "cached {cached}: {:?}",
                started.elapsed()
            );
            assert_eq!(f.driver.links.metadata_primary(), Some(PRIMARY));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_malformed_route_reply_when_looking_up_should_fail_without_a_frame() {
        let f = Fixture::new().await;
        f.cluster.script(
            &endpoint(0),
            GET_PARTITION_ROUTE_CODE,
            [Answer::Body(vec![1, 2, 3])],
        );

        let result = f.send_messages().await;

        assert_eq!(result.unwrap_err(), IggyError::InvalidFormat);
        assert!(f.sends().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_stopped_client_when_sending_should_fail_without_a_frame() {
        let f = Fixture::new().await;

        f.driver.links.disconnect();
        let disconnected = f.send_messages().await;
        f.driver.links.shutdown().await;
        let shut_down = f.send_messages().await;

        assert_eq!(disconnected.unwrap_err(), IggyError::NotConnected);
        assert_eq!(shut_down.unwrap_err(), IggyError::ClientShutdown);
        assert!(f.sends().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn given_one_deadline_when_lane_wait_lookups_and_attempts_run_should_never_exceed_it() {
        let f = Fixture::new().await;
        f.script_sends(repeat_n(
            Answer::Refuse(IggyError::TransientNotAccepted),
            ENDLESS,
        ));

        // The second call waits for the lane until the first one expires,
        // then looks up and sends until its own deadline.
        let (first, (second, elapsed)) = tokio::join!(f.send_messages(), async {
            sleep(SECOND_CALL_DELAY).await;
            let started = Instant::now();
            let result = f.send_messages().await;
            (result, started.elapsed())
        });

        assert_eq!(first.unwrap_err(), IggyError::TransientNotAccepted);
        assert_eq!(second.unwrap_err(), IggyError::TransientNotAccepted);
        assert!(elapsed <= OPERATION_TIMEOUT, "{elapsed:?}");
        assert!(
            elapsed >= OPERATION_TIMEOUT - RETRY_BACKOFF_MAX,
            "{elapsed:?}"
        );
        let sends = f.sends();
        assert!(
            sends
                .iter()
                .any(|send| send.header.request != sends[0].header.request),
            "the second call sent nothing after its lane wait"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lost_link_when_a_mutation_is_pending_should_return_operation_uncertain() {
        let f = Fixture::new().await;
        f.cluster.down.lock().unwrap().insert(endpoint(PRIMARY));
        f.script_sends([Answer::HangUp]);

        let result = f.send_messages().await;

        let op = f.sends()[0].header.request;
        assert_eq!(result.unwrap_err(), IggyError::OperationUncertain(op));
        assert_eq!(f.sends().len(), 1);
        assert_eq!(
            f.send_messages().await.unwrap_err(),
            IggyError::LaneBlocked(op)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_link_to_the_primary_when_the_deadline_passes_should_fail_with_58_without_sending()
     {
        // The partition primary, then the metadata primary of the fixture.
        for (code, primary) in [(SEND_MESSAGES_CODE, PRIMARY), (CREATE_STREAM_CODE, 0)] {
            let f = Fixture::new().await;
            f.cluster.down.lock().unwrap().insert(endpoint(primary));
            f.cluster.drop_connections(&endpoint(primary));
            eventually("the link to the primary is gone", || {
                !f.driver.links.ready_replicas().contains(&primary)
            })
            .await;

            let result = f.call_to(code).await;

            assert_eq!(
                result.unwrap_err(),
                IggyError::TransientNotAccepted,
                "{code}"
            );
            assert_eq!(f.cluster.count(code), 0, "{code}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_sign_in_that_outlasts_the_call_when_the_call_waits_for_the_session_should_fail_with_58_without_sending()
     {
        let f = Fixture::new().await;
        f.cluster
            .held_registers
            .lock()
            .unwrap()
            .insert("iggy".to_owned());
        *f.cluster.session.lock().unwrap() = None;
        f.cluster.drop_connections(&endpoint(0));
        eventually("the client signs in again", || {
            f.cluster.count(LOGIN_REGISTER_CODE) == 2
        })
        .await;

        let read = f
            .driver
            .send(GET_STREAMS_CODE, Bytes::new(), RequestTarget::Metadata)
            .await;

        assert_eq!(read, Err(IggyError::TransientNotAccepted));
        assert_eq!(f.cluster.count(GET_STREAMS_CODE), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_sent_mutation_when_an_eviction_ends_the_session_should_return_operation_uncertain()
     {
        // The reasons that the client reads as `Unauthenticated`.
        let reasons = [
            EvictionReason::NoSession,
            EvictionReason::SessionTooLow,
            EvictionReason::SessionReleaseMismatch,
            EvictionReason::UserInactive,
            EvictionReason::SessionError,
        ];
        for (code, replica) in [(SEND_MESSAGES_CODE, PRIMARY), (CREATE_STREAM_CODE, 0)] {
            for reason in reasons {
                let f = Fixture::new().await;
                let target = if code == SEND_MESSAGES_CODE {
                    f.partition(None)
                } else {
                    RequestTarget::Metadata
                };
                let fence = f.driver.session.fence();
                *f.cluster.session.lock().unwrap() = None;
                f.cluster
                    .script(&endpoint(replica), code, [Answer::Evict(reason)]);

                let result = f.call(code, target).await;

                let sent = f.cluster.received(code);
                let op = sent[0].header.request;
                assert_eq!(
                    result,
                    Err(IggyError::OperationUncertain(op)),
                    "{code}: {reason:?}"
                );
                assert_eq!(sent.len(), 1, "{code}: {reason:?}");
                eventually("every slot serves a new session", || {
                    f.driver.session.fence() > fence
                        && f.driver.links.ready_replicas().len() == usize::from(REPLICAS)
                })
                .await;
                let resolved = f
                    .driver
                    .resolve(op, Instant::now() + OPERATION_TIMEOUT)
                    .await;
                assert_eq!(
                    resolved,
                    Err(IggyError::OperationNotRetained(op)),
                    "{code}: {reason:?}"
                );
                f.call(code, target).await.unwrap();
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_live_session_when_evicted_for_no_session_should_replay_the_frozen_mutation() {
        for code in [SEND_MESSAGES_CODE, CREATE_STREAM_CODE] {
            let fixture = Fixture::new().await;
            let fence = fixture.driver.session.fence();
            let (replica, target) = if code == SEND_MESSAGES_CODE {
                (PRIMARY, fixture.partition(None))
            } else {
                (0, RequestTarget::Metadata)
            };
            fixture.cluster.script(
                &endpoint(replica),
                code,
                [Answer::Evict(EvictionReason::NoSession)],
            );

            fixture.call(code, target).await.unwrap();

            let sent = fixture.cluster.received(code);
            assert_eq!(sent.len(), 2);
            assert_eq!(sent[0].header.request, sent[1].header.request);
            assert_eq!(sent[0].header.session, sent[1].header.session);
            assert_eq!(sent[0].body, sent[1].body);
            assert_eq!(fixture.driver.session.fence(), fence);
            assert_eq!(fixture.cluster.count(LOGIN_REGISTER_CODE), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_sends_between_attempts_or_in_a_lane_wait_when_the_caller_signs_in_as_another_user_should_fail_with_40_without_sending_under_the_new_session()
     {
        let f = Fixture::new().await;
        f.script_sends(repeat_n(
            Answer::Refuse(IggyError::TransientNotAccepted),
            ENDLESS,
        ));
        let client = TcpClient::from_links(f.driver.links);
        let target = RequestTarget::Partition(PartitionTarget {
            stream_id: &f.stream_id,
            topic_id: &f.topic_id,
            partition_id: PARTITION_ID,
            context: None,
        });
        let send = || {
            client
                .driver
                .send(SEND_MESSAGES_CODE, Bytes::from_static(PAYLOAD), target)
        };
        let refused = || f.cluster.count(SEND_MESSAGES_CODE) > 0;

        // The first send pauses between refused attempts, and the second one
        // waits for the lane that the first one holds.
        let (between_attempts, in_lane_wait, ()) = tokio::join!(
            send(),
            async {
                eventually("the first send holds the lane", refused).await;
                send().await
            },
            async {
                eventually("the first send holds the lane", refused).await;
                sleep(CALLER_TIMEOUT).await;
                client.logout_user().await.unwrap();
                client.login_user(OTHER_USER, OTHER_PASSWORD).await.unwrap();
            }
        );

        assert_eq!(between_attempts, Err(IggyError::Unauthenticated));
        assert_eq!(in_lane_wait, Err(IggyError::Unauthenticated));
        let sessions: Vec<u64> = f
            .cluster
            .received(SEND_MESSAGES_CODE)
            .iter()
            .map(|send| send.header.session)
            .collect();
        assert!(
            sessions.iter().all(|session| *session == FIRST_SESSION),
            "sent under the sessions {sessions:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_raw_call_waiting_for_a_session_when_the_caller_signs_in_should_fail_with_40_without_sending()
     {
        let f = Fixture::new().await;
        let client = TcpClient::from_links(f.driver.links);
        client.logout_user().await.unwrap();

        // The configured sign-in is known, so the call waits for a session
        // instead of going out without one.
        let (read, ()) = tokio::join!(
            client
                .driver
                .send(GET_STREAMS_CODE, Bytes::new(), RequestTarget::Metadata),
            async {
                client.login_user(OTHER_USER, OTHER_PASSWORD).await.unwrap();
            }
        );

        assert_eq!(read, Err(IggyError::Unauthenticated));
        assert_eq!(f.cluster.count(GET_STREAMS_CODE), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_sends_between_attempts_or_in_a_lane_wait_when_the_caller_disconnects_should_fail_with_40_unless_the_client_shut_down()
     {
        for shutdown in [false, true] {
            let f = Fixture::new().await;
            f.script_sends(repeat_n(
                Answer::Refuse(IggyError::TransientNotAccepted),
                ENDLESS,
            ));
            let client = TcpClient::from_links(f.driver.links);
            let target = RequestTarget::Partition(PartitionTarget {
                stream_id: &f.stream_id,
                topic_id: &f.topic_id,
                partition_id: PARTITION_ID,
                context: None,
            });
            let send = || {
                client
                    .driver
                    .send(SEND_MESSAGES_CODE, Bytes::from_static(PAYLOAD), target)
            };
            let refused = || f.cluster.count(SEND_MESSAGES_CODE) > 0;

            let (between_attempts, in_lane_wait, ()) = tokio::join!(
                send(),
                async {
                    eventually("the first send holds the lane", refused).await;
                    send().await
                },
                async {
                    eventually("the first send holds the lane", refused).await;
                    sleep(CALLER_TIMEOUT).await;
                    if shutdown {
                        client.shutdown().await.unwrap();
                    } else {
                        client.disconnect().await.unwrap();
                    }
                }
            );

            let ended = if shutdown {
                IggyError::ClientShutdown
            } else {
                IggyError::Unauthenticated
            };
            assert_eq!(between_attempts, Err(ended.clone()), "shutdown={shutdown}");
            assert_eq!(in_lane_wait, Err(ended), "shutdown={shutdown}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_detached_replica_when_polling_should_rebind_and_continue_the_same_session() {
        for (auto_commit, strategy) in [
            (false, WirePollingStrategy::first()),
            (true, WirePollingStrategy::first()),
            (false, WirePollingStrategy::next()),
        ] {
            let fixture = Fixture::new().await;
            let fence = fixture.driver.session.fence();
            let binds = fixture.cluster.count(BIND_SESSION_CODE);
            fixture.cluster.script(
                &endpoint(PRIMARY),
                POLL_MESSAGES_CODE,
                [Answer::Refuse(IggyError::Unauthenticated)],
            );

            fixture
                .driver
                .poll(&fixture.poll(auto_commit, strategy), None)
                .await
                .unwrap();

            eventually("the detached replica binds the same session", || {
                fixture.cluster.count(BIND_SESSION_CODE) > binds
            })
            .await;
            assert_eq!(fixture.cluster.count(POLL_MESSAGES_CODE), 2);
            assert_eq!(fixture.driver.session.fence(), fence);
            assert_eq!(fixture.cluster.count(LOGIN_REGISTER_CODE), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_detached_replica_when_looking_up_a_route_should_try_another_ready_replica() {
        let fixture = Fixture::new().await;
        let fence = fixture.driver.session.fence();
        let binds = fixture.cluster.count(BIND_SESSION_CODE);
        fixture.cluster.script(
            &endpoint(0),
            GET_PARTITION_ROUTE_CODE,
            [Answer::Refuse(IggyError::Unauthenticated)],
        );

        fixture
            .driver
            .poll(&fixture.poll(false, WirePollingStrategy::first()), None)
            .await
            .unwrap();

        eventually("the detached lookup link binds again", || {
            fixture.cluster.count(BIND_SESSION_CODE) > binds
        })
        .await;
        assert_eq!(fixture.cluster.count(POLL_MESSAGES_CODE), 1);
        assert!(fixture.cluster.count(GET_PARTITION_ROUTE_CODE) > 1);
        assert_eq!(fixture.driver.session.fence(), fence);
        assert_eq!(fixture.cluster.count(LOGIN_REGISTER_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_ended_session_when_a_read_reauthenticates_should_continue() {
        let fixture = Fixture::new().await;
        *fixture.cluster.session.lock().unwrap() = None;
        fixture.cluster.script(
            &endpoint(0),
            GET_STREAMS_CODE,
            [Answer::Refuse(IggyError::Unauthenticated)],
        );

        fixture
            .driver
            .send(GET_STREAMS_CODE, Bytes::new(), RequestTarget::Metadata)
            .await
            .unwrap();

        assert!(fixture.driver.session.epoch().bound);
        assert_eq!(fixture.cluster.count(LOGIN_REGISTER_CODE), 2);
        assert_eq!(fixture.cluster.count(GET_STREAMS_CODE), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_deleted_user_when_a_read_reauthenticates_should_fail_before_the_deadline() {
        for configured in [false, true] {
            for delayed in [false, true] {
                let cluster = FakeCluster::new(REPLICAS);
                let credentials = Credentials::UsernamePassword("iggy".to_owned(), "iggy".into());
                let auto_login = if configured {
                    AutoLogin::Enabled(credentials.clone())
                } else {
                    AutoLogin::Disabled
                };
                let driver = driver_after_a_lost_session(&cluster, auto_login, credentials).await;
                *cluster.reject_credentials.lock().unwrap() = true;
                if delayed {
                    cluster.held_registers.lock().unwrap().insert("iggy".into());
                }
                let release = async {
                    if delayed {
                        eventually("reauthentication reaches the server", || {
                            cluster.count(LOGIN_REGISTER_CODE) == 2
                        })
                        .await;
                        sleep(RETRY_BACKOFF_MAX).await;
                        cluster.held_registers.lock().unwrap().clear();
                    }
                };
                let read = driver.send(GET_STREAMS_CODE, Bytes::new(), RequestTarget::Metadata);

                let (result, ()) = timeout(RECOVERY_TIMEOUT, async { tokio::join!(read, release) })
                    .await
                    .expect("a rejected sign-in must end the read before its deadline");

                assert_eq!(
                    result,
                    Err(IggyError::Unauthenticated),
                    "configured={configured}, delayed={delayed}"
                );
                assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 2);
                assert!(!driver.session.epoch().bound);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_rejected_token_when_a_read_reauthenticates_should_fail_before_the_deadline_and_forget_the_token()
     {
        let cluster = FakeCluster::new(REPLICAS);
        let token = Credentials::PersonalAccessToken("token".into());
        let driver = driver_after_a_lost_session(&cluster, AutoLogin::Disabled, token).await;
        for replica in 0..REPLICAS {
            cluster.script(
                &endpoint(replica),
                LOGIN_REGISTER_CODE,
                repeat_n(Answer::Evict(EvictionReason::InvalidToken), ENDLESS),
            );
        }

        let read = timeout(
            RECOVERY_TIMEOUT,
            driver.send(GET_STREAMS_CODE, Bytes::new(), RequestTarget::Metadata),
        )
        .await
        .expect("a rejected token must end the read before its deadline");

        assert_eq!(read, Err(IggyError::Unauthenticated));
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 2);
        assert!(driver.session.sign_in_credentials().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_raw_call_waiting_for_the_first_sign_in_when_the_server_rejects_it_should_fail_with_40_without_sending()
     {
        let cluster = FakeCluster::new(REPLICAS);
        *cluster.reject_credentials.lock().unwrap() = true;
        cluster
            .held_registers
            .lock()
            .unwrap()
            .insert("iggy".to_owned());
        let driver = OperationDriver::new(links(&cluster, &endpoint(0), policy()));
        let registering = || cluster.count(LOGIN_REGISTER_CODE) == 1;

        let (connect, read, ()) = timeout(RECOVERY_TIMEOUT, async {
            tokio::join!(
                driver.links.connect(),
                async {
                    eventually("the sign-in reaches the server", registering).await;
                    driver
                        .send(GET_STREAMS_CODE, Bytes::new(), RequestTarget::Metadata)
                        .await
                },
                async {
                    eventually("the sign-in reaches the server", registering).await;
                    sleep(CALLER_TIMEOUT).await;
                    cluster.held_registers.lock().unwrap().clear();
                }
            )
        })
        .await
        .expect("a rejected sign-in must end the waiting read before its deadline");

        assert_eq!(connect, Err(IggyError::InvalidCredentials));
        assert_eq!(read, Err(IggyError::Unauthenticated));
        assert_eq!(cluster.count(GET_STREAMS_CODE), 0);
    }

    /// A driver signed in with `credentials` on every replica, after the
    /// server lost the session. Every replica refuses each later read of the
    /// streams or the roster as unauthenticated.
    async fn driver_after_a_lost_session(
        cluster: &Arc<FakeCluster>,
        auto_login: AutoLogin,
        credentials: Credentials,
    ) -> OperationDriver {
        let session = Arc::new(SessionOwner::new(auto_login));
        session.remember(credentials, USER_ID);
        let links = ReplicaLinks::new(cluster.dialer(), endpoint(0), policy(), session);
        links.connect().await.unwrap();
        eventually("every replica is bound", || {
            links.ready_replicas().len() == usize::from(REPLICAS)
        })
        .await;
        *cluster.session.lock().unwrap() = None;
        for replica in 0..REPLICAS {
            for code in [GET_STREAMS_CODE, GET_CLUSTER_METADATA_CODE] {
                cluster.script(
                    &endpoint(replica),
                    code,
                    repeat_n(Answer::Refuse(IggyError::Unauthenticated), ENDLESS),
                );
            }
        }
        OperationDriver::new(links)
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_sent_mutation_when_its_reply_denies_it_as_unauthenticated_should_free_the_lane()
     {
        let f = Fixture::new().await;
        f.script_sends([Answer::Refuse(IggyError::Unauthenticated)]);

        let result = f.send_messages().await;

        let op = f.sends()[0].header.request;
        assert_eq!(result, Err(IggyError::Unauthenticated));
        assert_eq!(
            f.driver.abandon(op),
            Err(IggyError::OperationNotRetained(op))
        );
        f.send_messages().await.unwrap();
        assert_eq!(f.sends().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_first_attempt_when_a_reply_rejects_it_with_stale_client_should_fail_at_once_and_sign_in_again()
     {
        let refused = Answer::Refuse(IggyError::StaleClient);
        let committed = Answer::CommitRejected(COMMIT, IggyError::StaleClient);
        // The call, then the replica and the request that the reply rejects.
        let cases = [
            (CREATE_STREAM_CODE, 0, CREATE_STREAM_CODE, refused.clone()),
            (CREATE_STREAM_CODE, 0, CREATE_STREAM_CODE, committed),
            (
                SEND_MESSAGES_CODE,
                PRIMARY,
                SEND_MESSAGES_CODE,
                refused.clone(),
            ),
            (
                SEND_MESSAGES_CODE,
                0,
                GET_PARTITION_ROUTE_CODE,
                refused.clone(),
            ),
            (GET_STREAMS_CODE, 0, GET_STREAMS_CODE, refused.clone()),
            (POLL_MESSAGES_CODE, PRIMARY, POLL_MESSAGES_CODE, refused),
        ];
        for (code, replica, rejected, answer) in cases {
            let f = Fixture::new().await;
            let fence = f.driver.session.fence();
            let case = format!("{code} via {rejected}: {answer:?}");
            f.cluster.script(&endpoint(replica), rejected, [answer]);
            let mut events = f.driver.links.subscribe();
            while events.try_recv().is_ok() {}

            let result = f.call_to(code).await;

            assert_eq!(result, Err(IggyError::StaleClient), "{case}");
            assert_eq!(f.cluster.count(rejected), 1, "{case}");
            let sent = f.cluster.received(code);
            assert_eq!(sent.len(), usize::from(code == rejected), "{case}");
            if let Some(request) = sent.first() {
                let op = request.header.request;
                assert_eq!(
                    f.driver.abandon(op),
                    Err(IggyError::OperationNotRetained(op)),
                    "{case}"
                );
            }
            f.next_session(fence).await;
            assert_eq!(f.cluster.count(LOGIN_REGISTER_CODE), 2, "{case}");
            // A consumer polls again only after this event.
            let published: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
            assert_eq!(published.last(), Some(&DiagnosticEvent::SignedIn), "{case}");
            f.call_to(code).await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lost_link_when_the_retry_of_a_sent_mutation_is_refused_with_stale_client_should_end_uncertain_and_sign_in_again()
     {
        let f = Fixture::new().await;
        let fence = f.driver.session.fence();
        f.script_sends([Answer::HangUp, Answer::Refuse(IggyError::StaleClient)]);

        let result = f.send_messages().await;

        let sends = f.sends();
        let op = sends[0].header.request;
        assert_eq!(result, Err(IggyError::OperationUncertain(op)));
        assert_eq!(ops(&sends), [op; 2]);
        f.next_session(fence).await;
        assert_eq!(f.cluster.count(LOGIN_REGISTER_CODE), 2);
        assert_eq!(
            f.driver.abandon(op),
            Err(IggyError::OperationNotRetained(op))
        );
        f.send_messages().await.unwrap();
    }

    /// An admin deletes or deactivates the user while the reply of its
    /// mutation is lost. However the client learns that the session ended, the
    /// call ends uncertain and never with a final error. The sign-in after it
    /// fails, and the operation is not retained.
    #[tokio::test(start_paused = true)]
    async fn given_a_lost_reply_when_a_revocation_ends_the_session_should_end_uncertain_without_retaining_it()
     {
        // The answers to the mutation, and whether the session already ended
        // when its link binds again.
        let endings = [
            // The retry on a new link is refused with status 30.
            (
                vec![Answer::HangUp, Answer::Refuse(IggyError::StaleClient)],
                false,
            ),
            // The new link cannot bind, so no retry leaves the client.
            (vec![Answer::HangUp], true),
            // The retry on the same link is evicted, and the link cannot bind.
            (
                vec![
                    Answer::Refuse(IggyError::TransientNotCommitted),
                    Answer::Evict(EvictionReason::NoSession),
                ],
                true,
            ),
        ];
        // The sign-in of a deleted user, then of a deactivated one.
        let refusals = [
            EvictionReason::InvalidCredentials,
            EvictionReason::UserInactive,
        ];
        for (code, replica) in [(SEND_MESSAGES_CODE, PRIMARY), (CREATE_STREAM_CODE, 0)] {
            for (answers, ended) in &endings {
                for refusal in refusals {
                    let f = Fixture::new().await;
                    let fence = f.driver.session.fence();
                    let case = format!("{code} {answers:?} {refusal:?}");
                    for node in 0..REPLICAS {
                        f.cluster.script(
                            &endpoint(node),
                            LOGIN_REGISTER_CODE,
                            repeat_n(Answer::Evict(refusal), ENDLESS),
                        );
                    }
                    if *ended {
                        *f.cluster.session.lock().unwrap() = None;
                    }
                    f.cluster.script(&endpoint(replica), code, answers.clone());

                    let result = f.call_to(code).await;

                    let sent = f.cluster.received(code);
                    let op = sent[0].header.request;
                    assert_eq!(result, Err(IggyError::OperationUncertain(op)), "{case}");
                    assert_eq!(sent.len(), answers.len(), "{case}");
                    eventually("the sign-in after the session ended fails", || {
                        f.driver.session.fence() > fence && f.cluster.count(LOGIN_REGISTER_CODE) > 1
                    })
                    .await;
                    assert!(!f.driver.session.epoch().bound, "{case}");
                    assert_eq!(
                        f.driver.abandon(op),
                        Err(IggyError::OperationNotRetained(op)),
                        "{case}"
                    );
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_mutation_when_its_replay_commits_a_stale_client_rejection_should_keep_that_outcome_and_sign_in_again()
     {
        let f = Fixture::new().await;
        f.cluster.script(
            &endpoint(0),
            CREATE_STREAM_CODE,
            [
                Answer::Refuse(IggyError::TransientNotCommitted),
                Answer::Refuse(IggyError::RequestTooOld),
                Answer::CommitRejected(COMMIT, IggyError::StaleClient),
            ],
        );
        let failed = f.call(CREATE_STREAM_CODE, RequestTarget::Metadata).await;
        let op = f.cluster.received(CREATE_STREAM_CODE)[0].header.request;
        assert_eq!(failed, Err(IggyError::OperationUncertain(op)));
        let fence = f.driver.session.fence();

        let resolved = f
            .driver
            .resolve(op, Instant::now() + OPERATION_TIMEOUT)
            .await;

        assert_eq!(resolved, Err(IggyError::StaleClient));
        f.next_session(fence).await;
        assert_eq!(f.cluster.count(LOGIN_REGISTER_CODE), 2);
        let deadline = Instant::now() + OPERATION_TIMEOUT;
        assert_eq!(
            f.driver.resolve(op, deadline).await,
            Err(IggyError::StaleClient)
        );
        assert_eq!(f.cluster.count(CREATE_STREAM_CODE), 3);
        f.call(CREATE_STREAM_CODE, RequestTarget::Metadata)
            .await
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_stale_client_eviction_when_a_send_or_its_lookup_loses_the_link_should_bind_the_same_session_again()
     {
        for (replica, code) in [(PRIMARY, SEND_MESSAGES_CODE), (0, GET_PARTITION_ROUTE_CODE)] {
            let f = Fixture::new().await;
            let fence = f.driver.session.fence();
            let binds = f.cluster.count(BIND_SESSION_CODE);
            f.cluster.script(
                &endpoint(replica),
                code,
                [Answer::Evict(EvictionReason::StaleClient)],
            );

            f.send_messages().await.unwrap();

            eventually("the evicted link binds the session again", || {
                f.cluster.count(BIND_SESSION_CODE) > binds
            })
            .await;
            let sends = f.sends();
            assert!(
                ops(&sends).iter().all(|op| *op == sends[0].header.request),
                "{code}"
            );
            assert_eq!(f.driver.session.fence(), fence, "{code}");
            assert_eq!(f.cluster.count(LOGIN_REGISTER_CODE), 1, "{code}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_credentials_when_a_reply_rejects_the_session_with_stale_client_should_stay_unbound()
     {
        let mut f = Fixture::new().await;
        let session = Arc::new(SessionOwner::new(AutoLogin::Disabled));
        session.bind(FIRST_SESSION).unwrap();
        f.driver = OperationDriver::new(ReplicaLinks::new(
            f.cluster.dialer(),
            endpoint(0),
            policy(),
            session,
        ));
        f.driver.links.connect().await.unwrap();
        eventually("every slot serves the session", || {
            f.driver.links.ready_replicas().len() == usize::from(REPLICAS)
        })
        .await;
        let fence = f.driver.session.fence();
        f.cluster.script(
            &endpoint(0),
            CREATE_STREAM_CODE,
            [Answer::Refuse(IggyError::StaleClient)],
        );

        let result = f.call(CREATE_STREAM_CODE, RequestTarget::Metadata).await;

        assert_eq!(result, Err(IggyError::StaleClient));
        eventually("the client drops the session", || {
            f.driver.session.fence() > fence
        })
        .await;
        sleep(TEST_BUDGET).await;
        assert!(!f.driver.session.epoch().bound);
        assert_eq!(f.cluster.count(LOGIN_REGISTER_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_evicted_poll_when_it_moves_its_consumer_should_end_with_57_without_retention()
    {
        for (auto_commit, strategy, progress) in [
            (true, WirePollingStrategy::first(), true),
            (false, WirePollingStrategy::next(), true),
            (false, WirePollingStrategy::first(), false),
        ] {
            let f = Fixture::new().await;
            f.cluster.script(
                &endpoint(PRIMARY),
                POLL_MESSAGES_CODE,
                [Answer::Evict(EvictionReason::NoSession)],
            );

            let result = f.driver.poll(&f.poll(auto_commit, strategy), None).await;

            let polls = f.cluster.received(POLL_MESSAGES_CODE);
            if progress {
                assert_eq!(result, Err(IggyError::TransientNotCommitted));
                assert_eq!(polls.len(), 1);
            } else {
                assert!(result.is_ok(), "{result:?}");
                assert_eq!(polls.len(), 2);
            }
            let op = polls[0].header.request;
            assert_eq!(
                f.driver.abandon(op),
                Err(IggyError::OperationNotRetained(op))
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_slow_poll_when_a_send_shares_the_link_should_not_wait_for_the_poll() {
        let f = Fixture::new().await;
        f.cluster
            .script(&endpoint(PRIMARY), POLL_MESSAGES_CODE, [Answer::Silence]);
        let request = f.poll(false, WirePollingStrategy::first());
        let poll = f.driver.poll(&request, None);
        tokio::pin!(poll);
        let arrived = tokio::select! {
            _ = &mut poll => false,
            () = eventually("the poll waits at the primary", || {
                f.cluster.count(POLL_MESSAGES_CODE) == 1
            }) => true,
        };
        assert!(arrived);

        let sent = tokio::select! {
            sent = f.send_messages() => sent,
            polled = &mut poll => panic!("the poll ended first: {polled:?}"),
        };

        sent.unwrap();
        assert_eq!(endpoints(&f.sends()), [endpoint(PRIMARY)]);
    }
}
