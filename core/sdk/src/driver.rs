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
use futures::stream::{self, StreamExt, TryStreamExt};
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, CANCEL_DEFERRED_REQUEST_CODE, CHANGE_PASSWORD_CODE,
    DELETE_CONSUMER_OFFSET_CODE, FETCH_MESSAGES_CODE, GET_CONSUMER_OFFSET_CODE,
    GET_PARTITION_ROUTE_CODE, LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE,
    POLL_MESSAGES_CODE, POLL_MESSAGES_DEFERRED_CODE, SEND_MESSAGES_CODE,
    STORE_CONSUMER_OFFSET_CODE, UPDATE_RESOURCE_CODE,
};
use iggy_binary_protocol::consensus::{Operation, operation_for_code};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, GetConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::{
    DeferredPollMessagesRequest, FetchMessagesRequest, GetPartitionRouteRequest,
    MIN_DEFERRED_POLL_MAX_BYTES, MIN_DEFERRED_POLL_REPLY_MARGIN_US, PollMessagesRequest,
    SendMessagesHeader,
};
use iggy_binary_protocol::requests::system::{ResourceTarget, UpdateResourceRequest};
use iggy_binary_protocol::requests::users::ChangePasswordRequest;
use iggy_binary_protocol::responses::messages::{
    FetchMessagesResponse, FetchMessagesSectionIterator, PartitionRouteResponse,
};
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::{
    MAX_PARTITIONS_PER_REQUEST, STATUS_OK, WireDecode, WireEncode, WireOptions, WirePartitioning,
    WirePollingStrategy,
};
use iggy_common::{
    BinaryOperationLineage, ClientState, Credentials, DeferredPollOptions, DiagnosticEvent,
    FetchedSection, HeaderKind, Identifier, IggyError, OptionKey,
};
use route_cache::RouteCache;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit};
use tokio::time::{Instant, timeout_at};
use tracing::debug;

mod route_cache;

pub(crate) const MAX_DEFERRED_REQUESTS: usize = 64;
pub(crate) const MAX_DEFERRED_REQUESTS_PER_CONSUMER: usize = 16;
/// Route lookups in flight while [`OperationDriver::readiness_groups`] groups
/// the partitions of a wait. After a move, each stale partition needs one, so
/// a primary that held 333 of 1000 partitions on 3 nodes costs 21 round trips
/// instead of 333. Sixteen is a small share of the requests that one link
/// holds, so the polls and sends of the client keep their room.
const MAX_ROUTE_LOOKUPS_IN_FLIGHT: usize = 16;

tokio::task_local! {
    pub(crate) static DEFERRED_CONSUMER_CAPACITY: Arc<Semaphore>;
}

pub(crate) use route_cache::PartitionKey;

pub(crate) use iggy_common::{
    binary_operation_deadline as operation_deadline, with_binary_deadline as with_deadline,
};

/// Readiness fanout and fetches share the consumer's wire admission limit.
pub(crate) struct DeferredLease<'a> {
    _client: SemaphorePermit<'a>,
    _consumer: Option<OwnedSemaphorePermit>,
}

impl<'a> DeferredLease<'a> {
    pub(crate) async fn acquire(
        client: &'a Semaphore,
        deadline: Instant,
    ) -> Result<Self, IggyError> {
        timeout_at(deadline, async {
            // Queued work from one consumer must not reserve every client slot.
            let consumer = match DEFERRED_CONSUMER_CAPACITY.try_with(Arc::clone) {
                Ok(capacity) => Some(
                    capacity
                        .acquire_owned()
                        .await
                        .map_err(|_| IggyError::ClientShutdown)?,
                ),
                Err(_) => None,
            };
            let client = client
                .acquire()
                .await
                .map_err(|_| IggyError::ClientShutdown)?;
            Ok(Self {
                _client: client,
                _consumer: consumer,
            })
        })
        .await
        .map_err(|_| IggyError::TransientNotAccepted)?
    }
}

/// Where a binary client sends one request.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RequestTarget<'a> {
    Metadata,
    Partition(PartitionTarget<'a>),
    /// The replica the caller chose.
    Replica(u8),
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
    deferred_requests: tokio::sync::Semaphore,
}

impl OperationDriver {
    /// The driver shares the session owner of `links`.
    pub(crate) fn new(links: ReplicaLinks) -> Self {
        Self {
            session: Arc::clone(links.session()),
            links,
            routes: Mutex::default(),
            deferred_requests: tokio::sync::Semaphore::new(MAX_DEFERRED_REQUESTS),
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
        self.poll_with_options(request, context, None).await
    }

    /// An `auto_commit` poll whose outcome is unknown after it left the client
    /// ends with `FetchProgressUncertain` and blocks its partition until an
    /// abandon, because the server can still store its offset.
    pub(crate) async fn poll_with_options(
        &self,
        request: &PollMessagesRequest,
        context: Option<PartitionContext>,
        options: Option<DeferredPollOptions>,
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
        let started = Instant::now();
        let deadline = match options {
            Some(options) => {
                options.validate(request.count)?;
                started
                    .checked_add(options.request_timeout.get_duration())
                    .ok_or(IggyError::InvalidCommand)?
            }
            None => operation_deadline(),
        };
        with_deadline(deadline, async {
            let (code, payload, _lease) = match options {
                Some(options) => {
                    let lease = self.deferred_lease().await?;
                    // A link fails on a reply larger than it reads, so the
                    // server must not fill more.
                    let max_bytes =
                        self.links
                            .reply_body_limit()
                            .map_or(options.max_bytes, |limit| {
                                options
                                    .max_bytes
                                    .min(u32::try_from(limit).unwrap_or(u32::MAX))
                            });
                    let wire = DeferredPollMessagesRequest {
                        poll: request.clone(),
                        wait_us: options.max_wait.as_micros(),
                        min_count: options.min_count,
                        max_bytes,
                        request_timeout_us: options.request_timeout.as_micros(),
                    };
                    (POLL_MESSAGES_DEFERRED_CODE, wire.to_bytes(), Some(lease))
                }
                None => (POLL_MESSAGES_CODE, request.to_bytes(), None),
            };
            let call = Call::new(
                code,
                payload,
                RequestTarget::Partition(PartitionTarget {
                    stream_id: &stream_id,
                    topic_id: &topic_id,
                    partition_id,
                    context,
                }),
                deadline,
            );
            self.drive(Call {
                started,
                progress_poll: request.auto_commit
                    || request.strategy.kind == WirePollingStrategy::next().kind,
                committing_poll: request.auto_commit,
                deadline: operation_deadline(),
                ..call
            })
            .await
        })
        .await
    }

    /// The partitions grouped by primary endpoint and split to fit one wait
    /// request each. Each group keeps the order of `partitions`, and the first
    /// failed lookup in that order is the error.
    pub(crate) async fn readiness_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions: &[u32],
    ) -> Result<Vec<Vec<u32>>, IggyError> {
        let lineage = self.session.operation_lineage().lineage();
        let routes = self
            .routes_of(
                stream_id,
                topic_id,
                partitions,
                lineage,
                operation_deadline(),
            )
            .await?;
        let mut endpoints = std::collections::BTreeMap::<u8, Vec<u32>>::new();
        for (&partition_id, route) in partitions.iter().zip(routes) {
            endpoints
                .entry(route.primary_replica)
                .or_default()
                .push(partition_id);
        }
        let mut groups = Vec::with_capacity(endpoints.len());
        for partitions in endpoints.into_values() {
            groups.extend(
                partitions
                    .chunks(MAX_PARTITIONS_PER_REQUEST as usize)
                    .map(<[u32]>::to_vec),
            );
        }
        Ok(groups)
    }

    /// One fetch on the primary of its targets. The targets of a readiness
    /// group share it, so the call routes by the first one. The header
    /// carries context zero, and each target its own fence.
    pub(crate) async fn fetch_messages(
        &self,
        request: &FetchMessagesRequest,
    ) -> Result<Bytes, IggyError> {
        self.fetch_response(request).await.map(|reply| reply.body)
    }

    pub(crate) async fn fetch_sections(
        &self,
        request: &FetchMessagesRequest,
    ) -> Result<Vec<FetchedSection>, IggyError> {
        let reply = self.fetch_response(request).await?;
        let mut sections = FetchedSection::from_reply(&reply.body)?;
        if let Some(uncertain) = reply.uncertain_fetch {
            for section in &mut sections {
                if uncertain.partitions.contains(&section.partition_id) {
                    section.status = Err(IggyError::FetchProgressUncertain(uncertain.operation_id));
                }
            }
        }
        Ok(sections)
    }

    async fn fetch_response(&self, request: &FetchMessagesRequest) -> Result<CallReply, IggyError> {
        DeferredPollOptions::from(request).validate_fetch(request.count)?;
        let started = Instant::now();
        let deadline = started
            .checked_add(std::time::Duration::from_micros(request.request_timeout_us))
            .ok_or(IggyError::InvalidCommand)?;
        let stream_id = Identifier::try_from(&request.stream_id)?;
        let topic_id = Identifier::try_from(&request.topic_id)?;
        let partition_id = request
            .targets
            .first()
            .ok_or(IggyError::InvalidCommand)?
            .partition_id;
        let mut request = request.clone();
        if let Some(limit) = self.links.reply_body_limit() {
            fit_fetch_reply(&mut request, limit)?;
        }
        let progress_poll = request.auto_commit
            || request
                .targets
                .iter()
                .any(|target| target.strategy.kind == WirePollingStrategy::next().kind);
        with_deadline(deadline, async {
            let _lease = self.deferred_lease().await?;
            let call = Call::new(
                FETCH_MESSAGES_CODE,
                request.to_bytes(),
                RequestTarget::Partition(PartitionTarget {
                    stream_id: &stream_id,
                    topic_id: &topic_id,
                    partition_id,
                    context: Some(PartitionContext::default()),
                }),
                operation_deadline(),
            );
            self.drive_reply(Call {
                started,
                progress_poll,
                fetch: Some(request),
                ..call
            })
            .await
        })
        .await
    }

    /// Waits for one of the [`MAX_DEFERRED_REQUESTS`] slots. The wait spends
    /// the request budget, so a frame encodes its budget after the lease.
    async fn deferred_lease(&self) -> Result<DeferredLease<'_>, IggyError> {
        DeferredLease::acquire(&self.deferred_requests, operation_deadline()).await
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
            POLL_MESSAGES_DEFERRED_CODE => {
                let request = DeferredPollMessagesRequest::decode_from(&payload)
                    .map_err(|_| IggyError::InvalidCommand)?;
                return self
                    .poll_with_options(&request.poll, context, Some((&request).into()))
                    .await;
            }
            // Each target carries its own fence, and a zero one takes the route.
            FETCH_MESSAGES_CODE => {
                let request = FetchMessagesRequest::decode_from(&payload)
                    .map_err(|_| IggyError::InvalidCommand)?;
                return self.fetch_messages(&request).await;
            }
            // A cancel names a pending request by its id, and the server answers no
            // cancel, so a new raw request could only wait for its deadline.
            CANCEL_DEFERRED_REQUEST_CODE => return Err(IggyError::InvalidCommand),
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

    /// Sends a non-replicated request to replica `replica_id` and returns its
    /// reply. Writes and partition reads go through the routing of their own
    /// kind: a poll, a readiness wait, a fetch or a consumer-offset read needs
    /// the partition's primary and its incarnation, which a request to a
    /// chosen replica does not carry. The server answers no cancel, so a raw one
    /// could only wait for its deadline.
    pub(crate) async fn send_to_replica(
        &self,
        code: u32,
        payload: Bytes,
        replica_id: u8,
    ) -> Result<Bytes, IggyError> {
        if operation_for_code(code) != Operation::NonReplicated
            || matches!(
                code,
                POLL_MESSAGES_CODE
                    | POLL_MESSAGES_DEFERRED_CODE
                    | FETCH_MESSAGES_CODE
                    | CANCEL_DEFERRED_REQUEST_CODE
                    | GET_CONSUMER_OFFSET_CODE
            )
        {
            return Err(IggyError::InvalidCommand);
        }
        self.send(code, payload, RequestTarget::Replica(replica_id))
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

    async fn drive(&self, call: Call) -> Result<Bytes, IggyError> {
        self.drive_reply(call).await.map(|reply| reply.body)
    }

    async fn drive_reply(&self, mut call: Call) -> Result<CallReply, IggyError> {
        let lineage = self.session.operation_lineage().lineage();
        let started = SessionEpoch {
            lineage,
            ..self.session.epoch()
        };
        loop {
            let next = match self.attempt(&mut call, started).await {
                Ok(body) => {
                    let uncertain_fetch = match call.accept_fetch_reply(&body) {
                        Ok(uncertain) => uncertain,
                        Err(error) => return Err(call.end(error)),
                    };
                    self.refresh_sign_in(&call);
                    call.complete(Ok(()));
                    return Ok(CallReply {
                        body,
                        uncertain_fetch,
                    });
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
        self.fence_targets(call, started.lineage).await?;
        let context = self.context(call, route.as_ref());
        let mut lane_context = context;
        if lane_context.incarnation == 0
            && (call.progress_poll
                || matches!(
                    call.code,
                    GET_CONSUMER_OFFSET_CODE
                        | STORE_CONSUMER_OFFSET_CODE
                        | DELETE_CONSUMER_OFFSET_CODE
                ))
            && let Some(route) = &route
        {
            // Keep an explicit zero wire fence invalid while sharing local
            // progress admission with callers that captured the route.
            lane_context.incarnation = route.incarnation;
        }
        // The lane comes first, so a blocked lane ends the call at once, also
        // while no link is ready.
        let mut ticket = self.ticket(call, &lane_context).await?;
        if call.committing()
            && let Err(error) = ticket.reserve_fetch(call.deadline).await
        {
            call.ticket = Some(ticket);
            return Err(Next::Fail(error));
        }
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
                        if call.fetch.is_some()
                            && let Ok(body) = &outcome.result
                        {
                            // Caller isolation still rejects the call, but a received
                            // table proves which progress lanes can reopen.
                            call.accept_fetch_reply(body).map_err(Next::Fail)?;
                        } else {
                            // Progress polls can move a cursor without a replayable receipt.
                            call.uncertain |= call.committing()
                                && outcome.result.as_ref().is_err_and(fetch_error_is_uncertain);
                            call.uncertain |= call.progress_poll
                                && matches!(
                                    &outcome.result,
                                    Ok(_) | Err(IggyError::TransientNotCommitted)
                                );
                        }
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
                        && !call.committing_fetch()
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
            Err(error) => Err(call.lookup_failed(error)),
        }
    }

    /// Gives each target of a fetch without a fence the incarnation of its
    /// route, before the call freezes its first frame. Every later attempt
    /// keeps these fences.
    async fn fence_targets(&self, call: &mut Call, lineage: u64) -> Result<(), Next> {
        let (Some(partition), Some(request), None) =
            (&call.partition, &mut call.fetch, &call.frame)
        else {
            return Ok(());
        };
        let unfenced: Vec<u32> = request
            .targets
            .iter()
            .filter(|target| target.incarnation == 0)
            .map(|target| target.partition_id)
            .collect();
        if unfenced.is_empty() {
            return Ok(());
        }
        let routes = self
            .routes_of(
                &partition.key.stream_id,
                &partition.key.topic_id,
                &unfenced,
                lineage,
                call.deadline,
            )
            .await;
        let routes = match routes {
            Ok(routes) => routes,
            Err(error) => return Err(call.lookup_failed(error)),
        };
        for (target, route) in request
            .targets
            .iter_mut()
            .filter(|target| target.incarnation == 0)
            .zip(routes)
        {
            target.incarnation = route.incarnation;
        }
        call.payload = request.to_bytes();
        Ok(())
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
            None if call.progress_poll && (call.fetch.is_some() || call.committing_poll) => {
                // A ticket blocks only the progress lanes that it holds besides
                // its own, so a committing poll enters its lane as a fetch
                // target does.
                let partitions: Vec<_> = match &call.fetch {
                    Some(request) => request
                        .targets
                        .iter()
                        .map(|target| (target.partition_id, target.incarnation))
                        .collect(),
                    None => vec![(call.partition_id(), context.incarnation)],
                };
                self.session
                    .enter_progress_lanes(&partitions, call.deadline)
                    .await
                    .map_err(Next::Fail)
            }
            None => {
                let key = LaneKey::of(call.code, context, call.partition_id());
                if call.code == GET_CONSUMER_OFFSET_CODE {
                    return self
                        .session
                        .enter_progress_inspection(
                            LaneKey::Progress {
                                incarnation: context.incarnation,
                                partition_id: call.partition_id(),
                            },
                            call.deadline,
                        )
                        .await
                        .map_err(Next::Fail);
                }
                if call.progress_poll
                    || matches!(
                        call.code,
                        GET_CONSUMER_OFFSET_CODE
                            | STORE_CONSUMER_OFFSET_CODE
                            | DELETE_CONSUMER_OFFSET_CODE
                    )
                {
                    self.session
                        .enter_progress_lane(
                            LaneKey::Progress {
                                incarnation: context.incarnation,
                                partition_id: call.partition_id(),
                            },
                            key,
                            call.deadline,
                        )
                        .await
                        .map_err(Next::Fail)
                } else {
                    self.session
                        .enter_lane(key, call.deadline)
                        .await
                        .map_err(Next::Fail)
                }
            }
        }
    }

    /// The link of the next attempt: to the replica the caller chose, to the
    /// primary of the route, else to the metadata primary.
    async fn hop(
        &self,
        call: &mut Call,
        route: Option<PartitionRouteResponse>,
    ) -> Result<Hop, Next> {
        if let Some(replica) = call.replica {
            return self.replica_hop(call, replica).await;
        }
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

    /// The link of the replica the caller chose. A replica without a ready
    /// link is waited for. One the roster does not name is looked up in a
    /// fresh roster once, and then refused.
    async fn replica_hop(&self, call: &mut Call, replica: u8) -> Result<Hop, Next> {
        if let Ok(link) = self.links.link(Target::Replica(replica)) {
            return Ok(Hop { link, route: None });
        }
        call.refusal = IggyError::TransientNotAccepted;
        if self.links.has_replica(replica) {
            return Err(Next::Pause);
        }
        if call.roster_read {
            return Err(Next::Fail(IggyError::InvalidIdentifier));
        }
        call.roster_read = match self.links.link(Target::MetadataPrimary) {
            Ok(link) => self.read_roster(&link, call.deadline).await,
            Err(_) => false,
        };
        Err(if call.roster_read {
            Next::Again
        } else {
            Next::Pause
        })
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
            Ok(body) => {
                self.refused_sections(call, &body);
                return self.signed_in(call, hop, body);
            }
            Err(error) => error,
        };
        if call.committing() && sent && fetch_error_is_uncertain(&error) {
            call.uncertain = true;
            return Err(Next::Fail(error));
        }
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
            // The caller chose the replica, so its refusal is the answer.
            IggyError::NotPrimary if call.replica.is_some() => Err(Next::Fail(error)),
            IggyError::DeferredPollCapacityExceeded => {
                call.refusal = error;
                call.settle_sent();
                Err(Next::Pause)
            }
            // The refusal does not name the moved target, so the caller regroups.
            IggyError::NotPrimary if call.fetch.is_some() => {
                self.mark_targets(call, RouteCache::may_have_moved);
                Err(Next::Fail(error))
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

    /// Learns where the next attempt goes after `hop` failed one. A call to a
    /// chosen replica goes there again.
    async fn refresh(&self, call: &Call, hop: &Hop, failure: Failure) {
        if call.replica.is_some() {
            return;
        }
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
        if call.fetch.is_some() {
            // A fetch refusal does not name its target.
            self.mark_targets(call, RouteCache::invalidate_partitions);
        } else if let (Some(partition), Some(route)) = (&call.partition, &hop.route) {
            self.routes().invalidate(route, partition.key.partition_id);
        }
    }

    /// Applies `mark` to the routes of every target of a fetch `call`.
    fn mark_targets(
        &self,
        call: &Call,
        mark: fn(&mut RouteCache, &Identifier, &Identifier, &[u32]),
    ) {
        let (Some(partition), Some(request)) = (&call.partition, &call.fetch) else {
            return;
        };
        let partitions: Vec<u32> = request
            .targets
            .iter()
            .map(|target| target.partition_id)
            .collect();
        mark(
            &mut self.routes(),
            &partition.key.stream_id,
            &partition.key.topic_id,
            &partitions,
        );
    }

    /// Marks the routes of refused fetch sections as [`Self::settle`] marks
    /// the route of a refused call. `NotPrimary` sections make the next
    /// readiness groups look their routes up again.
    fn refused_sections(&self, call: &Call, body: &[u8]) {
        let (Some(partition), Some(_)) = (&call.partition, &call.fetch) else {
            return;
        };
        let Ok(sections) = FetchMessagesSectionIterator::new(body) else {
            return;
        };
        let mut moved = Vec::new();
        let mut invalid = Vec::new();
        for section in sections.filter(|section| section.header.status != STATUS_OK) {
            let partition_id = section.header.partition_id;
            match IggyError::from_code(section.header.status) {
                IggyError::NotPrimary => moved.push(partition_id),
                IggyError::HistoryUnavailable | IggyError::ConsumerGroupPartitionNotOwned(..) => {
                    invalid.push(partition_id);
                }
                error if is_namespace_miss(&error) => invalid.push(partition_id),
                _ => {}
            }
        }
        if moved.is_empty() && invalid.is_empty() {
            return;
        }
        let (stream_id, topic_id) = (&partition.key.stream_id, &partition.key.topic_id);
        let mut routes = self.routes();
        routes.may_have_moved(stream_id, topic_id, &moved);
        routes.invalidate_partitions(stream_id, topic_id, &invalid);
    }

    /// The routes of `partitions` in their order, with at most
    /// [`MAX_ROUTE_LOOKUPS_IN_FLIGHT`] lookups at once. The first failed
    /// lookup in that order is the error.
    async fn routes_of(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions: &[u32],
        lineage: u64,
        deadline: Instant,
    ) -> Result<Vec<PartitionRouteResponse>, IggyError> {
        stream::iter(partitions.iter().copied())
            .map(|partition_id| {
                let key = PartitionKey {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    partition_id,
                };
                async move { self.route(&key, lineage, deadline).await }
            })
            .buffered(MAX_ROUTE_LOOKUPS_IN_FLIGHT)
            .try_collect()
            .await
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
    /// retries until its deadline. A client with no ready replica, no session
    /// and no credentials cannot get a route, because the server refuses it
    /// every read and it never signs in by itself, so its lookup ends with
    /// `Unauthenticated` at once.
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
        if replicas.is_empty()
            && !self.session.epoch().bound
            && self.session.sign_in_credentials().is_none()
        {
            return Err(IggyError::Unauthenticated);
        }
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
    /// The replica the caller chose, `None` when the driver routes the call.
    replica: Option<u8>,
    /// A call to a replica the roster did not name read the roster once.
    roster_read: bool,
    started: Instant,
    deadline: Instant,
    ticket: Option<LaneTicket>,
    frame: Option<Frame>,
    /// Replaying a progress poll can advance its offset again without delivering the first result.
    progress_poll: bool,
    /// A poll that has the server store the offset of its last message.
    committing_poll: bool,
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
    /// The request of a fetch. Its targets name the progress lanes and the
    /// routes of the call, and a refusal of the fetch concerns all of them.
    fetch: Option<FetchMessagesRequest>,
}

struct CallReply {
    body: Bytes,
    uncertain_fetch: Option<UncertainFetch>,
}

/// Whether a committing fetch that failed with `error` can have taken effect. The HTTP client
/// applies the same list to the error id of a deferred poll reply.
pub(crate) fn fetch_error_is_uncertain(error: &IggyError) -> bool {
    !matches!(
        error,
        IggyError::FeatureUnavailable
            | IggyError::InvalidIdentifier
            | IggyError::TransientNotAccepted
            | IggyError::HistoryUnavailable
            | IggyError::NotPrimary
            | IggyError::DeferredPollCapacityExceeded
            // The server refuses a first message larger than `max_bytes` before it stores the
            // offset. A link that refuses a larger reply fails its sent calls with the same
            // error, and `attempt` marks such a progress call uncertain before this list.
            | IggyError::InvalidSizeBytes
            | IggyError::StreamIdNotFound(..)
            | IggyError::StreamNameNotFound(..)
            | IggyError::TopicIdNotFound(..)
            | IggyError::TopicNameNotFound(..)
            | IggyError::PartitionNotFound(..)
            | IggyError::InvalidOffset(..)
            | IggyError::ConsumerGroupPartitionNotOwned(..)
    )
}

fn fetch_section_is_uncertain(error: &IggyError) -> bool {
    !matches!(
        error,
        IggyError::InvalidSizeBytes
            | IggyError::HistoryUnavailable
            | IggyError::NotPrimary
            | IggyError::ConsumerGroupPartitionNotOwned(..)
    )
}

struct UncertainFetch {
    operation_id: u64,
    partitions: Vec<u32>,
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
    fn committing_fetch(&self) -> bool {
        self.fetch
            .as_ref()
            .is_some_and(|request| request.auto_commit)
    }

    /// The server can store a consumer offset after the client loses the
    /// reply, so an unknown outcome blocks the progress lanes of the call
    /// until an abandon.
    fn committing(&self) -> bool {
        self.committing_poll || self.committing_fetch()
    }

    /// Validate the table while the progress gates still belong to the call.
    /// Records remain opaque so a decode failure keeps the normal rewind path.
    fn accept_fetch_reply(&mut self, body: &Bytes) -> Result<Option<UncertainFetch>, IggyError> {
        let Some(request) = &self.fetch else {
            return Ok(None);
        };
        let sections = FetchMessagesSectionIterator::new(body).map_err(|_| {
            self.uncertain |= request.auto_commit;
            IggyError::InvalidFormat
        })?;
        let mut seen = HashSet::with_capacity(request.targets.len());
        let mut uncertain = Vec::new();
        let mut messages = 0_u64;
        for section in sections {
            let partition_id = section.header.partition_id;
            messages += u64::from(section.header.messages_count);
            if !seen.insert(partition_id)
                || !request
                    .targets
                    .iter()
                    .any(|target| target.partition_id == partition_id)
                || messages > u64::from(request.count)
            {
                self.uncertain |= request.auto_commit;
                return Err(IggyError::InvalidFormat);
            }
            if request.auto_commit
                && section.header.status != STATUS_OK
                && fetch_section_is_uncertain(&IggyError::from_code(section.header.status))
            {
                uncertain.push(partition_id);
            }
        }
        if uncertain.is_empty() {
            return Ok(None);
        }
        let mut ticket = self.ticket.take().ok_or(IggyError::InvalidConfiguration)?;
        let operation_id = ticket
            .retain_fetch_targets(uncertain.clone())
            .ok_or(IggyError::InvalidConfiguration)?;
        drop(ticket);
        Ok(Some(UncertainFetch {
            operation_id,
            partitions: uncertain,
        }))
    }

    fn new(code: u32, payload: Bytes, target: RequestTarget<'_>, deadline: Instant) -> Self {
        let (partition, replica) = match target {
            RequestTarget::Metadata => (None, None),
            RequestTarget::Partition(target) => (
                Some(PartitionCall {
                    key: PartitionKey::from(&target),
                    context: target.context,
                }),
                None,
            ),
            RequestTarget::Replica(replica) => (None, Some(replica)),
        };
        Self {
            code,
            payload,
            partition,
            replica,
            roster_read: false,
            started: Instant::now(),
            deadline,
            ticket: None,
            frame: None,
            progress_poll: false,
            committing_poll: false,
            uncertain: false,
            refusal: IggyError::TransientNotAccepted,
            retries: 0,
            not_primary: false,
            restarted: false,
            fetch: None,
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
        let payload = self.deferred_payload();
        let ticket = self.ticket.insert(ticket);
        let payload = payload.map_err(Next::Fail)?;
        let mut frame = match &self.frame {
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
        if let Some(payload) = payload {
            // Non-replicated reads have no replay receipt; their budgets can shrink
            // without changing the request identity used to match replies.
            frame.body = payload;
        }
        Ok((frame, ticket))
    }

    fn deferred_payload(&self) -> Result<Option<Bytes>, IggyError> {
        let remaining = |wait_us: u64, request_timeout_us: u64| {
            let now = Instant::now();
            let mut options = DeferredPollOptions {
                max_wait: wait_us.into(),
                request_timeout: request_timeout_us.into(),
                ..DeferredPollOptions::default()
            }
            .remaining(now.saturating_duration_since(self.started))?;
            options.request_timeout = options
                .request_timeout
                .get_duration()
                .min(self.deadline.saturating_duration_since(now))
                .into();
            let request_timeout_us = options.request_timeout.as_micros();
            if request_timeout_us == 0 {
                return Err(IggyError::TransientNotAccepted);
            }
            let wait_us = options
                .max_wait
                .as_micros()
                .min(request_timeout_us.saturating_sub(MIN_DEFERRED_POLL_REPLY_MARGIN_US));
            Ok((wait_us, request_timeout_us))
        };
        match self.code {
            POLL_MESSAGES_DEFERRED_CODE => {
                let mut request = DeferredPollMessagesRequest::decode_from(&self.payload)
                    .map_err(|_| IggyError::InvalidCommand)?;
                (request.wait_us, request.request_timeout_us) =
                    remaining(request.wait_us, request.request_timeout_us)?;
                Ok(Some(request.to_bytes()))
            }
            FETCH_MESSAGES_CODE => {
                let mut request = self.fetch.clone().ok_or(IggyError::InvalidCommand)?;
                (request.wait_us, request.request_timeout_us) =
                    remaining(request.wait_us, request.request_timeout_us)?;
                // A fetch needs the reply margin also without a wait.
                if request.request_timeout_us < MIN_DEFERRED_POLL_REPLY_MARGIN_US {
                    return Err(IggyError::TransientNotAccepted);
                }
                Ok(Some(request.to_bytes()))
            }
            _ => Ok(None),
        }
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

    /// The next step after a route lookup of the call failed with `error`.
    fn lookup_failed(&mut self, error: IggyError) -> Next {
        if retries_lookup(&error) {
            self.refusal = IggyError::TransientNotAccepted;
            Next::Pause
        } else {
            Next::Fail(error)
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

/// Lowers the byte limits of `request` until its whole reply fits in
/// `limit`, the largest reply body that a link reads. A link fails on a
/// larger reply, so the server must not fill more.
fn fit_fetch_reply(request: &mut FetchMessagesRequest, limit: usize) -> Result<(), IggyError> {
    let records = limit
        .checked_sub(FetchMessagesResponse::head_size(request.targets.len()))
        .filter(|records| *records >= MIN_DEFERRED_POLL_MAX_BYTES)
        .ok_or(IggyError::InvalidSizeBytes)?;
    request.max_bytes = request
        .max_bytes
        .min(u32::try_from(records).unwrap_or(u32::MAX));
    for target in &mut request.targets {
        target.max_bytes = target.max_bytes.min(request.max_bytes);
    }
    Ok(())
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
    use crate::clients::reader::fixtures::served;
    use crate::links::OPERATION_TIMEOUT;
    use crate::links::RETRY_BACKOFF_MAX;
    use crate::links::fake_cluster::{
        Answer, FIRST_SESSION, FakeCluster, Received, TEST_BUDGET, UNANSWERED_CODE, connected,
        endpoint, eventually, links, policy, topic_body,
    };
    use crate::tcp::tcp_client::TcpClient;
    use bytes::BytesMut;
    use futures::stream::FuturesUnordered;
    use iggy_binary_protocol::codes::{
        CREATE_STREAM_CODE, GET_CLUSTER_METADATA_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE,
        STORE_CONSUMER_OFFSET_CODE,
    };
    use iggy_binary_protocol::consensus::EvictionReason;
    use iggy_binary_protocol::requests::messages::{
        FetchMessagesTarget, RawMessage, SendMessagesEncoder,
    };
    use iggy_binary_protocol::responses::messages::{
        FetchMessagesSection, FetchMessagesSectionHeader, PollMessagesResponseHeader,
    };
    use iggy_binary_protocol::{WireConsumer, WireIdentifier};
    use iggy_common::{
        AutoLogin, Client, Consumer, Credentials, DEFAULT_POLL_MAX_BYTES, DiagnosticEvent,
        IggyMessage, MessageClient, Partitioning, PendingOperationsConfig, PollOptions,
        PollingStrategy, ServerCommitPolicy, UserClient,
    };
    use secrecy::SecretString;
    use std::iter::repeat_n;
    use std::num::NonZeroUsize;
    use std::str::FromStr;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tokio::time::{sleep, sleep_until, timeout};

    const REPLICAS: u8 = 3;
    const PRIMARY: u8 = 1;
    const PARTITION_ID: u32 = 1;
    /// A partition that keeps its primary when [`PARTITION_ID`] moves.
    const STAYED: u32 = 0;
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

        /// The body of a raw deferred poll that moves its consumer.
        fn deferred_poll(&self) -> Bytes {
            let options = DeferredPollOptions::default();
            DeferredPollMessagesRequest {
                poll: self.poll(true, WirePollingStrategy::next()),
                wait_us: options.max_wait.as_micros(),
                min_count: options.min_count,
                max_bytes: options.max_bytes,
                request_timeout_us: options.request_timeout.as_micros(),
            }
            .to_bytes()
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
                POLL_MESSAGES_DEFERRED_CODE => {
                    self.driver
                        .poll_with_options(
                            &self.poll(true, WirePollingStrategy::first()),
                            None,
                            Some(DeferredPollOptions::default()),
                        )
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
    async fn given_raw_cancel_when_sent_should_refuse_it_before_the_wire() {
        let f = Fixture::new().await;
        assert_eq!(
            f.driver
                .send_raw(CANCEL_DEFERRED_REQUEST_CODE, Bytes::new(), None)
                .await,
            Err(IggyError::InvalidCommand)
        );
        assert!(f.sends().is_empty());
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

    /// A fetch of consumer 1 from `partitions` by the default options. No
    /// target carries a fence.
    fn fetch(
        partitions: impl IntoIterator<Item = u32>,
        auto_commit: bool,
        strategy: WirePollingStrategy,
    ) -> FetchMessagesRequest {
        let options = DeferredPollOptions::default();
        FetchMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            wait_us: options.max_wait.as_micros(),
            request_timeout_us: options.request_timeout.as_micros(),
            min_count: options.min_count,
            count: options.min_count,
            max_bytes: options.max_bytes,
            auto_commit,
            targets: partitions
                .into_iter()
                .map(|partition_id| FetchMessagesTarget {
                    partition_id,
                    strategy: strategy.clone(),
                    max_bytes: options.max_bytes,
                    incarnation: 0,
                    owner_generation: 0,
                })
                .collect(),
        }
    }

    /// A fetch reply with an empty section of each `(partition_id, status)`.
    fn fetched_body(sections: &[(u32, u32)]) -> Answer {
        let response = FetchMessagesResponse {
            sections: sections
                .iter()
                .map(|&(partition_id, status)| FetchMessagesSection {
                    header: FetchMessagesSectionHeader {
                        partition_id,
                        status,
                        current_offset: 0,
                        messages_count: 0,
                        context: PartitionContext::default(),
                        next_offset: 0,
                        records_len: 0,
                    },
                    records: &[],
                })
                .collect(),
        };
        Answer::Body(response.to_bytes().unwrap().to_vec())
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
    async fn given_a_chosen_replica_when_a_read_is_sent_should_reach_only_that_replica_with_the_metadata_watermark()
     {
        let f = Fixture::new().await;

        f.driver
            .send_to_replica(GET_STREAMS_CODE, Bytes::from_static(PAYLOAD), 2)
            .await
            .unwrap();

        let reads = f.cluster.received(GET_STREAMS_CODE);
        assert_eq!(endpoints(&reads), [endpoint(2)]);
        assert_eq!(reads[0].body, PAYLOAD);
        assert_eq!(
            stamped_context(&reads[0]).metadata_op,
            f.driver.session.metadata_watermark()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_89_from_a_chosen_replica_when_reading_should_return_it_without_another_replica()
    {
        let f = Fixture::new().await;
        f.cluster.script(
            &endpoint(2),
            GET_STREAMS_CODE,
            [Answer::Refuse(IggyError::NotPrimary)],
        );

        let result = f
            .driver
            .send_to_replica(GET_STREAMS_CODE, Bytes::from_static(PAYLOAD), 2)
            .await;

        assert_eq!(result, Err(IggyError::NotPrimary));
        assert_eq!(
            endpoints(&f.cluster.received(GET_STREAMS_CODE)),
            [endpoint(2)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_58_and_a_dropped_link_from_a_chosen_replica_when_reading_should_retry_that_replica()
     {
        let f = Fixture::new().await;
        f.cluster.script(
            &endpoint(2),
            GET_STREAMS_CODE,
            [
                Answer::Refuse(IggyError::TransientNotAccepted),
                Answer::HangUp,
            ],
        );

        f.driver
            .send_to_replica(GET_STREAMS_CODE, Bytes::from_static(PAYLOAD), 2)
            .await
            .unwrap();

        assert_eq!(
            endpoints(&f.cluster.received(GET_STREAMS_CODE)),
            [endpoint(2), endpoint(2), endpoint(2)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_link_lost_after_the_write_when_the_chosen_replica_then_refuses_should_return_not_committed()
     {
        let f = Fixture::new().await;
        f.cluster.script(
            &endpoint(2),
            GET_STREAMS_CODE,
            [Answer::HangUp, Answer::Refuse(IggyError::NotPrimary)],
        );

        let result = f
            .driver
            .send_to_replica(GET_STREAMS_CODE, Bytes::from_static(PAYLOAD), 2)
            .await;

        assert_eq!(result, Err(IggyError::TransientNotCommitted));
        assert_eq!(
            endpoints(&f.cluster.received(GET_STREAMS_CODE)),
            [endpoint(2), endpoint(2)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_replica_the_roster_does_not_name_when_reading_should_fail_after_one_roster_read()
     {
        let f = Fixture::new().await;
        let rosters = f.cluster.count(GET_CLUSTER_METADATA_CODE);

        let result = f
            .driver
            .send_to_replica(GET_STREAMS_CODE, Bytes::from_static(PAYLOAD), REPLICAS)
            .await;

        assert_eq!(result, Err(IggyError::InvalidIdentifier));
        assert_eq!(f.cluster.count(GET_CLUSTER_METADATA_CODE), rosters + 1);
        assert!(f.cluster.received(GET_STREAMS_CODE).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_replicated_command_or_a_partition_read_when_sent_to_a_replica_should_refuse_before_the_wire()
     {
        let f = Fixture::new().await;
        let rosters = f.cluster.count(GET_CLUSTER_METADATA_CODE);

        for code in [
            CREATE_STREAM_CODE,
            SEND_MESSAGES_CODE,
            STORE_CONSUMER_OFFSET_CODE,
            POLL_MESSAGES_CODE,
            POLL_MESSAGES_DEFERRED_CODE,
            CANCEL_DEFERRED_REQUEST_CODE,
            GET_CONSUMER_OFFSET_CODE,
        ] {
            assert_eq!(
                f.driver
                    .send_to_replica(code, Bytes::from_static(PAYLOAD), 1)
                    .await,
                Err(IggyError::InvalidCommand)
            );
            assert!(f.cluster.received(code).is_empty());
        }
        assert_eq!(f.cluster.count(GET_CLUSTER_METADATA_CODE), rosters);
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
        for code in [POLL_MESSAGES_CODE, POLL_MESSAGES_DEFERRED_CODE] {
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
                        code,
                        [failure, Answer::Body(Vec::new())],
                    );
                    let request = f.poll(auto_commit, strategy.clone());
                    let options =
                        (code == POLL_MESSAGES_DEFERRED_CODE).then(DeferredPollOptions::default);
                    let result = f.driver.poll_with_options(&request, None, options).await;
                    let polls = f.cluster.received(code);
                    if progress && uncertain {
                        assert_eq!(polls.len(), 1, "uncertain progress must not be replayed");
                        let operation_id = polls[0].header.request;
                        if auto_commit {
                            assert_eq!(
                                result,
                                Err(IggyError::FetchProgressUncertain(operation_id))
                            );
                            let blocked =
                                Err(IggyError::PartitionLaneBlocked(PARTITION_ID, operation_id));
                            assert_eq!(
                                f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None)).await,
                                blocked
                            );
                            assert_eq!(
                                f.driver.poll_with_options(&request, None, options).await,
                                blocked
                            );
                            assert_eq!(f.cluster.count(code), 1);
                            f.driver.abandon(operation_id).unwrap();
                        } else {
                            assert_eq!(result, Err(IggyError::TransientNotCommitted));
                            assert_eq!(
                                f.driver.abandon(operation_id),
                                Err(IggyError::OperationNotRetained(operation_id))
                            );
                        }
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
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_progress_poll_when_cancelled_should_release_the_barrier_without_retention() {
        for code in [
            GET_CONSUMER_OFFSET_CODE,
            STORE_CONSUMER_OFFSET_CODE,
            DELETE_CONSUMER_OFFSET_CODE,
        ] {
            let f = Fixture::new().await;
            f.cluster.script(
                &endpoint(PRIMARY),
                POLL_MESSAGES_DEFERRED_CODE,
                [Answer::Silence],
            );
            let request = f.poll(false, WirePollingStrategy::next());
            let poll =
                f.driver
                    .poll_with_options(&request, None, Some(DeferredPollOptions::default()));
            let offset = f.call(code, f.partition(None));
            tokio::pin!(offset);
            let expired = timeout(CALLER_TIMEOUT, async {
                tokio::pin!(poll);
                tokio::select! {
                    _ = &mut poll => panic!("the unanswered poll returned"),
                    () = eventually("the progress poll arrived", || f.cluster.count(POLL_MESSAGES_DEFERRED_CODE) == 1) => {}
                }
                assert!(futures::poll!(&mut offset).is_pending());
                tokio::select! {
                    _ = &mut poll => panic!("the unanswered poll returned"),
                    _ = &mut offset => panic!("the offset request bypassed the progress barrier"),
                    sent = f.send_messages() => { sent.unwrap(); }
                }
                tokio::select! {
                    _ = &mut poll => panic!("the unanswered poll returned"),
                    _ = &mut offset => panic!("the offset request bypassed the progress barrier"),
                }
            })
            .await;
            assert!(expired.is_err());
            assert_eq!(
                f.cluster.count(SEND_MESSAGES_CODE),
                1,
                "an offset request waiting behind a poll must leave sends unblocked"
            );
            assert_eq!(f.cluster.count(code), 0);
            let poll_id = f.cluster.received(POLL_MESSAGES_DEFERRED_CODE)[0]
                .header
                .request;
            assert_eq!(
                f.driver.abandon(poll_id),
                Err(IggyError::OperationNotRetained(poll_id))
            );
            offset.await.unwrap();
            assert_eq!(f.cluster.count(code), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_committing_poll_when_dropped_after_the_send_should_block_its_partition_until_abandoned()
     {
        for poll_code in [POLL_MESSAGES_CODE, POLL_MESSAGES_DEFERRED_CODE] {
            for code in [
                GET_CONSUMER_OFFSET_CODE,
                STORE_CONSUMER_OFFSET_CODE,
                DELETE_CONSUMER_OFFSET_CODE,
            ] {
                let f = Fixture::new().await;
                let case = format!("{poll_code} then {code}");
                f.cluster
                    .script(&endpoint(PRIMARY), poll_code, [Answer::Silence]);
                let mut poll = Box::pin(f.call_to(poll_code));
                let mut offset = Box::pin(f.call(code, f.partition(None)));
                tokio::select! {
                    polled = &mut poll => panic!("the unanswered poll returned: {polled:?}"),
                    () = eventually("the committing poll arrived", || f.cluster.count(poll_code) == 1) => {}
                }
                assert!(futures::poll!(&mut offset).is_pending(), "{case}");
                tokio::select! {
                    polled = &mut poll => panic!("the unanswered poll returned: {polled:?}"),
                    _ = &mut offset => panic!("the offset request bypassed the progress barrier"),
                    sent = f.send_messages() => { sent.unwrap(); }
                }

                drop(poll);

                let op = f.cluster.received(poll_code)[0].header.request;
                let blocked = Err(IggyError::PartitionLaneBlocked(PARTITION_ID, op));
                if code == GET_CONSUMER_OFFSET_CODE {
                    offset.await.unwrap();
                } else {
                    assert_eq!(offset.await, blocked, "{case}");
                }
                assert_eq!(f.call_to(poll_code).await, blocked, "{case}");
                assert_eq!(
                    f.driver.resolve(op, Instant::now() + CALLER_TIMEOUT).await,
                    Err(IggyError::FetchProgressUncertain(op)),
                    "{case}"
                );
                assert_eq!(f.cluster.count(poll_code), 1, "{case}");
                f.driver.abandon(op).unwrap();
                f.call(code, f.partition(None)).await.unwrap();
                f.call_to(poll_code).await.unwrap();
                assert_eq!(f.cluster.count(poll_code), 2, "{case}");
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_retained_offset_mutation_when_progress_enters_should_return_its_blocking_id() {
        for code in [STORE_CONSUMER_OFFSET_CODE, DELETE_CONSUMER_OFFSET_CODE] {
            let f = Fixture::new().await;
            f.cluster.script(
                &endpoint(PRIMARY),
                code,
                [
                    Answer::Refuse(IggyError::TransientNotCommitted),
                    Answer::Refuse(IggyError::RequestTooOld),
                ],
            );
            let result = f.call(code, f.partition(None)).await;
            let op = f.cluster.received(code)[0].header.request;
            assert_eq!(result, Err(IggyError::OperationUncertain(op)));
            assert_eq!(
                f.call(GET_CONSUMER_OFFSET_CODE, f.partition(None)).await,
                Err(IggyError::LaneBlocked(op))
            );
            // A committing poll enters its lane as a fetch target, which names
            // the partition.
            for (auto_commit, strategy, blocked) in [
                (
                    true,
                    WirePollingStrategy::first(),
                    IggyError::PartitionLaneBlocked(PARTITION_ID, op),
                ),
                (
                    false,
                    WirePollingStrategy::next(),
                    IggyError::LaneBlocked(op),
                ),
            ] {
                let polled = f.driver.poll(&f.poll(auto_commit, strategy), None).await;
                assert_eq!(polled, Err(blocked));
            }
            assert_eq!(f.cluster.count(POLL_MESSAGES_CODE), 0);
            f.driver
                .poll(&f.poll(false, WirePollingStrategy::first()), None)
                .await
                .unwrap();
            f.driver.abandon(op).unwrap();
            f.driver
                .poll(&f.poll(false, WirePollingStrategy::next()), None)
                .await
                .unwrap();
            assert_eq!(f.cluster.count(POLL_MESSAGES_CODE), 2);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_retained_send_when_progress_poll_enters_should_keep_progress_independent() {
        let fixture = Fixture::new().await;
        let op = fixture.uncertain_send().await;
        fixture
            .driver
            .poll(&fixture.poll(false, WirePollingStrategy::next()), None)
            .await
            .unwrap();
        assert_eq!(fixture.cluster.count(POLL_MESSAGES_CODE), 1);
        assert_eq!(
            fixture.send_messages().await,
            Err(IggyError::LaneBlocked(op))
        );
        fixture.driver.abandon(op).unwrap();
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
    async fn given_more_partitions_on_one_endpoint_than_a_fetch_holds_when_grouping_should_split_them()
     {
        let f = Fixture::new().await;
        let partitions: Vec<u32> = (0..=MAX_PARTITIONS_PER_REQUEST).collect();

        let groups = f
            .driver
            .readiness_groups(&f.stream_id, &f.topic_id, &partitions)
            .await
            .unwrap();

        let (first, rest) = partitions.split_at(MAX_PARTITIONS_PER_REQUEST as usize);
        assert_eq!(groups, [first, rest]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_partitions_without_routes_when_grouping_should_look_up_a_bounded_number_at_once()
    {
        const PARTITIONS: u32 = 3 * MAX_ROUTE_LOOKUPS_IN_FLIGHT as u32;
        let f = Fixture::new().await;
        // Descending, so a group in the order that lookups finish differs.
        let partitions: Vec<u32> = (0..PARTITIONS).rev().collect();
        f.cluster.hold(GET_PARTITION_ROUTE_CODE);

        let (groups, ()) = tokio::join!(
            f.driver
                .readiness_groups(&f.stream_id, &f.topic_id, &partitions),
            async {
                eventually("the lookups fill the bound", || {
                    f.cluster.count(GET_PARTITION_ROUTE_CODE) >= MAX_ROUTE_LOOKUPS_IN_FLIGHT
                })
                .await;
                sleep(CALLER_TIMEOUT).await;
                assert_eq!(
                    f.cluster.count(GET_PARTITION_ROUTE_CODE),
                    MAX_ROUTE_LOOKUPS_IN_FLIGHT,
                    "a full bound starts no lookup"
                );
                for _ in 0..MAX_ROUTE_LOOKUPS_IN_FLIGHT {
                    f.cluster.release_latest(GET_PARTITION_ROUTE_CODE);
                }
                f.cluster.release(GET_PARTITION_ROUTE_CODE);
            }
        );

        assert_eq!(
            groups.unwrap(),
            [partitions.as_slice()],
            "a group keeps the order of the partitions"
        );
        assert_eq!(
            f.cluster.count(GET_PARTITION_ROUTE_CODE),
            partitions.len(),
            "one lookup per partition"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_consumer_limit_when_fetch_groups_share_capacity_should_bound_requests_and_release_leases()
     {
        const QUEUED_TIMEOUT: Duration = Duration::from_secs(2);
        let fixture = Fixture::new().await;
        let capacity = Arc::new(Semaphore::new(1));
        let options = DeferredPollOptions::default();
        let first = fetch([STAYED], false, WirePollingStrategy::offset(0));
        let second = fetch([PARTITION_ID], false, WirePollingStrategy::offset(0));
        fixture.cluster.script(
            &endpoint(0),
            GET_PARTITION_ROUTE_CODE,
            [route_body(ROUTE), route_body(MOVED)],
        );
        fixture
            .cluster
            .script(&endpoint(PRIMARY), FETCH_MESSAGES_CODE, [Answer::Silence]);
        let mut parked = Box::pin(
            DEFERRED_CONSUMER_CAPACITY
                .scope(Arc::clone(&capacity), fixture.driver.fetch_messages(&first)),
        );
        tokio::select! {
            reply = &mut parked => panic!("a parked fetch ended: {reply:?}"),
            () = eventually("the first group reaches its primary", || {
                fixture.cluster.count(FETCH_MESSAGES_CODE) == 1
            }) => {}
        }
        let short = FetchMessagesRequest {
            wait_us: 0,
            request_timeout_us: QUEUED_TIMEOUT.as_micros().try_into().unwrap(),
            ..second.clone()
        };
        let mut queued = Box::pin(
            DEFERRED_CONSUMER_CAPACITY
                .scope(Arc::clone(&capacity), fixture.driver.fetch_messages(&short)),
        );
        let mut other_poll = fixture.poll(false, WirePollingStrategy::first());
        other_poll.consumer = WireConsumer::consumer(WireIdentifier::numeric(2));
        tokio::select! {
            biased;
            reply = &mut parked => panic!("a parked fetch ended: {reply:?}"),
            reply = &mut queued => panic!("the second group passed its consumer's fetch: {reply:?}"),
            reply = DEFERRED_CONSUMER_CAPACITY.scope(
                Arc::new(Semaphore::new(1)),
                fixture.driver.poll_with_options(&other_poll, None, Some(options)),
            ) => { reply.unwrap(); }
        }
        assert_eq!(fixture.cluster.count(POLL_MESSAGES_DEFERRED_CODE), 1);
        assert_eq!(
            fixture.driver.deferred_requests.available_permits(),
            MAX_DEFERRED_REQUESTS - 1,
            "queued work must not reserve another consumer's client slots"
        );
        tokio::select! {
            reply = &mut parked => panic!("a parked fetch ended before the queued deadline: {reply:?}"),
            reply = &mut queued => assert_eq!(reply, Err(IggyError::TransientNotAccepted)),
        }
        assert_eq!(fixture.cluster.count(FETCH_MESSAGES_CODE), 1);
        assert_eq!(fixture.cluster.count(POLL_MESSAGES_DEFERRED_CODE), 1);
        assert_eq!(capacity.available_permits(), 0);
        drop(queued);
        drop(parked);
        assert_eq!(capacity.available_permits(), 1);
        assert_eq!(
            fixture.driver.deferred_requests.available_permits(),
            MAX_DEFERRED_REQUESTS
        );
        DEFERRED_CONSUMER_CAPACITY
            .scope(
                Arc::clone(&capacity),
                fixture.driver.fetch_messages(&second),
            )
            .await
            .unwrap();
        let fetched = fixture.cluster.received(FETCH_MESSAGES_CODE);
        assert_eq!(
            endpoints(&fetched),
            [endpoint(PRIMARY), endpoint(MOVED.primary_replica)]
        );
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_fetch_timeout_equal_to_the_margin_when_called_should_fail_before_routing() {
        for raw in [false, true] {
            let fixture = Fixture::new().await;
            let request = FetchMessagesRequest {
                wait_us: 0,
                request_timeout_us: MIN_DEFERRED_POLL_REPLY_MARGIN_US,
                ..fetch([PARTITION_ID], false, WirePollingStrategy::offset(0))
            };

            let result = if raw {
                fixture
                    .driver
                    .send_raw(FETCH_MESSAGES_CODE, request.to_bytes(), None)
                    .await
            } else {
                fixture.driver.fetch_messages(&request).await
            };

            assert_eq!(result, Err(IggyError::InvalidCommand));
            assert_eq!(fixture.cluster.count(GET_PARTITION_ROUTE_CODE), 0);
            assert_eq!(fixture.cluster.count(FETCH_MESSAGES_CODE), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_zero_wait_fetch_when_routing_spends_budget_should_send_only_with_the_reply_margin()
     {
        const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
        const ROUTE_DELAY: Duration = Duration::from_millis(500);
        const REPLY_MARGIN: Duration = Duration::from_micros(MIN_DEFERRED_POLL_REPLY_MARGIN_US);
        for delay in [
            ROUTE_DELAY,
            REQUEST_TIMEOUT - REPLY_MARGIN,
            REQUEST_TIMEOUT - ROUTE_DELAY,
        ] {
            let fixture = Fixture::new().await;
            let request = FetchMessagesRequest {
                wait_us: 0,
                request_timeout_us: REQUEST_TIMEOUT.as_micros() as u64,
                ..fetch([PARTITION_ID], false, WirePollingStrategy::offset(0))
            };
            fixture.cluster.hold(GET_PARTITION_ROUTE_CODE);

            let started = Instant::now();
            let (result, ()) = tokio::join!(fixture.driver.fetch_messages(&request), async {
                eventually("fetch route is held", || {
                    fixture.cluster.count(GET_PARTITION_ROUTE_CODE) == 1
                })
                .await;
                sleep_until(started + delay).await;
                fixture.cluster.release(GET_PARTITION_ROUTE_CODE);
            });

            let sent = fixture.cluster.received(FETCH_MESSAGES_CODE);
            if delay <= REQUEST_TIMEOUT - REPLY_MARGIN {
                result.unwrap();
                assert_eq!(sent.len(), 1);
                let sent = FetchMessagesRequest::decode_from(&sent[0].body).unwrap();
                assert_eq!(sent.wait_us, 0);
                assert!(sent.request_timeout_us >= MIN_DEFERRED_POLL_REPLY_MARGIN_US);
                assert!(sent.request_timeout_us <= (REQUEST_TIMEOUT - delay).as_micros() as u64);
            } else {
                assert_eq!(result, Err(IggyError::TransientNotAccepted));
                assert!(sent.is_empty());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_fetch_of_1_or_1000_targets_when_sent_should_reach_their_primary_with_context_zero_and_fence_each_target()
     {
        const OWNER_GENERATION: u64 = 4;
        for targets in [1, MAX_PARTITIONS_PER_REQUEST] {
            for raw in [false, true] {
                let f = Fixture::new().await;
                let mut request = fetch(0..targets, false, WirePollingStrategy::offset(0));
                // The fence of the caller stays, also one of an older route.
                request.targets[0].incarnation = RECREATED.incarnation;
                request.targets[0].owner_generation = OWNER_GENERATION;

                let reply = if raw {
                    f.driver
                        .send_raw(FETCH_MESSAGES_CODE, request.to_bytes(), None)
                        .await
                } else {
                    f.driver.fetch_messages(&request).await
                };

                reply.unwrap();
                let fetches = f.cluster.received(FETCH_MESSAGES_CODE);
                assert_eq!(endpoints(&fetches), [endpoint(PRIMARY)]);
                assert_eq!(stamped_context(&fetches[0]), PartitionContext::default());
                let sent = FetchMessagesRequest::decode_from(&fetches[0].body).unwrap();
                let fences: Vec<_> = sent
                    .targets
                    .iter()
                    .map(|target| (target.incarnation, target.owner_generation))
                    .collect();
                let mut expected = vec![(ROUTE.incarnation, 0); targets as usize];
                expected[0] = (RECREATED.incarnation, OWNER_GENERATION);
                assert_eq!(fences, expected);
                let frame = Frame {
                    header: fetches[0].header,
                    body: Bytes::from(fetches[0].body.clone()),
                };
                assert_eq!(
                    frame.response_body_limit(),
                    Some(
                        FetchMessagesResponse::head_size(targets as usize)
                            + request.max_bytes as usize
                    ),
                    "a link reads the table and the records"
                );
            }
        }
    }

    #[test]
    fn given_a_link_reply_limit_when_fitting_a_fetch_of_1_or_1000_targets_should_bound_its_whole_reply()
     {
        for targets in [1, MAX_PARTITIONS_PER_REQUEST] {
            let mut request = fetch(0..targets, false, WirePollingStrategy::offset(0));
            let unbounded = request.clone();
            fit_fetch_reply(&mut request, usize::MAX).unwrap();
            assert_eq!(
                request, unbounded,
                "a link that reads more keeps the limits"
            );

            let limit =
                FetchMessagesResponse::head_size(targets as usize) + MIN_DEFERRED_POLL_MAX_BYTES;
            fit_fetch_reply(&mut request, limit).unwrap();

            assert_eq!(request.reply_limit(), limit);
            assert!(
                request
                    .targets
                    .iter()
                    .all(|target| target.max_bytes == request.max_bytes)
            );
            request.validate().unwrap();
            assert_eq!(
                fit_fetch_reply(&mut request, limit - 1),
                Err(IggyError::InvalidSizeBytes)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_live_old_fetch_when_reconnected_should_block_stores_until_explicit_abandon() {
        for raw in [false, true] {
            let f = Fixture::new().await;
            let release = Arc::new(tokio::sync::Notify::new());
            let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            f.cluster.script(
                &endpoint(PRIMARY),
                FETCH_MESSAGES_CODE,
                [Answer::GatedBody {
                    release: Arc::clone(&release),
                    completed: Arc::clone(&completed),
                    body: 0_u32.to_le_bytes().to_vec(),
                }],
            );
            let request = fetch([STAYED, PARTITION_ID], true, WirePollingStrategy::offset(0));
            let result = if raw {
                f.driver
                    .send_raw(
                        FETCH_MESSAGES_CODE,
                        request.to_bytes(),
                        Some(PartitionContext::default()),
                    )
                    .await
            } else {
                f.driver.fetch_messages(&request).await
            };
            let operation_id = f.cluster.received(FETCH_MESSAGES_CODE)[0].header.request;
            assert!(
                matches!(result, Err(IggyError::FetchProgressUncertain(operation)) if operation == operation_id)
            );
            assert!(!completed.load(Ordering::SeqCst));
            f.cluster.drop_connections(&endpoint(PRIMARY));
            for partition_id in [STAYED, PARTITION_ID] {
                for context in [None, Some(PartitionContext::default())] {
                    let target = RequestTarget::Partition(PartitionTarget {
                        stream_id: &f.stream_id,
                        topic_id: &f.topic_id,
                        partition_id,
                        context,
                    });
                    if context.is_some() {
                        f.cluster.script(
                            &endpoint(PRIMARY),
                            GET_CONSUMER_OFFSET_CODE,
                            [Answer::Refuse(IggyError::HistoryUnavailable)],
                        );
                        assert_eq!(
                            f.call(GET_CONSUMER_OFFSET_CODE, target).await,
                            Err(IggyError::HistoryUnavailable),
                        );
                    } else {
                        f.call(GET_CONSUMER_OFFSET_CODE, target).await.unwrap();
                    }
                    for code in [STORE_CONSUMER_OFFSET_CODE, DELETE_CONSUMER_OFFSET_CODE] {
                        assert!(matches!(f.call(code, target).await,
                            Err(IggyError::PartitionLaneBlocked(partition, operation))
                            if partition == partition_id && operation == operation_id));
                    }
                }
            }
            for options in [None, Some(DeferredPollOptions::default())] {
                assert!(matches!(
                    f.driver.poll_with_options(
                        &f.poll(true, WirePollingStrategy::next()),
                        Some(PartitionContext::default()),
                        options,
                    ).await,
                    Err(IggyError::PartitionLaneBlocked(partition, operation))
                    if partition == PARTITION_ID && operation == operation_id));
            }
            let alias = Identifier::from_str("same-stream-alias").unwrap();
            let alias_target = RequestTarget::Partition(PartitionTarget {
                stream_id: &alias,
                topic_id: &f.topic_id,
                partition_id: PARTITION_ID,
                context: Some(PartitionContext::default()),
            });
            assert!(
                matches!(f.call(STORE_CONSUMER_OFFSET_CODE, alias_target).await,
            Err(IggyError::PartitionLaneBlocked(partition, operation))
            if partition == PARTITION_ID && operation == operation_id)
            );
            let sent = &f.cluster.received(FETCH_MESSAGES_CODE)[0].body;
            let wire = FetchMessagesRequest::decode_from(sent).unwrap();
            assert!(
                wire.targets
                    .iter()
                    .all(|target| target.incarnation == ROUTE.incarnation)
            );
            assert_eq!(f.cluster.count(STORE_CONSUMER_OFFSET_CODE), 0);
            assert_eq!(f.cluster.count(DELETE_CONSUMER_OFFSET_CODE), 0);
            assert_eq!(f.cluster.count(POLL_MESSAGES_CODE), 0);
            assert_eq!(f.cluster.count(POLL_MESSAGES_DEFERRED_CODE), 0);
            assert!(
                matches!(f.driver.resolve(operation_id, Instant::now() + CALLER_TIMEOUT).await,
            Err(IggyError::FetchProgressUncertain(operation)) if operation == operation_id)
            );
            assert_eq!(f.cluster.count(FETCH_MESSAGES_CODE), 1);
            assert!(
                !completed.load(Ordering::SeqCst),
                "the old server request has not finished"
            );
            f.driver.abandon(operation_id).unwrap();
            f.cluster.script(
                &endpoint(PRIMARY),
                STORE_CONSUMER_OFFSET_CODE,
                [Answer::Refuse(IggyError::HistoryUnavailable)],
            );
            assert_eq!(
                f.call(
                    STORE_CONSUMER_OFFSET_CODE,
                    f.partition(Some(PartitionContext::default())),
                )
                .await,
                Err(IggyError::HistoryUnavailable),
            );
            assert_eq!(
                stamped_context(&f.cluster.received(STORE_CONSUMER_OFFSET_CODE)[0]),
                PartitionContext::default(),
            );
            f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
                .await
                .unwrap();
            assert_eq!(f.cluster.count(STORE_CONSUMER_OFFSET_CODE), 2);
            assert!(
                !completed.load(Ordering::SeqCst),
                "abandon accepts uncertainty without stopping the old request"
            );
            release.notify_one();
            eventually("the old request completed after explicit release", || {
                completed.load(Ordering::SeqCst)
            })
            .await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_stale_alias_route_when_a_zero_context_store_is_sent_should_preserve_its_invalid_wire_fence()
     {
        let f = Fixture::new().await;
        let alias = Identifier::from_str("same-stream-alias").unwrap();
        let alias_target = RequestTarget::Partition(PartitionTarget {
            stream_id: &alias,
            topic_id: &f.topic_id,
            partition_id: PARTITION_ID,
            context: Some(PartitionContext::default()),
        });
        f.call(
            GET_CONSUMER_OFFSET_CODE,
            RequestTarget::Partition(PartitionTarget {
                stream_id: &alias,
                topic_id: &f.topic_id,
                partition_id: PARTITION_ID,
                context: None,
            }),
        )
        .await
        .unwrap();
        *f.cluster.route.lock().unwrap() = Some(RECREATED);
        f.cluster
            .script(&endpoint(PRIMARY), FETCH_MESSAGES_CODE, [Answer::Silence]);
        let fetched = f
            .driver
            .fetch_messages(&fetch([PARTITION_ID], true, WirePollingStrategy::offset(0)))
            .await;
        let operation_id = f.cluster.received(FETCH_MESSAGES_CODE)[0].header.request;
        assert!(matches!(fetched,
            Err(IggyError::FetchProgressUncertain(operation)) if operation == operation_id));
        f.cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Refuse(IggyError::HistoryUnavailable)],
        );
        assert_eq!(
            f.call(STORE_CONSUMER_OFFSET_CODE, alias_target).await,
            Err(IggyError::HistoryUnavailable),
        );
        let stores = f.cluster.received(STORE_CONSUMER_OFFSET_CODE);
        assert_eq!(stores.len(), 1);
        assert_eq!(stamped_context(&stores[0]), PartitionContext::default());
        assert!(!f.driver.session.retains_operation(stores[0].header.request));
        assert!(f.driver.session.retains_operation(operation_id));
        assert!(
            matches!(f.call(STORE_CONSUMER_OFFSET_CODE, alias_target).await,
            Err(IggyError::PartitionLaneBlocked(partition, operation))
            if partition == PARTITION_ID && operation == operation_id)
        );
        assert_eq!(f.cluster.count(STORE_CONSUMER_OFFSET_CODE), 1);
        assert_eq!(f.cluster.count(FETCH_MESSAGES_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_fetch_reply_after_session_reset_when_classified_should_preserve_known_outcomes_and_caller_isolation()
     {
        for automatic in [false, true] {
            for uncertain in [false, true] {
                let f = Fixture::new().await;
                let release = Arc::new(tokio::sync::Notify::new());
                let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let status = if uncertain {
                    IggyError::TransientNotCommitted.as_code()
                } else {
                    STATUS_OK
                };
                let Answer::Body(body) =
                    fetched_body(&[(STAYED, STATUS_OK), (PARTITION_ID, status)])
                else {
                    unreachable!();
                };
                f.cluster.script(
                    &endpoint(PRIMARY),
                    FETCH_MESSAGES_CODE,
                    [Answer::GatedBody {
                        release: Arc::clone(&release),
                        completed,
                        body,
                    }],
                );
                let request = fetch([STAYED, PARTITION_ID], true, WirePollingStrategy::offset(0));
                let (result, ()) = tokio::join!(f.driver.fetch_sections(&request), async {
                    eventually("the fetch reached the old session", || {
                        f.cluster.count(FETCH_MESSAGES_CODE) == 1
                    })
                    .await;
                    if automatic {
                        f.driver
                            .session
                            .reset_if_fence(f.driver.session.fence(), true)
                            .unwrap();
                    } else {
                        f.driver.session.reset().unwrap();
                    }
                    release.notify_one();
                });
                let operation_id = f.cluster.received(FETCH_MESSAGES_CODE)[0].header.request;
                if automatic {
                    let sections = result.unwrap();
                    assert_eq!(sections.len(), 2);
                    assert!(sections[0].status.is_ok());
                    if uncertain {
                        assert!(matches!(sections[1].status,
                            Err(IggyError::FetchProgressUncertain(operation)) if operation == operation_id));
                    } else {
                        assert!(sections[1].status.is_ok());
                    }
                } else {
                    assert_eq!(result.unwrap_err(), IggyError::Unauthenticated);
                }
                assert_eq!(f.driver.session.retains_operation(operation_id), uncertain);
                for partition_id in [STAYED, PARTITION_ID] {
                    let entered = f
                        .driver
                        .session
                        .enter_lane(
                            Some(LaneKey::Progress {
                                partition_id,
                                incarnation: ROUTE.incarnation,
                            }),
                            Instant::now() + CALLER_TIMEOUT,
                        )
                        .await;
                    if uncertain && partition_id == PARTITION_ID {
                        assert!(matches!(entered,
                            Err(IggyError::PartitionLaneBlocked(partition, operation))
                            if partition == partition_id && operation == operation_id));
                    } else {
                        entered.unwrap().complete();
                    }
                }
                assert_eq!(f.cluster.count(FETCH_MESSAGES_CODE), 1);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_partial_uncertain_fetch_sections_when_decoded_should_preserve_healthy_sections_and_recovery_identity()
     {
        for status in [3, 57, 58, 59, 90, 11001] {
            let f = Fixture::new().await;
            f.cluster.script(
                &endpoint(PRIMARY),
                FETCH_MESSAGES_CODE,
                [fetched_body(&[(STAYED, STATUS_OK), (PARTITION_ID, status)])],
            );
            let sections = f
                .driver
                .fetch_sections(&fetch(
                    [STAYED, PARTITION_ID],
                    true,
                    WirePollingStrategy::offset(0),
                ))
                .await
                .unwrap();
            let operation_id = f.cluster.received(FETCH_MESSAGES_CODE)[0].header.request;
            assert!(sections[0].status.is_ok());
            assert!(sections[0].decode().is_ok());
            assert!(
                matches!(sections[1].status, Err(IggyError::FetchProgressUncertain(operation)) if operation == operation_id)
            );
            let target = RequestTarget::Partition(PartitionTarget {
                stream_id: &f.stream_id,
                topic_id: &f.topic_id,
                partition_id: STAYED,
                context: None,
            });
            f.call(STORE_CONSUMER_OFFSET_CODE, target).await.unwrap();
            assert!(
                matches!(f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None)).await,
                Err(IggyError::PartitionLaneBlocked(partition, operation))
                if partition == PARTITION_ID && operation == operation_id)
            );
            f.driver.abandon(operation_id).unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_malformed_committing_fetch_table_when_returned_should_retain_all_targets() {
        for answer in [
            Answer::Body(Vec::new()),
            fetched_body(&[(STAYED, STATUS_OK), (STAYED, STATUS_OK)]),
            fetched_body(&[(PARTITION_ID + 1, STATUS_OK)]),
        ] {
            let f = Fixture::new().await;
            f.cluster
                .script(&endpoint(PRIMARY), FETCH_MESSAGES_CODE, [answer]);
            let result = f
                .driver
                .fetch_messages(&fetch(
                    [STAYED, PARTITION_ID],
                    true,
                    WirePollingStrategy::offset(0),
                ))
                .await;
            let operation_id = f.cluster.received(FETCH_MESSAGES_CODE)[0].header.request;
            assert!(
                matches!(result, Err(IggyError::FetchProgressUncertain(operation)) if operation == operation_id)
            );
            for partition_id in [STAYED, PARTITION_ID] {
                let target = RequestTarget::Partition(PartitionTarget {
                    stream_id: &f.stream_id,
                    topic_id: &f.topic_id,
                    partition_id,
                    context: None,
                });
                assert!(matches!(f.call(STORE_CONSUMER_OFFSET_CODE, target).await,
                    Err(IggyError::PartitionLaneBlocked(partition, operation))
                    if partition == partition_id && operation == operation_id));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_fetch_refusals_when_classified_should_keep_only_unknown_progress_blocked() {
        for (status, unknown) in [
            (5, false),
            (6, false),
            (58, false),
            (87, false),
            (89, false),
            (90, false),
            (1009, false),
            (1010, false),
            (2010, false),
            (2011, false),
            (3007, false),
            (4100, false),
            (5009, false),
            (3, true),
            (30, true),
            (40, true),
            (41, true),
            (57, true),
            (59, true),
            (80, true),
            (11001, true),
        ] {
            let f = Fixture::new().await;
            let fence = f.driver.session.fence();
            f.cluster.script(
                &endpoint(PRIMARY),
                FETCH_MESSAGES_CODE,
                [Answer::Refuse(IggyError::from_code(status))],
            );
            let result = f
                .driver
                .fetch_messages(&fetch(
                    [STAYED, PARTITION_ID],
                    true,
                    WirePollingStrategy::offset(0),
                ))
                .await;
            let operation_id = f.cluster.received(FETCH_MESSAGES_CODE)[0].header.request;
            if unknown {
                assert!(
                    matches!(result, Err(IggyError::FetchProgressUncertain(operation)) if operation == operation_id),
                    "status {status}: {result:?}"
                );
                if status == IggyError::StaleClient.as_code() {
                    f.next_session(fence).await;
                }
                assert!(
                    matches!(f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None)).await,
                    Err(IggyError::PartitionLaneBlocked(partition, operation))
                    if partition == PARTITION_ID && operation == operation_id),
                    "status {status}"
                );
                assert_eq!(
                    f.cluster.count(FETCH_MESSAGES_CODE),
                    1,
                    "unknown fetches never replay"
                );
            } else {
                assert!(
                    !matches!(result, Err(IggyError::FetchProgressUncertain(_))),
                    "status {status}: {result:?}"
                );
                assert!(matches!(
                    f.driver.abandon(operation_id),
                    Err(IggyError::OperationNotRetained(_))
                ));
                f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
                    .await
                    .unwrap();
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_proved_fetch_sections_when_records_fail_to_decode_should_leave_rewind_available()
    {
        let f = Fixture::new().await;
        let records = b"invalid batch";
        let body = FetchMessagesResponse {
            sections: vec![FetchMessagesSection {
                header: FetchMessagesSectionHeader {
                    partition_id: PARTITION_ID,
                    status: STATUS_OK,
                    current_offset: 7,
                    messages_count: 1,
                    context: PartitionContext::default(),
                    next_offset: 8,
                    records_len: records.len().try_into().unwrap(),
                },
                records,
            }],
        }
        .to_bytes()
        .unwrap();
        f.cluster.script(
            &endpoint(PRIMARY),
            FETCH_MESSAGES_CODE,
            [Answer::Body(body.to_vec())],
        );
        let sections = f
            .driver
            .fetch_sections(&fetch([PARTITION_ID], true, WirePollingStrategy::offset(0)))
            .await
            .unwrap();
        assert!(sections[0].decode().is_err());
        f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
            .await
            .unwrap();
        assert_eq!(f.cluster.count(STORE_CONSUMER_OFFSET_CODE), 1);
        for status in [80, 87, 89, 5009] {
            f.cluster.script(
                &endpoint(PRIMARY),
                FETCH_MESSAGES_CODE,
                [fetched_body(&[(PARTITION_ID, status)])],
            );
            let sections = f
                .driver
                .fetch_sections(&fetch([PARTITION_ID], true, WirePollingStrategy::offset(0)))
                .await
                .unwrap();
            assert_eq!(sections[0].status.as_ref().unwrap_err().as_code(), status);
            f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
                .await
                .unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lost_fetch_reply_when_retrying_should_resend_only_a_fetch_that_moves_no_consumer()
     {
        for (auto_commit, strategy, progress) in [
            (true, WirePollingStrategy::offset(0), true),
            (false, WirePollingStrategy::next(), true),
            (false, WirePollingStrategy::offset(0), false),
        ] {
            for (failure, uncertain) in [
                (Answer::Refuse(IggyError::TransientNotCommitted), true),
                (Answer::HangUp, true),
                (Answer::Refuse(IggyError::TransientNotAccepted), false),
            ] {
                let f = Fixture::new().await;
                f.cluster.script(
                    &endpoint(PRIMARY),
                    FETCH_MESSAGES_CODE,
                    [failure, fetched_body(&[])],
                );

                let result = f
                    .driver
                    .fetch_messages(&fetch(
                        [STAYED, PARTITION_ID],
                        auto_commit,
                        strategy.clone(),
                    ))
                    .await;

                let fetches = f.cluster.received(FETCH_MESSAGES_CODE);
                if progress && uncertain {
                    let operation_id = fetches[0].header.request;
                    let expected = if auto_commit {
                        IggyError::FetchProgressUncertain(operation_id)
                    } else {
                        IggyError::TransientNotCommitted
                    };
                    assert_eq!(result, Err(expected));
                    assert_eq!(
                        fetches.len(),
                        1,
                        "an uncertain progress fetch is never resent"
                    );
                    if auto_commit {
                        assert!(
                            matches!(f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None)).await,
                            Err(IggyError::PartitionLaneBlocked(partition, operation))
                            if partition == PARTITION_ID && operation == operation_id)
                        );
                        f.driver.abandon(operation_id).unwrap();
                    } else {
                        assert_eq!(
                            f.driver.abandon(operation_id),
                            Err(IggyError::OperationNotRetained(operation_id))
                        );
                    }
                    f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
                        .await
                        .unwrap();
                } else {
                    result.unwrap();
                    assert_eq!(
                        fetches.len(),
                        2,
                        "pure reads and refused fetches go out again"
                    );
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_fetch_in_flight_when_offsets_of_its_targets_change_should_hold_them_only_for_a_progress_fetch()
     {
        for (auto_commit, strategy, progress) in [
            (true, WirePollingStrategy::offset(0), true),
            (false, WirePollingStrategy::next(), true),
            (false, WirePollingStrategy::offset(0), false),
        ] {
            let f = Fixture::new().await;
            f.cluster
                .script(&endpoint(PRIMARY), FETCH_MESSAGES_CODE, [Answer::Silence]);
            let request = fetch([STAYED, PARTITION_ID], auto_commit, strategy);
            let mut fetched = Box::pin(f.driver.fetch_messages(&request));
            tokio::select! {
                result = &mut fetched => panic!("the unanswered fetch returned: {result:?}"),
                () = eventually("the fetch arrived", || f.cluster.count(FETCH_MESSAGES_CODE) == 1) => {}
            }
            timeout(CALLER_TIMEOUT, f.send_messages())
                .await
                .expect("a fetch takes no mutation lane")
                .unwrap();
            let store = |partition_id| {
                f.call(
                    STORE_CONSUMER_OFFSET_CODE,
                    RequestTarget::Partition(PartitionTarget {
                        stream_id: &f.stream_id,
                        topic_id: &f.topic_id,
                        partition_id,
                        context: None,
                    }),
                )
            };
            let mut stores = Box::pin(futures::future::join_all([
                store(STAYED),
                store(PARTITION_ID),
            ]));

            let stored = timeout(CALLER_TIMEOUT, &mut stores).await;

            if progress {
                assert!(
                    stored.is_err(),
                    "a store waits for a progress fetch of its partition"
                );
                assert_eq!(f.cluster.count(STORE_CONSUMER_OFFSET_CODE), 0);
                drop(fetched);
                for stored in stores.await {
                    if auto_commit {
                        assert!(matches!(stored, Err(IggyError::PartitionLaneBlocked(..))));
                    } else {
                        stored.unwrap();
                    }
                }
            } else {
                for stored in stored.unwrap() {
                    stored.unwrap();
                }
            }
            assert_eq!(
                f.cluster.count(STORE_CONSUMER_OFFSET_CODE),
                if auto_commit { 0 } else { 2 }
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_retained_offset_mutation_of_a_target_when_a_progress_fetch_enters_should_fail_naming_its_partition()
     {
        let f = Fixture::new().await;
        f.cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [
                Answer::Refuse(IggyError::TransientNotCommitted),
                Answer::Refuse(IggyError::RequestTooOld),
            ],
        );
        let result = f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None)).await;
        let op = f.cluster.received(STORE_CONSUMER_OFFSET_CODE)[0]
            .header
            .request;
        assert_eq!(result, Err(IggyError::OperationUncertain(op)));

        for (auto_commit, strategy) in [
            (true, WirePollingStrategy::offset(0)),
            (false, WirePollingStrategy::next()),
        ] {
            let fetched = f
                .driver
                .fetch_messages(&fetch([STAYED, PARTITION_ID], auto_commit, strategy))
                .await;
            assert!(
                matches!(
                    fetched,
                    Err(IggyError::PartitionLaneBlocked(partition_id, blocking))
                        if partition_id == PARTITION_ID && blocking == op
                ),
                "{fetched:?}"
            );
        }

        assert_eq!(f.cluster.count(FETCH_MESSAGES_CODE), 0);
        f.driver
            .fetch_messages(&fetch(
                [STAYED, PARTITION_ID],
                false,
                WirePollingStrategy::offset(0),
            ))
            .await
            .expect("a fetch that moves no consumer takes no lane");
        f.driver.abandon(op).unwrap();
        f.driver
            .fetch_messages(&fetch(
                [STAYED, PARTITION_ID],
                true,
                WirePollingStrategy::offset(0),
            ))
            .await
            .unwrap();
        assert_eq!(f.cluster.count(FETCH_MESSAGES_CODE), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_89_for_a_fetch_or_one_of_its_sections_when_settled_should_mark_the_refused_routes_moved()
     {
        // A partition whose section is refused because its route no longer holds.
        const REFUSED: u32 = 2;
        let stale_route_refusals = [
            None,
            Some(IggyError::HistoryUnavailable),
            Some(IggyError::ConsumerGroupPartitionNotOwned(0, REFUSED)),
            Some(IggyError::PartitionNotFound(
                REFUSED as usize,
                Identifier::default(),
                Identifier::default(),
            )),
        ];
        for refusal in stale_route_refusals {
            let whole_request = refusal.is_none();
            let f = Fixture::new().await;
            let answer = match &refusal {
                None => Answer::Refuse(IggyError::NotPrimary),
                Some(refusal) => fetched_body(&[
                    (STAYED, STATUS_OK),
                    (PARTITION_ID, IggyError::NotPrimary.as_code()),
                    (REFUSED, refusal.as_code()),
                ]),
            };
            f.cluster
                .script(&endpoint(PRIMARY), FETCH_MESSAGES_CODE, [answer]);

            let result = f
                .driver
                .fetch_messages(&fetch(
                    [STAYED, PARTITION_ID, REFUSED],
                    false,
                    WirePollingStrategy::offset(0),
                ))
                .await;

            assert_eq!(
                f.cluster.count(FETCH_MESSAGES_CODE),
                1,
                "the caller regroups a moved fetch"
            );
            let usable = |partition_id| {
                let key = PartitionKey {
                    stream_id: f.stream_id.clone(),
                    topic_id: f.topic_id.clone(),
                    partition_id,
                };
                f.driver.routes().usable(
                    &key,
                    f.driver.session.fence(),
                    f.driver.session.metadata_watermark(),
                )
            };
            if whole_request {
                assert_eq!(result, Err(IggyError::NotPrimary));
                assert_eq!(usable(STAYED), None);
            } else {
                result.unwrap();
                assert_eq!(usable(STAYED), Some(ROUTE), "{refusal:?}");
            }
            assert_eq!(usable(PARTITION_ID), None);
            assert_eq!(usable(REFUSED), None, "{refusal:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_full_client_when_a_consumer_poll_expires_should_release_its_consumer_lease() {
        let mut fixture = Fixture::new().await;
        fixture.driver.deferred_requests = Semaphore::new(1);
        fixture.cluster.script(
            &endpoint(PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Silence],
        );
        let poll = fixture.poll(false, WirePollingStrategy::first());
        let options = DeferredPollOptions::default();
        let mut parked = Box::pin(fixture.driver.poll_with_options(&poll, None, Some(options)));
        tokio::select! {
            reply = &mut parked => panic!("the client lease was released early: {reply:?}"),
            () = eventually("the first poll occupies the client lease", || {
                fixture.cluster.count(POLL_MESSAGES_DEFERRED_CODE) == 1
            }) => {}
        }
        let capacity = Arc::new(Semaphore::new(1));
        let short_options = DeferredPollOptions {
            max_wait: Duration::ZERO.into(),
            request_timeout: CALLER_TIMEOUT.into(),
            ..options
        };
        tokio::select! {
            reply = &mut parked => panic!("the client lease was released early: {reply:?}"),
            reply = DEFERRED_CONSUMER_CAPACITY.scope(
                Arc::clone(&capacity),
                fixture.driver.poll_with_options(&poll, None, Some(short_options)),
            ) => assert_eq!(reply, Err(IggyError::TransientNotAccepted)),
        }
        assert_eq!(fixture.cluster.count(POLL_MESSAGES_DEFERRED_CODE), 1);
        assert_eq!(capacity.available_permits(), 1);
        assert_eq!(fixture.driver.deferred_requests.available_permits(), 0);
        drop(parked);
        assert_eq!(fixture.driver.deferred_requests.available_permits(), 1);
        DEFERRED_CONSUMER_CAPACITY
            .scope(
                Arc::clone(&capacity),
                fixture.driver.poll_with_options(&poll, None, Some(options)),
            )
            .await
            .unwrap();
        assert_eq!(fixture.cluster.count(POLL_MESSAGES_DEFERRED_CODE), 2);
        assert_eq!(capacity.available_permits(), 1);
        assert_eq!(fixture.driver.deferred_requests.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_session_when_a_raw_deferred_call_needs_a_route_should_end_with_unauthenticated_at_once()
     {
        let mut f = Fixture::new().await;
        f.driver = OperationDriver::new(ReplicaLinks::new(
            f.cluster.dialer(),
            endpoint(0),
            policy(),
            Arc::new(SessionOwner::new(AutoLogin::Disabled)),
        ));
        f.driver.links.connect().await.unwrap();
        assert!(f.driver.links.ready_replicas().is_empty());
        for (code, payload) in [
            (POLL_MESSAGES_DEFERRED_CODE, f.deferred_poll()),
            (
                FETCH_MESSAGES_CODE,
                fetch([PARTITION_ID], false, WirePollingStrategy::next()).to_bytes(),
            ),
        ] {
            let result = timeout(RECOVERY_TIMEOUT, f.driver.send_raw(code, payload, None))
                .await
                .expect("the call must not wait for its deadline");
            assert_eq!(result, Err(IggyError::Unauthenticated), "{code}");
        }
        assert_eq!(f.cluster.count(GET_PARTITION_ROUTE_CODE), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_bound_session_without_credentials_when_its_replicas_bind_again_should_wait_for_a_route()
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
        f.cluster.hold(BIND_SESSION_CODE);
        for id in 0..REPLICAS {
            f.cluster.drop_connections(&endpoint(id));
        }
        eventually("no replica serves the session", || {
            f.driver.links.ready_replicas().is_empty()
        })
        .await;
        let mut poll = Box::pin(f.driver.send_raw(
            POLL_MESSAGES_DEFERRED_CODE,
            f.deferred_poll(),
            None,
        ));

        assert!(timeout(CALLER_TIMEOUT, &mut poll).await.is_err());
        f.cluster.release(BIND_SESSION_CODE);
        poll.await.unwrap();
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
    async fn given_a_committing_poll_when_its_deadline_passes_before_the_send_should_fail_with_58_and_leave_no_block()
     {
        for code in [POLL_MESSAGES_CODE, POLL_MESSAGES_DEFERRED_CODE] {
            let f = Fixture::new().await;
            f.cluster.down.lock().unwrap().insert(endpoint(PRIMARY));
            f.cluster.drop_connections(&endpoint(PRIMARY));
            eventually("the link to the primary is gone", || {
                !f.driver.links.ready_replicas().contains(&PRIMARY)
            })
            .await;

            let result = f.call_to(code).await;

            assert_eq!(result, Err(IggyError::TransientNotAccepted), "{code}");
            assert_eq!(f.cluster.count(code), 0, "{code}");
            f.cluster.down.lock().unwrap().clear();
            eventually("the link to the primary is back", || {
                f.driver.links.ready_replicas().contains(&PRIMARY)
            })
            .await;
            f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
                .await
                .unwrap();
            f.call_to(code).await.unwrap();
            assert_eq!(f.cluster.count(code), 1, "{code}");
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
            (GET_STREAMS_CODE, 0, GET_STREAMS_CODE, refused),
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

    /// The shard watchdog can refuse a read after the read stored the offset,
    /// so a committing poll treats every refusal outside the deny list of a
    /// committing fetch as an unknown outcome.
    #[tokio::test(start_paused = true)]
    async fn given_a_refusal_that_can_follow_the_read_when_the_poll_commits_should_block_its_partition_until_abandoned()
     {
        for code in [POLL_MESSAGES_CODE, POLL_MESSAGES_DEFERRED_CODE] {
            for refusal in [IggyError::ShardCommunicationError, IggyError::StaleClient] {
                for auto_commit in [true, false] {
                    let f = Fixture::new().await;
                    let fence = f.driver.session.fence();
                    let case = format!("{code} auto_commit={auto_commit}: {refusal}");
                    f.cluster
                        .script(&endpoint(PRIMARY), code, [Answer::Refuse(refusal.clone())]);
                    let request = f.poll(auto_commit, WirePollingStrategy::next());
                    let options =
                        (code == POLL_MESSAGES_DEFERRED_CODE).then(DeferredPollOptions::default);

                    let result = f.driver.poll_with_options(&request, None, options).await;

                    let op = f.cluster.received(code)[0].header.request;
                    if refusal == IggyError::StaleClient {
                        f.next_session(fence).await;
                    }
                    if auto_commit {
                        assert_eq!(result, Err(IggyError::FetchProgressUncertain(op)), "{case}");
                        let blocked = Err(IggyError::PartitionLaneBlocked(PARTITION_ID, op));
                        assert_eq!(
                            f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None)).await,
                            blocked,
                            "{case}"
                        );
                        assert_eq!(
                            f.driver.poll_with_options(&request, None, options).await,
                            blocked,
                            "{case}"
                        );
                        f.driver.abandon(op).unwrap();
                    } else {
                        assert_eq!(result, Err(refusal.clone()), "{case}");
                        assert_eq!(
                            f.driver.abandon(op),
                            Err(IggyError::OperationNotRetained(op)),
                            "{case}"
                        );
                    }
                    f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
                        .await
                        .unwrap();
                    f.driver
                        .poll_with_options(&request, None, options)
                        .await
                        .unwrap();
                    assert_eq!(f.cluster.count(code), 2, "{case}");
                }
            }
        }
    }

    /// The server refuses a first message larger than `max_bytes` before it stores the offset,
    /// so the refusal proves the outcome of a committing poll.
    #[tokio::test(start_paused = true)]
    async fn given_an_oversized_first_message_when_a_committing_poll_is_refused_should_leave_its_partition_open()
     {
        let f = Fixture::new().await;
        f.cluster.script(
            &endpoint(PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Refuse(IggyError::InvalidSizeBytes)],
        );
        let request = f.poll(true, WirePollingStrategy::next());
        let options = Some(DeferredPollOptions::default());

        let result = f.driver.poll_with_options(&request, None, options).await;

        assert_eq!(result, Err(IggyError::InvalidSizeBytes));
        let op = f.cluster.received(POLL_MESSAGES_DEFERRED_CODE)[0]
            .header
            .request;
        assert_eq!(
            f.driver.abandon(op),
            Err(IggyError::OperationNotRetained(op))
        );
        f.call(STORE_CONSUMER_OFFSET_CODE, f.partition(None))
            .await
            .unwrap();
        f.driver
            .poll_with_options(&request, None, options)
            .await
            .unwrap();
    }

    /// A first message larger than `max_bytes` comes alone in a second read with the consumer,
    /// the start and the server commit of the first one, so with `OnRead` the server stores the
    /// offset of that message only. A read without a byte limit is not repeated.
    #[tokio::test(start_paused = true)]
    async fn given_an_oversized_first_message_when_polled_should_read_it_alone_once() {
        const OFFSET: u64 = 3;
        for server_commit in [ServerCommitPolicy::Disabled, ServerCommitPolicy::OnRead] {
            for max_bytes in [DEFAULT_POLL_MAX_BYTES, u32::MAX] {
                let case = format!("{server_commit:?} max_bytes={max_bytes}");
                let f = Fixture::new().await;
                let (_, records) = served(PARTITION_ID, &[OFFSET], OFFSET + 1);
                let mut alone = PollMessagesResponseHeader {
                    partition_id: PARTITION_ID,
                    current_offset: OFFSET,
                    messages_count: 1,
                    context: PartitionContext::default(),
                }
                .to_bytes()
                .to_vec();
                alone.extend(records);
                f.cluster.script(
                    &endpoint(PRIMARY),
                    POLL_MESSAGES_DEFERRED_CODE,
                    [
                        Answer::Refuse(IggyError::InvalidSizeBytes),
                        Answer::Body(alone),
                    ],
                );
                let client = TcpClient::from_links(f.driver.links);
                let options = PollOptions {
                    max_bytes,
                    server_commit,
                    ..PollOptions::default()
                };

                let polled = client
                    .poll_messages(
                        &f.stream_id,
                        &f.topic_id,
                        PARTITION_ID,
                        &Consumer::default(),
                        &PollingStrategy::offset(OFFSET),
                        options,
                    )
                    .await;

                let reads: Vec<_> = f
                    .cluster
                    .received(POLL_MESSAGES_DEFERRED_CODE)
                    .iter()
                    .map(|read| DeferredPollMessagesRequest::decode_from(&read.body).unwrap())
                    .collect();
                if max_bytes == u32::MAX {
                    assert_eq!(polled.map(drop), Err(IggyError::InvalidSizeBytes), "{case}");
                    assert_eq!(reads.len(), 1, "{case}");
                    continue;
                }
                let offsets: Vec<u64> = polled
                    .unwrap()
                    .messages
                    .iter()
                    .map(|message| message.header.offset)
                    .collect();
                assert_eq!(offsets, [OFFSET], "{case}");
                let [first, second] = reads.as_slice() else {
                    panic!("{case}: one read alone after the refusal, got {reads:?}");
                };
                let expected = options.alone();
                assert_eq!(
                    (second.poll.count, second.min_count, second.max_bytes),
                    (expected.count, expected.min_count, expected.max_bytes),
                    "{case}"
                );
                assert_eq!(
                    (
                        &second.poll.consumer,
                        &second.poll.strategy,
                        second.poll.auto_commit
                    ),
                    (
                        &first.poll.consumer,
                        &first.poll.strategy,
                        server_commit == ServerCommitPolicy::OnRead
                    ),
                    "{case}"
                );
            }
        }
    }

    /// With count 1, a `Last` start would begin at the last message and skip the messages
    /// before it. The refusal proves that the server stored nothing, so it leaves no block.
    #[tokio::test(start_paused = true)]
    async fn given_an_oversized_first_message_of_a_last_start_when_polled_should_return_the_refusal()
     {
        for server_commit in [ServerCommitPolicy::Disabled, ServerCommitPolicy::OnRead] {
            let f = Fixture::new().await;
            f.cluster.script(
                &endpoint(PRIMARY),
                POLL_MESSAGES_DEFERRED_CODE,
                [Answer::Refuse(IggyError::InvalidSizeBytes)],
            );
            let client = TcpClient::from_links(f.driver.links);
            let options = PollOptions {
                server_commit,
                ..PollOptions::default()
            };

            let polled = client
                .poll_messages(
                    &f.stream_id,
                    &f.topic_id,
                    PARTITION_ID,
                    &Consumer::default(),
                    &PollingStrategy::last(),
                    options,
                )
                .await;

            assert_eq!(
                polled.map(drop),
                Err(IggyError::InvalidSizeBytes),
                "{server_commit:?}"
            );
            let reads = f.cluster.received(POLL_MESSAGES_DEFERRED_CODE);
            let [read] = reads.as_slice() else {
                panic!(
                    "{server_commit:?}: no read alone, got {} reads",
                    reads.len()
                );
            };
            let request = DeferredPollMessagesRequest::decode_from(&read.body).unwrap();
            assert_eq!(request.poll.count, options.count, "{server_commit:?}");
            let op = read.header.request;
            assert_eq!(
                client.driver.abandon(op),
                Err(IggyError::OperationNotRetained(op)),
                "{server_commit:?}"
            );
        }
    }

    /// A partition that cannot serve a poll answers empty with a zero context, and the server
    /// refuses that context as a fence with `HistoryUnavailable`. A loop that reads on with the
    /// continuation repeats its start under the context of the route instead.
    #[tokio::test(start_paused = true)]
    async fn given_a_fail_fast_empty_reply_when_polling_its_continuation_should_send_the_route_context()
     {
        const OFFSET: u64 = 3;
        let f = Fixture::new().await;
        let fail_fast = PollMessagesResponseHeader {
            partition_id: PARTITION_ID,
            current_offset: 0,
            messages_count: 0,
            context: PartitionContext::default(),
        }
        .to_bytes()
        .to_vec();
        f.cluster.script(
            &endpoint(PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Body(fail_fast.clone()), Answer::Body(fail_fast)],
        );
        let client = TcpClient::from_links(f.driver.links);
        let start = PollingStrategy::offset(OFFSET);

        let mut strategy = start;
        for _ in 0..2 {
            strategy = client
                .poll_messages(
                    &f.stream_id,
                    &f.topic_id,
                    PARTITION_ID,
                    &Consumer::default(),
                    &strategy,
                    PollOptions::default(),
                )
                .await
                .unwrap()
                .continuation();
        }

        assert_eq!(strategy, start);
        let reads = f.cluster.received(POLL_MESSAGES_DEFERRED_CODE);
        assert_eq!(reads.len(), 2);
        for read in &reads {
            let request = DeferredPollMessagesRequest::decode_from(&read.body).unwrap();
            assert_eq!(request.poll.strategy, WirePollingStrategy::offset(OFFSET));
            assert_eq!(stamped_context(read), ROUTE.context());
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
    async fn given_an_evicted_poll_when_it_moves_its_consumer_should_end_uncertain_without_a_replay()
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
            let op = polls[0].header.request;
            if progress {
                let uncertain = if auto_commit {
                    IggyError::FetchProgressUncertain(op)
                } else {
                    IggyError::TransientNotCommitted
                };
                assert_eq!(result, Err(uncertain));
                assert_eq!(polls.len(), 1);
            } else {
                assert!(result.is_ok(), "{result:?}");
                assert_eq!(polls.len(), 2);
            }
            let abandoned = if auto_commit {
                Ok(())
            } else {
                Err(IggyError::OperationNotRetained(op))
            };
            assert_eq!(f.driver.abandon(op), abandoned);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lease_wait_and_shorter_caller_deadline_when_polling_should_keep_a_valid_remaining_budget()
     {
        const LEASE_DELAY: Duration = Duration::from_millis(250);
        for budget in [Duration::from_secs(2), Duration::from_millis(500)] {
            let fixture = Fixture::new().await;
            let lease = fixture
                .driver
                .deferred_requests
                .acquire_many(MAX_DEFERRED_REQUESTS as u32)
                .await
                .unwrap();
            let options = DeferredPollOptions {
                max_wait: Duration::from_secs(5).into(),
                request_timeout: Duration::from_secs(8).into(),
                ..DeferredPollOptions::default()
            };
            let poll = fixture.poll(true, WirePollingStrategy::next());
            let deadline = Instant::now() + budget;
            let (result, ()) = tokio::join!(
                with_deadline(
                    deadline,
                    fixture.driver.poll_with_options(&poll, None, Some(options))
                ),
                async {
                    sleep(LEASE_DELAY).await;
                    drop(lease);
                }
            );
            result.unwrap();
            let polls = fixture.cluster.received(POLL_MESSAGES_DEFERRED_CODE);
            assert_eq!(polls.len(), 1);
            let request = DeferredPollMessagesRequest::decode_from(&polls[0].body).unwrap();
            assert!(request.request_timeout_us <= (budget - LEASE_DELAY).as_micros() as u64);
            assert!(request.request_timeout_us > 0);
            if request.request_timeout_us <= MIN_DEFERRED_POLL_REPLY_MARGIN_US {
                assert_eq!(
                    request.wait_us, 0,
                    "a short remaining budget permits only an immediate poll"
                );
            } else {
                assert!(request.wait_us > 0);
                assert!(
                    request.wait_us + MIN_DEFERRED_POLL_REPLY_MARGIN_US
                        <= request.request_timeout_us
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_deferred_routing_and_refusal_delays_when_retrying_should_shrink_each_wire_budget()
     {
        const WAIT: Duration = Duration::from_secs(5);
        const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
        const ROUTE_DELAY: Duration = Duration::from_millis(500);
        const REFUSAL_DELAY: Duration = Duration::from_secs(1);
        for (code, refusal) in [
            (POLL_MESSAGES_DEFERRED_CODE, IggyError::TransientNotAccepted),
            (POLL_MESSAGES_DEFERRED_CODE, IggyError::NotPrimary),
            (
                POLL_MESSAGES_DEFERRED_CODE,
                IggyError::DeferredPollCapacityExceeded,
            ),
            (FETCH_MESSAGES_CODE, IggyError::TransientNotAccepted),
            (FETCH_MESSAGES_CODE, IggyError::DeferredPollCapacityExceeded),
        ] {
            let fixture = Fixture::new().await;
            let options = DeferredPollOptions {
                max_wait: WAIT.into(),
                request_timeout: REQUEST_TIMEOUT.into(),
                ..DeferredPollOptions::default()
            };
            let poll = fixture.poll(true, WirePollingStrategy::next());
            let fetch_request = FetchMessagesRequest {
                wait_us: options.max_wait.as_micros(),
                request_timeout_us: options.request_timeout.as_micros(),
                ..fetch([PARTITION_ID], poll.auto_commit, poll.strategy.clone())
            };
            fixture.cluster.hold(GET_PARTITION_ROUTE_CODE);
            fixture.cluster.hold(code);
            if refusal == IggyError::NotPrimary {
                fixture.cluster.script(
                    &endpoint(PRIMARY),
                    GET_PARTITION_ROUTE_CODE,
                    [route_body(PartitionRouteResponse {
                        view: ROUTE.view + 1,
                        ..ROUTE
                    })],
                );
            }
            fixture.cluster.script(
                &endpoint(PRIMARY),
                code,
                [
                    Answer::Refuse(refusal),
                    if code == FETCH_MESSAGES_CODE {
                        fetched_body(&[])
                    } else {
                        Answer::Body(Vec::new())
                    },
                ],
            );
            let (result, ()) = tokio::join!(
                async {
                    match code {
                        POLL_MESSAGES_DEFERRED_CODE => {
                            fixture
                                .driver
                                .poll_with_options(&poll, None, Some(options))
                                .await
                        }
                        _ => fixture.driver.fetch_messages(&fetch_request).await,
                    }
                },
                async {
                    eventually("initial deferred route is held", || {
                        fixture.cluster.count(GET_PARTITION_ROUTE_CODE) == 1
                    })
                    .await;
                    sleep(ROUTE_DELAY).await;
                    fixture.cluster.release(GET_PARTITION_ROUTE_CODE);
                    eventually("initial deferred refusal is held", || {
                        fixture.cluster.count(code) == 1
                    })
                    .await;
                    sleep(REFUSAL_DELAY).await;
                    fixture.cluster.release(code);
                }
            );
            let attempts = fixture.cluster.received(code);
            assert!(
                result.is_ok(),
                "code={code}, result={result:?}, attempts={}",
                attempts.len()
            );
            assert_eq!(attempts.len(), 2);
            assert_eq!(
                bytemuck::bytes_of(&attempts[0].header),
                bytemuck::bytes_of(&attempts[1].header),
                "only the deferred budget changes on a refused attempt"
            );
            let budgets: Vec<_> = attempts
                .iter()
                .map(|attempt| match code {
                    POLL_MESSAGES_DEFERRED_CODE => {
                        let request =
                            DeferredPollMessagesRequest::decode_from(&attempt.body).unwrap();
                        assert_eq!(request.poll, poll);
                        assert_eq!(request.min_count, options.min_count);
                        assert_eq!(request.max_bytes, options.max_bytes);
                        (request.wait_us, request.request_timeout_us)
                    }
                    _ => {
                        let request = FetchMessagesRequest::decode_from(&attempt.body).unwrap();
                        let fenced = FetchMessagesTarget {
                            incarnation: ROUTE.incarnation,
                            ..fetch_request.targets[0].clone()
                        };
                        assert_eq!(request.targets, [fenced]);
                        (request.wait_us, request.request_timeout_us)
                    }
                })
                .collect();
            assert!(budgets[0].0 <= (WAIT - ROUTE_DELAY).as_micros() as u64);
            assert!(budgets[0].1 <= (REQUEST_TIMEOUT - ROUTE_DELAY).as_micros() as u64);
            assert!(budgets[1].0 < budgets[0].0);
            assert!(
                budgets[1].1 <= (REQUEST_TIMEOUT - ROUTE_DELAY - REFUSAL_DELAY).as_micros() as u64
            );
            assert!(budgets.iter().all(|(wait_us, timeout_us)| {
                *wait_us > 0 && *timeout_us >= *wait_us + MIN_DEFERRED_POLL_REPLY_MARGIN_US
            }));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_capacity_refusals_when_polling_should_back_off_without_refreshing_the_route() {
        for auto_commit in [false, true] {
            let fixture = Fixture::new().await;
            fixture.cluster.script(
                &endpoint(PRIMARY),
                POLL_MESSAGES_DEFERRED_CODE,
                [
                    Answer::Refuse(IggyError::DeferredPollCapacityExceeded),
                    Answer::Refuse(IggyError::DeferredPollCapacityExceeded),
                    Answer::Body(Vec::new()),
                ],
            );
            let request = fixture.poll(auto_commit, WirePollingStrategy::first());
            let started = Instant::now();

            fixture
                .driver
                .poll_with_options(&request, None, Some(DeferredPollOptions::default()))
                .await
                .unwrap();

            assert!(
                started.elapsed() > Duration::ZERO,
                "capacity refusals back off"
            );
            assert_eq!(fixture.cluster.count(GET_PARTITION_ROUTE_CODE), 1);
            let polls = fixture.cluster.received(POLL_MESSAGES_DEFERRED_CODE);
            assert_eq!(polls.len(), 3);
            assert!(polls.iter().all(|poll| {
                poll.endpoint == endpoint(PRIMARY) && poll.header.request == polls[0].header.request
            }));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_many_quiet_polls_when_a_client_is_shared_should_admit_another_consumer() {
        const QUIET_POLLS: usize = 16;
        let fixture = Fixture::new().await;
        fixture.cluster.script(
            &endpoint(PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            repeat_n(Answer::Silence, QUIET_POLLS + 1),
        );
        let request = fixture.poll(false, WirePollingStrategy::first());
        let mut polls: FuturesUnordered<_> = (0..=QUIET_POLLS)
            .map(|_| {
                fixture.driver.poll_with_options(
                    &request,
                    None,
                    Some(DeferredPollOptions::default()),
                )
            })
            .collect();

        tokio::select! {
            reply = polls.next() => panic!("a poll ended before every consumer was admitted: {reply:?}"),
            () = eventually("all consumers reach the primary", || {
                fixture.cluster.count(POLL_MESSAGES_DEFERRED_CODE) == QUIET_POLLS + 1
            }) => {}
        }
        tokio::select! {
            sent = fixture.send_messages() => { sent.unwrap(); }
            reply = polls.next() => panic!("a parked poll blocked the send: {reply:?}"),
        }
        drop(polls);
        assert_eq!(
            fixture.driver.deferred_requests.available_permits(),
            MAX_DEFERRED_REQUESTS,
            "abandoned consumers release their leases"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_slow_poll_when_a_send_shares_the_link_should_not_wait_for_the_poll() {
        for (auto_commit, strategy) in [
            (false, WirePollingStrategy::first()),
            (true, WirePollingStrategy::first()),
            (false, WirePollingStrategy::next()),
        ] {
            let f = Fixture::new().await;
            f.cluster.script(
                &endpoint(PRIMARY),
                POLL_MESSAGES_DEFERRED_CODE,
                [Answer::Silence],
            );
            let request = f.poll(auto_commit, strategy);
            let poll =
                f.driver
                    .poll_with_options(&request, None, Some(DeferredPollOptions::default()));
            tokio::pin!(poll);
            tokio::select! {
                result = &mut poll => panic!("the poll ended before its reply: {result:?}"),
                () = eventually("the poll waits at the primary", || f.cluster.count(POLL_MESSAGES_DEFERRED_CODE) == 1) => {}
            }
            let sent = tokio::select! {
                sent = f.send_messages() => sent,
                polled = &mut poll => panic!("the poll ended first: {polled:?}"),
            };
            sent.unwrap();
            assert_eq!(endpoints(&f.sends()), [endpoint(PRIMARY)]);
        }
    }
}
