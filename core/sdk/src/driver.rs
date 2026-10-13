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

// Unused until the clients send through the driver.
#![allow(dead_code)]

use crate::links::{
    Frame, Link, OPERATION_TIMEOUT, ReplicaLinks, Target, lost_link, pause_before_retry, read_reply,
};
use crate::session_owner::{LaneKey, LaneTicket, RetainedOperation, SessionOwner};
use bytes::Bytes;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, GET_PARTITION_ROUTE_CODE, LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE,
    SEND_MESSAGES_CODE,
};
use iggy_binary_protocol::consensus::{Operation, operation_for_code};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::{ClientState, Identifier, IggyError};
use route_cache::RouteCache;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::time::{Instant, timeout_at};
use tracing::debug;

mod poll_router;
mod route_cache;

pub(crate) use poll_router::{
    PollRouter, PollTransport, ROSTER_READ_TIMEOUT, is_poll_routing_code,
};
pub(crate) use route_cache::PartitionKey;

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

    /// Sends one request and returns the body of its reply.
    pub(crate) async fn send(
        &self,
        code: u32,
        payload: Bytes,
        target: RequestTarget<'_>,
    ) -> Result<Bytes, IggyError> {
        let deadline = Instant::now() + OPERATION_TIMEOUT;
        self.drive(Call::new(code, payload, target, deadline)).await
    }

    /// Replays the retained operation `op` until it has an outcome or
    /// `deadline` passes. Its success frees the lane.
    pub(crate) async fn resolve(&self, op: u64, deadline: Instant) -> Result<(), IggyError> {
        let (ticket, retained) = self.session.resume(op, deadline).await?;
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
        loop {
            let next = match self.attempt(&mut call).await {
                Ok(body) => {
                    call.complete();
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
            }
        }
    }

    /// One attempt of `call`, from the choice of its replica to its reply.
    async fn attempt(&self, call: &mut Call) -> Result<Bytes, Next> {
        if let Some(error) = self.stopped() {
            return Err(Next::Fail(error));
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
            self.wait_for_session(call).await?;
        }
        let route = self.route_for(call).await?;
        let context = self.context(call, route.as_ref());
        // The lane comes first, so a blocked lane ends the call at once, also
        // while no link is ready.
        let ticket = self.ticket(call, &context).await?;
        let hop = match self.hop(call, route).await {
            Ok(hop) => hop,
            Err(next) => {
                call.ticket = Some(ticket);
                return Err(next);
            }
        };
        let deadline = call.deadline;
        let (frame, ticket) = call.frame(ticket, context)?;
        let result = hop.link.send(frame, deadline, ticket.sent_flag()).await;
        let sent = *ticket.sent_flag();
        let result = result.and_then(|reply| read_reply(&self.session, reply));
        self.settle(call, &hop, result, sent).await
    }

    /// The error of a call on a client that nothing brings back.
    fn stopped(&self) -> Option<IggyError> {
        match self.links.state() {
            ClientState::Shutdown => Some(IggyError::ClientShutdown),
            ClientState::Disconnected => Some(IggyError::NotConnected),
            _ => None,
        }
    }

    /// Waits for a bound session while the links sign in again. Without
    /// credentials nothing signs in, so the call goes on unbound.
    async fn wait_for_session(&self, call: &Call) -> Result<(), Next> {
        if is_sign_in(call.code)
            || self.session.epoch().bound
            || self.session.sign_in_credentials().is_none()
        {
            return Ok(());
        }
        let mut epoch = self.session.subscribe_epoch();
        match timeout_at(call.deadline, epoch.wait_for(|epoch| epoch.bound)).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Next::Fail(IggyError::Unauthenticated)),
        }
    }

    /// The route of a partition call, `None` for any other call. A partition
    /// call looks up its route unless the cached one is usable.
    async fn route_for(&self, call: &mut Call) -> Result<Option<PartitionRouteResponse>, Next> {
        let Some(partition) = &call.partition else {
            return Ok(None);
        };
        match self.route(&partition.key, call.deadline).await {
            Ok(route) => Ok(Some(route)),
            Err(error) if retries_lookup(&error) => {
                call.refusal = error;
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
                let key = LaneKey::of(call.code, context, call.partition_id());
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
        let error = match self.links.link(target) {
            Ok(link) => return Ok(Hop { link, route }),
            Err(error) => error,
        };
        call.refusal = error;
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
                self.refresh(call, hop, Failure::Refused).await;
                Err(Next::Pause)
            }
            IggyError::NotPrimary => {
                // Neither was admitted, so the deadline reports both as 58.
                call.refusal = IggyError::TransientNotAccepted;
                self.refresh(call, hop, Failure::NotPrimary).await;
                Err(if repeated { Next::Pause } else { Next::Again })
            }
            error if lost_link(&error) => {
                call.uncertain |= sent;
                call.refusal = error;
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
            IggyError::ConsumerGroupPartitionNotOwned(..) | IggyError::PartitionNotFound(..) => {
                self.invalidate(call, hop);
                Err(Next::Fail(error))
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
        deadline: Instant,
    ) -> Result<PartitionRouteResponse, IggyError> {
        let fence = self.session.fence();
        let cached = self
            .routes()
            .usable(key, fence, self.session.metadata_watermark());
        match cached {
            Some(route) => Ok(route),
            None => self.lookup(key, fence, deadline).await,
        }
    }

    /// Asks one ready replica at a time for the route of `key`, in id order.
    /// The first one is the replica that a refusal named, else the metadata
    /// primary. An answer older than the cached route goes to the next one.
    async fn lookup(
        &self,
        key: &PartitionKey,
        fence: u64,
        deadline: Instant,
    ) -> Result<PartitionRouteResponse, IggyError> {
        let body = GetPartitionRouteRequest::try_from(key)?.to_bytes();
        let hinted = self.routes().ask_first(key);
        let first = hinted.or_else(|| self.links.metadata_primary());
        let mut replicas = self.links.ready_replicas();
        let start = first.map_or(0, |first| replicas.partition_point(|id| *id < first));
        replicas.rotate_left(start);
        let mut refusal = IggyError::NotConnected;
        for replica in replicas {
            let answer = match self.links.link(Target::Replica(replica)) {
                Ok(link) => {
                    self.read(&link, GET_PARTITION_ROUTE_CODE, body.clone(), deadline)
                        .await
                }
                Err(error) => Err(error),
            };
            let answer = answer.and_then(|answer| {
                PartitionRouteResponse::decode_from(&answer).map_err(|_| IggyError::InvalidFormat)
            });
            match answer {
                Ok(answer) => {
                    let merged = self.routes().merge(key, answer, fence);
                    match merged {
                        Some(route) => return Ok(route),
                        None => refusal = IggyError::TransientNotAccepted,
                    }
                }
                Err(error) if retries_lookup(&error) => refusal = error,
                Err(error) => return Err(error),
            }
        }
        Err(refusal)
    }

    /// One read on `link`, without a retry.
    async fn read(
        &self,
        link: &Link,
        code: u32,
        body: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, IggyError> {
        let mut ticket = self.session.enter_lane(None, deadline).await?;
        let frame = ticket.freeze(code, body, self.read_context(), None)?;
        let reply = link.send(frame, deadline, ticket.sent_flag()).await?;
        read_reply(&self.session, reply)
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
    /// primary that the roster names.
    async fn read_roster(&self, link: &Arc<Link>, deadline: Instant) {
        if let Ok(Err(error)) = timeout_at(deadline, self.links.refresh_roster(link)).await {
            debug!("A roster read failed: {error}");
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
    /// An attempt of the frame can have taken effect, so the call never
    /// freezes again.
    uncertain: bool,
    /// The error at the deadline, unless an attempt can have taken effect.
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

/// What a call does after an attempt that did not end it.
enum Next {
    Again,
    Pause,
    Fail(IggyError),
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
    /// after it started over.
    fn frame(
        &mut self,
        ticket: LaneTicket,
        context: PartitionContext,
    ) -> Result<(Frame, &mut LaneTicket), Next> {
        let ticket = self.ticket.insert(ticket);
        let frame = match &self.frame {
            Some(frame) => frame.clone(),
            None => {
                let partition = self
                    .partition
                    .as_ref()
                    .map(|partition| partition.key.clone());
                let frame = ticket
                    .freeze(self.code, self.payload.clone(), context, partition)
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

    fn complete(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            ticket.complete();
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

    /// Ends the call at its deadline with its last refusal.
    fn expire(mut self) -> IggyError {
        let refusal = std::mem::replace(&mut self.refusal, IggyError::TransientNotAccepted);
        self.end(refusal)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::links::RETRY_BACKOFF_MAX;
    use crate::links::fake_cluster::{
        Answer, FakeCluster, Received, UNANSWERED_CODE, connected, endpoint, eventually,
    };
    use iggy_binary_protocol::codes::{
        CREATE_STREAM_CODE, GET_CLUSTER_METADATA_CODE, GET_STREAMS_CODE, POLL_MESSAGES_CODE,
        STORE_CONSUMER_OFFSET_CODE,
    };
    use std::iter::repeat_n;
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

        assert_eq!(result.unwrap_err(), IggyError::NotConnected);
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

        f.call(POLL_MESSAGES_CODE, f.partition(None)).await.unwrap();
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
        let again = f
            .driver
            .resolve(op, Instant::now() + OPERATION_TIMEOUT)
            .await;
        assert_eq!(again.unwrap_err(), IggyError::OperationNotRetained(op));
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
    async fn given_replicas_that_refuse_every_lookup_when_the_deadline_passes_should_fail_with_the_last_refusal()
     {
        let f = Fixture::new().await;
        *f.cluster.route.lock().unwrap() = None;
        let started = Instant::now();

        let result = f.send_messages().await;

        assert_eq!(result.unwrap_err(), IggyError::TransientNotAccepted);
        let elapsed = started.elapsed();
        assert!(elapsed <= OPERATION_TIMEOUT, "{elapsed:?}");
        assert!(
            elapsed >= OPERATION_TIMEOUT - RETRY_BACKOFF_MAX,
            "{elapsed:?}"
        );
        assert!(f.sends().is_empty());
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
    async fn given_a_slow_poll_when_a_send_shares_the_link_should_not_wait_for_the_poll() {
        let f = Fixture::new().await;
        f.cluster
            .script(&endpoint(PRIMARY), POLL_MESSAGES_CODE, [Answer::Silence]);
        let poll = f.call(POLL_MESSAGES_CODE, f.partition(None));
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
