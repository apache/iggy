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

//! The store that the jobs of a tier service get: the provider, with the
//! timeouts and retries that [`ObjectStore`] leaves to its caller.

use std::cell::Cell;
use std::future::Future;
use std::ops::Range;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures::future::{self, Either};
use futures::{Stream, StreamExt};
use iggy_object_store::{
    DeleteOutcome, ErrorClass, ListPage, ListRequest, ObjectMeta, ObjectRange, ObjectStore,
    ObjectStoreError, PutBody, PutOptions,
};
use message_bus::MessageBus;
use tracing::debug;

/// How the store of a tier service treats the calls of its jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreCallPolicy {
    /// The longest wait for the answer to one attempt at a head, a get, a
    /// list or a delete.
    pub request_timeout: Duration,
    /// The longest a put waits for its provider to take the next piece of
    /// the body, or to answer after the last piece.
    pub progress_timeout: Duration,
    /// The most attempts at a head, a get, a list or a delete that keeps
    /// failing with a transient error, the first one included. A value of 1
    /// or less retries nothing.
    pub max_attempts: u32,
    /// The wait before the first retry, doubled for each retry after it.
    pub retry_delay: Duration,
    /// The longest wait between two attempts.
    pub max_retry_delay: Duration,
}

/// Provisional values, settled with the other tier service defaults.
impl Default for StoreCallPolicy {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            progress_timeout: Duration::from_secs(30),
            max_attempts: 3,
            retry_delay: Duration::from_millis(200),
            max_retry_delay: Duration::from_secs(5),
        }
    }
}

/// A provider behind the timeouts and retries of a tier service.
///
/// A head, a get, a list and a delete are safe to repeat. Each attempt has
/// [`StoreCallPolicy::request_timeout`], and an attempt that fails with an
/// [`ErrorClass::Transient`] error is tried again after a backoff, up to
/// [`StoreCallPolicy::max_attempts`] in all.
///
/// A put is never repeated, because its body streams once and only its caller
/// can build it again. It has no total deadline either, so a large body on a
/// slow link is never cut for being slow. It fails only when its provider
/// takes no piece of the body for [`StoreCallPolicy::progress_timeout`], or
/// gives no answer for that long after the last piece. A put that moves less
/// than one piece in that time counts as stalled.
///
/// A provider must therefore do the work that grows with the body while it
/// takes the pieces, not after the last one. [`iggy_object_store::FsStore`]
/// syncs its file as the pieces arrive for this reason.
///
/// A timeout fails the call as [`ObjectStoreError::Transport`], which is
/// transient, and drops the provider's call, which cancels it.
pub struct TierStore<S, B> {
    provider: S,
    bus: B,
    policy: StoreCallPolicy,
}

impl<S, B> TierStore<S, B>
where
    S: ObjectStore,
    B: MessageBus + Clone + 'static,
{
    #[must_use]
    pub const fn new(provider: S, bus: B, policy: StoreCallPolicy) -> Self {
        Self {
            provider,
            bus,
            policy,
        }
    }

    #[must_use]
    pub const fn provider(&self) -> &S {
        &self.provider
    }

    /// `call` until it succeeds, fails for a reason other than a transient
    /// one, or runs out of attempts. `target` names what the call is about in
    /// the log.
    async fn retried<T, F, Fut>(
        &self,
        operation: &'static str,
        target: &str,
        mut call: F,
    ) -> Result<T, ObjectStoreError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ObjectStoreError>>,
    {
        let mut delay = self.policy.retry_delay;
        let mut attempt = 1;
        loop {
            match self.bounded(operation, call()).await {
                Err(error)
                    if error.class() == ErrorClass::Transient
                        && attempt < self.policy.max_attempts =>
                {
                    debug!(
                        operation,
                        target,
                        attempt,
                        retry_in_ms = delay.as_millis(),
                        %error,
                        "tier store call failed, retrying"
                    );
                    self.bus.sleep(delay).await;
                    delay = delay.saturating_mul(2).min(self.policy.max_retry_delay);
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    /// One attempt at `call`, which fails as a transport error when it gets
    /// no answer within the request timeout.
    async fn bounded<T>(
        &self,
        operation: &'static str,
        call: impl Future<Output = Result<T, ObjectStoreError>>,
    ) -> Result<T, ObjectStoreError> {
        let call = pin!(call);
        let timeout = pin!(self.bus.sleep(self.policy.request_timeout));
        match future::select(call, timeout).await {
            Either::Left((result, _)) => result,
            Either::Right(((), _)) => Err(ObjectStoreError::Transport(format!(
                "{operation} got no answer within {:?}",
                self.policy.request_timeout
            ))),
        }
    }

    /// Resolves once `progress`, the time of the last step of a put, is the
    /// progress timeout in the past.
    async fn stalled(&self, progress: &Cell<u64>) {
        let timeout = micros(self.policy.progress_timeout);
        loop {
            let quiet = self.bus.monotonic_micros().saturating_sub(progress.get());
            if quiet >= timeout {
                return;
            }
            self.bus.sleep(Duration::from_micros(timeout - quiet)).await;
        }
    }
}

impl<S, B> ObjectStore for TierStore<S, B>
where
    S: ObjectStore,
    B: MessageBus + Clone + Unpin + 'static,
{
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        let progress = Rc::new(Cell::new(self.bus.monotonic_micros()));
        let len = body.len();
        let watched = Watched {
            pieces: body.into_pieces(),
            progress: Rc::clone(&progress),
            bus: self.bus.clone(),
        };
        let put = pin!(
            self.provider
                .put(key, PutBody::from_stream(len, watched), options)
        );
        let stalled = pin!(self.stalled(&progress));
        match future::select(put, stalled).await {
            Either::Left((result, _)) => result,
            Either::Right(((), _)) => Err(ObjectStoreError::Transport(format!(
                "the put of {key:?} made no progress for {:?}",
                self.policy.progress_timeout
            ))),
        }
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        self.retried("GET", key, || self.provider.get_range(key, range.clone()))
            .await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        self.retried("HEAD", key, || self.provider.head(key)).await
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        let first = keys.first().map_or("", String::as_str);
        self.retried("DELETE", first, || self.provider.delete(keys))
            .await
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        self.retried("LIST", request.prefix, || self.provider.list(request))
            .await
    }
}

/// The pieces of a put body, which note when the provider takes each one.
struct Watched<P, B> {
    pieces: P,
    progress: Rc<Cell<u64>>,
    /// The bus whose monotonic clock times each piece.
    bus: B,
}

impl<P: Stream + Unpin, B: MessageBus + Unpin> Stream for Watched<P, B> {
    type Item = P::Item;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<P::Item>> {
        let next = ready!(self.pieces.poll_next_unpin(context));
        // The provider took a piece or found the end of the body, so the
        // wait for its next step starts now.
        self.progress.set(self.bus.monotonic_micros());
        Poll::Ready(next)
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
