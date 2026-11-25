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
use std::future::{Future, poll_fn};
use std::io;
use std::ops::Range;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::Bytes;
use futures::future::{self, Either, FutureExt, LocalBoxFuture};
use futures::{Stream, StreamExt};
use iggy_object_store::{
    DeleteOutcome, ErrorClass, FreshObjectId, ListPage, ListRequest, ObjectMeta, ObjectRange,
    ObjectStore, ObjectStoreError, PutBody, PutMode, PutOptions, validate_metadata,
};
use message_bus::MessageBus;
use tracing::debug;

use crate::metrics::TierStoreCounters;

/// The user metadata entry that names the put that stored an object. Every
/// attempt of one put carries the same value, so a put that finds an object
/// under its key tells its own from another writer's.
pub const PUT_ID_METADATA: &str = "put-id";

const MIB: u64 = 1024 * 1024;

/// How the store of a tier service treats the calls of its jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreCallPolicy {
    /// The longest wait for the answer to one attempt at a head, a get, a
    /// list or a delete.
    pub request_timeout: Duration,
    /// The longest a put whose body streams, or an attempt at an overwrite
    /// whose body is held in memory, waits for its provider to take the next
    /// piece of the body, or to answer after the last piece.
    pub progress_timeout: Duration,
    /// The most attempts at a call that keeps failing with a transient
    /// error, the first one included: a head, a get, a list, a delete, and a
    /// put whose body is held in memory. A value of 1 or less retries
    /// nothing.
    pub max_attempts: u32,
    /// The wait before the first retry, doubled for each retry after it.
    pub retry_delay: Duration,
    /// The longest wait between two attempts.
    pub max_retry_delay: Duration,
    /// The longest an attempt at a create-only put whose body is held in
    /// memory waits for its provider to take the next piece of the body.
    pub attempt_stall_timeout: Duration,
    /// The longest such an attempt waits for an answer after its provider
    /// took the last piece.
    pub attempt_answer_timeout: Duration,
    pub second_attempts: SecondAttempts,
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
            attempt_stall_timeout: Duration::from_secs(1),
            attempt_answer_timeout: Duration::from_secs(5),
            second_attempts: SecondAttempts::default(),
        }
    }
}

/// When a slow create-only put whose body is held in memory gets a second
/// attempt beside its first one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecondAttempts {
    /// How long an attempt at an empty body runs before the second attempt
    /// starts. Zero starts none.
    pub after: Duration,
    /// What each MiB of the body adds to [`Self::after`].
    pub after_per_mib: Duration,
    /// The most second attempts that run at once on one store.
    pub slots: usize,
}

/// About the 99th percentile of a put by its size, measured on GCS, so about
/// one put in a hundred gets a second attempt.
impl Default for SecondAttempts {
    fn default() -> Self {
        Self {
            after: Duration::from_millis(300),
            after_per_mib: Duration::from_millis(60),
            slots: 4,
        }
    }
}

impl SecondAttempts {
    /// How long an attempt at a body of `len` bytes runs before the second
    /// attempt starts, `None` when none starts.
    #[must_use]
    pub fn threshold(&self, len: u64) -> Option<Duration> {
        if self.after.is_zero() || self.slots == 0 {
            return None;
        }
        let per_body = self.after_per_mib.as_nanos() * u128::from(len) / u128::from(MIB);
        Some(self.after.saturating_add(Duration::from_nanos(
            u64::try_from(per_body).unwrap_or(u64::MAX),
        )))
    }
}

/// The shard that a tier store puts for: its replica and shard, which keep
/// the ids of its puts apart from those of other shards and nodes, and the
/// counters of what its puts did.
#[derive(Debug, Clone, Default)]
pub struct StoreWriter {
    pub replica: u8,
    pub shard: u16,
    pub counters: TierStoreCounters,
}

/// A provider behind the timeouts and retries of a tier service.
///
/// A head, a get, a list and a delete are safe to repeat. Each attempt has
/// [`StoreCallPolicy::request_timeout`], and an attempt that fails with an
/// [`ErrorClass::Transient`] error is tried again after a backoff, up to
/// [`StoreCallPolicy::max_attempts`] in all.
///
/// A put whose body streams is never repeated, because its body streams once
/// and only its caller can build it again. It has no total deadline either,
/// so a large body on a slow link is never cut for being slow. It fails only
/// when its provider takes no piece of the body for
/// [`StoreCallPolicy::progress_timeout`], or gives no answer for that long
/// after the last piece. A put that moves less than one piece in that time
/// counts as stalled.
///
/// A put whose body is held in memory is sent again, with the same backoff
/// and up to the same number of attempts. Each attempt carries the same
/// [`PUT_ID_METADATA`] entry. An attempt at a create-only put is given up
/// after [`StoreCallPolicy::attempt_stall_timeout`] without a piece taken, or
/// [`StoreCallPolicy::attempt_answer_timeout`] without an answer after the
/// last piece. An attempt at an overwrite is given up only after
/// [`StoreCallPolicy::progress_timeout`] in either phase, because one cut
/// early can still land after a later put of other bytes under its key, while
/// a create that lands late finds the object and stores nothing. An attempt
/// that may have reached the store can have stored the
/// object, so a create-only put that the store then refuses counts as stored
/// when the object under its key carries its id, its size and, where both
/// sides know it, its CRC32C. A refusal of a put that nothing could have
/// stored before is the caller's to handle. A create-only put that runs past
/// [`SecondAttempts::threshold`] gets one second attempt on another
/// connection, while one of [`SecondAttempts::slots`] is free, and the first
/// attempt to store the object wins.
///
/// A provider must therefore do the work that grows with the body while it
/// takes the pieces, not after the last one. [`iggy_object_store::FsStore`]
/// syncs its file as the pieces arrive for this reason.
///
/// A timeout fails the attempt as [`ObjectStoreError::Transport`], which is
/// transient, and drops the provider's call, which cancels it.
pub struct TierStore<S, B> {
    provider: S,
    bus: B,
    policy: StoreCallPolicy,
    ids: PutIds,
    counters: TierStoreCounters,
    /// Second attempts that run now.
    second_attempts: Cell<usize>,
}

impl<S, B> TierStore<S, B>
where
    S: ObjectStore,
    B: MessageBus + Clone + 'static,
{
    #[must_use]
    pub fn new(provider: S, bus: B, policy: StoreCallPolicy, writer: StoreWriter) -> Self {
        let ids = PutIds {
            replica: writer.replica,
            shard: writer.shard,
            started_at: bus.realtime_micros(),
            sequence: Cell::new(0),
        };
        Self {
            provider,
            bus,
            policy,
            ids,
            counters: writer.counters,
            second_attempts: Cell::new(0),
        }
    }

    #[must_use]
    pub const fn provider(&self) -> &S {
        &self.provider
    }

    #[cfg(test)]
    pub(crate) const fn counters(&self) -> &TierStoreCounters {
        &self.counters
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

    /// Resolves with the timeout that ran out: `stall` while the provider
    /// has body left to take, and `answer` once it took the last piece, each
    /// counted from the provider's last step.
    async fn quiet(&self, progress: &Progress, stall: Duration, answer: Duration) -> Duration {
        loop {
            let timeout = if progress.ended.get() { answer } else { stall };
            let limit = micros(timeout);
            let quiet = self
                .bus
                .monotonic_micros()
                .saturating_sub(progress.at.get());
            if quiet >= limit {
                return timeout;
            }
            self.bus.sleep(Duration::from_micros(limit - quiet)).await;
        }
    }

    /// `options` with a new put id, `None` when the put is sent once: its
    /// body streams, the caller named the put, or the id does not fit.
    fn tagged(&self, body: &PutBody, options: &PutOptions) -> Option<PutOptions> {
        if !body.is_repeatable() || options.metadata.contains_key(PUT_ID_METADATA) {
            return None;
        }
        let id = self.ids.next()?;
        let mut tagged = options.clone();
        tagged
            .metadata
            .insert(PUT_ID_METADATA.to_owned(), format!("{id:032x}"));
        validate_metadata(&tagged.metadata)
            .is_ok()
            .then_some(tagged)
    }

    /// A slot for a second attempt, given back when it drops, `None` when
    /// every slot is taken.
    fn second_attempt_slot(&self) -> Option<SecondAttemptSlot<'_>> {
        let running = self.second_attempts.get();
        if running >= self.policy.second_attempts.slots {
            return None;
        }
        self.second_attempts.set(running + 1);
        Some(SecondAttemptSlot(&self.second_attempts))
    }
}

impl<S, B> TierStore<S, B>
where
    S: ObjectStore,
    B: MessageBus + Clone + Unpin + 'static,
{
    /// One attempt at a put, which fails as a transport error when its
    /// provider takes no piece of the body for `stall`, or gives no answer
    /// for `answer` after the last piece.
    async fn attempt(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
        stall: Duration,
        answer: Duration,
    ) -> Result<(), ObjectStoreError> {
        let len = body.len();
        let progress = Rc::new(Progress {
            at: Cell::new(self.bus.monotonic_micros()),
            left: Cell::new(len),
            ended: Cell::new(false),
        });
        let watched = Watched {
            pieces: body.into_pieces(),
            progress: Rc::clone(&progress),
            bus: self.bus.clone(),
        };
        let put = pin!(
            self.provider
                .put(key, PutBody::from_stream(len, watched), options)
        );
        let quiet = pin!(self.quiet(&progress, stall, answer));
        match future::select(put, quiet).await {
            Either::Left((result, _)) => result,
            Either::Right((timeout, _)) => Err(ObjectStoreError::Transport(format!(
                "the put of {key:?} made no progress for {timeout:?}"
            ))),
        }
    }

    /// One attempt at a put whose body is held in memory, with a copy of
    /// `body`: with the short deadlines of an attempt for a create-only put,
    /// and with the progress timeout for an overwrite, see [`TierStore`].
    async fn attempt_again(
        &self,
        key: &str,
        body: &PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        let Some(copy) = body.try_clone() else {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "the body of the put of {key:?} cannot be sent again"
            )));
        };
        let (stall, answer) = if options.mode == PutMode::IfAbsent {
            (
                self.policy.attempt_stall_timeout,
                self.policy.attempt_answer_timeout,
            )
        } else {
            (self.policy.progress_timeout, self.policy.progress_timeout)
        };
        self.attempt(key, copy, options, stall, answer).await
    }

    /// A put whose body is held in memory: rounds of attempts until one
    /// stores the object, or a round fails for a reason that another round
    /// does not change, or the rounds run out.
    async fn put_repeatable(
        &self,
        key: &str,
        body: &PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        let mut delay = self.policy.retry_delay;
        let mut reached = false;
        let mut attempt = 1;
        loop {
            let error = match self.round(key, body, options, &mut reached).await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            if !worth_another_attempt(&error, options) || attempt >= self.policy.max_attempts {
                return Err(error);
            }
            debug!(
                key,
                attempt,
                retry_in_ms = delay.as_millis(),
                may_have_stored = reached,
                %error,
                "tier store put failed, sending it again"
            );
            self.counters.record_put_retry();
            self.bus.sleep(delay).await;
            delay = delay.saturating_mul(2).min(self.policy.max_retry_delay);
            attempt += 1;
        }
    }

    /// One attempt, and a second one beside it once the first runs past the
    /// threshold of a create-only put. The round ends with the first attempt
    /// that stores the object, or once every attempt failed. `reached` says
    /// whether an attempt before this one may have reached the store, and the
    /// round adds its own.
    async fn round(
        &self,
        key: &str,
        body: &PutBody,
        options: &PutOptions,
        reached: &mut bool,
    ) -> Result<(), ObjectStoreError> {
        let if_absent = options.mode == PutMode::IfAbsent;
        let threshold = if if_absent {
            self.policy.second_attempts.threshold(body.len())
        } else {
            None
        };
        let mut first: Option<LocalBoxFuture<'_, _>> =
            Some(self.attempt_again(key, body, options).boxed_local());
        let mut second: Option<LocalBoxFuture<'_, _>> = None;
        let mut slow: Option<LocalBoxFuture<'_, ()>> =
            threshold.map(|after| self.bus.sleep(after).boxed_local());
        loop {
            let (result, by_second) = match next_event(&mut first, &mut second, &mut slow).await {
                Event::Slow => {
                    slow = None;
                    if let Some(slot) = self.second_attempt_slot() {
                        debug!(key, "tier store put is slow, sending a second attempt");
                        self.counters.record_second_attempt();
                        second = Some(
                            async move {
                                let _slot = slot;
                                self.attempt_again(key, body, options).await
                            }
                            .boxed_local(),
                        );
                    }
                    continue;
                }
                Event::First(result) => {
                    first = None;
                    slow = None;
                    (result, false)
                }
                Event::Second(result) => {
                    second = None;
                    (result, true)
                }
            };
            let other_runs = first.is_some() || second.is_some();
            match result {
                Ok(()) => {
                    if by_second {
                        self.counters.record_second_attempt_win();
                    }
                    return Ok(());
                }
                Err(refused @ ObjectStoreError::PreconditionFailed { .. })
                    if if_absent && (*reached || other_runs) =>
                {
                    // The object under the key decides now, so the other
                    // attempt is dropped, which cancels it.
                    drop((first.take(), second.take()));
                    return match self.holds_own_object(key, body.len(), options).await {
                        Ok(true) => {
                            debug!(key, "tier store put found the object it stored");
                            self.counters.record_own_object_found();
                            Ok(())
                        }
                        Ok(false) => Err(refused),
                        Err(error) => Err(error),
                    };
                }
                Err(error) => {
                    *reached |= error.put_may_have_stored();
                    if !other_runs {
                        return Err(error);
                    }
                    debug!(key, %error, "one attempt of a tier store put failed, the other runs");
                }
            }
        }
    }

    /// Whether the object under `key` is the one that a put of `len` bytes
    /// with `options` stores: an earlier attempt of the same put stored it.
    async fn holds_own_object(
        &self,
        key: &str,
        len: u64,
        options: &PutOptions,
    ) -> Result<bool, ObjectStoreError> {
        let Some(id) = options.metadata.get(PUT_ID_METADATA) else {
            return Ok(false);
        };
        let held = match self.head(key).await {
            Ok(held) => held,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(error),
        };
        let same_id = held.metadata.get(PUT_ID_METADATA) == Some(id);
        let same_size = held.size == len;
        let same_crc32c = held
            .crc32c
            .zip(options.crc32c)
            .is_none_or(|(stored, sent)| stored == sent);
        Ok(same_id && same_size && same_crc32c)
    }

    /// A put that streams: one attempt with the progress timeout.
    async fn put_once(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        let timeout = self.policy.progress_timeout;
        self.attempt(key, body, options, timeout, timeout).await
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
        match self.tagged(&body, options) {
            Some(tagged) => self.put_repeatable(key, &body, &tagged).await,
            None => self.put_once(key, body, options).await,
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

/// What ended the wait of a round.
enum Event {
    First(Result<(), ObjectStoreError>),
    Second(Result<(), ObjectStoreError>),
    /// The first attempt ran past the threshold of a second one.
    Slow,
}

/// The next of the attempts that ends, or the threshold that passes.
async fn next_event(
    first: &mut Option<LocalBoxFuture<'_, Result<(), ObjectStoreError>>>,
    second: &mut Option<LocalBoxFuture<'_, Result<(), ObjectStoreError>>>,
    slow: &mut Option<LocalBoxFuture<'_, ()>>,
) -> Event {
    poll_fn(|context| {
        if let Some(attempt) = first.as_mut()
            && let Poll::Ready(result) = attempt.as_mut().poll(context)
        {
            return Poll::Ready(Event::First(result));
        }
        if let Some(attempt) = second.as_mut()
            && let Poll::Ready(result) = attempt.as_mut().poll(context)
        {
            return Poll::Ready(Event::Second(result));
        }
        if let Some(timer) = slow.as_mut()
            && timer.as_mut().poll(context).is_ready()
        {
            return Poll::Ready(Event::Slow);
        }
        Poll::Pending
    })
    .await
}

/// Whether a put that failed with `error` can succeed when its body is sent
/// again.
fn worth_another_attempt(error: &ObjectStoreError, options: &PutOptions) -> bool {
    match error {
        // The bytes are the same on every attempt, so they miss the caller's
        // CRC32C again.
        ObjectStoreError::ChecksumMismatch { .. } => options.crc32c.is_none(),
        error => error.class() == ErrorClass::Transient,
    }
}

/// Mints the ids of the puts of one store in the layout of a
/// [`FreshObjectId`]: the bus's wall clock when the store started, the
/// replica and the shard keep them apart across restarts and nodes, and a
/// count keeps them apart within one store.
struct PutIds {
    replica: u8,
    shard: u16,
    started_at: u64,
    sequence: Cell<u64>,
}

impl PutIds {
    fn next(&self) -> Option<u128> {
        let sequence = self.sequence.get();
        self.sequence
            .set(sequence.wrapping_add(1) & FreshObjectId::SEQUENCE_MAX);
        FreshObjectId::new(self.replica, self.shard, self.started_at, sequence)
            .map(FreshObjectId::get)
    }
}

/// A running second attempt, which frees its slot when it ends or is
/// dropped.
struct SecondAttemptSlot<'a>(&'a Cell<usize>);

impl Drop for SecondAttemptSlot<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}

/// How far a provider got with the body of one attempt.
struct Progress {
    /// The bus's monotonic clock at the provider's last step.
    at: Cell<u64>,
    /// Bytes of the body the provider has not taken.
    left: Cell<u64>,
    /// Whether the provider took the whole body.
    ended: Cell<bool>,
}

/// The pieces of a put body, which note when the provider takes each one.
struct Watched<P, B> {
    pieces: P,
    progress: Rc<Progress>,
    /// The bus whose monotonic clock times each piece.
    bus: B,
}

impl<P, B> Stream for Watched<P, B>
where
    P: Stream<Item = io::Result<Bytes>> + Unpin,
    B: MessageBus + Unpin,
{
    type Item = P::Item;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<P::Item>> {
        let next = ready!(self.pieces.poll_next_unpin(context));
        let progress = &self.progress;
        match &next {
            Some(Ok(piece)) => {
                let left = progress.left.get().saturating_sub(piece.len() as u64);
                progress.left.set(left);
                if left == 0 {
                    progress.ended.set(true);
                }
            }
            None => progress.ended.set(true),
            Some(Err(_)) => {}
        }
        // The provider took a piece or found the end of the body, so the
        // wait for its next step starts now.
        progress.at.set(self.bus.monotonic_micros());
        Poll::Ready(next)
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
