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

mod parts;

use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::io;
use std::ops::Range;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker, ready};
use std::time::Duration;

use bytes::Bytes;
use futures::future::{self, Either, FutureExt, LocalBoxFuture};
use futures::{Stream, StreamExt};
use iggy_object_store::{
    DeleteOutcome, ErrorClass, FreshObjectId, KeyLayout, ListPage, ListRequest, ObjectMeta,
    ObjectRange, ObjectStore, ObjectStoreError, PutBody, PutMode, PutOptions, SweptUploads,
    validate_metadata,
};
use message_bus::MessageBus;
use tracing::debug;

use crate::metrics::TierStoreCounters;

pub use iggy_object_store::PUT_ID_METADATA;
pub use parts::PartsPolicy;

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
    /// create-only put whose body is held in memory. An overwrite held in
    /// memory is sent again only when its request never left. Every request
    /// of a put counts, a second attempt beside a slow one too. A value of 1
    /// or less retries nothing and starts no second attempt.
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
    pub parts: PartsPolicy,
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
            parts: PartsPolicy::default(),
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

/// The shard that a tier store puts for.
///
/// Its replica and shard keep the ids of its puts apart from those of other
/// shards and nodes. The layout is that of the root it puts under, and
/// without one it puts no object in parts. The counters count what its puts
/// did.
#[derive(Debug, Clone, Default)]
pub struct StoreWriter {
    pub replica: u8,
    pub shard: u16,
    pub layout: Option<KeyLayout>,
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
/// A create-only put whose body is held in memory is sent again, with the
/// same backoff and up to the same number of attempts. An overwrite is sent
/// again only after [`ObjectStoreError::NotSent`]: an attempt that may have
/// reached the store can land after a later overwrite of its key and roll it
/// back, so any other failure goes to the caller. Each attempt carries the
/// same [`PUT_ID_METADATA`] entry. An attempt at a create-only put is given up
/// after [`StoreCallPolicy::attempt_stall_timeout`] without a piece taken, or
/// [`StoreCallPolicy::attempt_answer_timeout`] without an answer after the
/// last piece. An attempt at an overwrite is given up only after
/// [`StoreCallPolicy::progress_timeout`] in either phase, because one cut
/// early can still land after a later put of other bytes under its key, while
/// a create that lands late finds the object and stores nothing. An attempt
/// that may have reached the store can have stored the
/// object, so a create-only put that the store then refuses counts as stored
/// when the object under its key carries its id, its size and its CRC32C,
/// which the store reports or the bytes it holds give. A refusal of a put
/// that nothing could have stored before is the caller's to handle. A
/// create-only put that runs past [`SecondAttempts::threshold`] gets one
/// second attempt on another connection, while one of
/// [`SecondAttempts::slots`] is free and the budget of
/// [`StoreCallPolicy::max_attempts`] has room for it, and the first attempt
/// to store the object wins.
///
/// A long-term segment of at least twice [`PartsPolicy::part_len`] goes up
/// in parts, on a provider that takes them, see [`PartsPolicy`]. Only a
/// create-only put held in memory whose options allow parts does.
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
    layout: Option<KeyLayout>,
    counters: TierStoreCounters,
    /// Second attempts that run now.
    second_attempts: Slots,
    /// Parts of uploads in parts that run now and hold a slot of
    /// [`PartsPolicy::in_flight`].
    parts_in_flight: Slots,
    /// Whether the store refused an upload in parts, so every put goes whole.
    parts_refused: Cell<bool>,
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
            layout: writer.layout,
            counters: writer.counters,
            second_attempts: Slots::default(),
            parts_in_flight: Slots::default(),
            parts_refused: Cell::new(false),
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

    /// The slots of [`PartsPolicy::in_flight`] that parts hold now.
    #[cfg(test)]
    pub(crate) const fn parts_in_flight(&self) -> usize {
        self.parts_in_flight.taken.get()
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
        self.retried_unbounded(operation, target, || self.bounded(operation, call()))
            .await
    }

    /// [`Self::retried`] without the request timeout, for a call whose
    /// provider bounds each of its requests.
    async fn retried_unbounded<T, F, Fut>(
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
            match call().await {
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
}

impl<S, B> TierStore<S, B>
where
    S: ObjectStore,
    B: MessageBus + Clone + Unpin + 'static,
{
    /// One attempt to send `body` with `send`, which fails as a transport
    /// error when the provider takes no piece of the body for `stall`, or
    /// gives no answer for `answer` after the last piece.
    async fn watched<T, F, Fut>(
        &self,
        key: &str,
        body: PutBody,
        stall: Duration,
        answer: Duration,
        send: F,
    ) -> Result<T, ObjectStoreError>
    where
        F: FnOnce(PutBody) -> Fut,
        Fut: Future<Output = Result<T, ObjectStoreError>>,
    {
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
        let call = pin!(send(PutBody::from_stream(len, watched)));
        let quiet = pin!(self.quiet(&progress, stall, answer));
        match future::select(call, quiet).await {
            Either::Left((result, _)) => result,
            Either::Right((timeout, _)) => Err(ObjectStoreError::Transport(format!(
                "the put of {key:?} made no progress for {timeout:?}"
            ))),
        }
    }

    /// One attempt at a put, see [`Self::watched`].
    async fn attempt(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
        stall: Duration,
        answer: Duration,
    ) -> Result<(), ObjectStoreError> {
        self.watched(key, body, stall, answer, |watched| {
            self.provider.put(key, watched, options)
        })
        .await
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
    /// does not change, or the requests of the rounds reach
    /// [`StoreCallPolicy::max_attempts`].
    async fn put_repeatable(
        &self,
        key: &str,
        body: &PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        let budget = self.policy.max_attempts.max(1);
        let mut delay = self.policy.retry_delay;
        let mut reached = false;
        let mut sent = 0;
        loop {
            let round = self.round(key, body, options, &mut reached, budget, &mut sent);
            let error = match round.await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            if !worth_sending_again(&error, options) || sent >= budget {
                return Err(error);
            }
            debug!(
                key,
                sent,
                retry_in_ms = delay.as_millis(),
                may_have_stored = reached,
                %error,
                "tier store put failed, sending it again"
            );
            self.counters.record_put_retry();
            self.bus.sleep(delay).await;
            delay = delay.saturating_mul(2).min(self.policy.max_retry_delay);
        }
    }

    /// One attempt, and a second one beside it once the first runs past the
    /// threshold of a create-only put, while `sent` leaves room for it in
    /// `budget`. The round ends with the first attempt that stores the
    /// object, or once every attempt failed, and adds its requests to `sent`.
    /// `reached` says whether an attempt before this one may have reached the
    /// store, and the round adds its own.
    async fn round(
        &self,
        key: &str,
        body: &PutBody,
        options: &PutOptions,
        reached: &mut bool,
        budget: u32,
        sent: &mut u32,
    ) -> Result<(), ObjectStoreError> {
        let if_absent = options.mode == PutMode::IfAbsent;
        *sent += 1;
        let threshold = if if_absent && *sent < budget {
            self.policy.second_attempts.threshold(body.len())
        } else {
            None
        };
        let raced = self
            .race(
                key,
                threshold,
                || self.attempt_again(key, body, options).boxed_local(),
                |error, other_runs| {
                    if matches!(error, ObjectStoreError::PreconditionFailed { .. })
                        && if_absent
                        && (*reached || other_runs)
                    {
                        return true;
                    }
                    *reached |= error.put_may_have_stored();
                    false
                },
            )
            .await;
        if raced.second_started {
            *sent += 1;
        }
        match raced.result {
            Ok(()) => {
                if raced.by_second {
                    self.counters.record_second_attempt_win();
                }
                Ok(())
            }
            // The object under the key decides, and the other attempt is
            // dropped already, which cancels it.
            Err(refused) if raced.decided => {
                let crc32c = match options.crc32c {
                    Some(crc32c) => Some(crc32c),
                    None => parts::crc32c_in_strides(body).await,
                };
                match self.holder(key, body.len(), options, crc32c).await {
                    Ok(Holder::Ours) => {
                        debug!(key, "tier store put found the object it stored");
                        self.counters.record_own_object_found();
                        Ok(())
                    }
                    Ok(Holder::Another | Holder::Nothing) => Err(refused),
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    /// One attempt made by `attempt`, and a second one beside it once the
    /// first runs past `threshold` while a slot of
    /// [`SecondAttempts::slots`] is free. The first attempt to succeed wins,
    /// and the other is dropped. A failure waits for the other attempt, unless
    /// `decisive` says that it ends the race: it gets each failure and whether
    /// the other attempt still runs.
    async fn race<'a, T: 'a>(
        &'a self,
        key: &str,
        threshold: Option<Duration>,
        attempt: impl Fn() -> LocalBoxFuture<'a, Result<T, ObjectStoreError>>,
        mut decisive: impl FnMut(&ObjectStoreError, bool) -> bool,
    ) -> Raced<T> {
        let mut first = Some(attempt());
        let mut second: Option<LocalBoxFuture<'a, Result<T, ObjectStoreError>>> = None;
        let mut slow: Option<LocalBoxFuture<'a, ()>> =
            threshold.map(|after| self.bus.sleep(after).boxed_local());
        let mut second_started = false;
        loop {
            let (result, by_second) = match next_event(&mut first, &mut second, &mut slow).await {
                Event::Slow => {
                    slow = None;
                    if let Some(slot) = self.second_attempts.take(self.policy.second_attempts.slots)
                    {
                        debug!(key, "tier store put is slow, sending a second attempt");
                        second_started = true;
                        self.counters.record_second_attempt();
                        let again = attempt();
                        second = Some(
                            async move {
                                let _slot = slot;
                                again.await
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
                Ok(value) => {
                    return Raced {
                        result: Ok(value),
                        by_second,
                        decided: false,
                        second_started,
                    };
                }
                Err(error) if decisive(&error, other_runs) => {
                    return Raced {
                        result: Err(error),
                        by_second,
                        decided: true,
                        second_started,
                    };
                }
                Err(error) if !other_runs => {
                    return Raced {
                        result: Err(error),
                        by_second,
                        decided: false,
                        second_started,
                    };
                }
                Err(error) => {
                    debug!(key, %error, "one attempt of a tier store put failed, the other runs");
                }
            }
        }
    }

    /// Who stored the object under `key`: the put of `len` bytes with
    /// `options`, as an earlier attempt of it, another writer, or nobody. The
    /// object is the put's own only with its put id, its size and its
    /// CRC32C, `crc32c`: the one that the store reports, or, from a store
    /// that reports none, the one of the bytes it holds. Without `crc32c`,
    /// no object is the put's own.
    async fn holder(
        &self,
        key: &str,
        len: u64,
        options: &PutOptions,
        crc32c: Option<u32>,
    ) -> Result<Holder, ObjectStoreError> {
        let (Some(id), Some(crc32c)) = (options.metadata.get(PUT_ID_METADATA), crc32c) else {
            return Ok(Holder::Another);
        };
        let held = match self.head(key).await {
            Ok(held) => held,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(Holder::Nothing),
            Err(error) => return Err(error),
        };
        if held.metadata.get(PUT_ID_METADATA) != Some(id) || held.size != len {
            return Ok(Holder::Another);
        }
        let stored = match held.crc32c {
            Some(stored) => Some(stored),
            None if len == 0 => Some(0),
            None => match self.get_range(key, 0..len).await {
                Ok(range) => parts::crc32c_in_strides(&PutBody::from_bytes(range.bytes)).await,
                Err(ObjectStoreError::NotFound { .. }) => return Ok(Holder::Nothing),
                Err(error) => return Err(error),
            },
        };
        Ok(if stored == Some(crc32c) {
            Holder::Ours
        } else {
            Holder::Another
        })
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
        let Some(tagged) = self.tagged(&body, options) else {
            return self.put_once(key, body, options).await;
        };
        if let Some(ranges) = self.parts_plan(&body, &tagged)
            && let Some(stored) = self.put_in_parts(key, &body, &tagged, &ranges).await
        {
            return stored;
        }
        self.put_repeatable(key, &body, &tagged).await
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

    async fn sweep_uploads(
        &self,
        layout: &KeyLayout,
        older_than: Duration,
        after: Option<&str>,
        request_timeout: Duration,
    ) -> Result<SweptUploads, ObjectStoreError> {
        // A page sends many requests, so a bound on the whole page would cut
        // it short at the first one that stalls.
        let request_timeout = request_timeout.min(self.policy.request_timeout);
        self.retried_unbounded("SWEEP", layout.root(), || {
            self.provider
                .sweep_uploads(layout, older_than, after, request_timeout)
        })
        .await
    }
}

/// How a race of attempts ended.
struct Raced<T> {
    result: Result<T, ObjectStoreError>,
    /// The second attempt gave the result.
    by_second: bool,
    /// The result is a failure that ended the race at once.
    decided: bool,
    /// The race sent a second attempt.
    second_started: bool,
}

/// Who stored the object under a key, as a put that the store refused finds
/// it.
enum Holder {
    /// An earlier attempt of the same put.
    Ours,
    /// Another writer, or one that the put cannot tell from another.
    Another,
    /// Nobody: the key holds no object.
    Nothing,
}

/// What ended the wait of a race.
enum Event<T> {
    First(Result<T, ObjectStoreError>),
    Second(Result<T, ObjectStoreError>),
    /// The first attempt ran past the threshold of a second one.
    Slow,
}

/// The next of the attempts that ends, or the threshold that passes.
async fn next_event<T>(
    first: &mut Option<LocalBoxFuture<'_, Result<T, ObjectStoreError>>>,
    second: &mut Option<LocalBoxFuture<'_, Result<T, ObjectStoreError>>>,
    slow: &mut Option<LocalBoxFuture<'_, ()>>,
) -> Event<T> {
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

/// Whether a whole put held in memory that failed with `error` is sent again.
/// An overwrite that may have left can land after a later overwrite of its
/// key, so only one that never left is.
fn worth_sending_again(error: &ObjectStoreError, options: &PutOptions) -> bool {
    if options.mode == PutMode::IfAbsent {
        worth_another_attempt(error, options)
    } else {
        matches!(error, ObjectStoreError::NotSent(_))
    }
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

/// The slots that calls of one kind hold while they run, up to a limit.
#[derive(Default)]
struct Slots {
    taken: Cell<usize>,
    /// How many slots were given back so far.
    released: Cell<u64>,
    /// The tasks that wait for a slot to come free.
    waiters: RefCell<Vec<Waker>>,
}

impl Slots {
    /// One of `limit` slots, given back when it drops, `None` when every
    /// slot is taken.
    fn take(&self, limit: usize) -> Option<Slot<'_>> {
        let taken = self.taken.get();
        if taken >= limit {
            return None;
        }
        self.taken.set(taken + 1);
        Some(Slot(self))
    }

    /// Resolves once a slot comes free after this call.
    fn freed(&self) -> impl Future<Output = ()> + '_ {
        let seen = self.released.get();
        poll_fn(move |context| {
            if self.released.get() != seen {
                return Poll::Ready(());
            }
            let mut waiters = self.waiters.borrow_mut();
            if !waiters
                .iter()
                .any(|waiter| waiter.will_wake(context.waker()))
            {
                waiters.push(context.waker().clone());
            }
            Poll::Pending
        })
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

/// A slot that [`Slots::take`] took, which it gives back when it drops.
struct Slot<'a>(&'a Slots);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        let slots = self.0;
        slots.taken.set(slots.taken.get().saturating_sub(1));
        slots.released.set(slots.released.get().wrapping_add(1));
        let waiters = std::mem::take(&mut *slots.waiters.borrow_mut());
        for waiter in waiters {
            waiter.wake();
        }
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
