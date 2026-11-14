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

//! How uploads, cold reads and deletes hand object store work to the tier
//! service of the shard that owns the partition.
//!
//! A job is an async closure that gets the store of the shard. The tier
//! service runs each job as a detached task on the shard, never inline in a
//! frame handler and never on the partition I/O lane, and caps how many jobs
//! of each [`TierJobClass`] run at once. Every job
//! belongs to a [`TierScope`]. When the scope is cancelled, its queued jobs
//! never start and its running jobs stop at their next await.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use futures::future::{self, Either, FutureExt, LocalBoxFuture};

use crate::store::ObjectStore;

/// What a tier job does. The tier service caps the jobs of each class that run
/// at once, and a class at its cap never starves another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TierJobClass {
    /// A fetch that a waiting poll needs. Uploads and speculative fetches
    /// never starve it.
    DemandFetch,
    /// The upload of a sealed segment.
    Upload,
    /// Readahead and chunk hydration that no poll waits for yet.
    SpeculativeFetch,
    /// Object deletes, on their own quota so that deletion keeps pace with
    /// uploads.
    Delete,
    /// Verify passes, manifests and the orphan sweep, which run when the shard
    /// is idle.
    Housekeeping,
}

impl TierJobClass {
    pub const ALL: [Self; 5] = [
        Self::DemandFetch,
        Self::Upload,
        Self::SpeculativeFetch,
        Self::Delete,
        Self::Housekeeping,
    ];
}

/// The jobs of one owner, such as a partition while this shard is its
/// primary.
///
/// The owner cancels the scope when the shard loses primacy, and drops it when
/// the incarnation retires. Either way the scope cancels every job submitted
/// under it, earlier or later: a queued job never starts, and a running job
/// is dropped at its next await.
#[derive(Debug, Default)]
pub struct TierScope {
    state: Rc<ScopeState>,
}

impl TierScope {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.state.cancel();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.get()
    }

    /// A view of the scope for jobs and executors, which can watch it but not
    /// cancel it.
    #[must_use]
    pub fn token(&self) -> TierScopeToken {
        TierScopeToken {
            state: Rc::clone(&self.state),
        }
    }
}

impl Drop for TierScope {
    fn drop(&mut self) {
        self.state.cancel();
    }
}

/// See [`TierScope::token`].
#[derive(Debug, Clone)]
pub struct TierScopeToken {
    state: Rc<ScopeState>,
}

impl TierScopeToken {
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.get()
    }

    /// Resolves once the scope is cancelled.
    pub fn cancelled(&self) -> ScopeCancelled {
        ScopeCancelled {
            state: Rc::clone(&self.state),
            waiter: None,
        }
    }

    /// Runs `job` until it ends, or until the scope is cancelled.
    ///
    /// Cancellation is polled first, so `job` is never polled again once the
    /// scope is cancelled before a poll.
    ///
    /// # Errors
    /// [`TierJobCancelled`] when the scope is cancelled before `job` ends.
    pub async fn run_until_cancelled<T>(
        &self,
        job: impl Future<Output = T>,
    ) -> Result<T, TierJobCancelled> {
        let cancelled = pin!(self.cancelled());
        let job = pin!(job);
        match future::select(cancelled, job).await {
            Either::Left(((), _)) => Err(TierJobCancelled),
            Either::Right((output, _)) => Ok(output),
        }
    }
}

#[derive(Debug, Default)]
struct ScopeState {
    cancelled: Cell<bool>,
    next_waiter: Cell<u64>,
    waiters: RefCell<BTreeMap<u64, Waker>>,
}

impl ScopeState {
    fn cancel(&self) {
        if self.cancelled.replace(true) {
            return;
        }
        // Taken out first, so a waker that polls at once finds no borrow.
        let waiters = mem::take(&mut *self.waiters.borrow_mut());
        for waker in waiters.into_values() {
            waker.wake();
        }
    }
}

/// See [`TierScopeToken::cancelled`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct ScopeCancelled {
    state: Rc<ScopeState>,
    waiter: Option<u64>,
}

impl Future for ScopeCancelled {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.state.cancelled.get() {
            self.forget_waker();
            return Poll::Ready(());
        }
        let this = &mut *self;
        let state = &this.state;
        let waiter = *this.waiter.get_or_insert_with(|| {
            let waiter = state.next_waiter.get();
            state.next_waiter.set(waiter + 1);
            waiter
        });
        state
            .waiters
            .borrow_mut()
            .entry(waiter)
            .and_modify(|waker| waker.clone_from(context.waker()))
            .or_insert_with(|| context.waker().clone());
        Poll::Pending
    }
}

impl ScopeCancelled {
    fn forget_waker(&mut self) {
        if let Some(waiter) = self.waiter.take() {
            self.state.waiters.borrow_mut().remove(&waiter);
        }
    }
}

impl Drop for ScopeCancelled {
    fn drop(&mut self) {
        self.forget_waker();
    }
}

/// What the tier service needs to know about a job before it runs it.
#[derive(Debug, Clone)]
pub struct TierJobRequest {
    class: TierJobClass,
    scope: TierScopeToken,
    bytes: u64,
}

impl TierJobRequest {
    #[must_use]
    pub const fn new(class: TierJobClass, scope: TierScopeToken) -> Self {
        Self {
            class,
            scope,
            bytes: 0,
        }
    }

    /// The same job, moving about `bytes` bytes. A class can cap the bytes of
    /// its running jobs as well as their number, and the bytes count a
    /// different thing in each class:
    ///
    /// - A fetch, on demand or speculative, holds its whole range in memory.
    ///   Its bytes are the length of the range, and the cap bounds memory.
    /// - An upload streams its segment and holds only a few pieces at once.
    ///   Its bytes are the length of the segment, and the cap bounds the
    ///   bytes in flight to the store.
    /// - Deletes and housekeeping count only jobs, and keep 0 bytes.
    ///
    /// A job with more bytes than the cap of its class runs alone, once no
    /// other job of its class runs, so a segment above the upload cap still
    /// uploads.
    #[must_use]
    pub const fn with_bytes(mut self, bytes: u64) -> Self {
        self.bytes = bytes;
        self
    }

    #[must_use]
    pub const fn class(&self) -> TierJobClass {
        self.class
    }

    #[must_use]
    pub const fn scope(&self) -> &TierScopeToken {
        &self.scope
    }

    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// Runs object store work for one shard: the tier service in the server, and
/// [`crate::InlineTierExecutor`] in tests.
pub trait TierExecutor {
    /// The store that jobs get. The tier service can wrap its provider here,
    /// for example to watch the progress of an upload.
    type Store: ObjectStore + 'static;

    /// Submits `job` under the class and scope of `request`.
    ///
    /// The job runs once its class admits it, unless its scope is cancelled
    /// first. The handle resolves to the job's output, or to
    /// [`TierJobCancelled`] when the scope is cancelled before the job
    /// finishes. Dropping the handle does not cancel the job, so a fetch
    /// that a poll stopped waiting for still warms the cache.
    ///
    /// The tests of the tier service in `shard::tier_service` cover these
    /// promises with its queue and its caps. [`crate::InlineTierExecutor`]
    /// has neither, and its own tests cover only what it models.
    #[must_use = "await the handle, or detach it to let the job run on"]
    fn submit<T, F, Fut>(&self, request: TierJobRequest, job: F) -> TierJobHandle<T>
    where
        T: 'static,
        F: FnOnce(Rc<Self::Store>) -> Fut + 'static,
        Fut: Future<Output = T> + 'static;
}

/// The result of a submitted job, see [`TierExecutor::submit`].
pub struct TierJobHandle<T> {
    result: LocalBoxFuture<'static, Result<T, TierJobCancelled>>,
}

impl<T: 'static> TierJobHandle<T> {
    /// A handle that resolves to what `result` resolves to. The tier service
    /// builds one over the channel that its detached task answers on.
    pub fn new(result: impl Future<Output = Result<T, TierJobCancelled>> + 'static) -> Self {
        Self {
            result: result.boxed_local(),
        }
    }

    /// The handle of a job that already finished.
    pub fn ready(result: Result<T, TierJobCancelled>) -> Self {
        Self::new(future::ready(result))
    }
}

impl<T> TierJobHandle<T> {
    /// Lets the job run on without anyone waiting for its result, as a
    /// speculative fetch does.
    pub fn detach(self) {}
}

impl<T> Future for TierJobHandle<T> {
    type Output = Result<T, TierJobCancelled>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.result.as_mut().poll(context)
    }
}

impl<T> fmt::Debug for TierJobHandle<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TierJobHandle")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the scope of the tier job was cancelled before the job finished")]
pub struct TierJobCancelled;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use super::*;

    #[derive(Default)]
    struct CountingWaker(AtomicUsize);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn poll_once(future: &mut ScopeCancelled, waker: &Waker) -> Poll<()> {
        Pin::new(future).poll(&mut Context::from_waker(waker))
    }

    #[test]
    fn given_waiting_futures_when_cancelling_should_wake_each_once() {
        let scope = TierScope::new();
        let token = scope.token();
        let counter = Arc::new(CountingWaker::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut first = token.cancelled();
        let mut second = token.cancelled();

        assert!(poll_once(&mut first, &waker).is_pending());
        assert!(poll_once(&mut first, &waker).is_pending());
        assert!(poll_once(&mut second, &waker).is_pending());
        scope.cancel();
        scope.cancel();

        assert_eq!(counter.0.load(Ordering::Relaxed), 2);
        assert!(poll_once(&mut first, &waker).is_ready());
        assert!(poll_once(&mut second, &waker).is_ready());
        assert!(token.is_cancelled());
    }

    #[test]
    fn given_dropped_scope_when_watching_token_should_see_it_cancelled() {
        let scope = TierScope::new();
        let token = scope.token();
        let mut waiting = token.cancelled();
        assert!(poll_once(&mut waiting, Waker::noop()).is_pending());

        drop(scope);

        assert!(token.is_cancelled());
        assert!(poll_once(&mut waiting, Waker::noop()).is_ready());
        assert!(poll_once(&mut token.cancelled(), Waker::noop()).is_ready());
    }

    #[test]
    fn given_dropped_waiter_when_scope_lives_on_should_forget_its_waker() {
        let scope = TierScope::new();
        let token = scope.token();

        for _ in 0..3 {
            let mut waiting = token.cancelled();
            assert!(poll_once(&mut waiting, Waker::noop()).is_pending());
        }

        assert!(scope.state.waiters.borrow().is_empty());
        assert!(!scope.is_cancelled());
    }

    #[test]
    fn given_request_when_building_should_carry_class_scope_and_bytes() {
        let scope = TierScope::new();

        let request = TierJobRequest::new(TierJobClass::Upload, scope.token()).with_bytes(4096);

        assert_eq!(request.class(), TierJobClass::Upload);
        assert_eq!(request.bytes(), 4096);
        assert!(!request.scope().is_cancelled());
        scope.cancel();
        assert!(request.scope().is_cancelled());
    }
}
