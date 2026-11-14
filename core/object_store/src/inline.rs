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

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker, ready};

use futures::future::{FutureExt, LocalBoxFuture};

use crate::store::ObjectStore;
use crate::tier::{TierExecutor, TierJobCancelled, TierJobHandle, TierJobRequest};

/// The jobs that went on waiting after their handles were dropped.
type DetachedJobs = Rc<RefCell<Vec<LocalBoxFuture<'static, ()>>>>;

/// A [`TierExecutor`] for tests. It runs each job inside `submit`, with no
/// class cap and no queue.
///
/// A job runs inside `submit` up to its first await that does not complete
/// at once, so a job against a [`crate::MemoryStore`] finishes there. The rest
/// of a job that waits runs while its handle is polled. Once the handle is
/// dropped or detached, the executor keeps the job, and the test runs it on
/// with [`Self::run_detached`].
///
/// A cancelled scope stops a job only when the job is next polled. A detached
/// job of a cancelled scope is dropped at the next [`Self::run_detached`], and
/// holds the store and its captures until then. A job whose scope is
/// cancelled while the job itself is polled runs on to its next await that
/// does not complete at once, and can still finish with its output.
pub struct InlineTierExecutor<S> {
    store: Rc<S>,
    detached: DetachedJobs,
}

impl<S> InlineTierExecutor<S> {
    #[must_use]
    pub fn new(store: S) -> Self {
        Self {
            store: Rc::new(store),
            detached: DetachedJobs::default(),
        }
    }

    /// The store that jobs reach, for a test to inspect.
    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Polls once each job whose handle was dropped or detached while the job
    /// waited, and drops the jobs that finish. Returns how many still wait.
    #[must_use]
    pub fn run_detached(&self) -> usize {
        let mut jobs = mem::take(&mut *self.detached.borrow_mut());
        let mut context = Context::from_waker(Waker::noop());
        jobs.retain_mut(|job| job.as_mut().poll(&mut context).is_pending());
        let mut detached = self.detached.borrow_mut();
        // A job that dropped another handle while it ran added that job.
        jobs.append(&mut detached);
        *detached = jobs;
        detached.len()
    }
}

impl<S: fmt::Debug> fmt::Debug for InlineTierExecutor<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InlineTierExecutor")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl<S: ObjectStore + 'static> TierExecutor for InlineTierExecutor<S> {
    type Store = S;

    fn submit<T, F, Fut>(&self, request: TierJobRequest, job: F) -> TierJobHandle<T>
    where
        T: 'static,
        F: FnOnce(Rc<S>) -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
    {
        let scope = request.scope().clone();
        if scope.is_cancelled() {
            return TierJobHandle::ready(Err(TierJobCancelled));
        }
        let store = Rc::clone(&self.store);
        let mut run = async move { scope.run_until_cancelled(job(store)).await }.boxed_local();
        match run.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => TierJobHandle::ready(result),
            Poll::Pending => TierJobHandle::new(WaitingJob {
                run: Some(run),
                detached: Rc::clone(&self.detached),
            }),
        }
    }
}

/// A job that went on waiting after `submit`. Its handle runs it, and hands it
/// to the executor if the handle goes first.
struct WaitingJob<T: 'static> {
    run: Option<LocalBoxFuture<'static, Result<T, TierJobCancelled>>>,
    detached: DetachedJobs,
}

impl<T: 'static> Future for WaitingJob<T> {
    type Output = Result<T, TierJobCancelled>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let run = self
            .run
            .as_mut()
            .expect("a tier job handle is polled after it resolved");
        let result = ready!(run.as_mut().poll(context));
        self.run = None;
        Poll::Ready(result)
    }
}

impl<T: 'static> Drop for WaitingJob<T> {
    fn drop(&mut self) {
        if let Some(run) = self.run.take() {
            self.detached
                .borrow_mut()
                .push(run.map(|_output| ()).boxed_local());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use bytes::Bytes;
    use futures::channel::oneshot;
    use futures::executor::block_on;

    use super::*;
    use crate::memory::MemoryStore;
    use crate::store::{PutBody, PutOptions};
    use crate::tier::{TierJobClass, TierScope};

    struct DropFlag(Rc<Cell<bool>>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    fn request(scope: &TierScope) -> TierJobRequest {
        TierJobRequest::new(TierJobClass::Upload, scope.token())
    }

    async fn put_marker(store: &MemoryStore) {
        store
            .put(
                "a/b",
                PutBody::from_bytes(Bytes::from_static(b"x")),
                &PutOptions::default(),
            )
            .await
            .unwrap();
    }

    #[test]
    fn given_store_job_when_submitting_should_finish_inside_submit() {
        let executor = InlineTierExecutor::new(MemoryStore::new());
        let scope = TierScope::new();

        let handle = executor.submit(request(&scope), |store| async move {
            put_marker(&store).await;
        });

        assert_eq!(executor.store().keys(), ["a/b"]);
        assert_eq!(block_on(handle), Ok(()));
    }

    #[test]
    fn given_detached_handle_of_finished_job_when_reading_store_should_keep_its_effects() {
        let executor = InlineTierExecutor::new(MemoryStore::new());
        let scope = TierScope::new();

        executor
            .submit(request(&scope), |store| async move {
                put_marker(&store).await;
            })
            .detach();

        assert_eq!(executor.store().keys(), ["a/b"]);
        assert_eq!(executor.run_detached(), 0);
    }

    #[test]
    fn given_cancelled_scope_when_submitting_should_never_start_job() {
        let executor = InlineTierExecutor::new(MemoryStore::new());
        let scope = TierScope::new();
        scope.cancel();
        let started = Rc::new(Cell::new(false));
        let flag = Rc::clone(&started);

        let handle = executor.submit(request(&scope), move |_| async move {
            flag.set(true);
        });

        assert_eq!(block_on(handle), Err(TierJobCancelled));
        assert!(!started.get());
    }

    #[test]
    fn given_waiting_job_when_scope_is_cancelled_should_drop_job_and_resolve_cancelled() {
        let executor = InlineTierExecutor::new(MemoryStore::new());
        let scope = TierScope::new();
        let (_sender, receiver) = oneshot::channel::<()>();
        let dropped = Rc::new(Cell::new(false));
        let flag = DropFlag(Rc::clone(&dropped));

        let handle = executor.submit(request(&scope), move |_| async move {
            let _flag = flag;
            receiver.await
        });
        assert!(!dropped.get());
        scope.cancel();

        assert_eq!(block_on(handle), Err(TierJobCancelled));
        assert!(dropped.get());
    }

    #[test]
    fn given_waiting_job_when_it_is_released_should_resolve_its_output() {
        let executor = InlineTierExecutor::new(MemoryStore::new());
        let scope = TierScope::new();
        let (sender, receiver) = oneshot::channel::<u32>();

        let handle = executor.submit(request(&scope), move |_| receiver);
        sender.send(7).unwrap();

        assert_eq!(block_on(handle), Ok(Ok(7)));
    }

    #[test]
    fn given_detached_waiting_job_when_released_should_finish_in_run_detached() {
        let executor = InlineTierExecutor::new(MemoryStore::new());
        let scope = TierScope::new();
        let (sender, receiver) = oneshot::channel::<()>();

        executor
            .submit(request(&scope), move |store| async move {
                receiver.await.unwrap();
                put_marker(&store).await;
            })
            .detach();
        assert_eq!(executor.run_detached(), 1);
        assert!(executor.store().is_empty());
        sender.send(()).unwrap();

        assert_eq!(executor.run_detached(), 0);
        assert_eq!(executor.store().keys(), ["a/b"]);
    }

    #[test]
    fn given_dropped_handle_of_waiting_job_when_scope_is_cancelled_should_drop_job() {
        let executor = InlineTierExecutor::new(MemoryStore::new());
        let scope = TierScope::new();
        let (_sender, receiver) = oneshot::channel::<()>();
        let dropped = Rc::new(Cell::new(false));
        let flag = DropFlag(Rc::clone(&dropped));

        let handle = executor.submit(request(&scope), move |_| async move {
            let _flag = flag;
            receiver.await
        });
        drop(handle);
        assert!(!dropped.get());
        scope.cancel();

        assert_eq!(executor.run_detached(), 0);
        assert!(dropped.get());
    }
}
