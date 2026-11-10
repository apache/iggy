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

use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use futures::future::{self, Either, FutureExt};

use crate::store::ObjectStore;
use crate::tier::{TierExecutor, TierJobCancelled, TierJobContext, TierJobHandle, TierJobRequest};

/// A [`TierExecutor`] for tests. It runs each job inside `submit`, with no
/// detached task, no class cap and no queue.
///
/// A job runs inside `submit` up to its first await that does not complete
/// at once, so a job against a [`crate::MemoryStore`] finishes there. The rest
/// of a job that waits runs while its handle is polled. Unlike in the tier
/// service, dropping or detaching that handle drops the job. Cancellation
/// works as in the tier service.
#[derive(Debug)]
pub struct InlineTierExecutor<S> {
    store: Rc<S>,
}

impl<S> InlineTierExecutor<S> {
    #[must_use]
    pub fn new(store: S) -> Self {
        Self {
            store: Rc::new(store),
        }
    }

    /// The store that jobs reach, for a test to inspect.
    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }
}

impl<S: ObjectStore + 'static> TierExecutor for InlineTierExecutor<S> {
    type Store = S;

    fn submit<T, F, Fut>(&self, request: TierJobRequest, job: F) -> TierJobHandle<T>
    where
        T: 'static,
        F: FnOnce(TierJobContext<S>) -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
    {
        let scope = request.scope().clone();
        if scope.is_cancelled() {
            return TierJobHandle::ready(Err(TierJobCancelled));
        }
        let context = TierJobContext::new(Rc::clone(&self.store));
        let mut run = async move {
            let cancelled = pin!(scope.cancelled());
            let job = pin!(job(context));
            // Cancellation is polled first, so a job never outlives its scope.
            match future::select(cancelled, job).await {
                Either::Left(((), _)) => Err(TierJobCancelled),
                Either::Right((output, _)) => Ok(output),
            }
        }
        .boxed_local();
        match run.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => TierJobHandle::ready(result),
            Poll::Pending => TierJobHandle::from_boxed(run),
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

    fn request(scope: &TierScope) -> TierJobRequest {
        TierJobRequest::new(TierJobClass::Upload, scope.token())
    }

    #[test]
    fn given_store_job_when_submitting_should_finish_inside_submit() {
        let executor = InlineTierExecutor::new(MemoryStore::new());
        let scope = TierScope::new();

        let handle = executor.submit(request(&scope), |context| async move {
            context
                .store()
                .put(
                    "a/b",
                    PutBody::from_bytes(Bytes::from_static(b"x")),
                    &PutOptions::default(),
                )
                .await
        });

        assert_eq!(executor.store().keys(), ["a/b"]);
        assert!(block_on(handle).unwrap().is_ok());
    }

    #[test]
    fn given_detached_handle_of_finished_job_when_reading_store_should_keep_its_effects() {
        let executor = InlineTierExecutor::new(MemoryStore::new());
        let scope = TierScope::new();

        executor
            .submit(request(&scope), |context| async move {
                context
                    .store()
                    .put(
                        "a/b",
                        PutBody::from_bytes(Bytes::from_static(b"x")),
                        &PutOptions::default(),
                    )
                    .await
            })
            .detach();

        assert_eq!(executor.store().keys(), ["a/b"]);
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
        struct DropFlag(Rc<Cell<bool>>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

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
}
