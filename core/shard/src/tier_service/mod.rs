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

//! The tier service of a shard: the [`TierExecutor`] that runs the object
//! store jobs of the shard's partitions.
//!
//! Every job runs as a detached task on the shard, the way an off-pump disk
//! poll does. It never runs inline in a frame handler and never on the
//! partition I/O lane, so the pump keeps running between the job's awaits.
//!
//! Each [`TierJobClass`] has its own caps, so a class at its cap never holds
//! back another class, and uploads never take the slots of demand fetches.
//! Within a class, jobs start in the order they came, and no job starts
//! ahead of the one in front of it, so a large job is never passed over for
//! good. The optional work yields across classes: a speculative fetch does
//! not start while a demand fetch waits, and housekeeping does not start
//! while a fetch or an upload waits. Uploads never wait for another class,
//! because a held upload keeps segments on a disk that is filling. Deletes
//! have their own quota and never wait either, so deletion keeps pace with
//! uploads.
//!
//! Jobs reach the store through [`TierStore`], which retries the calls that
//! are safe to repeat and fails a put that stops making progress.

#![allow(
    clippy::future_not_send,
    reason = "every job and store call runs on the shard that owns it and never crosses threads"
)]

mod store;
#[cfg(test)]
pub(crate) mod test_store;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;

use futures::channel::oneshot;
use futures::future::{self, Either, FutureExt, LocalBoxFuture};
use iggy_object_store::{
    ObjectStore, TierExecutor, TierJobCancelled, TierJobClass, TierJobHandle, TierJobRequest,
    TierScopeToken,
};
use message_bus::MessageBus;

pub use store::{StoreCallPolicy, TierStore};

const MIB: u64 = 1024 * 1024;

/// How many jobs of one class run at once, and how many bytes they hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassLimits {
    /// The most jobs of the class that run at once. A value below 1 counts
    /// as 1, because a class without a slot would keep its jobs forever.
    pub jobs: usize,
    /// The most bytes that the running jobs of the class hold, as
    /// [`TierJobRequest::with_bytes`] counts them. `None` counts only jobs.
    pub bytes: Option<u64>,
}

/// The caps of a tier service and how its store treats calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierServiceConfig {
    pub demand_fetch: ClassLimits,
    pub upload: ClassLimits,
    pub speculative_fetch: ClassLimits,
    pub delete: ClassLimits,
    pub housekeeping: ClassLimits,
    pub calls: StoreCallPolicy,
}

impl TierServiceConfig {
    #[must_use]
    pub const fn limits(&self, class: TierJobClass) -> ClassLimits {
        match class {
            TierJobClass::DemandFetch => self.demand_fetch,
            TierJobClass::Upload => self.upload,
            TierJobClass::SpeculativeFetch => self.speculative_fetch,
            TierJobClass::Delete => self.delete,
            TierJobClass::Housekeeping => self.housekeeping,
        }
    }
}

/// Provisional values. Measurements of uploads, cold reads and deletes on one
/// loaded shard settle them.
impl Default for TierServiceConfig {
    fn default() -> Self {
        Self {
            demand_fetch: ClassLimits {
                jobs: 16,
                bytes: Some(64 * MIB),
            },
            upload: ClassLimits {
                jobs: 2,
                bytes: Some(2048 * MIB),
            },
            speculative_fetch: ClassLimits {
                jobs: 4,
                bytes: Some(32 * MIB),
            },
            delete: ClassLimits {
                jobs: 2,
                bytes: None,
            },
            housekeeping: ClassLimits {
                jobs: 1,
                bytes: None,
            },
            calls: StoreCallPolicy::default(),
        }
    }
}

/// What runs and what waits in one class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClassLoad {
    pub running: usize,
    pub running_bytes: u64,
    pub queued: usize,
}

/// The tier service of one shard. A clone is another handle to the same
/// service.
pub struct TierService<S, B> {
    inner: Rc<Inner<S, B>>,
}

impl<S, B> Clone for TierService<S, B> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<S, B> TierService<S, B>
where
    S: ObjectStore + 'static,
    B: MessageBus + Clone + 'static,
{
    /// A service whose jobs reach `provider` and run as tasks that `bus`
    /// spawns, with the bus's clock for timeouts and backoff.
    #[must_use]
    pub fn new(provider: S, bus: B, config: &TierServiceConfig) -> Self {
        let store = Rc::new(TierStore::new(provider, bus.clone(), config.calls));
        let classes = TierJobClass::ALL.map(|class| ClassState::new(config.limits(class)));
        Self {
            inner: Rc::new(Inner {
                bus,
                store,
                classes: RefCell::new(classes),
            }),
        }
    }

    /// The store that the service's jobs get.
    #[must_use]
    pub fn store(&self) -> &TierStore<S, B> {
        &self.inner.store
    }

    #[must_use]
    pub fn load(&self, class: TierJobClass) -> ClassLoad {
        let classes = self.inner.classes.borrow();
        let state = &classes[slot(class)];
        ClassLoad {
            running: state.running,
            running_bytes: state.running_bytes,
            queued: state.queue.len(),
        }
    }
}

impl<S, B> TierExecutor for TierService<S, B>
where
    S: ObjectStore + 'static,
    B: MessageBus + Clone + Unpin + 'static,
{
    type Store = TierStore<S, B>;

    fn submit<T, F, Fut>(&self, request: TierJobRequest, job: F) -> TierJobHandle<T>
    where
        T: 'static,
        F: FnOnce(Rc<Self::Store>) -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
    {
        let scope = request.scope().clone();
        if scope.is_cancelled() {
            return TierJobHandle::ready(Err(TierJobCancelled));
        }
        let (answer, answered) = oneshot::channel();
        let job_scope = scope.clone();
        let store = Rc::clone(&self.inner.store);
        // An async block runs nothing before its first poll, so `job` starts
        // only once its class admits it, and not at all if its scope is
        // cancelled before that poll.
        let run = async move {
            let result = if job_scope.is_cancelled() {
                Err(TierJobCancelled)
            } else {
                job_scope.run_until_cancelled(job(store)).await
            };
            // Nobody waits once the handle is gone.
            let _ = answer.send(result);
        }
        .boxed_local();
        self.inner.enqueue(
            request.class(),
            QueuedJob {
                bytes: request.bytes(),
                scope: scope.clone(),
                run,
            },
        );
        TierJobHandle::new(async move {
            let answered = pin!(answered);
            let cancelled = pin!(scope.cancelled());
            // The answer is polled first, so a job that finished before its
            // scope was cancelled still hands over its output. A queued job
            // of a cancelled scope never starts, and its handle need not wait
            // for the queue to drop it.
            match future::select(answered, cancelled).await {
                Either::Left((answer, _)) => answer.unwrap_or(Err(TierJobCancelled)),
                Either::Right(((), _)) => Err(TierJobCancelled),
            }
        })
    }
}

/// A job that waits for its class to start it.
struct QueuedJob {
    bytes: u64,
    scope: TierScopeToken,
    run: LocalBoxFuture<'static, ()>,
}

struct ClassState {
    limits: ClassLimits,
    running: usize,
    running_bytes: u64,
    queue: VecDeque<QueuedJob>,
}

impl ClassState {
    fn new(limits: ClassLimits) -> Self {
        Self {
            limits: ClassLimits {
                jobs: limits.jobs.max(1),
                bytes: limits.bytes,
            },
            running: 0,
            running_bytes: 0,
            queue: VecDeque::new(),
        }
    }

    /// Whether a job of `bytes` can start beside the jobs that run. A job
    /// above the byte cap runs alone, once no other job of its class runs, so
    /// it still runs.
    fn fits(&self, bytes: u64) -> bool {
        self.running < self.limits.jobs
            && self.limits.bytes.is_none_or(|cap| {
                self.running == 0 || self.running_bytes.saturating_add(bytes) <= cap
            })
    }

    fn waits(&self) -> bool {
        !self.queue.is_empty()
    }
}

struct Inner<S, B> {
    bus: B,
    store: Rc<TierStore<S, B>>,
    /// Indexed by [`slot`].
    classes: RefCell<[ClassState; 5]>,
}

impl<S, B> Inner<S, B> {
    fn release(&self, class: TierJobClass, bytes: u64) {
        let mut classes = self.classes.borrow_mut();
        let state = &mut classes[slot(class)];
        state.running = state.running.saturating_sub(1);
        state.running_bytes = state.running_bytes.saturating_sub(bytes);
    }
}

impl<S, B> Inner<S, B>
where
    S: ObjectStore + 'static,
    B: MessageBus + Clone + 'static,
{
    fn enqueue(self: &Rc<Self>, class: TierJobClass, job: QueuedJob) {
        self.classes.borrow_mut()[slot(class)].queue.push_back(job);
        self.admit();
    }

    /// Start every queued job that its class and the waiting classes allow,
    /// after dropping the queued jobs of cancelled scopes.
    fn admit(self: &Rc<Self>) {
        let mut cancelled = Vec::new();
        let mut started = Vec::new();
        {
            let mut classes = self.classes.borrow_mut();
            for state in classes.iter_mut() {
                if !state.queue.iter().any(|job| job.scope.is_cancelled()) {
                    continue;
                }
                let (live, dead): (VecDeque<_>, VecDeque<_>) = state
                    .queue
                    .drain(..)
                    .partition(|job| !job.scope.is_cancelled());
                state.queue = live;
                cancelled.extend(dead);
            }
            for class in TierJobClass::ALL {
                while may_start(&classes, class) {
                    let state = &mut classes[slot(class)];
                    let Some(bytes) = state.queue.front().map(|job| job.bytes) else {
                        break;
                    };
                    if !state.fits(bytes) {
                        break;
                    }
                    let Some(job) = state.queue.pop_front() else {
                        break;
                    };
                    state.running += 1;
                    state.running_bytes = state.running_bytes.saturating_add(bytes);
                    started.push((class, job));
                }
            }
        }
        // Outside the borrow: what a job captured can run code as it drops.
        drop(cancelled);
        for (class, job) in started {
            self.spawn(class, job);
        }
    }

    fn spawn(self: &Rc<Self>, class: TierJobClass, job: QueuedJob) {
        let QueuedJob { bytes, run, .. } = job;
        // Made before the task, so a task that its runtime drops before its
        // first poll still gives the slot back.
        let held = HeldSlot {
            service: Rc::clone(self),
            class,
            bytes,
        };
        self.bus.spawn(async move {
            run.await;
            let service = Rc::clone(&held.service);
            drop(held);
            // Here and not in `HeldSlot::drop`: a task that its runtime drops
            // must not start other tasks.
            service.admit();
        });
    }
}

/// The slot of a running job in its class, given back when the job ends or
/// when its task is dropped.
struct HeldSlot<S, B> {
    service: Rc<Inner<S, B>>,
    class: TierJobClass,
    bytes: u64,
}

impl<S, B> Drop for HeldSlot<S, B> {
    fn drop(&mut self) {
        self.service.release(self.class, self.bytes);
    }
}

/// Whether the queue of `class` may start a job while the other classes wait
/// as they do.
fn may_start(classes: &[ClassState; 5], class: TierJobClass) -> bool {
    let waits = |other| classes[slot(other)].waits();
    match class {
        TierJobClass::DemandFetch | TierJobClass::Upload | TierJobClass::Delete => true,
        TierJobClass::SpeculativeFetch => !waits(TierJobClass::DemandFetch),
        TierJobClass::Housekeeping => {
            !waits(TierJobClass::DemandFetch)
                && !waits(TierJobClass::Upload)
                && !waits(TierJobClass::SpeculativeFetch)
        }
    }
}

const fn slot(class: TierJobClass) -> usize {
    match class {
        TierJobClass::DemandFetch => 0,
        TierJobClass::Upload => 1,
        TierJobClass::SpeculativeFetch => 2,
        TierJobClass::Delete => 3,
        TierJobClass::Housekeeping => 4,
    }
}
