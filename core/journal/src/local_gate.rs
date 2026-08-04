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

//! Single-shard async gate for critical sections that hold across an `.await`.
//!
//! Many futures can drive the same shard-local resource concurrently (a pump
//! loop, detached per-client tasks, repair drivers). This gate serializes them
//! without atomics: the shard is never `Sync`, so a `Cell` flag provides the
//! exclusion a `tokio::sync::Mutex` would buy with an atomic RMW per acquire.
//!
//! Not a general lock: single-threaded (`Cell`/`RefCell`, never `Sync`),
//! release wakes every waiter and poll order re-races (arrival-order FIFO
//! under `futures::join!`-style drivers), cancel-safe (dropping the guard
//! releases; dropping a waiter leaves only a stale waker). Non-reentrant: a
//! holder that re-acquires deadlocks itself.

use std::cell::{Cell, RefCell};

/// See the module docs. Callers hold the returned guard across the awaited
/// critical section; dropping it releases the gate and wakes every waiter.
pub struct LocalGate {
    busy: Cell<bool>,
    waiters: RefCell<Vec<std::task::Waker>>,
}

impl LocalGate {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            busy: Cell::new(false),
            waiters: RefCell::new(Vec::new()),
        }
    }

    // A manual `Future` impl does not inherit the async-fn unused lint, so
    // without these `gate.acquire();` and `gate.acquire().await;` both
    // compile clean as no-op exclusion (the guard drops at the semicolon).
    #[must_use = "acquire does nothing until awaited"]
    pub const fn acquire(&self) -> LocalGateAcquire<'_> {
        LocalGateAcquire { gate: self }
    }
}

impl Default for LocalGate {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use = "the acquire future must be awaited to take the gate"]
pub struct LocalGateAcquire<'a> {
    gate: &'a LocalGate,
}

impl<'a> std::future::Future for LocalGateAcquire<'a> {
    type Output = LocalGateGuard<'a>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.gate.busy.get() {
            // Re-polls while still busy push a duplicate waker; the extra
            // wake is spurious and harmless at pipeline-queue scale.
            self.gate.waiters.borrow_mut().push(cx.waker().clone());
            std::task::Poll::Pending
        } else {
            self.gate.busy.set(true);
            std::task::Poll::Ready(LocalGateGuard { gate: self.gate })
        }
    }
}

#[must_use = "dropping the guard immediately releases the gate"]
pub struct LocalGateGuard<'a> {
    gate: &'a LocalGate,
}

impl Drop for LocalGateGuard<'_> {
    fn drop(&mut self) {
        self.gate.busy.set(false);
        // Move the waiters out before waking: `wake()` only schedules under
        // compio today, but a waker that ever polled a waiter inline would
        // re-enter `acquire`'s `waiters.borrow_mut()` and panic the RefCell.
        let waiters = std::mem::take(&mut *self.gate.waiters.borrow_mut());
        for waker in waiters {
            waker.wake();
        }
    }
}
