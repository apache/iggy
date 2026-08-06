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

// NOT behind `cfg(debug_assertions)`: release is exactly the build where this
// gate is the only enforcement of the superblock single-writer contract (the
// `WritingGuard` tripwire is debug-only, `write` takes `&self`, and two
// overlapping writers collide on one fixed `.tmp` path and tear a slot while
// both return `Ok`).
#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use futures::future::join;
    use std::rc::Rc;

    /// Two writers queued on the same gate run one after the other, never
    /// interleaved -- the property a torn superblock slot depends on.
    #[compio::test]
    async fn given_two_waiters_when_acquiring_should_serialize() {
        let gate = LocalGate::new();
        let gate = &gate;
        let log: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));

        let first = {
            let log = Rc::clone(&log);
            async move {
                let guard = gate.acquire().await;
                log.borrow_mut().push("first:enter");
                // A yield inside the critical section: without exclusion the
                // second writer would slot in right here.
                compio::time::sleep(std::time::Duration::from_millis(1)).await;
                log.borrow_mut().push("first:exit");
                drop(guard);
            }
        };
        let second = {
            let log = Rc::clone(&log);
            async move {
                let guard = gate.acquire().await;
                log.borrow_mut().push("second:enter");
                compio::time::sleep(std::time::Duration::from_millis(1)).await;
                log.borrow_mut().push("second:exit");
                drop(guard);
            }
        };
        join(first, second).await;

        let log = log.borrow();
        assert_eq!(log.len(), 4, "both sections ran: {log:?}");
        let first_exit = log.iter().position(|entry| *entry == "first:exit").unwrap();
        let second_enter = log
            .iter()
            .position(|entry| *entry == "second:enter")
            .unwrap();
        assert!(
            first_exit < second_enter,
            "sections must not interleave: {log:?}"
        );
    }

    /// Dropping the guard wakes a queued waiter, so the gate does not wedge
    /// once contended.
    #[compio::test]
    async fn given_queued_waiter_when_guard_drops_should_wake_it() {
        let gate = LocalGate::new();
        let held = gate.acquire().await;

        let mut waiter = Box::pin(gate.acquire());
        assert!(
            waiter.as_mut().now_or_never().is_none(),
            "the gate is held, so the waiter must park"
        );

        drop(held);
        assert!(
            waiter.now_or_never().is_some(),
            "dropping the guard must wake the queued waiter"
        );
    }

    /// A waiter that goes away leaves only a stale waker behind: the next
    /// acquirer still gets the gate. Cancel safety is load-bearing here, since
    /// every acquire site sits in a future a shard can drop.
    #[compio::test]
    async fn given_dropped_waiter_when_guard_releases_should_not_wedge() {
        let gate = LocalGate::new();
        let held = gate.acquire().await;

        let mut abandoned = Box::pin(gate.acquire());
        assert!(abandoned.as_mut().now_or_never().is_none());
        drop(abandoned);

        drop(held);
        assert!(
            gate.acquire().now_or_never().is_some(),
            "a dropped waiter must not keep the gate busy"
        );
    }
}
