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
//! without atomics: the shard is never `Sync`, so a `Cell` state provides the
//! exclusion a `tokio::sync::Mutex` would buy with an atomic RMW per acquire.
//!
//! Not a general lock: single-threaded (`Cell`/`RefCell`, never `Sync`).
//! Waiters take the gate in arrival order: a release hands it to the oldest
//! waiter without freeing it, so a later acquirer never takes it first, in
//! whatever order the executor polls them. Dropping a borrowed guard releases.
//! Dropping a waiter gives up its place, or passes the gate on when it was
//! already handed the gate. An owned lease requires explicit release after
//! physical completion: dropping it keeps the resource fenced.
//! Non-reentrant: a holder that re-acquires deadlocks itself.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::task::Waker;

/// See the module docs. Borrowed guards release on drop; owned leases release
/// explicitly after physical completion and otherwise keep the gate closed.
///
/// Its exclusion is load-bearing in RELEASE, not only under
/// `debug_assertions`: this gate is the only enforcement of the superblock
/// single-writer contract there. The `WritingGuard` tripwire is debug-only,
/// `PingPongSuperblock::write` takes `&self`, and two overlapping writers
/// collide on one fixed `.tmp` path and tear a slot while both return `Ok`.
pub struct LocalGate {
    state: Cell<GateState>,
    waiters: RefCell<VecDeque<Waiter>>,
    next_ticket: Cell<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GateState {
    Free,
    Held,
    /// A release handed the gate to the waiter with this ticket, which has not
    /// taken it yet.
    Handed(u64),
}

struct Waiter {
    ticket: u64,
    waker: Waker,
}

impl LocalGate {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: Cell::new(GateState::Free),
            waiters: RefCell::new(VecDeque::new()),
            next_ticket: Cell::new(0),
        }
    }

    /// Waits for the gate. The returned future joins the waiter queue at its
    /// first poll, not here, so arrival order is first-poll order.
    // A manual `Future` impl does not inherit the async-fn unused lint, so
    // without these `gate.acquire();` and `gate.acquire().await;` both
    // compile clean as no-op exclusion (the guard drops at the semicolon).
    #[must_use = "acquire does nothing until awaited"]
    pub const fn acquire(&self) -> LocalGateAcquire<'_> {
        LocalGateAcquire {
            gate: self,
            ticket: None,
        }
    }

    #[must_use]
    pub const fn try_acquire(&self) -> Option<LocalGateGuard<'_>> {
        if self.take_free() {
            Some(LocalGateGuard { gate: self })
        } else {
            None
        }
    }

    /// A dropped owned lease leaves the resource fenced. Only an owner that
    /// observed physical completion may release it.
    #[must_use]
    pub fn try_acquire_owned(self: &Rc<Self>) -> Option<OwnedLocalGateGuard> {
        if !self.take_free() {
            return None;
        }
        Some(OwnedLocalGateGuard {
            gate: Rc::clone(self),
        })
    }

    /// A gate handed to a waiter is not free, so no acquirer overtakes it.
    const fn take_free(&self) -> bool {
        let free = matches!(self.state.get(), GateState::Free);
        if free {
            // `Cell::set` is not const, and `try_acquire` is.
            self.state.replace(GateState::Held);
        }
        free
    }

    fn release(&self) {
        // Pop before waking: a waker that polls inline can re-enter acquire and
        // borrow the waiter queue again.
        let next = self.waiters.borrow_mut().pop_front();
        match next {
            Some(waiter) => {
                self.state.set(GateState::Handed(waiter.ticket));
                waiter.waker.wake();
            }
            None => self.state.set(GateState::Free),
        }
    }
}

#[must_use = "release only after physical work has settled"]
pub struct OwnedLocalGateGuard {
    gate: Rc<LocalGate>,
}

impl OwnedLocalGateGuard {
    pub fn release(self) {
        self.gate.release();
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
    /// Its place in the waiter queue, from the first poll that found the gate
    /// busy.
    ticket: Option<u64>,
}

impl<'a> std::future::Future for LocalGateAcquire<'a> {
    type Output = LocalGateGuard<'a>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let gate = this.gate;
        let Some(ticket) = this.ticket else {
            // A release hands a contended gate over without freeing it, so a
            // free gate has no waiter to overtake.
            if gate.take_free() {
                return std::task::Poll::Ready(LocalGateGuard { gate });
            }
            let ticket = gate.next_ticket.get();
            gate.next_ticket.set(ticket.wrapping_add(1));
            gate.waiters.borrow_mut().push_back(Waiter {
                ticket,
                waker: cx.waker().clone(),
            });
            this.ticket = Some(ticket);
            return std::task::Poll::Pending;
        };
        if gate.state.get() == GateState::Handed(ticket) {
            gate.state.set(GateState::Held);
            this.ticket = None;
            return std::task::Poll::Ready(LocalGateGuard { gate });
        }
        // A waiter driven by `select!` or `join!` is polled on every wake of
        // its task, and the task can poll it with a new waker.
        if let Some(waiter) = gate
            .waiters
            .borrow_mut()
            .iter_mut()
            .find(|waiter| waiter.ticket == ticket)
        {
            waiter.waker.clone_from(cx.waker());
        }
        std::task::Poll::Pending
    }
}

impl Drop for LocalGateAcquire<'_> {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket else {
            return;
        };
        if self.gate.state.get() == GateState::Handed(ticket) {
            self.gate.release();
        } else {
            self.gate
                .waiters
                .borrow_mut()
                .retain(|waiter| waiter.ticket != ticket);
        }
    }
}

#[must_use = "dropping the guard immediately releases the gate"]
pub struct LocalGateGuard<'a> {
    gate: &'a LocalGate,
}

impl Drop for LocalGateGuard<'_> {
    fn drop(&mut self) {
        self.gate.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use futures::future::join;

    #[test]
    fn owned_lease_requires_explicit_settlement_before_reuse() {
        let gate = Rc::new(LocalGate::new());
        let held = gate.try_acquire_owned().unwrap();
        assert!(gate.try_acquire_owned().is_none());
        held.release();
        let interrupted = gate.try_acquire_owned().unwrap();
        drop(interrupted);
        assert!(gate.try_acquire_owned().is_none());
    }

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

    /// A waiter that goes away leaves the queue: the next acquirer still gets
    /// the gate. Cancel safety is load-bearing here, since every acquire site
    /// sits in a future a shard can drop.
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

    /// A release hands the gate to the oldest waiter. An acquirer that polls
    /// before that waiter runs again queues behind it, and so does a
    /// `try_acquire`.
    #[test]
    fn given_a_waiter_when_another_acquirer_polls_after_release_should_hand_the_gate_to_the_waiter()
    {
        let gate = LocalGate::new();
        let held = gate.try_acquire().unwrap();
        let mut waiter = Box::pin(gate.acquire());
        assert!(waiter.as_mut().now_or_never().is_none());

        drop(held);
        let mut later = Box::pin(gate.acquire());
        assert!(
            later.as_mut().now_or_never().is_none(),
            "a later acquirer must not take the gate handed to a waiter"
        );
        assert!(gate.try_acquire().is_none());

        let guard = waiter
            .now_or_never()
            .expect("the release handed the gate to the waiter");
        drop(guard);
        assert!(later.now_or_never().is_some());
        assert!(gate.try_acquire().is_some(), "the gate is free again");
    }

    /// A waiter dropped after a release handed it the gate passes the gate to
    /// the next waiter, so the gate does not wedge.
    #[test]
    fn given_a_waiter_handed_the_gate_when_dropped_should_pass_it_to_the_next_waiter() {
        let gate = LocalGate::new();
        let held = gate.try_acquire().unwrap();
        let mut handed = Box::pin(gate.acquire());
        let mut next = Box::pin(gate.acquire());
        assert!(handed.as_mut().now_or_never().is_none());
        assert!(next.as_mut().now_or_never().is_none());

        drop(held);
        drop(handed);
        let guard = next
            .now_or_never()
            .expect("the dropped waiter passed the gate on");
        drop(guard);
        assert!(gate.try_acquire().is_some(), "the gate is free again");
    }
}
