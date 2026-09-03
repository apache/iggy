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

//! The node-wide applied metadata frontier and the wait a read parks on.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

/// Highest metadata op whose apply has been PUBLISHED on this node, shared by
/// every shard, plus the wakers of the reads waiting for it to reach them.
///
/// `consensus.commit_min()` answers the same question but exists only on shard
/// 0, so a read served by a peer shard has no way to tell whether the node
/// caught up to an op its client already saw committed. One process-wide cell
/// does, for one `Acquire` load on the read fast path.
///
/// The op is `Release`-written right after each apply's `publish()` and
/// `Acquire`-read; observing `>= op` therefore happens-after that publish, so a
/// following left-right `enter()` is guaranteed to see the op. `fetch_max`
/// rather than `store` because three writers move it -- the commit loop, the
/// recovery seed, and a state-transfer install -- and only monotonicity makes
/// their order irrelevant.
///
/// A `std::sync::Mutex` guards the waiter list, not a `tokio` one: it is taken
/// and dropped inside [`Self::advance`] and inside one `poll`, never across an
/// `.await`, and it has to be `Sync` because the writer is shard 0's thread
/// while the sleepers are on every shard.
#[derive(Debug, Default)]
pub struct AppliedFrontier {
    op: AtomicU64,
    waiters: Mutex<Waiters>,
}

/// Registered waits, keyed by an id so a re-poll can refresh its own waker and
/// a dropped wait (an HTTP client that disconnected mid-read) can remove it.
#[derive(Debug, Default)]
struct Waiters {
    next_id: u64,
    entries: Vec<Waiter>,
}

#[derive(Debug)]
struct Waiter {
    id: u64,
    target: u64,
    waker: Waker,
}

impl AppliedFrontier {
    /// Highest metadata op this NODE has applied and published.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.op.load(Ordering::Acquire)
    }

    /// Publish `op` as applied and wake every read waiting at or below it.
    /// Monotone, so a lower value is a no-op and wakes nobody.
    ///
    /// Must run AFTER the apply's `publish()` and, on the commit path, in the
    /// same await-free region as `advance_commit_min`: a reader that sees the
    /// frontier must be guaranteed to see the op's effects.
    pub fn advance(&self, op: u64) {
        if self.op.fetch_max(op, Ordering::Release) >= op {
            return;
        }
        let mut waiters = self.waiters.lock().unwrap_or_else(PoisonError::into_inner);
        waiters.entries.retain(|waiter| {
            if waiter.target > op {
                return true;
            }
            waiter.waker.wake_by_ref();
            false
        });
    }

    /// A future that completes once the frontier covers `target`.
    ///
    /// Event-driven, not polled: the commit path wakes it, so a read resumes on
    /// the commit it was waiting for rather than on the next tick. It has no
    /// deadline of its own -- the caller composes one, because the two read
    /// planes measure time differently (the shard bus timer, virtual under the
    /// simulator, against `compio::time`).
    pub const fn reached(&self, target: u64) -> Reached<'_> {
        Reached {
            frontier: self,
            target,
            id: None,
        }
    }

    /// Register a wait for `target` under `existing` (its id from an earlier
    /// poll, if any), or report that the frontier already covers it.
    ///
    /// One lock acquisition, released with the return: the re-read under it is
    /// what closes the race with [`Self::advance`], which bumps the op and only
    /// then takes this lock, so an advance landing between a caller's load and
    /// this call is one the wait would otherwise sleep through.
    fn register(&self, existing: Option<u64>, target: u64, waker: &Waker) -> Registered {
        let mut waiters = self.waiters.lock().unwrap_or_else(PoisonError::into_inner);
        if self.get() >= target {
            return Registered::Ready;
        }
        // Refresh rather than stack: a re-poll may arrive under a different
        // task (a `select` re-driven elsewhere), and two entries for one wait
        // would leak the first.
        let id = if let Some(waiter) =
            existing.and_then(|id| waiters.entries.iter_mut().find(|waiter| waiter.id == id))
        {
            waiter.waker.clone_from(waker);
            waiter.id
        } else {
            let id = waiters.next_id;
            waiters.next_id += 1;
            waiters.entries.push(Waiter {
                id,
                target,
                waker: waker.clone(),
            });
            id
        };
        drop(waiters);
        Registered::Waiting(id)
    }

    /// Drop the registration `id`, if it is still listed.
    fn deregister(&self, id: u64) {
        self.waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .retain(|waiter| waiter.id != id);
    }

    /// Waits currently parked. For tests: a wait that outlives its future is a
    /// leaked waker.
    #[must_use]
    pub fn waiting(&self) -> usize {
        self.waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .len()
    }
}

/// Outcome of registering a wait: nothing to wait for, or the id the wait is
/// listed under.
#[derive(Debug, Clone, Copy)]
enum Registered {
    Ready,
    Waiting(u64),
}

/// The wait [`AppliedFrontier::reached`] hands out. Deregisters on drop, so a
/// cancelled read (a dropped axum handler future, a closed socket) leaves no
/// waker behind.
#[derive(Debug)]
pub struct Reached<'a> {
    frontier: &'a AppliedFrontier,
    target: u64,
    id: Option<u64>,
}

impl Future for Reached<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        // Ahead of the registration, so the steady state (a frontier already
        // past the target) costs one load and never touches the lock.
        if this.frontier.get() >= this.target {
            return Poll::Ready(());
        }
        match this
            .frontier
            .register(this.id, this.target, context.waker())
        {
            Registered::Ready => Poll::Ready(()),
            Registered::Waiting(id) => {
                this.id = Some(id);
                Poll::Pending
            }
        }
    }
}

impl Drop for Reached<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.frontier.deregister(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AppliedFrontier;
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    /// A frontier already at or above the target is the steady state, and it
    /// must cost neither a wake nor a registration: a gate that parked here
    /// would put a commit's latency on every metadata read in the cluster.
    #[test]
    fn given_a_frontier_at_the_target_when_waiting_should_be_ready_without_registering() {
        let frontier = AppliedFrontier::default();
        frontier.advance(7);

        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        for target in [0, 7] {
            let mut wait = pin!(frontier.reached(target));
            assert_eq!(wait.as_mut().poll(&mut context), Poll::Ready(()));
        }
        assert_eq!(frontier.waiting(), 0, "a ready wait registers nothing");
    }

    /// The wait is what replaces the poll loop, so the advance has to be what
    /// wakes it: park below the target, advance past it, and the wait must be
    /// woken and complete without any intervening timer.
    #[test]
    fn given_a_parked_wait_when_the_frontier_advances_should_wake_and_complete() {
        let frontier = AppliedFrontier::default();
        let woken = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let waker = futures::task::waker(Arc::new(FlagWaker {
            woken: Arc::clone(&woken),
        }));
        let mut context = Context::from_waker(&waker);

        let mut wait = pin!(frontier.reached(9));
        assert_eq!(wait.as_mut().poll(&mut context), Poll::Pending);
        assert_eq!(frontier.waiting(), 1);

        // Below the target: no wake, still parked.
        frontier.advance(8);
        assert!(!woken.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(frontier.waiting(), 1);

        frontier.advance(9);
        assert!(
            woken.load(std::sync::atomic::Ordering::Acquire),
            "the advance past the target must wake the parked read"
        );
        assert_eq!(
            frontier.waiting(),
            0,
            "a woken wait is off the list, so a later advance re-wakes nothing"
        );
        assert_eq!(wait.as_mut().poll(&mut context), Poll::Ready(()));
    }

    /// A re-poll must not stack a second registration, and a dropped wait must
    /// take its waker with it: an HTTP read is cancelled whenever its client
    /// disconnects mid-wait, and a leaked waker would be a leak per disconnect.
    #[test]
    fn given_a_repolled_wait_when_dropped_should_leave_no_registration() {
        let frontier = AppliedFrontier::default();
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        {
            let mut wait = pin!(frontier.reached(9));
            assert_eq!(wait.as_mut().poll(&mut context), Poll::Pending);
            assert_eq!(wait.as_mut().poll(&mut context), Poll::Pending);
            assert_eq!(frontier.waiting(), 1, "a re-poll refreshes, never stacks");
        }
        assert_eq!(frontier.waiting(), 0, "a dropped wait deregisters");
    }

    struct FlagWaker {
        woken: Arc<std::sync::atomic::AtomicBool>,
    }

    impl futures::task::ArcWake for FlagWaker {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self
                .woken
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
}
