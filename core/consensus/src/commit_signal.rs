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

//! Single-threaded edge-triggered notify.
//!
//! Lets a parked caller wake when `commit_min` advances. Multiple callers may
//! park concurrently (one per in-flight in-process submit); a notify wakes
//! every parked waiter, each re-checks its own gate and re-parks if still not
//! satisfied.
//!
//! Edge-triggered: a notify with no parked waiter is lost. Once a caller has
//! decided it must wait, it MUST arm-then-check: build the [`Notified`] future
//! (registering intent) BEFORE re-reading the gated state, so a notify landing
//! between that read and the await is not missed. Check the gate FIRST and only
//! arm when a wait is unavoidable - arming registers a roster slot, and the hot
//! already-satisfied path should leave the roster untouched.
//!
//! Each [`Notified`] deregisters its slot on drop (and `notify_all` drains the
//! roster), so the roster stays bounded to currently-live waiters.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// Slot for one parked waiter. `Rc`-shared between the [`Notified`] future and
/// the [`CommitSignal`]'s roster so a wake reaches the future's task.
#[derive(Default)]
struct Slot {
    waker: RefCell<Option<Waker>>,
    notified: RefCell<bool>,
}

/// Edge-triggered single-threaded notify. Not `Send`/`Sync`: state is
/// `Rc<RefCell<_>>`, thread-sharing would race at borrow.
///
/// The roster is an `Rc<RefCell<_>>` so each [`Notified`] keeps a handle to
/// deregister its own slot on drop, bounding the roster to live waiters.
#[derive(Debug, Default)]
pub struct CommitSignal {
    signal: Rc<RefCell<Vec<Rc<Slot>>>>,
}

impl std::fmt::Debug for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slot").finish_non_exhaustive()
    }
}

impl CommitSignal {
    /// Wake every parked waiter. Drains the roster: each waiter re-arms on its
    /// next `commit_advanced()` if its gate is still unsatisfied. A drained
    /// slot's [`Notified::drop`] becomes a no-op (the entry is already gone).
    pub fn notify_all(&self) {
        let slots = std::mem::take(&mut *self.signal.borrow_mut());
        for slot in slots {
            *slot.notified.borrow_mut() = true;
            if let Some(waker) = slot.waker.borrow_mut().take() {
                waker.wake();
            }
        }
    }

    /// Arm a single-use wait. Build this only once a wait is unavoidable and
    /// re-check the gate AFTER (arm-then-check) so a notify landing in between
    /// is not lost. The returned future resolves on the next `notify_all`.
    #[must_use]
    pub fn notified(&self) -> Notified {
        let slot = Rc::new(Slot::default());
        self.signal.borrow_mut().push(Rc::clone(&slot));
        Notified {
            slot,
            signal: Rc::clone(&self.signal),
        }
    }

    #[cfg(test)]
    fn parked_count(&self) -> usize {
        self.signal.borrow().len()
    }
}

/// One-shot future that resolves on the next [`CommitSignal::notify_all`].
/// Dropping before resolution deregisters its slot, so the roster stays
/// bounded to currently-live waiters.
pub struct Notified {
    slot: Rc<Slot>,
    signal: Rc<RefCell<Vec<Rc<Slot>>>>,
}

impl Future for Notified {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if *self.slot.notified.borrow() {
            return Poll::Ready(());
        }
        *self.slot.waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for Notified {
    fn drop(&mut self) {
        // Idempotent: if `notify_all` already drained the slot it is absent.
        self.signal
            .borrow_mut()
            .retain(|parked| !Rc::ptr_eq(parked, &self.slot));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Context;

    #[test]
    fn notify_wakes_armed_waiter() {
        let signal = CommitSignal::default();
        let mut fut = signal.notified();

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());

        signal.notify_all();
        assert!(Pin::new(&mut fut).poll(&mut cx).is_ready());
    }

    #[test]
    fn notify_before_poll_is_not_lost() {
        // Arm-then-check: future created (armed) before the notify; the notify
        // must still resolve it even though it was never polled Pending first.
        let signal = CommitSignal::default();
        let mut fut = signal.notified();
        signal.notify_all();

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut fut).poll(&mut cx).is_ready());
    }

    #[test]
    fn notify_wakes_all_parked_waiters() {
        let signal = CommitSignal::default();
        let mut a = signal.notified();
        let mut b = signal.notified();

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut a).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut b).poll(&mut cx).is_pending());

        signal.notify_all();
        assert!(Pin::new(&mut a).poll(&mut cx).is_ready());
        assert!(Pin::new(&mut b).poll(&mut cx).is_ready());
    }

    #[test]
    fn notify_with_no_waiter_is_dropped() {
        // Edge-triggered: a notify with nothing parked is lost. A waiter armed
        // afterwards stays pending (it must re-arm to catch the next edge).
        let signal = CommitSignal::default();
        signal.notify_all();

        let mut fut = signal.notified();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
    }

    #[test]
    fn dropping_unpolled_notified_deregisters_slot() {
        let signal = CommitSignal::default();
        assert_eq!(signal.parked_count(), 0);

        let armed = signal.notified();
        assert_eq!(signal.parked_count(), 1, "arming registers a slot");
        drop(armed);
        assert_eq!(signal.parked_count(), 0, "drop deregisters the slot");

        // A surviving waiter still wakes exactly once: the abandoned slot left
        // no stale entry to double-wake.
        let mut live = signal.notified();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut live).poll(&mut cx).is_pending());
        signal.notify_all();
        assert!(Pin::new(&mut live).poll(&mut cx).is_ready());
    }

    #[test]
    fn repeated_arm_drop_keeps_roster_bounded() {
        // Models the hot path's transient parks: many arm-then-abandon cycles
        // must not grow the roster (the leak this Drop impl prevents).
        let signal = CommitSignal::default();
        for _ in 0..1000 {
            let armed = signal.notified();
            assert_eq!(signal.parked_count(), 1);
            drop(armed);
            assert_eq!(signal.parked_count(), 0);
        }
        assert_eq!(signal.parked_count(), 0, "roster bounded to live waiters");
    }

    #[test]
    fn notify_all_then_drop_is_idempotent_no_op() {
        // A slot drained by notify_all is already gone; the eventual drop must
        // not panic or remove an unrelated waiter.
        let signal = CommitSignal::default();
        let mut resolved = signal.notified();
        let other = signal.notified();
        assert_eq!(signal.parked_count(), 2);

        signal.notify_all();
        assert_eq!(signal.parked_count(), 0, "notify_all drains the roster");

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut resolved).poll(&mut cx).is_ready());

        // Re-arm a fresh waiter, then drop the already-drained ones: their drop
        // must not touch the fresh slot.
        let fresh = signal.notified();
        assert_eq!(signal.parked_count(), 1);
        drop(resolved);
        drop(other);
        assert_eq!(signal.parked_count(), 1, "drained slots' drop is a no-op");
        drop(fresh);
        assert_eq!(signal.parked_count(), 0);
    }
}
