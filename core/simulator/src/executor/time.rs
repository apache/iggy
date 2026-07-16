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

//! Virtual clock for the deterministic executor.
//!
//! Time never flows on its own: it advances only when the simulation calls
//! [`DetExecutor::advance_time`](super::DetExecutor::advance_time). Sleep
//! futures register eagerly (the deadline is anchored at creation, matching
//! `compio::time::sleep` semantics that the shard pump's re-arm relies on)
//! and fire in strict `(deadline, seq)` order, so equal deadlines resolve
//! FIFO by creation order regardless of heap internals.

use std::cell::{Cell, RefCell};
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// A point on the simulated timeline, in nanoseconds since simulation start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimInstant(u64);

impl SimInstant {
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    #[must_use]
    pub const fn as_nanos(&self) -> u64 {
        self.0
    }
}

/// Cloneable handle to the executor's virtual clock.
#[derive(Debug, Clone)]
pub struct TimerHandle(Rc<TimeShared>);

impl TimerHandle {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self(Rc::new(TimeShared {
            now_nanos: Cell::new(0),
            queue: RefCell::new(TimerQueue {
                heap: BinaryHeap::new(),
                next_seq: 0,
            }),
        }))
    }

    /// Current virtual time.
    #[must_use]
    pub fn now(&self) -> SimInstant {
        SimInstant(self.0.now_nanos.get())
    }

    /// A future that completes once virtual time has advanced past
    /// `now + duration`. The deadline is fixed here, at creation.
    ///
    /// # Panics
    /// Panics if `duration` exceeds `u64::MAX` nanoseconds.
    #[must_use]
    pub fn sleep(&self, duration: Duration) -> SleepFuture {
        let cell = Rc::new(TimerCell {
            fired: Cell::new(false),
            cancelled: Cell::new(false),
            waker: RefCell::new(None),
        });
        if duration.is_zero() {
            // Mirror compio: a deadline that is not in the future resolves
            // immediately and never touches the timer wheel.
            cell.fired.set(true);
            return SleepFuture {
                cell: Rc::clone(&cell),
            };
        }
        let delta = u64::try_from(duration.as_nanos()).expect("sleep duration exceeds u64 nanos");
        let deadline = self.0.now_nanos.get().saturating_add(delta);
        let mut queue = self.0.queue.borrow_mut();
        let seq = queue.next_seq;
        queue.next_seq += 1;
        queue.heap.push(HeapEntry {
            deadline,
            seq,
            cell: Rc::clone(&cell),
        });
        drop(queue);
        SleepFuture { cell }
    }

    /// Advance virtual time by `delta`, firing every non-cancelled timer whose
    /// deadline is now due, in `(deadline, seq)` order. Returns the fired
    /// sequence numbers in fire order so the executor can fold them into its
    /// schedule trace.
    ///
    /// # Panics
    /// Panics if `delta` exceeds `u64::MAX` nanoseconds.
    pub(crate) fn advance(&self, delta: Duration) -> Vec<u64> {
        let delta = u64::try_from(delta.as_nanos()).expect("advance delta exceeds u64 nanos");
        // Publish the new time before firing wakes: a task woken by one of
        // these timers must observe the post-advance clock when it polls.
        let new_now = self.0.now_nanos.get().saturating_add(delta);
        self.0.now_nanos.set(new_now);

        let mut fired = Vec::new();
        let mut queue = self.0.queue.borrow_mut();
        while let Some(entry) = queue.heap.peek() {
            if entry.deadline > new_now {
                break;
            }
            let entry = queue.heap.pop().expect("peeked entry must pop");
            if entry.cell.cancelled.get() {
                continue;
            }
            entry.cell.fired.set(true);
            // Take the waker (releasing the cell borrow) before waking: `wake`
            // pushes onto the executor's wake queue, never back into this timer
            // queue, so holding the `queue` borrow across it cannot re-enter.
            let waker = entry.cell.waker.borrow_mut().take();
            if let Some(waker) = waker {
                waker.wake();
            }
            fired.push(entry.seq);
        }
        drop(queue);
        fired
    }
}

/// Future returned by [`TimerHandle::sleep`].
#[derive(Debug)]
pub struct SleepFuture {
    cell: Rc<TimerCell>,
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.cell.fired.get() {
            return Poll::Ready(());
        }
        let mut slot = self.cell.waker.borrow_mut();
        match slot.as_ref() {
            Some(existing) if existing.will_wake(cx.waker()) => {}
            _ => *slot = Some(cx.waker().clone()),
        }
        Poll::Pending
    }
}

impl Drop for SleepFuture {
    fn drop(&mut self) {
        // Lazy cancellation: the heap entry (and its `Rc<TimerCell>`) lingers
        // until `advance` reaches its deadline and skips it, so retention is
        // bounded by the deadline, not the future's lifetime (`BinaryHeap` has
        // no keyed removal, so eager cancellation would be O(n)). Clearing the
        // waker drops the task reference it pins.
        self.cell.cancelled.set(true);
        self.cell.waker.borrow_mut().take();
    }
}

#[derive(Debug)]
struct TimeShared {
    now_nanos: Cell<u64>,
    queue: RefCell<TimerQueue>,
}

#[derive(Debug)]
struct TimerQueue {
    heap: BinaryHeap<HeapEntry>,
    /// Monotonic tie-breaker: equal deadlines fire in creation order.
    next_seq: u64,
}

#[derive(Debug)]
struct TimerCell {
    fired: Cell<bool>,
    cancelled: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

#[derive(Debug)]
struct HeapEntry {
    deadline: u64,
    seq: u64,
    cell: Rc<TimerCell>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.seq == other.seq
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    /// Reversed on `(deadline, seq)`: `BinaryHeap` is a max-heap, so the
    /// inversion makes `peek`/`pop` yield the earliest deadline, FIFO within
    /// a deadline.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (other.deadline, other.seq).cmp(&(self.deadline, self.seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker;

    fn poll_once(future: &mut SleepFuture) -> Poll<()> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        Pin::new(future).poll(&mut cx)
    }

    #[test]
    fn zero_duration_completes_immediately() {
        let handle = TimerHandle::new();
        let mut sleep = handle.sleep(Duration::ZERO);
        assert_eq!(poll_once(&mut sleep), Poll::Ready(()));
    }

    #[test]
    fn fires_at_creation_anchored_deadline() {
        let handle = TimerHandle::new();
        let mut sleep = handle.sleep(Duration::from_millis(10));
        assert_eq!(poll_once(&mut sleep), Poll::Pending);

        // Partial advance: nothing is due yet, and the first poll landing
        // after time already moved must not re-anchor the deadline to poll
        // time (a poll-anchored deadline would sit at 16ms).
        assert!(handle.advance(Duration::from_millis(6)).is_empty());
        assert_eq!(poll_once(&mut sleep), Poll::Pending);

        // Crossing the creation-anchored 10ms deadline fires exactly once.
        assert_eq!(handle.advance(Duration::from_millis(4)).len(), 1);
        assert_eq!(poll_once(&mut sleep), Poll::Ready(()));
        assert_eq!(handle.now().as_nanos(), 10_000_000);
    }

    #[test]
    fn equal_deadlines_fire_fifo_by_creation() {
        let handle = TimerHandle::new();
        let _first = handle.sleep(Duration::from_millis(5));
        let _second = handle.sleep(Duration::from_millis(5));
        let fired = handle.advance(Duration::from_millis(5));
        assert_eq!(fired, vec![0, 1], "equal deadlines must fire in seq order");
    }

    #[test]
    fn cancelled_on_drop_never_fires() {
        let handle = TimerHandle::new();
        let sleep = handle.sleep(Duration::from_millis(5));
        drop(sleep);
        assert!(handle.advance(Duration::from_millis(10)).is_empty());
    }
}
