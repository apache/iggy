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

//! Single-threaded one-shot channel.
//!
//! Per [`crate::PipelineEntry`]: sender on entry, receiver on caller. Commit
//! handler fires via [`crate::PipelineEntry::take_reply_sender`]. Drop
//! without send -> `Err(Canceled)` (view-change cleanup via
//! [`crate::Pipeline::cancel_all_subscribers`]).

use futures_core::future::FusedFuture;
use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// `Sender` dropped without sending. Mirrors `futures::channel::oneshot::Canceled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Canceled;

impl fmt::Display for Canceled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("oneshot sender dropped without sending")
    }
}

impl std::error::Error for Canceled {}

enum State<T> {
    Empty,
    Waiting(Waker),
    Ready(T),
    /// Sender dropped; receiver resolves `Err(Canceled)` then `Done`.
    SenderDropped,
    /// Receiver dropped; `Sender::send` -> `Err(value)`.
    ReceiverDropped,
    /// Resolved. Re-polls return `Err(Canceled)` idempotently, prevents
    /// a `select!`/`Fuse` re-poll from registering a waker nothing can fire.
    Done,
}

struct Inner<T> {
    state: RefCell<State<T>>,
}

/// Send half. Single-use; drop without send -> receiver gets `Err(Canceled)`.
pub struct Sender<T> {
    inner: Rc<Inner<T>>,
}

/// Receive half. `Future<Output = Result<T, Canceled>>`. `FusedFuture`, safe
/// in `select!`/`Fuse`. `Unpin`, `.await` works without `pin!`.
pub struct Receiver<T> {
    inner: Rc<Inner<T>>,
}

// Explicit Unpin: compile error if a future field becomes !Unpin.
impl<T> Unpin for Sender<T> {}
impl<T> Unpin for Receiver<T> {}

// Compile-time: Sender/Receiver are NEITHER Send NOR Sync. Pattern from
// `static_assertions::assert_not_impl_any!`: blanket `()` impl + marker
// impls gated on Send/Sync break inference if either is added.
//
// # Safety
// State is `Rc<RefCell<_>>`; thread-sharing would race at borrow.
const _: fn() = || {
    trait AmbiguousIfImpl<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
    impl<T: ?Sized + Send> AmbiguousIfImpl<u8> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfImpl<u16> for T {}

    let _ = <Sender<u64> as AmbiguousIfImpl<_>>::some_item;
    let _ = <Receiver<u64> as AmbiguousIfImpl<_>>::some_item;
};

// Compile-time: both halves stay Unpin even when payload T is !Unpin.
const _: () = {
    const fn assert_unpin<T: Unpin>() {}
    struct NotUnpin {
        _pinned: std::marker::PhantomPinned,
    }
    assert_unpin::<Sender<u64>>();
    assert_unpin::<Receiver<u64>>();
    assert_unpin::<Sender<NotUnpin>>();
    assert_unpin::<Receiver<NotUnpin>>();
};

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

/// New one-shot channel.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Rc::new(Inner {
        state: RefCell::new(State::Empty),
    });
    (
        Sender {
            inner: Rc::clone(&inner),
        },
        Receiver { inner },
    )
}

impl<T> Sender<T> {
    /// `true` iff `Receiver` dropped. No waker; on single-threaded runtime
    /// `false` is valid until next `.await`. Sender stays usable.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(*self.inner.state.borrow(), State::ReceiverDropped)
    }

    /// Deliver `value`. Buffers in `State::Ready` until poll/drop.
    /// Receiver-drop-without-poll silently drops the value; check
    /// [`Self::is_closed`] first to avoid producing.
    ///
    /// # Errors
    /// `Err(value)` if receiver already dropped, caller recovers payload.
    pub fn send(self, value: T) -> Result<(), T> {
        let mut state = self.inner.state.borrow_mut();
        match mem::replace(&mut *state, State::Empty) {
            State::Empty => {
                *state = State::Ready(value);
                Ok(())
            }
            State::Waiting(waker) => {
                *state = State::Ready(value);
                drop(state);
                waker.wake();
                Ok(())
            }
            State::ReceiverDropped => {
                *state = State::ReceiverDropped;
                Err(value)
            }
            // Unreachable: Ready/SenderDropped/Done require Sender already
            // consumed/dropped. Debug-assert + fall through to Err(value);
            // future refactor violating invariant fails loudly in dev,
            // stays receiver-gone-equivalent in prod.
            other @ (State::Ready(_) | State::SenderDropped | State::Done) => {
                let label = match &other {
                    State::Ready(_) => "Ready",
                    State::SenderDropped => "SenderDropped",
                    State::Done => "Done",
                    _ => unreachable!(),
                };
                debug_assert!(false, "oneshot::Sender::send: unreachable state {label}");
                // Restore (mem::replace moved it out). Ready round-trips
                // its buffered value so receiver still gets original.
                *state = other;
                Err(value)
            }
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.borrow_mut();
        match mem::replace(&mut *state, State::SenderDropped) {
            State::Empty => {}
            State::Waiting(waker) => {
                drop(state);
                waker.wake();
            }
            // send already terminated, or receiver dropped first,
            // preserve terminal state.
            other => *state = other,
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, Canceled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, Canceled>> {
        let mut state = self.inner.state.borrow_mut();
        match mem::replace(&mut *state, State::Empty) {
            State::Empty => {
                *state = State::Waiting(cx.waker().clone());
                Poll::Pending
            }
            State::Waiting(old) => {
                let waker = if old.will_wake(cx.waker()) {
                    old
                } else {
                    cx.waker().clone()
                };
                *state = State::Waiting(waker);
                Poll::Pending
            }
            State::Ready(value) => {
                // Transition to Done: re-polls return Err(Canceled), no
                // dangling waker (sender gone).
                *state = State::Done;
                Poll::Ready(Ok(value))
            }
            State::SenderDropped | State::Done => {
                *state = State::Done;
                Poll::Ready(Err(Canceled))
            }
            // ReceiverDropped is set by Receiver::drop; can't be alive +
            // dropped simultaneously.
            State::ReceiverDropped => unreachable!(),
        }
    }
}

impl<T> FusedFuture for Receiver<T> {
    fn is_terminated(&self) -> bool {
        matches!(*self.inner.state.borrow(), State::Done)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Replacing Ready(_) drops any undelivered value.
        let _ = mem::replace(&mut *self.inner.state.borrow_mut(), State::ReceiverDropped);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sender_drop_while_waiting_wakes_with_canceled() {
        let (tx, rx) = channel::<u64>();

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(rx);
        assert!(fut.as_mut().poll(&mut cx).is_pending());

        drop(tx);

        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Err(Canceled)) => {}
            other => panic!("expected Ready(Err(Canceled)), got {other:?}"),
        }
    }

    #[test]
    fn ready_value_dropped_when_receiver_drops_unread() {
        let sentinel = Rc::new(());
        let weak = Rc::downgrade(&sentinel);

        let (tx, rx) = channel::<Rc<()>>();
        tx.send(sentinel).expect("receiver alive");
        assert!(weak.upgrade().is_some());
        drop(rx);
        assert!(weak.upgrade().is_none(), "Ready dropped on receiver drop");
    }

    #[test]
    fn is_closed_reflects_receiver_drop() {
        let (tx, rx) = channel::<u64>();
        assert!(!tx.is_closed());
        drop(rx);
        assert!(tx.is_closed());
        // Sender stays usable; send returns Err(value).
        match tx.send(99) {
            Err(value) => assert_eq!(value, 99),
            Ok(()) => panic!("expected Err after receiver drop"),
        }
    }

    #[test]
    fn is_closed_false_while_waiting() {
        // is_closed flips only on receiver drop, never on Empty/Waiting.
        let (tx, rx) = channel::<u64>();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(rx);
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert!(!tx.is_closed());
    }

    // Regression: select!/Fuse re-poll after Ready(Ok) must not register a
    // waker (sender gone). Terminal Done guarantees Err(Canceled) idempotency
    // so consumer can't stall indistinguishably from stuck consensus.
    #[test]
    fn poll_after_ready_ok_returns_ready_canceled() {
        let (tx, rx) = channel::<u64>();
        tx.send(42).expect("receiver alive");

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(rx);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(value)) => assert_eq!(value, 42),
            other => panic!("expected Ready(Ok(42)), got {other:?}"),
        }
        // Second poll must NOT be Pending, Done state exists to prevent that deadlock.
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Err(Canceled)) => {}
            other => panic!("expected Ready(Err(Canceled)) on re-poll, got {other:?}"),
        }
        // Third poll: still Ready(Err(Canceled)), not Pending.
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Err(Canceled)) => {}
            other => panic!("expected Ready(Err(Canceled)) on third poll, got {other:?}"),
        }
    }

    #[test]
    fn poll_after_ready_canceled_stays_ready_canceled() {
        let (tx, rx) = channel::<u64>();
        drop(tx);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(rx);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Err(Canceled)) => {}
            other => panic!("expected Ready(Err(Canceled)), got {other:?}"),
        }
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Err(Canceled)) => {}
            other => panic!("expected Ready(Err(Canceled)) on re-poll, got {other:?}"),
        }
    }

    #[test]
    fn is_terminated_lifecycle() {
        let (tx, rx) = channel::<u64>();
        assert!(!rx.is_terminated(), "fresh: not terminated");

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(rx);
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert!(!fut.is_terminated(), "waiting: not terminated");

        tx.send(7).expect("receiver alive");
        assert!(
            !fut.is_terminated(),
            "Ready-but-unpolled: not terminated (select! must poll)"
        );

        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(7)) => {}
            other => panic!("expected Ready(Ok(7)), got {other:?}"),
        }
        assert!(fut.is_terminated(), "post-delivery: terminated");
    }

    #[test]
    fn is_terminated_after_sender_drop_is_observed() {
        let (tx, rx) = channel::<u64>();
        drop(tx);
        // Pre-poll: SenderDropped visible, Canceled not yet seen.
        assert!(!rx.is_terminated());

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(rx);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Err(Canceled)) => {}
            other => panic!("expected Ready(Err(Canceled)), got {other:?}"),
        }
        assert!(fut.is_terminated(), "post-Canceled: terminated");
    }

    // No-pin call shape (works because Receiver: Unpin).
    #[test]
    fn receiver_polled_via_pin_new_directly() {
        let (tx, rx) = channel::<u64>();
        tx.send(123).expect("receiver alive");

        let mut rx = rx;
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut rx).poll(&mut cx) {
            Poll::Ready(Ok(value)) => assert_eq!(value, 123),
            other => panic!("expected Ready(Ok(123)), got {other:?}"),
        }
    }
}
