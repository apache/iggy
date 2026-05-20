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

//! Root shutdown token for the message bus.
//!
//! A single [`Shutdown`] is paired with any number of cloned [`ShutdownToken`]s.
//! Tasks (accept loops, read loops, reconnect periodics) keep a clone and
//! `select!` on [`ShutdownToken::wait`] or poll [`ShutdownToken::is_triggered`].
//! When [`Shutdown::trigger`] is called, every clone observes the transition
//! in the same runtime turn.
//!
//! Single-threaded compio means `Rc<Cell<bool>>` is correct; no `Arc`/atomics.

use async_channel::{Receiver, Sender};
use futures::FutureExt;
use futures::pin_mut;
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

/// Owning side of the shutdown signal.
///
/// Create via [`Shutdown::new`], hand out [`ShutdownToken`]s via [`ShutdownToken::clone`],
/// and call [`Shutdown::trigger`] exactly once to stop everyone.
///
/// `Clone` is supported so the installer can hand one clone to the
/// connection registry (which holds it as the per-conn trigger handle)
/// and another to the writer task scopeguard. Both clones share the
/// same `triggered` cell and `Sender`, so [`Shutdown::trigger`] is
/// idempotent and safe to call from any clone.
#[derive(Debug, Clone)]
pub struct Shutdown {
    sender: Sender<()>,
    triggered: Rc<Cell<bool>>,
}

/// Cheaply cloneable observer for a [`Shutdown`].
#[derive(Debug, Clone)]
pub struct ShutdownToken {
    receiver: Receiver<()>,
    triggered: Rc<Cell<bool>>,
}

impl Shutdown {
    /// Create a new shutdown signal and its first observer token.
    #[must_use]
    pub fn new() -> (Self, ShutdownToken) {
        let (sender, receiver) = async_channel::bounded::<()>(1);
        let triggered = Rc::new(Cell::new(false));
        let shutdown = Self {
            sender,
            triggered: triggered.clone(),
        };
        let token = ShutdownToken {
            receiver,
            triggered,
        };
        (shutdown, token)
    }

    /// Flip the bit and close the broadcast channel.
    ///
    /// Idempotent: a second call is a no-op. Does not consume `self` so
    /// the bus can keep the `Shutdown` as a plain field and trigger via
    /// a shared reference.
    pub fn trigger(&self) {
        if !self.triggered.replace(true) {
            self.sender.close();
        }
    }

    /// Whether [`trigger`](Self::trigger) has been called.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        self.triggered.get()
    }
}

impl ShutdownToken {
    /// O(1) non-blocking check. True after [`Shutdown::trigger`] returns.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        self.triggered.get()
    }

    /// Resolves the instant shutdown fires.
    ///
    /// Safe to poll concurrently from many tasks because `async_channel`
    /// receivers resolve to `Err(Closed)` for every waiter once the sender
    /// side closes.
    #[allow(clippy::future_not_send)]
    pub async fn wait(&self) {
        // `recv()` on a closed channel returns immediately with an error,
        // so this loop exits in one await as soon as the sender side closes.
        while self.receiver.recv().await.is_ok() {}
    }

    /// Sleep for `duration`, returning early if shutdown fires.
    ///
    /// Returns `true` if the full duration elapsed, `false` if shutdown
    /// interrupted the sleep. Used by periodic tasks:
    ///
    /// ```ignore
    /// while token.sleep_or_shutdown(PERIOD).await {
    ///     tick().await;
    /// }
    /// ```
    #[allow(clippy::future_not_send)]
    pub async fn sleep_or_shutdown(&self, duration: Duration) -> bool {
        if self.is_triggered() {
            return false;
        }
        futures::select! {
            () = self.wait().fuse() => false,
            () = compio::time::sleep(duration).fuse() => !self.is_triggered(),
        }
    }
}

/// Two-source fused shutdown observer.
///
/// Used by transports that must wake on either the bus-wide shutdown OR a
/// per-connection shutdown the installer triggers on insert-race. Carries
/// both [`ShutdownToken`]s and resolves [`Self::wait`] when either fires.
/// Cheap to clone (each clone is two `Rc<Cell<bool>>` clones).
///
/// Replaces an earlier design that spawned a per-connection bridge task to
/// fan-in the two tokens onto a third [`Shutdown`]; folding the merge into
/// the await site removes the spawn and the third channel without changing
/// observable semantics.
#[derive(Debug, Clone)]
pub struct FusedShutdown {
    bus: ShutdownToken,
    conn: ShutdownToken,
}

impl FusedShutdown {
    /// Pair the bus-wide token with a per-connection token.
    #[must_use]
    pub const fn new(bus: ShutdownToken, conn: ShutdownToken) -> Self {
        Self { bus, conn }
    }

    /// Construct a fused token from a single source. Both halves alias the
    /// same underlying signal; useful for unit tests that drive a transport
    /// in isolation, with no installer to provide a per-connection token.
    #[must_use]
    pub fn single(token: ShutdownToken) -> Self {
        Self {
            bus: token.clone(),
            conn: token,
        }
    }

    /// O(1) non-blocking check. True once either source has been triggered.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        self.bus.is_triggered() || self.conn.is_triggered()
    }

    /// Resolves the instant either source fires. Bus signal is preferred
    /// when both are simultaneously ready (matches the installer's
    /// shutdown-precedence convention).
    #[allow(clippy::future_not_send)]
    pub async fn wait(&self) {
        let bus = self.bus.wait().fuse();
        let conn = self.conn.wait().fuse();
        pin_mut!(bus, conn);
        futures::select_biased! {
            () = bus => {}
            () = conn => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[compio::test]
    async fn trigger_sets_flag_and_resolves_wait() {
        let (shutdown, token) = Shutdown::new();
        assert!(!token.is_triggered());

        shutdown.trigger();
        assert!(token.is_triggered());
        token.wait().await; // resolves immediately
    }

    #[compio::test]
    async fn wait_resolves_for_all_clones() {
        let (shutdown, token_a) = Shutdown::new();
        let token_b = token_a.clone();

        shutdown.trigger();
        token_a.wait().await;
        token_b.wait().await;
        assert!(token_a.is_triggered());
        assert!(token_b.is_triggered());
    }

    #[compio::test]
    async fn sleep_or_shutdown_returns_true_on_elapsed() {
        let (_shutdown, token) = Shutdown::new();
        let ok = token.sleep_or_shutdown(Duration::from_millis(20)).await;
        assert!(ok);
        assert!(!token.is_triggered());
    }

    #[compio::test]
    async fn sleep_or_shutdown_returns_false_when_triggered_first() {
        let (shutdown, token) = Shutdown::new();
        // Already triggered
        shutdown.trigger();
        let ok = token.sleep_or_shutdown(Duration::from_secs(10)).await;
        assert!(!ok);
    }

    #[compio::test]
    async fn sleep_or_shutdown_interrupted_mid_sleep() {
        let (shutdown, token) = Shutdown::new();
        let token_clone = token.clone();
        let sleeper = compio::runtime::spawn(async move {
            token_clone.sleep_or_shutdown(Duration::from_secs(10)).await
        });
        // Let the sleeper reach the select
        compio::time::sleep(Duration::from_millis(5)).await;
        shutdown.trigger();
        let ok = sleeper.await.expect("task ok");
        assert!(!ok);
    }

    #[compio::test]
    async fn fused_wait_resolves_when_bus_fires() {
        let (bus, bus_token) = Shutdown::new();
        let (_conn, conn_token) = Shutdown::new();
        let fused = FusedShutdown::new(bus_token, conn_token);
        assert!(!fused.is_triggered());
        bus.trigger();
        fused.wait().await;
        assert!(fused.is_triggered());
    }

    #[compio::test]
    async fn fused_wait_resolves_when_conn_fires() {
        let (_bus, bus_token) = Shutdown::new();
        let (conn, conn_token) = Shutdown::new();
        let fused = FusedShutdown::new(bus_token, conn_token);
        assert!(!fused.is_triggered());
        conn.trigger();
        fused.wait().await;
        assert!(fused.is_triggered());
    }

    #[compio::test]
    async fn fused_single_aliases_the_one_token() {
        let (shutdown, token) = Shutdown::new();
        let fused = FusedShutdown::single(token);
        shutdown.trigger();
        fused.wait().await;
        assert!(fused.is_triggered());
    }
}
