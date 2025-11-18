/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use async_channel::{Receiver, Sender, bounded};
use futures::FutureExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::trace;

/// Coordinates graceful shutdown across multiple tasks
#[derive(Clone)]
pub struct Shutdown {
    sender: Sender<()>,
    is_triggered: Arc<AtomicBool>,
}

impl Shutdown {
    pub fn new() -> (Self, ShutdownToken) {
        let (sender, receiver) = bounded(1);
        let is_triggered = Arc::new(AtomicBool::new(false));

        let shutdown = Self {
            sender,
            is_triggered: is_triggered.clone(),
        };

        let token = ShutdownToken {
            receiver,
            is_triggered,
        };

        (shutdown, token)
    }

    pub fn trigger(&self) {
        if self.is_triggered.swap(true, Ordering::SeqCst) {
            return;
        }

        trace!("Triggering shutdown signal");
        let _ = self.sender.close();
    }

    pub fn is_triggered(&self) -> bool {
        self.is_triggered.load(Ordering::Relaxed)
    }
}

/// Token held by tasks to receive shutdown signals
#[derive(Clone)]
pub struct ShutdownToken {
    receiver: Receiver<()>,
    is_triggered: Arc<AtomicBool>,
}

impl ShutdownToken {
    /// Wait for shutdown signal
    pub async fn wait(&self) {
        let _ = self.receiver.recv().await;
    }

    /// Check if shutdown has been triggered (non-blocking)
    pub fn is_triggered(&self) -> bool {
        self.is_triggered.load(Ordering::Relaxed)
    }

    /// Sleep for the specified duration or until shutdown is triggered
    /// Returns true if the full duration elapsed, false if shutdown was triggered
    pub async fn sleep_or_shutdown(&self, duration: Duration) -> bool {
        futures::select! {
            _ = self.wait().fuse() => false,
            _ = compio::time::sleep(duration).fuse() => !self.is_triggered(),
        }
    }

    /// Creates a scoped shutdown pair (child `Shutdown`, combined `ShutdownToken`).
    ///
    /// This is a bit complicated, but it needs to be this way to avoid deadlocks.
    ///
    /// The returned token fires when EITHER the parent or the child is triggered,
    /// while a child trigger does NOT propagate back to the parent.
    /// Internally spawns a tiny forwarder to merge both signals into one channel,
    /// so callers can await a single `wait()` and use fast `is_triggered()` checks
    /// without writing `select!` at every call site.
    /// Use when a subtree needs cancelation that respects parent cancelation,
    /// but can also be canceled locally.
    pub fn child(&self) -> (Shutdown, ShutdownToken) {
        let (child_shutdown, child_token) = Shutdown::new();
        let parent_receiver = self.receiver.clone();
        let child_receiver = child_token.receiver.clone();

        let (combined_sender, combined_receiver) = bounded(1);
        let combined_is_triggered = Arc::new(AtomicBool::new(false));

        let parent_triggered = self.is_triggered.clone();
        let child_triggered = child_token.is_triggered.clone();
        let combined_flag_for_task = combined_is_triggered.clone();

        compio::runtime::spawn(async move {
            futures::select! {
                _ = parent_receiver.recv().fuse() => {
                    trace!("Child token triggered by parent shutdown");
                },
                _ = child_receiver.recv().fuse() => {
                    trace!("Child token triggered by child shutdown");
                },
            }

            if parent_triggered.load(Ordering::Relaxed) || child_triggered.load(Ordering::Relaxed) {
                combined_flag_for_task.store(true, Ordering::SeqCst);
            }

            let _ = combined_sender.close();
        })
        .detach();

        let combined_token = ShutdownToken {
            receiver: combined_receiver,
            is_triggered: combined_is_triggered,
        };

        (child_shutdown, combined_token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[compio::test]
    async fn test_shutdown_trigger() {
        let (shutdown, token) = Shutdown::new();

        assert!(!token.is_triggered());

        shutdown.trigger();

        assert!(token.is_triggered());

        token.wait().await;
    }

    #[compio::test]
    async fn test_sleep_or_shutdown_completes() {
        let (_shutdown, token) = Shutdown::new();

        let completed = token.sleep_or_shutdown(Duration::from_millis(10)).await;
        assert!(completed);
    }

    #[compio::test]
    async fn test_sleep_or_shutdown_interrupted() {
        let (shutdown, token) = Shutdown::new();

        // Trigger shutdown after a short delay
        let shutdown_clone = shutdown.clone();
        compio::runtime::spawn(async move {
            compio::time::sleep(Duration::from_millis(10)).await;
            shutdown_clone.trigger();
        })
        .detach();

        // Should be interrupted
        let completed = token.sleep_or_shutdown(Duration::from_secs(10)).await;
        assert!(!completed);
    }

    #[compio::test]
    async fn test_child_token_parent_trigger() {
        let (parent_shutdown, parent_token) = Shutdown::new();
        let (_child_shutdown, combined_token) = parent_token.child();

        assert!(!combined_token.is_triggered());

        // Trigger parent shutdown
        parent_shutdown.trigger();

        // Combined token should be triggered
        combined_token.wait().await;
        assert!(combined_token.is_triggered());
    }

    #[compio::test]
    async fn test_child_token_child_trigger() {
        let (_parent_shutdown, parent_token) = Shutdown::new();
        let (child_shutdown, combined_token) = parent_token.child();

        assert!(!combined_token.is_triggered());

        // Trigger child shutdown
        child_shutdown.trigger();

        // Combined token should be triggered
        combined_token.wait().await;
        assert!(combined_token.is_triggered());
    }

    #[compio::test]
    async fn test_child_token_no_polling_overhead() {
        let (_parent_shutdown, parent_token) = Shutdown::new();
        let (_child_shutdown, combined_token) = parent_token.child();

        // Test that we can create many child tokens without performance issues
        let start = std::time::Instant::now();
        for _ in 0..100 {
            let _ = combined_token.child();
        }
        let elapsed = start.elapsed();

        // Should complete very quickly since there's no polling
        assert!(
            elapsed.as_millis() < 100,
            "Creating child tokens took too long: {:?}",
            elapsed
        );
    }
}
