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

//! Node-wide cap on open client sockets.
//!
//! Shard 0 accepts every client socket, but the owning shard closes it. Each
//! socket therefore carries a [`ConnectionPermit`] to its owning shard, and
//! the permit frees its slot on drop.
//!
//! The count is a process-wide static, because one node runs per process. A
//! permit that pointed at its count would add 16 bytes to the client setup
//! frames, and every slot of every shard inbox pays for the largest frame.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tracing::warn;

/// Shortest interval between two refusal warnings. A client that reconnects
/// in a loop would otherwise log one line per attempt.
const REFUSAL_LOG_INTERVAL: Duration = Duration::from_secs(10);

static LIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

/// The open client sockets of the node, capped at `max`.
///
/// Owned by shard 0, where every client accept runs.
#[derive(Debug)]
pub struct ConnectionCap {
    max: Option<usize>,
    last_refusal_log: Cell<Option<Instant>>,
    refused_since_log: Cell<u64>,
}

impl ConnectionCap {
    /// A cap of `max` open sockets. `None` counts sockets and refuses none.
    #[must_use]
    pub const fn new(max: Option<usize>) -> Self {
        Self {
            max,
            last_refusal_log: Cell::new(None),
            refused_since_log: Cell::new(0),
        }
    }

    #[must_use]
    pub const fn max(&self) -> Option<usize> {
        self.max
    }

    /// Sockets of this process that hold a permit now.
    #[must_use]
    pub fn live(&self) -> usize {
        LIVE_CONNECTIONS.load(Ordering::Relaxed)
    }

    /// Take a slot for a socket that was just accepted, or `None` at the
    /// cap. The caller closes a refused socket by dropping it.
    pub fn try_acquire(&self) -> Option<ConnectionPermit> {
        if let Some(max) = self.max {
            let admitted =
                LIVE_CONNECTIONS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                    (live < max).then_some(live + 1)
                });
            if let Err(live) = admitted {
                self.log_refusal(live, max);
                return None;
            }
        } else {
            LIVE_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
        }
        Some(ConnectionPermit { _private: () })
    }

    fn log_refusal(&self, live: usize, max: usize) {
        let refused = self.refused_since_log.get() + 1;
        let now = Instant::now();
        if self
            .last_refusal_log
            .get()
            .is_some_and(|last| now.duration_since(last) < REFUSAL_LOG_INTERVAL)
        {
            self.refused_since_log.set(refused);
            return;
        }
        self.last_refusal_log.set(Some(now));
        self.refused_since_log.set(0);
        warn!(
            live,
            max, refused, "client connection cap reached, closing new client connections"
        );
    }
}

/// The slot of one open client socket in the [`ConnectionCap`].
///
/// It travels with the socket to the owning shard and must drop only after
/// the socket closes: dropping it frees the slot.
#[derive(Debug)]
#[must_use = "dropping the permit frees the slot at once"]
pub struct ConnectionPermit {
    _private: (),
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        LIVE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // One test, because every cap shares the process-wide count and the test
    // harness runs tests in parallel.
    #[test]
    fn given_cap_when_sockets_open_and_close_should_count_every_permit() {
        let cap = ConnectionCap::new(Some(2));
        let first = cap.try_acquire().expect("the first slot is free");
        let second = cap.try_acquire().expect("the second slot is free");
        assert!(
            cap.try_acquire().is_none(),
            "the cap refuses a third socket"
        );
        assert_eq!(cap.live(), 2);

        drop(first);
        assert_eq!(cap.live(), 1);
        let third = cap.try_acquire().expect("a dropped permit frees its slot");

        std::thread::spawn(move || drop(second))
            .join()
            .expect("the dropping thread exits");
        assert_eq!(
            cap.live(),
            1,
            "a permit dropped on another shard frees its slot"
        );
        drop(third);

        let uncapped = ConnectionCap::new(None);
        let permits: Vec<_> = (0..1000).map(|_| uncapped.try_acquire()).collect();
        assert!(permits.iter().all(Option::is_some));
        assert_eq!(uncapped.live(), 1000, "no cap still counts");
        drop(permits);
        assert_eq!(uncapped.live(), 0);

        assert!(ConnectionCap::new(Some(0)).try_acquire().is_none());
        assert_eq!(cap.live(), 0);
    }
}
