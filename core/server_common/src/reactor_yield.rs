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

//! A yield that is guaranteed to suspend.
//!
//! Long CPU passes (recovery walks, artifact hashing) hand the core back to
//! the reactor by awaiting a short timer. The runtime's timer wheel registers
//! a timer only when its deadline is still in the future at the wheel's OWN
//! clock re-read, and its sleep future completes on the first poll without
//! ever suspending when registration is refused -- so a fixed short duration
//! is a race against the code path between the two clock reads. A bare 1 us
//! sleep wins that race ~99.8% of the time even on a cold debug build, but
//! the losses are silent: at the walk's ~512 refills per GiB a 2e-3 loss
//! rate skips a yield every few GiB and nothing notices, because the pass
//! still completes. No constant wins the race deterministically; the only
//! deterministic shape is to retry with a growing duration until one
//! registration wins.

use std::time::Duration;
use tracing::error;

/// First attempted timer duration. The common case: on a warm path one
/// microsecond outlives the registration window and the first attempt wins.
const YIELD_FIRST_ATTEMPT: Duration = Duration::from_micros(1);

/// Ceiling for the attempt doubling, so no single attempt parks the caller
/// for more than a second even under a wildly stalling clock.
const YIELD_ATTEMPT_CAP: Duration = Duration::from_secs(1);

/// Attempts before giving up. Reaching it takes the clock advancing by the
/// attempted duration between `sleep()`'s deadline capture and the wheel's
/// re-read, 32 consecutive times with doubling durations -- a broken timer
/// runtime, not a lost race. An unyieldable reactor must cost throughput,
/// never wedge the boot, so the loop is bounded and the terminal case
/// degrades to not yielding at all, loudly.
const YIELD_MAX_ATTEMPTS: u32 = 32;

/// Hands the core back to the reactor: a poll of a REGISTERED timer returns
/// `Pending`, so awaiting it suspends, on any machine, by construction.
///
/// A registered timer with a near-now deadline fires on the reactor's next
/// turn, so the attempted duration barely throttles the caller at the 1 us
/// first attempt (~12 us measured per yield); it does throttle roughly
/// linearly once attempts grow past ~10 us, which only a lost race causes. A
/// bare self-waking yield is no alternative: this runtime does not reliably
/// re-poll a task that wakes itself from inside its own poll, and a task
/// parked that way may never resume.
pub async fn yield_to_reactor() {
    let mut attempt = YIELD_FIRST_ATTEMPT;
    for _ in 0..YIELD_MAX_ATTEMPTS {
        let mut timer = std::pin::pin!(compio::time::sleep(attempt));
        if futures::poll!(timer.as_mut()).is_pending() {
            timer.await;
            return;
        }
        attempt = (attempt * 2).min(YIELD_ATTEMPT_CAP);
    }
    error!(
        "no timer registration won in {YIELD_MAX_ATTEMPTS} attempts; \
         continuing without yielding to the reactor"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Waker};

    // Pins the suspension point itself: a yield whose future is Ready on its
    // first poll never hands the core back, and nothing else would notice
    // (callers still finish their pass, just without ever suspending).
    #[compio::test]
    async fn given_yield_future_when_polled_once_should_be_pending() {
        let mut future = std::pin::pin!(yield_to_reactor());
        let mut context = Context::from_waker(Waker::noop());
        assert!(
            future.as_mut().poll(&mut context).is_pending(),
            "the first poll must register a real timer instead of completing inline"
        );
    }

    #[compio::test]
    async fn given_yield_future_when_awaited_should_complete() {
        yield_to_reactor().await;
    }
}
