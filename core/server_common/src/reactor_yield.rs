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
//! is a race against the code path between the two clock reads, and the
//! window is machine- and build-dependent (a debug-build cold path loses a
//! 1 us head start essentially always). No constant wins that race; the only
//! deterministic shape is to retry with a growing duration until one
//! registration wins.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// First attempted timer duration. The common case: on a warm path one
/// microsecond outlives the registration window and the first attempt wins.
const YIELD_FIRST_ATTEMPT: Duration = Duration::from_micros(1);

/// Ceiling for the attempt doubling. Reaching it would mean a whole-second
/// deadline was already in the past by the time the wheel re-read the clock:
/// a broken or frozen clock, not a lost race. Registration is guaranteed
/// long before; the cap only keeps the retry loop's growth finite.
const YIELD_ATTEMPT_CAP: Duration = Duration::from_secs(1);

/// Hands the core back to the reactor: the first poll ALWAYS returns
/// `Pending` with a real timer registered, on any machine, by construction.
///
/// A registered timer with a near-now deadline fires on the reactor's next
/// turn, so the attempted duration does not throttle the caller; it only has
/// to be long enough to register. A bare self-waking yield is no
/// alternative: this runtime does not reliably re-poll a task that wakes
/// itself from inside its own poll, and a task parked that way may never
/// resume.
pub async fn yield_to_reactor() {
    RegisteredYield {
        registered: None,
        attempt: YIELD_FIRST_ATTEMPT,
    }
    .await;
}

struct RegisteredYield {
    /// The timer that won registration; later polls delegate to it.
    registered: Option<Pin<Box<dyn Future<Output = ()>>>>,
    attempt: Duration,
}

impl Future for RegisteredYield {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if let Some(timer) = this.registered.as_mut() {
            return timer.as_mut().poll(context);
        }
        loop {
            // One allocation per attempt; at the callers' once-per-window
            // cadence that is noise against the work being yielded from.
            let mut timer: Pin<Box<dyn Future<Output = ()>>> =
                Box::pin(compio::time::sleep(this.attempt));
            if timer.as_mut().poll(context).is_pending() {
                this.registered = Some(timer);
                return Poll::Pending;
            }
            debug_assert!(
                this.attempt < YIELD_ATTEMPT_CAP,
                "a {:?} timer deadline was already in the past at registration; \
                 the runtime clock is broken or frozen",
                this.attempt
            );
            this.attempt = (this.attempt * 2).min(YIELD_ATTEMPT_CAP);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Waker;

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
