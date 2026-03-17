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

//! Shared retry and resilience utilities for connector implementations.
//!
//! Provides:
//! - [`CircuitBreaker`] — consecutive-failure circuit breaker
//! - [`parse_duration`] — humantime duration parsing with fallback
//! - [`jitter`] — ±20 % random jitter for retry delays
//! - [`exponential_backoff`] — capped exponential backoff
//! - [`parse_retry_after`] — HTTP `Retry-After` header parsing

use humantime::Duration as HumanDuration;
use rand::RngExt as _;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Circuit breaker
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CircuitState {
    consecutive_failures: u32,
    open_until: Option<tokio::time::Instant>,
}

/// A simple consecutive-failure circuit breaker.
///
/// All mutable state is held under a single [`Mutex`] so that
/// `consecutive_failures` and `open_until` are always updated atomically,
/// preventing races between concurrent `record_failure` / `is_open` callers.
#[derive(Debug)]
pub struct CircuitBreaker {
    threshold: u32,
    cool_down: Duration,
    state: Mutex<CircuitState>,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cool_down: Duration) -> Self {
        Self {
            threshold,
            cool_down,
            state: Mutex::new(CircuitState {
                consecutive_failures: 0,
                open_until: None,
            }),
        }
    }

    /// Called on every successful operation — resets the failure counter and
    /// closes the circuit atomically.
    ///
    /// Uses `try_lock` so success never blocks on the hot path; at worst one
    /// extra failure is needed to re-open an already-closing circuit.
    pub fn record_success(&self) {
        if let Ok(mut s) = self.state.try_lock() {
            s.consecutive_failures = 0;
            s.open_until = None;
        }
    }

    /// Called after all retries for one operation have failed. May open the
    /// circuit once the failure count reaches the configured threshold.
    pub async fn record_failure(&self) {
        let mut s = self.state.lock().await;
        s.consecutive_failures = s.consecutive_failures.saturating_add(1);
        if s.consecutive_failures >= self.threshold {
            let deadline = tokio::time::Instant::now() + self.cool_down;
            s.open_until = Some(deadline);
            warn!(
                "Circuit breaker OPENED after {} consecutive failures. \
                 Pausing for {:?}.",
                s.consecutive_failures, self.cool_down
            );
        }
    }

    /// Returns `true` if the circuit is open (callers should skip the
    /// operation). Transitions to half-open automatically once the cool-down
    /// has elapsed.
    pub async fn is_open(&self) -> bool {
        let mut s = self.state.lock().await;
        match s.open_until {
            None => false,
            Some(deadline) if tokio::time::Instant::now() < deadline => true,
            Some(_) => {
                // Cool-down elapsed: half-open — let one probe through.
                s.open_until = None;
                s.consecutive_failures = 0;
                info!("Circuit breaker entering HALF-OPEN state.");
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Duration / backoff helpers
// ---------------------------------------------------------------------------

/// Parse a human-readable duration string (e.g. `"5s"`, `"1m30s"`) using
/// [`humantime`]. Falls back to 1 second if parsing fails.
pub fn parse_duration(value: Option<&str>, default_value: &str) -> Duration {
    let raw = value.unwrap_or(default_value);
    HumanDuration::from_str(raw)
        .map(|d| d.into())
        .unwrap_or_else(|_| Duration::from_secs(1))
}

/// Apply ±20 % random jitter to `base` to spread retry storms.
pub fn jitter(base: Duration) -> Duration {
    let millis = base.as_millis() as u64;
    let jitter_range = millis / 5; // 20% of base
    if jitter_range == 0 {
        return base;
    }
    let delta = rand::rng().random_range(0..=jitter_range * 2);
    Duration::from_millis(millis.saturating_sub(jitter_range).saturating_add(delta))
}

/// True exponential backoff: `base × 2^attempt`, capped at `max_delay`.
pub fn exponential_backoff(base: Duration, attempt: u32, max_delay: Duration) -> Duration {
    let factor = 2u64.saturating_pow(attempt);
    let millis = base
        .as_millis()
        .saturating_mul(factor as u128)
        .min(max_delay.as_millis());
    let millis_u64 = u64::try_from(millis).unwrap_or(u64::MAX);
    Duration::from_millis(millis_u64)
}

/// Parse a `Retry-After` header value (integer seconds).
/// Returns `None` for HTTP-date values — callers should fall back to their
/// own backoff strategy.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    if let Ok(secs) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    None
}
