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
//! - [`HttpRetryMiddleware`] — `reqwest-middleware` middleware with
//!   exponential back-off, jitter, and `Retry-After` header support
//! - [`build_retry_client`] — wraps a `reqwest::Client` with the middleware
//! - [`check_connectivity`] — single health-check probe (GET /health)
//! - [`check_connectivity_with_retry`] — startup probe with exponential backoff
//! - [`ConnectivityConfig`] — parameters for the startup retry loop
//! - [`is_transient_status`] — transient HTTP status predicate
//! - [`parse_duration`] — humantime duration parsing with fallback
//! - [`jitter`] — ±20 % random jitter for retry delays
//! - [`exponential_backoff`] — capped exponential backoff
//! - [`parse_retry_after`] — HTTP `Retry-After` header parsing

use anyhow::anyhow;
use http::Extensions;
use humantime::Duration as HumanDuration;
use rand::RngExt as _;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware, Next};
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
/// [`humantime`]. Falls back to 1 second if parsing fails, and emits a
/// `warn!` so misconfigured values (e.g. `"5sec"` instead of `"5s"`) are
/// visible in logs rather than silently causing unexpected retry timing.
pub fn parse_duration(value: Option<&str>, default_value: &str) -> Duration {
    let raw = value.unwrap_or(default_value);
    HumanDuration::from_str(raw)
        .map(|d| d.into())
        .unwrap_or_else(|e| {
            // Only warn when the caller supplied a bad value; a bad
            // default_value is a programming error caught in tests, not a
            // runtime config issue worth alarming operators about.
            if value.is_some() {
                warn!(
                    "Invalid duration {:?}: {e}. Falling back to 1s. \
                     Use humantime format, e.g. \"5s\", \"1m30s\", \"200ms\".",
                    raw
                );
            }
            Duration::from_secs(1)
        })
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

// ---------------------------------------------------------------------------
// reqwest-middleware retry implementation
// ---------------------------------------------------------------------------

/// Returns `true` for HTTP status codes that are worth retrying:
/// `429 Too Many Requests` and all `5xx` server errors.
pub fn is_transient_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

/// Per-request retry middleware for HTTP connectors.
///
/// Wraps a `reqwest-middleware` stack and retries transient failures
/// (HTTP 429, 5xx, network errors) with exponential back-off and ±20 % jitter.
/// A `Retry-After` response header on a 429 overrides the calculated delay.
///
/// The `log_prefix` parameter identifies the connector in log messages
/// (e.g. `"InfluxDB"`, `"Elasticsearch"`), allowing this middleware to be
/// reused across connectors without misleading log output.
///
/// The `max_retries` parameter is the *total attempt count* (not the number
/// of extra attempts), consistent with the rest of the connector retry config:
/// - `max_retries = 1` → one attempt, no retries on failure
/// - `max_retries = 3` → up to three attempts (two retries after a failure)
///
/// Non-transient error responses (4xx except 429) are returned as-is so
/// callers can inspect the status and body to build a meaningful error.
#[derive(Debug, Clone)]
pub struct HttpRetryMiddleware {
    max_retries: u32,
    retry_delay: Duration,
    max_delay: Duration,
    log_prefix: &'static str,
}

impl HttpRetryMiddleware {
    pub fn new(
        max_retries: u32,
        retry_delay: Duration,
        max_delay: Duration,
        log_prefix: &'static str,
    ) -> Self {
        Self {
            max_retries,
            retry_delay,
            max_delay,
            log_prefix,
        }
    }
}

#[async_trait::async_trait]
impl Middleware for HttpRetryMiddleware {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let mut current_req = req;
        let mut attempts = 0u32;

        loop {
            // Clone before consuming — Bytes / JSON bodies are reference-counted
            // so this is O(1), not a deep copy of the payload.
            let next_req = current_req.try_clone();

            match next.clone().run(current_req, extensions).await {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        return Ok(response);
                    }

                    // Parse Retry-After header on 429 before falling back to
                    // our own calculated backoff.
                    let retry_after = if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        response
                            .headers()
                            .get("Retry-After")
                            .and_then(|v| v.to_str().ok())
                            .and_then(parse_retry_after)
                    } else {
                        None
                    };

                    attempts += 1;
                    if is_transient_status(status) && attempts < self.max_retries {
                        // Consume the error body for logging, then retry.
                        let body_text = response.text().await.unwrap_or_default();
                        let delay = retry_after.unwrap_or_else(|| {
                            jitter(exponential_backoff(
                                self.retry_delay,
                                attempts,
                                self.max_delay,
                            ))
                        });
                        warn!(
                            "{} transient error {status} \
                             (attempt {attempts}/{}): {body_text}. \
                             Retrying in {delay:?}...",
                            self.log_prefix, self.max_retries
                        );
                        tokio::time::sleep(delay).await;
                        current_req = match next_req {
                            Some(r) => r,
                            None => {
                                return Err(reqwest_middleware::Error::Middleware(anyhow!(
                                    "request body is not cloneable — cannot retry"
                                )));
                            }
                        };
                        continue;
                    }

                    // Non-transient status or retries exhausted — pass the
                    // response through so the caller can read the body and
                    // build a meaningful error message.
                    return Ok(response);
                }
                Err(e) => {
                    attempts += 1;
                    if attempts < self.max_retries {
                        let delay = jitter(exponential_backoff(
                            self.retry_delay,
                            attempts,
                            self.max_delay,
                        ));
                        warn!(
                            "{} network error (attempt {attempts}/{}): {e}. \
                             Retrying in {delay:?}...",
                            self.log_prefix, self.max_retries
                        );
                        tokio::time::sleep(delay).await;
                        current_req = match next_req {
                            Some(r) => r,
                            None => return Err(e),
                        };
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }
}

/// Wrap a raw [`reqwest::Client`] in a [`ClientWithMiddleware`] that
/// automatically retries transient HTTP failures.
///
/// The `log_prefix` parameter is included in all retry log messages to
/// identify which connector is retrying (e.g. `"InfluxDB"`, `"Elasticsearch"`).
///
/// The middleware uses the same `max_retries` semantics as the rest of the
/// connector retry config (total attempt count, not number of extra retries).
pub fn build_retry_client(
    client: reqwest::Client,
    max_retries: u32,
    retry_delay: Duration,
    max_delay: Duration,
    log_prefix: &'static str,
) -> ClientWithMiddleware {
    ClientBuilder::new(client)
        .with(HttpRetryMiddleware::new(
            max_retries,
            retry_delay,
            max_delay,
            log_prefix,
        ))
        .build()
}

// ---------------------------------------------------------------------------
// Shared connectivity helper
// ---------------------------------------------------------------------------

/// Configuration for the startup connectivity retry loop.
///
/// This is intentionally separate from per-request retry config so that
/// startup can wait patiently for a service (e.g. 10 retries over 60 s)
/// without affecting the shorter per-request retry window used during
/// normal operation.
pub struct ConnectivityConfig {
    pub max_open_retries: u32,
    pub open_retry_max_delay: Duration,
    pub retry_delay: Duration,
}

/// Probe `url` with a plain GET and return `Ok(())` if the response is 2xx.
///
/// This is a single, non-retried attempt. The caller is responsible for the
/// outer retry loop (see [`check_connectivity_with_retry`]).
pub async fn check_connectivity(
    client: &reqwest::Client,
    url: reqwest::Url,
    connector_label: &str,
) -> Result<(), crate::Error> {
    let response = client.get(url).send().await.map_err(|e| {
        crate::Error::Connection(format!("{connector_label} health check failed: {e}"))
    })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "failed to read response body".to_string());
        return Err(crate::Error::Connection(format!(
            "{connector_label} health check returned status {status}: {body}"
        )));
    }
    Ok(())
}

/// Retry [`check_connectivity`] with exponential backoff + jitter.
///
/// `connector_label` is used in log messages (e.g. `"InfluxDB sink connector ID: 1"`).
/// `connector_id` is included in log messages for multi-instance deployments.
pub async fn check_connectivity_with_retry(
    client: &reqwest::Client,
    url: reqwest::Url,
    connector_label: &str,
    connector_id: u32,
    cfg: &ConnectivityConfig,
) -> Result<(), crate::Error> {
    let max_open_retries = cfg.max_open_retries.max(1);
    let mut attempt = 0u32;

    loop {
        match check_connectivity(client, url.clone(), connector_label).await {
            Ok(()) => {
                if attempt > 0 {
                    tracing::info!(
                        "{connector_label} connectivity established after {attempt} retries \
                         for connector ID: {connector_id}"
                    );
                }
                return Ok(());
            }
            Err(e) => {
                attempt += 1;
                if attempt >= max_open_retries {
                    tracing::error!(
                        "{connector_label} connectivity check failed after {attempt} attempts \
                         for connector ID: {connector_id}. Giving up: {e}"
                    );
                    return Err(e);
                }
                let backoff = jitter(exponential_backoff(
                    cfg.retry_delay,
                    attempt,
                    cfg.open_retry_max_delay,
                ));
                tracing::warn!(
                    "{connector_label} health check failed \
                     (attempt {attempt}/{max_open_retries}) \
                     for connector ID: {connector_id}. Retrying in {backoff:?}: {e}"
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }
}
