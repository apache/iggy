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

//! Shared retry and resilience utilities for connector implementations.
//!
//! Provides:
//! - [`CircuitBreaker`] — consecutive-failure circuit breaker
//! - [`HttpRetryMiddleware`] — `reqwest-middleware` middleware with
//!   exponential back-off, jitter, and `Retry-After` header support
//! - [`build_retry_client`] — wraps a `reqwest::Client` with the middleware
//! - [`check_connectivity`] — single health-check probe (GET /health)
//! - [`check_connectivity_with_retry`] — startup probe with exponential backoff
//! - [`retry_async`] — generic retry loop for fallible async operations
//! - [`RetryFailure`] — terminal outcome of [`retry_async`], with attempt count
//! - [`RetryPolicy`] — attempt budget and backoff bounds for [`retry_async`]
//! - [`is_transient_status`] — transient HTTP status predicate
//! - [`parse_duration`] — humantime duration parsing with fallback
//! - [`exponential_backoff`] — capped exponential backoff
//! - [`retry_backoff`] — jittered backoff for a 1-based retry number
//! - [`parse_retry_after`] — HTTP `Retry-After` header parsing

use anyhow::anyhow;
use http::Extensions;
use humantime::Duration as HumanDuration;
use rand::RngExt as _;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware, Next};
use std::fmt;
use std::future::Future;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

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
pub(crate) fn jitter(base: Duration) -> Duration {
    let millis = base.as_millis() as u64;
    let jitter_range = millis / 5; // 20% of base
    if jitter_range == 0 {
        return base;
    }
    let delta = rand::rng().random_range(0..=jitter_range * 2);
    Duration::from_millis(millis.saturating_sub(jitter_range).saturating_add(delta))
}

/// True exponential backoff: `base × 2^attempt`, capped at `max_delay`.
///
/// `attempt` is 0-based. Retry loops count from 1, so passing their counter
/// here makes the first retry wait twice `base`; use [`retry_backoff`], which
/// takes a 1-based retry number and applies jitter and the cap.
pub fn exponential_backoff(base: Duration, attempt: u32, max_delay: Duration) -> Duration {
    let factor = 2u64.saturating_pow(attempt);
    let millis = base
        .as_millis()
        .saturating_mul(factor as u128)
        .min(max_delay.as_millis());
    Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
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
// Generic retry loop
// ---------------------------------------------------------------------------

/// Parameters for [`retry_async`].
///
/// `max_attempts` is a *total attempt count*, not a count of extra retries:
/// `1` runs the operation once and never retries, `3` allows two retries. `0`
/// behaves as `1`, so a misconfigured value degrades to a single attempt
/// rather than skipping the operation entirely.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl RetryPolicy {
    /// Jittered backoff before retry number `retry` (1-based). See
    /// [`retry_backoff`].
    pub fn backoff(&self, retry: u32) -> Duration {
        retry_backoff(self.base_delay, retry, self.max_delay)
    }
}

/// Jittered, capped backoff before retry number `retry` (1-based): the first
/// retry waits `base_delay`, the second `2 × base_delay`, and so on, which is
/// the convention the `retry_delay` config fields document.
///
/// The cap is re-applied after jittering, because ±20 % jitter on an
/// already-capped delay can otherwise land above `max_delay`, which the config
/// fields document as a strict upper bound.
///
/// Prefer [`retry_async`], which calls this for you. Reach for it directly
/// only in a loop that cannot be expressed as a retried `Result`.
pub fn retry_backoff(base_delay: Duration, retry: u32, max_delay: Duration) -> Duration {
    jitter(exponential_backoff(
        base_delay,
        retry.saturating_sub(1),
        max_delay,
    ))
    .min(max_delay)
}

/// Why [`retry_async`] stopped.
///
/// Carries the attempt count so a caller can write its own terminal log: the
/// helper owns the per-retry line, giving up is the caller's to report.
#[derive(Debug)]
pub struct RetryFailure<E> {
    pub error: E,
    /// Attempts actually made, including the first.
    pub attempts: u32,
    /// `true` when the attempt budget ran out, `false` when `should_retry`
    /// rejected the error and no further attempt was made.
    pub exhausted: bool,
}

impl<E> RetryFailure<E> {
    /// Discard the attempt bookkeeping and keep the underlying error.
    pub fn into_error(self) -> E {
        self.error
    }
}

// No `source()`: `Display` already prints the inner error, so returning it
// here repeats the same text in an error chain.
impl<E> std::error::Error for RetryFailure<E> where E: std::error::Error + 'static {}

impl<E: fmt::Display> fmt::Display for RetryFailure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = if self.exhausted {
            "ran out of attempts"
        } else {
            "hit a non-retryable error"
        };
        let plural = if self.attempts == 1 {
            "attempt"
        } else {
            "attempts"
        };
        write!(
            f,
            "{reason} after {} {plural}: {}",
            self.attempts, self.error
        )
    }
}

/// Run `operation`, retrying while it fails with an error `should_retry`
/// accepts and the attempt budget in `policy` is not exhausted.
///
/// This is the retry skeleton for connectors whose failures surface as `Err`,
/// including backends that report failure in-band (a 200 response carrying a
/// failure status in its body) and so cannot use [`HttpRetryMiddleware`],
/// which classifies on the status code alone.
///
/// `context` identifies the connector and operation in every log line this
/// emits, e.g. `"Doris sink ID 3 Stream Load (label=abc)"`. Callers build it
/// before the first attempt, so keep it off per-message paths.
///
/// The per-retry line is logged here. Giving up is not: the returned
/// [`RetryFailure`] carries the attempt count and which condition ended the
/// loop, so a caller logs the terminal failure at the level and wording that
/// suit it. The error itself is returned unchanged.
pub async fn retry_async<T, E, S, Op, Fut>(
    policy: RetryPolicy,
    context: &str,
    should_retry: S,
    mut operation: Op,
) -> Result<T, RetryFailure<E>>
where
    S: Fn(&E) -> bool,
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    let max_attempts = policy.max_attempts.max(1);
    let mut attempt = 0u32;

    loop {
        let error = match operation().await {
            Ok(value) => {
                if attempt > 0 {
                    let plural = if attempt == 1 { "retry" } else { "retries" };
                    info!("{context} succeeded after {attempt} {plural}.");
                }
                return Ok(value);
            }
            Err(error) => error,
        };

        attempt += 1;
        let retryable = should_retry(&error);
        let budget_spent = attempt >= max_attempts;
        if !retryable || budget_spent {
            return Err(RetryFailure {
                error,
                attempts: attempt,
                // A non-retryable error is the reason we stopped even when the
                // budget happened to run out on the same attempt.
                exhausted: retryable && budget_spent,
            });
        }

        let delay = policy.backoff(attempt);
        warn!(
            "{context} failed on attempt {attempt}/{max_attempts}: {error}. \
             Retrying in {delay:?}..."
        );
        tokio::time::sleep(delay).await;
    }
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
/// `max_retries` is a total attempt count, as described on [`RetryPolicy`].
///
/// Non-transient error responses (4xx except 429) are returned as-is so
/// callers can inspect the status and body to build a meaningful error.
///
/// The loop is hand-written rather than delegating to [`retry_async`] because
/// a retry here is driven by an `Ok(Response)` carrying a transient status,
/// and the final response is handed back to the caller instead of being turned
/// into an `Err`. Backoff still comes from [`RetryPolicy`], so the timing
/// matches every other connector retry: the first retry waits `retry_delay`.
#[derive(Debug, Clone)]
pub struct HttpRetryMiddleware {
    policy: RetryPolicy,
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
            policy: RetryPolicy {
                max_attempts: max_retries,
                base_delay: retry_delay,
                max_delay,
            },
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

                    // Parse Retry-After on 429 before falling back to our own
                    // calculated backoff.
                    let retry_after = if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        response
                            .headers()
                            .get("Retry-After")
                            .and_then(|value| value.to_str().ok())
                            .and_then(parse_retry_after)
                    } else {
                        None
                    };

                    attempts += 1;
                    if is_transient_status(status) && attempts < self.policy.max_attempts {
                        // Consume the error body for logging, then retry.
                        let body_text = response.text().await.unwrap_or_default();
                        let delay = retry_after.unwrap_or_else(|| self.policy.backoff(attempts));
                        warn!(
                            "{} transient error {status} \
                             (attempt {attempts}/{}): {body_text}. \
                             Retrying in {delay:?}...",
                            self.log_prefix, self.policy.max_attempts
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
                    if attempts < self.policy.max_attempts {
                        let delay = self.policy.backoff(attempts);
                        warn!(
                            "{} network error (attempt {attempts}/{}): {e}. \
                             Retrying in {delay:?}...",
                            self.log_prefix, self.policy.max_attempts
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
/// `connector_label` names the connector in log messages (e.g. `"InfluxDB sink"`).
/// `connector_id` is included in log messages for multi-instance deployments.
///
/// Startup usually wants a more patient policy than the per-request one (ten
/// attempts over a minute, say), so callers pass their own [`RetryPolicy`]
/// rather than reusing the one that governs live traffic.
pub async fn check_connectivity_with_retry(
    client: &reqwest::Client,
    url: reqwest::Url,
    connector_label: &str,
    connector_id: u32,
    policy: RetryPolicy,
) -> Result<(), crate::Error> {
    let context =
        format!("{connector_label} startup connectivity for connector ID: {connector_id}");

    retry_async(
        policy,
        &context,
        |_| true,
        || check_connectivity(client, url.clone(), connector_label),
    )
    .await
    .map_err(|failure| {
        // `open()`'s Err reaches the FFI boundary and is dropped there, so the
        // runtime logs only "Plugin initialization failed". Some callers log
        // the error again themselves.
        error!("{context} {failure}");
        failure.into_error()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::time::Instant;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const BASE: Duration = Duration::from_millis(100);
    const MAX: Duration = Duration::from_secs(10);

    // `jitter` is ±20 %, so every timing assertion below is a band around the
    // nominal delay rather than an equality.
    const JITTER_LOW: f64 = 0.8;
    const JITTER_HIGH: f64 = 1.2;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestError {
        Transient,
        Permanent,
    }

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Transient => write!(f, "transient"),
                Self::Permanent => write!(f, "permanent"),
            }
        }
    }

    fn should_retry(error: &TestError) -> bool {
        matches!(error, TestError::Transient)
    }

    fn policy(max_attempts: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            base_delay: BASE,
            max_delay: MAX,
        }
    }

    /// Operation that fails with `error` for the first `failures` calls and then
    /// succeeds, returning the 1-based number of the call that succeeded.
    fn failing_times(
        calls: &Cell<u32>,
        failures: u32,
        error: TestError,
    ) -> impl FnMut() -> std::future::Ready<Result<u32, TestError>> + '_ {
        move || {
            let call = calls.get() + 1;
            calls.set(call);
            std::future::ready(if call <= failures {
                Err(error)
            } else {
                Ok(call)
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_transient_failure_should_retry_until_it_succeeds() {
        let calls = Cell::new(0);
        let result = retry_async(
            policy(4),
            "test",
            should_retry,
            failing_times(&calls, 2, TestError::Transient),
        )
        .await;

        assert_eq!(result.map_err(RetryFailure::into_error), Ok(3));
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_permanent_failure_should_not_retry() {
        let calls = Cell::new(0);
        let result = retry_async(
            policy(4),
            "test",
            should_retry,
            failing_times(&calls, 2, TestError::Permanent),
        )
        .await;

        let failure = result.expect_err("permanent error should not retry");
        assert_eq!(failure.error, TestError::Permanent);
        assert_eq!(failure.attempts, 1);
        assert!(
            !failure.exhausted,
            "should_retry rejected it, budget untouched"
        );
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_permanent_failure_on_the_last_attempt_should_not_report_exhaustion() {
        // Budget of 1 makes the last attempt also the first, so both stop
        // conditions fire at once; the non-retryable one is the real reason.
        let calls = Cell::new(0);
        let result = retry_async(
            policy(1),
            "test",
            should_retry,
            failing_times(&calls, u32::MAX, TestError::Permanent),
        )
        .await;

        let failure = result.expect_err("permanent error should fail");
        assert!(
            !failure.exhausted,
            "reported exhaustion for an error that was never retryable"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_exhausted_budget_should_return_the_last_error() {
        let calls = Cell::new(0);
        // Distinct error per attempt, so "last" is actually discriminated.
        let result: Result<u32, RetryFailure<u32>> = retry_async(
            policy(3),
            "test",
            |_| true,
            || {
                let call = calls.get() + 1;
                calls.set(call);
                std::future::ready(Err(call))
            },
        )
        .await;

        let failure = result.expect_err("budget should be exhausted");
        assert_eq!(failure.error, 3, "should surface the final attempt's error");
        assert_eq!(failure.attempts, 3);
        assert!(failure.exhausted);
        assert_eq!(calls.get(), 3, "max_attempts is a total attempt count");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_single_attempt_budget_should_run_the_operation_once() {
        for max_attempts in [0, 1] {
            let calls = Cell::new(0);
            let result = retry_async(
                policy(max_attempts),
                "test",
                should_retry,
                failing_times(&calls, u32::MAX, TestError::Transient),
            )
            .await;

            let failure = result.expect_err("single attempt should fail");
            assert_eq!(failure.error, TestError::Transient);
            assert!(failure.exhausted, "max_attempts = {max_attempts}");
            assert_eq!(calls.get(), 1, "max_attempts = {max_attempts}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_successive_retries_should_double_the_delay() {
        let calls = Cell::new(0);
        let started = tokio::time::Instant::now();
        let result = retry_async(
            policy(3),
            "test",
            should_retry,
            failing_times(&calls, 2, TestError::Transient),
        )
        .await;
        let elapsed = started.elapsed();

        assert_eq!(result.map_err(RetryFailure::into_error), Ok(3));
        // base + 2 × base, each independently jittered. Passing the retry
        // number straight to `exponential_backoff` would give 2 + 4 × base.
        let nominal = BASE * 3;
        assert!(
            elapsed >= nominal.mul_f64(JITTER_LOW) && elapsed <= nominal.mul_f64(JITTER_HIGH),
            "two retries waited {elapsed:?}, expected roughly {nominal:?}"
        );
    }

    // `HttpRetryMiddleware` is the path every HTTP connector rides, and nothing
    // covered it before: these cases pin the first delay and that a server's
    // `Retry-After` outranks the computed backoff.
    fn retry_client(max_attempts: u32, base: Duration, max: Duration) -> ClientWithMiddleware {
        build_retry_client(reqwest::Client::new(), max_attempts, base, max, "test")
    }

    async fn mock_then_ok(status: u16, headers: &[(&str, &str)]) -> MockServer {
        let server = MockServer::start().await;
        let mut first = ResponseTemplate::new(status);
        for (name, value) in headers {
            first = first.insert_header(*name, *value);
        }
        Mock::given(method("GET"))
            .respond_with(first)
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn given_no_retry_after_should_wait_the_base_delay_on_the_first_retry() {
        let server = mock_then_ok(503, &[]).await;
        // 1s rather than 200ms so the pass band and the bug's band do not
        // overlap: the bug produces jitter(2s) in [1.6s, 2.4s], a correct run
        // produces jitter(1s) in [0.8s, 1.2s], and 1.4s separates them with
        // room for two loopback round trips.
        let base = Duration::from_secs(1);
        let client = retry_client(3, base, Duration::from_secs(30));

        let started = Instant::now();
        let response = client.get(server.uri()).send().await.unwrap();
        let elapsed = started.elapsed();

        assert_eq!(response.status(), 200);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            2,
            "expected exactly one retry"
        );
        assert!(
            elapsed >= base.mul_f64(JITTER_LOW),
            "first retry waited {elapsed:?}, expected roughly {base:?}"
        );
        // Guards the middleware's own `policy.backoff(attempts)` wiring, which
        // the pure `retry_backoff` tests do not reach: feeding a 1-based
        // counter to the 0-based `exponential_backoff` doubles this delay.
        // A correct run tops out at 1.2 x base and the bug starts at 1.6 x, so
        // 1.5 x leaves the widest margin for loopback round trips on a loaded
        // runner while still failing on the bug.
        assert!(
            elapsed < base.mul_f64(1.5),
            "first retry waited {elapsed:?}, past the {base:?} the config asks for"
        );
    }

    #[tokio::test]
    async fn given_a_retry_after_should_take_precedence_over_the_computed_backoff() {
        let server = mock_then_ok(429, &[("Retry-After", "1")]).await;
        // Backoff bounded far below the header, so honoring it is visible.
        let client = retry_client(3, Duration::from_millis(10), Duration::from_millis(50));

        let started = Instant::now();
        let response = client.get(server.uri()).send().await.unwrap();
        let elapsed = started.elapsed();

        assert_eq!(response.status(), 200);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            2,
            "expected exactly one retry"
        );
        assert!(
            elapsed >= Duration::from_millis(900),
            "used the computed backoff instead of Retry-After: waited {elapsed:?}"
        );
        // The header asks for 1s. Anything far past it means the middleware
        // added its own backoff on top instead of honoring the header.
        assert!(
            elapsed < Duration::from_millis(1500),
            "waited {elapsed:?}, past the 1s the header asked for"
        );
    }

    #[test]
    fn given_a_retry_number_should_back_off_from_the_base_delay() {
        let policy = policy(8);
        for (retry, factor) in [(1u32, 1.0), (2, 2.0), (3, 4.0), (4, 8.0)] {
            let delay = policy.backoff(retry);
            let nominal = BASE.mul_f64(factor);
            assert!(
                delay >= nominal.mul_f64(JITTER_LOW) && delay <= nominal.mul_f64(JITTER_HIGH),
                "retry {retry} backed off {delay:?}, expected roughly {nominal:?}"
            );
        }
    }

    #[test]
    fn given_jitter_on_a_capped_delay_should_never_exceed_max_delay() {
        // Base far above the cap, so every draw starts clamped and only jitter
        // could push it back over.
        let policy = RetryPolicy {
            max_attempts: 8,
            base_delay: Duration::from_secs(30),
            max_delay: Duration::from_secs(1),
        };
        for retry in 1..=8 {
            for _ in 0..64 {
                assert!(policy.backoff(retry) <= policy.max_delay);
            }
        }
    }
}
