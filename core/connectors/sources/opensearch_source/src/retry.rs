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

use iggy_connector_sdk::retry::{exponential_backoff, jitter, parse_retry_after};
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

pub(crate) const DEFAULT_MAX_RETRIES: u32 = 3;
pub(crate) const DEFAULT_RETRY_DELAY: &str = "1s";
pub(crate) const DEFAULT_RETRY_MAX_DELAY: &str = "30s";
pub(crate) const DEFAULT_MAX_OPEN_RETRIES: u32 = 5;
pub(crate) const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "30s";
pub(crate) const DEFAULT_CB_THRESHOLD: u32 = 5;
pub(crate) const DEFAULT_CB_COOL_DOWN: &str = "60s";

/// Total attempt count (minimum 1), consistent with InfluxDB connector config.
pub(crate) fn normalized_max_attempts(max_retries: u32) -> u32 {
    max_retries.max(1)
}

/// Returns true for HTTP status codes worth retrying: 429 and 5xx.
pub(crate) fn is_transient_status(status: u16) -> bool {
    status == 429 || (500..600).contains(&status)
}

pub(crate) struct RetryBackoff {
    pub delay: Duration,
    pub max_delay: Duration,
}

pub(crate) async fn sleep_before_retry(
    operation: &str,
    connector_id: u32,
    attempt: u32,
    max_attempts: u32,
    backoff: &RetryBackoff,
    retry_after: Option<&str>,
    reason: &str,
) {
    let delay = retry_after.and_then(parse_retry_after).unwrap_or_else(|| {
        jitter(exponential_backoff(
            backoff.delay,
            attempt,
            backoff.max_delay,
        ))
    });
    warn!(
        connector_id,
        operation,
        attempt,
        max_attempts,
        delay_ms = delay.as_millis(),
        reason,
        "OpenSearch request failed; retrying"
    );
    sleep(delay).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_429_or_5xx_when_check_transient_should_return_true() {
        assert!(is_transient_status(429));
        assert!(is_transient_status(503));
        assert!(!is_transient_status(404));
        assert!(!is_transient_status(400));
    }

    #[test]
    fn given_zero_max_retries_when_normalize_should_return_one() {
        assert_eq!(normalized_max_attempts(0), 1);
        assert_eq!(normalized_max_attempts(3), 3);
    }
}
