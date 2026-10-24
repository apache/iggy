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

use std::time::Duration;

use crate::{
    IggyDuration, IggyError, MAX_DEFERRED_POLL_TIMEOUT_US, MAX_DEFERRED_POLL_WAIT_US,
    MIN_DEFERRED_POLL_MAX_BYTES,
};
use iggy_binary_protocol::requests::messages::{
    DeferredPollMessagesRequest, deferred_timeout_covers_wait,
};

pub const DEFAULT_POLL_MAX_BYTES: u32 = 1024 * 1024;
pub const DEFAULT_POLL_REQUEST_TIMEOUT: Duration = Duration::from_secs(11);

/// Readiness and response limits, independent of the maximum message count.
/// The wait bounds batching delay; the transport's request timeout also allows
/// bounded storage and network work. A request that waits must leave
/// [`crate::MIN_DEFERRED_POLL_REPLY_MARGIN_US`] between its wait and its
/// timeout for the final read and the reply. Byte limits include the binary
/// response metadata and batch framing, including for HTTP's equivalent
/// selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeferredPollOptions {
    pub max_wait: IggyDuration,
    pub min_count: u32,
    pub max_bytes: u32,
    pub request_timeout: IggyDuration,
}

impl Default for DeferredPollOptions {
    fn default() -> Self {
        Self {
            max_wait: IggyDuration::ONE_SECOND,
            min_count: 1,
            max_bytes: DEFAULT_POLL_MAX_BYTES,
            request_timeout: DEFAULT_POLL_REQUEST_TIMEOUT.into(),
        }
    }
}

impl DeferredPollOptions {
    pub fn validate(&self, count: u32) -> Result<(), IggyError> {
        if self.min_count == 0 || self.min_count > count {
            return Err(IggyError::InvalidMessagesCount);
        }
        // Compare durations first: microseconds above u64 wrap on the wire.
        let max_wait = self.max_wait.get_duration();
        let request_timeout = self.request_timeout.get_duration();
        if max_wait > Duration::from_micros(MAX_DEFERRED_POLL_WAIT_US)
            || request_timeout < Duration::from_micros(1)
            || request_timeout > Duration::from_micros(MAX_DEFERRED_POLL_TIMEOUT_US)
            || !deferred_timeout_covers_wait(
                self.max_wait.as_micros(),
                self.request_timeout.as_micros(),
            )
        {
            return Err(IggyError::InvalidCommand);
        }
        if (self.max_bytes as usize) < MIN_DEFERRED_POLL_MAX_BYTES {
            return Err(IggyError::InvalidSizeBytes);
        }
        Ok(())
    }

    /// Preserve one client budget across assignment refreshes and routing retries.
    /// Both limits shrink by `elapsed`, so a wait keeps its reply margin.
    ///
    /// # Errors
    /// Returns `TransientNotAccepted` once the budget is spent, because this
    /// attempt never left the client.
    pub fn remaining(self, elapsed: Duration) -> Result<Self, IggyError> {
        let request_timeout = self.request_timeout.get_duration().saturating_sub(elapsed);
        if request_timeout.as_micros() == 0 {
            return Err(IggyError::TransientNotAccepted);
        }
        Ok(Self {
            max_wait: self.max_wait.get_duration().saturating_sub(elapsed).into(),
            request_timeout: request_timeout.into(),
            ..self
        })
    }
}

impl From<&DeferredPollMessagesRequest> for DeferredPollOptions {
    fn from(request: &DeferredPollMessagesRequest) -> Self {
        Self {
            max_wait: request.wait_us.into(),
            min_count: request.min_count,
            max_bytes: request.max_bytes,
            request_timeout: request.request_timeout_us.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MIN_DEFERRED_POLL_REPLY_MARGIN_US;

    fn options(max_wait: Duration, request_timeout: Duration) -> DeferredPollOptions {
        DeferredPollOptions {
            max_wait: max_wait.into(),
            request_timeout: request_timeout.into(),
            ..Default::default()
        }
    }

    #[test]
    fn given_wait_when_validating_should_require_the_reply_margin_before_the_timeout() {
        let margin = Duration::from_micros(MIN_DEFERRED_POLL_REPLY_MARGIN_US);
        let wait = Duration::from_secs(2);
        assert!(DeferredPollOptions::default().validate(1).is_ok());
        assert!(options(wait, wait + margin).validate(1).is_ok());
        assert!(
            options(Duration::ZERO, Duration::from_micros(1))
                .validate(1)
                .is_ok()
        );
        for invalid in [
            options(wait, wait + margin - Duration::from_micros(1)),
            options(wait, wait),
            options(Duration::ZERO, Duration::from_nanos(999)),
            options(Duration::from_secs(u64::MAX), Duration::from_secs(u64::MAX)),
        ] {
            assert!(
                matches!(invalid.validate(1), Err(IggyError::InvalidCommand)),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn given_spent_budget_when_retrying_should_report_a_request_that_never_left() {
        let options = options(Duration::from_secs(1), Duration::from_secs(3));
        let remaining = options.remaining(Duration::from_secs(2)).unwrap();
        assert_eq!(remaining.max_wait.get_duration(), Duration::ZERO);
        assert!(remaining.validate(1).is_ok());
        assert!(matches!(
            options.remaining(Duration::from_secs(3)),
            Err(IggyError::TransientNotAccepted)
        ));
    }
}
