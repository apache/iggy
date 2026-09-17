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

use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::{IggyDuration, IggyError, MAX_DEFERRED_POLL_WAIT_US};
use iggy_binary_protocol::requests::messages::DeferredPollMessagesRequest;
use iggy_binary_protocol::requests::messages::poll_messages::MAX_DEFERRED_POLL_TIMEOUT_US;

pub const DEFAULT_POLL_MAX_BYTES: u32 = 1024 * 1024;
pub const DEFAULT_POLL_REQUEST_TIMEOUT: Duration = Duration::from_secs(11);

/// Readiness and response limits, independent of the maximum message count.
/// The wait bounds batching delay; the transport's request timeout also allows
/// bounded storage and network work. Byte limits include the binary response
/// metadata and batch framing, including for HTTP's equivalent selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
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
        if self.max_wait.as_micros() > MAX_DEFERRED_POLL_WAIT_US
            || self.request_timeout.is_zero()
            || self.request_timeout.as_micros() > MAX_DEFERRED_POLL_TIMEOUT_US
            || self.max_wait.get_duration() > self.request_timeout.get_duration()
        {
            return Err(IggyError::InvalidCommand);
        }
        if self.max_bytes
            < iggy_binary_protocol::responses::messages::poll_messages::POLL_RESPONSE_HEADER_SIZE
                as u32
        {
            return Err(IggyError::InvalidSizeBytes);
        }
        Ok(())
    }

    /// Preserve one client budget across assignment refreshes and routing retries.
    pub fn remaining(self, elapsed: Duration) -> Result<Self, IggyError> {
        let request_timeout = self.request_timeout.get_duration().saturating_sub(elapsed);
        if request_timeout.as_micros() == 0 {
            return Err(IggyError::TransientNotCommitted);
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
