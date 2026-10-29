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

use ext_php_rs::{exception::PhpResult, php_class, php_enum, php_impl, types::Zval};
use iggy::prelude::{
    IggyDuration, PollOptions as RustPollOptions, ServerCommitPolicy as RustServerCommitPolicy,
};

use crate::client::optional_unsigned;
use crate::error::to_php_exception;

/// Who stores the offset of a poll.
#[php_enum]
#[php(name = "Iggy\\ServerCommitPolicy")]
#[derive(Clone, Copy)]
pub enum ServerCommitPolicy {
    /// The application or the consumer commits.
    #[php(value = "disabled")]
    Disabled,
    /// The server stores the offset of the last message of a reply as part of the reply.
    #[php(value = "on_read")]
    OnRead,
}

impl From<ServerCommitPolicy> for RustServerCommitPolicy {
    fn from(value: ServerCommitPolicy) -> Self {
        match value {
            ServerCommitPolicy::Disabled => Self::Disabled,
            ServerCommitPolicy::OnRead => Self::OnRead,
        }
    }
}

impl From<RustServerCommitPolicy> for ServerCommitPolicy {
    fn from(value: RustServerCommitPolicy) -> Self {
        match value {
            RustServerCommitPolicy::Disabled => Self::Disabled,
            RustServerCommitPolicy::OnRead => Self::OnRead,
        }
    }
}

/// The limits of one poll of one partition.
#[php_class]
#[php(name = "Iggy\\PollOptions")]
#[derive(Clone, Copy)]
pub struct PollOptions {
    pub(crate) inner: RustPollOptions,
}

#[php_impl]
impl PollOptions {
    /// Every argument left null keeps its default: up to 1000 messages, no wait, at least 1
    /// message, 1 MiB and ServerCommitPolicy::Disabled.
    ///
    /// `max_wait_micros` is at most 600 seconds, and the server waits that long for
    /// `min_count` messages, which must be 1 to `count`. A reader or a consumer that waits 0
    /// polls an idle topic in a busy loop, so give it a wait. A reply holds at most
    /// `max_bytes`, and a message larger than that comes alone. Invalid limits are refused
    /// here, before anything is sent.
    #[php(constructor)]
    pub fn __construct(
        count: Option<&Zval>,
        max_wait_micros: Option<&Zval>,
        min_count: Option<&Zval>,
        max_bytes: Option<&Zval>,
        server_commit: Option<ServerCommitPolicy>,
    ) -> PhpResult<Self> {
        let defaults = RustPollOptions::default();
        let inner = RustPollOptions {
            count: optional_unsigned(count, "count")?.unwrap_or(defaults.count),
            max_wait: optional_unsigned::<u64>(max_wait_micros, "max_wait_micros")?
                .map_or(defaults.max_wait, IggyDuration::from),
            min_count: optional_unsigned(min_count, "min_count")?.unwrap_or(defaults.min_count),
            max_bytes: optional_unsigned(max_bytes, "max_bytes")?.unwrap_or(defaults.max_bytes),
            server_commit: server_commit.map_or(defaults.server_commit, Into::into),
        };
        inner.validate().map_err(to_php_exception)?;
        Ok(Self { inner })
    }

    #[php(getter)]
    pub fn count(&self) -> u32 {
        self.inner.count
    }

    #[php(getter, change_case = "snake_case")]
    pub fn max_wait_micros(&self) -> u64 {
        self.inner.max_wait.as_micros()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn min_count(&self) -> u32 {
        self.inner.min_count
    }

    #[php(getter, change_case = "snake_case")]
    pub fn max_bytes(&self) -> u32 {
        self.inner.max_bytes
    }

    #[php(getter, change_case = "snake_case")]
    pub fn server_commit(&self) -> ServerCommitPolicy {
        self.inner.server_commit.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_no_arguments_when_constructed_should_keep_the_sdk_defaults() {
        let options = PollOptions::__construct(None, None, None, None, None).unwrap();

        assert_eq!(options.inner, RustPollOptions::default());
    }

    #[test]
    fn server_commit_policy_round_trips_every_case() {
        for policy in [
            RustServerCommitPolicy::Disabled,
            RustServerCommitPolicy::OnRead,
        ] {
            assert_eq!(
                RustServerCommitPolicy::from(ServerCommitPolicy::from(policy)),
                policy
            );
        }
    }
}
