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

use std::num::NonZeroU32;

use ext_php_rs::{exception::PhpResult, php_class, php_enum, php_impl, types::Zval};
use iggy::prelude::{
    AutoCommit as RustAutoCommit, CommitAfter as RustCommitAfter, CommitEvery as RustCommitEvery,
    CommitPolicy as RustCommitPolicy, IggyDuration, PollOptions as RustPollOptions,
};

use crate::client::{non_zero_duration_micros, optional_unsigned};
use crate::error::to_php_exception;

/// When the offsets of the messages that a read returns are stored.
#[php_class]
#[php(name = "Iggy\\CommitPolicy")]
#[derive(Clone, Copy)]
pub struct CommitPolicy {
    pub(crate) inner: RustCommitPolicy,
}

#[php_impl]
impl CommitPolicy {
    /// Nothing is stored. The application commits checkpoints. The default.
    pub fn manual() -> Self {
        Self {
            inner: RustCommitPolicy::Manual,
        }
    }

    /// The server stores the offset of the last message of each read as part of the read: one
    /// round trip, at most once.
    pub fn on_read() -> Self {
        Self {
            inner: RustCommitPolicy::OnRead,
        }
    }

    /// A reader or a consumer stores the latest counted position, at least once with
    /// CommitAfter::Processing. pollMessages() refuses it with InvalidConfiguration (2).
    #[php(name = "auto")]
    pub fn automatic(auto: &AutoCommit) -> Self {
        Self {
            inner: RustCommitPolicy::Auto(auto.inner),
        }
    }

    /// `manual`, `on_read` or `auto`.
    #[php(getter)]
    pub fn kind(&self) -> &'static str {
        match self.inner {
            RustCommitPolicy::Manual => "manual",
            RustCommitPolicy::OnRead => "on_read",
            RustCommitPolicy::Auto(_) => "auto",
        }
    }
}

/// When CommitPolicy::auto() stores. It also stores on close, on shutdown and before a
/// rebalance takes a partition away. A store only moves forward.
#[php_class]
#[php(name = "Iggy\\AutoCommit")]
#[derive(Clone, Copy)]
pub struct AutoCommit {
    pub(crate) inner: RustAutoCommit,
}

#[php_impl]
impl AutoCommit {
    /// Stores the latest counted position every `interval_micros`, when it moved, and also on
    /// the event `every`. A message counts as `after` says, CommitAfter::Processing when null.
    /// Throws for a zero interval, and without an interval and an event, which would store
    /// only on close.
    #[php(constructor)]
    pub fn __construct(
        interval_micros: Option<&Zval>,
        every: Option<&CommitEvery>,
        after: Option<CommitAfter>,
    ) -> PhpResult<Self> {
        let interval = optional_unsigned::<u64>(interval_micros, "interval_micros")?
            .map(|micros| non_zero_duration_micros("interval_micros", micros))
            .transpose()?;
        let every = every.map(|every| every.inner);
        if interval.is_none() && every.is_none() {
            return Err(to_php_exception(
                "an AutoCommit needs an 'interval_micros' or an 'every' event",
            ));
        }
        Ok(Self {
            inner: RustAutoCommit {
                interval,
                every,
                after: after.map_or_else(RustCommitAfter::default, Into::into),
            },
        })
    }

    /// Every 10,000 messages or every 5 seconds, whichever comes first, after processing.
    #[php(name = "default")]
    pub fn defaults() -> Self {
        Self {
            inner: RustAutoCommit::default(),
        }
    }
}

/// The event on which CommitPolicy::auto() also stores.
#[php_class]
#[php(name = "Iggy\\CommitEvery")]
#[derive(Clone, Copy)]
pub struct CommitEvery {
    pub(crate) inner: RustCommitEvery,
}

#[php_impl]
impl CommitEvery {
    /// Each counted message, or each counted batch of a reader.
    pub fn message() -> Self {
        Self {
            inner: RustCommitEvery::Message,
        }
    }

    /// `count` counted messages of a partition since its last store. Throws for zero.
    pub fn messages(count: u32) -> PhpResult<Self> {
        let count = NonZeroU32::new(count).ok_or_else(|| {
            to_php_exception("'count' of CommitEvery::messages() must not be zero")
        })?;
        Ok(Self {
            inner: RustCommitEvery::Messages(count),
        })
    }

    /// The last message of each batch.
    pub fn batch() -> Self {
        Self {
            inner: RustCommitEvery::Batch,
        }
    }
}

/// When a message counts for CommitPolicy::auto().
#[php_enum]
#[php(name = "Iggy\\CommitAfter")]
#[derive(Clone, Copy)]
pub enum CommitAfter {
    /// When the application asks for the next message or batch, returns from its callback, or
    /// closes the reader. A callback that throws leaves its message uncounted.
    #[php(value = "processing")]
    Processing,
    /// When the message or the batch is handed over.
    #[php(value = "delivery")]
    Delivery,
    /// Only when the application calls mark() with its checkpoint.
    #[php(value = "marked")]
    Marked,
}

impl From<CommitAfter> for RustCommitAfter {
    fn from(value: CommitAfter) -> Self {
        match value {
            CommitAfter::Processing => Self::Processing,
            CommitAfter::Delivery => Self::Delivery,
            CommitAfter::Marked => Self::Marked,
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
    /// message, 1 MiB and CommitPolicy::manual().
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
        commit: Option<&CommitPolicy>,
    ) -> PhpResult<Self> {
        let defaults = RustPollOptions::default();
        let inner = RustPollOptions {
            count: optional_unsigned(count, "count")?.unwrap_or(defaults.count),
            max_wait: optional_unsigned::<u64>(max_wait_micros, "max_wait_micros")?
                .map_or(defaults.max_wait, IggyDuration::from),
            min_count: optional_unsigned(min_count, "min_count")?.unwrap_or(defaults.min_count),
            max_bytes: optional_unsigned(max_bytes, "max_bytes")?.unwrap_or(defaults.max_bytes),
            commit: commit.map_or(defaults.commit, |commit| commit.inner),
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

    #[php(getter)]
    pub fn commit(&self) -> CommitPolicy {
        CommitPolicy {
            inner: self.inner.commit,
        }
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
    fn given_auto_commit_parts_when_constructed_should_keep_them() {
        let every = CommitEvery::messages(7).unwrap();

        let auto = AutoCommit::__construct(None, Some(&every), Some(CommitAfter::Marked)).unwrap();

        assert_eq!(
            auto.inner,
            RustAutoCommit {
                interval: None,
                every: NonZeroU32::new(7).map(RustCommitEvery::Messages),
                after: RustCommitAfter::Marked,
            }
        );
        assert_eq!(AutoCommit::defaults().inner, RustAutoCommit::default());
    }
}
