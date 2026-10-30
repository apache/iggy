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
use std::time::Duration;

use crate::{
    DEFAULT_POLL_MAX_BYTES, DeferredPollOptions, IggyDuration, IggyError, NonZeroIggyDuration,
};

const DEFAULT_POLL_COUNT: u32 = 1000;
const DEFAULT_POLL_MIN_COUNT: u32 = 1;
/// Keeps the 1 s reply gap of the protocol and today's 11 s default (1 s wait plus 10 s).
const POLL_REQUEST_TIMEOUT_MARGIN: Duration = Duration::from_secs(10);
const DEFAULT_AUTO_COMMIT_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_AUTO_COMMIT_MESSAGES: NonZeroU32 = NonZeroU32::MIN.saturating_add(9_999);

/// When the offsets of the messages that a read returns are stored.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CommitPolicy {
    /// Nothing is stored. The application commits checkpoints.
    #[default]
    Manual,
    /// The server stores the offset of the last message of each read as part of the read, in one
    /// round trip. While the primary stays up a message is delivered at most once. A failover
    /// before the store replicates can deliver the last batch again.
    OnRead,
    /// The client stores the latest counted position, at least once with
    /// [`CommitAfter::Processing`]. A one-shot poll refuses it with
    /// [`IggyError::InvalidConfiguration`], because it has nothing to count. A partition reader
    /// and a consumer accept it.
    Auto(AutoCommit),
}

/// When [`CommitPolicy::Auto`] stores. It needs `interval` or `every`. It also stores on close,
/// on shutdown and before a rebalance takes a partition away. A store only moves forward, and a
/// position of an older generation of its partition, after a seek or a new assignment, is
/// dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoCommit {
    /// Store the latest counted position at this interval, when it moved.
    pub interval: Option<NonZeroIggyDuration>,
    /// Also store it on this event.
    pub every: Option<CommitEvery>,
    /// When a message counts.
    pub after: CommitAfter,
}

/// The event of [`AutoCommit::every`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitEvery {
    /// Each counted message, or each counted batch of a partition reader.
    Message,
    /// This many counted messages of a partition since its last store.
    Messages(NonZeroU32),
    /// The last message of each batch.
    Batch,
}

/// When a message counts for [`CommitPolicy::Auto`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CommitAfter {
    /// Once the application asks for the next message or batch, returns from its handler with
    /// success, or closes the reader. A handler that fails leaves its message uncounted.
    #[default]
    Processing,
    /// When the message is handed over.
    Delivery,
    /// Only when the application marks its checkpoint, so messages processed concurrently count
    /// once they are done.
    Marked,
}

/// The limits of one read: a poll of one partition, or a fetch that a partition reader sends
/// for several partitions of one primary. A fetch applies `count`, `min_count` and `max_bytes`
/// to all of its partitions together. A reader runs several fetches at once, each within these
/// limits, for partitions with more messages waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollOptions {
    /// Most messages in one read, over all partitions of a fetch. Default 1000.
    pub count: u32,
    /// Zero answers at once. At most 600 s.
    pub max_wait: IggyDuration,
    /// At least 1 and at most `count`, over all partitions of a fetch. Default 1.
    pub min_count: u32,
    /// Default 1 MiB, over all partitions of a fetch. A first message larger than `max_bytes`
    /// comes alone in the reply, so a read always makes progress.
    pub max_bytes: u32,
    /// Default [`CommitPolicy::Manual`].
    pub commit: CommitPolicy,
}

impl Default for PollOptions {
    fn default() -> Self {
        Self {
            count: DEFAULT_POLL_COUNT,
            max_wait: Duration::ZERO.into(),
            min_count: DEFAULT_POLL_MIN_COUNT,
            max_bytes: DEFAULT_POLL_MAX_BYTES,
            commit: CommitPolicy::Manual,
        }
    }
}

/// Every 10,000 messages or 5 s, whichever comes first, after [`CommitAfter::Processing`].
impl Default for AutoCommit {
    fn default() -> Self {
        Self {
            interval: NonZeroIggyDuration::new(DEFAULT_AUTO_COMMIT_INTERVAL).ok(),
            every: Some(CommitEvery::Messages(DEFAULT_AUTO_COMMIT_MESSAGES)),
            after: CommitAfter::Processing,
        }
    }
}

impl PollOptions {
    /// The defaults with a wait of up to `max_wait` for `min_count` messages.
    pub fn wait(max_wait: IggyDuration) -> Self {
        Self {
            max_wait,
            ..Self::default()
        }
    }

    /// # Errors
    /// [`IggyError::InvalidMessagesCount`] for a zero `count` or a `min_count` outside
    /// `1..=count`, [`IggyError::InvalidCommand`] for a `max_wait` above
    /// [`MAX_DEFERRED_POLL_WAIT_US`](crate::MAX_DEFERRED_POLL_WAIT_US),
    /// [`IggyError::InvalidSizeBytes`] for a `max_bytes` below
    /// [`MIN_DEFERRED_POLL_MAX_BYTES`](crate::MIN_DEFERRED_POLL_MAX_BYTES), and
    /// [`IggyError::InvalidConfiguration`] for [`CommitPolicy::Auto`] without an `interval` or an
    /// `every`, which would store only on close.
    pub fn validate(&self) -> Result<(), IggyError> {
        if let CommitPolicy::Auto(AutoCommit {
            interval: None,
            every: None,
            ..
        }) = self.commit
        {
            return Err(IggyError::InvalidConfiguration);
        }
        DeferredPollOptions::from(*self).validate(self.count)
    }

    /// The limit of the whole request: `max_wait` plus 10 s.
    pub fn request_timeout(&self) -> IggyDuration {
        self.max_wait
            .get_duration()
            .saturating_add(POLL_REQUEST_TIMEOUT_MARGIN)
            .into()
    }
}

/// The wire limits of a poll.
#[doc(hidden)]
impl From<PollOptions> for DeferredPollOptions {
    fn from(options: PollOptions) -> Self {
        Self {
            max_wait: options.max_wait,
            min_count: options.min_count,
            max_bytes: options.max_bytes,
            request_timeout: options.request_timeout(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MAX_DEFERRED_POLL_WAIT_US, MIN_DEFERRED_POLL_MAX_BYTES};

    const MAX_WAIT: Duration = Duration::from_micros(MAX_DEFERRED_POLL_WAIT_US);
    const MIN_COUNT: u32 = 2;

    fn min_max_bytes() -> u32 {
        u32::try_from(MIN_DEFERRED_POLL_MAX_BYTES).unwrap()
    }

    #[test]
    fn given_no_options_when_defaulted_should_answer_at_once_and_commit_nothing() {
        let options = PollOptions::default();

        assert_eq!(
            options,
            PollOptions {
                count: 1000,
                max_wait: Duration::ZERO.into(),
                min_count: 1,
                max_bytes: 1024 * 1024,
                commit: CommitPolicy::Manual,
            }
        );
        assert_eq!(options.validate(), Ok(()));
    }

    #[test]
    fn given_no_auto_commit_settings_when_defaulted_should_store_every_10000_messages_or_5_seconds_after_processing()
     {
        assert_eq!(
            AutoCommit::default(),
            AutoCommit {
                interval: Some(NonZeroIggyDuration::new(Duration::from_secs(5)).unwrap()),
                every: Some(CommitEvery::Messages(NonZeroU32::new(10_000).unwrap())),
                after: CommitAfter::Processing,
            }
        );
    }

    #[test]
    fn given_auto_commit_without_an_interval_or_an_event_when_validating_should_refuse_it() {
        let auto = |interval, every| PollOptions {
            commit: CommitPolicy::Auto(AutoCommit {
                interval,
                every,
                after: CommitAfter::Delivery,
            }),
            ..PollOptions::default()
        };

        assert_eq!(
            auto(None, None).validate(),
            Err(IggyError::InvalidConfiguration)
        );
        assert_eq!(
            auto(Some(NonZeroIggyDuration::ONE_SECOND), None).validate(),
            Ok(())
        );
        assert_eq!(auto(None, Some(CommitEvery::Batch)).validate(), Ok(()));
    }

    #[test]
    fn given_a_wait_when_building_options_should_keep_the_other_defaults() {
        let max_wait = IggyDuration::ONE_SECOND;

        assert_eq!(
            PollOptions::wait(max_wait),
            PollOptions {
                max_wait,
                ..PollOptions::default()
            }
        );
    }

    #[test]
    fn given_invalid_options_when_validating_should_fail_like_the_wire_options() {
        let valid = PollOptions::wait(MAX_WAIT.into());
        assert_eq!(valid.validate(), Ok(()));
        let cases = [
            (
                PollOptions { count: 0, ..valid },
                IggyError::InvalidMessagesCount,
            ),
            (
                PollOptions {
                    min_count: 0,
                    ..valid
                },
                IggyError::InvalidMessagesCount,
            ),
            (
                PollOptions {
                    min_count: valid.count + 1,
                    ..valid
                },
                IggyError::InvalidMessagesCount,
            ),
            (
                PollOptions::wait((MAX_WAIT + Duration::from_micros(1)).into()),
                IggyError::InvalidCommand,
            ),
            (
                PollOptions::wait(Duration::MAX.into()),
                IggyError::InvalidCommand,
            ),
            (
                PollOptions {
                    max_bytes: min_max_bytes() - 1,
                    ..valid
                },
                IggyError::InvalidSizeBytes,
            ),
        ];

        for (options, error) in cases {
            assert_eq!(options.validate(), Err(error), "{options:?}");
        }
        let smallest = PollOptions {
            max_bytes: min_max_bytes(),
            ..valid
        };
        assert_eq!(smallest.validate(), Ok(()));
    }

    #[test]
    fn given_options_when_converted_to_the_wire_should_allow_ten_seconds_beyond_the_wait() {
        for (max_wait, request_timeout) in [
            (Duration::ZERO, Duration::from_secs(10)),
            (MAX_WAIT, MAX_WAIT + Duration::from_secs(10)),
        ] {
            let options = PollOptions {
                min_count: MIN_COUNT,
                max_bytes: min_max_bytes(),
                ..PollOptions::wait(max_wait.into())
            };

            let wire = DeferredPollOptions::from(options);

            assert_eq!(options.request_timeout(), request_timeout.into());
            assert_eq!(
                wire,
                DeferredPollOptions {
                    max_wait: max_wait.into(),
                    min_count: MIN_COUNT,
                    max_bytes: min_max_bytes(),
                    request_timeout: request_timeout.into(),
                }
            );
            assert_eq!(wire.validate(options.count), Ok(()));
        }
    }
}
