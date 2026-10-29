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

use tokio::time::Instant;

use crate::{
    DEFAULT_POLL_MAX_BYTES, DeferredPollOptions, IggyDuration, IggyError, PolledMessages,
    PollingKind, PollingStrategy,
};

const DEFAULT_POLL_COUNT: u32 = 1000;
const DEFAULT_POLL_MIN_COUNT: u32 = 1;
/// Keeps the 1 s reply gap of the protocol and today's 11 s default (1 s wait plus 10 s).
const POLL_REQUEST_TIMEOUT_MARGIN: Duration = Duration::from_secs(10);
/// The count of a read that takes one message, so a message larger than `max_bytes` comes alone.
const ALONE_COUNT: u32 = 1;

/// Who stores the offset of a poll.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ServerCommitPolicy {
    /// The application or `IggyConsumer` commits.
    #[default]
    Disabled,
    /// The server stores the offset of the last message of a reply as part of the reply.
    OnRead,
}

/// The limits of one poll of one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollOptions {
    /// Most messages in one batch. Default 1000.
    pub count: u32,
    /// Zero answers at once. At most 600 s.
    pub max_wait: IggyDuration,
    /// At least 1 and at most `count`. Default 1.
    pub min_count: u32,
    /// Default 1 MiB. A message larger than `max_bytes` comes alone in its own read. At a `Last`
    /// start with a `count` above 1, such a first message fails the read with
    /// [`IggyError::InvalidSizeBytes`] instead, and the server stores no offset: a read alone
    /// would start at the last message and skip the messages before it.
    pub max_bytes: u32,
    /// Default [`ServerCommitPolicy::Disabled`].
    pub server_commit: ServerCommitPolicy,
}

impl Default for PollOptions {
    fn default() -> Self {
        Self {
            count: DEFAULT_POLL_COUNT,
            max_wait: Duration::ZERO.into(),
            min_count: DEFAULT_POLL_MIN_COUNT,
            max_bytes: DEFAULT_POLL_MAX_BYTES,
            server_commit: ServerCommitPolicy::Disabled,
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
    /// [`MAX_DEFERRED_POLL_WAIT_US`](crate::MAX_DEFERRED_POLL_WAIT_US), and
    /// [`IggyError::InvalidSizeBytes`] for a `max_bytes` below
    /// [`MIN_DEFERRED_POLL_MAX_BYTES`](crate::MIN_DEFERRED_POLL_MAX_BYTES).
    pub fn validate(&self) -> Result<(), IggyError> {
        DeferredPollOptions::from(*self).validate(self.count)
    }

    /// The limit of the whole request: `max_wait` plus 10 s.
    pub fn request_timeout(&self) -> IggyDuration {
        self.max_wait
            .get_duration()
            .saturating_add(POLL_REQUEST_TIMEOUT_MARGIN)
            .into()
    }

    /// The options of the read that takes a first message larger than `max_bytes` alone: one
    /// message without a byte limit, with the same wait and server commit.
    #[doc(hidden)]
    #[must_use]
    pub fn alone(self) -> Self {
        Self {
            count: ALONE_COUNT,
            min_count: ALONE_COUNT,
            max_bytes: u32::MAX,
            ..self
        }
    }

    /// Runs `poll` with the message count and the wire limits of these options. After the
    /// server refuses a first message larger than `max_bytes`, runs it once more with
    /// [`Self::alone`] in what is left of the request timeout. The server refuses such a message
    /// before it stores an offset, so the second read is safe with `OnRead` too. A `Last`
    /// `strategy` with a count above 1 gets no second read, because the server counts a `Last`
    /// start back from the last message by the count of the read.
    ///
    /// # Errors
    /// The error of the last read, or [`IggyError::TransientNotAccepted`] when no time is left
    /// for the second one.
    #[doc(hidden)]
    pub async fn poll_oversized_alone<F, Fut>(
        self,
        strategy: &PollingStrategy,
        mut poll: F,
    ) -> Result<PolledMessages, IggyError>
    where
        F: FnMut(u32, DeferredPollOptions) -> Fut,
        Fut: Future<Output = Result<PolledMessages, IggyError>>,
    {
        let started = Instant::now();
        match poll(self.count, self.into()).await {
            Err(IggyError::InvalidSizeBytes)
                if self.max_bytes != u32::MAX
                    && (strategy.kind != PollingKind::Last || self.count == ALONE_COUNT) =>
            {
                let alone = self.alone();
                let options = DeferredPollOptions::from(alone).remaining(started.elapsed())?;
                poll(alone.count, options).await
            }
            polled => polled,
        }
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
    use crate::{IggyTimestamp, MAX_DEFERRED_POLL_WAIT_US, MIN_DEFERRED_POLL_MAX_BYTES};

    const MAX_WAIT: Duration = Duration::from_micros(MAX_DEFERRED_POLL_WAIT_US);
    const MIN_COUNT: u32 = 2;
    const START: u64 = 5;

    fn min_max_bytes() -> u32 {
        u32::try_from(MIN_DEFERRED_POLL_MAX_BYTES).unwrap()
    }

    type Answer = Result<PolledMessages, IggyError>;

    /// Runs [`PollOptions::poll_oversized_alone`] from `strategy` on reads that answer with
    /// `answers` in order, and returns its result with the count and the wire limits of each read.
    async fn poll_oversized_alone(
        options: PollOptions,
        strategy: PollingStrategy,
        answers: impl IntoIterator<Item = Answer>,
    ) -> (Answer, Vec<(u32, DeferredPollOptions)>) {
        let mut answers = answers.into_iter();
        let mut reads = Vec::new();
        let polled = options
            .poll_oversized_alone(&strategy, |count, wire| {
                reads.push((count, wire));
                std::future::ready(answers.next().unwrap())
            })
            .await;
        (polled, reads)
    }

    #[test]
    fn given_no_options_when_defaulted_should_answer_at_once_without_a_server_commit() {
        let options = PollOptions::default();

        assert_eq!(
            options,
            PollOptions {
                count: 1000,
                max_wait: Duration::ZERO.into(),
                min_count: 1,
                max_bytes: 1024 * 1024,
                server_commit: ServerCommitPolicy::Disabled,
            }
        );
        assert_eq!(options.validate(), Ok(()));
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
    fn given_options_when_reading_alone_should_take_one_message_without_a_byte_limit() {
        let options = PollOptions {
            count: MIN_COUNT + 1,
            min_count: MIN_COUNT,
            max_bytes: min_max_bytes(),
            server_commit: ServerCommitPolicy::OnRead,
            ..PollOptions::wait(IggyDuration::ONE_SECOND)
        };

        let alone = options.alone();

        assert_eq!(
            alone,
            PollOptions {
                count: 1,
                min_count: 1,
                max_bytes: u32::MAX,
                ..options
            }
        );
        assert_eq!(alone.validate(), Ok(()));
    }

    /// Each of these reads begins at the same offset as its read alone. A `Last` start does so
    /// only with count 1, because both reads then begin at the last message.
    #[tokio::test]
    async fn given_a_refused_oversized_message_when_polling_should_read_it_alone_once() {
        let wait = PollOptions::wait(IggyDuration::ONE_SECOND);
        let one_message = PollOptions {
            count: ALONE_COUNT,
            ..wait
        };
        for (options, strategy) in [
            (wait, PollingStrategy::offset(START)),
            (wait, PollingStrategy::timestamp(IggyTimestamp::from(START))),
            (wait, PollingStrategy::first()),
            (wait, PollingStrategy::next()),
            (one_message, PollingStrategy::last()),
        ] {
            let first = DeferredPollOptions::from(options);
            for (second, expected) in [
                (Ok(PolledMessages::empty()), Ok(0)),
                (
                    Err(IggyError::InvalidSizeBytes),
                    Err(IggyError::InvalidSizeBytes),
                ),
            ] {
                let (polled, reads) = poll_oversized_alone(
                    options,
                    strategy,
                    [Err(IggyError::InvalidSizeBytes), second],
                )
                .await;

                assert_eq!(polled.map(|polled| polled.count), expected, "{strategy}");
                assert_eq!(reads.len(), 2, "{strategy}: one read alone, and no more");
                assert_eq!(reads[0], (options.count, first), "{strategy}");
                let (count, wire) = reads[1];
                let alone = options.alone();
                assert_eq!(
                    (count, wire.min_count, wire.max_bytes),
                    (alone.count, alone.min_count, alone.max_bytes),
                    "{strategy}: the message alone, without a byte limit"
                );
                assert!(
                    wire.request_timeout.get_duration() <= first.request_timeout.get_duration()
                        && wire.max_wait.get_duration() <= first.max_wait.get_duration(),
                    "{strategy}: the second read spends what is left of the request timeout"
                );
            }
        }
    }

    /// A read alone of a `Last` start of more than one message would begin at the last message
    /// and skip the window that the refused read asked for.
    #[tokio::test]
    async fn given_no_byte_limit_a_last_start_or_another_refusal_when_polling_should_not_read_again()
     {
        let unlimited = PollOptions {
            max_bytes: u32::MAX,
            ..PollOptions::default()
        };
        let two_messages = PollOptions {
            count: ALONE_COUNT + 1,
            ..PollOptions::default()
        };
        let next = PollingStrategy::next();
        let last = PollingStrategy::last();
        for (options, strategy, refusal) in [
            (unlimited, next, IggyError::InvalidSizeBytes),
            (two_messages, last, IggyError::InvalidSizeBytes),
            (PollOptions::default(), last, IggyError::InvalidSizeBytes),
            (
                PollOptions::default(),
                next,
                IggyError::InvalidMessagesCount,
            ),
        ] {
            let case = format!("{strategy} count {}", options.count);
            let (polled, reads) =
                poll_oversized_alone(options, strategy, [Err(refusal.clone())]).await;

            assert_eq!(polled.map(|polled| polled.count), Err(refusal), "{case}");
            assert_eq!(
                reads,
                [(options.count, DeferredPollOptions::from(options))],
                "{case}"
            );
        }
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
