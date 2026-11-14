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

use crate::clients::consumer::IggyConsumer;
use crate::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use crate::prelude::{CommitAfter, ReadError};
use async_trait::async_trait;
use std::future::poll_fn;
use tokio::sync::oneshot;
use tracing::{error, info, trace, warn};

#[async_trait]
impl<'a> IggyConsumerMessageExt<'a> for IggyConsumer {
    /// Reads messages until a shutdown signal arrives, and hands each one to `message_consumer`.
    ///
    /// Each turn of the loop checks `shutdown_rx` first and then waits for a message. A signal
    /// that arrives while a handler runs stops the loop after that handler returns. Dropping the
    /// sender counts as a signal.
    ///
    /// The loop returns `Ok(())` in three cases: (1) the signal arrived, (2) the sender was
    /// dropped, or (3) the consumer has no more messages, as after
    /// [`shutdown()`](crate::prelude::IggyConsumer::shutdown).
    /// After the two signal cases the consumer stays usable, so call
    /// [`shutdown()`](crate::prelude::IggyConsumer::shutdown) afterwards to store the counted
    /// offsets and leave the consumer group.
    ///
    /// # Committing after a message
    ///
    /// Under [`CommitPolicy::Auto`], the [`CommitAfter`] of the poll options sets when a message
    /// counts:
    ///
    /// | Variant | The message counts |
    /// | --- | --- |
    /// | [`CommitAfter::Delivery`] | before the handler runs |
    /// | [`CommitAfter::Processing`] | after the handler returns `Ok`. A handler that fails ends the loop with its error, and its message stays uncounted, so the next run reads it again |
    /// | [`CommitAfter::Marked`] | never from this loop, because the handler gets no checkpoint to mark |
    ///
    /// A counted message is not stored at once. The consumer stores the latest counted position
    /// at the interval or event of [`AutoCommit`], and on shutdown. Under
    /// [`CommitPolicy::Manual`] the handler gets no handle to commit with, so with
    /// [`PollingStrategy::next()`] each run reads the same messages again. The loop logs a
    /// warning for that case.
    ///
    /// # Errors
    ///
    /// A [`ReadError`] with the partition of the error, if it has one, and one of these errors:
    ///
    /// * `IggyError::InvalidServerAddress`: The server address is invalid.
    /// * `IggyError::InvalidClientAddress`: The client address is invalid.
    /// * `IggyError::InvalidConfiguration`: The consumer was not initialized.
    /// * The error of the handler, with the partition of its message, under
    ///   `CommitAfter::Processing`.
    /// * `IggyError::ClientShutdown`: The client has been shut down.
    /// * `IggyError::LaneBlocked`: A retained operation that the consumer does not resolve blocks
    ///   its polls and requires caller recovery.
    /// * `IggyError::PartitionLaneBlocked`: With `CommitPolicy::OnRead`, another call on the
    ///   client left a committing read of the partition with an unknown outcome, and the
    ///   partition stays stopped until the caller abandons the operation and seeks. A lost reply
    ///   of the consumer itself ends nothing: the reader reads on and hands over what it stored.
    /// * `IggyError::HistoryUnavailable`: The partition has a new history, and it stays stopped
    ///   until the caller seeks.
    /// * `IggyError::Unauthorized`, `IggyError::PartitionNotFound` and the other refusals that a
    ///   retry does not change, with a partition: the reader retried the partition with a longer
    ///   wait each time, and it stays stopped until the caller seeks.
    /// * Any error with a [`ReadError::skip`]: A message of the partition does not decode, and
    ///   the reader reads it again until the caller seeks to the skip.
    /// * `IggyError::FetchProgressUncertain` with a [`ReadError::lost`] range: the reader cannot
    ///   read again the messages that a read with a lost reply stored. The error stops nothing,
    ///   and a seek after it drops the batch that follows the range. A new call goes on with
    ///   that batch. If the reader also stopped the partition, that call then ends with the
    ///   error that stopped it.
    /// * `IggyError::InvalidNumberEncoding`, `IggyError::InvalidMessagePayloadLength` or
    ///   `IggyError::InvalidMessagesCount` without a skip: A reply of the partition does not
    ///   decode, and the reader cannot name the message to skip. With
    ///   `CommitPolicy::OnRead`, a read from `Next` stops the partition until the caller
    ///   seeks.
    /// * Any other poll error, once the client cannot come back without the caller: it has no
    ///   link and no replica link that tries to reconnect, and the heartbeat does not restart it,
    ///   because reconnection is disabled or the client holds no session to resume, as after
    ///   `disconnect()`.
    ///
    /// Any other error is logged and the consumption goes on. A poll never dials. The client
    /// reconnects while a replica link tries again, and the heartbeat of `IggyClient` restarts
    /// the links that used up their retries. Once the client has signed in again, the consumer
    /// rejoins its group and resolves an uncertain group create or join of its own. A handler
    /// error under any other commit policy is logged too, and that message is skipped.
    ///
    /// [`AutoCommit`]: crate::prelude::AutoCommit
    /// [`CommitAfter`]: crate::prelude::CommitAfter
    /// [`CommitAfter::Delivery`]: crate::prelude::CommitAfter::Delivery
    /// [`CommitAfter::Marked`]: crate::prelude::CommitAfter::Marked
    /// [`CommitAfter::Processing`]: crate::prelude::CommitAfter::Processing
    /// [`CommitPolicy::Auto`]: crate::prelude::CommitPolicy::Auto
    /// [`CommitPolicy::Manual`]: crate::prelude::CommitPolicy::Manual
    /// [`PollingStrategy::next()`]: crate::prelude::PollingStrategy::next
    async fn consume_messages<P>(
        &mut self,
        message_consumer: &'a P,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), ReadError>
    where
        P: MessageConsumer + Sync,
    {
        // Unlike a caller of `next()`, a message consumer gets no handle to commit with.
        if self.relies_on_manual_commits() {
            warn!(
                "Consumer: {name} on topic: {topic} and stream: {stream} never stores offsets, because it runs with CommitPolicy::Manual. With PollingStrategy::next(), each restart reads the same messages again. Set CommitPolicy::Auto in the poll options to store them.",
                name = self.name(),
                topic = self.topic(),
                stream = self.stream()
            );
        }

        loop {
            tokio::select! {
                biased;
                // Check first if we have received a shutdown signal
                _ = &mut shutdown_rx => {
                    info!("Received shutdown signal, stopping message consumption from consumer {name} on topic: {topic} and stream: {stream}",
                        name = self.name(), topic = self.topic(), stream = self.stream());
                    break;
                }

                message = poll_fn(|cx| self.poll_message(cx)) => {
                    match message {
                        Some(Ok((received_message, ends_batch))) => {
                            let checkpoint = received_message.checkpoint();
                            let partition_id = received_message.partition_id;
                            let current_offset = received_message.current_offset;
                            let message_offset = received_message.message.header.offset;
                            let counts_after = self.counts_after();
                            if counts_after == Some(CommitAfter::Delivery) {
                                self.count(checkpoint, ends_batch);
                            }
                            match message_consumer.consume(received_message).await {
                                Ok(()) => {
                                    trace!("Message at offset: {message_offset}/{current_offset}, partition: {partition_id} has been handled by consumer: {name} on topic: {topic} and stream: {stream}",
                                        name = self.name(), topic = self.topic(), stream = self.stream());
                                    if counts_after == Some(CommitAfter::Processing) {
                                        self.count(checkpoint, ends_batch);
                                    }
                                }
                                Err(err) => {
                                    error!("Error while handling message at offset: {message_offset}/{current_offset}, partition: {partition_id} for consumer: {name} on topic: {topic} and stream: {stream} due to error: {err}",
                                        name = self.name(), topic = self.topic(), stream = self.stream());
                                    if counts_after == Some(CommitAfter::Processing) {
                                        return Err(ReadError::new(Some(partition_id), err));
                                    }
                                }
                            }
                        }
                        Some(Err(err)) => {
                            let ends = self.read_error_ends_consumption(&err).await;
                            if ends {
                                error!("Client error: {err} for consumer: {name} on topic: {topic} and stream: {stream}",
                                    name = self.name(), topic = self.topic(), stream = self.stream());
                                return Err(err);
                            }
                            error!("Error while handling message: {err} for consumer: {name} on topic: {topic} and stream: {stream}",
                                name = self.name(), topic = self.topic(), stream = self.stream());
                        }
                        None => break,
                    }
                }

            }
        }

        Ok(())
    }
}
