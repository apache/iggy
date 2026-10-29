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
use crate::prelude::{IggyError, ReadError};
use async_trait::async_trait;
use std::future::poll_fn;
use tokio::sync::oneshot;
use tracing::{error, info, trace, warn};

#[async_trait]
impl<'a> IggyConsumerMessageExt<'a> for IggyConsumer {
    /// Consume messages from the stream and process them with the given message consumer.
    ///
    /// The event of the client commit policy counts each message after the handler returns,
    /// also after a failure.
    ///
    /// # Arguments
    ///
    /// * `message_consumer`: The message consumer to use. This must be a reference to a static
    /// object that implements the `MessageConsumer` trait.
    /// * `shutdown_rx`: A receiver which will receive a shutdown signal, which will be used to
    /// stop message consumption.
    ///
    /// # Errors
    ///
    /// A [`ReadError`] with the partition of the error, if it has one, and one of these errors:
    ///
    /// * `IggyError::InvalidServerAddress`: The server address is invalid.
    /// * `IggyError::InvalidClientAddress`: The client address is invalid.
    /// * `IggyError::InvalidConfiguration`: The consumer was not initialized.
    /// * `IggyError::ClientShutdown`: The client has been shut down.
    /// * `IggyError::LaneBlocked`: A retained operation that the consumer does not resolve blocks
    ///   its polls and requires caller recovery.
    /// * `IggyError::PartitionLaneBlocked` or `IggyError::FetchProgressUncertain`: With
    ///   `ServerCommitPolicy::OnRead`, a read of the partition has an unknown outcome, and the
    ///   partition stays stopped until the caller abandons the operation and seeks.
    /// * `IggyError::HistoryUnavailable`: The partition has a new history, and it stays stopped
    ///   until the caller seeks.
    /// * Any error with a [`ReadError::skip`]: A message of the partition does not decode, and
    ///   the reader reads it again until the caller seeks to the skip.
    /// * `IggyError::InvalidNumberEncoding`, `IggyError::InvalidMessagePayloadLength` or
    ///   `IggyError::InvalidMessagesCount` without a skip: A reply of the partition does not
    ///   decode, and the reader cannot name the message to skip. With
    ///   `ServerCommitPolicy::OnRead`, a read from `Next` stops the partition until the caller
    ///   seeks.
    /// * Any other poll error, once the client cannot come back without the caller: it has no
    ///   link and no replica link that tries to reconnect, and the heartbeat does not restart it,
    ///   because reconnection is disabled or the client holds no session to resume, as after
    ///   `disconnect()`.
    ///
    /// Any other error is logged and the consumption goes on. A poll never dials. The client
    /// reconnects while a replica link tries again, and the heartbeat of `IggyClient` restarts
    /// the links that used up their retries. Once the client has signed in again, the consumer
    /// rejoins its group and resolves an uncertain group create or join of its own.
    ///
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
                "Consumer: {name} on topic: {topic} and stream: {stream} never stores offsets, because it runs with the default ClientCommitPolicy. With PollingStrategy::next(), each restart reads the same messages again. Set client_commit to store them.",
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

                message = poll_fn(|cx| self.poll_message(cx, false)) => {
                    match message {
                        Some(Ok((received_message, ends_batch))) => {
                            let checkpoint = received_message.checkpoint();
                            let partition_id = received_message.partition_id;
                            let current_offset = received_message.current_offset;
                            let message_offset = received_message.message.header.offset;
                            if let Err(err) = message_consumer.consume(received_message).await {
                                error!("Error while handling message at offset: {message_offset}/{current_offset}, partition: {partition_id} for consumer: {name} on topic: {topic} and stream: {stream} due to error: {err}",
                                    name = self.name(), topic = self.topic(), stream = self.stream());
                            } else {
                                trace!("Message at offset: {message_offset}/{current_offset}, partition: {partition_id} has been handled by consumer: {name} on topic: {topic} and stream: {stream}",
                                    name = self.name(), topic = self.topic(), stream = self.stream());
                            }
                            self.on_consumed(checkpoint, ends_batch);
                        }
                        Some(Err(err)) => {
                            let ends = err.skip.is_some()
                                || matches!(
                                    err.error,
                                    IggyError::InvalidServerAddress
                                        | IggyError::InvalidClientAddress
                                        | IggyError::InvalidConfiguration
                                        | IggyError::LaneBlocked(_)
                                        | IggyError::PartitionLaneBlocked(..)
                                        | IggyError::FetchProgressUncertain(_)
                                        | IggyError::HistoryUnavailable
                                        | IggyError::InvalidNumberEncoding
                                        | IggyError::InvalidMessagePayloadLength
                                        | IggyError::InvalidMessagesCount
                                        | IggyError::ClientShutdown
                                )
                                || !self.client_can_recover().await;
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
