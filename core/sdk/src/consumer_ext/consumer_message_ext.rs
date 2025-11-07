/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::clients::consumer::{AutoCommit, AutoCommitAfter, IggyConsumer};
use crate::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use crate::prelude::IggyError;
use async_trait::async_trait;
use futures_util::StreamExt;
use tokio::sync::oneshot;
use tracing::{error, info, trace};

#[async_trait]
impl<'a> IggyConsumerMessageExt<'a> for IggyConsumer {
    /// Consume messages from the stream and process them with the given message consumer.
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
    /// * `IggyError::Disconnected`: The client has been disconnected.
    /// * `IggyError::CannotEstablishConnection`: The client cannot establish a connection to iggy.
    /// * `IggyError::StaleClient`: This client is stale and cannot be used to consume messages.
    /// * `IggyError::InvalidServerAddress`: The server address is invalid.
    /// * `IggyError::InvalidClientAddress`: The client address is invalid.
    /// * `IggyError::NotConnected`: The client is not connected.
    /// * `IggyError::ClientShutdown`: The client has been shut down.
    ///
    async fn consume_messages<P>(
        &mut self,
        message_consumer: &'a P,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), IggyError>
    where
        P: MessageConsumer + Sync,
    {
        let auto_commit = self.auto_commit();
        let store_offset_after_each_message = matches!(
            auto_commit,
            AutoCommit::After(AutoCommitAfter::ConsumingEachMessage)
                | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::ConsumingEachMessage)
        );

        let store_offset_after_all_messages = matches!(
            auto_commit,
            AutoCommit::After(AutoCommitAfter::ConsumingAllMessages)
                | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::ConsumingAllMessages)
        );

        let store_after_every_nth_message = match auto_commit {
            AutoCommit::After(AutoCommitAfter::ConsumingEveryNthMessage(n))
            | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::ConsumingEveryNthMessage(n)) => {
                n as u64
            }
            _ => 0,
        };

        loop {
            tokio::select! {
                // Check first if we have received a shutdown signal
                _ = &mut shutdown_rx => {
                    info!("Received shutdown signal, stopping message consumption from consumer {name} on topic: {topic} and stream: {stream}",
                        name = self.name(), topic = self.topic(), stream = self.stream());
                    break;
                }

                message = self.next() => {
                    match message {
                        Some(Ok(received_message)) => {
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

                            if store_offset_after_each_message {
                                trace!("Storing offset: {message_offset}/{current_offset}, partition: {partition_id}, after each message for consumer: {name} on topic: {topic} and stream: {stream}",
                                    name = self.name(), topic = self.topic(), stream = self.stream());
                                self.send_store_offset(partition_id, message_offset);
                            } else if store_after_every_nth_message > 0  && message_offset % store_after_every_nth_message == 0 {
                                trace!("Storing offset: {message_offset}/{current_offset}, partition: {partition_id}, after every {store_after_every_nth_message} message for consumer: {name} on topic: {topic} and stream: {stream}",
                                    store_after_every_nth_message = store_after_every_nth_message, name = self.name(), topic = self.topic(), stream = self.stream());
                                self.send_store_offset(partition_id, message_offset);
                            } else if store_offset_after_all_messages && message_offset == current_offset {
                                trace!("Storing offset: {message_offset}/{current_offset}, partition: {partition_id}, after all messages for consumer: {name} on topic: {topic} and stream: {stream}",
                                    name = self.name(), topic = self.topic(), stream = self.stream());
                                self.send_store_offset(partition_id, message_offset);
                            }
                        }
                        Some(Err(err)) => {
                            match err {
                                IggyError::Disconnected |
                                IggyError::CannotEstablishConnection |
                                IggyError::StaleClient |
                                IggyError::InvalidServerAddress |
                                IggyError::InvalidClientAddress |
                                IggyError::NotConnected |
                                IggyError::ClientShutdown => {
                                    error!("Client error: {err} for consumer: {name} on topic: {topic} and stream: {stream}",
                                        name = self.name(), topic = self.topic(), stream = self.stream());
                                    return Err(err);
                                }
                                _ => {
                                    error!("Error while handling message: {err} for consumer: {name} on topic: {topic} and stream: {stream}",
                                        name = self.name(), topic = self.topic(), stream = self.stream());
                                    continue;
                                }
                            }
                        }
                        None => break,
                    }
                }

            }
        }

        Ok(())
    }
}
