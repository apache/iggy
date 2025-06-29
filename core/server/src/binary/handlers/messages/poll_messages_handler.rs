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

use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::messages::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::sender::SenderKind;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardMessage, ShardRequest};
use crate::shard::{IggyShard, ShardRequestResult};
use crate::shard::system::messages::PollingArgs;
use crate::streaming::segments::IggyMessagesBatchSet;
use crate::streaming::session::Session;
use crate::to_iovec;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::{IggyError, PollMessages};
use std::io::IoSlice;
use std::rc::Rc;
use tracing::{debug, trace};

#[derive(Debug)]
pub struct IggyPollMetadata {
    pub partition_id: u32,
    pub current_offset: u64,
}

impl IggyPollMetadata {
    pub fn new(partition_id: u32, current_offset: u64) -> Self {
        Self {
            partition_id,
            current_offset,
        }
    }
}

impl ServerCommandHandler for PollMessages {
    fn code(&self) -> u32 {
        iggy_common::POLL_MESSAGES_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let stream = shard
            .get_stream(&self.stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {}",
                    self.stream_id
                )
            })?;
        let topic = stream.get_topic(
            &self.topic_id,
        ).with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - topic not found for stream ID: {}, topic_id: {}",
            self.stream_id, self.topic_id
        ))?;

        let PollMessages {
            consumer,
            partition_id,
            strategy,
            count,
            auto_commit,
            ..
        } = self;
        let args = PollingArgs::new(strategy, count, auto_commit);

        shard.permissioner
            .borrow()
            .poll_messages(session.get_user_id(), topic.stream_id, topic.topic_id)
            .with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to poll messages for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;

        if !topic.has_partitions() {
            return Err(IggyError::NoPartitions(topic.topic_id, topic.stream_id));
        }

        // There might be no partition assigned, if it's the consumer group member without any partitions.
        let Some((consumer, partition_id)) = topic
            .resolve_consumer_with_partition_id(&consumer, session.client_id, partition_id, true)
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to resolve consumer with partition id, consumer: {consumer}, client ID: {}, partition ID: {:?}", session.client_id, partition_id))? else {
                todo!("Send early response");
            //return Ok((IggyPollMetadata::new(0, 0), IggyMessagesBatchSet::empty()));
        };

        let namespace = IggyNamespace::new(stream.stream_id, topic.topic_id, partition_id);
        let request = ShardRequest::PollMessages {
            consumer,
            partition_id,
            args,
            count,
        };
        let message = ShardMessage::Request(request);
        let (metadata, batch) = match shard.send_request_to_shard(&namespace, message).await {
            ShardRequestResult::SameShard(message) => {
                match message {
                    ShardMessage::Request(request) => {
                        match request {
                            ShardRequest::PollMessages {
                                consumer,
                                partition_id,
                                args,
                                count,
                            } => {
                                topic.get_messages(consumer, partition_id, args.strategy, count).await?
                                
                            }
                            _ => unreachable!(
                                "Expected a SendMessages request inside of SendMessages handler, impossible state"
                            ),
                        }
                    }
                    _ => unreachable!(
                        "Expected a request message inside of an command handler, impossible state"
                    ),
                }
            }
            ShardRequestResult::Result(result) => {
                match result? {
                    ShardResponse::PollMessages(response) => {
                        response
                    }
                    ShardResponse::ErrorResponse(err) => {
                        return Err(err);
                    }
                    _ => unreachable!(
                        "Expected a PollMessages response inside of PollMessages handler, impossible state"
                    ),
                }
            }
        };


        // Collect all chunks first into a Vec to extend their lifetimes.
        // This ensures the Bytes (in reality Arc<[u8]>) references from each IggyMessagesBatch stay alive
        // throughout the async vectored I/O operation, preventing "borrowed value does not live
        // long enough" errors while optimizing transmission by using larger chunks.

        // 4 bytes for partition_id + 8 bytes for current_offset + 4 bytes for messages_count + size of all batches.
        let response_length = 4 + 8 + 4 + batch.size();
        let response_length_bytes = response_length.to_le_bytes();

        let partition_id = metadata.partition_id.to_le_bytes();
        let current_offset = metadata.current_offset.to_le_bytes();
        let count = batch.count().to_le_bytes();

        let mut iovecs = Vec::with_capacity(batch.containers_count() + 3);
        iovecs.push(to_iovec(&partition_id));
        iovecs.push(to_iovec(&current_offset));
        iovecs.push(to_iovec(&count));

        iovecs.extend(batch.iter().map(|m| to_iovec(&m)));
        trace!(
            "Sending {} messages to client ({} bytes) to client",
            batch.count(),
            response_length
        );

        sender
            .send_ok_response_vectored(&response_length_bytes, iovecs)
            .await?;
        Ok(())
    }
}

impl BinaryServerCommand for PollMessages {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::PollMessages(poll_messages) => Ok(poll_messages),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
