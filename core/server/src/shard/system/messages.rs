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

use super::COMPONENT;
use crate::binary::handlers::messages::poll_messages_handler::IggyPollMetadata;
use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::streaming::partitions::partition;
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet};
use crate::streaming::session::Session;
use crate::streaming::utils::PooledBuffer;
use async_zip::tokio::read::stream;
use error_set::ErrContext;
use futures::stream_select;
use iggy_common::{
    BytesSerializable, Consumer, EncryptorKind, IGGY_MESSAGE_HEADER_SIZE, Identifier, IggyError,
    Partitioning, PollingStrategy, UserId,
};
use tracing::{error, trace};

impl IggyShard {
    pub async fn append_messages(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!(
                "Failed to get stream with ID: {} (error: {})",
                stream_id, error
            )
        })?;
        let stream_id = stream.stream_id;
        let numeric_topic_id = stream.get_topic(topic_id).map(|topic| topic.topic_id).with_error_context(|error| {
            format!(
                "Failed to get topic with ID: {} (error: {})",
                topic_id, error
            )
        })?;
        // TODO: We should look into refactoring those permissioners, so they can accept `Identifier` instead of numeric IDs.
        // Validate permissions for given user on stream and topic.
        self.permissioner.borrow().append_messages(
            user_id,
            stream.stream_id,
            numeric_topic_id
        ).with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - permission denied to append messages for user {} on stream ID: {}, topic ID: {}",
            user_id,
            stream.stream_id,
            numeric_topic_id
        ))?;
        // Encrypt messages if encryptor is enabled in configuration.
        let batch = self.maybe_encrypt_messages(batch)?;
        let messages_count = batch.count();
        stream
            .append_messages(topic_id, partitioning, async |topic, partition_id| {
                let namespace = IggyNamespace::new(stream_id, topic.topic_id, partition_id);
                let payload = ShardRequestPayload::SendMessages { batch };
                let request = ShardRequest::new(stream_id, numeric_topic_id, partition_id, payload);
                let message = ShardMessage::Request(request);

                match self
                    .send_request_to_shard_or_recoil(&namespace, message)
                    .await?
                {
                    ShardSendRequestResult::Recoil(message) => {
                        if let ShardMessage::Request( ShardRequest { partition_id, payload, .. } ) = message
                            && let ShardRequestPayload::SendMessages { batch } = payload
                        {
                            topic.append_messages(partition_id, batch).await.with_error_context(|error| {
                                format!("{COMPONENT}: Failed to append messages to stream_id: {stream_id}, topic_id: {topic_id}, partition_id: {partition_id}, error: {error})")
                            })
                        } else {
                            unreachable!(
                                "Expected a SendMessages request inside of SendMessages handler, impossible state"
                            );
                        }
                    }
                    ShardSendRequestResult::Response(response) => {
                        match response {
                            ShardResponse::SendMessages =>  { Ok(()) }
                            ShardResponse::ErrorResponse(err) => {
                                Err(err)
                            }
                            _ => unreachable!(
                                "Expected a SendMessages response inside of SendMessages handler, impossible state"
                            ),
                        }

                    }
                }
            })
            .await?;

        self.metrics.increment_messages(messages_count as u64);
        Ok(())
    }

    pub async fn poll_messages(
        &self,
        client_id: u32,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer: Consumer,
        maybe_partition_id: Option<u32>,
        args: PollingArgs,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - stream not found for stream ID: {}",
                stream_id
            )
        })?;
        let stream_id = stream.stream_id;
        let numeric_topic_id = stream.get_topic(topic_id).map(|topic| topic.topic_id).with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - topic not found for stream ID: {}, topic_id: {}",
                stream_id, topic_id
            )
        })?;

        self.permissioner
            .borrow()
            .poll_messages(user_id, stream_id, numeric_topic_id)
            .with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to poll messages for user {} on stream ID: {}, topic ID: {}",
                user_id,
                stream_id,
                numeric_topic_id
            ))?;

        // There might be no partition assigned, if it's the consumer group member without any partitions.
        //return Ok((IggyPollMetadata::new(0, 0), IggyMessagesBatchSet::empty()));

        let (metadata, batch) = stream.poll_messages(topic_id, client_id, consumer, maybe_partition_id, args.auto_commit, async |topic, consumer, partition_id|  {
            let namespace = IggyNamespace::new(stream.stream_id, topic.topic_id, partition_id);
            let payload = ShardRequestPayload::PollMessages {
                consumer,
                args,
            };
            let request = ShardRequest::new(stream.stream_id, topic.topic_id, partition_id, payload);
            let message = ShardMessage::Request(request);

            match self
                    .send_request_to_shard_or_recoil(&namespace, message)
                    .await?
                {
                    ShardSendRequestResult::Recoil(message) => {
                        if let ShardMessage::Request( ShardRequest { partition_id, payload, .. } ) = message
                            && let ShardRequestPayload::PollMessages { consumer, args } = payload
                        {
                            topic.get_messages(consumer, partition_id, args.strategy, args.count).await.with_error_context(|error| {
                                format!("{COMPONENT}: Failed to get messages for stream_id: {stream_id}, topic_id: {topic_id}, partition_id: {partition_id}, error: {error})")
                            })
                        } else {
                            unreachable!(
                                "Expected a PollMessages request inside of PollMessages handler, impossible state"
                            );
                        }
                    }
                    ShardSendRequestResult::Response(response) => {
                        match response {
                            ShardResponse::PollMessages(result) =>  { Ok(result) }
                            ShardResponse::ErrorResponse(err) => {
                                Err(err)
                            }
                            _ => unreachable!(
                                "Expected a SendMessages response inside of SendMessages handler, impossible state"
                            ),
                        }

                    }
                }
        }).await?;

        let batch = if let Some(_encryptor) = &self.encryptor {
            //TODO: Bring back decryptor
            todo!();
            //self.decrypt_messages(batch, encryptor.as_ref()).await?
        } else {
            batch
        };

        Ok((metadata, batch))
    }

    pub async fn flush_unsaved_buffer(
        &self,
        session: &Session,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(&stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}")
        })?;
        let stream_id = stream.stream_id;
        let topic = self.find_topic(session, &stream, &topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic ID: {topic_id}"))?;
        self.permissioner.borrow().append_messages(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id
        ).with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - permission denied to append messages for user {} on stream ID: {}, topic ID: {}",
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id
        ))?;
        topic.flush_unsaved_buffer(partition_id, fsync).await?;
        Ok(())
    }

    pub fn maybe_encrypt_messages(
        &self,
        batch: IggyMessagesBatchMut,
    ) -> Result<IggyMessagesBatchMut, IggyError> {
        let encryptor = match self.encryptor.as_ref() {
            Some(encryptor) => encryptor,
            None => return Ok(batch),
        };
        let mut encrypted_messages = PooledBuffer::with_capacity(batch.size() as usize * 2);
        let count = batch.count();
        let mut indexes = IggyIndexesMut::with_capacity(batch.count() as usize, 0);
        let mut position = 0;

        for message in batch.iter() {
            let header = message.header();
            let payload_length = header.payload_length();
            let user_headers_length = header.user_headers_length();
            let payload_bytes = message.payload();
            let user_headers_bytes = message.user_headers();

            let encrypted_payload = encryptor.encrypt(payload_bytes);

            match encrypted_payload {
                Ok(encrypted_payload) => {
                    encrypted_messages.extend_from_slice(&header.to_bytes());
                    encrypted_messages.extend_from_slice(&encrypted_payload);
                    if let Some(user_headers_bytes) = user_headers_bytes {
                        encrypted_messages.extend_from_slice(user_headers_bytes);
                    }
                    indexes.insert(0, position as u32, 0);
                    position += IGGY_MESSAGE_HEADER_SIZE + payload_length + user_headers_length;
                }
                Err(error) => {
                    error!("Cannot encrypt the message. Error: {}", error);
                    continue;
                }
            }
        }

        Ok(IggyMessagesBatchMut::from_indexes_and_messages(
            count,
            indexes,
            encrypted_messages,
        ))
    }
}

#[derive(Debug)]
pub struct PollingArgs {
    pub strategy: PollingStrategy,
    pub count: u32,
    pub auto_commit: bool,
}

impl PollingArgs {
    pub fn new(strategy: PollingStrategy, count: u32, auto_commit: bool) -> Self {
        Self {
            strategy,
            count,
            auto_commit,
        }
    }
}
