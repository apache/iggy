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

use crate::binary::handlers::messages::poll_messages_handler::IggyPollMetadata;
use crate::shard::IggyShard;
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet};
use crate::streaming::session::Session;
use crate::streaming::utils::PooledBuffer;
use error_set::ErrContext;
use iggy_common::{
    BytesSerializable, Confirmation, Consumer, EncryptorKind, IGGY_MESSAGE_HEADER_SIZE, Identifier,
    IggyError, Partitioning, PollingStrategy,
};
use tracing::{error, trace};

impl IggyShard {
    pub async fn poll_messages(
        &self,
        session: &Session,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        args: PollingArgs,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        self.ensure_authenticated(session)?;
        if args.count == 0 {
            return Err(IggyError::InvalidMessagesCount);
        }

        let topic = self.find_topic(session, stream_id, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;
        self.permissioner
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
        let Some((polling_consumer, partition_id)) = topic
            .resolve_consumer_with_partition_id(consumer, session.client_id, partition_id, true)
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to resolve consumer with partition id, consumer: {consumer}, client ID: {}, partition ID: {:?}", session.client_id, partition_id))? else {
            return Ok((IggyPollMetadata::new(0, 0), IggyMessagesBatchSet::empty()));
        };

        let (metadata, batch_set) = topic
            .get_messages(polling_consumer, partition_id, args.strategy, args.count)
            .await?;

        if args.auto_commit && !batch_set.is_empty() {
            let offset = batch_set
                .last_offset()
                .expect("Batch set should have at least one batch");
            trace!(
                "Last offset: {} will be automatically stored for {}, stream: {}, topic: {}, partition: {}",
                offset, consumer, stream_id, topic_id, partition_id
            );
            topic
                .store_consumer_offset_internal(polling_consumer, offset, partition_id)
                .await
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to store consumer offset internal, polling consumer: {polling_consumer}, offset: {offset}, partition ID: {partition_id}")) ?;
        }

        let batch_set = if let Some(encryptor) = &self.encryptor {
            self.decrypt_messages(batch_set, encryptor.as_ref()).await?
        } else {
            batch_set
        };

        Ok((metadata, batch_set))
    }

    pub async fn append_messages(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: IggyMessagesBatchMut,
        confirmation: Option<Confirmation>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let topic = self.find_topic(session, stream_id, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream_id: {stream_id}, topic_id: {topic_id}"))?;
        self.permissioner.append_messages(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id
        ).with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - permission denied to append messages for user {} on stream ID: {}, topic ID: {}",
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id
        ))?;
        let messages_count = messages.count();

        // Encrypt messages if encryptor is configured
        let messages = if let Some(encryptor) = &self.encryptor {
            self.encrypt_messages(messages, encryptor.as_ref())?
        } else {
            messages
        };

        topic
            .append_messages(partitioning, messages, confirmation)
            .await?;

        self.metrics.increment_messages(messages_count as u64);
        Ok(())
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
        let topic = self.find_topic(session, &stream_id, &topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;
        self.permissioner.append_messages(
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

    async fn decrypt_messages(
        &self,
        batches: IggyMessagesBatchSet,
        encryptor: &EncryptorKind,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let mut decrypted_batches = Vec::with_capacity(batches.containers_count());
        for batch in batches.iter() {
            let count = batch.count();

            let mut indexes = IggyIndexesMut::with_capacity(batch.count() as usize, 0);
            let mut decrypted_messages = PooledBuffer::with_capacity(batch.size() as usize);
            let mut position = 0;

            for message in batch.iter() {
                let payload = encryptor.decrypt(message.payload());
                match payload {
                    Ok(payload) => {
                        message.header().write_to_buffer(&mut decrypted_messages);
                        decrypted_messages.extend_from_slice(&payload);
                        if let Some(user_headers) = message.user_headers() {
                            decrypted_messages.extend_from_slice(user_headers);
                        }
                        indexes.insert(0, position as u32, 0);
                        position += message.size();
                    }
                    Err(error) => {
                        error!("Cannot decrypt the message. Error: {}", error);
                        continue;
                    }
                }
            }
            let decrypted_batch =
                IggyMessagesBatchMut::from_indexes_and_messages(count, indexes, decrypted_messages);
            decrypted_batches.push(decrypted_batch);
        }

        Ok(IggyMessagesBatchSet::from_vec(decrypted_batches))
    }

    fn encrypt_messages(
        &self,
        batch: IggyMessagesBatchMut,
        encryptor: &EncryptorKind,
    ) -> Result<IggyMessagesBatchMut, IggyError> {
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
