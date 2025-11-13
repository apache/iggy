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
use crate::shard::namespace::{IggyFullNamespace, IggyNamespace};
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet};
use crate::streaming::traits::MainOps;
use crate::streaming::utils::PooledBuffer;
use crate::streaming::{partitions, streams, topics};
use err_trail::ErrContext;
use iggy_common::{
    BytesSerializable, Consumer, EncryptorKind, IGGY_MESSAGE_HEADER_SIZE, Identifier, IggyError,
    Partitioning, PartitioningKind, PollingKind, PollingStrategy,
};
use std::sync::atomic::Ordering;
use tracing::error;

impl IggyShard {
    pub async fn append_messages(
        &self,
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        partitioning: &Partitioning,
        batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        self.ensure_topic_exists(&stream_id, &topic_id)?;

        let numeric_stream_id = self
            .streams
            .with_stream_by_id(&stream_id, streams::helpers::get_stream_id());

        let numeric_topic_id =
            self.streams
                .with_topic_by_id(&stream_id, &topic_id, topics::helpers::get_topic_id());

        // Validate permissions for given user on stream and topic.
        self.permissioner
            .borrow()
            .append_messages(
                user_id,
                numeric_stream_id,
                numeric_topic_id,
            )
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - permission denied to append messages for user {} on stream ID: {}, topic ID: {}", user_id, numeric_stream_id as u32, numeric_topic_id as u32)
            })?;

        if batch.count() == 0 {
            return Ok(());
        }

        let partition_id =
            self.streams
                .with_topic_by_id(
                    &stream_id,
                    &topic_id,
                    |(root, auxilary, ..)| match partitioning.kind {
                        PartitioningKind::Balanced => {
                            let upperbound = root.partitions().len();
                            Ok(auxilary.get_next_partition_id(upperbound))
                        }
                        PartitioningKind::PartitionId => Ok(u32::from_le_bytes(
                            partitioning.value[..partitioning.length as usize]
                                .try_into()
                                .map_err(|_| IggyError::InvalidNumberEncoding)?,
                        ) as usize),
                        PartitioningKind::MessagesKey => {
                            let upperbound = root.partitions().len();
                            Ok(
                                topics::helpers::calculate_partition_id_by_messages_key_hash(
                                    upperbound,
                                    &partitioning.value,
                                ),
                            )
                        }
                    },
                )?;

        self.ensure_partition_exists(&stream_id, &topic_id, partition_id)?;
        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
        let payload = ShardRequestPayload::SendMessages { batch };
        let request = ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
        let message = ShardMessage::Request(request);
        match self
            .send_request_to_shard_or_recoil(Some(&namespace), message)
            .await?
        {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest {
                    stream_id,
                    topic_id,
                    partition_id,
                    payload,
                }) = message
                    && let ShardRequestPayload::SendMessages { batch } = payload
                {
                    let ns = IggyFullNamespace::new(stream_id, topic_id, partition_id);
                    // Encrypt messages if encryptor is enabled in configuration.
                    let batch = self.maybe_encrypt_messages(batch)?;
                    let messages_count = batch.count();
                    self.streams
                        .append_messages(&self.config.system, &self.task_registry, &ns, batch)
                        .await?;
                    self.metrics.increment_messages(messages_count as u64);
                    Ok(())
                } else {
                    unreachable!(
                        "Expected a SendMessages request inside of SendMessages handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::SendMessages => Ok(()),
                ShardResponse::ErrorResponse(err) => Err(err),
                _ => unreachable!(
                    "Expected a SendMessages response inside of SendMessages handler, impossible state"
                ),
            },
        }?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn poll_messages(
        &self,
        client_id: u32,
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        consumer: Consumer,
        maybe_partition_id: Option<u32>,
        args: PollingArgs,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        self.ensure_topic_exists(&stream_id, &topic_id)?;

        let numeric_stream_id = self
            .streams
            .with_stream_by_id(&stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.streams
                .with_topic_by_id(&stream_id, &topic_id, topics::helpers::get_topic_id());

        self.permissioner
            .borrow()
            .poll_messages(user_id, numeric_stream_id, numeric_topic_id)
            .with_error(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to poll messages for user {} on stream ID: {}, topic ID: {}",
                user_id,
                stream_id,
                numeric_topic_id
            ))?;

        // Resolve partition ID
        let Some((consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            &stream_id,
            &topic_id,
            &consumer,
            client_id,
            maybe_partition_id,
            true,
        )?
        else {
            return Ok((IggyPollMetadata::new(0, 0), IggyMessagesBatchSet::empty()));
        };

        self.ensure_partition_exists(&stream_id, &topic_id, partition_id)?;

        let current_offset = self.streams.with_partition_by_id(
            &stream_id,
            &topic_id,
            partition_id,
            |(_, _, _, offset, ..)| offset.load(Ordering::Relaxed),
        );
        if args.strategy.kind == PollingKind::Offset && args.strategy.value > current_offset
            || args.count == 0
        {
            return Ok((
                IggyPollMetadata::new(partition_id as u32, current_offset),
                IggyMessagesBatchSet::empty(),
            ));
        }

        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
        let payload = ShardRequestPayload::PollMessages { consumer, args };
        let request = ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
        let message = ShardMessage::Request(request);
        let (metadata, batch) = match self
            .send_request_to_shard_or_recoil(Some(&namespace), message)
            .await?
        {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest {
                    partition_id,
                    payload,
                    ..
                }) = message
                    && let ShardRequestPayload::PollMessages { consumer, args } = payload
                {
                    let ns = IggyFullNamespace::new(stream_id, topic_id, partition_id);
                    let auto_commit = args.auto_commit;
                    let (metadata, batches) =
                        self.streams.poll_messages(&ns, consumer, args).await?;
                    let stream_id = ns.stream_id();
                    let topic_id = ns.topic_id();

                    if auto_commit && !batches.is_empty() {
                        let offset = batches
                            .last_offset()
                            .expect("Batch set should have at least one batch");
                        self.streams
                            .auto_commit_consumer_offset(
                                &self.config.system,
                                stream_id,
                                topic_id,
                                partition_id,
                                consumer,
                                offset,
                            )
                            .await?;
                    }
                    Ok((metadata, batches))
                } else {
                    unreachable!(
                        "Expected a PollMessages request inside of PollMessages handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::PollMessages(result) => Ok(result),
                ShardResponse::ErrorResponse(err) => Err(err),
                _ => unreachable!(
                    "Expected a SendMessages response inside of SendMessages handler, impossible state"
                ),
            },
        }?;

        let batch = if let Some(encryptor) = &self.encryptor {
            self.decrypt_messages(batch, encryptor).await?
        } else {
            batch
        };

        Ok((metadata, batch))
    }

    pub async fn flush_unsaved_buffer(
        &self,
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        fsync: bool,
    ) -> Result<(), IggyError> {
        self.ensure_partition_exists(&stream_id, &topic_id, partition_id)?;

        let numeric_stream_id = self
            .streams
            .with_stream_by_id(&stream_id, streams::helpers::get_stream_id());

        let numeric_topic_id =
            self.streams
                .with_topic_by_id(&stream_id, &topic_id, topics::helpers::get_topic_id());

        // Validate permissions for given user on stream and topic.
        self.permissioner
            .borrow()
            .append_messages(user_id, numeric_stream_id, numeric_topic_id)
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - permission denied to flush unsaved buffer for user {} on stream ID: {}, topic ID: {}", user_id, numeric_stream_id as u32, numeric_topic_id as u32)
            })?;

        self.ensure_partition_exists(&stream_id, &topic_id, partition_id)?;

        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
        let payload = ShardRequestPayload::FlushUnsavedBuffer { fsync };
        let request = ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
        let message = ShardMessage::Request(request);
        match self
            .send_request_to_shard_or_recoil(Some(&namespace), message)
            .await?
        {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest {
                    stream_id,
                    topic_id,
                    partition_id,
                    payload,
                }) = message
                    && let ShardRequestPayload::FlushUnsavedBuffer { fsync } = payload
                {
                    self.flush_unsaved_buffer_base(&stream_id, &topic_id, partition_id, fsync)
                        .await?;
                    Ok(())
                } else {
                    unreachable!(
                        "Expected a FlushUnsavedBuffer request inside of FlushUnsavedBuffer handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::FlushUnsavedBuffer => Ok(()),
                ShardResponse::ErrorResponse(err) => Err(err),
                _ => unreachable!(
                    "Expected a FlushUnsavedBuffer response inside of FlushUnsavedBuffer handler, impossible state"
                ),
            },
        }?;

        Ok(())
    }

    pub(crate) async fn flush_unsaved_buffer_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        fsync: bool,
    ) -> Result<(), IggyError> {
        let batches = self.streams.with_partition_by_id_mut(
            stream_id,
            topic_id,
            partition_id,
            partitions::helpers::commit_journal(),
        );

        self.streams
            .persist_messages_to_disk(
                stream_id,
                topic_id,
                partition_id,
                batches,
                &self.config.system,
            )
            .await?;

        // Ensure all data is flushed to disk before returning
        if fsync {
            self.streams
                .fsync_all_messages(stream_id, topic_id, partition_id)
                .await?;
        }

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
                        // Update the header with the decrypted payload length
                        let mut header = message.header().to_header();
                        header.payload_length = payload.len() as u32;

                        decrypted_messages.extend_from_slice(&header.to_bytes());
                        decrypted_messages.extend_from_slice(&payload);
                        if let Some(user_headers) = message.user_headers() {
                            decrypted_messages.extend_from_slice(user_headers);
                        }
                        position += IGGY_MESSAGE_HEADER_SIZE
                            + payload.len()
                            + message.header().user_headers_length();
                        indexes.insert(0, position as u32, 0);
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
            let header = message.header().to_header();
            let user_headers_length = header.user_headers_length;
            let payload_bytes = message.payload();
            let user_headers_bytes = message.user_headers();

            let encrypted_payload = encryptor.encrypt(payload_bytes);
            match encrypted_payload {
                Ok(encrypted_payload) => {
                    let mut updated_header = header;
                    updated_header.payload_length = encrypted_payload.len() as u32;

                    encrypted_messages.extend_from_slice(&updated_header.to_bytes());
                    encrypted_messages.extend_from_slice(&encrypted_payload);
                    if let Some(user_headers_bytes) = user_headers_bytes {
                        encrypted_messages.extend_from_slice(user_headers_bytes);
                    }
                    position += IGGY_MESSAGE_HEADER_SIZE
                        + encrypted_payload.len()
                        + user_headers_length as usize;
                    indexes.insert(0, position as u32, 0);
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
