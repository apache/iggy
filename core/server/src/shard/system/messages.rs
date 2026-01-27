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
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::streaming::partitions::journal::Journal;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet};
use err_trail::ErrContext;
use iggy_common::PooledBuffer;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{
    BytesSerializable, Consumer, EncryptorKind, IGGY_MESSAGE_HEADER_SIZE, Identifier, IggyError,
    PollingStrategy,
};
use std::sync::atomic::Ordering;
use tracing::error;

impl IggyShard {
    pub async fn append_messages(
        &self,
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        let (stream, topic, _) = self.resolve_partition_id(&stream_id, &topic_id, partition_id)?;

        self.metadata
            .perm_append_messages(user_id, stream, topic)
            .error(|e: &IggyError| {
                format!("{COMPONENT} (error: {e}) - permission denied to append messages for user {} on stream ID: {}, topic ID: {}", user_id, stream as u32, topic as u32)
            })?;

        if batch.count() == 0 {
            return Ok(());
        }

        // TODO(tungtose): DRY this code
        let namespace = IggyNamespace::new(stream, topic, partition_id);
        let payload = ShardRequestPayload::SendMessages { batch };
        let request = ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
        let message = ShardMessage::Request(request);
        match self
            .send_request_to_shard_or_recoil(Some(&namespace), message)
            .await?
        {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest {
                    stream_id: _,
                    topic_id: _,
                    partition_id,
                    payload,
                }) = message
                    && let ShardRequestPayload::SendMessages { batch } = payload
                {
                    let batch = self.maybe_encrypt_messages(batch)?;
                    let messages_count = batch.count();

                    let namespace = IggyNamespace::new(stream, topic, partition_id);
                    self.ensure_partition(&namespace).await?;

                    self.append_messages_to_local_partition(&namespace, batch, &self.config.system)
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
        let (stream, topic) = self.resolve_topic_id(&stream_id, &topic_id)?;

        self.metadata
            .perm_poll_messages(user_id, stream, topic)
            .error(|e: &IggyError| format!(
                "{COMPONENT} (error: {e}) - permission denied to poll messages for user {} on stream ID: {}, topic ID: {}",
                user_id,
                stream_id,
                topic
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

        let namespace = IggyNamespace::new(stream, topic, partition_id);

        if args.count == 0 {
            let current_offset = self
                .local_partitions
                .borrow()
                .get(&namespace)
                .map(|data| data.offset.load(Ordering::Relaxed))
                .unwrap_or(0);
            return Ok((
                IggyPollMetadata::new(partition_id as u32, current_offset),
                IggyMessagesBatchSet::empty(),
            ));
        }

        // Offset validation is done by the owning shard after routing
        let payload = ShardRequestPayload::PollMessages { consumer, args };
        let request = ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
        let message = ShardMessage::Request(request);
        let (metadata, batch) = match self
            .send_request_to_shard_or_recoil(Some(&namespace), message)
            .await?
        {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest {
                    partition_id: _,
                    payload,
                    ..
                }) = message
                    && let ShardRequestPayload::PollMessages { consumer, args } = payload
                {
                    self.ensure_partition(&namespace).await?;

                    let auto_commit = args.auto_commit;

                    let (poll_metadata, batches) = self
                        .poll_messages_from_local_partition(&namespace, consumer, args)
                        .await?;

                    if auto_commit && !batches.is_empty() {
                        let offset = batches
                            .last_offset()
                            .expect("Batch set should have at least one batch");
                        self.auto_commit_consumer_offset_from_local_partition(
                            &namespace, consumer, offset,
                        )
                        .await?;
                    }
                    Ok((poll_metadata, batches))
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
        let (stream, topic, _) = self.resolve_partition_id(&stream_id, &topic_id, partition_id)?;

        self.metadata
            .perm_append_messages(user_id, stream, topic)
            .error(|e: &IggyError| {
                format!("{COMPONENT} (error: {e}) - permission denied to flush unsaved buffer for user {} on stream ID: {}, topic ID: {}", user_id, stream as u32, topic as u32)
            })?;

        let namespace = IggyNamespace::new(stream, topic, partition_id);
        let payload = ShardRequestPayload::FlushUnsavedBuffer { fsync };
        let request = ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
        let message = ShardMessage::Request(request);
        match self
            .send_request_to_shard_or_recoil(Some(&namespace), message)
            .await?
        {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest {
                    partition_id,
                    payload,
                    ..
                }) = message
                    && let ShardRequestPayload::FlushUnsavedBuffer { fsync } = payload
                {
                    let namespace = IggyNamespace::new(stream, topic, partition_id);
                    self.flush_unsaved_buffer_from_local_partitions(&namespace, fsync)
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
        stream: usize,
        topic: usize,
        partition_id: usize,
        fsync: bool,
    ) -> Result<u32, IggyError> {
        let namespace = IggyNamespace::new(stream, topic, partition_id);
        self.flush_unsaved_buffer_from_local_partitions(&namespace, fsync)
            .await
    }

    /// Flushes unsaved messages from the partition store to disk.
    /// Returns the number of messages saved.
    pub(crate) async fn flush_unsaved_buffer_from_local_partitions(
        &self,
        namespace: &IggyNamespace,
        fsync: bool,
    ) -> Result<u32, IggyError> {
        let write_lock = {
            let partitions = self.local_partitions.borrow();
            let Some(partition) = partitions.get(namespace) else {
                return Ok(0);
            };
            partition.write_lock.clone()
        };

        let _write_guard = write_lock.lock().await;

        let frozen_batches = {
            let mut partitions = self.local_partitions.borrow_mut();
            let Some(partition) = partitions.get_mut(namespace) else {
                return Ok(0);
            };
            if !partition.log.has_segments() || partition.log.journal().is_empty() {
                return Ok(0);
            }
            let batches = partition.log.journal_mut().commit();
            partition.log.ensure_indexes();
            batches.append_indexes_to(partition.log.active_indexes_mut().unwrap());

            let frozen: Vec<_> = batches
                .into_inner()
                .into_iter()
                .map(|mut b| b.freeze())
                .collect();
            partition.log.set_in_flight(frozen.clone());
            frozen
        };

        let saved_count = self
            .persist_frozen_batches_to_disk(namespace, frozen_batches)
            .await?;

        if fsync {
            self.fsync_all_messages_from_local_partitions(namespace)
                .await?;
        }

        Ok(saved_count)
    }

    pub(crate) async fn fsync_all_messages_from_local_partitions(
        &self,
        namespace: &IggyNamespace,
    ) -> Result<(), IggyError> {
        let storage = {
            let partitions = self.local_partitions.borrow();
            let Some(partition) = partitions.get(namespace) else {
                return Ok(());
            };
            if !partition.log.has_segments() {
                return Ok(());
            }
            partition.log.active_storage().clone()
        };

        if storage.messages_writer.is_none() || storage.index_writer.is_none() {
            return Ok(());
        }

        if let Some(ref messages_writer) = storage.messages_writer
            && let Err(e) = messages_writer.fsync().await
        {
            tracing::error!(
                "Failed to fsync messages writer for partition {:?}: {}",
                namespace,
                e
            );
            return Err(e);
        }

        if let Some(ref index_writer) = storage.index_writer
            && let Err(e) = index_writer.fsync().await
        {
            tracing::error!(
                "Failed to fsync index writer for partition {:?}: {}",
                namespace,
                e
            );
            return Err(e);
        }

        Ok(())
    }

    pub(crate) async fn auto_commit_consumer_offset_from_local_partition(
        &self,
        namespace: &IggyNamespace,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        let (offset_value, path) = {
            let partitions = self.local_partitions.borrow();
            let partition = partitions.get(namespace).ok_or_else(|| {
                IggyError::PartitionNotFound(
                    namespace.partition_id(),
                    Identifier::numeric(namespace.topic_id() as u32).unwrap(),
                    Identifier::numeric(namespace.stream_id() as u32).unwrap(),
                )
            })?;

            match consumer {
                PollingConsumer::Consumer(consumer_id, _) => {
                    tracing::trace!(
                        "Auto-committing offset {} for consumer {} on partition {:?}",
                        offset,
                        consumer_id,
                        namespace
                    );
                    let hdl = partition.consumer_offsets.pin();
                    let item = hdl.get_or_insert(
                        consumer_id,
                        crate::streaming::partitions::consumer_offset::ConsumerOffset::default_for_consumer(
                            consumer_id as u32,
                            &self.config.system.get_consumer_offsets_path(
                                namespace.stream_id(),
                                namespace.topic_id(),
                                namespace.partition_id(),
                            ),
                        ),
                    );
                    item.offset.store(offset, Ordering::Relaxed);
                    (item.offset.load(Ordering::Relaxed), item.path.clone())
                }
                PollingConsumer::ConsumerGroup(consumer_group_id, _) => {
                    tracing::trace!(
                        "Auto-committing offset {} for consumer group {} on partition {:?}",
                        offset,
                        consumer_group_id.0,
                        namespace
                    );
                    let hdl = partition.consumer_group_offsets.pin();
                    let item = hdl.get_or_insert(
                        consumer_group_id,
                        crate::streaming::partitions::consumer_offset::ConsumerOffset::default_for_consumer_group(
                            consumer_group_id,
                            &self.config.system.get_consumer_group_offsets_path(
                                namespace.stream_id(),
                                namespace.topic_id(),
                                namespace.partition_id(),
                            ),
                        ),
                    );
                    item.offset.store(offset, Ordering::Relaxed);
                    (item.offset.load(Ordering::Relaxed), item.path.clone())
                }
            }
        };

        crate::streaming::partitions::storage::persist_offset(&path, offset_value).await?;
        Ok(())
    }

    pub async fn append_messages_to_local_partition(
        &self,
        namespace: &IggyNamespace,
        mut batch: IggyMessagesBatchMut,
        config: &crate::configs::system::SystemConfig,
    ) -> Result<(), IggyError> {
        let write_lock = {
            let partitions = self.local_partitions.borrow();
            let partition = partitions
                .get(namespace)
                .expect("local_partitions: partition must exist");
            partition.write_lock.clone()
        };

        let _write_guard = write_lock.lock().await;

        let (
            current_offset,
            current_position,
            segment_start_offset,
            segment_index,
            message_deduplicator,
        ) = {
            let partitions = self.local_partitions.borrow();
            let partition = partitions
                .get(namespace)
                .expect("local_partitions: partition must exist");

            let current_offset = if partition.should_increment_offset {
                partition.offset.load(Ordering::Relaxed) + 1
            } else {
                0
            };

            let segment = partition.log.active_segment();
            let segment_index = partition.log.segments().len() - 1;

            (
                current_offset,
                segment.current_position,
                segment.start_offset,
                segment_index,
                partition.message_deduplicator.clone(),
            )
        };

        batch
            .prepare_for_persistence(
                segment_start_offset,
                current_offset,
                current_position,
                message_deduplicator.as_ref(),
            )
            .await;

        let (journal_messages_count, journal_size, is_full) = {
            let mut partitions = self.local_partitions.borrow_mut();
            let partition = partitions
                .get_mut(namespace)
                .expect("local_partitions: partition must exist");

            let segment = &mut partition.log.segments_mut()[segment_index];

            if segment.end_offset == 0 {
                segment.start_timestamp = batch.first_timestamp().unwrap();
            }

            let batch_messages_size = batch.size();
            let batch_messages_count = batch.count();

            partition
                .stats
                .increment_size_bytes(batch_messages_size as u64);
            partition
                .stats
                .increment_messages_count(batch_messages_count as u64);

            segment.end_timestamp = batch.last_timestamp().unwrap();
            segment.end_offset = batch.last_offset().unwrap();

            let (journal_messages_count, journal_size) =
                partition.log.journal_mut().append(batch)?;

            let last_offset = if batch_messages_count == 0 {
                current_offset
            } else {
                current_offset + batch_messages_count as u64 - 1
            };

            if partition.should_increment_offset {
                partition.offset.store(last_offset, Ordering::Relaxed);
            } else {
                partition.should_increment_offset = true;
                partition.offset.store(last_offset, Ordering::Relaxed);
            }
            partition.stats.set_current_offset(last_offset);
            partition.log.segments_mut()[segment_index].current_position += batch_messages_size;

            let is_full = partition.log.segments()[segment_index].is_full();

            (journal_messages_count, journal_size, is_full)
        };

        let unsaved_messages_count_exceeded =
            journal_messages_count >= config.partition.messages_required_to_save;
        let unsaved_messages_size_exceeded = journal_size
            >= config
                .partition
                .size_of_messages_required_to_save
                .as_bytes_u64() as u32;

        if is_full || unsaved_messages_count_exceeded || unsaved_messages_size_exceeded {
            let frozen_batches = {
                let mut partitions = self.local_partitions.borrow_mut();
                let partition = partitions
                    .get_mut(namespace)
                    .expect("local_partitions: partition must exist");
                let batches = partition.log.journal_mut().commit();
                partition.log.ensure_indexes();
                batches.append_indexes_to(partition.log.active_indexes_mut().unwrap());

                let frozen: Vec<_> = batches
                    .into_inner()
                    .into_iter()
                    .map(|mut b| b.freeze())
                    .collect();
                partition.log.set_in_flight(frozen.clone());
                frozen
            };

            self.persist_frozen_batches_to_disk(namespace, frozen_batches)
                .await?;

            if is_full {
                self.rotate_segment_in_local_partitions(namespace).await?;
            }
        }

        Ok(())
    }

    /// Persists already-frozen batches to disk. Caller must have set in_flight buffer.
    async fn persist_frozen_batches_to_disk(
        &self,
        namespace: &IggyNamespace,
        frozen_batches: Vec<iggy_common::IggyMessagesBatch>,
    ) -> Result<u32, IggyError> {
        let batch_count: u32 = frozen_batches.iter().map(|b| b.count()).sum();

        if batch_count == 0 {
            return Ok(0);
        }

        let (messages_writer, index_writer, segment_index) = {
            let partitions = self.local_partitions.borrow();
            let partition = partitions
                .get(namespace)
                .expect("local_partitions: partition must exist");

            if !partition.log.has_segments() {
                return Ok(0);
            }

            let segment_index = partition.log.segments().len() - 1;
            let messages_writer = partition
                .log
                .active_storage()
                .messages_writer
                .as_ref()
                .expect("Messages writer not initialized")
                .clone();
            let index_writer = partition
                .log
                .active_storage()
                .index_writer
                .as_ref()
                .expect("Index writer not initialized")
                .clone();
            (messages_writer, index_writer, segment_index)
        };

        let saved = messages_writer
            .as_ref()
            .save_frozen_batches(&frozen_batches)
            .await?;

        let unsaved_indexes_slice = {
            let partitions = self.local_partitions.borrow();
            let partition = partitions
                .get(namespace)
                .expect("local_partitions: partition must exist");
            partition.log.indexes()[segment_index]
                .as_ref()
                .expect("indexes must exist for segment being persisted")
                .unsaved_slice()
        };

        index_writer
            .as_ref()
            .save_indexes(unsaved_indexes_slice)
            .await?;

        tracing::trace!(
            "Persisted {} messages on disk for partition: {:?}, total bytes written: {}.",
            batch_count,
            namespace,
            saved
        );

        {
            let mut partitions = self.local_partitions.borrow_mut();
            let partition = partitions
                .get_mut(namespace)
                .expect("local_partitions: partition must exist");

            let indexes = partition.log.indexes_mut()[segment_index]
                .as_mut()
                .expect("indexes must exist for segment being persisted");
            indexes.mark_saved();

            let segment = &mut partition.log.segments_mut()[segment_index];
            segment.size =
                iggy_common::IggyByteSize::from(segment.size.as_bytes_u64() + saved.as_bytes_u64());

            partition.log.clear_in_flight();
        }

        Ok(batch_count)
    }

    pub async fn poll_messages_from_local_partition(
        &self,
        namespace: &IggyNamespace,
        consumer: crate::streaming::polling_consumer::PollingConsumer,
        args: PollingArgs,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        crate::streaming::partitions::ops::poll_messages(
            &self.local_partitions,
            namespace,
            consumer,
            args,
        )
        .await
    }

    async fn decrypt_messages(
        &self,
        batches: IggyMessagesBatchSet,
        encryptor: &EncryptorKind,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let mut decrypted_batches = Vec::with_capacity(batches.containers_count());
        for batch in batches.iter() {
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
                IggyMessagesBatchMut::from_indexes_and_messages(indexes, decrypted_messages);
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
