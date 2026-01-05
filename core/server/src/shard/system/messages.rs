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
    PollingKind, PollingStrategy,
};
use tracing::{error, trace};

impl IggyShard {
    pub async fn append_messages(
        &self,
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        self.ensure_topic_exists(&stream_id, &topic_id)?;

        // Get IDs from SharedMetadata (source of truth for cross-shard consistency)
        let metadata = self.shared_metadata.load();
        let numeric_stream_id = metadata
            .get_stream_id(&stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = metadata
            .get_topic_id(numeric_stream_id, &topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;
        drop(metadata);

        // Validate permissions for given user on stream and topic.
        self.permissioner
            .append_messages(
                user_id,
                numeric_stream_id,
                numeric_topic_id,
            )
            .error(|e: &IggyError| {
                format!("{COMPONENT} (error: {e}) - permission denied to append messages for user {} on stream ID: {}, topic ID: {}", user_id, numeric_stream_id as u32, numeric_topic_id as u32)
            })?;

        if batch.count() == 0 {
            return Ok(());
        }

        self.ensure_partition_exists(&stream_id, &topic_id, partition_id)?;

        // TODO(tungtose): DRY this code
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
                    // Convert identifiers to numeric IDs using SharedMetadata
                    let metadata = self.shared_metadata.load();
                    let numeric_stream_id = metadata
                        .get_stream_id(&stream_id)
                        .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
                    let numeric_topic_id = metadata
                        .get_topic_id(numeric_stream_id, &topic_id)
                        .ok_or_else(|| {
                            IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone())
                        })?;
                    drop(metadata);

                    let namespace =
                        IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);

                    // Ensure partition exists in partition_store (lazy init if needed)
                    if !self.partition_store.borrow().contains(&namespace) {
                        self.ensure_local_partition(&stream_id, &topic_id, partition_id)
                            .await?;
                    }

                    let batch = self.maybe_encrypt_messages(batch)?;
                    let messages_count = batch.count();
                    self.append_to_partition_store(&namespace, batch).await?;
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

        // Get IDs from SharedMetadata (source of truth for cross-shard consistency)
        let metadata = self.shared_metadata.load();
        let numeric_stream_id = metadata
            .get_stream_id(&stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = metadata
            .get_topic_id(numeric_stream_id, &topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;
        drop(metadata);

        self.permissioner
            .poll_messages(user_id, numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| format!(
                "{COMPONENT} (error: {e}) - permission denied to poll messages for user {} on stream ID: {}, topic ID: {}",
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

        // Early return for zero count (this is universal)
        if args.count == 0 {
            let current_offset = self
                .shared_metadata
                .get_partition_offset(numeric_stream_id, numeric_topic_id, partition_id)
                .unwrap_or(0);
            return Ok((
                IggyPollMetadata::new(partition_id as u32, current_offset),
                IggyMessagesBatchSet::empty(),
            ));
        }

        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);

        // Only do the offset range check if this shard owns the partition.
        // For cross-shard requests, the owning shard will do this check.
        let owns_partition = self
            .find_shard(&namespace)
            .is_some_and(|shard| shard.id == self.id);

        if owns_partition && args.strategy.kind == PollingKind::Offset {
            let current_offset = self
                .shared_metadata
                .get_partition_offset(numeric_stream_id, numeric_topic_id, partition_id)
                .unwrap_or(0);
            if args.strategy.value > current_offset {
                return Ok((
                    IggyPollMetadata::new(partition_id as u32, current_offset),
                    IggyMessagesBatchSet::empty(),
                ));
            }
        }

        // Namespace already created above, just reuse it
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
                    // Convert identifiers to numeric IDs using SharedMetadata
                    let metadata_guard = self.shared_metadata.load();
                    let numeric_stream_id = metadata_guard
                        .get_stream_id(&stream_id)
                        .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
                    let numeric_topic_id = metadata_guard
                        .get_topic_id(numeric_stream_id, &topic_id)
                        .ok_or_else(|| {
                            IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone())
                        })?;
                    drop(metadata_guard);

                    let namespace =
                        IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
                    let auto_commit = args.auto_commit;

                    // Ensure partition exists in partition_store (lazy init if needed)
                    if !self.partition_store.borrow().contains(&namespace) {
                        self.ensure_local_partition(&stream_id, &topic_id, partition_id)
                            .await?;
                    }

                    let (poll_metadata, batches) = self
                        .poll_from_partition_store(&namespace, partition_id, consumer, args)
                        .await?;

                    // Handle auto_commit via partition_store
                    if auto_commit && !batches.is_empty() {
                        let offset = batches
                            .last_offset()
                            .expect("Batch set should have at least one batch");
                        self.store_consumer_offset_in_partition_store(&namespace, consumer, offset)
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
        self.ensure_partition_exists(&stream_id, &topic_id, partition_id)?;

        // Get IDs from SharedMetadata (source of truth for cross-shard consistency)
        let metadata = self.shared_metadata.load();
        let numeric_stream_id = metadata
            .get_stream_id(&stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = metadata
            .get_topic_id(numeric_stream_id, &topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;
        drop(metadata);

        // Validate permissions for given user on stream and topic.
        self.permissioner
            .append_messages(user_id, numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!("{COMPONENT} (error: {e}) - permission denied to flush unsaved buffer for user {} on stream ID: {}, topic ID: {}", user_id, numeric_stream_id as u32, numeric_topic_id as u32)
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
                    // Ensure partition is lazily loaded (creates slab hierarchy if needed)
                    self.ensure_local_partition(&stream_id, &topic_id, partition_id)
                        .await?;

                    // Convert identifiers to numeric IDs using SharedMetadata
                    let metadata_guard = self.shared_metadata.load();
                    let numeric_stream_id = metadata_guard
                        .get_stream_id(&stream_id)
                        .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
                    let numeric_topic_id = metadata_guard
                        .get_topic_id(numeric_stream_id, &topic_id)
                        .ok_or_else(|| {
                            IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone())
                        })?;
                    drop(metadata_guard);

                    let stream_ident =
                        Identifier::numeric(numeric_stream_id as u32).expect("valid stream id");
                    let topic_ident =
                        Identifier::numeric(numeric_topic_id as u32).expect("valid topic id");
                    self.flush_unsaved_buffer_base(
                        &stream_ident,
                        &topic_ident,
                        partition_id,
                        fsync,
                    )
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
        // Resolve identifiers to numeric IDs
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);

        // Get partition from partition_store
        let rc_partition = self.partition_store.borrow().get_rc(&namespace).ok_or(
            IggyError::PartitionNotFound(partition_id, topic_id.clone(), stream_id.clone()),
        )?;

        // Persist messages to disk
        {
            let mut partition_data = rc_partition.borrow_mut();
            partition_data
                .persist_messages("flush_unsaved_buffer", &self.config.system)
                .await?;
        }

        // Ensure all data is flushed to disk before returning
        if fsync {
            let partition_data = rc_partition.borrow();
            partition_data.fsync_all_messages().await?;
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

    /// Append messages using partition_store for direct data access.
    ///
    /// This method uses SharedMetadata for validation (lock-free reads) and
    /// partition_store for data operations, bypassing the slab closure pattern.
    pub async fn append_messages_via_store(
        &self,
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        // Validate and resolve IDs via SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(&stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, &topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        // Validate partition exists via SharedMetadata
        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
        if !self.shared_metadata.partition_exists(&namespace) {
            return Err(IggyError::PartitionNotFound(
                partition_id,
                topic_id.clone(),
                stream_id.clone(),
            ));
        }

        // Validate permissions
        self.permissioner
            .append_messages(user_id, numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to append messages for user {} on stream ID: {}, topic ID: {}",
                    user_id, numeric_stream_id as u32, numeric_topic_id as u32
                )
            })?;

        if batch.count() == 0 {
            return Ok(());
        }

        // Route request to owning shard
        let payload = ShardRequestPayload::SendMessages { batch };
        let request = ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
        let message = ShardMessage::Request(request);

        match self
            .send_request_to_shard_or_recoil(Some(&namespace), message)
            .await?
        {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::SendMessages { batch } = payload
                {
                    // Encrypt if configured
                    let batch = self.maybe_encrypt_messages(batch)?;
                    let messages_count = batch.count();

                    // Append directly to partition_store
                    self.append_to_partition_store(&namespace, batch).await?;
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

    /// Append a batch of messages directly to partition_store.
    ///
    /// This bypasses the slab closure pattern for O(1) partition access.
    /// Used by handlers when processing routed SendMessages requests.
    ///
    /// Offset is read/written from SharedMetadata's PartitionMeta.offset,
    /// ensuring cross-shard consistency.
    pub(crate) async fn append_to_partition_store(
        &self,
        namespace: &IggyNamespace,
        mut batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        // Get the shared partition offset from SharedMetadata (source of truth)
        let partition_meta = self
            .shared_metadata
            .get_partition(namespace)
            .ok_or_else(|| {
                IggyError::PartitionNotFound(
                    namespace.partition_id(),
                    Identifier::numeric(namespace.topic_id() as u32).expect("valid topic id"),
                    Identifier::numeric(namespace.stream_id() as u32).expect("valid stream id"),
                )
            })?;

        // Extract partition data synchronously to get values needed for prepare_for_persistence
        let (segment_start_offset, current_offset, current_position, deduplicator) = {
            let store = self.partition_store.borrow();
            let rc_partition = store.get(namespace).ok_or_else(|| {
                IggyError::PartitionNotFound(
                    namespace.partition_id(),
                    Identifier::numeric(namespace.topic_id() as u32).expect("valid topic id"),
                    Identifier::numeric(namespace.stream_id() as u32).expect("valid stream id"),
                )
            })?;

            let partition_data = rc_partition.borrow();
            let segment = partition_data.log.active_segment();
            let segment_start_offset = segment.start_offset;
            let current_position = segment.current_position;
            // Use SharedMetadata's offset (not the cached one in partition_data)
            let current_offset = partition_meta.next_offset();
            let deduplicator = partition_data.message_deduplicator.clone();
            (
                segment_start_offset,
                current_offset,
                current_position,
                deduplicator,
            )
        };

        // Prepare messages for persistence (sets offsets, timestamps, checksums)
        // This is async due to deduplication checks
        batch
            .prepare_for_persistence(
                segment_start_offset,
                current_offset,
                current_position,
                deduplicator.as_ref(),
            )
            .await;

        // Now do the actual append synchronously
        let store = self.partition_store.borrow();
        let rc_partition = store.get(namespace).ok_or_else(|| {
            IggyError::PartitionNotFound(
                namespace.partition_id(),
                Identifier::numeric(namespace.topic_id() as u32).expect("valid topic id"),
                Identifier::numeric(namespace.stream_id() as u32).expect("valid stream id"),
            )
        })?;

        let mut partition_data = rc_partition.borrow_mut();

        // Extract batch metadata (now with correct offsets from prepare_for_persistence)
        let batch_messages_size = batch.size();
        let batch_messages_count = batch.count();
        let first_timestamp = batch.first_timestamp().unwrap_or(0);
        let last_timestamp = batch.last_timestamp().unwrap_or(0);

        // Update stats
        partition_data
            .stats
            .increment_size_bytes(batch_messages_size as u64);
        partition_data
            .stats
            .increment_messages_count(batch_messages_count as u64);

        // Append to journal
        partition_data.log.journal_mut().append(batch)?;

        // Update segment metadata
        let segment = partition_data.log.active_segment_mut();
        if segment.start_timestamp == 0 {
            segment.start_timestamp = first_timestamp;
        }
        segment.end_timestamp = last_timestamp;
        segment.current_position += batch_messages_size;

        // Calculate and store the last offset in SharedMetadata (source of truth)
        let last_offset = if batch_messages_count == 0 {
            current_offset
        } else {
            current_offset + batch_messages_count as u64 - 1
        };
        segment.end_offset = last_offset;

        // Store offset in SharedMetadata's PartitionMeta (shared across all shards)
        partition_meta.store_offset(last_offset);

        // Update partition stats offset (needed for poll_messages which uses stats.current_offset())
        partition_data.stats.set_current_offset(last_offset);

        trace!(
            "Appended {} messages ({} bytes) to partition {:?}, new offset: {}",
            batch_messages_count, batch_messages_size, namespace, last_offset
        );

        Ok(())
    }

    /// Poll messages directly from partition_store.
    ///
    /// This bypasses the slab closure pattern for O(1) partition access.
    /// Used by handlers when processing routed PollMessages requests.
    #[allow(clippy::await_holding_refcell_ref)]
    pub(crate) async fn poll_from_partition_store(
        &self,
        namespace: &IggyNamespace,
        partition_id: usize,
        consumer: PollingConsumer,
        args: PollingArgs,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        let store = self.partition_store.borrow();
        let rc_partition = store.get(namespace).ok_or_else(|| {
            IggyError::PartitionNotFound(
                namespace.partition_id(),
                Identifier::numeric(namespace.topic_id() as u32).expect("valid topic id"),
                Identifier::numeric(namespace.stream_id() as u32).expect("valid stream id"),
            )
        })?;

        let partition_data = rc_partition.borrow();
        partition_data
            .poll_messages(partition_id, consumer, args)
            .await
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

    /// Store consumer offset in partition_store for auto_commit.
    pub(crate) async fn store_consumer_offset_in_partition_store(
        &self,
        namespace: &IggyNamespace,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        let rc_partition = self
            .partition_store
            .borrow()
            .get_rc(namespace)
            .ok_or_else(|| {
                IggyError::PartitionNotFound(
                    namespace.partition_id(),
                    Identifier::numeric(namespace.topic_id() as u32).expect("valid topic id"),
                    Identifier::numeric(namespace.stream_id() as u32).expect("valid stream id"),
                )
            })?;

        let partition_data = rc_partition.borrow();
        partition_data
            .store_consumer_offset(&self.config.system, consumer, offset)
            .await
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
