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
use crate::shard_trace;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet};
use crate::streaming::session::Session;
use crate::streaming::traits::MainOps;
use crate::streaming::utils::{PooledBuffer, hash};
use crate::streaming::{partitions, streams, topics};
use error_set::ErrContext;
use iggy_common::{
    BytesSerializable, Consumer, EncryptorKind, IGGY_MESSAGE_HEADER_SIZE, Identifier, IggyError,
    IggyTimestamp, Partitioning, PartitioningKind, PollingKind, PollingStrategy,
};
use std::sync::atomic::Ordering;
use tracing::{error, trace};

impl IggyShard {
    pub async fn append_messages(
        &self,
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        partitioning: &Partitioning,
        batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        // TODO: move to helpers.
        fn calculate_partition_id_by_messages_key_hash(
            shard_id: u16,
            upperbound: usize,
            messages_key: &[u8],
        ) -> usize {
            let messages_key_hash = hash::calculate_32(messages_key) as usize;
            let mut partition_id = messages_key_hash % upperbound;
            if partition_id == 0 {
                partition_id = upperbound;
            }
            shard_trace!(
                shard_id,
                "Calculated partition ID: {} for messages key: {:?}, hash: {}",
                partition_id,
                messages_key,
                messages_key_hash
            );
            partition_id
        }

        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(&stream_id, streams::helpers::get_stream_id());

        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(&stream_id, &topic_id, topics::helpers::get_topic_id());

        // Validate permissions for given user on stream and topic.
        self.permissioner
            .borrow()
            .append_messages(
                user_id,
                numeric_stream_id as u32,
                numeric_topic_id as u32,
            )
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - permission denied to append messages for user {} on stream ID: {}, topic ID: {}", user_id, numeric_stream_id as u32, numeric_topic_id as u32)
            })?;

        if batch.count() == 0 {
            return Ok(());
        }

        let partition_id =
            self.streams2
                .with_topic_by_id(
                    &stream_id,
                    &topic_id,
                    |(root, auxilary, ..)| match partitioning.kind {
                        PartitioningKind::Balanced => {
                            let upperbound = root.partitions().len();
                            Ok(auxilary.get_next_partition_id(self.id, upperbound))
                        }
                        PartitioningKind::PartitionId => Ok(u32::from_le_bytes(
                            partitioning.value[..partitioning.length as usize]
                                .try_into()
                                .map_err(|_| IggyError::InvalidNumberEncoding)?,
                        ) as usize),
                        PartitioningKind::MessagesKey => {
                            let upperbound = root.partitions().len();
                            Ok(calculate_partition_id_by_messages_key_hash(
                                self.id,
                                upperbound,
                                &partitioning.value,
                            ))
                        }
                    },
                )?;

        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
        let payload = ShardRequestPayload::SendMessages { batch };
        let request = ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
        let message = ShardMessage::Request(request);
        match self
            .send_request_to_shard_or_recoil(&namespace, message)
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
                    self.streams2
                        .append_messages(
                            self.id,
                            &self.config.system,
                            &self.task_registry,
                            &ns,
                            batch,
                        )
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
        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(&stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(&stream_id, &topic_id, topics::helpers::get_topic_id());

        self.permissioner
            .borrow()
            .poll_messages(user_id, numeric_stream_id as u32, numeric_topic_id as u32)
            .with_error_context(|error| format!(
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
        ) else {
            return Ok((IggyPollMetadata::new(0, 0), IggyMessagesBatchSet::empty()));
        };

        let has_partition = self
            .streams2
            .with_topic_by_id(&stream_id, &topic_id, |(root, ..)| {
                root.partitions().exists(partition_id)
            });
        if !has_partition {
            return Err(IggyError::NoPartitions(
                numeric_topic_id as u32,
                numeric_stream_id as u32,
            ));
        }

        let current_offset = self.streams2.with_partition_by_id(
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
            .send_request_to_shard_or_recoil(&namespace, message)
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
                        self.streams2.poll_messages(&ns, consumer, args).await?;
                    let stream_id = ns.stream_id();
                    let topic_id = ns.topic_id();

                    if auto_commit && !batches.is_empty() {
                        let offset = batches
                            .last_offset()
                            .expect("Batch set should have at least one batch");
                        trace!(
                            "Last offset: {} will be automatically stored for {}, stream: {}, topic: {}, partition: {}",
                            offset, consumer, numeric_stream_id, numeric_topic_id, partition_id
                        );
                        match consumer {
                            PollingConsumer::Consumer(consumer_id, _) => {
                                self.streams2.with_partition_by_id(
                                    &stream_id,
                                    &topic_id,
                                    partition_id,
                                    partitions::helpers::store_consumer_offset(
                                        consumer_id,
                                        numeric_stream_id,
                                        numeric_topic_id,
                                        partition_id,
                                        offset,
                                        &self.config.system,
                                    ),
                                );

                                let (offset_value, path) = self.streams2.with_partition_by_id(
                                    &stream_id,
                                    &topic_id,
                                    partition_id,
                                    |(.., offsets, _, _)| {
                                        let hdl = offsets.pin();
                                        let item = hdl.get(&consumer_id).expect(
                                            "persist_consumer_offset_to_disk: offset not found",
                                        );
                                        let offset =
                                            item.offset.load(std::sync::atomic::Ordering::Relaxed);
                                        let path = item.path.clone();
                                        (offset, path)
                                    },
                                );
                                partitions::storage2::persist_offset(self.id, &path, offset_value)
                                    .await?;
                            }
                            PollingConsumer::ConsumerGroup(cg_id, _) => {
                                self.streams2.with_partition_by_id(
                                    &stream_id,
                                    &topic_id,
                                    partition_id,
                                    partitions::helpers::store_consumer_group_member_offset(
                                        cg_id,
                                        numeric_stream_id,
                                        numeric_topic_id,
                                        partition_id,
                                        offset,
                                        &self.config.system,
                                    ),
                                );

                                let (offset_value, path) = self.streams2.with_partition_by_id(
                                    &stream_id,
                                    &topic_id,
                                    partition_id,
                                    |(.., offsets, _)| {
                                        let hdl = offsets.pin();
                                        let item = hdl
                                            .get(&cg_id)
                                            .expect("persist_consumer_group_member_offset_to_disk: offset not found");
                                        let offset = item.offset.load(std::sync::atomic::Ordering::Relaxed);
                                        let path = item.path.clone();
                                        (offset, path)
                                    },
                                );
                                partitions::storage2::persist_offset(self.id, &path, offset_value)
                                    .await?;
                            }
                        }
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
        session: &Session,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        todo!();
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
