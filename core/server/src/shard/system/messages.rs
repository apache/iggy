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

use std::sync::atomic::Ordering;

use super::COMPONENT;
use crate::binary::handlers::messages::poll_messages_handler::IggyPollMetadata;
use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::shard_trace;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet};
use crate::streaming::session::Session;
use crate::streaming::utils::{PooledBuffer, hash};
use crate::streaming::{partitions, streams, topics};
use error_set::ErrContext;

use iggy_common::{
    BytesSerializable, Consumer, IGGY_MESSAGE_HEADER_SIZE, Identifier, IggyError, IggyTimestamp,
    Partitioning, PartitioningKind, PollingKind, PollingStrategy,
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
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());

        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());

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
                    stream_id,
                    topic_id,
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
        let payload = ShardRequestPayload::SendMessages { batch: batch };
        let request = ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
        let message = ShardMessage::Request(request);
        match self
            .send_request_to_shard_or_recoil(&namespace, message)
            .await?
        {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest {
                    partition_id,
                    payload,
                    ..
                }) = message
                    && let ShardRequestPayload::SendMessages { batch } = payload
                {
                    // Encrypt messages if encryptor is enabled in configuration.
                    let mut batch = self.maybe_encrypt_messages(batch)?;
                    let messages_count = batch.count();

                    let current_offset = self.streams2.with_partition_by_id(
                        stream_id,
                        topic_id,
                        partition_id,
                        partitions::helpers::calculate_current_offset(),
                    );

                    self.streams2
                        .with_partition_by_id_async(
                            stream_id,
                            topic_id,
                            partition_id,
                            partitions::helpers::deduplicate_messages(current_offset, &mut batch),
                        )
                        .await;

                    let (journal_messages_count, journal_size) =
                        self.streams2.with_partition_by_id_mut(
                            stream_id,
                            topic_id,
                            partition_id,
                            partitions::helpers::append_to_journal(self.id, current_offset, batch),
                        )?;

                    let unsaved_messages_count_exceeded = journal_messages_count
                        >= self.config.system.partition.messages_required_to_save;
                    let unsaved_messages_size_exceeded = journal_size
                        >= self
                            .config
                            .system
                            .partition
                            .size_of_messages_required_to_save
                            .as_bytes_u64() as u32;

                    let is_full = self.streams2.with_partition_by_id(
                        stream_id,
                        topic_id,
                        partition_id,
                        partitions::helpers::is_segment_full(),
                    );

                    // Try committing the journal
                    if is_full || unsaved_messages_count_exceeded || unsaved_messages_size_exceeded
                    {
                        self.streams2
                            .persist_messages(
                                self.id,
                                stream_id,
                                topic_id,
                                partition_id,
                                unsaved_messages_count_exceeded,
                                unsaved_messages_size_exceeded,
                                journal_messages_count,
                                journal_size,
                                &self.config.system,
                            )
                            .await?;

                        if is_full {
                            self.streams2
                                .handle_full_segment(
                                    self.id,
                                    stream_id,
                                    topic_id,
                                    partition_id,
                                    &self.config.system,
                                )
                                .await?;
                            self.metrics.increment_messages(messages_count as u64);
                        }
                    }
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
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer: Consumer,
        maybe_partition_id: Option<u32>,
        args: PollingArgs,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());

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
            stream_id,
            topic_id,
            &consumer,
            client_id,
            maybe_partition_id,
            true,
        ) else {
            return Ok((IggyPollMetadata::new(0, 0), IggyMessagesBatchSet::empty()));
        };

        let has_partition = self
            .streams2
            .with_topic_by_id(stream_id, topic_id, |(root, ..)| {
                root.partitions().exists(partition_id)
            });
        if !has_partition {
            return Err(IggyError::NoPartitions(
                numeric_topic_id as u32,
                numeric_stream_id as u32,
            ));
        }

        let current_offset = self.streams2.with_partition_by_id(
            stream_id,
            topic_id,
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
                    let metadata = IggyPollMetadata::new(partition_id as u32, current_offset);
                    let count = args.count;
                    let strategy = args.strategy;
                    let value = strategy.value;
                    let batches = match strategy.kind {
                        PollingKind::Offset => {
                            let offset = value;
                            // We have to remember to keep the invariant from the if that is on line 496.
                            // Alternatively a better design would be to get rid of that if and move the validations here.
                            let batches = self
                                .streams2
                                .get_messages_by_offset(
                                    stream_id,
                                    topic_id,
                                    partition_id,
                                    offset,
                                    count,
                                )
                                .await?;
                            Ok(batches)
                        }
                        PollingKind::Timestamp => {
                            let timestamp = IggyTimestamp::from(value);
                            let timestamp_ts = timestamp.as_micros();
                            trace!(
                                "Getting {count} messages by timestamp: {} for partition: {}...",
                                timestamp_ts, partition_id
                            );

                            let batches = self
                                .streams2
                                .get_messages_by_timestamp(
                                    stream_id,
                                    topic_id,
                                    partition_id,
                                    timestamp_ts,
                                    count,
                                )
                                .await?;
                            Ok(batches)
                        }
                        PollingKind::First => {
                            let first_offset = self.streams2.with_partition_by_id(
                                stream_id,
                                topic_id,
                                partition_id,
                                |(_, _, _, _, _, _, log)| {
                                    log.segments()
                                        .first()
                                        .map(|segment| segment.start_offset)
                                        .unwrap_or(0)
                                },
                            );

                            let batches = self
                                .streams2
                                .get_messages_by_offset(
                                    stream_id,
                                    topic_id,
                                    partition_id,
                                    first_offset,
                                    count,
                                )
                                .await?;
                            Ok(batches)
                        }
                        PollingKind::Last => {
                            let (start_offset, actual_count) = self.streams2.with_partition_by_id(
                                stream_id,
                                topic_id,
                                partition_id,
                                |(_, _, _, offset, _, _, _)| {
                                    let current_offset = offset.load(Ordering::Relaxed);
                                    let mut requested_count = 0;
                                    if requested_count > current_offset + 1 {
                                        requested_count = current_offset + 1
                                    }
                                    let start_offset = 1 + current_offset - requested_count;
                                    (start_offset, requested_count as u32)
                                },
                            );

                            let batches = self
                                .streams2
                                .get_messages_by_offset(
                                    stream_id,
                                    topic_id,
                                    partition_id,
                                    start_offset,
                                    actual_count,
                                )
                                .await?;
                            Ok(batches)
                        }
                        PollingKind::Next => {
                            let (consumer_offset, consumer_id) = match consumer {
                                PollingConsumer::Consumer(consumer_id, _) => (
                                    self.streams2
                                        .with_partition_by_id(
                                            stream_id,
                                            topic_id,
                                            partition_id,
                                            partitions::helpers::get_consumer_offset(consumer_id),
                                        )
                                        .map(|c_offset| c_offset.stored_offset),
                                    consumer_id,
                                ),
                                PollingConsumer::ConsumerGroup(cg_id, _) => (
                                    self.streams2
                                        .with_partition_by_id(
                                            stream_id,
                                            topic_id,
                                            partition_id,
                                            partitions::helpers::get_consumer_group_member_offset(
                                                cg_id,
                                            ),
                                        )
                                        .map(|cg_offset| cg_offset.stored_offset),
                                    cg_id,
                                ),
                            };

                            let Some(consumer_offset) = consumer_offset else {
                                return Err(IggyError::ConsumerOffsetNotFound(consumer_id));
                            };
                            let offset = consumer_offset + 1;
                            trace!(
                                "Getting next messages for consumer id: {} for partition: {} from offset: {}...",
                                consumer_id, partition_id, offset
                            );
                            let batches = self
                                .streams2
                                .get_messages_by_offset(
                                    stream_id,
                                    topic_id,
                                    partition_id,
                                    offset,
                                    count,
                                )
                                .await?;
                            Ok(batches)
                        }
                    }?;

                    if args.auto_commit && !batches.is_empty() {
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
                                    stream_id,
                                    topic_id,
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
                                self.streams2
                                    .with_partition_by_id_async(
                                        stream_id,
                                        topic_id,
                                        partition_id,
                                        partitions::helpers::persist_consumer_offset_to_disk(
                                            self.id,
                                            consumer_id,
                                        ),
                                    )
                                    .await?;
                            }
                            PollingConsumer::ConsumerGroup(cg_id, _) => {
                                self.streams2.with_partition_by_id(
                                    stream_id,
                                    topic_id,
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
                                self.streams2.with_partition_by_id_async(
                                    stream_id,
                                    topic_id,
                                    partition_id,
                                    partitions::helpers::persist_consumer_group_member_offset_to_disk(
                                        self.id,
                                        cg_id,
                                    ),
                                )
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
        todo!();
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
