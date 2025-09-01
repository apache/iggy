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
use crate::configs::cache_indexes::CacheIndexesConfig;
use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::streaming::partitions::journal::Journal;
use crate::streaming::partitions::partition2::PartitionRoot;
use crate::streaming::segments::storage::create_segment_storage;
use crate::streaming::segments::{
    IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet, Segment2,
};
use crate::streaming::session::Session;
use crate::streaming::utils::{PooledBuffer, hash};
use crate::streaming::{streams, topics};
use crate::{shard_info, shard_trace};
use error_set::ErrContext;

use iggy_common::{
    BytesSerializable, Consumer, IGGY_MESSAGE_HEADER_SIZE, Identifier, IggyError, Partitioning,
    PartitioningKind, PollingStrategy,
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

        // Encrypt messages if encryptor is enabled in configuration.
        let mut batch = self.maybe_encrypt_messages(batch)?;
        let messages_count = batch.count();

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

        // Deduplicate messages and adjust their offsets.
        let current_offset = self
            .streams2
            .with_partition_by_id_async(
                stream_id,
                topic_id,
                partition_id,
                async |(root, _, deduplicator, offset, _, _, log)| {
                    let segment = log.active_segment();
                    let current_offset = if !root.should_increment_offset() {
                        0
                    } else {
                        offset.load(Ordering::Relaxed) + 1
                    };
                    batch
                        .prepare_for_persistence(
                            segment.start_offset,
                            current_offset,
                            segment.size,
                            deduplicator.as_ref(),
                        )
                        .await;
                    current_offset
                },
            )
            .await;

        // Append to the journal.
        let (journal_messages_count, journal_size) = self.streams2.with_partition_by_id_mut(
            stream_id,
            topic_id,
            partition_id,
            |(root, stats, _, offset, .., log)| {
                let segment = log.active_segment_mut();

                if segment.end_offset == 0 {
                    segment.start_timestamp = batch.first_timestamp().unwrap();
                }

                let batch_messages_size = batch.size();
                let batch_messages_count = batch.count();

                segment.end_timestamp = batch.last_timestamp().unwrap();
                segment.end_offset = batch.last_offset().unwrap();
                segment.size += batch_messages_size;

                let (journal_messages_count, journal_size) =
                    log.journal_mut().append(self.id, batch)?;

                stats.increment_messages_count(batch_messages_count as u64);
                stats.increment_size_bytes(batch_messages_size as u64);

                let last_offset = if batch_messages_count == 0 {
                    current_offset
                } else {
                    current_offset + batch_messages_count as u64 - 1
                };

                if root.should_increment_offset() {
                    offset.store(last_offset, Ordering::Relaxed);
                } else {
                    root.set_should_increment_offset(true);
                    offset.store(last_offset, Ordering::Relaxed);
                }

                Ok((journal_messages_count, journal_size))
            },
        )?;

        let unsaved_messages_count_exceeded =
            journal_messages_count >= self.config.system.partition.messages_required_to_save;
        let unsaved_messages_size_exceeded = journal_size
            >= self
                .config
                .system
                .partition
                .size_of_messages_required_to_save
                .as_bytes_u64() as u32;

        let is_full =
            self.streams2
                .with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
                    log.active_segment().is_full()
                });

        // Try committing the journal
        if is_full || unsaved_messages_count_exceeded || unsaved_messages_size_exceeded {
            let batches = self.streams2.with_partition_by_id_mut(
                stream_id,
                topic_id,
                partition_id,
                |(.., log)| {
                    let batches = log.journal_mut().commit();
                    log.ensure_indexes();
                    batches.append_indexes_to(log.indexes_mut().unwrap());
                    batches
                },
            );

            let (saved, batch_count) = self.streams2
                .with_partition_by_id_async(
                    stream_id,
                    topic_id,
                    partition_id,
                    async |(.., log)| {
                        let reason = if unsaved_messages_count_exceeded {
                            format!(
                                "unsaved messages count exceeded: {}, max from config: {}",
                                journal_messages_count,
                                self.config.system.partition.messages_required_to_save,
                            )
                        } else if unsaved_messages_size_exceeded {
                            format!(
                                "unsaved messages size exceeded: {}, max from config: {}",
                                journal_size,
                                self.config.system.partition.size_of_messages_required_to_save,
                            )
                        } else {
                            format!(
                                "segment is full, current size: {}, max from config: {}",
                                log.active_segment().size,
                                self.config.system.segment.size,
                            )
                        };
                        shard_trace!(
                            self.id,
                            "Persisting messages on disk for stream ID: {}, topic ID: {}, partition ID: {} because {}...",
                            stream_id,
                            topic_id,
                            partition_id,
                            reason
                        );

                        let batch_count = batches.count();
                        let batch_size = batches.size();

                        let storage = log.active_storage();
                        let saved = storage
                            .messages_writer
                            .as_ref()
                            .expect("Messages writer not initialized")
                            .save_batch_set(batches)
                            .await
                            .with_error_context(|error| {
                                let segment = log.active_segment();
                                format!(
                                    "Failed to save batch of {batch_count} messages \
                                    ({batch_size} bytes) to {segment}. {error}",
                                )
                            })?;

                        let unsaved_indexes_slice = log.indexes().unwrap().unsaved_slice();
                        let len = unsaved_indexes_slice.len();
                        storage
                            .index_writer
                            .as_ref()
                            .expect("Index writer not initialized")
                            .save_indexes(unsaved_indexes_slice)
                            .await
                            .with_error_context(|error| {
                                let segment = log.active_segment();
                                format!(
                                    "Failed to save index of {len} indexes to {segment}. {error}",
                                )
                            })?;

                        shard_trace!(
                            self.id,
                            "Persisted {} messages on disk for stream ID: {}, topic ID: {}, for partition with ID: {}, total bytes written: {}.",
                            batch_count, stream_id, topic_id, partition_id, saved
                        );

                        Ok((saved, batch_count))
                    },
                )
                .await?;

            self.streams2.with_partition_by_id_mut(
                stream_id,
                topic_id,
                partition_id,
                |(_, stats, .., log)| {
                    log.active_segment_mut().size += saved.as_bytes_u32();
                    log.indexes_mut().unwrap().mark_saved();
                    if self.config.system.segment.cache_indexes == CacheIndexesConfig::None {
                        log.indexes_mut().unwrap().clear();
                    }
                    stats.increment_size_bytes(saved.as_bytes_u64());
                    stats.increment_messages_count(batch_count as u64);
                },
            );

            // Handle possibly full segment after saving.
            let is_segment_full = self.streams2.with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                |(.., log)| log.active_segment().is_full(),
            );

            if is_segment_full {
                let (start_offset, size, end_offset) = self.streams2.with_partition_by_id(
                    stream_id,
                    topic_id,
                    partition_id,
                    |(.., log)| {
                        (
                            log.active_segment().start_offset,
                            log.active_segment().size,
                            log.active_segment().end_offset,
                        )
                    },
                );

                let numeric_stream_id = self
                    .streams2
                    .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
                let numeric_topic_id = self.streams2.with_topic_by_id(
                    stream_id,
                    topic_id,
                    topics::helpers::get_topic_id(),
                );

                if self.config.system.segment.cache_indexes == CacheIndexesConfig::OpenSegment
                    || self.config.system.segment.cache_indexes == CacheIndexesConfig::None
                {
                    self.streams2.with_partition_by_id_mut(
                        stream_id,
                        topic_id,
                        partition_id,
                        |(.., log)| {
                            log.clear_indexes();
                        },
                    );
                }

                self.streams2.with_partition_by_id_mut(
                    stream_id,
                    topic_id,
                    partition_id,
                    |(.., log)| {
                        log.active_segment_mut().sealed = true;
                    },
                );
                let (log_writer, index_writer) = self.streams2.with_partition_by_id_mut(
                    stream_id,
                    topic_id,
                    partition_id,
                    |(.., log)| log.active_storage_mut().shutdown(),
                );

                compio::runtime::spawn(async move {
                    let _ = log_writer.fsync().await;
                })
                .detach();
                compio::runtime::spawn(async move {
                    let _ = index_writer.fsync().await;
                    drop(index_writer)
                })
                .detach();

                shard_info!(
                    self.id,
                    "Closed segment for stream: {}, topic: {} with start offset: {}, end offset: {}, size: {} for partition with ID: {}.",
                    stream_id,
                    topic_id,
                    start_offset,
                    end_offset,
                    size,
                    partition_id
                );

                let messages_size = 0;
                let indexes_size = 0;
                let segment = Segment2::new(
                    end_offset + 1,
                    self.config.system.segment.size,
                    self.config.system.segment.message_expiry,
                );

                let storage = create_segment_storage(
                    &self.config.system,
                    numeric_stream_id,
                    numeric_topic_id,
                    partition_id,
                    messages_size,
                    indexes_size,
                    end_offset + 1,
                )
                .await?;
                self.streams2.with_partition_by_id_mut(
                    stream_id,
                    topic_id,
                    partition_id,
                    |(.., log)| {
                        log.add_persisted_segment(segment, storage);
                    },
                );
            }
        }

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
        todo!();
        /*
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
        */
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
