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

use iggy_common::sharding::IggyNamespace;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize,
};
use server::{
    configs::system::SystemConfig,
    metadata::Metadata,
    shard::{
        namespace::IggyFullNamespace,
        shard_local_partitions::{PartitionData, ShardLocalPartitions},
        system::messages::PollingArgs,
        task_registry::TaskRegistry,
        transmission::connector::ShardConnector,
    },
    streaming::{
        partitions::{
            helpers::create_message_deduplicator,
            journal::Journal,
            partition::{ConsumerGroupOffsets, ConsumerOffsets},
            storage::create_partition_file_hierarchy,
        },
        polling_consumer::PollingConsumer,
        segments::{
            IggyMessagesBatchMut, IggyMessagesBatchSet, Segment, storage::create_segment_storage,
        },
        streams::storage::create_stream_file_hierarchy,
        topics::storage::create_topic_file_hierarchy,
        traits::MainOps,
    },
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

mod common;
mod get_by_offset;
mod get_by_timestamp;
mod snapshot;

/// Test harness that uses SharedMetadata + PartitionDataStore
pub struct TestStreams {
    pub shared_metadata: &'static Metadata,
    pub partition_store: RefCell<ShardLocalPartitions>,
}

impl TestStreams {
    pub fn new(shared_metadata: &'static Metadata) -> Self {
        Self {
            shared_metadata,
            partition_store: RefCell::new(ShardLocalPartitions::new()),
        }
    }

    /// Access partition data by Identifier-based stream/topic IDs.
    /// Converts Identifiers to numeric IDs using SharedMetadata, then accesses partition_store.
    pub fn with_partition_by_id<F, R>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        f: F,
    ) -> R
    where
        F: FnOnce(
            (
                &PartitionData,
                usize,
                usize,
                &Arc<AtomicU64>,
                &Arc<ConsumerOffsets>,
                &Arc<ConsumerGroupOffsets>,
            ),
        ) -> R,
    {
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .expect("Stream must exist");
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .expect("Topic must exist");

        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
        let store = self.partition_store.borrow();
        let partition_data = store.get(&namespace).expect("Partition must exist");

        f((
            partition_data,
            numeric_stream_id,
            numeric_topic_id,
            &partition_data.offset,
            &partition_data.consumer_offsets,
            &partition_data.consumer_group_offsets,
        ))
    }
}

impl MainOps for TestStreams {
    type Namespace = IggyFullNamespace;
    type PollingArgs = PollingArgs;
    type Consumer = PollingConsumer;
    type In = IggyMessagesBatchMut;
    type Out = (
        server::binary::handlers::messages::poll_messages_handler::IggyPollMetadata,
        IggyMessagesBatchSet,
    );
    type Error = IggyError;

    async fn append_messages(
        &self,
        config: &SystemConfig,
        _registry: &Rc<TaskRegistry>,
        ns: &Self::Namespace,
        mut batch: Self::In,
    ) -> Result<(), Self::Error> {
        // Resolve Identifier to numeric IDs
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(ns.stream_id())
            .expect("Stream must exist");
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, ns.topic_id())
            .expect("Topic must exist");
        let partition_id = ns.partition_id();

        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);

        if batch.count() == 0 {
            return Ok(());
        }

        // Get necessary data from partition_store
        let (current_offset, current_position, segment_start_offset, message_deduplicator) = {
            let store = self.partition_store.borrow();
            let partition_data = store
                .get(&namespace)
                .expect("partition_store: partition must exist");

            let current_offset = if partition_data.should_increment_offset {
                partition_data.offset.load(Ordering::Relaxed) + 1
            } else {
                0
            };

            let segment = partition_data.log.active_segment();
            let current_position = segment.current_position;
            let segment_start_offset = segment.start_offset;
            let message_deduplicator = partition_data.message_deduplicator.clone();

            (
                current_offset,
                current_position,
                segment_start_offset,
                message_deduplicator,
            )
        };

        // Prepare batch for persistence (outside the borrow)
        batch
            .prepare_for_persistence(
                segment_start_offset,
                current_offset,
                current_position,
                message_deduplicator.as_ref(),
            )
            .await;

        // Append to journal
        let (journal_messages_count, journal_size) = {
            let mut store = self.partition_store.borrow_mut();
            let partition_data = store
                .get_mut(&namespace)
                .expect("partition_store: partition must exist");

            let segment = partition_data.log.active_segment_mut();

            if segment.end_offset == 0 {
                segment.start_timestamp = batch.first_timestamp().unwrap();
            }

            let batch_messages_size = batch.size();
            let batch_messages_count = batch.count();

            partition_data
                .stats
                .increment_size_bytes(batch_messages_size as u64);
            partition_data
                .stats
                .increment_messages_count(batch_messages_count as u64);

            segment.end_timestamp = batch.last_timestamp().unwrap();
            segment.end_offset = batch.last_offset().unwrap();

            let (journal_messages_count, journal_size) =
                partition_data.log.journal_mut().append(batch)?;

            let last_offset = if batch_messages_count == 0 {
                current_offset
            } else {
                current_offset + batch_messages_count as u64 - 1
            };

            if partition_data.should_increment_offset {
                partition_data.offset.store(last_offset, Ordering::Relaxed);
            } else {
                partition_data.should_increment_offset = true;
                partition_data.offset.store(last_offset, Ordering::Relaxed);
            }
            partition_data.log.active_segment_mut().current_position += batch_messages_size;

            (journal_messages_count, journal_size)
        };

        // Check if journal should be persisted
        let unsaved_messages_count_exceeded =
            journal_messages_count >= config.partition.messages_required_to_save;
        let unsaved_messages_size_exceeded = journal_size
            >= config
                .partition
                .size_of_messages_required_to_save
                .as_bytes_u64() as u32;

        let is_full = {
            let store = self.partition_store.borrow();
            let partition_data = store
                .get(&namespace)
                .expect("partition_store: partition must exist");
            partition_data.log.active_segment().is_full()
        };

        // Persist if needed
        if is_full || unsaved_messages_count_exceeded || unsaved_messages_size_exceeded {
            // Commit journal and persist to disk
            let batches = {
                let mut store = self.partition_store.borrow_mut();
                let partition_data = store
                    .get_mut(&namespace)
                    .expect("partition_store: partition must exist");
                let batches = partition_data.log.journal_mut().commit();
                partition_data.log.ensure_indexes();
                batches.append_indexes_to(partition_data.log.active_indexes_mut().unwrap());
                batches
            };

            self.persist_messages_to_disk(&namespace, batches).await?;
        }

        Ok(())
    }

    async fn poll_messages(
        &self,
        ns: &Self::Namespace,
        consumer: Self::Consumer,
        args: Self::PollingArgs,
    ) -> Result<Self::Out, Self::Error> {
        // Resolve Identifier to numeric IDs
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(ns.stream_id())
            .expect("Stream must exist");
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, ns.topic_id())
            .expect("Topic must exist");
        let partition_id = ns.partition_id();

        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);

        // Delegate to the shared partition_ops module
        server::streaming::partition_ops::poll_messages(
            &self.partition_store,
            &namespace,
            consumer,
            args,
        )
        .await
    }
}

impl TestStreams {
    /// Persist messages to disk using partition_store's storage.
    async fn persist_messages_to_disk(
        &self,
        namespace: &IggyNamespace,
        batches: IggyMessagesBatchSet,
    ) -> Result<u32, IggyError> {
        let batch_count = batches.count();

        if batch_count == 0 {
            return Ok(0);
        }

        // Extract storage from partition_store
        let (messages_writer, index_writer, has_segments) = {
            let store = self.partition_store.borrow();
            let partition_data = store
                .get(namespace)
                .expect("partition_store: partition must exist");

            if !partition_data.log.has_segments() {
                return Ok(0);
            }

            let messages_writer = partition_data
                .log
                .active_storage()
                .messages_writer
                .as_ref()
                .expect("Messages writer not initialized")
                .clone();
            let index_writer = partition_data
                .log
                .active_storage()
                .index_writer
                .as_ref()
                .expect("Index writer not initialized")
                .clone();
            (messages_writer, index_writer, true)
        };

        if !has_segments {
            return Ok(0);
        }

        // Lock and save messages
        let guard = messages_writer.lock.lock().await;
        let saved = messages_writer.as_ref().save_batch_set(batches).await?;

        // Get unsaved indexes from partition_store
        let unsaved_indexes_slice = {
            let store = self.partition_store.borrow();
            let partition_data = store
                .get(namespace)
                .expect("partition_store: partition must exist");
            partition_data.log.active_indexes().unwrap().unsaved_slice()
        };

        // Save indexes
        index_writer
            .as_ref()
            .save_indexes(unsaved_indexes_slice)
            .await?;

        // Update index and increment segment stats
        {
            let mut store = self.partition_store.borrow_mut();
            let partition_data = store
                .get_mut(namespace)
                .expect("partition_store: partition must exist");

            let indexes = partition_data.log.active_indexes_mut().unwrap();
            indexes.mark_saved();

            let segment = partition_data.log.active_segment_mut();
            segment.size =
                iggy_common::IggyByteSize::from(segment.size.as_bytes_u64() + saved.as_bytes_u64());
        }

        drop(guard);
        Ok(batch_count)
    }
}

struct BootstrapResult {
    streams: TestStreams,
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: usize,
    #[allow(dead_code)]
    namespace: IggyNamespace,
    task_registry: Rc<TaskRegistry>,
}

async fn bootstrap_test_environment(
    shard_id: u16,
    config: &SystemConfig,
) -> Result<BootstrapResult, IggyError> {
    let stream_name = "stream-1".to_owned();
    let topic_name = "topic-1".to_owned();
    let topic_expiry = IggyExpiry::NeverExpire;
    let topic_size = MaxTopicSize::Unlimited;
    let partitions_count = 1;

    // Create SharedMetadata (leaked for 'static lifetime in tests)
    let shared_metadata: &'static Metadata = Box::leak(Box::new(Metadata::default()));

    // Create stream in SharedMetadata
    let stream_id_num: usize = 1;
    let _stream_stats = shared_metadata.register_stream(
        stream_id_num,
        Arc::from(stream_name.as_str()),
        IggyTimestamp::now(),
    );
    create_stream_file_hierarchy(stream_id_num, config).await?;

    // Create topic in SharedMetadata
    let topic_id_num: usize = 1;
    let message_expiry = config.resolve_message_expiry(topic_expiry);
    let max_topic_size = config.resolve_max_topic_size(topic_size)?;
    let _topic_stats = shared_metadata.register_topic(
        stream_id_num,
        topic_id_num,
        Arc::from(topic_name.as_str()),
        IggyTimestamp::now(),
        message_expiry,
        CompressionAlgorithm::default(),
        max_topic_size,
        1, // replication_factor
        partitions_count,
    );
    create_topic_file_hierarchy(stream_id_num, topic_id_num, config).await?;

    // Create partition in SharedMetadata
    let partition_id: usize = 0;
    let namespace = IggyNamespace::new(stream_id_num, topic_id_num, partition_id);
    let partition_stats = shared_metadata.register_partition(
        stream_id_num,
        topic_id_num,
        partition_id,
        IggyTimestamp::now(),
    );
    create_partition_file_hierarchy(stream_id_num, topic_id_num, partition_id, config).await?;

    // Create TestStreams with partition data
    let streams = TestStreams::new(shared_metadata);

    // Initialize partition data in partition_store
    let start_offset = 0u64;
    let segment = Segment::new(
        start_offset,
        config.segment.size,
        config.segment.message_expiry,
    );
    let messages_size = 0;
    let indexes_size = 0;
    let storage = create_segment_storage(
        config,
        stream_id_num,
        topic_id_num,
        partition_id,
        messages_size,
        indexes_size,
        start_offset,
    )
    .await?;

    // Create partition data with the log
    let consumer_offsets = Arc::new(ConsumerOffsets::with_capacity(0));
    let consumer_group_offsets = Arc::new(ConsumerGroupOffsets::with_capacity(0));
    let message_deduplicator = create_message_deduplicator(config).map(Arc::new);
    let current_offset = Arc::new(AtomicU64::new(0));

    let mut partition_data = PartitionData::new(
        partition_stats,
        current_offset,
        consumer_offsets,
        consumer_group_offsets,
        message_deduplicator,
        IggyTimestamp::now(),
        0,     // revision_id
        false, // should_increment_offset
    );

    // Add the segment to the log
    partition_data.log.add_persisted_segment(segment, storage);

    // Insert into partition store
    streams
        .partition_store
        .borrow_mut()
        .insert(namespace, partition_data);

    // Create a test task registry with dummy stop sender from ShardConnector
    let connector: ShardConnector<()> = ShardConnector::new(shard_id);
    let task_registry = Rc::new(TaskRegistry::new(shard_id, vec![connector.stop_sender]));

    let stream_id = Identifier::numeric(stream_id_num as u32).unwrap();
    let topic_id = Identifier::numeric(topic_id_num as u32).unwrap();

    Ok(BootstrapResult {
        streams,
        stream_id,
        topic_id,
        partition_id,
        namespace,
        task_registry,
    })
}
