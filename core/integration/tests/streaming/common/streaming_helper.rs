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
use iggy_common::{Identifier, IggyError};
use server::binary::handlers::messages::poll_messages_handler::IggyPollMetadata;
use server::configs::system::SystemConfig;
use server::partition_store::{PartitionDataStore, RcPartitionData};
use server::shard::system::messages::PollingArgs;
use server::shard::task_registry::TaskRegistry;
use server::streaming::partitions::journal::Journal;
use server::streaming::polling_consumer::PollingConsumer;
use server::streaming::segments::IggyMessagesBatchMut;
use server::streaming::segments::IggyMessagesBatchSet;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::Ordering;

/// Test helper for streaming operations.
///
/// Provides append_messages and poll_messages methods that work with
/// the new SharedMetadata + PartitionDataStore architecture.
pub struct StreamingTestHelper {
    partition_store: RefCell<PartitionDataStore>,
}

fn partition_not_found(ns: &IggyNamespace) -> IggyError {
    IggyError::PartitionNotFound(
        ns.partition_id(),
        Identifier::numeric(ns.topic_id() as u32).expect("valid topic id"),
        Identifier::numeric(ns.stream_id() as u32).expect("valid stream id"),
    )
}

impl StreamingTestHelper {
    pub fn new(partition_store: PartitionDataStore) -> Self {
        Self {
            partition_store: RefCell::new(partition_store),
        }
    }

    /// Append messages to a partition.
    pub async fn append_messages(
        &self,
        config: &SystemConfig,
        _registry: &Rc<TaskRegistry>,
        ns: &IggyNamespace,
        mut batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        // Extract data needed for prepare_for_persistence
        let (segment_start_offset, current_offset, current_position, deduplicator) = {
            let store = self.partition_store.borrow();
            let rc_partition = store.get(ns).ok_or_else(|| partition_not_found(ns))?;
            let partition_data = rc_partition.borrow();

            let segment = partition_data.log.active_segment();
            let segment_start_offset = segment.start_offset;
            let current_position = segment.current_position;
            let current_offset = partition_data.offset.load(Ordering::Relaxed);
            let next_offset = if partition_data.should_increment_offset {
                current_offset + 1
            } else {
                0
            };
            let deduplicator = partition_data.message_deduplicator.clone();
            (
                segment_start_offset,
                next_offset,
                current_position,
                deduplicator,
            )
        };

        // Prepare messages for persistence (sets offsets, timestamps, checksums)
        batch
            .prepare_for_persistence(
                segment_start_offset,
                current_offset,
                current_position,
                deduplicator.as_ref(),
            )
            .await;

        // Now do the actual append
        let store = self.partition_store.borrow();
        let rc_partition = store.get(ns).ok_or_else(|| partition_not_found(ns))?;
        let mut partition_data = rc_partition.borrow_mut();

        // Extract batch metadata
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

        // Calculate and store the last offset
        let last_offset = if batch_messages_count == 0 {
            current_offset
        } else {
            current_offset + batch_messages_count as u64 - 1
        };
        segment.end_offset = last_offset;

        // Store offset in partition data
        partition_data.offset.store(last_offset, Ordering::Relaxed);
        partition_data.stats.set_current_offset(last_offset);
        partition_data.should_increment_offset = true;

        // Persist messages if required
        let messages_in_journal = partition_data.log.journal().inner().messages_count;
        if messages_in_journal >= config.partition.messages_required_to_save {
            drop(partition_data);
            drop(store);

            let store = self.partition_store.borrow();
            let rc_partition = store.get(ns).ok_or_else(|| partition_not_found(ns))?;
            let mut partition_data = rc_partition.borrow_mut();
            partition_data.persist_messages("threshold", config).await?;
        }

        Ok(())
    }

    /// Poll messages from a partition.
    pub async fn poll_messages(
        &self,
        ns: &IggyNamespace,
        consumer: PollingConsumer,
        args: PollingArgs,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        let partition_rc = {
            let store = self.partition_store.borrow();
            store.get_rc(ns).ok_or_else(|| partition_not_found(ns))?
        };

        let partition_data = partition_rc.borrow();
        partition_data
            .poll_messages(ns.partition_id(), consumer, args)
            .await
    }

    /// Get current offset for a partition.
    pub fn get_current_offset(&self, ns: &IggyNamespace) -> Option<u64> {
        let store = self.partition_store.borrow();
        store
            .get(ns)
            .map(|rc| rc.borrow().offset.load(Ordering::Relaxed))
    }

    /// Get the underlying partition data (for testing).
    #[allow(dead_code)]
    pub fn get_partition(&self, ns: &IggyNamespace) -> Option<RcPartitionData> {
        let store = self.partition_store.borrow();
        store.get_rc(ns)
    }
}
