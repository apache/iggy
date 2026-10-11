// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exercise consumer bookmark recovery across simulated power loss.
//!
//! A consumer offset is a bookmark: storing 2 means `Next` starts at message 3.
//! Recovery must preserve bookmark 2 and return messages 3 and 4. The test
//! covers individual consumers, groups, and external groups under both offset
//! policies. An external group is never polled, so its bookmark is read
//! directly.
//!
//! The harness uses production consumer recovery with `SimStorage`, then polls
//! through the real `Next` path. Message recovery is narrower than server boot:
//! a helper replays a durable journal into a new partition. No partition memory
//! survives recovery.

use super::tests::owned_prepare;
use super::{Crash, SimStorage};
use configs::server::ServerConfig;
use consensus::{LocalPipeline, Sequencer, VsrConsensus};
use futures::executor::block_on;
use iggy_common::{
    ConsumerKind, Durability, IggyByteSize, PartitionStats, PollingStrategy, TopicRuntimeOptions,
};
use journal::durable_storage::{DurableFile, DurableStorage, OpenMode};
use journal::{DurableAppend, PartitionPrepareJournal};
use message_bus::IggyMessageBus;
use partitions::offset_storage::persist_offset_with_storage;
use partitions::{
    IggyPartition, IggyPartitions, Partition, PartitionPathLayout, PartitionsConfig, PollingArgs,
    PollingConsumer, configure_consumer_offsets_with_storage,
};
use server_common::send_messages::decode_batch_slice;
use server_common::sharding::{IggyNamespace, ShardId};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

const CREATED_REVISION: u64 = 7;
const STORED_OFFSET: u64 = 2;
const CONSUMER_ID: usize = 7;
const GROUP_ID: usize = 9;
const FRESH_MESSAGE_COUNT: u64 = 5;

type TestPartition = IggyPartition<Rc<IggyMessageBus>>;

/// Own the simulated filesystem and configuration, but no live partition state.
///
/// Offset files use the server's directory layout. The separate message journal
/// supplies history for rebuilding each new partition.
struct BookmarkStorageHarness {
    storage: SimStorage,
    config: PartitionsConfig,
    namespace: IggyNamespace,
    /// Policy for consumer offsets; message durability is established by the fixture.
    policy: Durability,
}

impl BookmarkStorageHarness {
    /// Store bookmark 2 for every kind.
    /// All files and their directory entries are durable before the test starts,
    /// including when the selected offset policy does not require an immediate sync.
    async fn with_stored_progress(policy: Durability) -> Self {
        let server_config = ServerConfig {
            path: "/bookmarks".to_owned(),
            ..ServerConfig::default()
        };
        let harness = Self {
            storage: SimStorage::default(),
            config: PartitionsConfig {
                path_layout: PartitionPathLayout {
                    streams_root: server_config.get_streams_path(),
                },
                ..partition_config()
            },
            namespace: IggyNamespace::new(0, 0, 42),
            policy,
        };
        for kind in ConsumerKind::ALL {
            let directory = harness.offset_directory(kind);
            harness
                .storage
                .create_directories(&directory)
                .await
                .unwrap();
            for ancestor in directory.ancestors() {
                harness.storage.sync_directory(ancestor).await.unwrap();
            }
        }
        harness
            .persist_bookmark(ConsumerKind::Consumer, CONSUMER_ID)
            .await;
        harness
            .persist_bookmark(ConsumerKind::ConsumerGroup, GROUP_ID)
            .await;
        // Kept under the same Iggy group as the group bookmark, as the gateway keys it.
        harness
            .persist_bookmark(ConsumerKind::ExternalGroup, GROUP_ID)
            .await;
        harness
    }

    /// Persist bookmark 2 without adding it to any partition's live offset maps.
    async fn persist_bookmark(&self, kind: ConsumerKind, consumer_id: usize) {
        let directory = self.offset_directory(kind);
        let path = directory.join(consumer_id.to_string());
        persist_offset_with_storage(
            &self.storage,
            path.to_str().unwrap(),
            STORED_OFFSET,
            self.policy.is_persisted(),
        )
        .await
        .unwrap();
        // Establish the same durable input under both policies. The replicated
        // policy does not itself require the cursor write to sync immediately.
        self.storage
            .open(&path, OpenMode::Read)
            .await
            .unwrap()
            .sync()
            .await
            .unwrap();
        self.storage.sync_directory(&directory).await.unwrap();
    }

    /// Journal five messages at offsets 0 through 4 without touching offset directories.
    async fn persist_fresh_history(&self) {
        let mut journal = PartitionPrepareJournal::open_with_storage(
            &self.journal_directory(),
            self.namespace.inner(),
            CREATED_REVISION,
            self.storage.clone(),
        )
        .await
        .unwrap();
        let mut parent_checksum = 0;
        for offset in 0..FRESH_MESSAGE_COUNT {
            let operation_number = offset + 1;
            // The helper fills the payload with the operation number, allowing
            // polls to verify message contents as well as their offsets.
            let prepare = owned_prepare(operation_number, parent_checksum, offset);
            parent_checksum = prepare.header().checksum;
            journal.append(prepare.into_frozen()).await.unwrap();
        }
        journal.sync().await.unwrap();
        self.storage
            .sync_directory(Path::new(&self.partition_directory()))
            .await
            .unwrap();
    }

    /// Rebuild messages and consumer progress from storage.
    ///
    /// Journal entries are appended and committed into a fresh in-memory partition
    /// before the shared server offset loader runs. This supplies real readable
    /// messages for `Next` without claiming to exercise the full server boot path.
    async fn recover_partition(&self) -> TestPartition {
        let journal = PartitionPrepareJournal::open_with_storage(
            &self.journal_directory(),
            self.namespace.inner(),
            CREATED_REVISION,
            self.storage.clone(),
        )
        .await
        .unwrap();
        let mut partition = self.empty_partition();
        for prepare in journal.prepares().await.unwrap() {
            let operation_number = prepare.header().op;
            partition.append_messages(prepare).await.unwrap();
            partition
                .consensus()
                .sequencer()
                .set_sequence(operation_number);
            partition.consensus().advance_commit_max(operation_number);
            partition.commit_journal(&partition_config()).await;
            assert!(partition.fatal().is_none());
        }
        let current_offset = partition.offsets().commit_offset;
        assert_eq!(current_offset, FRESH_MESSAGE_COUNT - 1);
        // The loader clamps saved progress to this offset, as server boot does.
        configure_consumer_offsets_with_storage(
            &self.storage,
            &mut partition,
            &self.config,
            self.namespace,
            current_offset,
        )
        .await
        .unwrap();
        partition
    }

    /// Poll `Next` for the consumer and group, checking actual offsets and payloads.
    /// This executes and completes real polls with automatic offset commits disabled;
    /// it consumes the partition because completing polls can update live tracking.
    async fn poll_next_and_assert_messages(
        &self,
        partition: TestPartition,
        expected_offsets: &[u64],
    ) {
        let partitions = IggyPartitions::new(ShardId::new(0), partition_config());
        partitions.insert(self.namespace, partition);
        for consumer in consumers() {
            let plan = partitions
                .build_poll_snapshot(
                    &self.namespace,
                    consumer,
                    &PollingArgs {
                        strategy: PollingStrategy::next(),
                        count: 10,
                        auto_commit: false,
                    },
                )
                .unwrap();
            assert!(!plan.needs_off_pump_io());
            let completion = partitions
                .complete_poll(&self.namespace, plan.execute().await)
                .unwrap();
            assert!(completion.replication.is_none());
            let actual_offsets: Vec<_> = completion
                .fragments
                .iter()
                .map(|fragment| {
                    let batch = decode_batch_slice(fragment.as_slice()).unwrap();
                    assert_eq!(batch.message_count(), 1);
                    let message = batch.iter().next().unwrap();
                    let expected_byte = u8::try_from(batch.header.base_offset + 1).unwrap();
                    assert!(message.payload.iter().all(|byte| *byte == expected_byte));
                    batch.header.base_offset
                })
                .collect();
            assert_eq!(
                actual_offsets, expected_offsets,
                "{:?}, {consumer:?}",
                self.policy
            );
        }
    }

    /// Create a partition with no recovered messages or consumer progress.
    /// Its identity matches the durable records so recovery can accept those records.
    fn empty_partition(&self) -> TestPartition {
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            self.namespace.inner(),
            Rc::new(IggyMessageBus::new(0)),
            LocalPipeline::new(),
        );
        consensus.init();
        let mut partition = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(1024 * 1024),
        );
        partition.set_partition_dir(self.partition_directory());
        partition.set_created_revision(CREATED_REVISION);
        partition.set_runtime_options(TopicRuntimeOptions {
            durability: Durability::Replicated,
            consumer_offset_durability: self.policy,
            ..TopicRuntimeOptions::default()
        });
        partition
    }

    fn partition_directory(&self) -> String {
        self.config.get_partition_path(0, 0, 42)
    }

    fn journal_directory(&self) -> PathBuf {
        Path::new(&self.partition_directory()).join("fresh-history")
    }

    fn offset_directory(&self, kind: ConsumerKind) -> PathBuf {
        match kind {
            ConsumerKind::Consumer => self.config.get_consumer_offsets_path(0, 0, 42).into(),
            ConsumerKind::ConsumerGroup => {
                self.config.get_consumer_group_offsets_path(0, 0, 42).into()
            }
            ConsumerKind::ExternalGroup => {
                self.config.get_external_group_offsets_path(0, 0, 42).into()
            }
        }
    }
}

/// Assert bookmark 2 for every kind. The external group is never polled, so its
/// bookmark is read directly rather than through a polling consumer.
fn assert_bookmarks(partition: &TestPartition) {
    for consumer in consumers() {
        assert_eq!(
            partition.get_consumer_offset(consumer),
            Some(STORED_OFFSET),
            "{consumer:?}"
        );
    }
    let group_id = u32::try_from(GROUP_ID).expect("group id fits u32");
    assert_eq!(
        partition.external_group_offset(group_id),
        Some(STORED_OFFSET),
        "external group {group_id}"
    );
}

fn consumers() -> [PollingConsumer; 2] {
    [
        PollingConsumer::Consumer(CONSUMER_ID, 42),
        PollingConsumer::ConsumerGroup(GROUP_ID, 1),
    ]
}

fn partition_config() -> PartitionsConfig {
    PartitionsConfig {
        messages_required_to_save: 100,
        size_of_messages_required_to_save: IggyByteSize::from(1024 * 1024),
        validate_checksum: true,
        segment_size: IggyByteSize::from(1024 * 1024),
        preallocate_segments: false,
        encryptor: None,
        path_layout: PartitionPathLayout::default(),
    }
}

/// After power loss, a new partition must load the consumer and group
/// bookmarks from storage. Both saved bookmarks are 2, so Next must
/// return messages 3 and 4.
#[test]
fn given_stored_progress_when_power_is_lost_should_recover_both_consumer_bookmarks() {
    block_on(async {
        for policy in [Durability::Replicated, Durability::Persisted] {
            let harness = BookmarkStorageHarness::with_stored_progress(policy).await;
            harness.persist_fresh_history().await;

            harness.storage.crash(Crash::PowerLoss);
            let recovered = harness.recover_partition().await;

            assert_bookmarks(&recovered);
            // Bookmark 2 means the first three messages were already consumed.
            harness
                .poll_next_and_assert_messages(recovered, &[3, 4])
                .await;
        }
    });
}
