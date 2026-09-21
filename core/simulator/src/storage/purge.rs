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

//! Exercise purge completion and consumer recovery across simulated power loss.
//!
//! A consumer offset is a bookmark: storing 2 means `Next` starts at message 3.
//! Purge must remove those bookmarks as well as the old messages, so consumers
//! can read a replacement history from offset 0. The purge generation marker
//! records which purge was applied; it is separate from consumer progress.
//!
//! The two controls make that distinction observable. Without a purge, recovery
//! must preserve bookmark 2 and return messages 3 and 4. After a completed purge,
//! recovery must find no bookmarks and return all five messages, 0 through 4.
//! Both controls cover individual consumers and groups under both offset policies.
//!
//! The harness enters after message history has been reset. It uses production
//! purge completion and consumer recovery with `SimStorage`, then polls through
//! the real `Next` path. Message recovery is narrower than server boot: a helper
//! replays a durable journal into a new partition. No partition memory survives
//! recovery. Failure cases inject an error before either offset directory sync
//! takes effect. They check both the immediate completion contract and recovery
//! with fresh history. Purge retries and write acknowledgments are not exercised.

use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use configs::server::ServerConfig;
use consensus::{LocalPipeline, Sequencer, VsrConsensus};
use futures::executor::block_on;
use iggy_common::{
    ConsumerKind, Durability, IggyByteSize, PartitionStats, PollingStrategy, TopicRuntimeOptions,
};
use journal::durable_storage::{DurableFile, DurableStorage, OpenMode};
use journal::{DurableAppend, PartitionPrepareJournal};
use message_bus::IggyMessageBus;
use partitions::offset_storage::{
    persist_offset_with_storage, persist_purge_generation_with_storage,
};
use partitions::{
    IggyPartition, IggyPartitions, Partition, PartitionPathLayout, PartitionsConfig, PollingArgs,
    PollingConsumer,
};
use server::configure_consumer_offsets_with_storage;
use server_common::send_messages::decode_batch_slice;
use server_common::sharding::{IggyNamespace, ShardId};

use super::tests::owned_prepare;
use super::{Crash, FaultMode, SimStorage, StorageOperation};

const CREATED_REVISION: u64 = 7;
const OLD_GENERATION: u64 = 4;
const NEW_GENERATION: u64 = 5;
const STORED_OFFSET: u64 = 2;
const CONSUMER_ID: usize = 7;
const GROUP_ID: usize = 9;
const STRAY_ID: usize = 99;
const FRESH_MESSAGE_COUNT: u64 = 5;

type TestPartition = IggyPartition<Rc<IggyMessageBus>>;

/// Own the simulated filesystem and configuration, but no live partition state.
///
/// Offset files and the purge marker use the server's directory layout. The
/// separate message journal supplies history for rebuilding each new partition.
struct PurgeStorageHarness {
    storage: SimStorage,
    config: ServerConfig,
    namespace: IggyNamespace,
    /// Policy for consumer offsets; message durability is established by the fixture.
    policy: Durability,
}

#[derive(Debug)]
struct ConsumerPoll {
    consumer: PollingConsumer,
    stored_offset: Option<u64>,
    offsets: Vec<u64>,
}

impl PurgeStorageHarness {
    /// Store bookmark 2 for a consumer and a group, plus the earlier purge marker.
    /// All files and their directory entries are durable before the test starts,
    /// including when the selected offset policy does not require an immediate sync.
    async fn with_stored_progress(policy: Durability) -> Self {
        let harness = Self {
            storage: SimStorage::default(),
            config: ServerConfig {
                path: "/purge".to_owned(),
                ..ServerConfig::default()
            },
            namespace: IggyNamespace::new(0, 0, 42),
            policy,
        };
        for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
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
        persist_purge_generation_with_storage(
            &harness.storage,
            &format!("{}/purge.gen", harness.partition_directory()),
            OLD_GENERATION,
            CREATED_REVISION,
        )
        .await
        .unwrap();
        harness
    }

    /// Persist bookmark 2 without adding it to any partition's live offset maps.
    /// This also creates stray files that the purge must discover by scanning.
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
    /// Syncing the message journal must not accidentally make bookmark cleanup durable.
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
        // Never write back the whole simulated filesystem here: that would also
        // make an offset deletion durable after its own directory sync failed.
        self.storage
            .sync_directory(Path::new(&self.partition_directory()))
            .await
            .unwrap();
    }

    /// Rebuild messages, the applied purge marker, and consumer progress from storage.
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
        self.recover_progress(&mut partition, current_offset).await;
        partition
    }

    /// Use the shared marker and offset loaders, including the server's offset clamping.
    /// `current_offset` is the message bound against which saved progress is checked.
    async fn recover_progress(&self, partition: &mut TestPartition, current_offset: u64) {
        partition
            .hydrate_applied_purge_generation_with_storage(&self.storage)
            .await
            .unwrap();
        configure_consumer_offsets_with_storage(
            &self.storage,
            partition,
            &self.config,
            self.namespace,
            current_offset,
        )
        .await
        .unwrap();
    }

    /// Poll both consumers before asserting so a failure cannot hide the second result.
    async fn poll_next_and_assert_messages(
        &self,
        partition: TestPartition,
        expected_offsets: &[u64],
    ) {
        for poll in Box::pin(self.poll_next(partition)).await {
            assert_eq!(
                poll.offsets, expected_offsets,
                "{:?}, {:?}",
                self.policy, poll.consumer
            );
        }
    }

    /// Complete real polls with automatic commits disabled, retaining each saved
    /// bookmark for diagnostics. Poll completion can still update live tracking.
    async fn poll_next(&self, partition: TestPartition) -> Vec<ConsumerPoll> {
        let stored_offsets = consumers().map(|consumer| partition.get_consumer_offset(consumer));
        let partitions = IggyPartitions::new(ShardId::new(0), partition_config());
        partitions.insert(self.namespace, partition);
        let mut polls = Vec::with_capacity(consumers().len());
        for (consumer, stored_offset) in consumers().into_iter().zip(stored_offsets) {
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
            let offsets = completion
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
            polls.push(ConsumerPoll {
                consumer,
                stored_offset,
                offsets,
            });
        }
        polls
    }

    async fn partition_with_stored_progress(&self) -> TestPartition {
        let mut partition = self.empty_partition();
        self.recover_progress(&mut partition, STORED_OFFSET).await;
        assert_eq!(partition.applied_purge_generation(), OLD_GENERATION);
        for consumer in consumers() {
            assert_eq!(partition.get_consumer_offset(consumer), Some(STORED_OFFSET));
        }
        partition
    }

    /// Discover the barrier in a successful run instead of hardcoding an operation
    /// number. Completion syncs the consumer directory, then the group directory,
    /// before syncing the completion marker's parent. Return the expected trace
    /// through the selected barrier so the failing run must reach the same point.
    async fn arm_offset_directory_sync_failure(
        &self,
        failed_kind: ConsumerKind,
    ) -> Vec<StorageOperation> {
        let control = Self::with_stored_progress(self.policy).await;
        let mut partition = control.partition_with_stored_progress().await;
        control.storage.clear_trace();
        partition
            .complete_purge_with_storage(&control.storage, NEW_GENERATION)
            .await
            .unwrap();
        let trace = control.storage.trace();
        let directory_index = match failed_kind {
            ConsumerKind::Consumer => 0,
            ConsumerKind::ConsumerGroup => 1,
        };
        let (operation_index, _) = trace
            .iter()
            .enumerate()
            .filter(|(_, operation)| **operation == StorageOperation::DirectorySync)
            .nth(directory_index)
            .expect("purge must sync each offset directory before recording completion");
        self.storage.fail_at(operation_index, FaultMode::Before);
        trace[..=operation_index].to_vec()
    }

    async fn assert_failure_reached_after_unlinks(&self, expected_trace: &[StorageOperation]) {
        let actual_trace = self.storage.trace();
        assert_eq!(
            actual_trace.get(..expected_trace.len()),
            Some(expected_trace),
            "purge must reach the selected directory sync before the injected failure"
        );
        for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
            let directory = self.offset_directory(kind);
            assert!(
                self.storage.entries(&directory).await.unwrap().is_empty(),
                "{:?}, {kind:?}: bookmark files must be unlinked before the sync failure",
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
        }
    }
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
            let harness = PurgeStorageHarness::with_stored_progress(policy).await;
            harness.persist_fresh_history().await;

            harness.storage.crash(Crash::PowerLoss);
            let recovered = harness.recover_partition().await;

            assert_eq!(recovered.applied_purge_generation(), OLD_GENERATION);
            for consumer in consumers() {
                assert_eq!(recovered.get_consumer_offset(consumer), Some(STORED_OFFSET));
            }
            // Bookmark 2 means the first three messages were already consumed.
            harness
                .poll_next_and_assert_messages(recovered, &[3, 4])
                .await;
        }
    });
}

/// Purge must remove the consumer and group bookmarks before fresh messages arrive.
/// After power loss, a new partition must find no saved bookmarks, so Next returns
/// all fresh messages, 0 through 4. Restoring either old bookmark of 2 would skip
/// messages 0 through 2 even though that bookmark is still within the new history.
#[test]
fn given_completed_purge_when_power_is_lost_should_read_all_fresh_messages() {
    block_on(async {
        for policy in [Durability::Replicated, Durability::Persisted] {
            // Start at the completion phase: message history has been reset,
            // but the old consumer and group bookmarks still need to be cleared.
            let harness = PurgeStorageHarness::with_stored_progress(policy).await;
            let mut partition = harness.empty_partition();
            harness
                .recover_progress(&mut partition, STORED_OFFSET)
                .await;
            assert_eq!(
                partition.applied_purge_generation(),
                OLD_GENERATION,
                "{policy:?}: setup must load the earlier purge marker"
            );
            for consumer in consumers() {
                assert_eq!(
                    partition.get_consumer_offset(consumer),
                    Some(STORED_OFFSET),
                    "{policy:?}, {consumer:?}: setup must load the old bookmark"
                );
            }

            // These files arrive after recovery, so only the production directory
            // sweep can discover them. Both directories must be cleaned durably.
            for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
                harness.persist_bookmark(kind, STRAY_ID).await;
            }

            // Check the purge's effects on the live partition before discarding it.
            partition
                .complete_purge_with_storage(&harness.storage, NEW_GENERATION)
                .await
                .expect("complete purge cleanup");
            assert_eq!(
                partition.applied_purge_generation(),
                NEW_GENERATION,
                "{policy:?}: purge must advance the applied generation"
            );
            for consumer in consumers() {
                assert_eq!(
                    partition.get_consumer_offset(consumer),
                    None,
                    "{policy:?}, {consumer:?}: purge must clear the live bookmark"
                );
            }
            for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
                assert_eq!(
                    partition.durable_consumer_offset_count(kind),
                    0,
                    "{policy:?}, {kind:?}: purge must clear durability tracking"
                );
                let directory = harness.offset_directory(kind);
                let remaining_paths: Vec<_> = harness
                    .storage
                    .entries(&directory)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|entry| directory.join(entry.name))
                    .collect();
                assert!(
                    remaining_paths.is_empty(),
                    "{policy:?}, {kind:?}: purge left bookmark files: {remaining_paths:?}"
                );
            }
            drop(partition);

            // Save messages 0 through 4 after purge, then simulate power loss.
            // The new partition must load its messages and bookmarks from storage.
            harness.persist_fresh_history().await;
            harness.storage.crash(Crash::PowerLoss);
            let recovered = harness.recover_partition().await;

            assert_eq!(
                recovered.applied_purge_generation(),
                NEW_GENERATION,
                "{policy:?}: the completed purge marker must survive power loss"
            );
            for consumer in consumers() {
                assert_eq!(
                    recovered.get_consumer_offset(consumer),
                    None,
                    "{policy:?}, {consumer:?}: a deleted bookmark must not return after power loss"
                );
            }
            for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
                assert_eq!(
                    recovered.durable_consumer_offset_count(kind),
                    0,
                    "{policy:?}, {kind:?}: recovery must find no durable bookmarks"
                );
            }
            harness
                .poll_next_and_assert_messages(recovered, &[0, 1, 2, 3, 4])
                .await;
        }
    });
}

/// A directory sync error must leave completion pending even before a crash.
/// Read the marker into a new partition before checking the result, so a failure
/// reports the live and stored generations as well as the returned status.
async fn assert_failed_sync_leaves_purge_pending(policy: Durability, failed_kind: ConsumerKind) {
    let harness = PurgeStorageHarness::with_stored_progress(policy).await;
    let mut partition = harness.partition_with_stored_progress().await;
    let expected_trace = harness.arm_offset_directory_sync_failure(failed_kind).await;

    let result = partition
        .complete_purge_with_storage(&harness.storage, NEW_GENERATION)
        .await;
    harness
        .assert_failure_reached_after_unlinks(&expected_trace)
        .await;
    let live_generation = partition.applied_purge_generation();
    let mut reloaded = harness.empty_partition();
    reloaded
        .hydrate_applied_purge_generation_with_storage(&harness.storage)
        .await
        .unwrap();
    let stored_generation = reloaded.applied_purge_generation();

    assert!(
        result.is_err(),
        "{policy:?}, {failed_kind:?}: sync failure must reject purge; got {result:?}, \
         live generation {live_generation}, stored generation {stored_generation}"
    );
    assert_eq!(live_generation, OLD_GENERATION);
    assert_eq!(stored_generation, OLD_GENERATION);
}

/// A completed purge must never recover an old bookmark alongside fresh history.
/// If cleanup reports failure, retaining the earlier generation keeps cleanup
/// pending; this test does not demand cleared bookmarks before that retry occurs.
async fn assert_failed_sync_recovers_consistently(policy: Durability, failed_kind: ConsumerKind) {
    let harness = PurgeStorageHarness::with_stored_progress(policy).await;
    let mut partition = harness.partition_with_stored_progress().await;
    let expected_trace = harness.arm_offset_directory_sync_failure(failed_kind).await;

    let result = partition
        .complete_purge_with_storage(&harness.storage, NEW_GENERATION)
        .await;
    harness
        .assert_failure_reached_after_unlinks(&expected_trace)
        .await;
    drop(partition);

    // These durable messages make bookmark 2 valid in the new history, so recovery
    // cannot hide the stale bookmark by clamping it to the last message. This
    // fixture does not pass through the server's write acknowledgment path.
    harness.persist_fresh_history().await;
    harness.storage.crash(Crash::PowerLoss);
    let recovered = harness.recover_partition().await;
    let recovered_generation = recovered.applied_purge_generation();
    let failed_consumer = match failed_kind {
        ConsumerKind::Consumer => PollingConsumer::Consumer(CONSUMER_ID, 42),
        ConsumerKind::ConsumerGroup => PollingConsumer::ConsumerGroup(GROUP_ID, 1),
    };
    let restored_bookmark = recovered.get_consumer_offset(failed_consumer);
    let polls = harness.poll_next(recovered).await;

    let safe_recovery = match &result {
        Err(_) => recovered_generation == OLD_GENERATION,
        Ok(()) => {
            recovered_generation == NEW_GENERATION
                && polls
                    .iter()
                    .all(|poll| poll.stored_offset.is_none() && poll.offsets == [0, 1, 2, 3, 4])
        }
    };
    assert!(
        safe_recovery,
        "{policy:?}, {failed_kind:?}: purge must remain pending or recover all fresh \
         messages without old bookmarks; got {result:?}, generation {recovered_generation}, \
         bookmark from the directory that failed to sync {restored_bookmark:?}, polls {polls:?}"
    );
}

#[test]
fn given_replicated_consumer_sync_failure_when_completing_purge_should_leave_generation_pending() {
    block_on(assert_failed_sync_leaves_purge_pending(
        Durability::Replicated,
        ConsumerKind::Consumer,
    ));
}

#[test]
fn given_replicated_group_sync_failure_when_completing_purge_should_leave_generation_pending() {
    block_on(assert_failed_sync_leaves_purge_pending(
        Durability::Replicated,
        ConsumerKind::ConsumerGroup,
    ));
}

#[test]
fn given_persisted_consumer_sync_failure_when_completing_purge_should_leave_generation_pending() {
    block_on(assert_failed_sync_leaves_purge_pending(
        Durability::Persisted,
        ConsumerKind::Consumer,
    ));
}

#[test]
fn given_persisted_group_sync_failure_when_completing_purge_should_leave_generation_pending() {
    block_on(assert_failed_sync_leaves_purge_pending(
        Durability::Persisted,
        ConsumerKind::ConsumerGroup,
    ));
}

#[test]
fn given_replicated_consumer_sync_failure_when_power_is_lost_should_recover_consistently() {
    block_on(assert_failed_sync_recovers_consistently(
        Durability::Replicated,
        ConsumerKind::Consumer,
    ));
}

#[test]
fn given_replicated_group_sync_failure_when_power_is_lost_should_recover_consistently() {
    block_on(assert_failed_sync_recovers_consistently(
        Durability::Replicated,
        ConsumerKind::ConsumerGroup,
    ));
}

#[test]
fn given_persisted_consumer_sync_failure_when_power_is_lost_should_recover_consistently() {
    block_on(assert_failed_sync_recovers_consistently(
        Durability::Persisted,
        ConsumerKind::Consumer,
    ));
}

#[test]
fn given_persisted_group_sync_failure_when_power_is_lost_should_recover_consistently() {
    block_on(assert_failed_sync_recovers_consistently(
        Durability::Persisted,
        ConsumerKind::ConsumerGroup,
    ));
}
