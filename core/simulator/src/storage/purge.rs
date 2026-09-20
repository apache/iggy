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

//! Focused purge completion and boot recovery over the simulated filesystem.
//! The fixture enters after the message reset, with old consumer progress still
//! present. Fresh messages use a durable journal so power loss tests offset
//! recovery without assuming that the old partition's memory survived.

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
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

const CREATED_REVISION: u64 = 7;
const OLD_GENERATION: u64 = 4;
const NEW_GENERATION: u64 = 5;
const STORED_OFFSET: u64 = 2;
const CONSUMER_ID: usize = 7;
const GROUP_ID: usize = 9;
const STRAY_ID: usize = 99;
const FRESH_MESSAGE_COUNT: u64 = 5;

type TestPartition = IggyPartition<Rc<IggyMessageBus>>;

struct PurgeStorageHarness {
    storage: SimStorage,
    config: ServerConfig,
    namespace: IggyNamespace,
    policy: Durability,
}

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
            harness.assert_next_messages(recovered, &[3, 4]).await;
        }
    });
}

#[test]
fn given_completed_purge_when_power_is_lost_should_read_all_fresh_messages() {
    block_on(async {
        for policy in [Durability::Replicated, Durability::Persisted] {
            let harness = PurgeStorageHarness::with_stored_progress(policy).await;
            let mut partition = harness.empty_partition();
            harness
                .recover_progress(&mut partition, STORED_OFFSET)
                .await;
            assert_eq!(partition.applied_purge_generation(), OLD_GENERATION);
            for consumer in consumers() {
                assert_eq!(partition.get_consumer_offset(consumer), Some(STORED_OFFSET));
            }

            // These files arrive after recovery, so only the production directory
            // sweep can discover them. Both directories must be cleaned durably.
            for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
                harness.persist_bookmark(kind, STRAY_ID).await;
            }
            partition
                .complete_purge_with_storage(&harness.storage, NEW_GENERATION)
                .await
                .expect("complete purge cleanup");
            assert_eq!(partition.applied_purge_generation(), NEW_GENERATION);
            for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
                assert_eq!(partition.durable_consumer_offset_count(kind), 0);
                assert!(
                    harness
                        .storage
                        .entries(&harness.offset_directory(kind))
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
            drop(partition);

            // The old offset 2 is inside the new history. A stale recovered
            // bookmark would silently skip its first three messages.
            harness.persist_fresh_history().await;
            harness.storage.crash(Crash::PowerLoss);
            let recovered = harness.recover_partition().await;

            assert_eq!(recovered.applied_purge_generation(), NEW_GENERATION);
            for consumer in consumers() {
                assert_eq!(recovered.get_consumer_offset(consumer), None);
            }
            for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
                assert_eq!(recovered.durable_consumer_offset_count(kind), 0);
            }
            harness
                .assert_next_messages(recovered, &[0, 1, 2, 3, 4])
                .await;
        }
    });
}

impl PurgeStorageHarness {
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

    async fn persist_fresh_history(&self) {
        let mut journal = PartitionPrepareJournal::open_with_storage(
            &self.journal_directory(),
            self.namespace.inner(),
            CREATED_REVISION,
            self.storage.clone(),
        )
        .await
        .unwrap();
        let mut parent = 0;
        for offset in 0..FRESH_MESSAGE_COUNT {
            let prepare = owned_prepare(offset + 1, parent, offset);
            parent = prepare.header().checksum;
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

    async fn assert_next_messages(&self, partition: TestPartition, expected_offsets: &[u64]) {
        let partitions = IggyPartitions::new(ShardId::new(0), partition_config());
        partitions.insert(self.namespace, partition);
        for consumer in consumers() {
            let plan = partitions
                .build_poll_snapshot(
                    &self.namespace,
                    consumer,
                    &PollingArgs::new(PollingStrategy::next(), 10, false),
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
