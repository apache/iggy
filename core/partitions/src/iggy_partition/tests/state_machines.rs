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

use super::*;

/// A backup on disk at `directory` with a counter machine owning
/// [`COMMAND_KIND`], its sends and offsets under `durability`.
async fn backup_with_counter(
    config: &PartitionsConfig,
    directory: &std::path::Path,
    durability: iggy_common::Durability,
) -> Box<IggyPartition<RecordingBus>> {
    const BACKUP: u8 = 2;
    let (backup, _) = recording_partition_at(BACKUP, 3);
    let mut backup = Box::new(backup);
    backup.runtime_options.durability = durability;
    backup.runtime_options.consumer_offset_durability = durability;
    backup
        .register_state_machine(Box::new(
            crate::state_machine::registry::tests::CounterMachine::new(1, &[COMMAND_KIND]),
        ))
        .unwrap();
    backup.set_partition_dir(directory.to_string_lossy().into_owned());
    set_offset_dirs_under(&mut backup, directory);
    Box::pin(backup.open_persistence()).await.unwrap();
    backup.log.retire_front().unwrap();
    Box::pin(backup.install_empty_segment(config, 0))
        .await
        .unwrap();
    backup.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    backup
}

fn sent_prepare_oks(partition: &IggyPartition<RecordingBus>) -> Vec<u64> {
    partition
        .consensus()
        .message_bus()
        .sent_to_replicas
        .borrow()
        .iter()
        .filter_map(|(_, frame)| {
            let header = bytemuck::checked::try_from_bytes::<iggy_binary_protocol::GenericHeader>(
                frame
                    .as_slice()
                    .get(..size_of::<iggy_binary_protocol::GenericHeader>())?,
            )
            .ok()?;
            (header.command == Command::PrepareOk)
                .then(|| bytemuck::checked::from_bytes::<PrepareOkHeader>(frame.as_slice()).op)
        })
        .collect()
}

/// A command a backup receives between two batches goes to the
/// state-machine log, both logs persist, and the commit applies it.
#[compio::test]
async fn given_a_backup_when_a_command_replicates_between_batches_should_journal_persist_and_apply_it()
 {
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let mut backup =
        backup_with_counter(&config, &directory, iggy_common::Durability::Persisted).await;
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());

    let first = checksummed_segment_prepare(1, 0, 0, b"before");
    let command = command_prepare(2, first.header().checksum, COMMAND_KIND, 9);
    let third = checksummed_segment_prepare(3, command.header().checksum, 1, b"after");
    for prepare in [first, command, third] {
        Box::pin(backup.on_replicate(prepare)).await;
    }
    persistence.drain().await.unwrap();
    assert!(persistence.holds_commands());
    assert_eq!(persistence.durable_op(), 3);
    let journal = &backup.log.journal().inner;
    assert_eq!(journal.command_ops(), vec![2]);
    assert_eq!(
        journal
            .resident_entries()
            .iter()
            .map(peek_op)
            .collect::<Vec<_>>(),
        vec![1, 3]
    );

    Box::pin(commit_on_heartbeat(&mut backup, &config, 3)).await;
    Box::pin(settle_partition_io(&mut backup, &config)).await;
    assert!(backup.fatal().is_none(), "{:?}", backup.fatal());
    assert_eq!(backup.consensus.commit_min(), 3);
    assert_eq!(backup.state_machines().applied_op(), 2);
    assert_eq!(
        backup
            .state_machine::<crate::state_machine::registry::tests::CounterMachine>(
                crate::state_machine::MachineId(1)
            )
            .map(|machine| machine.total),
        Some(9)
    );
    drop(backup);
    persistence.retire();
    drop(persistence);

    let wal = directory.join("prepares-0");
    let (_, recovered) = PartitionPersistence::open(&wal, IggyNamespace::new(1, 1, 0).inner(), 0)
        .await
        .unwrap();
    let recovered: Vec<(u64, Operation)> = recovered
        .iter()
        .map(|prepare| (prepare.header().op, prepare.header().operation))
        .collect();
    assert_eq!(
        recovered,
        vec![
            (1, Operation::SendMessages),
            (2, Operation::StateMachineCommand),
            (3, Operation::SendMessages),
        ]
    );
}

/// Under the default durability a backup writes its sends without a
/// barrier, and each command's barrier has to cover them, or the command
/// never becomes durable, its `PrepareOk` never goes out, and the commit
/// stops at it. Two commands in one view, so the first's creation of the
/// state-machine log is not what carries the second.
#[compio::test]
async fn given_replicated_durability_when_two_commands_replicate_in_one_view_should_ack_and_commit_both()
 {
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let mut backup =
        backup_with_counter(&config, &directory, iggy_common::Durability::Replicated).await;
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());

    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let second = command_prepare(2, first.header().checksum, COMMAND_KIND, 9);
    let third = checksummed_segment_prepare(3, second.header().checksum, 1, b"third");
    let fourth = command_prepare(4, third.header().checksum, COMMAND_KIND, 5);
    let fifth = checksummed_segment_prepare(5, fourth.header().checksum, 2, b"fifth");
    for prepare in [first, second, third, fourth, fifth] {
        Box::pin(backup.on_replicate(prepare)).await;
    }
    persistence.drain().await.unwrap();
    Box::pin(backup.drive_persistence()).await;
    assert!(persistence.is_durable_through(4));
    let acked = sent_prepare_oks(&backup);
    assert!(
        acked.contains(&2) && acked.contains(&4),
        "both commands are acknowledged: {acked:?}"
    );

    Box::pin(commit_on_heartbeat(&mut backup, &config, 5)).await;
    Box::pin(settle_partition_io(&mut backup, &config)).await;
    assert!(backup.fatal().is_none(), "{:?}", backup.fatal());
    assert_eq!(backup.consensus.commit_min(), 5);
    assert_eq!(backup.state_machines().applied_op(), 4);
    assert_eq!(
        backup
            .state_machine::<crate::state_machine::registry::tests::CounterMachine>(
                crate::state_machine::MachineId(1)
            )
            .map(|machine| machine.total),
        Some(14)
    );
}
