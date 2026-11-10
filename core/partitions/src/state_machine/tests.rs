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

use consensus::{LocalPipeline, PIPELINE_PREPARE_QUEUE_MAX, PIPELINE_REQUEST_QUEUE_MAX, Sequencer};
use iggy_binary_protocol::{Operation, RoutedRequestHeader};
use iggy_common::IggyError;
use message_bus::IggyMessageBus;

use super::registry::tests::CounterMachine;
use super::{
    Applied, CommandKind, MachineCommand, MachineId, MachineRestoreError, PartitionStateMachine,
    SNAPSHOT_BYTES_MAX, StateMachineError, state_machine_pipeline_slots,
};
use crate::iggy_partition::tests::{
    build_segment_record, checksumless_send_request, repair_config, test_partition_with_pipeline,
    test_partition_with_replicas,
};
use crate::state_transfer::PartitionTransferUnavailable;
use crate::{IggyPartition, Partition};

const MACHINE: u16 = 1;
const KIND: u16 = 10;

/// Boxed: a partition is large, and a test holding several on its stack
/// overflows it in a debug build.
fn partition_with_counter(replicas: u8) -> Box<IggyPartition<IggyMessageBus>> {
    with_counter(test_partition_with_replicas(replicas))
}

fn with_counter(partition: IggyPartition<IggyMessageBus>) -> Box<IggyPartition<IggyMessageBus>> {
    let mut partition = Box::new(partition);
    partition
        .register_state_machine(Box::new(CounterMachine::new(MACHINE, &[KIND])))
        .unwrap();
    partition
}

fn head(partition: &IggyPartition<IggyMessageBus>) -> u64 {
    partition.consensus().sequencer().current_sequence()
}

fn counter_total(partition: &IggyPartition<IggyMessageBus>) -> u64 {
    partition
        .state_machine::<CounterMachine>(MachineId(MACHINE))
        .unwrap()
        .total
}

async fn command(partition: &mut IggyPartition<IggyMessageBus>, value: u64, request: u64) {
    let request = partition
        .command_request(CommandKind(KIND), &value.to_le_bytes(), request)
        .unwrap();
    Box::pin(partition.on_request(request, None)).await;
}

/// One request per client, since a client's next request waits for its last.
async fn send(partition: &mut IggyPartition<IggyMessageBus>, offset: u64, client: u128) {
    let namespace = partition.namespace();
    let mut send = checksumless_send_request(namespace, 1);
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(namespace, offset));
    let watermark = partition.required_metadata_frontier;
    let send = send.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = watermark;
        header.client = client;
        *next = header;
    });
    Box::pin(partition.on_request(send, None)).await;
}

async fn commit_all(partition: &mut IggyPartition<IggyMessageBus>) {
    let config = repair_config();
    partition.consensus().advance_commit_max(head(partition));
    for _ in 0..4 {
        Box::pin(partition.commit_journal(&config)).await;
    }
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus().commit_min(), head(partition));
}

/// The status a refused request gets back.
async fn refusal(
    partition: &mut IggyPartition<IggyMessageBus>,
    request: server_common::Message<RoutedRequestHeader>,
) -> u32 {
    let (sender, receiver) = consensus::oneshot_channel();
    Box::pin(partition.on_request(request, Some(sender))).await;
    receiver.await.unwrap().header().status
}

#[compio::test]
async fn given_commands_between_sends_when_committed_should_apply_them_in_op_order() {
    let mut partition = partition_with_counter(1);
    send(&mut partition, 0, 1).await;
    command(&mut partition, 5, 1).await;
    send(&mut partition, 1, 2).await;
    command(&mut partition, 0, 2).await;
    command(&mut partition, 7, 3).await;
    assert_eq!(head(&partition), 5);

    commit_all(&mut partition).await;

    // The zero is refused and commits as a no-op.
    assert_eq!(counter_total(&partition), 12);
    assert_eq!(partition.state_machines().applied_op(), 5);
    let journal = &partition.log.journal().inner;
    assert!(
        journal.command_ops().is_empty(),
        "a single replica has nobody to repair, so it keeps no committed command"
    );
    assert!(
        journal
            .resident_entries()
            .iter()
            .all(|entry| crate::iggy_partition::peek_operation(entry)
                != Operation::StateMachineCommand),
        "no command reaches the data log"
    );
    assert_eq!(partition.offsets().commit_offset, 1, "both sends committed");
}

#[compio::test]
async fn given_a_fatal_failure_when_asking_for_primacy_should_deny_it() {
    let mut partition = partition_with_counter(1);
    assert!(partition.state_machines_current());
    partition.fatal = Some(crate::types::FatalCommit {
        namespace_raw: partition.namespace().inner(),
        op: 0,
        operation: None,
    });
    assert!(!partition.state_machines_current());
}

#[compio::test]
async fn given_unowned_or_client_commands_when_requested_should_refuse_them() {
    let mut partition = partition_with_counter(1);
    let unowned = partition
        .command_request(CommandKind(KIND + 1), &1u64.to_le_bytes(), 1)
        .unwrap();
    assert_eq!(
        refusal(&mut partition, unowned).await,
        IggyError::InvalidCommand.as_code()
    );
    let from_client = partition
        .command_request(CommandKind(KIND), &1u64.to_le_bytes(), 2)
        .unwrap()
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.client = 7;
            *next = header;
        });
    assert_eq!(
        refusal(&mut partition, from_client).await,
        IggyError::InvalidCommand.as_code()
    );
    assert_eq!(head(&partition), 0);

    let mut bare = Box::new(test_partition_with_replicas(1));
    let request = bare
        .command_request(CommandKind(KIND), &1u64.to_le_bytes(), 1)
        .unwrap();
    assert_eq!(
        refusal(&mut bare, request).await,
        IggyError::InvalidCommand.as_code(),
        "a partition without machines admits no command"
    );
    assert_eq!(head(&bare), 0);
}

#[compio::test]
async fn given_uncommitted_commands_when_their_share_is_full_should_refuse_more() {
    // Three replicas and no peers, so nothing commits and every command stays
    // in the pipeline.
    let mut partition = partition_with_counter(3);
    let slots = partition.command_pipeline_slots();
    assert_eq!(
        slots,
        state_machine_pipeline_slots(PIPELINE_PREPARE_QUEUE_MAX)
    );
    for request in 1..=slots as u64 + 1 {
        command(&mut partition, 1, request).await;
    }
    let full = partition
        .command_request(CommandKind(KIND), &1u64.to_le_bytes(), slots as u64 + 2)
        .unwrap();
    assert_eq!(
        refusal(&mut partition, full).await,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(head(&partition), slots as u64);
    assert_eq!(partition.consensus().pipeline_len(), slots);
}

/// The share follows the pipeline depth the node configures, so a shallow
/// pipeline still leaves producers most of its slots.
#[compio::test]
async fn given_a_shallow_pipeline_when_commands_fill_their_share_should_still_admit_sends() {
    const DEPTH: usize = 4;
    let mut partition = with_counter(test_partition_with_pipeline(
        3,
        LocalPipeline::with_capacities(DEPTH, PIPELINE_REQUEST_QUEUE_MAX),
    ));
    assert_eq!(partition.command_pipeline_slots(), 1);
    for request in 1..=3 {
        command(&mut partition, 1, request).await;
    }
    assert_eq!(head(&partition), 1);
    send(&mut partition, 0, 1).await;
    assert_eq!(head(&partition), 2, "the send takes a slot commands left");
}

#[test]
fn given_pipeline_depths_when_sharing_should_give_commands_a_quarter_and_at_least_one_slot() {
    assert_eq!(state_machine_pipeline_slots(1), 1);
    assert_eq!(state_machine_pipeline_slots(4), 1);
    assert_eq!(state_machine_pipeline_slots(8), 2);
    assert_eq!(state_machine_pipeline_slots(PIPELINE_PREPARE_QUEUE_MAX), 8);
    assert_eq!(state_machine_pipeline_slots(127), 31);
}

#[compio::test]
async fn given_applied_commands_when_checkpointed_and_restored_should_bring_the_machines_back() {
    let directory = tempfile::tempdir().unwrap();
    let path = partition_path(&directory);

    let mut partition = partition_with_counter(1);
    partition.set_partition_dir(path.clone());
    command(&mut partition, 3, 1).await;
    command(&mut partition, 4, 2).await;
    commit_all(&mut partition).await;
    let checkpoint = partition.consensus().commit_min();
    Box::pin(partition.persist_retry_checkpoint(checkpoint))
        .await
        .unwrap();

    let mut recovered = partition_with_counter(1);
    recovered.set_partition_dir(path.clone());
    Box::pin(recovered.restore_retry_checkpoint(checkpoint))
        .await
        .unwrap();
    assert_eq!(counter_total(&recovered), 7);
    assert_eq!(recovered.state_machines().applied_op(), checkpoint);

    let mut other = Box::new(test_partition_with_replicas(1));
    other
        .register_state_machine(Box::new(CounterMachine::new(MACHINE + 1, &[KIND])))
        .unwrap();
    other.set_partition_dir(path);
    assert!(
        Box::pin(other.restore_retry_checkpoint(checkpoint))
            .await
            .is_err(),
        "a checkpoint naming a machine this partition does not register is refused"
    );
}

/// A checkpoint cut before a machine existed restores with that machine at
/// its start: no command of its kinds could commit before it owned them.
#[compio::test]
async fn given_a_checkpoint_from_before_a_machine_when_restored_should_start_the_machine_fresh() {
    let directory = tempfile::tempdir().unwrap();
    let path = partition_path(&directory);

    let mut partition = Box::new(test_partition_with_replicas(1));
    partition.set_partition_dir(path.clone());
    send(&mut partition, 0, 1).await;
    commit_all(&mut partition).await;
    let checkpoint = partition.consensus().commit_min();
    assert!(checkpoint > 0);
    Box::pin(partition.persist_retry_checkpoint(checkpoint))
        .await
        .unwrap();

    let mut upgraded = partition_with_counter(1);
    upgraded.set_partition_dir(path);
    Box::pin(upgraded.restore_retry_checkpoint(checkpoint))
        .await
        .unwrap();
    assert_eq!(counter_total(&upgraded), 0);
    assert_eq!(upgraded.state_machines().applied_op(), checkpoint);
}

/// A machine whose state alone fills the whole snapshot bound.
struct OversizedMachine;

impl PartitionStateMachine for OversizedMachine {
    fn id(&self) -> MachineId {
        MachineId(MACHINE)
    }

    fn command_kinds(&self) -> &[CommandKind] {
        &[]
    }

    fn apply(&mut self, _: u64, _: MachineCommand<'_>) -> Applied {
        Applied::Refused
    }

    fn snapshot_into(&self, out: &mut Vec<u8>) {
        out.resize(out.len() + SNAPSHOT_BYTES_MAX, 0);
    }

    fn restore(&mut self, _: &[u8]) -> Result<(), MachineRestoreError> {
        Ok(())
    }

    fn reset(&mut self) {}
}

/// Neither a checkpoint nor a transfer offer writes a snapshot that every
/// reader, this replica's next boot included, would refuse.
#[compio::test]
async fn given_machines_past_the_snapshot_bound_when_capturing_should_refuse_checkpoint_and_offer()
{
    let mut partition = Box::new(test_partition_with_replicas(1));
    partition
        .register_state_machine(Box::new(OversizedMachine))
        .unwrap();

    let error = partition
        .encode_retry_checkpoint(0)
        .unwrap_err()
        .into_inner()
        .and_then(|error| error.downcast::<StateMachineError>().ok());
    assert!(
        matches!(
            error.as_deref(),
            Some(StateMachineError::SnapshotTooLarge { length }) if *length > SNAPSHOT_BYTES_MAX
        ),
        "{error:?}"
    );
    assert!(matches!(
        partition.offsets_wire_snapshot_for_test(),
        Err(PartitionTransferUnavailable::StateMachines(
            StateMachineError::SnapshotTooLarge { .. }
        ))
    ));
}

fn partition_path(directory: &tempfile::TempDir) -> String {
    let path = directory.path().join("partition");
    std::fs::create_dir_all(path.join("prepares-0")).unwrap();
    path.to_string_lossy().into_owned()
}
