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

use iggy_binary_protocol::{
    PrepareHeader, RoutedRequestHeader,
    requests::{partitions::TransitionPartitionHistoryRequest, system::SessionIdentity},
};
use iggy_common::{ConsumerKind, IggyError};
use server_common::Message;

use crate::PollingConsumer;

#[cfg(target_os = "linux")]
#[compio::test]
async fn checkpoint_index_sync_failure_fences_during_transfer_flush() {
    let directory = tempfile::tempdir().unwrap();
    let config = repair_config();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let prepare = checksummed_segment_prepare(1, 0, 0, b"durable");
    persistence.append(prepare.into_frozen(), true).unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition.consensus.restore_commit_state(1, 1);
    // fsync on /dev/null fails, standing in for a writeback error.
    let writer = IggyIndexWriter::new("/dev/null", Rc::new(AtomicU64::new(0)), true, false)
        .await
        .unwrap();
    let active = partition.log.index_writers().len() - 1;
    partition.log.index_writers_mut()[active] = Some(Rc::new(writer));
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    // What `drive_transfer_plan` leaves behind when the flush block then
    // falls through to `drive_checkpoint_io`.
    partition.maintenance.transfer_flush = true;
    partition.request_flush(1);
    partition.maintenance.checkpoint = Some(PendingCheckpoint::Index(1));
    let crate::PartitionIoStep::Ready(plan) = partition
        .io_plan(crate::PartitionIoContinuation::Checkpoint)
        .unwrap()
    else {
        panic!("the checkpoint index sync must plan");
    };
    let captured = partition.capture_io(plan, &config).unwrap().unwrap();
    assert!(matches!(captured.job, crate::PartitionIoJob::IndexSync(_)));
    let result = captured.job.execute().await;
    assert!(partition.accept_io(captured.identity, result).is_err());
    assert!(
        partition.fatal().is_some(),
        "a failed checkpoint index sync must fence inside a transfer flush too"
    );
}

/// The history is live, so the incarnation stamp is the only thing that
/// tells admission this send was aimed at an older partition with this id.
#[compio::test]
async fn given_live_history_when_send_names_an_older_incarnation_should_refuse_it() {
    const INCARNATION: u64 = 7;
    const OLDER_INCARNATION: u64 = 5;
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_created_revision(INCARNATION);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    let namespace = partition.namespace();
    let send_at = |incarnation: u64| {
        let mut send = checksumless_send_request(namespace, 1).transmute_header(
            |header, next: &mut RoutedRequestHeader| {
                *next = header;
                next.partition_incarnation = incarnation;
            },
        );
        send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
            .copy_from_slice(&build_segment_record(namespace, 0));
        send
    };
    assert!(!partition.history_deleted());

    let (sender, refusal) = consensus::oneshot_channel();
    partition
        .on_request(send_at(OLDER_INCARNATION), Some(sender))
        .await;
    // Checked before the await: a wrongly admitted send never gets a reply.
    assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
    assert_eq!(
        refusal.await.unwrap().header().status,
        IggyError::HistoryUnavailable.as_code()
    );

    partition.on_request(send_at(INCARNATION), None).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
}

#[compio::test]
async fn given_deleted_history_when_retirement_is_replicated_or_repaired_should_commit_it() {
    const FENCE_OP: u64 = 1;
    const RETIREMENT_OP: u64 = 2;
    let config = repair_config();
    let directory = tempfile::tempdir().unwrap();
    let (mut primary, _) = recording_partition_at(0, 3);
    primary.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let transition = TransitionPartitionHistoryRequest {
        incarnation: primary.created_revision(),
        metadata_op: 10,
    };
    primary
        .on_request(
            partition_control_request(
                &primary,
                Operation::TransitionPartitionHistory,
                &transition.to_bytes(),
                transition.metadata_op,
            ),
            None,
        )
        .await;
    primary.consensus.advance_commit_max(FENCE_OP);
    primary.commit_journal(&config).await;
    assert!(primary.history_deleted());
    let bytes = primary.log.journal().inner.repair_entry(FENCE_OP).unwrap();
    let fence =
        Message::<PrepareHeader>::try_from(Owned::copy_from_slice(bytes.as_slice())).unwrap();
    let identity = SessionIdentity {
        client_id: 1,
        session: 1,
        metadata_watermark: 20,
    };
    assert!(primary.session_retired(identity));
    primary
        .on_request(retire_session_request(primary.namespace(), identity), None)
        .await;
    let bytes = primary
        .log
        .journal()
        .inner
        .repair_entry(RETIREMENT_OP)
        .expect("retirement must remain journaled after the terminal fence");
    let retirement =
        Message::<PrepareHeader>::try_from(Owned::copy_from_slice(bytes.as_slice())).unwrap();

    for repaired in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (mut backup, _) = recording_partition_at(1, 3);
        backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
        backup.on_replicate(fence.clone()).await;
        backup.consensus.advance_commit_max(FENCE_OP);
        backup.commit_journal(&config).await;
        assert!(backup.history_deleted());
        if repaired {
            backup.repair = Some(armed_session(RETIREMENT_OP, 0));
            backup.apply_repaired_prepare(retirement.clone()).await;
        } else {
            backup.on_replicate(retirement.clone()).await;
        }
        assert!(
            backup.log.journal().inner.holds_op(RETIREMENT_OP),
            "retirement after the delete fence must reach the backup, repaired={repaired}"
        );
        backup.consensus.advance_commit_max(RETIREMENT_OP);
        backup.commit_journal(&config).await;
        assert_eq!(backup.consensus.commit_min(), RETIREMENT_OP);
        assert!(backup.session_retired(identity));
        assert!(backup.fatal().is_none());
    }
}

#[test]
fn recovered_history_keeps_the_observed_batch_read_floor() {
    let (mut partition, _) = recording_partition();
    assert_eq!(partition.widest_committed_batch(), 0);
    partition.widest_batch_bytes.set(4096);
    assert_eq!(partition.widest_committed_batch(), 4096);
    partition.recovered_durable_offset = Some(100);
    assert_eq!(partition.widest_committed_batch(), 4096);
}

#[compio::test]
async fn given_read_when_state_is_installed_should_reject_the_previous_history() {
    // State installation writes replacement offset tables, so the fixture
    // needs filesystem paths even though its message log starts in memory.
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(
        directory
            .path()
            .join("consumers")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] = Some(
        directory
            .path()
            .join("groups")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] = Some(
        directory
            .path()
            .join("external_groups")
            .to_string_lossy()
            .into_owned(),
    );
    let group_id = 7;
    let member_id = 1;
    let auto_commit_enabled = true;
    let auto_commit_disabled = false;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let old_automatic_result = poll_read_result(&partition, consumer, auto_commit_enabled, Some(4));
    let old_manual_result = poll_read_result(&partition, consumer, auto_commit_disabled, Some(4));

    // Replace the history after both reads have captured its old identity.
    // Offset 4 is below the new frontier, so its value alone cannot establish
    // that either result still belongs to the installed history.
    let installed_state = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 10,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    let installed_commit_operation = 12;
    partition
        .install_state_transfer(
            &repair_config(),
            installed_commit_operation,
            Vec::new(),
            &installed_state.encode(),
        )
        .await
        .unwrap();

    for old_result in [old_automatic_result, old_manual_result] {
        assert!(matches!(
            partition.complete_poll(old_result),
            Err(IggyError::TransientNotAccepted)
        ));
    }
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(
        last_polled, None,
        "old reads must not restore group progress"
    );
    assert_eq!(committed, None, "old reads must not commit an offset");
    assert_eq!(partition.consensus.pipeline_len(), 0);
}
