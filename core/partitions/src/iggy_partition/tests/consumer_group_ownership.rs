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

#[test]
fn given_delayed_poll_after_stream_recreation_when_replying_should_keep_original_encryptor() {
    let encryptor = |seed: u8| {
        Arc::new(EncryptorKind::Aes256Gcm(
            Aes256GcmEncryptor::new(&[seed; 32]).expect("valid key"),
        ))
    };
    let (original_key, replacement_key) = (encryptor(1), encryptor(2));
    let mut original = test_partition();
    original.set_encryptor(Some(Arc::clone(&original_key)));
    let read = poll_read_result(&original, PollingConsumer::Consumer(1, 0), false, Some(0));
    let completion = original.complete_poll(read).expect("completes");

    // The stream is deleted and its id reused before the reply is built.
    drop(original);
    let mut recreated = test_partition();
    recreated.set_encryptor(Some(Arc::clone(&replacement_key)));

    let pinned = completion.encryptor.expect("an encrypted stream's poll");
    assert!(Arc::ptr_eq(&pinned, &original_key));
    assert!(!Arc::ptr_eq(&pinned, &replacement_key));
    let mut plaintext = test_partition();
    let read = poll_read_result(&plaintext, PollingConsumer::Consumer(1, 0), false, None);
    assert!(
        plaintext
            .complete_poll(read)
            .expect("completes")
            .encryptor
            .is_none()
    );
}

#[test]
fn given_empty_poll_of_encrypted_stream_when_replying_should_not_carry_its_key() {
    let encryptor = Arc::new(EncryptorKind::Aes256Gcm(
        Aes256GcmEncryptor::new(&[3; 32]).expect("valid key"),
    ));
    let mut partition = test_partition();
    partition.set_encryptor(Some(Arc::clone(&encryptor)));

    let empty = poll_read_result(&partition, PollingConsumer::Consumer(1, 0), false, None);
    let completion = partition.complete_poll(empty).expect("completes");

    assert!(completion.fragments.is_empty());
    assert!(completion.encryptor.is_none());
    assert_eq!(Arc::strong_count(&encryptor), 2);
    let read = poll_read_result(&partition, PollingConsumer::Consumer(1, 0), false, Some(0));
    let pinned = partition
        .complete_poll(read)
        .expect("completes")
        .encryptor
        .expect("a poll with fragments carries its stream's key");
    assert!(Arc::ptr_eq(&pinned, &encryptor));
}

#[compio::test]
async fn given_idle_owner_when_polled_or_view_changed_should_require_drain_evidence() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    assert!(partition.consumer_group_drained(GROUP_ID));
    let read = partition
        .build_poll_plan(
            PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
            &PollingArgs::new(iggy_common::PollingStrategy::next(), 1, false),
            false,
        )
        .execute_resident();
    assert!(!partition.consumer_group_drained(GROUP_ID));
    assert!(read.fragments.is_empty());
    partition.complete_poll(read).unwrap();
    assert!(partition.consumer_group_drained(GROUP_ID));
    let read = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
        false,
        Some(0),
    );
    partition.complete_poll(read).unwrap();
    assert!(!partition.consumer_group_drained(GROUP_ID));
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert!(!partition.consumer_group_drained(GROUP_ID));
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    assert!(partition.consumer_group_drained(GROUP_ID));
}

#[compio::test]
async fn given_recorded_last_polled_when_owner_install_applies_should_clear_it() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const POLLED_OFFSET: u64 = 0;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let read = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
        false,
        Some(POLLED_OFFSET),
    );
    partition.complete_poll(read).unwrap();
    assert_eq!(
        partition.group_offset_state(GROUP_ID).0,
        Some(POLLED_OFFSET)
    );

    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID + 1).await;

    assert_eq!(
        partition.group_offset_state(GROUP_ID).0,
        None,
        "a new owner must not inherit the last polled offset"
    );
}

#[compio::test]
async fn given_busy_pipeline_when_owner_fence_arrives_should_queue_and_refuse_late_poll() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let late = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
        false,
        Some(0),
    );
    let mut send = checksumless_send_request(partition.namespace(), 1);
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    let send = send.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        *next = header;
    });
    partition.on_request(send, None).await;
    let send_op = partition.consensus.sequencer().current_sequence();
    let installation = InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id: GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id: CLIENT_ID + 1,
            session: 1,
            generation: 2,
        },
        metadata_op: partition.required_metadata_frontier + 1,
    };
    let request = partition_control_request(
        &partition,
        Operation::InstallConsumerGroupOwner,
        &installation.to_bytes(),
        installation.metadata_op,
    );
    partition.on_request(request, None).await;
    assert!(
        partition.consensus.sequencer().current_sequence() > send_op
            || partition.consensus.request_queue_len() > 0
    );
    assert!(matches!(
        partition.complete_poll(late),
        Err(IggyError::TransientNotAccepted)
    ));
    assert_eq!(partition.group_offset_state(GROUP_ID).0, None);
    for _ in 0..2 {
        let op = partition.consensus.sequencer().current_sequence();
        partition.consensus.advance_commit_max(op);
        partition.commit_journal(&repair_config()).await;
    }
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installation.owner)
    );
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
}

/// Journal installs that replace one client in two groups, apply neither
/// and change the view. A view change can leave this state, and the rebuilt
/// latch then names only the second install. Returns the first install.
async fn unapplied_installs_after_view_change(
    partition: &mut IggyPartition<IggyMessageBus>,
) -> InstallConsumerGroupOwnerRequest {
    const GROUPS: [u64; 2] = [7, 8];
    const REPLACED_CLIENT: u128 = 42;
    for group_id in GROUPS {
        install_test_owner(partition, group_id, REPLACED_CLIENT).await;
    }
    let first = next_test_installation(partition, GROUPS[0], REPLACED_CLIENT + 1);
    let second = InstallConsumerGroupOwnerRequest {
        metadata_op: first.metadata_op + 1,
        ..next_test_installation(partition, GROUPS[1], REPLACED_CLIENT + 1)
    };
    for installation in [first, second] {
        let prepare = partition.pipeline_local_request(
            owner_install_request(partition, &installation),
            None,
            None,
        );
        partition.on_replicate(prepare).await;
    }
    partition.consensus.set_view(1);
    first
}

/// A store under an unassigned owner fails at apply on every replica, so
/// the read that would prepare it must not complete.
#[compio::test]
async fn given_unassigned_captured_owner_when_group_read_completes_should_refuse_before_auto_commit()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let mut unassigned = next_test_installation(&partition, GROUP_ID, 0);
    unassigned.owner.session = 0;
    partition
        .on_request(owner_install_request(&partition, &unassigned), None)
        .await;
    commit_log(&mut partition).await;
    let op = partition.consensus.sequencer().current_sequence();
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(unassigned.owner)
    );
    partition.stats.increment_messages_count(1);
    let read = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
        true,
        Some(0),
    );

    let refusal = complete_poll_and_commit(&mut partition, read).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.sequencer().current_sequence(), op);
    assert!(
        matches!(
            refusal,
            Some(IggyError::ConsumerGroupPartitionNotOwned(client, 0))
                if client == unassigned.owner.client_id
        ),
        "{refusal:?}"
    );
    assert_eq!(partition.group_offset_state(GROUP_ID), (None, None));
}

/// A view change can leave an install that this replica has not applied.
/// The replaced owner's read must wait for it even without a commit, and
/// the successor is served once it applies.
#[compio::test]
async fn given_unapplied_install_after_view_change_when_replaced_owner_read_completes_should_refuse()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let consumer = PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0);
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let successor = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    let prepare =
        partition.pipeline_local_request(owner_install_request(&partition, &successor), None, None);
    partition.on_replicate(prepare).await;
    partition.consensus.set_view(1);
    let read = poll_read_result(&partition, consumer, false, Some(0));

    assert!(matches!(
        partition.complete_poll(read),
        Err(IggyError::TransientNotAccepted)
    ));
    assert_eq!(partition.group_offset_state(GROUP_ID), (None, None));

    commit_log(&mut partition).await;
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(successor.owner)
    );
    let read = poll_read_result(&partition, consumer, false, Some(0));
    partition.complete_poll(read).unwrap();
    assert_eq!(partition.group_offset_state(GROUP_ID).0, Some(0));
}

/// A view change can leave unapplied installs for several groups, and the
/// rebuilt latch holds only the last one. A store prepared under an earlier
/// group's replaced owner would fail at apply on every replica.
#[compio::test]
async fn given_unapplied_installs_for_two_groups_after_view_change_when_first_group_auto_commits_should_refuse_without_fatal_commit()
 {
    let mut partition = test_partition();
    let first = unapplied_installs_after_view_change(&mut partition).await;
    partition.stats.increment_messages_count(1);
    let head = partition.consensus.sequencer().current_sequence();
    let read = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(first.group_id).unwrap(), 0),
        true,
        Some(0),
    );

    let refusal = complete_poll_and_commit(&mut partition, read).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    assert!(
        matches!(refusal, Some(IggyError::TransientNotAccepted)),
        "{refusal:?}"
    );
    assert_eq!(
        partition.consumer_group_owner(first.group_id),
        Some(first.owner)
    );
    assert_eq!(partition.group_offset_state(first.group_id), (None, None));
}

/// The same state as above, but the replaced owner stores explicitly. Its
/// store would land behind the first install and fail at apply on every
/// replica, so admission waits until every unapplied install has applied.
#[compio::test]
async fn given_unapplied_installs_for_two_groups_after_view_change_when_replaced_owner_stores_should_refuse_without_fatal_commit()
 {
    let mut partition = test_partition();
    let first = unapplied_installs_after_view_change(&mut partition).await;
    partition.stats.increment_messages_count(1);
    let replaced = partition.consumer_group_owner(first.group_id).unwrap();
    let head = partition.consensus.sequencer().current_sequence();
    let group = u32::try_from(first.group_id).unwrap();
    let store = |owner: ConsumerGroupOwner, metadata_watermark: u64| {
        store_offset_request(
            owner.client_id,
            1,
            ConsumerKind::ConsumerGroup,
            group,
            0,
            AckLevel::Quorum,
        )
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = metadata_watermark;
            header.owner_generation = owner.generation;
            *next = header;
        })
    };
    let (sender, receiver) = consensus::oneshot_channel();

    let frontier = partition.required_metadata_frontier;
    partition
        .on_request(store(replaced, frontier), Some(sender))
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(
        partition.consumer_group_owner(first.group_id),
        Some(first.owner)
    );
    assert_eq!(partition.group_offset_state(first.group_id), (None, None));

    let frontier = partition.required_metadata_frontier;
    partition
        .on_request(store(first.owner, frontier), None)
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.group_offset_state(first.group_id).1, Some(0));
}

/// A store without a poll position carries owner 0. It binds to the
/// installed owner only from that owner's client and session, and only
/// when the writer routed by metadata that already decided the install.
/// Any other owner-0 store stays refused, so a store never lands under an
/// owner its writer did not see.
#[compio::test]
async fn given_owner_zero_store_when_admitted_should_bind_only_the_installed_owner_it_routed_by() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = (1 << 64) + 42;
    const STORED_OFFSET: u64 = 0;
    let group = u32::try_from(GROUP_ID).unwrap();
    let mut partition = test_partition();
    partition.stats.increment_messages_count(1);
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let installed = partition.consumer_group_owners[&GROUP_ID].installation;
    let frontier = partition.required_metadata_frontier;
    assert!(installed.metadata_op <= frontier);
    let store = |request: u64, session: u64, minimum_metadata_op: u64| {
        store_offset_request(
            CLIENT_ID,
            request,
            ConsumerKind::ConsumerGroup,
            group,
            STORED_OFFSET,
            AckLevel::Quorum,
        )
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.session = session;
            header.metadata_watermark = frontier;
            header.minimum_metadata_op = minimum_metadata_op;
            *next = header;
        })
    };
    let head = partition.consensus.sequencer().current_sequence();

    for (request, session, minimum_metadata_op) in [
        (1, installed.owner.session, installed.metadata_op - 1),
        (2, installed.owner.session + 1, installed.metadata_op),
    ] {
        let (sender, receiver) = consensus::oneshot_channel();
        partition
            .on_request(store(request, session, minimum_metadata_op), Some(sender))
            .await;
        assert_eq!(
            receiver.await.unwrap().header().status,
            IggyError::ConsumerGroupPartitionNotOwned(CLIENT_ID, 0).as_code(),
            "session {session} routed by op {minimum_metadata_op} must not bind"
        );
    }
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);

    partition
        .on_request(
            store(3, installed.owner.session, installed.metadata_op),
            None,
        )
        .await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), head + 1);
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition.group_offset_state(GROUP_ID).1,
        Some(STORED_OFFSET)
    );
}

#[compio::test]
async fn given_later_install_in_the_log_when_an_earlier_install_publishes_should_keep_the_group_fenced()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    partition.stats.increment_messages_count(1);
    let earlier = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    partition
        .on_request(owner_install_request(&partition, &earlier), None)
        .await;
    let earlier_op = partition.consensus.sequencer().current_sequence();
    // Admission refuses a second install while one is pending, but a view
    // change can leave a committed install that has not applied ahead of a
    // newer one, so the newer install enters the log directly.
    let later = InstallConsumerGroupOwnerRequest {
        owner: ConsumerGroupOwner {
            client_id: CLIENT_ID + 2,
            session: 1,
            generation: earlier.owner.generation + 1,
        },
        metadata_op: earlier.metadata_op + 1,
        ..earlier
    };
    let prepare =
        partition.pipeline_local_request(owner_install_request(&partition, &later), None, None);
    partition.on_replicate(prepare).await;
    let later_op = earlier_op + 1;
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert_eq!(partition.pending_owner_install.get(), Some(later));

    partition.consensus.advance_commit_max(earlier_op);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(earlier.owner)
    );
    assert_eq!(partition.pending_owner_install.get(), Some(later));

    let store = store_offset_request(
        earlier.owner.client_id,
        1,
        ConsumerKind::ConsumerGroup,
        u32::try_from(GROUP_ID).unwrap(),
        0,
        AckLevel::Quorum,
    )
    .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        header.owner_generation = earlier.owner.generation;
        *next = header;
    });
    let (sender, receiver) = consensus::oneshot_channel();
    partition.on_request(store, Some(sender)).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        later_op,
        "an offset under the replaced owner must not land behind the later install"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    partition.consensus.advance_commit_max(later_op);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consumer_group_owner(GROUP_ID), Some(later.owner));
    assert!(partition.pending_owner_install.get().is_none());
}

/// Repair can journal an install ahead of a lower op, so the tail header is
/// not the highest op. The first scan of the next view must still find the
/// install, or a store under the replaced owner lands behind it and fails at
/// apply on every replica.
#[compio::test]
async fn given_install_repaired_ahead_of_lower_op_when_view_starts_should_refuse_replaced_owner_store()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const CONSUMER_ID: u32 = 9;
    let mut origin = test_partition();
    let mut partition = test_partition();
    install_test_owner(&mut origin, GROUP_ID, CLIENT_ID).await;
    let installed_op = install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let replaced = partition.consumer_group_owner(GROUP_ID).unwrap();
    for replica in [&origin, &partition] {
        replica.stats.increment_messages_count(1);
    }
    let consumer_store = store_offset_request(
        CLIENT_ID + 2,
        1,
        ConsumerKind::Consumer,
        CONSUMER_ID,
        0,
        AckLevel::Quorum,
    )
    .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = origin.required_metadata_frontier;
        *next = header;
    });
    origin.on_request(consumer_store, None).await;
    let successor = next_test_installation(&origin, GROUP_ID, CLIENT_ID + 1);
    origin
        .on_request(owner_install_request(&origin, &successor), None)
        .await;
    let head = origin.consensus.sequencer().current_sequence();
    assert_eq!(head, installed_op + 2);

    partition.repair = Some(armed_session(head, installed_op));
    for op in [head, head - 1] {
        partition
            .apply_repaired_prepare(retained_prepare(&origin, op))
            .await;
    }
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    // A view start keeps the journal order and rebuilds the pipeline from it.
    partition.consensus.set_view(1);
    partition.consensus.with_pipeline_mut(|pipeline| {
        for op in installed_op + 1..=head {
            let header = partition.log.journal().inner.header_by_op(op).unwrap();
            pipeline.push(PipelineEntry::new(header));
        }
    });

    let store = store_offset_request(
        replaced.client_id,
        1,
        ConsumerKind::ConsumerGroup,
        u32::try_from(GROUP_ID).unwrap(),
        0,
        AckLevel::Quorum,
    )
    .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        header.owner_generation = replaced.generation;
        *next = header;
    });
    let (sender, receiver) = consensus::oneshot_channel();
    partition.on_request(store, Some(sender)).await;
    commit_log(&mut partition).await;

    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(successor.owner)
    );
}

#[compio::test]
async fn given_pending_install_when_the_same_install_arrives_should_refuse_the_copy() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let installation = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    partition
        .on_request(owner_install_request(&partition, &installation), None)
        .await;
    let install_op = partition.consensus.sequencer().current_sequence();
    assert!(partition.consumer_group_owner_admitted(&installation));
    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(
            owner_install_request(&partition, &installation),
            Some(sender),
        )
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        install_op
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    partition.consensus.advance_commit_max(install_op);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installation.owner)
    );
    assert!(partition.pending_owner_install.get().is_none());
    assert!(partition.consumer_group_owner_admitted(&installation));
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        Some(install_op)
    );
}

#[compio::test]
async fn given_shared_flag_when_fences_publish_should_raise_it() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const METADATA_OP: u64 = 10;
    let config = repair_config();
    let mut partition = test_partition();
    let published = Rc::new(Cell::new(false));
    partition.set_fence_published_flag(Rc::clone(&published));

    let installation = next_test_installation(&partition, GROUP_ID, CLIENT_ID);
    partition
        .on_request(owner_install_request(&partition, &installation), None)
        .await;
    let install_op = partition.consensus.sequencer().current_sequence();
    assert!(!published.get(), "an admitted install is not published");
    partition.consensus.advance_commit_max(install_op);
    partition.commit_journal(&config).await;
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        Some(install_op)
    );
    assert!(published.replace(false));

    let transition = TransitionPartitionHistoryRequest {
        incarnation: partition.created_revision(),
        metadata_op: METADATA_OP,
    };
    let fence = partition_control_request(
        &partition,
        Operation::TransitionPartitionHistory,
        &transition.to_bytes(),
        METADATA_OP,
    );
    partition.on_request(fence, None).await;
    let fence_op = partition.consensus.sequencer().current_sequence();
    assert!(!published.get(), "an admitted fence is not published");
    partition.consensus.advance_commit_max(fence_op);
    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition.installed_history_transition(&transition),
        Some(fence_op)
    );
    assert!(published.get());
}

#[compio::test]
async fn given_pending_install_when_a_send_arrives_should_admit_it() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let installation = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    partition
        .on_request(owner_install_request(&partition, &installation), None)
        .await;
    let install_op = partition.consensus.sequencer().current_sequence();
    let mut send = checksumless_send_request(partition.namespace(), 1);
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    let send = send.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        *next = header;
    });
    partition.on_request(send, None).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        install_op + 1
    );

    partition.consensus.advance_commit_max(install_op + 1);
    partition.commit_journal(&repair_config()).await;
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.commit_min(), install_op + 1);
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installation.owner)
    );
}

#[compio::test]
async fn given_send_committed_behind_install_when_the_install_publishes_should_prove_it() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let config = repair_config();
    let mut partition = test_partition();
    let installation = next_test_installation(&partition, GROUP_ID, CLIENT_ID);
    partition
        .on_request(owner_install_request(&partition, &installation), None)
        .await;
    let install_op = partition.consensus.sequencer().current_sequence();
    let send = history_send_request(&partition, 1, partition.required_metadata_frontier);
    partition.on_request(send, None).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        install_op + 1
    );
    partition.consensus.advance_commit_max(install_op + 1);

    let selected = partition.select_persistable_commits(&config);
    assert!(partition.select_commit(selected, &config, true));
    loop {
        match partition.drive_commit().unwrap() {
            CommitStep::Progress => {}
            CommitStep::Publish => break,
            CommitStep::Ready | CommitStep::Pending => {
                panic!("an install-only walk needs no I/O before it publishes")
            }
        }
    }
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installation.owner)
    );
    assert_eq!(partition.consensus.commit_min(), install_op - 1);
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        None,
        "an applied install proves nothing before its walk publishes"
    );

    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.commit_min(), install_op);
    assert_eq!(partition.consensus.commit_max(), install_op + 1);
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        Some(install_op),
        "a published install must not wait for later writes to apply"
    );
}

#[compio::test]
async fn given_full_queues_when_an_install_is_refused_should_not_block_its_successor() {
    const GROUP_ID: u64 = 8;
    const CLIENT_ID: u128 = 42;
    const CONSUMER_IDS: [u32; 2] = [7, 9];
    let (mut partition, _) =
        recording_partition_with_pipeline(0, 3, LocalPipeline::with_capacities(1, 1));
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    partition.stats.increment_messages_count(1);
    for (client_id, consumer_id) in (100..).zip(CONSUMER_IDS) {
        let store = store_offset_request(
            client_id,
            1,
            ConsumerKind::Consumer,
            consumer_id,
            0,
            AckLevel::Quorum,
        )
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = partition.required_metadata_frontier;
            *next = header;
        });
        partition.on_request(store, None).await;
    }
    assert_eq!(partition.consensus.request_queue_len(), 1);

    let refused = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(owner_install_request(&partition, &refused), Some(sender))
        .await;
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert!(partition.pending_owner_install.get().is_none());

    drop(partition.consensus.pop_queued_request());
    // Metadata replaced the refused install, so only its successor arrives.
    let successor = InstallConsumerGroupOwnerRequest {
        owner: ConsumerGroupOwner {
            client_id: CLIENT_ID + 2,
            session: 1,
            generation: refused.owner.generation + 1,
        },
        metadata_op: refused.metadata_op + 1,
        ..refused
    };
    partition
        .on_request(owner_install_request(&partition, &successor), None)
        .await;
    assert_eq!(partition.consensus.request_queue_len(), 1);
    assert_eq!(partition.pending_owner_install.get(), Some(successor));
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
}

#[compio::test]
async fn given_owner_capacity_when_installing_new_group_should_refuse_before_commit() {
    let mut partition = test_partition();
    let incarnation = partition.created_revision();
    for group_id in 0..u64::from(crate::state_transfer::CONSUMER_OFFSETS_ENTRIES_MAX) {
        partition.consumer_group_owners.insert(
            group_id,
            InstalledConsumerGroupOwner {
                installation: InstallConsumerGroupOwnerRequest {
                    incarnation,
                    group_id,
                    owner: ConsumerGroupOwner {
                        client_id: 0,
                        session: 0,
                        generation: 1,
                    },
                    metadata_op: 1,
                },
                partition_op: 1,
            },
        );
    }
    let mut installation = InstallConsumerGroupOwnerRequest {
        incarnation,
        group_id: u64::from(crate::state_transfer::CONSUMER_OFFSETS_ENTRIES_MAX),
        owner: ConsumerGroupOwner {
            client_id: 0,
            session: 0,
            generation: 2,
        },
        metadata_op: 2,
    };
    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(
            partition_control_request(
                &partition,
                Operation::InstallConsumerGroupOwner,
                &installation.to_bytes(),
                installation.metadata_op,
            ),
            Some(sender),
        )
        .await;
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
    assert!(partition.pending_owner_install.get().is_none());
    assert!(partition.fatal().is_none());
    installation.group_id = 0;
    partition
        .on_request(
            partition_control_request(
                &partition,
                Operation::InstallConsumerGroupOwner,
                &installation.to_bytes(),
                installation.metadata_op,
            ),
            None,
        )
        .await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consumer_group_owner(0), Some(installation.owner));
}

/// A user revocation can decide two owners at one op, and a reader can
/// install the first. The second must fail admission, not its commit.
#[compio::test]
async fn given_two_owner_decisions_in_one_op_when_the_second_install_arrives_should_refuse_at_admission()
 {
    const GROUP_ID: u64 = 7;
    const DECISION_OP: u64 = 5;
    let mut partition = test_partition();
    let config = repair_config();
    let incarnation = partition.created_revision();
    let installation = |client_id, generation| InstallConsumerGroupOwnerRequest {
        incarnation,
        group_id: GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id,
            session: 1,
            generation,
        },
        metadata_op: generation,
    };
    let installed = installation(42, DECISION_OP);
    partition
        .on_request(owner_install_request(&partition, &installed), None)
        .await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&config).await;
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installed.owner)
    );

    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(
            owner_install_request(&partition, &installation(43, DECISION_OP)),
            Some(sender),
        )
        .await;
    // A prepared install answers only at its commit, so check the
    // sequencer before awaiting the reply.
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        1,
        "a conflicting install must not be prepared"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::InvalidCommand.as_code()
    );
    assert!(partition.pending_owner_install.get().is_none());
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition.consumer_group_owner(GROUP_ID),
        Some(installed.owner)
    );

    let higher = installation(45, DECISION_OP + 1);
    for (admitted, owner) in [
        (installation(44, DECISION_OP - 1), installed.owner),
        (installed, installed.owner),
        (higher, higher.owner),
    ] {
        let op = partition.consensus.sequencer().current_sequence()
            + u64::from(admitted.owner.generation > installed.owner.generation);
        partition
            .on_request(owner_install_request(&partition, &admitted), None)
            .await;
        assert_eq!(
            partition.consensus.sequencer().current_sequence(),
            op,
            "{admitted:?}"
        );
        partition.consensus.advance_commit_max(op);
        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
        assert_eq!(
            partition.consumer_group_owner(GROUP_ID),
            Some(owner),
            "{admitted:?}"
        );
    }
}

#[compio::test]
async fn given_superseded_or_applied_owner_installs_when_resubmitted_should_not_consume_ops() {
    const GROUP_ID: u64 = 7;
    const FIRST_CLIENT: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, FIRST_CLIENT).await;
    let stale = partition.consumer_group_owners[&GROUP_ID].installation;
    let installed_op = install_test_owner(&mut partition, GROUP_ID, FIRST_CLIENT + 1).await;
    let installed = partition.consumer_group_owners[&GROUP_ID].installation;
    for installation in [stale, installed] {
        let (sender, receiver) = consensus::oneshot_channel();
        partition
            .on_request(
                owner_install_request(&partition, &installation),
                Some(sender),
            )
            .await;
        assert_eq!(
            partition.consensus.sequencer().current_sequence(),
            installed_op,
            "an applied or superseded installation must not consume a log entry"
        );
        assert_eq!(
            receiver.await.unwrap().header().status,
            IggyError::TransientNotAccepted.as_code()
        );
        assert!(partition.consumer_group_owner_admitted(&installation));
        assert!(partition.pending_owner_install.get().is_none());
        assert_eq!(
            partition.consumer_group_owner(GROUP_ID),
            Some(installed.owner)
        );
    }
    let foreign = InstallConsumerGroupOwnerRequest {
        incarnation: installed.incarnation + 1,
        ..stale
    };
    assert!(!partition.consumer_group_owner_admitted(&foreign));
    install_test_owner(&mut partition, GROUP_ID, FIRST_CLIENT + 2).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        installed_op + 1
    );
}

#[compio::test]
async fn given_confirmed_group_deletion_when_retired_and_restarted_should_reject_late_owner() {
    const LIVE_GROUP_ID: u64 = 1;
    const DELETED_GROUP_ID: u64 = 2;
    const CLIENT_ID: u128 = 42;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    install_test_owner(&mut partition, LIVE_GROUP_ID, CLIENT_ID).await;
    install_test_owner(&mut partition, DELETED_GROUP_ID, CLIENT_ID).await;
    let late = partition.consumer_group_owners[&DELETED_GROUP_ID].installation;
    let mut unassigned = late;
    unassigned.owner = ConsumerGroupOwner {
        client_id: 0,
        session: 0,
        generation: late.owner.generation + 1,
    };
    unassigned.metadata_op = partition.required_metadata_frontier + 1;
    let request = partition_control_request(
        &partition,
        Operation::InstallConsumerGroupOwner,
        &unassigned.to_bytes(),
        unassigned.metadata_op,
    );
    partition.on_request(request, None).await;
    partition
        .consensus
        .advance_commit_max(partition.consensus.sequencer().current_sequence());
    partition.commit_journal(&repair_config()).await;
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: DELETED_GROUP_ID + 1,
        live_group_ids: vec![LIVE_GROUP_ID],
    };
    let request = partition_control_request(
        &partition,
        Operation::RetireConsumerGroupOwners,
        &catalog.to_bytes(),
        catalog.metadata_op,
    );
    partition.on_request(request, None).await;
    let request = partition_control_request(
        &partition,
        Operation::InstallConsumerGroupOwner,
        &late.to_bytes(),
        late.metadata_op,
    );
    partition.on_request(request, None).await;
    let checkpoint = partition.consensus.sequencer().current_sequence();
    partition.consensus.advance_commit_max(checkpoint);
    partition.commit_journal(&repair_config()).await;
    settle_partition_io(&mut partition, &repair_config()).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consumer_group_owner(DELETED_GROUP_ID), None);
    assert_eq!(partition.consumer_group_owners.len(), 1);
    partition
        .persist_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    drop(partition);
    let mut recovered = test_partition();
    recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
    recovered
        .restore_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    recovered
        .consensus
        .restore_commit_state(checkpoint, checkpoint);
    recovered.consensus.sequencer().set_sequence(checkpoint);
    assert_eq!(recovered.owner_retirement, Some(catalog));
    assert_eq!(recovered.consumer_group_owner(DELETED_GROUP_ID), None);
    assert!(recovered.consumer_group_owner(LIVE_GROUP_ID).is_some());
    let request = partition_control_request(
        &recovered,
        Operation::InstallConsumerGroupOwner,
        &late.to_bytes(),
        late.metadata_op,
    );
    recovered.on_request(request, None).await;
    assert_eq!(
        recovered.consensus.sequencer().current_sequence(),
        checkpoint
    );
    install_test_owner(&mut recovered, DELETED_GROUP_ID + 1, CLIENT_ID).await;
    assert_eq!(recovered.consumer_group_owners.len(), 2);
}

#[compio::test]
async fn given_retired_owner_with_group_offset_when_catalog_arrives_should_refuse_until_offset_is_deleted()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const STORED_OFFSET: u64 = 7;
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let fence = install_unassigned_test_owner(&mut partition, GROUP_ID).await;
    let offset_key = u32::try_from(GROUP_ID).unwrap();
    partition.seed_recovered_consumer_offset(
        ConsumerKind::ConsumerGroup,
        offset_key,
        STORED_OFFSET,
        STORED_OFFSET,
    );
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: GROUP_ID + 1,
        live_group_ids: Vec::new(),
    };
    let head = partition.consensus.sequencer().current_sequence();

    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(owner_retirement_request(&partition, &catalog), Some(sender))
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a catalog must wait while a group it retires has an offset"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    partition
        .on_request(fenced_group_offset_delete(&fence), None)
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert!(
        !partition
            .durable_consumer_offsets
            .contains(ConsumerKind::ConsumerGroup, offset_key)
    );

    partition
        .on_request(owner_retirement_request(&partition, &catalog), None)
        .await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), head + 2);
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert!(!partition.consumer_group_owners.contains_key(&GROUP_ID));
    assert_eq!(partition.owner_retirement, Some(catalog));
}

/// A boot loads offset files that can be newer than its checkpoint, so two
/// replicas can apply one catalog with different offsets.
#[test]
fn given_replicas_with_different_offset_files_when_catalog_applies_should_keep_identical_owners() {
    const LIVE_GROUP_ID: u64 = 1;
    const RETIRED_GROUP_ID: u64 = 2;
    const STORED_OFFSET: u64 = 7;
    let mut with_offset = test_partition();
    let mut without_offset = test_partition();
    let live = InstallConsumerGroupOwnerRequest {
        incarnation: with_offset.created_revision(),
        group_id: LIVE_GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id: 42,
            session: 1,
            generation: 1,
        },
        metadata_op: 1,
    };
    let retired = InstallConsumerGroupOwnerRequest {
        group_id: RETIRED_GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id: 0,
            session: 0,
            generation: 2,
        },
        metadata_op: 2,
        ..live
    };
    for (partition_op, installation) in (1..).zip([live, retired]) {
        for replica in [&mut with_offset, &mut without_offset] {
            replica
                .apply_consumer_group_owner(installation, partition_op)
                .unwrap();
        }
    }
    with_offset.seed_recovered_consumer_offset(
        ConsumerKind::ConsumerGroup,
        u32::try_from(RETIRED_GROUP_ID).unwrap(),
        STORED_OFFSET,
        STORED_OFFSET,
    );
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: live.incarnation,
        metadata_op: retired.metadata_op + 1,
        next_group_id: RETIRED_GROUP_ID + 1,
        live_group_ids: vec![LIVE_GROUP_ID],
    };

    for replica in [&mut with_offset, &mut without_offset] {
        replica
            .apply_consumer_group_owner_retirement(catalog.clone())
            .unwrap();
    }
    let owners = [&with_offset, &without_offset].map(|replica| {
        replica
            .consumer_group_owners
            .keys()
            .copied()
            .collect::<Vec<_>>()
    });
    assert_eq!(owners[0], owners[1], "replicas diverged on one catalog");
    assert_eq!(owners[0], [LIVE_GROUP_ID]);
}

/// A failed `NoAck` write strands a durable group offset. The reconciler
/// sends no delete for it until a restart, a transfer or a later write
/// clears the mark, and the other deleted groups must not wait for that.
#[compio::test]
async fn given_stranded_offset_of_deleted_group_when_retiring_should_keep_it_until_the_mark_clears()
{
    const STRANDED_GROUP_ID: u64 = 7;
    const FREE_GROUP_ID: u64 = 8;
    const CLIENT_ID: u128 = 42;
    const STORED_OFFSET: u64 = 7;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    install_test_owner(&mut partition, STRANDED_GROUP_ID, CLIENT_ID).await;
    install_test_owner(&mut partition, FREE_GROUP_ID, CLIENT_ID).await;
    let offset_key = u32::try_from(STRANDED_GROUP_ID).unwrap();
    partition.seed_recovered_consumer_offset(
        ConsumerKind::ConsumerGroup,
        offset_key,
        STORED_OFFSET,
        STORED_OFFSET,
    );
    partition
        .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
        .record_stranded(offset_key);
    let fence = install_unassigned_test_owner(&mut partition, STRANDED_GROUP_ID).await;
    install_unassigned_test_owner(&mut partition, FREE_GROUP_ID).await;
    let is_live = |group_id| group_id > FREE_GROUP_ID;

    assert_eq!(
        partition.dead_group_offset_keys(is_live),
        Vec::<(ConsumerKind, u32)>::new()
    );
    assert_eq!(
        partition.retirement_kept_groups(is_live),
        Some(vec![STRANDED_GROUP_ID]),
        "the free group must not wait for a stranded offset"
    );
    let keeping = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: FREE_GROUP_ID + 1,
        live_group_ids: vec![STRANDED_GROUP_ID],
    };
    partition
        .on_request(owner_retirement_request(&partition, &keeping), None)
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition
            .consumer_group_owners
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [STRANDED_GROUP_ID]
    );
    let checkpoint = partition.consensus.commit_min();
    partition
        .persist_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    drop(partition);

    // The restart loads the offset file again, with no stranded mark.
    let mut recovered = test_partition();
    recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
    recovered
        .restore_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    recovered
        .consensus
        .restore_commit_state(checkpoint, checkpoint);
    recovered.consensus.sequencer().set_sequence(checkpoint);
    recovered.seed_recovered_consumer_offset(
        ConsumerKind::ConsumerGroup,
        offset_key,
        STORED_OFFSET,
        STORED_OFFSET,
    );
    assert_eq!(recovered.retirement_kept_groups(is_live), None);
    assert_eq!(
        recovered.dead_group_offset_keys(is_live),
        [(ConsumerKind::ConsumerGroup, offset_key)]
    );
    recovered
        .on_request(fenced_group_offset_delete(&fence), None)
        .await;
    commit_log(&mut recovered).await;
    assert_eq!(recovered.retirement_kept_groups(is_live), Some(Vec::new()));
    let retiring = RetireConsumerGroupOwnersRequest {
        live_group_ids: Vec::new(),
        ..keeping
    };
    recovered
        .on_request(owner_retirement_request(&recovered, &retiring), None)
        .await;
    commit_log(&mut recovered).await;
    assert!(recovered.fatal().is_none(), "{:?}", recovered.fatal());
    assert!(recovered.consumer_group_owners.is_empty());
    assert_eq!(recovered.owner_retirement, Some(retiring));
}

/// A boot that cannot read a group offset file, or a `NoAck` delete whose
/// directory sync fails, strands the key with no durable entry. The group
/// retires at once, and when a later boot loads that file, the cleanup of
/// a retired group deletes it with no owner row.
#[compio::test]
async fn given_unloaded_offset_of_deleted_group_when_a_later_boot_loads_it_should_stay_deletable() {
    const UNREAD_GROUP_ID: u64 = 7;
    const UNSYNCED_GROUP_ID: u64 = 8;
    const FREE_GROUP_ID: u64 = 9;
    const CLIENT_ID: u128 = 42;
    const STORED_OFFSET: u64 = 7;
    const STRANDED_GROUP_IDS: [u64; 2] = [UNREAD_GROUP_ID, UNSYNCED_GROUP_ID];
    const DELETED_GROUP_IDS: [u64; 3] = [UNREAD_GROUP_ID, UNSYNCED_GROUP_ID, FREE_GROUP_ID];
    let kind = ConsumerKind::ConsumerGroup;
    let keys = STRANDED_GROUP_IDS.map(|group_id| u32::try_from(group_id).unwrap());
    let [unread_key, unsynced_key] = keys;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
    let group_offsets = directory.path().join("groups");
    std::fs::create_dir(&group_offsets).unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[kind.index()] =
        Some(group_offsets.to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    assert!(partition.seed_stranded_consumer_offset(kind, unread_key));
    for group_id in DELETED_GROUP_IDS {
        install_test_owner(&mut partition, group_id, CLIENT_ID).await;
    }
    let stored = PendingConsumerOffsetCommit::upsert(kind, unsynced_key, STORED_OFFSET);
    partition
        .persist_consumer_offset_commit(stored)
        .await
        .unwrap();
    partition.apply_consumer_offset_commit(stored);
    partition
        .consumer_offset_dir_sync_fault
        .set(Some(kind.index()));
    let delete = delete_offset_request_with_ack(CLIENT_ID, 1, kind, unsynced_key, AckLevel::NoAck);
    partition
        .apply_consumer_offset_no_ack(Box::new(*delete.header()), kind, unsynced_key, None, None)
        .await;
    partition.consumer_offset_dir_sync_fault.set(None);
    assert!(!group_offsets.join(unsynced_key.to_string()).exists());
    for key in keys {
        assert!(!partition.durable_consumer_offsets.contains(kind, key));
        assert!(
            partition
                .consumer_offset_capacity_for(kind)
                .is_stranded(key)
        );
    }
    for group_id in DELETED_GROUP_IDS {
        install_unassigned_test_owner(&mut partition, group_id).await;
    }
    let is_live = |group_id| group_id > FREE_GROUP_ID;

    assert_eq!(
        partition.retirement_kept_groups(is_live),
        Some(Vec::new()),
        "a stranded key with no durable entry must not keep its deleted group"
    );
    let retiring = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: FREE_GROUP_ID + 1,
        live_group_ids: Vec::new(),
    };
    partition
        .on_request(owner_retirement_request(&partition, &retiring), None)
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert!(partition.consumer_group_owners.is_empty());
    let checkpoint = partition.consensus.commit_min();
    partition
        .persist_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    drop(partition);

    // The later boot reads the first file, and a crash undid the unsynced
    // unlink of the second.
    let mut recovered = test_partition();
    recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
    recovered
        .restore_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    recovered
        .consensus
        .restore_commit_state(checkpoint, checkpoint);
    recovered.consensus.sequencer().set_sequence(checkpoint);
    for key in keys {
        recovered.seed_recovered_consumer_offset(kind, key, STORED_OFFSET, STORED_OFFSET);
    }
    assert!(recovered.consumer_group_owners.is_empty());
    assert_eq!(recovered.owner_retirement, Some(retiring));
    assert_eq!(
        recovered.dead_group_offset_keys(is_live),
        keys.map(|key| (kind, key))
    );
    for key in keys {
        let cleanup =
            delete_offset_request_with_ack(AUTO_COMMIT_CLIENT_ID, 1, kind, key, AckLevel::Quorum);
        recovered.on_request(cleanup, None).await;
    }
    commit_log(&mut recovered).await;
    assert!(recovered.fatal().is_none(), "{:?}", recovered.fatal());
    assert_eq!(
        recovered.durable_consumer_offset_count(kind),
        0,
        "the cleanup must delete the offsets of retired groups"
    );
}

/// The old primary had no copy of the group offset and prepared a catalog
/// that retires the group. This replica holds a copy, leads the next view
/// before that catalog applies, and admits the delete under the fence. The
/// catalog then commits first and prunes the row.
#[compio::test]
async fn given_unapplied_catalog_retiring_group_when_its_durable_offset_is_deleted_should_not_fence()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const STORED_OFFSET: u64 = 7;
    let kind = ConsumerKind::ConsumerGroup;
    let key = u32::try_from(GROUP_ID).unwrap();
    let mut partition = test_partition();
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let fence = install_unassigned_test_owner(&mut partition, GROUP_ID).await;
    partition.seed_recovered_consumer_offset(kind, key, STORED_OFFSET, STORED_OFFSET);
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: GROUP_ID + 1,
        live_group_ids: Vec::new(),
    };
    let prepare = partition.pipeline_local_request(
        owner_retirement_request(&partition, &catalog),
        None,
        None,
    );
    let catalog_op = prepare.header().op;
    partition.on_replicate(prepare).await;
    partition.consensus.set_view(1);

    partition
        .on_request(fenced_group_offset_delete(&fence), None)
        .await;
    let delete_op = partition.consensus.sequencer().current_sequence();
    assert_eq!(
        delete_op,
        catalog_op + 1,
        "the delete must be prepared behind the catalog"
    );
    commit_log(&mut partition).await;

    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus.commit_min(), delete_op);
    assert!(!partition.durable_consumer_offsets.contains(kind, key));
    assert_eq!(partition.owner_retirement, Some(catalog));
}

/// A group with no owner row passes the owner check only for the
/// auto-commit delete of its offset, and only once the installed catalog
/// retires the group.
#[compio::test]
async fn given_retired_group_without_row_when_other_requests_arrive_should_refuse() {
    const GROUP_ID: u64 = 7;
    const UNRETIRED_KEY: u32 = 9;
    const CLIENT_ID: u128 = 42;
    let kind = ConsumerKind::ConsumerGroup;
    let key = u32::try_from(GROUP_ID).unwrap();
    let mut partition = test_partition();
    partition.stats.increment_messages_count(1);
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    install_unassigned_test_owner(&mut partition, GROUP_ID).await;
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: GROUP_ID + 1,
        live_group_ids: Vec::new(),
    };
    partition
        .on_request(owner_retirement_request(&partition, &catalog), None)
        .await;
    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert!(partition.consumer_group_retired(GROUP_ID));
    assert!(!partition.consumer_group_retired(u64::from(UNRETIRED_KEY)));
    partition.seed_recovered_consumer_offset(kind, key, 0, 0);
    partition.seed_recovered_consumer_offset(kind, UNRETIRED_KEY, 0, 0);
    let frontier = partition.required_metadata_frontier;
    let head = partition.consensus.sequencer().current_sequence();

    // A current watermark keeps the frontier check from refusing it first.
    let client_delete = delete_offset_request_with_ack(CLIENT_ID, 1, kind, key, AckLevel::Quorum)
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = frontier;
            *next = header;
        });
    partition.on_request(client_delete, None).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a client delete of a retired group must be refused"
    );
    partition
        .on_request(
            store_offset_request(AUTO_COMMIT_CLIENT_ID, 2, kind, key, 0, AckLevel::Quorum),
            None,
        )
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a store of a retired group must be refused"
    );
    partition
        .on_request(
            delete_offset_request_with_ack(
                AUTO_COMMIT_CLIENT_ID,
                3,
                kind,
                UNRETIRED_KEY,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a delete for a group that has no row and no retiring catalog must be refused"
    );
    partition
        .on_request(
            delete_offset_request_with_ack(AUTO_COMMIT_CLIENT_ID, 4, kind, key, AckLevel::Quorum),
            None,
        )
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head + 1,
        "the cleanup delete of a retired group passes"
    );
}

/// Each primary keeps the deleted groups it saw with a durable stranded
/// offset. So after a view change, a catalog can keep a group that an
/// earlier catalog in the log retired.
#[test]
fn given_catalog_keeping_a_retired_group_when_applied_should_keep_it_retired() {
    const LIVE_GROUP_ID: u64 = 1;
    const RETIRED_GROUP_ID: u64 = 2;
    let mut partition = test_partition();
    let live = InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id: LIVE_GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id: 42,
            session: 1,
            generation: 1,
        },
        metadata_op: 1,
    };
    let retired = InstallConsumerGroupOwnerRequest {
        group_id: RETIRED_GROUP_ID,
        owner: ConsumerGroupOwner {
            client_id: 0,
            session: 0,
            generation: 2,
        },
        metadata_op: 2,
        ..live
    };
    for (partition_op, installation) in (1..).zip([live, retired]) {
        partition
            .apply_consumer_group_owner(installation, partition_op)
            .unwrap();
    }
    let retiring = RetireConsumerGroupOwnersRequest {
        incarnation: live.incarnation,
        metadata_op: retired.metadata_op + 1,
        next_group_id: RETIRED_GROUP_ID + 1,
        live_group_ids: vec![LIVE_GROUP_ID],
    };
    let keeping = RetireConsumerGroupOwnersRequest {
        live_group_ids: vec![LIVE_GROUP_ID, RETIRED_GROUP_ID],
        ..retiring
    };
    let later = RetireConsumerGroupOwnersRequest {
        metadata_op: retiring.metadata_op + 1,
        ..keeping.clone()
    };

    for catalog in [retiring, keeping, later] {
        let applied = partition.apply_consumer_group_owner_retirement(catalog);
        assert!(
            applied.is_ok(),
            "{applied:?}: a catalog that keeps a retired group must not fence the partition"
        );
    }
    partition.apply_consumer_group_owner(retired, 3).unwrap();
    assert_eq!(
        partition
            .consumer_group_owners
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [LIVE_GROUP_ID],
        "a retired group stays retired"
    );
}

#[test]
fn given_a_parked_poll_when_a_group_owner_is_installed_or_retired_should_notify_it() {
    // A parked group read learned that it lost its partition only from a
    // maintenance sweep, so a fetch over many partitions waited for it.
    let mut partition = test_partition();
    let notified = Rc::new(Cell::new(0_u32));
    let counter = Rc::clone(&notified);
    partition.set_poll_notifier(Some(Rc::new(move || counter.set(counter.get() + 1))));
    let install = InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id: 7,
        owner: ConsumerGroupOwner {
            client_id: 1,
            session: 1,
            generation: 1,
        },
        metadata_op: 1,
    };
    partition
        .apply_consumer_group_owner(install, 1)
        .expect("the owner installs");
    assert_eq!(
        notified.get(),
        1,
        "an installed owner wakes the parked reads"
    );
    partition
        .apply_consumer_group_owner(
            InstallConsumerGroupOwnerRequest {
                owner: ConsumerGroupOwner {
                    client_id: 0,
                    session: 0,
                    generation: 2,
                },
                metadata_op: install.metadata_op + 1,
                ..install
            },
            2,
        )
        .expect("the unassigned owner installs");
    partition
        .apply_consumer_group_owner_retirement(RetireConsumerGroupOwnersRequest {
            incarnation: install.incarnation,
            metadata_op: install.metadata_op + 2,
            next_group_id: install.group_id + 1,
            live_group_ids: Vec::new(),
        })
        .expect("the retirement applies");
    assert_eq!(
        notified.get(),
        3,
        "each owner change wakes the parked reads"
    );
}

/// An install for a group that a catalog retires assigns the row again
/// when it commits ahead of the catalog, and the catalog apply then fences
/// the partition. After a view change the latch names only the last
/// install of the log, so the barrier must hold the catalog too.
#[compio::test]
async fn given_unapplied_install_for_retired_group_when_catalog_arrives_should_refuse_it() {
    const LIVE_GROUP_ID: u64 = 1;
    const RETIRED_GROUP_ID: u64 = 2;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    install_test_owner(&mut partition, RETIRED_GROUP_ID, CLIENT_ID).await;
    install_unassigned_test_owner(&mut partition, RETIRED_GROUP_ID).await;
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: RETIRED_GROUP_ID + 1,
        live_group_ids: vec![LIVE_GROUP_ID],
    };
    let obsolete = next_test_installation(&partition, RETIRED_GROUP_ID, CLIENT_ID);
    partition
        .on_request(owner_install_request(&partition, &obsolete), None)
        .await;
    let obsolete_op = partition.consensus.sequencer().current_sequence();
    assert_eq!(partition.pending_owner_install.get(), Some(obsolete));

    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(owner_retirement_request(&partition, &catalog), Some(sender))
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        obsolete_op,
        "a catalog must wait for a pending install of a group it retires"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    let live = next_test_installation(&partition, LIVE_GROUP_ID, CLIENT_ID);
    let prepare =
        partition.pipeline_local_request(owner_install_request(&partition, &live), None, None);
    partition.on_replicate(prepare).await;
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert_eq!(partition.pending_owner_install.get(), Some(live));
    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(owner_retirement_request(&partition, &catalog), Some(sender))
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        obsolete_op + 1,
        "a catalog must wait for every install the log retains"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );

    commit_log(&mut partition).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(
        partition.consumer_group_owner(RETIRED_GROUP_ID),
        Some(obsolete.owner)
    );
}
