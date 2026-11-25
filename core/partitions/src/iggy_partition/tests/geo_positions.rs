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

use std::collections::BTreeMap;

use iggy_object_store::{GeoPosition, PositionHolder, PositionValue};

use super::*;
use crate::geo_positions::{
    ADOPT_GEO_POSITIONS, GeoNote, LocalGroup, NOTE_GEO_POSITIONS, encode_adoptions, encode_notes,
};
use crate::state_machine::MachineCommand;

const GROUP: u32 = 3;

fn partition() -> IggyPartition<IggyMessageBus> {
    let mut partition = test_partition();
    partition.register_builtin_state_machines().unwrap();
    partition
}

fn holder() -> PositionHolder {
    PositionHolder::Group("billing".to_owned())
}

fn billing(consuming: bool) -> LocalGroup {
    LocalGroup {
        group_id: GROUP,
        name: "billing".to_owned(),
        consuming,
    }
}

fn offset(reset: u64, offset: u64) -> GeoPosition {
    GeoPosition {
        reset,
        value: PositionValue::Offset(offset),
    }
}

/// The group's offset as a commit leaves it: the durable table and the
/// in-memory one.
fn commit(partition: &IggyPartition<IggyMessageBus>, stored: u64) {
    partition.durable_consumer_offsets.record_explicit(
        ConsumerKind::ConsumerGroup,
        GROUP,
        stored,
        stored,
    );
    partition.consumer_group_offsets.borrow_mut().insert(
        ConsumerGroupId(GROUP as usize),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, GROUP, stored, String::new()),
    );
}

fn apply_notes(
    partition: &mut IggyPartition<IggyMessageBus>,
    op: u64,
    notes: &[(PositionHolder, GeoNote)],
) {
    for payload in encode_notes(notes) {
        partition
            .state_machines
            .apply(
                op,
                MachineCommand {
                    kind: NOTE_GEO_POSITIONS,
                    payload: &payload,
                },
            )
            .unwrap();
    }
}

fn apply_adoptions(
    partition: &mut IggyPartition<IggyMessageBus>,
    op: u64,
    adoptions: &[(PositionHolder, GeoPosition)],
) {
    for payload in encode_adoptions(adoptions) {
        partition
            .state_machines
            .apply(
                op,
                MachineCommand {
                    kind: ADOPT_GEO_POSITIONS,
                    payload: &payload,
                },
            )
            .unwrap();
    }
}

fn group_offset(partition: &IggyPartition<IggyMessageBus>) -> Option<u64> {
    partition.get_consumer_offset(PollingConsumer::ConsumerGroup(GROUP as usize, 0))
}

#[test]
fn given_a_group_when_its_commit_changes_should_note_it_once_and_nothing_once_folded() {
    let mut partition = partition();
    commit(&partition, 100);
    let groups = [billing(false)];

    assert!(
        !partition.geo_folded(ConsumerKind::ConsumerGroup, GROUP),
        "no entry yet"
    );
    let notes = partition.geo_notes(&groups);
    assert_eq!(
        notes,
        [(
            holder(),
            GeoNote::Local {
                id: GROUP,
                committed: Some(100)
            }
        )]
    );
    apply_notes(&mut partition, 1, &notes);
    assert_eq!(partition.geo_notes(&groups), [], "folded");
    assert!(partition.geo_folded(ConsumerKind::ConsumerGroup, GROUP));

    commit(&partition, 120);
    assert!(!partition.geo_folded(ConsumerKind::ConsumerGroup, GROUP));
    let notes = partition.geo_notes(&groups);
    apply_notes(&mut partition, 2, &notes);
    assert_eq!(
        partition
            .geo_positions()
            .unwrap()
            .get(&holder())
            .unwrap()
            .position,
        offset(0, 120)
    );

    let gone = partition.geo_notes(&[]);
    assert_eq!(gone, [(holder(), GeoNote::Gone)]);
}

#[test]
fn given_a_peer_position_when_the_group_is_idle_should_read_from_it_until_the_group_stores_its_own()
{
    let mut partition = partition();
    commit(&partition, 100);
    let groups = [billing(false)];
    let notes = partition.geo_notes(&groups);
    apply_notes(&mut partition, 1, &notes);
    let peer = BTreeMap::from([(holder(), offset(0, 250))]);

    let adoptions = partition.geo_adoptions(&groups, &peer, false);
    assert_eq!(adoptions, [(holder(), offset(0, 250))]);
    apply_adoptions(&mut partition, 2, &adoptions);
    assert_eq!(group_offset(&partition), Some(250));
    assert_eq!(
        partition.geo_adoptions(&groups, &peer, false),
        [],
        "nothing left to take"
    );

    commit(&partition, 255);
    assert_eq!(group_offset(&partition), Some(255), "its own progress");
    commit(&partition, 40);
    assert_eq!(group_offset(&partition), Some(40), "its own move back");
    let notes = partition.geo_notes(&groups);
    apply_notes(&mut partition, 3, &notes);
    assert_eq!(
        partition
            .geo_positions()
            .unwrap()
            .get(&holder())
            .unwrap()
            .position,
        offset(1, 40),
        "a move back below the peer's offset bumps the reset"
    );
}

#[test]
fn given_a_group_consuming_here_when_a_peer_holds_a_higher_offset_should_take_it_only_while_polls_wait()
 {
    let mut partition = partition();
    commit(&partition, 100);
    let consuming = [billing(true)];
    let notes = partition.geo_notes(&consuming);
    apply_notes(&mut partition, 1, &notes);
    let peer = BTreeMap::from([(holder(), offset(0, 250))]);

    assert_eq!(partition.geo_adoptions(&consuming, &peer, false), []);
    assert_eq!(
        partition.geo_adoptions(&consuming, &peer, true),
        [(holder(), offset(0, 250))]
    );
}

#[test]
fn given_a_commit_not_folded_yet_when_a_peer_holds_a_higher_offset_should_wait_for_the_fold() {
    let mut partition = partition();
    commit(&partition, 100);
    let groups = [billing(false)];
    let notes = partition.geo_notes(&groups);
    apply_notes(&mut partition, 1, &notes);
    commit(&partition, 130);
    let peer = BTreeMap::from([(holder(), offset(0, 250))]);

    assert_eq!(partition.geo_adoptions(&groups, &peer, false), []);
}

#[test]
fn given_a_name_without_a_local_group_when_a_peer_publishes_it_should_keep_it_for_a_group_created_later()
 {
    let mut partition = partition();
    let peer = BTreeMap::from([(holder(), offset(2, 77))]);

    let adoptions = partition.geo_adoptions(&[], &peer, false);
    apply_adoptions(&mut partition, 1, &adoptions);
    assert_eq!(group_offset(&partition), None, "no group is bound yet");

    let notes = partition.geo_notes(&[billing(false)]);
    apply_notes(&mut partition, 2, &notes);
    assert_eq!(
        group_offset(&partition),
        Some(77),
        "the new group reads from the peer's position"
    );
}

#[test]
fn given_a_position_ahead_of_the_data_when_reading_should_keep_it() {
    let mut partition = partition();
    let groups = [billing(false)];
    let notes = partition.geo_notes(&groups);
    apply_notes(&mut partition, 1, &notes);
    let peer = BTreeMap::from([(holder(), offset(0, 1_000_000))]);

    let adoptions = partition.geo_adoptions(&groups, &peer, false);
    apply_adoptions(&mut partition, 2, &adoptions);

    assert_eq!(partition.offsets().commit_offset, 0, "an empty partition");
    assert_eq!(group_offset(&partition), Some(1_000_000));
}

/// The external offset of the group as a commit leaves it.
fn commit_external(partition: &IggyPartition<IggyMessageBus>, stored: u64) {
    partition.durable_consumer_offsets.record_explicit(
        ConsumerKind::ExternalGroup,
        GROUP,
        stored,
        stored,
    );
}

/// The plain consumer's offset as a commit leaves it.
fn commit_consumer(partition: &IggyPartition<IggyMessageBus>, consumer_id: u32, stored: u64) {
    partition.durable_consumer_offsets.record_explicit(
        ConsumerKind::Consumer,
        consumer_id,
        stored,
        stored,
    );
    partition.consumer_offsets.borrow_mut().insert(
        consumer_id as usize,
        ConsumerOffset::new(ConsumerKind::Consumer, consumer_id, stored, String::new()),
    );
}

fn consumer_offset(partition: &IggyPartition<IggyMessageBus>, consumer_id: u32) -> Option<u64> {
    partition.get_consumer_offset(PollingConsumer::Consumer(consumer_id as usize, 0))
}

#[test]
fn given_a_group_that_never_stored_external_offsets_when_folding_should_note_only_its_own() {
    let partition = partition();

    let notes = partition.geo_notes(&[billing(false)]);

    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].0, holder());
}

#[test]
fn given_external_offsets_of_a_consuming_group_when_a_peer_holds_a_higher_one_should_take_it() {
    let mut partition = partition();
    commit_external(&partition, 40);
    let consuming = [billing(true)];
    let external = PositionHolder::ExternalGroup("billing".to_owned());
    let notes = partition.geo_notes(&consuming);
    assert!(notes.contains(&(
        external.clone(),
        GeoNote::Local {
            id: GROUP,
            committed: Some(40)
        }
    )));
    apply_notes(&mut partition, 1, &notes);
    assert!(partition.geo_folded(ConsumerKind::ExternalGroup, GROUP));
    let peer = BTreeMap::from([(external.clone(), offset(0, 90)), (holder(), offset(0, 90))]);

    let adoptions = partition.geo_adoptions(&consuming, &peer, false);

    assert_eq!(
        adoptions,
        [(external, offset(0, 90))],
        "members guard only the group's own offsets"
    );
    apply_adoptions(&mut partition, 2, &adoptions);
    assert_eq!(partition.geo_external_group_offset(GROUP), Some(90));
    assert_eq!(partition.external_group_offset(GROUP), Some(40), "stored");
    commit_external(&partition, 95);
    assert_eq!(partition.geo_external_group_offset(GROUP), Some(95));
}

#[test]
fn given_external_offsets_of_a_group_missing_here_when_a_peer_publishes_them_should_read_them_by_name()
 {
    let mut partition = partition();
    let external = PositionHolder::ExternalGroup("kafka.cg.billing".to_owned());
    let peer = BTreeMap::from([(external.clone(), offset(0, 300))]);

    let adoptions = partition.geo_adoptions(&[], &peer, false);
    apply_adoptions(&mut partition, 1, &adoptions);

    assert_eq!(partition.geo_position_offset(&external), Some(300));
    assert_eq!(
        partition.geo_position_offset(&PositionHolder::ExternalGroup("other".to_owned())),
        None
    );
}

#[test]
fn given_a_plain_consumer_when_a_peer_holds_a_higher_offset_should_resume_there() {
    let mut partition = partition();
    commit_consumer(&partition, 7, 10);
    let notes = partition.geo_notes(&[]);
    assert_eq!(
        notes,
        [(
            PositionHolder::Consumer(7),
            GeoNote::Local {
                id: 7,
                committed: Some(10)
            }
        )]
    );
    apply_notes(&mut partition, 1, &notes);
    let peer = BTreeMap::from([(PositionHolder::Consumer(7), offset(0, 80))]);

    let adoptions = partition.geo_adoptions(&[], &peer, false);
    apply_adoptions(&mut partition, 2, &adoptions);

    assert_eq!(consumer_offset(&partition, 7), Some(80));
    assert_eq!(consumer_offset(&partition, 8), None, "another consumer");
    commit_consumer(&partition, 7, 30);
    assert_eq!(consumer_offset(&partition, 7), Some(30), "its own seek");
    let notes = partition.geo_notes(&[]);
    apply_notes(&mut partition, 3, &notes);
    assert_eq!(
        partition
            .geo_positions()
            .unwrap()
            .get(&PositionHolder::Consumer(7))
            .unwrap()
            .position,
        offset(1, 30),
        "a seek below the peer's offset bumps the reset"
    );
}

#[test]
fn given_a_plain_consumer_that_never_stored_here_when_a_peer_publishes_it_should_bind_and_read_it()
{
    let mut partition = partition();
    let peer = BTreeMap::from([(PositionHolder::Consumer(7), offset(0, 55))]);
    let adoptions = partition.geo_adoptions(&[], &peer, false);
    apply_adoptions(&mut partition, 1, &adoptions);
    assert_eq!(consumer_offset(&partition, 7), None, "not bound yet");

    let notes = partition.geo_notes(&[]);
    assert_eq!(
        notes,
        [(
            PositionHolder::Consumer(7),
            GeoNote::Local {
                id: 7,
                committed: None
            }
        )]
    );
    apply_notes(&mut partition, 2, &notes);

    assert_eq!(consumer_offset(&partition, 7), Some(55));
    assert_eq!(partition.geo_notes(&[]), [], "folded");
}

#[test]
fn given_a_plain_consumer_whose_offset_was_deleted_when_folding_should_count_a_move_back() {
    let mut partition = partition();
    commit_consumer(&partition, 7, 10);
    let notes = partition.geo_notes(&[]);
    apply_notes(&mut partition, 1, &notes);
    partition
        .durable_consumer_offsets
        .remove(ConsumerKind::Consumer, 7);
    partition.consumer_offsets.borrow_mut().remove(&7);

    let notes = partition.geo_notes(&[]);
    apply_notes(&mut partition, 2, &notes);

    assert_eq!(
        partition
            .geo_positions()
            .unwrap()
            .get(&PositionHolder::Consumer(7))
            .unwrap()
            .position,
        GeoPosition {
            reset: 1,
            value: PositionValue::Unset
        }
    );
    assert_eq!(consumer_offset(&partition, 7), None);
}
