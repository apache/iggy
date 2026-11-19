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

use iggy_object_store::{GeoPosition, PositionValue};

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
    notes: &[(String, GeoNote)],
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
    adoptions: &[(String, GeoPosition)],
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

    assert!(!partition.geo_group_folded(GROUP), "no entry yet");
    let notes = partition.geo_notes(&groups);
    assert_eq!(
        notes,
        [(
            "billing".to_owned(),
            GeoNote::Group {
                group_id: GROUP,
                committed: Some(100)
            }
        )]
    );
    apply_notes(&mut partition, 1, &notes);
    assert_eq!(partition.geo_notes(&groups), [], "folded");
    assert!(partition.geo_group_folded(GROUP));

    commit(&partition, 120);
    assert!(!partition.geo_group_folded(GROUP));
    let notes = partition.geo_notes(&groups);
    apply_notes(&mut partition, 2, &notes);
    assert_eq!(
        partition
            .geo_positions()
            .unwrap()
            .get("billing")
            .unwrap()
            .position,
        offset(0, 120)
    );

    let gone = partition.geo_notes(&[]);
    assert_eq!(gone, [("billing".to_owned(), GeoNote::Gone)]);
}

#[test]
fn given_a_peer_position_when_the_group_is_idle_should_read_from_it_until_the_group_stores_its_own()
{
    let mut partition = partition();
    commit(&partition, 100);
    let groups = [billing(false)];
    let notes = partition.geo_notes(&groups);
    apply_notes(&mut partition, 1, &notes);
    let peer = BTreeMap::from([("billing".to_owned(), offset(0, 250))]);

    let adoptions = partition.geo_adoptions(&groups, &peer, false);
    assert_eq!(adoptions, [("billing".to_owned(), offset(0, 250))]);
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
            .get("billing")
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
    let peer = BTreeMap::from([("billing".to_owned(), offset(0, 250))]);

    assert_eq!(partition.geo_adoptions(&consuming, &peer, false), []);
    assert_eq!(
        partition.geo_adoptions(&consuming, &peer, true),
        [("billing".to_owned(), offset(0, 250))]
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
    let peer = BTreeMap::from([("billing".to_owned(), offset(0, 250))]);

    assert_eq!(partition.geo_adoptions(&groups, &peer, false), []);
}

#[test]
fn given_a_name_without_a_local_group_when_a_peer_publishes_it_should_keep_it_for_a_group_created_later()
 {
    let mut partition = partition();
    let peer = BTreeMap::from([("billing".to_owned(), offset(2, 77))]);

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
    let peer = BTreeMap::from([("billing".to_owned(), offset(0, 1_000_000))]);

    let adoptions = partition.geo_adoptions(&groups, &peer, false);
    apply_adoptions(&mut partition, 2, &adoptions);

    assert_eq!(partition.offsets().commit_offset, 0, "an empty partition");
    assert_eq!(group_offset(&partition), Some(1_000_000));
}
