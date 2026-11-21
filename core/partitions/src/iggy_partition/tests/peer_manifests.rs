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

//! The peer manifests that a follower commits: the deltas its primary
//! proposes for a fetched manifest, the machine every replica applies them
//! to, and the peer log that follows the machine.

use super::peer_log::{follower, listed, log_shape, manifest_of, unattached};
use super::segment_chain::PREFIX;
use super::*;
use crate::peer_manifests::{
    DELTA_SEGMENTS_MAX, FOLLOW_PEER_MANIFEST, PEER_MANIFEST_MACHINE, PeerManifestCommitError,
    PeerManifestDelta, PeerManifests, deltas,
};
use crate::state_machine::{Applied, MachineCommand, PartitionStateMachine};
use crate::{PeerManifestRefusal, SegmentLocation};
use iggy_object_store::{MemoryStore, ObjectPrefix, PartitionManifest};

/// A manifest of `count` one-segment runs of ten offsets from `first`,
/// above `trim_point`.
fn run_of(trim_point: Option<u64>, first: u64, count: u64) -> PartitionManifest {
    let ranges: Vec<(u64, u64)> = (0..count)
        .map(|index| (first + index * 10, first + index * 10 + 9))
        .collect();
    manifest_of(trim_point, &ranges)
}

/// Apply every delta that takes the machine from what it holds to
/// `fetched`, and return the manifest of each step.
fn commit_all(machine: &mut PeerManifests, fetched: &PartitionManifest) -> Vec<PartitionManifest> {
    let steps = deltas(machine.applied(), fetched).unwrap();
    steps
        .iter()
        .enumerate()
        .map(|(op, delta)| {
            assert_eq!(apply(machine, op as u64 + 1, delta), Applied::Changed);
            machine.applied().unwrap().clone()
        })
        .collect()
}

fn apply(machine: &mut PeerManifests, op: u64, delta: &PeerManifestDelta) -> Applied {
    let payload = delta.encode();
    machine.apply(
        op,
        MachineCommand {
            kind: FOLLOW_PEER_MANIFEST,
            payload: &payload,
        },
    )
}

/// Commit `delta` on `partition` as a replica does at commit.
fn commit(partition: &mut IggyPartition<IggyMessageBus>, op: u64, delta: &PeerManifestDelta) {
    let payload = delta.encode();
    let applied = partition
        .state_machines
        .apply(
            op,
            MachineCommand {
                kind: FOLLOW_PEER_MANIFEST,
                payload: &payload,
            },
        )
        .unwrap();
    if applied == Applied::Changed {
        partition.follow_committed_peer_manifest();
    }
}

#[test]
fn given_each_delta_shape_when_encoded_should_decode_to_the_same_delta() {
    let fetched = run_of(Some(4), 5, 3);
    for delta in [
        PeerManifestDelta {
            prefix: PREFIX,
            published_at: 77,
            floor: fetched.floor,
            trim_point: fetched.trim_point,
            segments_keyed_by_both_offsets: true,
            kept: 2,
            segments: fetched.segments,
        },
        PeerManifestDelta {
            prefix: PREFIX,
            published_at: 0,
            floor: None,
            trim_point: None,
            segments_keyed_by_both_offsets: false,
            kept: 0,
            segments: Vec::new(),
        },
    ] {
        let payload = delta.encode();
        assert_eq!(PeerManifestDelta::decode(&payload).as_ref(), Some(&delta));
        assert_eq!(
            PeerManifestDelta::decode(&payload[..payload.len() - 1]),
            None
        );
    }
}

#[test]
fn given_a_long_first_manifest_when_split_should_commit_it_in_bounded_steps_with_rising_floors() {
    let mut machine = PeerManifests::default();
    let count = DELTA_SEGMENTS_MAX as u64 * 2 + 10;
    let fetched = run_of(None, 0, count);

    let steps = commit_all(&mut machine, &fetched);

    assert_eq!(steps.len(), 3);
    let floors: Vec<Option<u64>> = steps.iter().map(|step| step.floor).collect();
    assert!(floors.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(machine.applied(), Some(&fetched));
    for delta in deltas(None, &fetched).unwrap() {
        assert!(delta.segments.len() <= DELTA_SEGMENTS_MAX);
        assert!(delta.encode().len() + 8 <= crate::state_machine::COMMAND_BYTES_MAX);
    }
}

#[test]
fn given_an_appended_and_trimmed_manifest_when_split_should_list_only_the_new_segments() {
    let mut machine = PeerManifests::default();
    commit_all(&mut machine, &run_of(None, 0, 4));
    let fetched = run_of(Some(9), 10, 5);

    let steps = deltas(machine.applied(), &fetched).unwrap();

    assert_eq!(steps.len(), 1);
    assert_eq!(
        steps[0].kept, 3,
        "the three segments above the trim point stay"
    );
    assert_eq!(steps[0].segments, fetched.segments[3..]);
    commit_all(&mut machine, &fetched);
    assert_eq!(machine.applied(), Some(&fetched));
}

#[test]
fn given_a_committed_delta_when_committed_again_should_hold_the_same_manifest() {
    let mut machine = PeerManifests::default();
    commit_all(&mut machine, &run_of(None, 0, 2));
    let fetched = run_of(Some(9), 10, 3);
    let delta = deltas(machine.applied(), &fetched).unwrap().remove(0);

    assert_eq!(apply(&mut machine, 10, &delta), Applied::Changed);
    assert_eq!(apply(&mut machine, 11, &delta), Applied::Changed);

    assert_eq!(machine.applied(), Some(&fetched));
}

#[test]
fn given_deltas_that_break_the_log_when_applied_should_refuse_each_and_keep_the_manifest() {
    let mut machine = PeerManifests::default();
    let applied = run_of(None, 0, 3);
    commit_all(&mut machine, &applied);
    let newer = deltas(machine.applied(), &run_of(None, 0, 4))
        .unwrap()
        .remove(0);
    let other_prefix = PeerManifestDelta {
        prefix: ObjectPrefix {
            created_revision: PREFIX.created_revision + 1,
            ..PREFIX
        },
        ..newer.clone()
    };
    let keeps_too_many = PeerManifestDelta {
        kept: 9,
        ..newer.clone()
    };
    let goes_back = PeerManifestDelta {
        floor: Some(19),
        kept: 2,
        segments: Vec::new(),
        ..newer
    };
    let with_a_gap = PeerManifestDelta {
        segments: vec![listed(40, 49)],
        floor: Some(49),
        ..newer
    };

    for (op, delta) in [other_prefix, keeps_too_many, goes_back, with_a_gap]
        .iter()
        .enumerate()
    {
        assert_eq!(
            apply(&mut machine, op as u64 + 10, delta),
            Applied::Refused,
            "{delta:?}"
        );
        assert_eq!(machine.applied(), Some(&applied));
    }
    assert_eq!(
        machine.apply(
            20,
            MachineCommand {
                kind: FOLLOW_PEER_MANIFEST,
                payload: &[1, 2, 3],
            },
        ),
        Applied::Refused
    );
}

#[test]
fn given_a_committed_manifest_when_snapshotted_should_restore_the_same_manifest() {
    let mut machine = PeerManifests::default();
    assert!(!machine.holds_state());
    let fetched = run_of(Some(4), 5, 6);
    commit_all(&mut machine, &fetched);
    let mut snapshot = Vec::new();
    machine.snapshot_into(&mut snapshot);

    let mut restored = PeerManifests::default();
    restored.restore(&snapshot).unwrap();

    assert_eq!(restored.applied(), Some(&fetched));
    assert!(restored.restore(&snapshot[..snapshot.len() - 1]).is_err());
    restored.reset();
    assert_eq!(restored.applied(), None);
}

#[compio::test]
async fn given_committed_deltas_when_a_follower_applies_them_should_bring_its_peer_log_along() {
    let (_directory, mut partition) = follower(Rc::new(MemoryStore::new()));
    partition.register_builtin_state_machines().unwrap();
    let remote = |start, end| (start, end, SegmentLocation::Remote);
    let tail = |start| (start, start, SegmentLocation::Local);
    let first = run_of(None, 0, 2);
    let second = run_of(Some(9), 10, 3);

    let mut op = 0;
    for fetched in [&first, &second] {
        for delta in deltas(partition.committed_peer_manifest(), fetched).unwrap() {
            op += 1;
            commit(&mut partition, op, &delta);
        }
    }

    assert_eq!(partition.committed_peer_manifest(), Some(&second));
    assert_eq!(
        log_shape(&partition),
        (
            vec![remote(10, 19), remote(20, 29), remote(30, 39), tail(40)],
            39,
            true
        )
    );
}

#[compio::test]
async fn given_a_restarted_follower_with_committed_manifests_when_it_follows_again_should_rebuild_its_log()
 {
    let (_directory, mut before) = follower(Rc::new(MemoryStore::new()));
    before.register_builtin_state_machines().unwrap();
    let fetched = run_of(None, 0, 3);
    for (op, delta) in deltas(None, &fetched).unwrap().iter().enumerate() {
        commit(&mut before, op as u64 + 1, delta);
    }
    let snapshot = before.machines_snapshot_at(1).unwrap().unwrap();

    let (_restarted_directory, mut restarted) = unattached();
    restarted.register_builtin_state_machines().unwrap();
    restarted.restore_machines(Some(&snapshot), 1).unwrap();
    // A restart can count the peer's offsets before the log follows it.
    restarted.set_offset_space_used(true);
    restarted
        .follow_peer_log(super::peer_log::ORIGIN_ROOT, PREFIX)
        .unwrap();

    assert_eq!(log_shape(&restarted), log_shape(&before));
}

#[compio::test]
async fn given_a_fetched_manifest_when_asking_for_its_commands_should_skip_a_same_one_and_refuse_an_older_one()
 {
    let (_directory, mut partition) = follower(Rc::new(MemoryStore::new()));
    partition.register_builtin_state_machines().unwrap();
    let fetched = run_of(None, 0, 3);
    let requests = partition.peer_manifest_requests(&fetched, 1).unwrap();
    assert_eq!(requests.len(), 1);
    for (op, delta) in deltas(None, &fetched).unwrap().iter().enumerate() {
        commit(&mut partition, op as u64 + 1, delta);
    }

    let republished = PartitionManifest {
        published_at: fetched.published_at + 10,
        ..fetched
    };
    assert!(
        partition
            .peer_manifest_requests(&republished, 2)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        partition.peer_manifest_requests(&run_of(None, 0, 2), 2),
        Err(PeerManifestCommitError::Refused(
            PeerManifestRefusal::GoesBack { .. }
        ))
    ));
}

#[test]
fn given_the_machine_when_registered_should_own_the_follow_command() {
    let machine = PeerManifests::default();
    assert_eq!(machine.id(), PEER_MANIFEST_MACHINE);
    assert_eq!(machine.command_kinds(), [FOLLOW_PEER_MANIFEST]);
}
