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

use iggy_common::IggyError;
use message_bus::IggyMessageBus;

use super::{
    ADVANCE_PAYLOAD_SIZE, ADVANCE_TIERED_FLOOR, AdvanceDecodeError, AdvanceTieredFloor,
    SEGMENT_HASH_SIZE, TIERED_FLOOR_MACHINE, TieredFloor,
};
use crate::IggyPartition;
use crate::iggy_partition::tests::test_partition_with_replicas;
use crate::state_machine::tests::{commit_all, head, partition_path, refusal};
use crate::state_machine::{Applied, MachineCommand, PartitionStateMachine, RegistrationError};

const HASH: [u8; SEGMENT_HASH_SIZE] = [7; SEGMENT_HASH_SIZE];

const fn advance(start: u64, up_to: u64) -> AdvanceTieredFloor {
    AdvanceTieredFloor {
        start,
        up_to,
        hash: HASH,
    }
}

fn apply(floor: &mut TieredFloor, op: u64, advance: AdvanceTieredFloor) -> Applied {
    let payload = advance.encode();
    floor.apply(
        op,
        MachineCommand {
            kind: ADVANCE_TIERED_FLOOR,
            payload: &payload,
        },
    )
}

fn floor_at(up_to: u64) -> TieredFloor {
    let mut floor = TieredFloor::default();
    assert_eq!(apply(&mut floor, 1, advance(0, up_to)), Applied::Changed);
    floor
}

#[test]
fn given_an_empty_floor_when_an_advance_starts_at_zero_should_raise_the_floor() {
    let mut floor = TieredFloor::default();
    assert_eq!(floor.tiered_up_to_offset(), None);
    assert_eq!(apply(&mut floor, 1, advance(0, 9)), Applied::Changed);
    assert_eq!(floor.tiered_up_to_offset(), Some(9));
}

#[test]
fn given_an_empty_floor_when_an_advance_starts_past_zero_should_refuse_it() {
    let mut floor = TieredFloor::default();
    assert_eq!(apply(&mut floor, 1, advance(1, 9)), Applied::Refused);
    assert_eq!(floor.tiered_up_to_offset(), None);
}

#[test]
fn given_a_floor_when_advances_follow_it_should_raise_it_and_refuse_gaps() {
    let mut floor = floor_at(9);
    assert_eq!(apply(&mut floor, 2, advance(10, 19)), Applied::Changed);
    assert_eq!(
        apply(&mut floor, 3, advance(21, 30)),
        Applied::Refused,
        "offset 20 was never uploaded"
    );
    assert_eq!(floor.tiered_up_to_offset(), Some(19));
    assert_eq!(apply(&mut floor, 4, advance(15, 25)), Applied::Changed);
    assert_eq!(floor.tiered_up_to_offset(), Some(25));
}

#[test]
fn given_a_floor_when_an_advance_repeats_or_ends_below_it_should_keep_the_floor() {
    let mut floor = floor_at(19);
    assert_eq!(apply(&mut floor, 2, advance(0, 19)), Applied::Changed);
    assert_eq!(apply(&mut floor, 3, advance(0, 9)), Applied::Changed);
    assert_eq!(floor.tiered_up_to_offset(), Some(19));
}

#[test]
fn given_an_advance_that_ends_before_it_starts_when_applied_should_refuse_it() {
    let mut floor = floor_at(9);
    assert_eq!(apply(&mut floor, 2, advance(10, 9)), Applied::Refused);
    assert_eq!(floor.tiered_up_to_offset(), Some(9));
}

#[test]
fn given_a_malformed_payload_when_applied_should_refuse_it() {
    let mut floor = TieredFloor::default();
    let payload = advance(0, 9).encode();
    let mut long = payload.to_vec();
    long.push(0);
    for bytes in [
        &payload[..0],
        &payload[..ADVANCE_PAYLOAD_SIZE - 1],
        &long[..],
    ] {
        let command = MachineCommand {
            kind: ADVANCE_TIERED_FLOOR,
            payload: bytes,
        };
        assert_eq!(floor.apply(1, command), Applied::Refused);
    }
    assert_eq!(floor.tiered_up_to_offset(), None);
}

#[test]
fn given_a_floor_at_the_last_offset_when_advanced_again_should_keep_it() {
    let mut floor = floor_at(u64::MAX);
    assert_eq!(
        apply(&mut floor, 2, advance(u64::MAX, u64::MAX)),
        Applied::Changed
    );
    assert_eq!(floor.tiered_up_to_offset(), Some(u64::MAX));
}

#[test]
fn given_an_advance_when_encoded_and_decoded_should_round_trip() {
    let mut hash = HASH;
    hash[0] = 1;
    hash[SEGMENT_HASH_SIZE - 1] = 2;
    let advance = AdvanceTieredFloor {
        start: 3,
        up_to: u64::MAX,
        hash,
    };
    assert_eq!(
        AdvanceTieredFloor::try_from(&advance.encode()[..]),
        Ok(advance)
    );

    let short = [0; ADVANCE_PAYLOAD_SIZE - 1];
    assert_eq!(
        AdvanceTieredFloor::try_from(&short[..]),
        Err(AdvanceDecodeError {
            length: ADVANCE_PAYLOAD_SIZE - 1
        })
    );
}

#[test]
fn given_a_floor_when_snapshotted_and_restored_should_bring_it_back() {
    let mut empty = Vec::new();
    TieredFloor::default().snapshot_into(&mut empty);
    assert_eq!(empty, Vec::<u8>::new());
    let mut restored = floor_at(5);
    restored.restore(&empty).unwrap();
    assert_eq!(restored.tiered_up_to_offset(), None);

    let mut bytes = Vec::new();
    floor_at(19).snapshot_into(&mut bytes);
    assert_eq!(bytes, 19u64.to_le_bytes());
    restored.restore(&bytes).unwrap();
    assert_eq!(restored.tiered_up_to_offset(), Some(19));

    assert!(restored.restore(&[0; 3]).is_err());
    restored.reset();
    assert_eq!(restored, TieredFloor::default());
}

/// Registered the way every builder does it. Boxed: a partition is large, and
/// a test holding several on its stack overflows it in a debug build.
fn with_floor(partition: IggyPartition<IggyMessageBus>) -> Box<IggyPartition<IggyMessageBus>> {
    let mut partition = Box::new(partition);
    partition.register_builtin_state_machines().unwrap();
    partition
}

async fn propose(
    partition: &mut IggyPartition<IggyMessageBus>,
    advance: AdvanceTieredFloor,
    request: u64,
) {
    let request = partition
        .advance_tiered_floor_request(&advance, request)
        .unwrap();
    Box::pin(partition.on_request(request, None)).await;
}

#[compio::test]
async fn given_a_registered_floor_when_advances_commit_should_raise_it_in_op_order() {
    let mut partition = with_floor(test_partition_with_replicas(1));
    propose(&mut partition, advance(0, 9), 1).await;
    propose(&mut partition, advance(21, 30), 2).await;
    propose(&mut partition, advance(10, 19), 3).await;
    assert_eq!(head(&partition), 3);

    commit_all(&mut partition).await;

    // The gap commits as a no-op, so the advance after it still follows.
    assert_eq!(partition.tiered_floor(), Some(19));
    assert_eq!(partition.state_machines().applied_op(), 3);
}

#[compio::test]
async fn given_an_advanced_floor_when_checkpointed_and_restored_should_bring_it_back() {
    let directory = tempfile::tempdir().unwrap();
    let path = partition_path(&directory);

    let mut partition = with_floor(test_partition_with_replicas(1));
    partition.set_partition_dir(path.clone());
    propose(&mut partition, advance(0, 9), 1).await;
    commit_all(&mut partition).await;
    let checkpoint = partition.consensus().commit_min();
    Box::pin(partition.persist_retry_checkpoint(checkpoint))
        .await
        .unwrap();

    let mut recovered = with_floor(test_partition_with_replicas(1));
    recovered.set_partition_dir(path);
    Box::pin(recovered.restore_retry_checkpoint(checkpoint))
        .await
        .unwrap();
    assert_eq!(recovered.tiered_floor(), Some(9));
    assert_eq!(recovered.state_machines().applied_op(), checkpoint);
}

#[compio::test]
async fn given_the_builtin_machines_when_registered_should_register_the_floor_once() {
    let mut partition = with_floor(test_partition_with_replicas(1));
    assert!(
        partition
            .state_machine::<TieredFloor>(TIERED_FLOOR_MACHINE)
            .is_some()
    );
    assert_eq!(partition.tiered_floor(), None);
    assert_eq!(
        partition.register_builtin_state_machines(),
        Err(RegistrationError::DuplicateMachine(TIERED_FLOOR_MACHINE))
    );
}

/// An empty floor writes nothing, so a partition checkpoints byte for byte as
/// one without machines, and a release without the floor reads the result.
#[compio::test]
async fn given_an_empty_floor_when_checkpointing_should_match_a_partition_without_machines() {
    let registered = with_floor(test_partition_with_replicas(1));
    let bare = Box::new(test_partition_with_replicas(1));
    assert_eq!(
        registered.encode_retry_checkpoint(0).unwrap(),
        bare.encode_retry_checkpoint(0).unwrap()
    );
}

/// A partition that never registered the floor admits none of its commands.
#[compio::test]
async fn given_a_partition_without_the_floor_when_an_advance_is_requested_should_refuse_it() {
    let mut partition = Box::new(test_partition_with_replicas(1));
    assert_eq!(partition.tiered_floor(), None);
    let request = partition
        .advance_tiered_floor_request(&advance(0, 9), 1)
        .unwrap();
    assert_eq!(
        refusal(&mut partition, request).await,
        IggyError::InvalidCommand.as_code()
    );
    assert_eq!(head(&partition), 0);
}
