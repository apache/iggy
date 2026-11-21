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

use iggy_object_store::{ManifestSegment, SEGMENT_HASH_LEN};

use super::{
    LONG_TERM_SEGMENTS_MAX, LongTermLog, PUBLISH_SEGMENT, PublishSegment, SEGMENT_PAYLOAD_SIZE,
};
use crate::state_machine::{Applied, MachineCommand, PartitionStateMachine};

fn segment(start_offset: u64, end_offset: u64) -> ManifestSegment {
    ManifestSegment {
        start_offset,
        end_offset,
        start_timestamp: 1_000 + start_offset,
        end_timestamp: 1_000 + end_offset,
        max_timestamp: 2_000 + end_offset,
        size: (end_offset - start_offset + 1) * 100,
        crc32c: 0xdead_0000 | u32::try_from(start_offset).unwrap(),
        hash: [u8::try_from(start_offset % 251).unwrap(); SEGMENT_HASH_LEN],
    }
}

fn publish(log: &mut LongTermLog, op: u64, segment: ManifestSegment) -> Applied {
    let payload = PublishSegment(segment).encode();
    log.apply(
        op,
        MachineCommand {
            kind: PUBLISH_SEGMENT,
            payload: &payload,
        },
    )
}

fn log_of(segments: &[(u64, u64)]) -> LongTermLog {
    let mut log = LongTermLog::default();
    for (op, (start, end)) in (1..).zip(segments) {
        assert_eq!(
            publish(&mut log, op, segment(*start, *end)),
            Applied::Changed
        );
    }
    log
}

#[test]
fn given_segments_that_continue_each_other_when_publishing_should_append_them_and_raise_the_floor()
{
    let mut log = LongTermLog::default();
    assert_eq!(log.floor(), None);
    assert!(!log.holds_state());

    assert_eq!(publish(&mut log, 1, segment(0, 9)), Applied::Changed);
    assert_eq!(publish(&mut log, 2, segment(10, 10)), Applied::Changed);

    assert_eq!(log.segments(), [segment(0, 9), segment(10, 10)]);
    assert_eq!(log.floor(), Some(10));
    assert!(log.holds_state());
}

#[test]
fn given_a_segment_that_leaves_a_gap_or_overlaps_when_publishing_should_refuse_it() {
    let mut log = log_of(&[(0, 9)]);
    let reversed = ManifestSegment {
        end_offset: 9,
        ..segment(10, 12)
    };

    for refused in [segment(11, 20), segment(9, 20), segment(0, 4), reversed] {
        assert_eq!(publish(&mut log, 2, refused), Applied::Refused);
    }
    assert_eq!(
        publish(&mut LongTermLog::default(), 1, segment(1, 5)),
        Applied::Refused,
        "the first segment starts at the first offset"
    );

    assert_eq!(log.segments(), [segment(0, 9)]);
}

/// A primary that never learned its command committed sends it again.
#[test]
fn given_the_segment_the_log_ends_with_when_publishing_it_again_should_keep_the_log() {
    let mut log = log_of(&[(0, 9), (10, 19)]);

    assert_eq!(publish(&mut log, 3, segment(10, 19)), Applied::Changed);

    assert_eq!(log.segments(), [segment(0, 9), segment(10, 19)]);
    // The same offsets with other bytes are another segment.
    let other = ManifestSegment {
        crc32c: 7,
        ..segment(10, 19)
    };
    assert_eq!(publish(&mut log, 4, other), Applied::Refused);
    assert_eq!(publish(&mut log, 5, segment(0, 9)), Applied::Refused);
}

#[test]
fn given_a_payload_that_is_not_a_segment_when_applying_should_refuse_it() {
    let mut log = LongTermLog::default();
    let mut padded = PublishSegment(segment(0, 9)).encode();
    padded[6 * size_of::<u64>() + size_of::<u32>()] = 1;
    let whole = PublishSegment(segment(0, 9)).encode();

    for payload in [&padded[..], &whole[..SEGMENT_PAYLOAD_SIZE - 1], &[]] {
        let applied = log.apply(
            1,
            MachineCommand {
                kind: PUBLISH_SEGMENT,
                payload,
            },
        );
        assert_eq!(applied, Applied::Refused);
    }
    assert!(!log.holds_state());
}

#[test]
fn given_a_log_when_snapshotting_and_restoring_should_give_it_back() {
    let log = log_of(&[(0, 9), (10, 19), (20, 20)]);
    let mut snapshot = vec![0xff; 3];
    log.snapshot_into(&mut snapshot);
    assert_eq!(snapshot.len(), 3 + 3 * SEGMENT_PAYLOAD_SIZE);

    let mut restored = log_of(&[(0, 4)]);
    restored.restore(&snapshot[3..]).unwrap();

    assert_eq!(restored, log);
    restored.restore(&[]).unwrap();
    assert_eq!(restored, LongTermLog::default());
    let mut reset = log;
    reset.reset();
    assert_eq!(reset, LongTermLog::default());
}

/// Retention removes segments from the front, so a restored log can start
/// anywhere. It still has no gap after its first segment.
#[test]
fn given_a_snapshot_that_starts_past_zero_when_restoring_should_take_it() {
    let mut snapshot = Vec::new();
    for bytes in [segment(30, 39), segment(40, 49)].map(|segment| PublishSegment(segment).encode())
    {
        snapshot.extend_from_slice(&bytes);
    }
    let mut log = LongTermLog::default();

    log.restore(&snapshot).unwrap();

    assert_eq!(log.segments(), [segment(30, 39), segment(40, 49)]);
    assert_eq!(publish(&mut log, 9, segment(50, 59)), Applied::Changed);
}

#[test]
fn given_a_malformed_snapshot_when_restoring_should_refuse_it_and_keep_the_log() {
    let log = log_of(&[(0, 9)]);
    let gap: Vec<u8> = [segment(0, 9), segment(11, 19)]
        .iter()
        .flat_map(|segment| PublishSegment(*segment).encode())
        .collect();
    let cut = &gap[..=SEGMENT_PAYLOAD_SIZE];

    for snapshot in [&gap[..], cut] {
        let mut restored = log.clone();
        assert!(restored.restore(snapshot).is_err());
        assert_eq!(restored, log);
    }
}

#[test]
fn given_a_full_log_when_publishing_should_refuse_the_segment() {
    let mut log = LongTermLog::default();
    for offset in 0..LONG_TERM_SEGMENTS_MAX as u64 {
        assert_eq!(
            publish(&mut log, offset + 1, segment(offset, offset)),
            Applied::Changed
        );
    }
    let next = LONG_TERM_SEGMENTS_MAX as u64;

    assert_eq!(
        publish(&mut log, next + 1, segment(next, next)),
        Applied::Refused
    );

    let mut snapshot = Vec::new();
    log.snapshot_into(&mut snapshot);
    assert!(snapshot.len() < crate::state_machine::SNAPSHOT_BYTES_MAX);
}
