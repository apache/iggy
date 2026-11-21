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
    EXPIRE_PAYLOAD_SIZE, EXPIRE_REGISTRATIONS, ExpireRegistrations, LONG_TERM_REGISTRATIONS_MAX,
    LONG_TERM_SEGMENTS_MAX, LongTermLog, ObjectRegistration, PUBLISH_SEGMENT, PublishSegment,
    REGISTER_OBJECT, REGISTRATION_PAYLOAD_SIZE, REPLACE_SEGMENTS, REPLACE_SOURCES_MAX,
    RegisterObject, ReplaceSegments, SEGMENT_PAYLOAD_SIZE, TRIM_PAYLOAD_SIZE, TRIM_SEGMENTS,
    TrimSegments,
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

/// Bytes of the count in front of each part of a snapshot.
const COUNT: usize = size_of::<u32>();

/// A snapshot written by hand, part by part, so a test can write one that
/// no log would.
fn raw_snapshot(
    segments: &[ManifestSegment],
    registrations: &[ObjectRegistration],
    expired: &[(u64, u64)],
) -> Vec<u8> {
    let mut snapshot = Vec::new();
    snapshot.extend_from_slice(&u32::try_from(segments.len()).unwrap().to_le_bytes());
    for segment in segments {
        snapshot.extend_from_slice(&PublishSegment(*segment).encode());
    }
    snapshot.extend_from_slice(&u32::try_from(registrations.len()).unwrap().to_le_bytes());
    for registration in registrations {
        snapshot.extend_from_slice(&RegisterObject(*registration).encode());
    }
    snapshot.extend_from_slice(&u32::try_from(expired.len()).unwrap().to_le_bytes());
    for (start_offset, end_offset) in expired {
        snapshot.extend_from_slice(&start_offset.to_le_bytes());
        snapshot.extend_from_slice(&end_offset.to_le_bytes());
    }
    snapshot
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
    assert_eq!(snapshot.len(), 3 + 3 * COUNT + 3 * SEGMENT_PAYLOAD_SIZE);
    assert_eq!(snapshot[3..], raw_snapshot(log.segments(), &[], &[]));

    let mut restored = log_of(&[(0, 4)]);
    restored.restore(&snapshot[3..]).unwrap();

    assert_eq!(restored, log);
    assert!(
        restored.restore(&[]).is_err(),
        "a log without state writes no part, and one with state writes its counts"
    );
    assert_eq!(restored, log);
    let mut empty = Vec::new();
    LongTermLog::default().snapshot_into(&mut empty);
    restored.restore(&empty).unwrap();
    assert_eq!(restored, LongTermLog::default());
    let mut reset = log;
    reset.reset();
    assert_eq!(reset, LongTermLog::default());
}

/// Retention removes segments from the front, so a restored log can start
/// anywhere. It still has no gap after its first segment.
#[test]
fn given_a_snapshot_that_starts_past_zero_when_restoring_should_take_it() {
    let snapshot = raw_snapshot(&[segment(30, 39), segment(40, 49)], &[], &[]);
    let mut log = LongTermLog::default();

    log.restore(&snapshot).unwrap();

    assert_eq!(log.segments(), [segment(30, 39), segment(40, 49)]);
    assert_eq!(publish(&mut log, 9, segment(50, 59)), Applied::Changed);
}

#[test]
fn given_a_malformed_snapshot_when_restoring_should_refuse_it_and_keep_the_log() {
    let log = log_of(&[(0, 9)]);
    let gap = raw_snapshot(&[segment(0, 9), segment(11, 19)], &[], &[]);
    let whole = raw_snapshot(&[segment(0, 9), segment(10, 19)], &[], &[]);
    let cut = &whole[..whole.len() - 1];
    let mut trailing = whole.clone();
    trailing.push(0);

    for snapshot in [&gap[..], cut, &trailing[..]] {
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

fn replace(log: &mut LongTermLog, op: u64, merged: ManifestSegment) -> Applied {
    let payload = ReplaceSegments(merged).encode();
    log.apply(
        op,
        MachineCommand {
            kind: REPLACE_SEGMENTS,
            payload: &payload,
        },
    )
}

fn trim(log: &mut LongTermLog, op: u64, up_to_offset: u64) -> Applied {
    let payload = TrimSegments { up_to_offset }.encode();
    log.apply(
        op,
        MachineCommand {
            kind: TRIM_SEGMENTS,
            payload: &payload,
        },
    )
}

/// The segment that holds the bytes of `sources` back to back.
fn merged(sources: &[ManifestSegment]) -> ManifestSegment {
    let (first, last) = (sources.first().unwrap(), sources.last().unwrap());
    ManifestSegment {
        start_offset: first.start_offset,
        end_offset: last.end_offset,
        start_timestamp: first.start_timestamp,
        end_timestamp: last.end_timestamp,
        max_timestamp: sources
            .iter()
            .map(|source| source.max_timestamp)
            .max()
            .unwrap(),
        size: sources.iter().map(|source| source.size).sum(),
        crc32c: sources[1..].iter().fold(first.crc32c, |crc32c, source| {
            crc32c::crc32c_combine(crc32c, source.crc32c, usize::try_from(source.size).unwrap())
        }),
        hash: [0xab; SEGMENT_HASH_LEN],
    }
}

/// A log of `count` segments of one offset each.
fn log_of_single_offsets(count: u64) -> LongTermLog {
    let mut log = LongTermLog::default();
    for offset in 0..count {
        assert_eq!(
            publish(&mut log, offset + 1, segment(offset, offset)),
            Applied::Changed
        );
    }
    log
}

#[test]
fn given_a_run_of_segments_when_replacing_it_should_put_the_merged_segment_in_its_place() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29), (30, 39)]);
    let middle = merged(&[segment(10, 19), segment(20, 29)]);

    assert_eq!(replace(&mut log, 5, middle), Applied::Changed);

    assert_eq!(log.segments(), [segment(0, 9), middle, segment(30, 39)]);
    assert_eq!(log.floor(), Some(39), "a replace never moves the floor");
    assert_eq!(log.trim_point(), None);

    // A merged segment is a source like any other, at either end of the log.
    let front = merged(&[segment(0, 9), middle]);
    assert_eq!(replace(&mut log, 6, front), Applied::Changed);
    assert_eq!(log.segments(), [front, segment(30, 39)]);
    let whole = merged(&[front, segment(30, 39)]);
    assert_eq!(replace(&mut log, 7, whole), Applied::Changed);
    assert_eq!(log.segments(), [whole]);
    assert_eq!(log.floor(), Some(39));
    assert_eq!(publish(&mut log, 8, segment(40, 49)), Applied::Changed);
}

/// The checksum of the merged segment is the one of the bytes of its sources
/// back to back, which the log works out from their checksums and sizes.
#[test]
fn given_segments_of_known_bytes_when_replacing_them_should_take_the_checksum_of_their_bytes_together()
 {
    let parts: [&[u8]; 3] = [b"first segment", b"2", b"the third and last segment"];
    let mut log = LongTermLog::default();
    let mut start_offset = 0;
    let mut sources = Vec::new();
    for (op, bytes) in (1..).zip(parts) {
        let source = ManifestSegment {
            size: bytes.len() as u64,
            crc32c: crc32c::crc32c(bytes),
            ..segment(start_offset, start_offset + 4)
        };
        assert_eq!(publish(&mut log, op, source), Applied::Changed);
        sources.push(source);
        start_offset += 5;
    }
    let of_the_bytes = ManifestSegment {
        crc32c: crc32c::crc32c(&parts.concat()),
        ..merged(&sources)
    };
    assert_eq!(of_the_bytes, merged(&sources));

    assert_eq!(replace(&mut log, 4, of_the_bytes), Applied::Changed);

    assert_eq!(log.segments(), [of_the_bytes]);
}

#[test]
fn given_a_merged_segment_that_starts_or_ends_inside_a_segment_when_replacing_should_refuse_it() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29), (30, 39)]);
    assert_eq!(trim(&mut log, 5, 9), Applied::Changed);
    let before = log.clone();
    let run = merged(&[segment(10, 19), segment(20, 29)]);

    for refused in [
        ManifestSegment {
            start_offset: 15,
            ..run
        },
        ManifestSegment {
            end_offset: 25,
            ..run
        },
        ManifestSegment {
            end_offset: 40,
            ..merged(&[segment(20, 29), segment(30, 39)])
        },
        // The trim removed the segment it starts at.
        merged(&[segment(0, 9), segment(10, 19)]),
        // It ends where a segment does, before the one it starts at.
        ManifestSegment {
            start_offset: 30,
            end_offset: 19,
            ..run
        },
    ] {
        assert_eq!(replace(&mut log, 6, refused), Applied::Refused);
    }
    assert_eq!(
        replace(&mut LongTermLog::default(), 1, run),
        Applied::Refused
    );

    assert_eq!(log, before);
    assert_eq!(
        log.segments(),
        [segment(10, 19), segment(20, 29), segment(30, 39)]
    );
}

/// A segment in the place of one segment would be other bytes for the same
/// offsets, under the key the first one has.
#[test]
fn given_a_single_source_when_replacing_should_refuse_it() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29)]);
    let before = log.clone();
    let other_bytes = ManifestSegment {
        hash: [0xab; SEGMENT_HASH_LEN],
        ..segment(10, 19)
    };

    assert_eq!(replace(&mut log, 4, other_bytes), Applied::Refused);

    assert_eq!(log, before);
}

/// What the merged segment records besides its hash follows from its
/// sources, so a replica can refuse a segment that is not their bytes.
#[test]
fn given_a_merged_segment_its_sources_do_not_add_up_to_when_replacing_should_refuse_it() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29), (30, 39)]);
    let before = log.clone();
    let run = merged(&[segment(10, 19), segment(20, 29), segment(30, 39)]);

    for (field, refused) in [
        (
            "size",
            ManifestSegment {
                size: run.size + 1,
                ..run
            },
        ),
        (
            "start_timestamp",
            ManifestSegment {
                start_timestamp: run.start_timestamp + 1,
                ..run
            },
        ),
        (
            "end_timestamp",
            ManifestSegment {
                end_timestamp: run.end_timestamp - 1,
                ..run
            },
        ),
        (
            "max_timestamp",
            ManifestSegment {
                max_timestamp: run.max_timestamp + 1,
                ..run
            },
        ),
        (
            "crc32c",
            ManifestSegment {
                crc32c: run.crc32c ^ 1,
                ..run
            },
        ),
    ] {
        assert_eq!(replace(&mut log, 5, refused), Applied::Refused, "{field}");
        assert_eq!(log, before, "{field}");
    }

    assert_eq!(replace(&mut log, 6, run), Applied::Changed);
}

/// The newest message of a run need not be in its last segment.
#[test]
fn given_a_source_in_the_middle_with_the_newest_message_when_replacing_should_take_its_timestamp() {
    let newest = ManifestSegment {
        max_timestamp: 9_000,
        ..segment(10, 19)
    };
    let mut log = LongTermLog::default();
    for (op, source) in (1..).zip([segment(0, 9), newest, segment(20, 29)]) {
        assert_eq!(publish(&mut log, op, source), Applied::Changed);
    }
    let run = merged(&[segment(0, 9), newest, segment(20, 29)]);
    assert_eq!(run.max_timestamp, 9_000);

    assert_eq!(replace(&mut log, 4, run), Applied::Changed);

    assert_eq!(log.segments(), [run]);
}

#[test]
fn given_more_sources_than_one_replace_takes_when_replacing_should_refuse_it() {
    let sources_max = REPLACE_SOURCES_MAX as u64;
    let mut log = log_of_single_offsets(sources_max + 1);
    let too_many = merged(log.segments());
    let most = merged(&log.segments()[..REPLACE_SOURCES_MAX]);

    assert_eq!(replace(&mut log, 1, too_many), Applied::Refused);
    assert_eq!(log.segments().len(), REPLACE_SOURCES_MAX + 1);

    assert_eq!(replace(&mut log, 2, most), Applied::Changed);
    assert_eq!(log.segments(), [most, segment(sources_max, sources_max)]);
}

/// A replace makes the log shorter, so a full log takes it, and then takes
/// the segment it refused before.
#[test]
fn given_a_full_log_when_replacing_should_accept_it_and_make_room_for_a_publish() {
    let segments_max = LONG_TERM_SEGMENTS_MAX as u64;
    let mut log = log_of_single_offsets(segments_max);
    let next = segment(segments_max, segments_max);
    assert_eq!(publish(&mut log, 1, next), Applied::Refused);
    let pair = merged(&[segment(0, 0), segment(1, 1)]);

    assert_eq!(replace(&mut log, 2, pair), Applied::Changed);

    assert_eq!(log.segments().len(), LONG_TERM_SEGMENTS_MAX - 1);
    assert_eq!(publish(&mut log, 3, next), Applied::Changed);
    assert_eq!(log.segments().len(), LONG_TERM_SEGMENTS_MAX);
    assert_eq!(log.floor(), Some(segments_max));
}

#[test]
fn given_a_full_log_when_trimming_should_make_room_for_a_publish() {
    let segments_max = LONG_TERM_SEGMENTS_MAX as u64;
    let mut log = log_of_single_offsets(segments_max);
    let next = segment(segments_max, segments_max);
    assert_eq!(publish(&mut log, 1, next), Applied::Refused);

    assert_eq!(trim(&mut log, 2, 0), Applied::Changed);

    assert_eq!(publish(&mut log, 3, next), Applied::Changed);
    assert_eq!(log.segments().len(), LONG_TERM_SEGMENTS_MAX);
}

/// A primary that never learned its command committed sends it again.
#[test]
fn given_a_replace_that_applied_when_applying_it_again_should_keep_the_log() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29)]);
    let run = merged(&[segment(0, 9), segment(10, 19)]);
    assert_eq!(replace(&mut log, 4, run), Applied::Changed);
    let replaced = log.clone();

    assert_eq!(replace(&mut log, 5, run), Applied::Changed);
    assert_eq!(log, replaced);

    // Once the merged segment is itself replaced, its command is refused.
    let whole = merged(&[run, segment(20, 29)]);
    assert_eq!(replace(&mut log, 6, whole), Applied::Changed);
    assert_eq!(replace(&mut log, 7, run), Applied::Refused);
    assert_eq!(log.segments(), [whole]);
}

/// The log no longer ends with the segment, so its command is not taken for
/// the one sent twice. Its offsets are below the floor, so it is refused.
#[test]
fn given_a_publish_sent_again_after_its_segment_was_merged_when_applying_should_refuse_it_and_keep_the_log()
 {
    let mut log = log_of(&[(0, 9), (10, 19)]);
    let run = merged(&[segment(0, 9), segment(10, 19)]);
    assert_eq!(replace(&mut log, 3, run), Applied::Changed);

    assert_eq!(publish(&mut log, 4, segment(10, 19)), Applied::Refused);

    assert_eq!(log.segments(), [run]);
    assert_eq!(log.floor(), Some(19));
    assert_eq!(publish(&mut log, 5, segment(20, 29)), Applied::Changed);
}

#[test]
fn given_a_payload_that_is_not_its_command_when_replacing_or_trimming_should_refuse_it() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29)]);
    let before = log.clone();
    let whole = ReplaceSegments(merged(&[segment(0, 9), segment(10, 19)])).encode();
    let mut padded = whole;
    padded[6 * size_of::<u64>() + size_of::<u32>()] = 1;

    for payload in [&padded[..], &whole[..SEGMENT_PAYLOAD_SIZE - 1], &[]] {
        let applied = log.apply(
            4,
            MachineCommand {
                kind: REPLACE_SEGMENTS,
                payload,
            },
        );
        assert_eq!(applied, Applied::Refused);
    }
    let up_to = TrimSegments { up_to_offset: 9 }.encode();
    let too_long = [up_to.as_slice(), &[0]].concat();
    for payload in [&up_to[..TRIM_PAYLOAD_SIZE - 1], &too_long[..], &[]] {
        let applied = log.apply(
            5,
            MachineCommand {
                kind: TRIM_SEGMENTS,
                payload,
            },
        );
        assert_eq!(applied, Applied::Refused);
    }
    // A kind the log does not own never reaches it through the registry.
    let applied = log.apply(
        6,
        MachineCommand {
            kind: crate::tiered_floor::ADVANCE_TIERED_FLOOR,
            payload: &up_to,
        },
    );
    assert_eq!(applied, Applied::Refused);

    assert_eq!(log, before);
}

#[test]
fn given_segments_that_end_at_or_below_an_offset_when_trimming_should_remove_them_from_the_front() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29), (30, 39)]);
    assert_eq!(log.trim_point(), None);

    // A segment that holds an offset above the trim stays whole.
    assert_eq!(trim(&mut log, 5, 18), Applied::Changed);
    assert_eq!(
        log.segments(),
        [segment(10, 19), segment(20, 29), segment(30, 39)]
    );
    assert_eq!(log.trim_point(), Some(9));

    assert_eq!(trim(&mut log, 6, 29), Applied::Changed);
    assert_eq!(log.segments(), [segment(30, 39)]);
    assert_eq!(log.trim_point(), Some(29));
    assert_eq!(log.floor(), Some(39));
}

/// The floor and the first offset of the next segment are read from the last
/// segment, so a trim never removes it.
#[test]
fn given_a_trim_at_or_past_the_floor_when_applying_should_keep_the_last_segment() {
    for up_to_offset in [39, 40, u64::MAX] {
        let mut log = log_of(&[(0, 9), (10, 19), (20, 39)]);

        assert_eq!(trim(&mut log, 4, up_to_offset), Applied::Changed);

        assert_eq!(log.segments(), [segment(20, 39)]);
        assert_eq!(log.floor(), Some(39));
        assert_eq!(log.trim_point(), Some(19));
        assert!(log.holds_state());
        assert_eq!(publish(&mut log, 5, segment(40, 49)), Applied::Changed);
        assert_eq!(log.segments(), [segment(20, 39), segment(40, 49)]);
    }
}

#[test]
fn given_a_trim_that_applied_when_applying_it_or_a_lower_one_again_should_keep_the_log() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29)]);
    assert_eq!(trim(&mut log, 4, 19), Applied::Changed);
    let trimmed = log.clone();

    for up_to_offset in [19, 9, 0] {
        assert_eq!(trim(&mut log, 5, up_to_offset), Applied::Changed);
        assert_eq!(log, trimmed);
    }

    let mut empty = LongTermLog::default();
    assert_eq!(trim(&mut empty, 1, 100), Applied::Changed);
    assert_eq!(empty, LongTermLog::default());
    assert!(!empty.holds_state());
    assert_eq!(empty.trim_point(), None);
}

#[test]
fn given_a_log_after_a_replace_and_a_trim_when_snapshotting_and_restoring_should_give_it_back() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29), (30, 39)]);
    let run = merged(&[segment(10, 19), segment(20, 29)]);
    assert_eq!(replace(&mut log, 5, run), Applied::Changed);
    assert_eq!(trim(&mut log, 6, 9), Applied::Changed);
    assert_eq!(log.segments(), [run, segment(30, 39)]);
    let mut snapshot = Vec::new();
    log.snapshot_into(&mut snapshot);
    assert_eq!(snapshot.len(), 3 * COUNT + 2 * SEGMENT_PAYLOAD_SIZE);

    let mut restored = LongTermLog::default();
    restored.restore(&snapshot).unwrap();

    assert_eq!(restored, log);
    assert_eq!(restored.trim_point(), Some(9));
    assert_eq!(restored.floor(), Some(39));
    // The restored log goes on as the one it was taken from.
    let tail = merged(&[run, segment(30, 39)]);
    for log in [&mut log, &mut restored] {
        assert_eq!(publish(log, 7, segment(40, 49)), Applied::Changed);
        assert_eq!(replace(log, 8, tail), Applied::Changed);
        assert_eq!(trim(log, 9, 39), Applied::Changed);
    }
    assert_eq!(restored, log);
    assert_eq!(restored.segments(), [segment(40, 49)]);
}

fn registration(start_offset: u64, end_offset: u64, registered_at: u64) -> ObjectRegistration {
    ObjectRegistration {
        start_offset,
        end_offset,
        registered_at,
    }
}

fn register(log: &mut LongTermLog, op: u64, registration: ObjectRegistration) -> Applied {
    let payload = RegisterObject(registration).encode();
    log.apply(
        op,
        MachineCommand {
            kind: REGISTER_OBJECT,
            payload: &payload,
        },
    )
}

fn expire(log: &mut LongTermLog, op: u64, before: u64) -> Applied {
    let payload = ExpireRegistrations { before }.encode();
    log.apply(
        op,
        MachineCommand {
            kind: EXPIRE_REGISTRATIONS,
            payload: &payload,
        },
    )
}

fn keys(log: &LongTermLog) -> Vec<(u64, u64)> {
    log.registrations()
        .iter()
        .map(ObjectRegistration::key)
        .collect()
}

#[test]
fn given_the_next_segment_or_a_run_when_registering_should_hold_each_in_offset_order() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29)]);

    for (op, (start_offset, end_offset)) in (4..).zip([(30, 35), (10, 29), (0, 19)]) {
        assert_eq!(
            register(&mut log, op, registration(start_offset, end_offset, op)),
            Applied::Changed
        );
    }

    assert_eq!(keys(&log), [(0, 19), (10, 29), (30, 35)]);
    assert_eq!(log.segments().len(), 3, "a registration lists nothing");
    let mut empty = LongTermLog::default();
    assert_eq!(
        register(&mut empty, 1, registration(0, 4, 1)),
        Applied::Changed
    );
    assert!(empty.holds_state());
}

#[test]
fn given_offsets_no_command_lists_when_registering_should_refuse_them() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29)]);
    let refused = [
        ((31, 40), "past the next segment"),
        ((30, 29), "ends before it starts"),
        ((5, 19), "starts inside a segment"),
        ((0, 15), "ends inside a segment"),
        ((10, 19), "a listed segment"),
        ((0, 35), "from a start to past the floor"),
    ];

    for ((start_offset, end_offset), what) in refused {
        assert_eq!(
            register(&mut log, 4, registration(start_offset, end_offset, 1)),
            Applied::Refused,
            "{what}"
        );
    }
    assert_eq!(log.registrations(), []);

    let sources = REPLACE_SOURCES_MAX as u64;
    let mut many = log_of_single_offsets(sources + 1);
    assert_eq!(
        register(&mut many, sources + 2, registration(0, sources, 1)),
        Applied::Refused,
        "more sources than one replace takes"
    );
    assert_eq!(
        register(&mut many, sources + 3, registration(0, sources - 1, 1)),
        Applied::Changed
    );
}

/// A primary that never learned its registration committed sends it again,
/// and the next primary registers what an earlier one stored to take it as
/// its own. Either way the later time stands.
#[test]
fn given_a_registered_object_when_registering_it_again_should_keep_the_later_time() {
    let mut log = log_of(&[(0, 9), (10, 19)]);
    assert_eq!(
        register(&mut log, 3, registration(0, 19, 100)),
        Applied::Changed
    );
    let registered = log.clone();

    assert_eq!(
        register(&mut log, 4, registration(0, 19, 100)),
        Applied::Changed
    );
    assert_eq!(log, registered);
    assert_eq!(
        register(&mut log, 5, registration(0, 19, 50)),
        Applied::Changed
    );
    assert_eq!(log, registered, "an earlier time changes nothing");

    assert_eq!(
        register(&mut log, 6, registration(0, 19, 200)),
        Applied::Changed
    );
    assert_eq!(log.registration(0, 19), Some(&registration(0, 19, 200)));
}

/// Three primaries registered the next segment, each with its own end. The
/// publish of one clears its registration. One that ends below the new
/// floor is dead by the segments, and one that ends above it waits for its
/// expiry.
#[test]
fn given_registrations_of_the_next_segment_when_one_is_published_should_clear_it_and_drop_the_dead()
{
    let mut log = log_of(&[(0, 9)]);
    for (op, end_offset) in (2..).zip([14, 19, 25]) {
        assert_eq!(
            register(&mut log, op, registration(10, end_offset, op)),
            Applied::Changed
        );
    }

    assert_eq!(publish(&mut log, 5, segment(10, 19)), Applied::Changed);

    assert_eq!(keys(&log), [(10, 25)]);
    assert_eq!(
        publish(&mut log, 6, segment(10, 19)),
        Applied::Changed,
        "sent again"
    );
    assert_eq!(keys(&log), [(10, 25)]);
}

#[test]
fn given_registered_runs_when_a_replace_commits_should_clear_its_own_and_drop_those_it_cuts() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29), (30, 39)]);
    for (op, (start_offset, end_offset)) in (5..).zip([(0, 19), (10, 29), (20, 39)]) {
        assert_eq!(
            register(&mut log, op, registration(start_offset, end_offset, op)),
            Applied::Changed
        );
    }

    let front = merged(&[segment(0, 9), segment(10, 19)]);
    assert_eq!(replace(&mut log, 8, front), Applied::Changed);

    assert_eq!(keys(&log), [(20, 39)]);
}

/// The registration of the next segment outlives any time: the rewrite of an
/// idle partition picks the same offsets again and must still publish them.
/// Once the floor passes its first offset, it expires like any other.
#[test]
fn given_registrations_older_than_an_expiry_when_it_applies_should_expire_them_but_that_of_the_next_segment()
 {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29)]);
    for (op, registered) in (4..).zip([
        registration(0, 19, 100),
        registration(10, 29, 300),
        registration(30, 39, 50),
    ]) {
        assert_eq!(register(&mut log, op, registered), Applied::Changed);
    }
    assert!(log.expires_any(200));
    assert!(!log.expires_any(100), "a registration at the time stays");

    assert_eq!(expire(&mut log, 7, 200), Applied::Changed);

    assert_eq!(log.expired(), [(0, 19)]);
    assert_eq!(keys(&log), [(10, 29), (30, 39)]);
    let expired_once = log.clone();
    assert_eq!(expire(&mut log, 8, 200), Applied::Changed);
    assert_eq!(log, expired_once, "applied again, it changes nothing");
    assert!(!log.expires_any(200));

    assert_eq!(expire(&mut log, 9, 400), Applied::Changed);
    assert_eq!(log.expired(), [(0, 19), (10, 29)]);
    assert_eq!(keys(&log), [(30, 39)]);
}

/// An expired object may be deleted by any primary, so no command names its
/// offsets again.
#[test]
fn given_an_expired_registration_when_registering_or_replacing_its_offsets_should_refuse_both() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29)]);
    assert_eq!(
        register(&mut log, 4, registration(0, 19, 100)),
        Applied::Changed
    );
    assert_eq!(expire(&mut log, 5, 200), Applied::Changed);

    assert_eq!(
        register(&mut log, 6, registration(0, 19, 300)),
        Applied::Refused
    );
    let run = merged(&[segment(0, 9), segment(10, 19)]);
    assert_eq!(replace(&mut log, 7, run), Applied::Refused);
    assert_eq!(log.expired(), [(0, 19)]);
    assert_eq!(
        log.classify(0, 19),
        crate::long_term_delete::LongTermObject::Dead
    );
    // A run past it is another object.
    let longer = merged(&[segment(0, 9), segment(10, 19), segment(20, 29)]);
    assert_eq!(replace(&mut log, 8, longer), Applied::Changed);
    assert_eq!(log.expired(), [], "the segments make it dead now");
}

/// A primary stored the next segment and lost primacy, and the next primary
/// stored a shorter one and published it, then one that ends where the
/// first did. The first object starts and ends where segments do, so the
/// segments alone never make it dead: its registration does, once expired.
#[test]
fn given_the_next_segment_of_a_deposed_primary_when_another_ends_where_it_did_should_expire_it_once_the_floor_is_past_its_start()
 {
    let mut log = log_of(&[(0, 9)]);
    assert_eq!(
        register(&mut log, 2, registration(10, 29, 100)),
        Applied::Changed
    );
    assert_eq!(expire(&mut log, 3, 1_000), Applied::Changed);
    assert_eq!(log.expired(), [], "it is still the next segment");

    assert_eq!(publish(&mut log, 4, segment(10, 19)), Applied::Changed);
    assert_eq!(publish(&mut log, 5, segment(20, 29)), Applied::Changed);
    assert_eq!(
        log.classify(10, 29),
        crate::long_term_delete::LongTermObject::Registered
    );
    assert_eq!(expire(&mut log, 6, 1_000), Applied::Changed);

    assert_eq!(log.expired(), [(10, 29)]);
    assert_eq!(
        log.classify(10, 29),
        crate::long_term_delete::LongTermObject::Dead
    );
}

#[test]
fn given_a_trim_past_where_registrations_start_when_it_applies_should_drop_them() {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29), (30, 39)]);
    assert_eq!(
        register(&mut log, 5, registration(0, 19, 100)),
        Applied::Changed
    );
    assert_eq!(
        register(&mut log, 6, registration(10, 29, 100)),
        Applied::Changed
    );
    assert_eq!(
        register(&mut log, 7, registration(20, 39, 300)),
        Applied::Changed
    );
    assert_eq!(expire(&mut log, 8, 200), Applied::Changed);

    assert_eq!(trim(&mut log, 9, 19), Applied::Changed);

    assert_eq!(log.expired(), []);
    assert_eq!(keys(&log), [(20, 39)]);
}

/// The registrations and the expired objects together never pass the limit,
/// so the snapshot stays bounded. An expiry needs no room of its own, and a
/// registration the log holds is still renewed.
#[test]
fn given_a_log_that_holds_the_most_registrations_when_registering_another_should_refuse_it() {
    let most = LONG_TERM_REGISTRATIONS_MAX as u64;
    let mut log = log_of_single_offsets(most + 2);
    let mut op = most + 3;
    for start_offset in 0..most {
        op += 1;
        assert_eq!(
            register(
                &mut log,
                op,
                registration(start_offset, start_offset + 1, 100)
            ),
            Applied::Changed
        );
    }
    assert!(log.registrations_full());

    assert_eq!(
        register(&mut log, op + 1, registration(most, most + 1, 100)),
        Applied::Refused
    );
    assert_eq!(
        register(&mut log, op + 2, registration(most + 2, most + 2, 100)),
        Applied::Refused,
        "the next segment too"
    );
    assert_eq!(
        register(&mut log, op + 3, registration(0, 1, 500)),
        Applied::Changed
    );
    assert_eq!(expire(&mut log, op + 4, 200), Applied::Changed);
    assert_eq!(log.expired().len(), LONG_TERM_REGISTRATIONS_MAX - 1);
    assert!(log.registrations_full());

    let mut snapshot = Vec::new();
    log.snapshot_into(&mut snapshot);
    let mut restored = LongTermLog::default();
    restored.restore(&snapshot).unwrap();
    assert_eq!(restored, log);
}

#[test]
fn given_a_log_with_registered_and_expired_objects_when_snapshotting_and_restoring_should_give_it_back()
 {
    let mut log = log_of(&[(0, 9), (10, 19), (20, 29)]);
    for (op, registered) in (4..).zip([
        registration(0, 19, 100),
        registration(10, 29, 300),
        registration(30, 39, 50),
    ]) {
        assert_eq!(register(&mut log, op, registered), Applied::Changed);
    }
    assert_eq!(expire(&mut log, 7, 200), Applied::Changed);
    let mut snapshot = Vec::new();
    log.snapshot_into(&mut snapshot);
    assert_eq!(
        snapshot.len(),
        3 * COUNT + 3 * SEGMENT_PAYLOAD_SIZE + 2 * REGISTRATION_PAYLOAD_SIZE + 2 * size_of::<u64>()
    );

    let mut restored = LongTermLog::default();
    restored.restore(&snapshot).unwrap();

    assert_eq!(restored, log);
    for log in [&mut log, &mut restored] {
        assert_eq!(publish(log, 8, segment(30, 39)), Applied::Changed);
        assert_eq!(expire(log, 9, 400), Applied::Changed);
    }
    assert_eq!(restored, log);
    assert_eq!(restored.expired(), [(0, 19), (10, 29)]);
    assert_eq!(restored.registrations(), []);
}

#[test]
fn given_a_snapshot_whose_registrations_no_log_holds_when_restoring_should_refuse_it() {
    let segments = [segment(0, 9), segment(10, 19), segment(20, 29)];
    let held = log_of(&[(0, 9)]);
    let mut over: Vec<ObjectRegistration> = (0..=LONG_TERM_REGISTRATIONS_MAX as u64)
        .map(|offset| registration(30, 30 + offset, 1))
        .collect();
    over.truncate(LONG_TERM_REGISTRATIONS_MAX);
    let refused = [
        (
            raw_snapshot(
                &segments,
                &[registration(10, 29, 1), registration(0, 19, 1)],
                &[],
            ),
            "registrations out of order",
        ),
        (
            raw_snapshot(&segments, &[registration(5, 19, 1)], &[]),
            "a registration the segments make dead",
        ),
        (
            raw_snapshot(&segments, &[registration(31, 40, 1)], &[]),
            "a registration past the next segment",
        ),
        (
            raw_snapshot(&segments, &[registration(10, 19, 1)], &[]),
            "a registration of a listed segment",
        ),
        (
            raw_snapshot(&segments, &[], &[(30, 39)]),
            "an expired next segment",
        ),
        (
            raw_snapshot(&segments, &[registration(0, 19, 1)], &[(0, 19)]),
            "an object both registered and expired",
        ),
        (
            raw_snapshot(&segments, &[], &[(10, 29), (0, 19)]),
            "expired objects out of order",
        ),
        (
            raw_snapshot(&segments, &over, &[(0, 19)]),
            "more registrations and expired objects than the log holds",
        ),
    ];

    for (snapshot, what) in refused {
        let mut restored = held.clone();
        assert!(restored.restore(&snapshot).is_err(), "{what}");
        assert_eq!(restored, held, "{what}");
    }
}

/// The snapshot of a log of segments alone carried no counts. A build that
/// reads counts refuses it rather than take its bytes for others.
#[test]
fn given_a_snapshot_of_segments_alone_when_restoring_should_refuse_it() {
    for segments in [&[segment(0, 9)][..], &[segment(0, 9), segment(10, 19)]] {
        let uncounted: Vec<u8> = segments
            .iter()
            .flat_map(|segment| PublishSegment(*segment).encode())
            .collect();
        let mut log = LongTermLog::default();

        assert!(log.restore(&uncounted).is_err());
        assert_eq!(log, LongTermLog::default());
    }
}

#[test]
fn given_a_payload_of_another_size_when_registering_or_expiring_should_refuse_it() {
    let mut log = log_of(&[(0, 9), (10, 19)]);
    let whole = RegisterObject(registration(0, 19, 1)).encode();
    let time = ExpireRegistrations { before: 1 }.encode();

    for (kind, payload) in [
        (REGISTER_OBJECT, &whole[..REGISTRATION_PAYLOAD_SIZE - 1]),
        (REGISTER_OBJECT, &[0; REGISTRATION_PAYLOAD_SIZE + 1][..]),
        (EXPIRE_REGISTRATIONS, &time[..EXPIRE_PAYLOAD_SIZE - 1]),
        (EXPIRE_REGISTRATIONS, &[]),
    ] {
        assert_eq!(
            log.apply(3, MachineCommand { kind, payload }),
            Applied::Refused
        );
    }
    assert_eq!(log.registrations(), []);
}
