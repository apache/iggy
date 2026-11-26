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

use std::time::SystemTime;

use iggy_object_store::{SEGMENT_HASH_LEN, UserMetadata};

use super::*;
use crate::segment_chain::HashSource;

const PREFIX: ObjectPrefix = ObjectPrefix {
    stream_id: 1,
    topic_id: 2,
    partition_id: 0,
    created_revision: 7,
};
const INDEX_BYTES: u64 = 3 * IGGY_INDEX_SIZE as u64;
const VIEW: u32 = 4;

fn record(start: u64) -> SegmentRecord {
    SegmentRecord {
        start_offset: start,
        end_offset: start + 9,
        start_timestamp: 1_000 + start,
        end_timestamp: 1_009 + start,
        max_timestamp: 1_009 + start,
        size: 1_000 + start,
        incarnation: PREFIX.created_revision,
        hash: [u8::try_from(start % 251).unwrap(); SEGMENT_HASH_LEN],
        crc32c: u32::try_from(start).unwrap() ^ 0x00ab_cdef,
        source: HashSource::CommitPath,
    }
}

/// The records of `count` segments of ten offsets each, from offset 0.
fn records(count: u64) -> Vec<SegmentRecord> {
    (0..count).map(|segment| record(segment * 10)).collect()
}

fn meta(size: u64, crc32c: Option<u32>, metadata: UserMetadata) -> ObjectMeta {
    ObjectMeta {
        size,
        etag: "etag".to_owned(),
        last_modified: SystemTime::UNIX_EPOCH,
        metadata,
        crc32c,
    }
}

fn metadata_of(record: &SegmentRecord) -> UserMetadata {
    UserMetadata::from(&SegmentMetadata {
        start_offset: record.start_offset,
        end_offset: record.end_offset,
        start_timestamp: record.start_timestamp,
        end_timestamp: record.end_timestamp,
        max_timestamp: record.max_timestamp,
        size: record.size,
        hash: record.hash,
    })
}

/// What the store answers for a segment it holds as `record` describes it.
fn stored_log(record: &SegmentRecord) -> ObjectHead {
    ObjectHead::Found(meta(record.size, Some(record.crc32c), metadata_of(record)))
}

fn stored_index() -> ObjectHead {
    ObjectHead::Found(meta(INDEX_BYTES, Some(1), UserMetadata::new()))
}

fn held(record: SegmentRecord) -> SegmentHeads {
    SegmentHeads {
        log: stored_log(&record),
        index: stored_index(),
        record,
    }
}

fn log_missing(record: SegmentRecord) -> SegmentHeads {
    SegmentHeads {
        log: ObjectHead::NotFound,
        ..held(record)
    }
}

fn report(answered: Vec<SegmentHeads>) -> SegmentVerifyReport {
    SegmentVerifyReport {
        answered,
        unanswered: None,
    }
}

fn unanswered(
    answered: Vec<SegmentHeads>,
    start_offset: u64,
    error: ObjectStoreError,
) -> SegmentVerifyReport {
    SegmentVerifyReport {
        answered,
        unanswered: Some(Unanswered {
            start_offset,
            error,
        }),
    }
}

fn transient() -> ObjectStoreError {
    ObjectStoreError::Unavailable("503".to_owned())
}

/// What a store that answers every HEAD but those of the segment at
/// `failing`, with a transient error, reports for `segments`.
fn failing_at(segments: &[SegmentRecord], failing: u64) -> SegmentVerifyReport {
    let answered: Vec<SegmentHeads> = segments
        .iter()
        .copied()
        .take_while(|segment| segment.start_offset != failing)
        .map(held)
        .collect();
    if answered.len() == segments.len() {
        report(answered)
    } else {
        unanswered(answered, failing, transient())
    }
}

/// Run the jobs that end with the store not answering for the segment at
/// `failing`, until the pass sets it aside.
fn set_aside(state: &mut SegmentVerifies, stored: &[SegmentRecord], failing: u64) {
    for id in 1..=u64::from(SEGMENT_VERIFY_ATTEMPTS) {
        let (id, segments) = next_job(state, at(0), id, stored).expect("a job");
        assert_eq!(segments[0].start_offset, failing);
        let settled = state
            .finish(id, &Ok(failing_at(&segments, failing)), chain(stored))
            .expect("the awaited job");
        assert!(!settled.postponed);
    }
}

fn at(micros: u64) -> IggyTimestamp {
    IggyTimestamp::from(micros)
}

fn interval_micros() -> u64 {
    u64::try_from(SEGMENT_VERIFY_INTERVAL.as_micros()).unwrap()
}

/// A lookup of the chain that stores every segment of `stored` as recorded.
fn chain(stored: &[SegmentRecord]) -> impl Fn(u64) -> Option<SegmentRecord> + '_ {
    |start_offset| {
        stored
            .iter()
            .find(|segment| segment.start_offset == start_offset)
            .copied()
    }
}

/// The next job the state starts with `stored` in the chain, as `(id,
/// segments)`.
fn next_job(
    state: &mut SegmentVerifies,
    now: IggyTimestamp,
    id: u64,
    stored: &[SegmentRecord],
) -> Option<(u64, Vec<SegmentRecord>)> {
    match state.begin(now, id, PREFIX, |cursor| {
        stored
            .iter()
            .filter(move |segment| segment.start_offset >= cursor)
            .copied()
            .collect::<Vec<_>>()
    }) {
        VerifyBegin::Job { id, job, .. } => {
            assert_eq!(job.prefix, PREFIX);
            Some((id, job.segments))
        }
        VerifyBegin::Idle | VerifyBegin::PassEnded => None,
    }
}

fn starts(segments: &[SegmentRecord]) -> Vec<u64> {
    segments
        .iter()
        .map(|segment| segment.start_offset)
        .collect()
}

/// Run one job over `stored` whose answers `answer` makes, and return what
/// it settled.
fn run_job(
    state: &mut SegmentVerifies,
    id: u64,
    stored: &[SegmentRecord],
    answer: impl Fn(SegmentRecord) -> SegmentHeads,
) -> VerifySettled {
    let (id, segments) = next_job(state, at(0), id, stored).expect("a job");
    let outcome = Ok(report(segments.into_iter().map(answer).collect()));
    state
        .finish(id, &outcome, chain(stored))
        .expect("the awaited job")
}

#[test]
fn given_an_object_as_recorded_when_comparing_should_find_nothing() {
    assert_eq!(finding(&held(record(0))), None);
}

#[test]
fn given_a_log_of_another_size_when_comparing_should_find_it_diverged_in_size() {
    let record = record(0);
    let heads = SegmentHeads {
        log: ObjectHead::Found(meta(
            record.size + 1,
            Some(record.crc32c),
            metadata_of(&record),
        )),
        ..held(record)
    };

    assert_eq!(
        finding(&heads),
        Some(Finding::Diverged(
            SegmentObject::Log,
            Divergence::Size {
                stored: record.size + 1,
                recorded: record.size
            }
        ))
    );
}

#[test]
fn given_a_log_with_another_crc32c_when_comparing_should_find_it_diverged_in_checksum() {
    let record = record(0);
    let heads = SegmentHeads {
        log: ObjectHead::Found(meta(
            record.size,
            Some(record.crc32c ^ 1),
            metadata_of(&record),
        )),
        ..held(record)
    };

    assert_eq!(
        finding(&heads),
        Some(Finding::Diverged(
            SegmentObject::Log,
            Divergence::Crc32c {
                stored: record.crc32c ^ 1,
                recorded: record.crc32c
            }
        ))
    );
}

/// The hash a writer declared is all such a store gives back, so it stands
/// in for the CRC32C.
#[test]
fn given_a_store_that_keeps_no_crc32c_when_comparing_should_compare_the_hash_in_the_metadata() {
    let record = record(0);
    let without_crc32c = |metadata| SegmentHeads {
        log: ObjectHead::Found(meta(record.size, None, metadata)),
        ..held(record)
    };
    let other = SegmentRecord {
        hash: [0xff; SEGMENT_HASH_LEN],
        ..record
    };

    assert_eq!(finding(&without_crc32c(metadata_of(&record))), None);
    assert_eq!(
        finding(&without_crc32c(metadata_of(&other))),
        Some(Finding::Diverged(SegmentObject::Log, Divergence::Hash))
    );
    assert_eq!(
        finding(&without_crc32c(UserMetadata::new())),
        Some(Finding::Diverged(SegmentObject::Log, Divergence::Hash))
    );
}

#[test]
fn given_an_index_that_is_not_whole_entries_when_comparing_should_find_it_diverged() {
    for size in [0, INDEX_BYTES - 1] {
        let heads = SegmentHeads {
            index: ObjectHead::Found(meta(size, Some(1), UserMetadata::new())),
            ..held(record(0))
        };

        assert_eq!(
            finding(&heads),
            Some(Finding::Diverged(
                SegmentObject::Index,
                Divergence::IndexEntries { stored: size }
            )),
            "an index of {size} bytes"
        );
    }
}

#[test]
fn given_a_missing_index_beside_a_diverged_log_when_comparing_should_report_the_missing_index() {
    let record = record(0);
    let heads = SegmentHeads {
        log: ObjectHead::Found(meta(
            record.size + 1,
            Some(record.crc32c),
            metadata_of(&record),
        )),
        index: ObjectHead::NotFound,
        record,
    };

    assert_eq!(
        finding(&heads),
        Some(Finding::Missing(SegmentObject::Index))
    );
    assert_eq!(
        finding(&log_missing(record)),
        Some(Finding::Missing(SegmentObject::Log))
    );
}

#[test]
fn given_more_stored_segments_than_one_job_takes_when_a_pass_runs_should_check_them_in_capped_jobs_that_resume_where_the_last_stopped()
 {
    let stored = records(SEGMENT_VERIFY_BATCH_MAX as u64 * 2 + 3);
    let mut state = SegmentVerifies::new(VIEW);
    let mut checked = Vec::new();

    for id in 1.. {
        let Some((id, segments)) = next_job(&mut state, at(0), id, &stored) else {
            break;
        };
        assert!(segments.len() <= SEGMENT_VERIFY_BATCH_MAX);
        checked.extend(starts(&segments));
        let outcome = Ok(report(segments.into_iter().map(held).collect()));
        state.finish(id, &outcome, chain(&stored)).unwrap();
    }

    assert_eq!(checked, starts(&stored), "each segment once, in order");
}

#[test]
fn given_a_job_that_runs_when_asking_should_start_no_other() {
    let stored = records(3);
    let mut state = SegmentVerifies::new(VIEW);
    next_job(&mut state, at(0), 1, &stored).unwrap();

    assert_eq!(next_job(&mut state, at(0), 2, &stored), None);
}

#[test]
fn given_a_pass_that_ended_when_asking_should_start_the_next_only_an_interval_after_its_start() {
    let stored = records(2);
    let mut state = SegmentVerifies::new(VIEW);
    let started = 5_000;
    let (id, segments) = next_job(&mut state, at(started), 1, &stored).unwrap();
    let outcome = Ok(report(segments.into_iter().map(held).collect()));
    state.finish(id, &outcome, chain(&stored)).unwrap();
    assert!(matches!(
        state.begin(at(started + 1), 2, PREFIX, |_| Vec::new()),
        VerifyBegin::PassEnded
    ));

    assert_eq!(
        next_job(&mut state, at(started + interval_micros() - 1), 2, &stored),
        None
    );
    let (_, segments) = next_job(&mut state, at(started + interval_micros()), 2, &stored).unwrap();
    assert_eq!(starts(&segments), starts(&stored));
}

#[test]
fn given_no_stored_segment_when_the_first_pass_runs_should_end_it_at_once() {
    let mut state = SegmentVerifies::new(VIEW);

    assert!(matches!(
        state.begin(at(0), 1, PREFIX, |_| Vec::new()),
        VerifyBegin::PassEnded
    ));
    assert!(matches!(
        state.begin(at(1), 1, PREFIX, |_| Vec::new()),
        VerifyBegin::Idle
    ));
}

#[test]
fn given_a_transient_error_when_a_job_ends_should_find_nothing_and_check_that_segment_in_the_next_job()
 {
    let stored = records(3);
    let mut state = SegmentVerifies::new(VIEW);
    let (id, _) = next_job(&mut state, at(0), 1, &stored).unwrap();

    let settled = state
        .finish(
            id,
            &Ok(unanswered(
                vec![held(stored[0])],
                stored[1].start_offset,
                transient(),
            )),
            chain(&stored),
        )
        .unwrap();

    assert_eq!(settled.checked, 1);
    assert_eq!(settled.transitions, []);
    assert_eq!((state.missing(), state.diverged()), (0, 0));
    let (_, next) = next_job(&mut state, at(0), 2, &stored).unwrap();
    assert_eq!(starts(&next), starts(&stored[1..]));
}

#[test]
fn given_a_segment_the_store_never_answers_when_a_pass_runs_should_check_the_others_and_count_it_neither_missing_nor_checked()
 {
    let stored = records(3);
    let failing = stored[1].start_offset;
    let mut state = SegmentVerifies::new(VIEW);
    let mut asked = 0;
    let mut checked = 0;
    let mut postponed = 0;

    for id in 1.. {
        let Some((id, segments)) = next_job(&mut state, at(0), id, &stored) else {
            break;
        };
        asked += segments
            .iter()
            .filter(|segment| segment.start_offset == failing)
            .count();
        let settled = state
            .finish(id, &Ok(failing_at(&segments, failing)), chain(&stored))
            .unwrap();
        checked += settled.checked;
        postponed += usize::from(settled.postponed);
    }

    assert_eq!(asked, SEGMENT_VERIFY_ATTEMPTS as usize + 1);
    assert_eq!(checked, 2);
    assert_eq!(postponed, 1);
    assert_eq!((state.missing(), state.diverged()), (0, 0));
    let (_, next_pass) = next_job(&mut state, at(interval_micros()), 99, &stored).unwrap();
    assert_eq!(starts(&next_pass), starts(&stored));
}

#[test]
fn given_a_segment_set_aside_when_the_store_answers_it_after_the_others_should_check_it_in_the_same_pass()
 {
    let stored = records(2);
    let failing = stored[0].start_offset;
    let mut state = SegmentVerifies::new(VIEW);
    set_aside(&mut state, &stored, failing);

    let (id, rest) = next_job(&mut state, at(0), 10, &stored).unwrap();
    assert_eq!(starts(&rest), starts(&stored[1..]));
    state
        .finish(
            id,
            &Ok(report(rest.into_iter().map(held).collect())),
            chain(&stored),
        )
        .unwrap();
    let (id, again) = next_job(&mut state, at(0), 11, &stored).unwrap();
    assert_eq!(starts(&again), [failing]);
    let settled = state
        .finish(
            id,
            &Ok(report(again.into_iter().map(log_missing).collect())),
            chain(&stored),
        )
        .unwrap();

    assert_eq!(settled.checked, 1);
    assert_eq!(
        settled.transitions,
        [Transition::Found {
            record: stored[0],
            finding: Finding::Missing(SegmentObject::Log)
        }]
    );
    assert!(matches!(
        state.begin(at(0), 12, PREFIX, |_| Vec::new()),
        VerifyBegin::PassEnded
    ));
}

#[test]
fn given_a_segment_set_aside_when_retention_trims_it_should_not_ask_for_it_again() {
    let stored = records(2);
    let failing = stored[0].start_offset;
    let mut state = SegmentVerifies::new(VIEW);
    set_aside(&mut state, &stored, failing);

    state.trim(stored[0].end_offset);

    let (id, rest) = next_job(&mut state, at(0), 10, &stored[1..]).unwrap();
    assert_eq!(starts(&rest), starts(&stored[1..]));
    state
        .finish(
            id,
            &Ok(report(rest.into_iter().map(held).collect())),
            chain(&stored[1..]),
        )
        .unwrap();
    assert!(matches!(
        state.begin(at(0), 11, PREFIX, |_| Vec::new()),
        VerifyBegin::PassEnded
    ));
}

#[test]
fn given_a_permanent_error_when_a_job_ends_should_find_nothing_and_leave_that_segment_for_the_next_pass()
 {
    let stored = records(3);
    let mut state = SegmentVerifies::new(VIEW);
    let (id, _) = next_job(&mut state, at(0), 1, &stored).unwrap();

    state
        .finish(
            id,
            &Ok(unanswered(
                Vec::new(),
                stored[0].start_offset,
                ObjectStoreError::Malformed("no header".to_owned()),
            )),
            chain(&stored),
        )
        .unwrap();

    assert_eq!((state.missing(), state.diverged()), (0, 0));
    let (_, next) = next_job(&mut state, at(0), 2, &stored).unwrap();
    assert_eq!(starts(&next), starts(&stored[1..]));
}

#[test]
fn given_a_cancelled_job_when_its_result_arrives_should_check_the_same_segments_in_the_next_job() {
    let stored = records(3);
    let mut state = SegmentVerifies::new(VIEW);
    let (id, first) = next_job(&mut state, at(0), 1, &stored).unwrap();

    state
        .finish(id, &Err(TierJobCancelled), chain(&stored))
        .unwrap();

    let (_, again) = next_job(&mut state, at(0), 2, &stored).unwrap();
    assert_eq!(again, first);
}

#[test]
fn given_the_result_of_another_job_when_it_arrives_should_change_nothing() {
    let stored = records(1);
    let mut state = SegmentVerifies::new(VIEW);
    let (id, _) = next_job(&mut state, at(0), 1, &stored).unwrap();

    assert_eq!(
        state.finish(
            id + 1,
            &Ok(report(vec![log_missing(stored[0])])),
            chain(&stored)
        ),
        None
    );
    assert_eq!(state.missing(), 0);
    assert_eq!(next_job(&mut state, at(0), 2, &stored), None, "still runs");
}

#[test]
fn given_a_missing_object_when_two_passes_find_it_should_report_it_found_once() {
    let stored = records(2);
    let mut state = SegmentVerifies::new(VIEW);
    let first = run_job(&mut state, 1, &stored, |record| {
        if record.start_offset == 0 {
            log_missing(record)
        } else {
            held(record)
        }
    });
    assert!(matches!(
        state.begin(at(1), 2, PREFIX, |_| Vec::new()),
        VerifyBegin::PassEnded
    ));

    let (id, segments) = next_job(&mut state, at(interval_micros()), 2, &stored).unwrap();
    let outcome = Ok(report(
        segments
            .into_iter()
            .map(|record| {
                if record.start_offset == 0 {
                    log_missing(record)
                } else {
                    held(record)
                }
            })
            .collect(),
    ));
    let second = state.finish(id, &outcome, chain(&stored)).unwrap();

    assert_eq!(
        first.transitions,
        [Transition::Found {
            record: stored[0],
            finding: Finding::Missing(SegmentObject::Log)
        }]
    );
    assert_eq!(second.transitions, []);
    assert_eq!((state.missing(), state.diverged()), (1, 0));
}

#[test]
fn given_a_missing_object_when_a_later_pass_finds_it_as_recorded_should_report_it_cleared_once() {
    let stored = records(1);
    let mut state = SegmentVerifies::new(VIEW);
    run_job(&mut state, 1, &stored, log_missing);
    assert_eq!(state.missing(), 1);
    state.begin(at(1), 2, PREFIX, |_| Vec::new());

    let (id, segments) = next_job(&mut state, at(interval_micros()), 2, &stored).unwrap();
    let cleared = state
        .finish(
            id,
            &Ok(report(segments.iter().copied().map(held).collect())),
            chain(&stored),
        )
        .unwrap();
    state.begin(at(interval_micros() + 1), 3, PREFIX, |_| Vec::new());
    let (id, segments) = next_job(&mut state, at(interval_micros() * 2), 3, &stored).unwrap();
    let after = state
        .finish(
            id,
            &Ok(report(segments.into_iter().map(held).collect())),
            chain(&stored),
        )
        .unwrap();

    assert_eq!(
        cleared.transitions,
        [Transition::Cleared {
            record: stored[0],
            finding: Finding::Missing(SegmentObject::Log)
        }]
    );
    assert_eq!(after.transitions, []);
    assert_eq!((state.missing(), state.diverged()), (0, 0));
}

#[test]
fn given_a_missing_object_that_comes_back_with_other_bytes_should_report_the_new_finding() {
    let stored = records(1);
    let mut state = SegmentVerifies::new(VIEW);
    run_job(&mut state, 1, &stored, log_missing);
    state.begin(at(1), 2, PREFIX, |_| Vec::new());

    let (id, segments) = next_job(&mut state, at(interval_micros()), 2, &stored).unwrap();
    let replaced = segments
        .into_iter()
        .map(|record| SegmentHeads {
            log: ObjectHead::Found(meta(
                record.size,
                Some(record.crc32c ^ 1),
                metadata_of(&record),
            )),
            ..held(record)
        })
        .collect();
    let settled = state
        .finish(id, &Ok(report(replaced)), chain(&stored))
        .unwrap();

    assert!(matches!(
        settled.transitions[..],
        [Transition::Found {
            finding: Finding::Diverged(SegmentObject::Log, Divergence::Crc32c { .. }),
            ..
        }]
    ));
    assert_eq!((state.missing(), state.diverged()), (0, 1));
}

#[test]
fn given_a_segment_trimmed_while_its_job_ran_when_the_job_finds_it_missing_should_drop_the_finding()
{
    let stored = records(2);
    let mut state = SegmentVerifies::new(VIEW);
    let (id, segments) = next_job(&mut state, at(0), 1, &stored).unwrap();

    let settled = state
        .finish(
            id,
            &Ok(report(segments.into_iter().map(log_missing).collect())),
            chain(&stored[1..]),
        )
        .unwrap();

    assert_eq!(
        settled.transitions,
        [Transition::Found {
            record: stored[1],
            finding: Finding::Missing(SegmentObject::Log)
        }]
    );
    assert_eq!(settled.checked, 1);
    assert_eq!(state.missing(), 1);
}

#[test]
fn given_a_segment_whose_record_changed_while_its_job_ran_when_the_job_finds_it_diverged_should_drop_the_finding()
 {
    let stored = records(1);
    let mut state = SegmentVerifies::new(VIEW);
    let (id, segments) = next_job(&mut state, at(0), 1, &stored).unwrap();
    let rewritten = [SegmentRecord {
        end_offset: stored[0].end_offset + 1,
        ..stored[0]
    }];

    let settled = state
        .finish(
            id,
            &Ok(report(segments.into_iter().map(log_missing).collect())),
            chain(&rewritten),
        )
        .unwrap();

    assert_eq!(settled, VerifySettled::default());
    assert_eq!(state.missing(), 0);
}

#[test]
fn given_findings_when_retention_trims_their_segments_should_drop_them() {
    let stored = records(3);
    let mut state = SegmentVerifies::new(VIEW);
    run_job(&mut state, 1, &stored, log_missing);
    assert_eq!(state.missing(), 3);

    assert_eq!(state.trim(stored[1].end_offset), 2);

    assert_eq!((state.missing(), state.diverged()), (1, 0));
    assert_eq!(state.trim(stored[1].end_offset), 0);
}

#[test]
fn given_missing_and_diverged_segments_when_counting_should_count_each_kind() {
    let stored = records(3);
    let mut state = SegmentVerifies::new(VIEW);

    run_job(&mut state, 1, &stored, |record| match record.start_offset {
        0 => log_missing(record),
        10 => SegmentHeads {
            index: ObjectHead::Found(meta(1, Some(1), UserMetadata::new())),
            ..held(record)
        },
        _ => held(record),
    });

    assert_eq!((state.missing(), state.diverged()), (1, 1));
}
