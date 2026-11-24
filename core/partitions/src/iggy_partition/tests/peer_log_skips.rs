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

//! A partition that sees only some of its origin's manifests. The origin
//! overwrites the manifest on every change, so between two that a follower
//! fetches it may have published, merged and trimmed any number of times.

use super::peer_log::{accepted, follower, listed, log_shape, plan, polled};
use super::peer_log_merges::{
    delete_segments, held, keyed, keyed_origin, merged, polls_up_to, remote, store_merged, tail,
};
use super::segment_chain::PREFIX;
use super::*;
use crate::long_term::{
    LongTermLog, PUBLISH_SEGMENT, PublishSegment, REPLACE_SEGMENTS, ReplaceSegments, TRIM_SEGMENTS,
    TrimSegments, concatenated,
};
use crate::state_machine::{Applied, CommandKind, MachineCommand, PartitionStateMachine};
use crate::{PeerManifestContradiction, PeerManifestRefusal};
use iggy_common::PollingStrategy;
use iggy_object_store::{
    FsStore, ManifestSegment, MemoryStore, PartitionManifest, SEGMENT_HASH_LEN,
};

/// Everything a manifest decides of a follower: its segments, its offsets,
/// what it counts and what it says it applied. The bytes its polls may see
/// count every segment it ever held, so they tell which manifests it saw.
fn state_of(partition: &IggyPartition<IggyMessageBus>) -> impl PartialEq + std::fmt::Debug {
    let segments: Vec<_> = partition
        .log
        .segments()
        .iter()
        .map(|segment| {
            (
                (segment.start_offset, segment.end_offset, segment.location),
                segment.size.as_bytes_u64(),
                (
                    segment.start_timestamp,
                    segment.end_timestamp,
                    segment.max_timestamp,
                ),
            )
        })
        .collect();
    let peer = partition.peer_log().unwrap();
    let (size_bytes, messages, _) = held(partition);
    (
        segments,
        log_shape(partition),
        (size_bytes, messages),
        partition.stats.segments_count_inconsistent(),
        peer.applied(),
        peer.keyed_by_both_offsets(),
    )
}

#[compio::test]
async fn given_a_follower_that_missed_the_manifest_of_a_publish_when_applying_its_merge_should_return_every_message_once_in_order()
 {
    let mut origin = Box::pin(keyed_origin(11)).await;
    let segments = origin.segments.clone();
    assert_eq!(segments.len(), 5);
    let (directory, mut follower) = follower(Rc::new(FsStore::new(origin.bucket.path())));
    let (_seen_all_directory, mut seen_all) =
        super::peer_log::follower(Rc::new(FsStore::new(origin.bucket.path())));
    let _ = follower
        .apply_peer_manifest(&keyed(None, &segments[..1]))
        .unwrap();
    let _ = seen_all
        .apply_peer_manifest(&keyed(None, &segments[..1]))
        .unwrap();
    let over_replaced = plan(&mut follower, PollingStrategy::offset(0), 10)
        .execute()
        .await;
    assert_eq!(over_replaced.read_error, None);
    let first_index = directory.path().join(format!("{:020}-{:020}.index", 0, 1));
    assert!(first_index.exists());

    // The origin publishes the second segment and merges it with the first.
    // The follower fetches no manifest in between.
    let _ = seen_all
        .apply_peer_manifest(&keyed(None, &segments[..2]))
        .unwrap();
    let front = Box::pin(store_merged(&origin.store, &segments[..2])).await;
    Box::pin(delete_segments(&origin.store, &segments[..2])).await;
    let merging = keyed(None, &[front]);
    let _ = seen_all.apply_peer_manifest(&merging).unwrap();
    let applied = follower.apply_peer_manifest(&merging).unwrap();

    assert_eq!(
        (applied.appended, applied.trimmed, applied.replaced),
        (0, 0, 1)
    );
    assert_eq!(log_shape(&follower).0, [remote(0, 3), tail(4)]);
    assert_eq!(state_of(&follower), state_of(&seen_all));
    assert_eq!(
        accepted(&mut follower, over_replaced),
        Err(IggyError::TransientNotAccepted),
        "its plan named a segment the log no longer holds"
    );
    let expected = Box::pin(polled(&mut origin.partition, PollingStrategy::offset(0), 4)).await;
    assert_eq!(expected.1, 4);
    let polls = Box::pin(polls_up_to(&mut follower, 3)).await;
    assert_eq!(polls[0], expected);
    let mut one_by_one = Vec::new();
    for offset in 0..=3 {
        let (matched, messages, bytes) =
            Box::pin(polled(&mut follower, PollingStrategy::offset(offset), 1)).await;
        assert_eq!((matched, messages), (Some(offset), 1));
        one_by_one.extend(bytes);
    }
    assert_eq!(one_by_one, expected.2);
    Box::pin(applied.remove_retired_indexes()).await;
    assert!(
        !first_index.exists(),
        "the index of a replaced segment is retired"
    );
    assert!(
        directory
            .path()
            .join(format!("{:020}-{:020}.index", 0, 3))
            .exists()
    );

    // The same again: two more segments, and one merge over all four.
    let whole = Box::pin(store_merged(
        &origin.store,
        &[front, segments[2], segments[3]],
    ))
    .await;
    Box::pin(delete_segments(
        &origin.store,
        &[front, segments[2], segments[3]],
    ))
    .await;
    let applied = follower
        .apply_peer_manifest(&keyed(None, &[whole, segments[4]]))
        .unwrap();

    assert_eq!(
        (applied.appended, applied.trimmed, applied.replaced),
        (1, 0, 1)
    );
    assert_eq!(
        log_shape(&follower).0,
        [remote(0, 7), remote(8, 9), tail(10)]
    );
    assert_eq!(
        Box::pin(polled(&mut follower, PollingStrategy::offset(0), 100)).await,
        Box::pin(polled(
            &mut origin.partition,
            PollingStrategy::offset(0),
            10
        ))
        .await
    );
    assert_eq!(follower.stats.messages_count_inconsistent(), 10);
}

#[compio::test]
async fn given_a_merge_past_the_floor_twice_in_a_row_when_applying_should_count_only_what_is_new() {
    let [a, b, c, d] = [listed(0, 9), listed(10, 19), listed(20, 29), listed(30, 39)];
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let _ = follower.apply_peer_manifest(&keyed(None, &[a])).unwrap();
    assert_eq!(held(&follower), (1_000, 10, 1_000));

    let first = follower
        .apply_peer_manifest(&keyed(None, &[merged(&[a, b])]))
        .unwrap();

    assert_eq!((first.appended, first.trimmed, first.replaced), (0, 0, 1));
    assert_eq!(log_shape(&follower).0, [remote(0, 19), tail(20)]);
    assert_eq!(held(&follower), (2_000, 20, 2_000));
    assert_eq!(follower.stats.segments_count_inconsistent(), 2);
    assert_eq!(follower.offsets().commit_offset, 19);

    let second = follower
        .apply_peer_manifest(&keyed(None, &[merged(&[a, b, c, d])]))
        .unwrap();

    assert_eq!(
        (second.appended, second.trimmed, second.replaced),
        (0, 0, 1)
    );
    assert_eq!(log_shape(&follower).0, [remote(0, 39), tail(40)]);
    assert_eq!(held(&follower), (4_000, 40, 4_000));
    assert_eq!(follower.stats.segments_count_inconsistent(), 2);
    assert_eq!(follower.offsets().commit_offset, 39);
}

#[compio::test]
async fn given_a_publish_a_merge_and_a_trim_between_two_manifests_when_applying_the_second_should_end_as_a_follower_that_saw_each()
 {
    let [a, b, c, d, e] = [
        listed(0, 9),
        listed(10, 19),
        listed(20, 29),
        listed(30, 39),
        listed(40, 49),
    ];
    let history = [
        keyed(None, &[a, b]),
        keyed(None, &[a, b, c]),
        keyed(None, &[a, merged(&[b, c])]),
        keyed(None, &[a, merged(&[b, c]), d]),
        keyed(Some(9), &[merged(&[b, c]), d]),
        keyed(Some(9), &[merged(&[b, c]), d, e]),
        keyed(Some(9), &[merged(&[b, c, d]), e]),
    ];
    let (_seen_all_directory, mut seen_all) = follower(Rc::new(MemoryStore::new()));
    for manifest in &history {
        let _ = seen_all.apply_peer_manifest(manifest).unwrap();
    }

    for seen in 0..history.len() - 1 {
        let (_directory, mut skipping) = follower(Rc::new(MemoryStore::new()));
        let _ = skipping.apply_peer_manifest(&history[seen]).unwrap();
        let _ = skipping
            .apply_peer_manifest(&history[history.len() - 1])
            .unwrap();
        assert_eq!(
            state_of(&skipping),
            state_of(&seen_all),
            "after manifest {seen}"
        );
    }

    let (_directory, mut skipping) = follower(Rc::new(MemoryStore::new()));
    let _ = skipping.apply_peer_manifest(&history[0]).unwrap();
    let at_once = skipping
        .apply_peer_manifest(&history[history.len() - 1])
        .unwrap();
    assert_eq!(
        (at_once.appended, at_once.trimmed, at_once.replaced),
        (1, 1, 1)
    );
    assert_eq!(
        log_shape(&skipping).0,
        [remote(10, 39), remote(40, 49), tail(50)]
    );
    assert_eq!(
        held(&skipping),
        (4_000, 40, 5_000),
        "its polls could see the trimmed segment before, and the rest since"
    );
}

/// What `apply_peer_manifest` answers and whether it left the log alone.
fn refused(
    follower: &mut IggyPartition<IggyMessageBus>,
    manifest: &PartitionManifest,
) -> PeerManifestRefusal {
    let before = format!("{:?}", state_of(follower));
    let refusal = follower.apply_peer_manifest(manifest).unwrap_err();
    assert_eq!(format!("{:?}", state_of(follower)), before);
    refusal
}

#[compio::test]
async fn given_a_listed_segment_that_starts_inside_an_applied_one_when_applying_should_refuse_it_as_diverged()
 {
    let [a, b, c, d] = [listed(0, 9), listed(10, 19), listed(20, 29), listed(30, 39)];
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let _ = follower
        .apply_peer_manifest(&keyed(None, &[a, b, c, d]))
        .unwrap();
    // It starts with the first timestamp of the segment it cuts and holds
    // offsets the log never saw, so nothing but its start gives it away.
    let past_the_floor = ManifestSegment {
        start_offset: 15,
        end_offset: 49,
        size: 10_000,
        ..merged(&[b, c, d])
    };
    let cases = [
        (
            "after a trim to the middle of a segment",
            keyed(Some(14), &[past_the_floor]),
            10,
        ),
        (
            "after a trim to the middle, over applied segments alone",
            keyed(
                Some(14),
                &[
                    ManifestSegment {
                        start_offset: 15,
                        ..merged(&[b, c])
                    },
                    d,
                ],
            ),
            10,
        ),
        (
            "without a trim",
            keyed(
                None,
                &[
                    ManifestSegment {
                        start_offset: 5,
                        ..a
                    },
                    b,
                    c,
                    d,
                ],
            ),
            0,
        ),
    ];

    for (case, manifest, start_offset) in cases {
        assert_eq!(
            refused(&mut follower, &manifest),
            PeerManifestRefusal::Diverged {
                start_offset,
                contradiction: PeerManifestContradiction::StartsOffBoundary
            },
            "{case}"
        );
    }
}

#[compio::test]
async fn given_a_listed_segment_that_ends_inside_an_applied_one_when_applying_should_refuse_it_as_diverged()
 {
    let [a, b, c, d] = [listed(0, 9), listed(10, 19), listed(20, 29), listed(30, 39)];
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let _ = follower
        .apply_peer_manifest(&keyed(None, &[a, b, c]))
        .unwrap();
    let cases = [
        (
            "inside the segment it starts with",
            vec![a, listed(10, 14), listed(15, 19), c, d],
            10,
        ),
        (
            "inside a later segment",
            vec![
                ManifestSegment {
                    end_offset: 14,
                    ..merged(&[a, b])
                },
                listed(15, 29),
            ],
            0,
        ),
        (
            "inside the last segment, before one that ends past the floor",
            vec![a, b, listed(20, 24), listed(25, 39)],
            20,
        ),
    ];

    for (case, segments, start_offset) in cases {
        assert_eq!(
            refused(&mut follower, &keyed(None, &segments)),
            PeerManifestRefusal::Diverged {
                start_offset,
                contradiction: PeerManifestContradiction::EndsOffBoundary
            },
            "{case}"
        );
    }
}

#[compio::test]
async fn given_a_listed_segment_past_the_floor_that_is_not_what_its_applied_part_adds_up_to_when_applying_should_refuse_it_as_diverged()
 {
    let [a, b, c, d] = [listed(0, 9), listed(10, 19), listed(20, 29), listed(30, 39)];
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let _ = follower
        .apply_peer_manifest(&keyed(None, &[a, b, c]))
        .unwrap();
    let seen = merged(&[b, c]);
    let past_the_floor = merged(&[b, c, d]);
    let cases = [
        (
            "another first timestamp",
            ManifestSegment {
                start_timestamp: past_the_floor.start_timestamp + 1,
                ..past_the_floor
            },
        ),
        (
            "no byte for the offsets past the floor",
            ManifestSegment {
                size: seen.size,
                ..past_the_floor
            },
        ),
        (
            "fewer bytes than the applied segments hold",
            ManifestSegment {
                size: seen.size - 1,
                ..past_the_floor
            },
        ),
        (
            "a newest timestamp below one of an applied segment",
            ManifestSegment {
                max_timestamp: seen.max_timestamp - 1,
                ..past_the_floor
            },
        ),
    ];

    for (case, entry) in cases {
        assert_eq!(
            refused(&mut follower, &keyed(None, &[a, entry])),
            PeerManifestRefusal::Diverged {
                start_offset: 10,
                contradiction: PeerManifestContradiction::DoesNotAddUp
            },
            "{case}"
        );
    }
    let _ = follower
        .apply_peer_manifest(&keyed(None, &[a, past_the_floor]))
        .unwrap();
}

/// Offsets the histories stay below.
const HISTORY_OFFSETS: u64 = 400;
const HISTORY_STEPS: usize = 60;

/// The long-term log of an origin under random commands, which is what its
/// manifests list. Xorshift, so the run needs no new dependency.
struct OriginHistory {
    random: u64,
    log: LongTermLog,
    op: u64,
}

impl OriginHistory {
    fn new(seed: u64) -> Self {
        Self {
            random: seed,
            log: LongTermLog::default(),
            op: 0,
        }
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.random ^= self.random << 13;
        self.random ^= self.random >> 7;
        self.random ^= self.random << 17;
        self.random % bound
    }

    fn apply(&mut self, kind: CommandKind, payload: &[u8]) {
        self.op += 1;
        let applied = self.log.apply(self.op, MachineCommand { kind, payload });
        assert_eq!(applied, Applied::Changed);
    }

    fn publish(&mut self) {
        let start_offset = self.log.floor().map_or(0, |floor| floor + 1);
        let end_offset = start_offset + self.below(4);
        if end_offset >= HISTORY_OFFSETS {
            return;
        }
        let segment = ManifestSegment {
            start_offset,
            end_offset,
            start_timestamp: 1_000 + start_offset,
            end_timestamp: 1_000 + end_offset,
            max_timestamp: 1_000 + end_offset + self.below(8),
            size: 1 + self.below(100),
            crc32c: u32::try_from(self.below(u64::from(u32::MAX))).unwrap(),
            hash: [u8::try_from(self.below(251)).unwrap(); SEGMENT_HASH_LEN],
        };
        self.apply(PUBLISH_SEGMENT, &PublishSegment(segment).encode());
    }

    fn replace(&mut self) {
        let count = u64::try_from(self.log.segments().len()).unwrap();
        if count < 2 {
            return;
        }
        let first = self.below(count - 1);
        let last = first + 1 + self.below((count - first - 1).min(4));
        let hash = [u8::try_from(self.below(251)).unwrap(); SEGMENT_HASH_LEN];
        let sources =
            &self.log.segments()[usize::try_from(first).unwrap()..=usize::try_from(last).unwrap()];
        let merged = concatenated(sources, hash).unwrap();
        self.apply(REPLACE_SEGMENTS, &ReplaceSegments(merged).encode());
    }

    fn trim(&mut self) {
        let up_to_offset = self.below(self.log.floor().map_or(1, |floor| floor + 3));
        self.apply(TRIM_SEGMENTS, &TrimSegments { up_to_offset }.encode());
    }

    fn step(&mut self) {
        match self.below(8) {
            0..4 => self.publish(),
            4..7 => self.replace(),
            _ => self.trim(),
        }
    }

    fn manifest(&self) -> PartitionManifest {
        PartitionManifest {
            prefix: PREFIX,
            published_at: self.op,
            floor: self.log.floor(),
            trim_point: self.log.trim_point(),
            segments_keyed_by_both_offsets: true,
            segments: self.log.segments().to_vec(),
        }
    }
}

/// The origin's history is one line of manifests, each newer than the one
/// before it. Whichever of them a follower fetches, it has to take each one
/// and stand where a follower that fetched them all stands.
#[compio::test]
async fn given_random_histories_of_an_origin_when_a_follower_applies_any_of_its_manifests_in_order_should_refuse_none_and_end_as_one_that_applied_all()
 {
    let mut merges_past_the_floor = 0;
    let mut merges_seen_late = 0;
    for seed in 1..=150 {
        let mut origin = OriginHistory::new(seed);
        let (_seen_all_directory, mut seen_all) = follower(Rc::new(MemoryStore::new()));
        let (_directory, mut skipping) = follower(Rc::new(MemoryStore::new()));
        for step in 0..HISTORY_STEPS {
            origin.step();
            let manifest = origin.manifest();
            let stepped = seen_all
                .apply_peer_manifest(&manifest)
                .unwrap_or_else(|refusal| panic!("seed {seed} step {step}: {refusal}"));
            if origin.below(3) != 0 && step + 1 != HISTORY_STEPS {
                continue;
            }
            let floor_before = skipping
                .peer_log()
                .unwrap()
                .applied()
                .and_then(|at| at.floor);
            let skipped = skipping
                .apply_peer_manifest(&manifest)
                .unwrap_or_else(|refusal| panic!("seed {seed} step {step}: {refusal}"));
            assert_eq!(
                state_of(&skipping),
                state_of(&seen_all),
                "seed {seed} step {step}"
            );
            if skipped.replaced > stepped.replaced {
                merges_seen_late += 1;
                if skipped.appended == 0 && manifest.floor > floor_before {
                    merges_past_the_floor += 1;
                }
            }
        }
    }
    assert!(
        merges_seen_late > 100 && merges_past_the_floor > 10,
        "the histories merged between the manifests a follower saw: {merges_seen_late}, \
         {merges_past_the_floor} of them past its floor"
    );
}
