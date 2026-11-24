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

use std::time::UNIX_EPOCH;

use iggy_object_store::{
    FreshObjectId, GeoLog, LongTermKey, PositionsKey, SegmentKey, SegmentObject, StoreIdentity,
};

use super::*;

const ORPHAN: ObjectPrefix = ObjectPrefix {
    stream_id: 1,
    topic_id: 2,
    partition_id: 0,
    created_revision: 7,
};
/// The revision committed metadata reached before the listing.
const REVISION: u64 = 10;
const OURS: u128 = 0x0123_4567_89ab_cdef_0011_2233_4455_6677;

fn layout() -> KeyLayout {
    KeyLayout::new("iggy").unwrap()
}

fn at(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

/// The claim of the root, at second 100.
fn claim() -> ClaimStamp {
    ClaimStamp {
        cluster_uid: OURS,
        etag: "claim".to_owned(),
        claimed_at: at(100),
    }
}

fn stamp() -> SweepStamp {
    SweepStamp {
        view: 3,
        claim: claim(),
    }
}

fn listed(key: String, secs: u64) -> ListedObject {
    ListedObject {
        key,
        size: 1,
        etag: String::new(),
        last_modified: at(secs),
    }
}

fn segment(prefix: ObjectPrefix, start_offset: u64, object: SegmentObject) -> String {
    layout().segment_key(&SegmentKey {
        prefix,
        start_offset,
        object,
    })
}

fn long_term(prefix: ObjectPrefix) -> String {
    layout().long_term_key(&LongTermKey {
        prefix,
        start_offset: 0,
        end_offset: 9,
        object: SegmentObject::Log,
    })
}

fn topic_of(prefix: ObjectPrefix) -> TopicKey {
    TopicKey {
        stream_id: prefix.stream_id,
        topic_id: prefix.topic_id,
        created_revision: prefix.created_revision,
    }
}

/// A listing of `objects`, each a key with the second it was stored at.
fn listing(objects: &[(String, u64)]) -> RootListing {
    let mut listing = RootListing::default();
    for (key, secs) in objects {
        listing.take(&layout(), &listed(key.clone(), *secs));
    }
    listing
}

/// One segment and the manifest of `prefix`, stored at `secs`.
fn incarnation(prefix: ObjectPrefix, secs: u64) -> Vec<(String, u64)> {
    vec![
        (segment(prefix, 0, SegmentObject::Log), secs),
        (segment(prefix, 0, SegmentObject::Index), secs),
        (layout().manifest_key(&prefix), secs),
    ]
}

fn held_nothing(_: &ObjectPrefix) -> bool {
    false
}

fn judged(objects: &[(String, u64)], holds: impl Fn(&ObjectPrefix) -> bool) -> Verdict {
    let listing = listing(objects);
    let listed = listing.incarnations[&ORPHAN];
    verdict(
        &ORPHAN,
        &listed,
        &listing.topics,
        REVISION,
        claim().claimed_at,
        holds,
    )
}

#[test]
fn given_keys_of_every_kind_when_classifying_should_name_the_incarnation_of_each_and_nothing_else()
{
    let layout = layout();
    let positions = layout.positions_key(&PositionsKey {
        log: GeoLog {
            origin: "dc-a".to_owned(),
            stream_id: 1,
            topic_id: 2,
            created_revision: 7,
        },
        replica: 0,
    });
    let fresh = layout.fresh_key(FreshObjectId::new(0, 0, 0, 1).unwrap().get());

    for object in SegmentObject::ALL {
        assert_eq!(
            RootKey::of(&layout, &segment(ORPHAN, 0, object)),
            RootKey::Segment(ORPHAN)
        );
    }
    assert_eq!(
        RootKey::of(&layout, &long_term(ORPHAN)),
        RootKey::LongTerm(ORPHAN)
    );
    assert_eq!(
        RootKey::of(&layout, &layout.manifest_key(&ORPHAN)),
        RootKey::Manifest(ORPHAN)
    );
    assert_eq!(
        RootKey::of(&layout, &layout.topic_key(&topic_of(ORPHAN))),
        RootKey::Topic(topic_of(ORPHAN))
    );
    for key in [
        layout.identity_key(),
        fresh,
        positions,
        layout.recovery_key(7),
        layout.recovery_plan_key(7, &ORPHAN),
        "iggy/meta/1/2/0/7/manifest.bin.tmp".to_owned(),
        "iggy/meta/notes.txt".to_owned(),
    ] {
        assert_eq!(RootKey::of(&layout, &key), RootKey::Meta, "{key}");
    }
    for key in [
        "iggy/1/2/0/7/00000000000000000000.log.tmp",
        "iggy/1/2/0/7/0.log",
        "iggy/01/2/0/7/00000000000000000000.log",
        "iggy/b/1/2/0/7/00000000000000000000.log",
        "iggy/README",
    ] {
        assert_eq!(RootKey::of(&layout, key), RootKey::Unrecognised, "{key}");
    }
}

#[test]
fn given_a_listing_when_taking_its_objects_should_sum_each_incarnation_and_count_the_unrecognised()
{
    let other = ObjectPrefix {
        partition_id: 1,
        ..ORPHAN
    };
    let listing = listing(&[
        (segment(ORPHAN, 0, SegmentObject::Log), 300),
        (segment(ORPHAN, 0, SegmentObject::Index), 200),
        (layout().manifest_key(&ORPHAN), 400),
        (long_term(other), 500),
        (layout().topic_key(&topic_of(other)), 600),
        (layout().identity_key(), 50),
        ("iggy/1/2/0/7/scratch".to_owned(), 700),
    ]);

    assert_eq!(
        listing.incarnations[&ORPHAN],
        ListedIncarnation {
            objects: 3,
            manifest: true,
            long_term: false,
            oldest: at(200),
            newest: at(400),
        }
    );
    assert_eq!(
        listing.incarnations[&other],
        ListedIncarnation {
            objects: 1,
            manifest: false,
            long_term: true,
            oldest: at(500),
            newest: at(500),
        }
    );
    assert_eq!(listing.incarnations.len(), 2);
    assert_eq!(listing.topics, BTreeSet::from([topic_of(other)]));
    assert_eq!(listing.unrecognised, 1);
}

#[test]
fn given_a_listing_cut_into_jobs_when_merging_should_equal_the_listing_in_one_part() {
    let objects = [
        (segment(ORPHAN, 0, SegmentObject::Log), 300),
        (segment(ORPHAN, 10, SegmentObject::Log), 900),
        (layout().manifest_key(&ORPHAN), 150),
        (long_term(ORPHAN), 400),
        (layout().topic_key(&topic_of(ORPHAN)), 600),
        ("iggy/junk".to_owned(), 1),
    ];
    let whole = listing(&objects);

    for cut in 0..=objects.len() {
        let mut merged = listing(&objects[..cut]);
        merged.merge(listing(&objects[cut..]));
        assert_eq!(merged, whole, "cut at {cut}");
    }
}

#[test]
fn given_an_incarnation_that_committed_metadata_holds_when_judging_should_keep_it_live() {
    let holds = |prefix: &ObjectPrefix| *prefix == ORPHAN;

    assert_eq!(judged(&incarnation(ORPHAN, 200), holds), Verdict::Live);
    assert_eq!(
        judged(&incarnation(ORPHAN, 200), held_nothing),
        Verdict::Orphan
    );
}

#[test]
fn given_an_incarnation_above_the_revision_read_before_the_listing_when_judging_should_keep_it() {
    let at_revision = ObjectPrefix {
        created_revision: REVISION,
        ..ORPHAN
    };
    let above = ObjectPrefix {
        created_revision: REVISION + 1,
        ..ORPHAN
    };
    let listing = listing(&[incarnation(at_revision, 200), incarnation(above, 200)].concat());
    let judge = |prefix: &ObjectPrefix| {
        verdict(
            prefix,
            &listing.incarnations[prefix],
            &listing.topics,
            REVISION,
            claim().claimed_at,
            held_nothing,
        )
    };

    assert_eq!(judge(&at_revision), Verdict::Orphan);
    assert_eq!(judge(&above), Verdict::AfterSnapshot);
}

#[test]
fn given_an_incarnation_with_a_long_term_segment_when_judging_should_leave_it_to_the_geo_sweep() {
    let mut objects = incarnation(ORPHAN, 200);
    objects.push((long_term(ORPHAN), 200));

    assert_eq!(judged(&objects, held_nothing), Verdict::Geo);
    assert_eq!(
        judged(&[(long_term(ORPHAN), 200)], held_nothing),
        Verdict::Geo
    );
}

#[test]
fn given_a_geo_topic_object_of_the_incarnation_when_judging_should_leave_it_to_the_geo_sweep() {
    let mut objects = incarnation(ORPHAN, 200);
    objects.push((layout().topic_key(&topic_of(ORPHAN)), 200));
    let other_revision = TopicKey {
        created_revision: ORPHAN.created_revision + 1,
        ..topic_of(ORPHAN)
    };
    let mut other_topic = incarnation(ORPHAN, 200);
    other_topic.push((layout().topic_key(&other_revision), 200));

    assert_eq!(judged(&objects, held_nothing), Verdict::Geo);
    assert_eq!(judged(&other_topic, held_nothing), Verdict::Orphan);
}

#[test]
fn given_an_object_no_newer_than_the_claim_when_judging_should_leave_the_incarnation_to_the_operator()
 {
    let mut one_before = incarnation(ORPHAN, 200);
    one_before.push((segment(ORPHAN, 10, SegmentObject::Log), 99));

    assert_eq!(judged(&one_before, held_nothing), Verdict::BeforeClaim);
    assert_eq!(
        judged(&incarnation(ORPHAN, 100), held_nothing),
        Verdict::BeforeClaim
    );
    assert_eq!(
        judged(&incarnation(ORPHAN, 101), held_nothing),
        Verdict::Orphan
    );
}

#[test]
fn given_a_listing_when_finding_should_keep_the_orphans_with_their_newest_object_and_count_the_rest()
 {
    let live = ObjectPrefix {
        partition_id: 1,
        ..ORPHAN
    };
    let retired = ObjectPrefix {
        partition_id: 2,
        ..ORPHAN
    };
    let geo = ObjectPrefix {
        partition_id: 3,
        ..ORPHAN
    };
    let mut objects = incarnation(ORPHAN, 200);
    objects.push((segment(ORPHAN, 10, SegmentObject::Log), 250));
    objects.extend(incarnation(live, 200));
    objects.extend(incarnation(retired, 50));
    objects.extend(incarnation(geo, 200));
    objects.push((long_term(geo), 200));

    let findings = SweepFindings::of(&listing(&objects), REVISION, claim().claimed_at, |prefix| {
        *prefix == live
    });

    assert_eq!(findings.orphans, BTreeMap::from([(ORPHAN, at(250))]));
    assert_eq!(findings.before_claim, BTreeMap::from([(retired, 3)]));
    assert_eq!(findings.objects_before_claim(), 3);
}

#[test]
fn given_one_full_sweep_when_settling_should_delete_nothing() {
    let mut candidates = OrphanCandidates::default();
    let orphans = BTreeMap::from([(ORPHAN, at(200))]);

    assert_eq!(
        candidates.settle(stamp(), &orphans, Some(at(1_000))),
        Vec::new()
    );
    assert_eq!(candidates.len(), 1);
}

#[test]
fn given_two_full_sweeps_in_a_row_when_settling_should_delete_the_orphans_both_found_past_their_grace()
 {
    let mut candidates = OrphanCandidates::default();
    let recent = ObjectPrefix {
        partition_id: 1,
        ..ORPHAN
    };
    let only_second = ObjectPrefix {
        partition_id: 2,
        ..ORPHAN
    };
    candidates.settle(
        stamp(),
        &BTreeMap::from([(ORPHAN, at(200)), (recent, at(200))]),
        Some(at(1_000)),
    );

    let due = candidates.settle(
        stamp(),
        &BTreeMap::from([(ORPHAN, at(500)), (recent, at(501)), (only_second, at(200))]),
        Some(at(500)),
    );

    assert_eq!(due, vec![ORPHAN]);
    assert_eq!(candidates.len(), 3);
}

#[test]
fn given_a_sweep_that_did_not_find_an_orphan_when_settling_should_start_its_count_again() {
    let mut candidates = OrphanCandidates::default();
    let found = BTreeMap::from([(ORPHAN, at(200))]);
    candidates.settle(stamp(), &found, Some(at(1_000)));
    candidates.settle(stamp(), &BTreeMap::new(), Some(at(1_000)));

    assert_eq!(
        candidates.settle(stamp(), &found, Some(at(1_000))),
        Vec::new()
    );
    assert_eq!(
        candidates.settle(stamp(), &found, Some(at(1_000))),
        vec![ORPHAN]
    );
}

#[test]
fn given_sweeps_under_another_view_or_claim_when_settling_should_start_the_count_again() {
    let found = BTreeMap::from([(ORPHAN, at(200))]);
    let other_view = SweepStamp {
        view: stamp().view + 1,
        ..stamp()
    };
    let other_etag = SweepStamp {
        claim: ClaimStamp {
            etag: "replaced".to_owned(),
            ..claim()
        },
        ..stamp()
    };
    let other_cluster = SweepStamp {
        claim: ClaimStamp {
            cluster_uid: OURS + 1,
            ..claim()
        },
        ..stamp()
    };
    let later_claim = SweepStamp {
        claim: ClaimStamp {
            claimed_at: at(150),
            ..claim()
        },
        ..stamp()
    };

    for second in [other_view, other_etag, other_cluster, later_claim] {
        let mut candidates = OrphanCandidates::default();
        candidates.settle(stamp(), &found, Some(at(1_000)));

        assert_eq!(
            candidates.settle(second.clone(), &found, Some(at(1_000))),
            Vec::new(),
            "{second:?}"
        );
        assert_eq!(
            candidates.settle(second.clone(), &found, Some(at(1_000))),
            vec![ORPHAN],
            "{second:?}"
        );
    }
}

#[test]
fn given_cleared_candidates_when_settling_should_need_two_sweeps_again() {
    let mut candidates = OrphanCandidates::default();
    let found = BTreeMap::from([(ORPHAN, at(200))]);
    candidates.settle(stamp(), &found, Some(at(1_000)));

    candidates.clear();

    assert!(candidates.is_empty());
    assert_eq!(
        candidates.settle(stamp(), &found, Some(at(1_000))),
        Vec::new()
    );
}

#[test]
fn given_no_moment_old_enough_when_settling_should_delete_nothing() {
    let mut candidates = OrphanCandidates::default();
    let found = BTreeMap::from([(ORPHAN, at(0))]);
    candidates.settle(stamp(), &found, None);

    assert_eq!(candidates.settle(stamp(), &found, None), Vec::new());
    assert_eq!(quiet_since(at(5), Duration::MAX), None);
    assert_eq!(quiet_since(at(5), Duration::from_secs(5)), Some(at(0)));
}

#[test]
fn given_a_grace_when_timing_the_sweeps_should_sweep_every_hour_or_every_grace_when_shorter() {
    assert_eq!(
        sweep_interval(Duration::from_hours(6)),
        ORPHAN_SWEEP_INTERVAL
    );
    assert_eq!(
        sweep_interval(Duration::from_secs(3)),
        Duration::from_secs(3)
    );
}

fn guard() -> OrphanGuard {
    OrphanGuard {
        claim: claim(),
        quiet_since: at(500),
    }
}

#[test]
fn given_the_identity_object_the_delete_reads_when_checking_the_claim_should_take_only_the_same_object()
 {
    let ours = IdentityRead::Found(StoreIdentity { cluster_uid: OURS });
    let theirs = IdentityRead::Found(StoreIdentity {
        cluster_uid: OURS + 1,
    });

    assert!(guard().same_claim(&ours, Some("claim")));
    assert!(!guard().same_claim(&ours, Some("replaced")));
    assert!(!guard().same_claim(&ours, None));
    assert!(!guard().same_claim(&theirs, Some("claim")));
    assert!(!guard().same_claim(&IdentityRead::Missing, Some("claim")));
    assert!(!guard().same_claim(&IdentityRead::Invalid, Some("claim")));
}

#[test]
fn given_what_the_delete_lists_when_checking_the_guard_should_refuse_anything_the_sweep_did_not_judge()
 {
    let guard = guard();

    assert_eq!(guard.refusal([at(101), at(500)], false, false), None);
    assert_eq!(guard.refusal([], false, false), None);
    assert_eq!(
        guard.refusal([at(200), at(501)], false, false),
        Some(OrphanRefusal::Recent)
    );
    assert_eq!(
        guard.refusal([at(100), at(200)], false, false),
        Some(OrphanRefusal::BeforeClaim)
    );
    assert_eq!(
        guard.refusal([at(200)], true, false),
        Some(OrphanRefusal::LongTerm)
    );
    assert_eq!(
        guard.refusal([at(200)], false, true),
        Some(OrphanRefusal::TopicObject)
    );
}
