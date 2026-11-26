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

use std::sync::OnceLock;

use iggy_object_store::{
    FreshObjectId, GeoLog, LongTermKey, PositionsKey, SegmentKey, SegmentObject,
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
const GRACE: Duration = Duration::from_secs(60);

fn layout() -> KeyLayout {
    KeyLayout::new("iggy").unwrap()
}

/// A moment the store gave.
fn at(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

/// A moment of this node's monotonic clock, `secs` after the first one a
/// test took.
fn node(secs: u64) -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now) + Duration::from_secs(secs)
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
        listing.take(&layout(), &listed(key.clone(), *secs), claim().claimed_at);
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
    verdict(&ORPHAN, &listed, &listing.topics, REVISION, holds)
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
            before_claim: 0,
            long_term: false,
            newest: at(400),
        }
    );
    assert_eq!(
        listing.incarnations[&other],
        ListedIncarnation {
            objects: 1,
            before_claim: 0,
            long_term: true,
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
        (segment(ORPHAN, 20, SegmentObject::Log), 50),
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

    let findings = SweepFindings::of(&listing(&objects), REVISION, |prefix| *prefix == live);

    assert_eq!(
        findings.orphans.keys().copied().collect::<Vec<_>>(),
        [ORPHAN]
    );
    assert_eq!(findings.orphans[&ORPHAN].newest, at(250));
    assert_eq!(findings.orphans[&ORPHAN].objects, 4);
    assert_eq!(findings.before_claim, BTreeMap::from([(retired, 3)]));
    assert_eq!(findings.objects_before_claim(), 3);
}

#[test]
fn given_an_incarnation_with_objects_on_both_sides_of_the_claim_when_finding_should_count_only_those_no_newer_than_it()
 {
    let mut objects = incarnation(ORPHAN, 200);
    objects.push((segment(ORPHAN, 10, SegmentObject::Log), 99));
    objects.push((segment(ORPHAN, 10, SegmentObject::Index), 100));
    let listing = listing(&objects);

    let findings = SweepFindings::of(&listing, REVISION, held_nothing);

    assert_eq!(listing.incarnations[&ORPHAN].objects, 5);
    assert_eq!(listing.incarnations[&ORPHAN].before_claim, 2);
    assert_eq!(findings.before_claim, BTreeMap::from([(ORPHAN, 2)]));
    assert_eq!(findings.objects_before_claim(), 2);
}

/// What a listing found of an orphan: its newest moment and its objects.
fn found(newest: u64, objects: usize) -> BTreeMap<ObjectPrefix, ListedIncarnation> {
    BTreeMap::from([(
        ORPHAN,
        ListedIncarnation {
            objects,
            before_claim: 0,
            long_term: false,
            newest: at(newest),
        },
    )])
}

/// Settle sweeps of `stamp` that found `orphans`, one starting at each of
/// `starts`, and return what the last one found due.
fn settled(
    candidates: &mut OrphanCandidates,
    stamp: &SweepStamp,
    orphans: &BTreeMap<ObjectPrefix, ListedIncarnation>,
    starts: &[Instant],
) -> Vec<(ObjectPrefix, SystemTime)> {
    let mut due = Vec::new();
    for started in starts {
        due = candidates.settle(stamp.clone(), orphans, *started, GRACE);
    }
    due
}

#[test]
fn given_one_full_sweep_when_settling_should_delete_nothing() {
    let mut candidates = OrphanCandidates::default();

    assert_eq!(
        candidates.settle(stamp(), &found(200, 3), node(0), GRACE),
        Vec::new()
    );
    assert_eq!(candidates.len(), 1);
}

#[test]
fn given_sweeps_that_list_an_orphan_the_same_when_settling_should_count_the_grace_from_the_start_of_the_second()
 {
    let mut candidates = OrphanCandidates::default();
    let orphan = found(200, 3);
    let just_short = node(10 + 60).checked_sub(Duration::from_micros(1)).unwrap();

    assert_eq!(
        settled(
            &mut candidates,
            &stamp(),
            &orphan,
            &[node(0), node(10), just_short]
        ),
        Vec::new()
    );
    assert_eq!(
        candidates.settle(stamp(), &orphan, node(10 + 60), GRACE),
        vec![(ORPHAN, at(200))]
    );
}

#[test]
fn given_a_node_clock_ahead_of_the_store_when_settling_should_still_wait_the_grace_from_the_second_sweep()
 {
    let mut candidates = OrphanCandidates::default();
    // This node's clock reads far past the moment the store gave the
    // newest object.
    let orphan = found(200, 3);
    let ahead = 1_000_000;

    assert_eq!(
        settled(
            &mut candidates,
            &stamp(),
            &orphan,
            &[node(ahead), node(ahead + 60)]
        ),
        Vec::new()
    );
    assert_eq!(
        candidates.settle(stamp(), &orphan, node(ahead + 120), GRACE),
        vec![(ORPHAN, at(200))]
    );
}

#[test]
fn given_an_orphan_that_gains_an_object_between_sweeps_when_settling_should_wait_the_full_grace_again()
 {
    for (case, changed) in [
        ("a newer object", found(250, 4)),
        ("an object in the same second", found(200, 4)),
    ] {
        let mut candidates = OrphanCandidates::default();
        settled(
            &mut candidates,
            &stamp(),
            &found(200, 3),
            &[node(0), node(60)],
        );

        assert_eq!(
            settled(
                &mut candidates,
                &stamp(),
                &changed,
                &[node(120), node(180), node(239)]
            ),
            Vec::new(),
            "{case}"
        );
        assert_eq!(
            candidates.settle(stamp(), &changed, node(240), GRACE),
            vec![(ORPHAN, changed[&ORPHAN].newest)],
            "{case}"
        );
    }
}

#[test]
fn given_a_sweep_that_did_not_find_an_orphan_when_settling_should_start_its_count_again() {
    let mut candidates = OrphanCandidates::default();
    let orphan = found(200, 3);
    settled(&mut candidates, &stamp(), &orphan, &[node(0), node(60)]);
    candidates.settle(stamp(), &BTreeMap::new(), node(120), GRACE);

    assert_eq!(
        settled(
            &mut candidates,
            &stamp(),
            &orphan,
            &[node(180), node(240), node(299)]
        ),
        Vec::new()
    );
    assert_eq!(
        candidates.settle(stamp(), &orphan, node(300), GRACE),
        vec![(ORPHAN, at(200))]
    );
}

#[test]
fn given_sweeps_under_another_view_or_claim_when_settling_should_start_the_count_again() {
    let orphan = found(200, 3);
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
        settled(&mut candidates, &stamp(), &orphan, &[node(0), node(60)]);

        assert_eq!(
            settled(
                &mut candidates,
                &second,
                &orphan,
                &[node(120), node(180), node(239)]
            ),
            Vec::new(),
            "{second:?}"
        );
        assert_eq!(
            candidates.settle(second.clone(), &orphan, node(240), GRACE),
            vec![(ORPHAN, at(200))],
            "{second:?}"
        );
    }
}

#[test]
fn given_cleared_candidates_when_settling_should_start_the_count_again() {
    let mut candidates = OrphanCandidates::default();
    let orphan = found(200, 3);
    settled(&mut candidates, &stamp(), &orphan, &[node(0), node(60)]);

    candidates.clear();

    assert!(candidates.is_empty());
    assert_eq!(
        settled(&mut candidates, &stamp(), &orphan, &[node(120), node(180)]),
        Vec::new()
    );
}

#[test]
fn given_sweeps_that_start_before_the_monotonic_clock_reaches_the_grace_when_settling_should_delete_nothing()
 {
    let mut candidates = OrphanCandidates::default();
    let orphan = found(200, 3);

    assert_eq!(
        settled(
            &mut candidates,
            &stamp(),
            &orphan,
            &[node(100), node(160), node(50)]
        ),
        Vec::new()
    );
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
        view: 3,
        claim: claim(),
        newest: at(500),
    }
}

#[test]
fn given_the_claim_the_delete_reads_when_checking_it_should_take_only_the_same_object_at_the_same_moment()
 {
    let replaced = ClaimStamp {
        etag: "replaced".to_owned(),
        ..claim()
    };
    let theirs = ClaimStamp {
        cluster_uid: OURS + 1,
        ..claim()
    };
    let same_etag_later = ClaimStamp {
        claimed_at: at(100) + Duration::from_millis(1),
        ..claim()
    };

    assert!(guard().same_claim(Some(&claim())));
    for read in [&replaced, &theirs, &same_etag_later] {
        assert!(!guard().same_claim(Some(read)), "{read:?}");
    }
    assert!(!guard().same_claim(None));
}

#[test]
fn given_objects_from_the_second_of_the_claim_with_a_finer_moment_when_judging_should_count_them_before_the_claim()
 {
    let within = at(100) + Duration::from_millis(700);
    let next_second = at(101);

    assert!(no_newer_than_claim(within, at(100)));
    assert!(no_newer_than_claim(at(100), within));
    assert!(!no_newer_than_claim(next_second, within));

    let mut objects = incarnation(ORPHAN, 200);
    objects.push((segment(ORPHAN, 10, SegmentObject::Log), 0));
    let mut listing = listing(&objects);
    listing.take(
        &layout(),
        &ListedObject {
            last_modified: within,
            ..listed(segment(ORPHAN, 20, SegmentObject::Log), 0)
        },
        claim().claimed_at,
    );
    assert_eq!(listing.incarnations[&ORPHAN].before_claim, 2);

    let mut check = OrphanCheck::default();
    check.take(&guard(), within, false);
    assert_eq!(check.refusal(false), Some(OrphanRefusal::BeforeClaim));
    let mut check = OrphanCheck::default();
    check.take(&guard(), next_second, false);
    assert_eq!(check.refusal(false), None);
}

/// The check of objects, each stored at a second and of a long-term
/// segment or not.
fn checked(objects: &[(u64, bool)]) -> OrphanCheck {
    let mut check = OrphanCheck::default();
    for (secs, long_term) in objects {
        check.take(&guard(), at(*secs), *long_term);
    }
    check
}

#[test]
fn given_what_the_delete_lists_when_checking_the_guard_should_refuse_anything_the_sweep_did_not_judge()
 {
    assert_eq!(checked(&[(101, false), (500, false)]).refusal(false), None);
    assert_eq!(checked(&[]).refusal(false), None);
    assert_eq!(
        checked(&[(200, false), (501, false)]).refusal(false),
        Some(OrphanRefusal::Recent)
    );
    assert_eq!(
        checked(&[(100, false), (200, false)]).refusal(false),
        Some(OrphanRefusal::BeforeClaim)
    );
    assert_eq!(
        checked(&[(200, true)]).refusal(false),
        Some(OrphanRefusal::LongTerm)
    );
    assert_eq!(
        checked(&[(200, false)]).refusal(true),
        Some(OrphanRefusal::TopicObject)
    );
}

#[test]
fn given_objects_taken_page_by_page_when_checking_should_keep_the_first_age_refusal_below_the_long_term_and_topic_checks()
 {
    let mut check = checked(&[(200, false), (501, false)]);
    check.take(&guard(), at(100), false);

    assert_eq!(check.age, Some(OrphanRefusal::Recent));
    assert_eq!(check.refusal(false), Some(OrphanRefusal::Recent));
    assert_eq!(check.refusal(true), Some(OrphanRefusal::TopicObject));

    check.take(&guard(), at(300), true);

    assert_eq!(check.refusal(true), Some(OrphanRefusal::LongTerm));
    assert_eq!(check.age, Some(OrphanRefusal::Recent));
}
