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

use std::ops::Range;

use iggy_object_store::{
    DeleteOutcome, InlineTierExecutor, ListPage, ListRequest, ManifestSegment, MemoryStore,
    ObjectMeta, ObjectRange, PutBody, PutOptions, SEGMENT_HASH_LEN, TierScope, UserMetadata,
};

use super::*;

const PREFIX: ObjectPrefix = ObjectPrefix {
    stream_id: 1,
    topic_id: 2,
    partition_id: 0,
    created_revision: 7,
};
const INDEX_BYTES: u64 = 2 * IGGY_INDEX_SIZE as u64;

fn metadata(start: u64, end: u64) -> SegmentMetadata {
    SegmentMetadata {
        start_offset: start,
        end_offset: end,
        start_timestamp: 1_000 + start,
        end_timestamp: 1_000 + end,
        max_timestamp: 1_000 + end,
        size: (end - start + 1) * 100,
        hash: [u8::try_from(start % 251).unwrap(); SEGMENT_HASH_LEN],
    }
}

fn crc(start: u64) -> u32 {
    u32::try_from(start).unwrap() ^ 0x00ab_cdef
}

/// The view of a store that holds the segments `objects` whole, as their
/// uploads left them, asked to walk from `from` to `front`.
fn view(objects: &[(u64, u64)], from: u64, front: u64) -> StoreView {
    let mut listed = BTreeMap::new();
    let mut logs = BTreeMap::new();
    for &(start, end) in objects {
        let metadata = metadata(start, end);
        listed.insert(
            start,
            ListedSegment {
                log_size: Some(metadata.size),
                index_size: Some(INDEX_BYTES),
            },
        );
        logs.insert(
            start,
            StoredLog::Found {
                size: metadata.size,
                crc32c: Some(crc(start)),
                metadata: Ok(metadata),
            },
        );
    }
    StoreView {
        request: StoreViewRequest {
            prefix: PREFIX,
            from,
            front,
            local_sealed: Vec::new(),
        },
        listed,
        logs,
        manifest: ManifestRead::Absent,
    }
}

fn manifest(segments: &[(u64, u64)], trim_point: Option<u64>) -> ManifestRead {
    ManifestRead::Read(PartitionManifest {
        prefix: PREFIX,
        published_at: 1,
        floor: segments.last().map(|(_, end)| *end),
        trim_point,
        segments_keyed_by_both_offsets: false,
        segments: segments
            .iter()
            .map(|&(start, end)| {
                let metadata = metadata(start, end);
                ManifestSegment {
                    start_offset: start,
                    end_offset: end,
                    start_timestamp: metadata.start_timestamp,
                    end_timestamp: metadata.end_timestamp,
                    max_timestamp: metadata.max_timestamp,
                    size: metadata.size,
                    crc32c: crc(start),
                    hash: metadata.hash,
                }
            })
            .collect(),
    })
}

fn remote(start: u64, end: u64) -> ChainRecord {
    let metadata = metadata(start, end);
    ChainRecord::Remote(SegmentRecord {
        start_offset: start,
        end_offset: end,
        start_timestamp: metadata.start_timestamp,
        end_timestamp: metadata.end_timestamp,
        max_timestamp: metadata.max_timestamp,
        size: metadata.size,
        incarnation: PREFIX.created_revision,
        hash: metadata.hash,
        crc32c: crc(start),
        source: HashSource::Disk,
    })
}

/// What a partition knows of itself: its first `.log` at `front`, retention
/// through `watermark`, the floor at `floor`, one replica.
struct Local {
    front: u64,
    watermark: u64,
    floor: Option<u64>,
    stems: Vec<u64>,
    anchors: BTreeMap<u64, SegmentAnchor>,
    sealed: Vec<(u64, u64)>,
}

impl Local {
    fn new(front: u64, watermark: u64, floor: Option<u64>) -> Self {
        Self {
            front,
            watermark,
            floor,
            stems: Vec::new(),
            anchors: BTreeMap::new(),
            sealed: Vec::new(),
        }
    }

    fn input(&self) -> RebuildInput<'_> {
        RebuildInput {
            local: LocalFront {
                front: Some(self.front),
                watermark: self.watermark,
                floor: self.floor,
                stems_without_log: &self.stems,
                replicated: false,
            },
            created_revision: PREFIX.created_revision,
            anchors: &self.anchors,
            local_sealed: &self.sealed,
        }
    }

    fn rebuild(&self, view: &StoreView) -> Result<RebuiltFront, ChainLost> {
        rebuild_front(view, &self.input())
    }
}

fn local_front(
    front: Option<u64>,
    watermark: u64,
    floor: Option<u64>,
    stems: &[u64],
    replicated: bool,
) -> ChainLoss {
    chain_loss(&LocalFront {
        front,
        watermark,
        floor,
        stems_without_log: stems,
        replicated,
    })
}

fn lost(from: u64, to: u64, reason: ChainLostReason) -> ChainLost {
    ChainLost { from, to, reason }
}

#[test]
fn given_no_local_log_when_deciding_should_refuse() {
    for (floor, stems, replicated) in [
        (None, &[][..], false),
        (Some(99), &[0, 50][..], false),
        (Some(99), &[][..], true),
    ] {
        assert_eq!(
            local_front(None, 0, floor, stems, replicated),
            ChainLoss::Refuse(lost(0, u64::MAX, ChainLostReason::NoLocalLog))
        );
    }
    assert_eq!(
        local_front(None, 41, Some(99), &[], false),
        ChainLoss::Refuse(lost(42, u64::MAX, ChainLostReason::NoLocalLog))
    );
}

#[test]
fn given_a_front_at_or_below_the_first_kept_offset_when_deciding_should_open_a_fresh_chain() {
    for replicated in [false, true] {
        assert_eq!(
            local_front(Some(0), 0, Some(99), &[], replicated),
            ChainLoss::Fresh
        );
        assert_eq!(
            local_front(Some(42), 41, Some(99), &[10, 20], replicated),
            ChainLoss::Fresh
        );
        assert_eq!(
            local_front(Some(30), 41, Some(99), &[10], replicated),
            ChainLoss::Fresh
        );
    }
}

#[test]
fn given_a_front_above_the_trim_with_no_trace_of_an_eviction_on_one_replica_when_deciding_should_open_a_fresh_chain()
 {
    assert_eq!(local_front(Some(50), 0, None, &[], false), ChainLoss::Fresh);
    // Files of segments retention removed say nothing about an eviction.
    assert_eq!(
        local_front(Some(50), 20, None, &[10, 20], false),
        ChainLoss::Fresh
    );
}

#[test]
fn given_a_front_above_the_trim_with_no_trace_of_an_eviction_on_a_replicated_partition_when_deciding_should_refuse()
 {
    assert_eq!(
        local_front(Some(50), 20, None, &[], true),
        ChainLoss::Refuse(lost(21, 49, ChainLostReason::UncommittedTrim))
    );
}

#[test]
fn given_a_front_above_the_trim_and_a_floor_or_a_stem_when_deciding_should_rebuild() {
    for replicated in [false, true] {
        assert_eq!(
            local_front(Some(50), 0, Some(49), &[], replicated),
            ChainLoss::Rebuild
        );
        assert_eq!(
            local_front(Some(50), 0, None, &[10], replicated),
            ChainLoss::Rebuild
        );
        assert_eq!(
            local_front(Some(50), 20, None, &[21], replicated),
            ChainLoss::Rebuild
        );
    }
}

#[test]
fn given_a_store_that_holds_the_whole_front_when_rebuilding_should_chain_it_from_the_trim() {
    let view = view(&[(10, 19), (20, 29), (30, 39), (40, 49)], 20, 50);
    let local = Local::new(50, 19, Some(49));

    let rebuilt = local.rebuild(&view).unwrap();

    assert_eq!(
        rebuilt.records,
        vec![
            ChainRecord::TrimPoint(19),
            remote(20, 29),
            remote(30, 39),
            remote(40, 49),
        ]
    );
    assert_eq!(rebuilt.remote_segments, 3);
    assert_eq!(rebuilt.trim_point, Some(19));
}

#[test]
fn given_nothing_trimmed_when_rebuilding_should_chain_from_offset_zero_without_a_trim_point() {
    let view = view(&[(0, 9), (10, 19)], 0, 20);
    let local = Local::new(20, 0, Some(19));

    let rebuilt = local.rebuild(&view).unwrap();

    assert_eq!(rebuilt.records, vec![remote(0, 9), remote(10, 19)]);
    assert_eq!(rebuilt.trim_point, None);
}

#[test]
fn given_a_trim_inside_a_segment_when_rebuilding_should_start_at_the_segment_that_holds_it() {
    let view = view(&[(0, 9), (10, 19), (20, 29)], 15, 30);
    let local = Local::new(30, 14, Some(29));

    let rebuilt = local.rebuild(&view).unwrap();

    assert_eq!(
        rebuilt.records,
        vec![ChainRecord::TrimPoint(14), remote(10, 19), remote(20, 29)]
    );
}

#[test]
fn given_trimmed_objects_still_in_the_store_when_rebuilding_should_start_at_the_first_kept_offset()
{
    // The object at 10 is gone, so a walk from the first listed key would
    // stop there.
    let mut view = view(&[(0, 9), (20, 29), (30, 39)], 20, 40);
    view.manifest = manifest(&[(0, 9), (20, 29), (30, 39)], Some(19));
    let local = Local::new(40, 19, Some(39));

    let rebuilt = local.rebuild(&view).unwrap();

    assert_eq!(
        rebuilt.records,
        vec![ChainRecord::TrimPoint(19), remote(20, 29), remote(30, 39)]
    );
}

#[test]
fn given_a_manifest_and_a_store_with_other_trims_when_rebuilding_should_take_the_trim_from_the_watermark()
 {
    let mut view = view(&[(0, 9), (10, 19), (20, 29), (30, 39)], 20, 40);
    view.manifest = manifest(&[(10, 19), (20, 29), (30, 39)], Some(9));
    let local = Local::new(40, 19, Some(39));

    let rebuilt = local.rebuild(&view).unwrap();

    assert_eq!(rebuilt.trim_point, Some(19));
    assert_eq!(rebuilt.records[0], ChainRecord::TrimPoint(19));
    assert!(
        rebuilt
            .records
            .iter()
            .filter_map(ChainRecord::segment)
            .all(|segment| segment.end_offset > 19),
        "{:?}",
        rebuilt.records
    );
}

#[test]
fn given_no_object_at_the_first_kept_offset_when_rebuilding_should_refuse_up_to_the_next_object() {
    let view = view(&[(20, 29), (30, 39)], 10, 40);
    let local = Local::new(40, 9, Some(39));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(10, 19, ChainLostReason::MissingObject))
    );
}

#[test]
fn given_a_trimmed_object_below_a_missing_first_one_when_rebuilding_should_refuse_the_missing_range()
 {
    let view = view(&[(0, 9), (20, 29)], 10, 30);
    let local = Local::new(30, 9, Some(29));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(10, 19, ChainLostReason::MissingObject))
    );
}

#[test]
fn given_a_missing_middle_object_when_rebuilding_should_refuse_its_range() {
    let view = view(&[(0, 9), (20, 29)], 0, 30);
    let local = Local::new(30, 0, Some(29));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(10, 19, ChainLostReason::MissingObject))
    );
}

#[test]
fn given_a_missing_last_object_when_rebuilding_should_refuse_up_to_the_front() {
    let view = view(&[(0, 9)], 0, 30);
    let local = Local::new(30, 0, Some(29));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(10, 29, ChainLostReason::MissingObject))
    );
}

#[test]
fn given_an_object_that_ends_above_the_floor_when_rebuilding_should_refuse() {
    let view = view(&[(0, 9), (10, 29)], 0, 30);
    let local = Local::new(30, 0, Some(19));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(
            10,
            29,
            ChainLostReason::AboveFloor { floor: Some(19) }
        ))
    );
}

#[test]
fn given_a_front_above_the_floor_when_rebuilding_should_refuse_the_offsets_above_it() {
    let view = view(&[(0, 9), (10, 19), (20, 29)], 0, 30);
    let local = Local::new(30, 0, Some(19));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(
            20,
            29,
            ChainLostReason::AboveFloor { floor: Some(19) }
        ))
    );
}

#[test]
fn given_no_floor_when_rebuilding_should_refuse_the_whole_front() {
    let view = view(&[(0, 9)], 0, 10);
    let local = Local::new(10, 0, None);

    assert_eq!(
        local.rebuild(&view),
        Err(lost(0, 9, ChainLostReason::AboveFloor { floor: None }))
    );
}

#[test]
fn given_metadata_that_names_another_start_when_rebuilding_should_refuse() {
    let mut view = view(&[(0, 9), (10, 19)], 0, 20);
    view.logs.insert(
        10,
        StoredLog::Found {
            size: metadata(10, 19).size,
            crc32c: Some(crc(10)),
            metadata: Ok(SegmentMetadata {
                start_offset: 11,
                ..metadata(10, 19)
            }),
        },
    );
    let local = Local::new(20, 0, Some(19));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(
            10,
            19,
            ChainLostReason::MisplacedObject {
                start: 10,
                recorded: 11
            }
        ))
    );
}

#[test]
fn given_metadata_that_does_not_decode_when_rebuilding_should_refuse() {
    let mut view = view(&[(0, 9), (10, 19)], 0, 20);
    view.logs.insert(
        0,
        StoredLog::Found {
            size: metadata(0, 9).size,
            crc32c: Some(crc(0)),
            metadata: Err(SegmentMetadataError::Missing("end-offset")),
        },
    );
    let local = Local::new(20, 0, Some(19));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(0, 9, ChainLostReason::UnreadableMetadata { start: 0 }))
    );
}

#[test]
fn given_an_object_of_another_size_than_its_metadata_when_rebuilding_should_refuse() {
    let size = metadata(10, 19).size;
    for (listed, headed) in [(size + 1, size + 1), (size, size + 1), (size + 1, size)] {
        let mut view = view(&[(0, 9), (10, 19)], 0, 20);
        view.listed.insert(
            10,
            ListedSegment {
                log_size: Some(listed),
                index_size: Some(INDEX_BYTES),
            },
        );
        view.logs.insert(
            10,
            StoredLog::Found {
                size: headed,
                crc32c: Some(crc(10)),
                metadata: Ok(metadata(10, 19)),
            },
        );
        let local = Local::new(20, 0, Some(19));

        assert_eq!(
            local.rebuild(&view),
            Err(lost(
                10,
                19,
                ChainLostReason::SizeMismatch {
                    start: 10,
                    listed,
                    recorded: size
                }
            )),
            "listed {listed}, head {headed}"
        );
    }
}

#[test]
fn given_an_object_gone_after_the_listing_when_rebuilding_should_refuse_it_as_missing() {
    let mut view = view(&[(0, 9), (10, 19)], 0, 20);
    view.logs.insert(10, StoredLog::Gone);
    let local = Local::new(20, 0, Some(19));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(10, 19, ChainLostReason::MissingObject))
    );
}

#[test]
fn given_a_store_without_checksums_when_the_manifest_lists_the_segment_should_take_its_checksum() {
    let mut view = view(&[(0, 9), (10, 19)], 0, 20);
    for start in [0, 10] {
        if let Some(StoredLog::Found { crc32c, .. }) = view.logs.get_mut(&start) {
            *crc32c = None;
        }
    }
    view.manifest = manifest(&[(0, 9), (10, 19)], None);
    let local = Local::new(20, 0, Some(19));

    let rebuilt = local.rebuild(&view).unwrap();

    assert_eq!(rebuilt.records, vec![remote(0, 9), remote(10, 19)]);
}

#[test]
fn given_a_store_without_checksums_when_the_manifest_does_not_list_the_segment_should_refuse() {
    let mut view = view(&[(0, 9), (10, 19)], 0, 20);
    for start in [0, 10] {
        if let Some(StoredLog::Found { crc32c, .. }) = view.logs.get_mut(&start) {
            *crc32c = None;
        }
    }
    // Published before the second segment was confirmed.
    view.manifest = manifest(&[(0, 9)], None);
    let local = Local::new(20, 0, Some(19));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(10, 19, ChainLostReason::NoChecksum { start: 10 }))
    );
}

#[test]
fn given_a_missing_or_partial_index_when_rebuilding_should_refuse() {
    for (index_size, reason) in [
        (None, ChainLostReason::MissingIndex { start: 10 }),
        (Some(0), ChainLostReason::IndexSize { start: 10, size: 0 }),
        (
            Some(INDEX_BYTES + 1),
            ChainLostReason::IndexSize {
                start: 10,
                size: INDEX_BYTES + 1,
            },
        ),
    ] {
        let mut view = view(&[(0, 9), (10, 19)], 0, 20);
        if let Some(listed) = view.listed.get_mut(&10) {
            listed.index_size = index_size;
        }
        let local = Local::new(20, 0, Some(19));

        assert_eq!(
            local.rebuild(&view),
            Err(lost(10, 19, reason)),
            "{index_size:?}"
        );
    }
}

#[test]
fn given_an_index_without_its_log_when_rebuilding_should_refuse_it_as_missing() {
    let mut view = view(&[(0, 9), (10, 19)], 0, 20);
    if let Some(listed) = view.listed.get_mut(&10) {
        listed.log_size = None;
    }
    view.logs.remove(&10);
    let local = Local::new(20, 0, Some(19));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(10, 19, ChainLostReason::MissingObject))
    );
}

#[test]
fn given_a_manifest_that_records_a_segment_differently_when_rebuilding_should_refuse() {
    let mut disagreeing = view(&[(0, 9), (10, 19)], 0, 20);
    disagreeing.manifest = manifest(&[(0, 9), (10, 19)], None);
    if let ManifestRead::Read(manifest) = &mut disagreeing.manifest {
        manifest.segments[1].hash[0] ^= 1;
    }
    let local = Local::new(20, 0, Some(19));

    assert_eq!(
        local.rebuild(&disagreeing),
        Err(lost(
            10,
            19,
            ChainLostReason::ManifestDisagrees { start: 10 }
        ))
    );

    if let ManifestRead::Read(manifest) = &mut disagreeing.manifest {
        manifest.segments[1].hash[0] ^= 1;
        manifest.segments[0].crc32c ^= 1;
    }
    assert_eq!(
        local.rebuild(&disagreeing),
        Err(lost(0, 9, ChainLostReason::ManifestDisagrees { start: 0 }))
    );
}

#[test]
fn given_a_manifest_that_lists_a_segment_the_walk_skips_when_rebuilding_should_refuse() {
    let mut view = view(&[(0, 9), (20, 29)], 0, 30);
    view.manifest = manifest(&[(0, 9), (10, 19), (20, 29)], None);
    let mut local = Local::new(30, 0, Some(29));
    local.anchors.insert(
        20,
        SegmentAnchor {
            planted_start: 20,
            sealed_start: 0,
            sealed_end: 9,
        },
    );

    assert_eq!(
        local.rebuild(&view),
        Err(lost(10, 19, ChainLostReason::ManifestSkipped { start: 10 }))
    );
}

#[test]
fn given_a_manifest_that_lags_the_store_when_rebuilding_should_take_the_later_segments_from_the_store()
 {
    let mut view = view(&[(0, 9), (10, 19), (20, 29)], 0, 30);
    view.manifest = manifest(&[(0, 9)], None);
    let local = Local::new(30, 0, Some(29));

    let rebuilt = local.rebuild(&view).unwrap();

    assert_eq!(
        rebuilt.records,
        vec![remote(0, 9), remote(10, 19), remote(20, 29)]
    );
}

#[test]
fn given_a_manifest_of_another_incarnation_when_rebuilding_should_refuse() {
    let mut view = view(&[(0, 9)], 0, 10);
    view.manifest = manifest(&[(0, 9)], None);
    if let ManifestRead::Read(manifest) = &mut view.manifest {
        manifest.prefix.created_revision += 1;
    }
    let local = Local::new(10, 0, Some(9));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(0, 9, ChainLostReason::ForeignManifest))
    );
}

#[test]
fn given_a_manifest_that_was_not_read_when_rebuilding_should_rebuild_without_it() {
    for manifest in [
        ManifestRead::TooLarge {
            size: MANIFEST_CROSS_CHECK_BYTES_MAX + 1,
        },
        ManifestRead::Undecodable(ManifestError::Truncated(3)),
    ] {
        let mut view = view(&[(0, 9), (10, 19)], 0, 20);
        view.manifest = manifest;
        let local = Local::new(20, 0, Some(19));

        assert_eq!(
            local.rebuild(&view).unwrap().records,
            vec![remote(0, 9), remote(10, 19)]
        );
    }
}

#[test]
fn given_a_stem_on_disk_that_the_walk_does_not_take_when_rebuilding_should_refuse() {
    let view = view(&[(0, 9), (10, 19)], 0, 20);
    let mut local = Local::new(20, 0, Some(19));
    local.stems = vec![0, 5, 10];

    assert_eq!(
        local.rebuild(&view),
        Err(lost(5, 9, ChainLostReason::StrayFile { start: 5 }))
    );
}

#[test]
fn given_stems_of_walked_and_of_trimmed_segments_when_rebuilding_should_accept_them() {
    let view = view(&[(10, 19), (20, 29)], 10, 30);
    let mut local = Local::new(30, 9, Some(29));
    local.stems = vec![0, 5, 10, 20];

    assert!(local.rebuild(&view).is_ok());
}

#[test]
fn given_a_gap_an_anchor_names_when_rebuilding_should_walk_past_it() {
    let view = view(&[(0, 9), (100, 109)], 0, 110);
    let mut local = Local::new(110, 0, Some(109));
    local.anchors.insert(
        100,
        SegmentAnchor {
            planted_start: 100,
            sealed_start: 0,
            sealed_end: 9,
        },
    );
    local.stems = vec![0, 100];

    let rebuilt = local.rebuild(&view).unwrap();

    assert_eq!(rebuilt.records, vec![remote(0, 9), remote(100, 109)]);
}

#[test]
fn given_a_gap_up_to_the_front_that_an_anchor_names_when_rebuilding_should_end_before_it() {
    let view = view(&[(0, 9)], 0, 100);
    let mut local = Local::new(100, 0, Some(9));
    local.anchors.insert(
        100,
        SegmentAnchor {
            planted_start: 100,
            sealed_start: 0,
            sealed_end: 9,
        },
    );

    assert_eq!(local.rebuild(&view).unwrap().records, vec![remote(0, 9)]);
}

#[test]
fn given_a_gap_without_an_anchor_when_rebuilding_should_refuse_it() {
    let view = view(&[(0, 9), (100, 109)], 0, 110);
    let mut local = Local::new(110, 0, Some(109));
    // An anchor of another predecessor covers nothing here.
    local.anchors.insert(
        100,
        SegmentAnchor {
            planted_start: 100,
            sealed_start: 0,
            sealed_end: 8,
        },
    );

    assert_eq!(
        local.rebuild(&view),
        Err(lost(10, 99, ChainLostReason::MissingObject))
    );
}

#[test]
fn given_an_object_that_reaches_the_local_front_when_rebuilding_should_refuse() {
    let view = view(&[(0, 9), (10, 24)], 0, 20);
    let local = Local::new(20, 0, Some(24));

    assert_eq!(
        local.rebuild(&view),
        Err(lost(
            10,
            19,
            ChainLostReason::OverlapsFront { start: 10, end: 24 }
        ))
    );
}

#[test]
fn given_unconfirmed_objects_above_the_floor_and_between_confirmed_starts_when_rebuilding_should_ignore_them()
 {
    // A deposed primary sealed at 5 and uploaded; nobody confirmed it, nor the
    // upload at 30 above the floor.
    let mut view = view(&[(0, 9), (5, 14), (10, 19), (20, 29), (30, 39)], 0, 30);
    view.manifest = manifest(&[(0, 9), (10, 19), (20, 29)], None);
    let local = Local::new(30, 0, Some(29));

    let rebuilt = local.rebuild(&view).unwrap();

    assert_eq!(
        rebuilt.records,
        vec![remote(0, 9), remote(10, 19), remote(20, 29)]
    );
}

#[test]
fn given_local_sealed_segments_stored_as_they_end_when_rebuilding_should_record_their_advances() {
    let mut view = view(&[(0, 9), (10, 19), (20, 29), (30, 39)], 0, 20);
    // Stored with another end than the local segment.
    view.logs.insert(
        30,
        StoredLog::Found {
            size: metadata(30, 39).size,
            crc32c: Some(crc(30)),
            metadata: Ok(metadata(30, 39)),
        },
    );
    let mut local = Local::new(20, 0, Some(39));
    local.sealed = vec![(20, 29), (30, 38), (40, 49)];

    let rebuilt = local.rebuild(&view).unwrap();

    assert_eq!(
        rebuilt.records,
        vec![
            remote(0, 9),
            remote(10, 19),
            ChainRecord::Advanced(AdvanceTieredFloor {
                start: 20,
                up_to: 29,
                hash: metadata(20, 29).hash,
            }),
        ]
    );
    assert_eq!(rebuilt.confirmed_local, 1);
}

#[test]
fn given_a_rebuilt_chain_when_reading_it_back_should_count_every_remote_segment_as_live() {
    let view = view(&[(10, 19), (20, 29)], 15, 30);
    let local = Local::new(30, 14, Some(29));
    let rebuilt = local.rebuild(&view).unwrap();
    let chain = crate::segment_chain::Chain {
        prefix: PREFIX,
        valid_len: 0,
        records: rebuilt.records,
        torn_tail: false,
    };

    let decoded = crate::segment_chain::decode_chain(&chain.encode()).unwrap();

    assert_eq!(decoded.trim_point(), Some(14));
    assert_eq!(
        decoded
            .remote_segments()
            .iter()
            .map(|segment| (segment.start_offset, segment.end_offset))
            .collect::<Vec<_>>(),
        vec![(10, 19), (20, 29)]
    );
}

#[compio::test]
async fn given_a_store_when_reading_its_view_should_list_head_and_read_the_manifest() {
    let store = MemoryStore::new();
    let layout = KeyLayout::new("iggy").unwrap();
    for (start, end) in [(0, 9), (10, 19), (20, 29), (30, 39)] {
        put_segment(&store, &layout, start, end).await;
    }
    // The store holds zeros, whose checksums the manifest records.
    let ManifestRead::Read(mut stored) = manifest(&[(10, 19), (20, 29)], Some(9)) else {
        unreachable!()
    };
    for segment in &mut stored.segments {
        segment.crc32c = zeros_crc32c(segment.size);
    }
    store
        .put(
            &layout.manifest_key(&PREFIX),
            PutBody::from_bytes(stored.encode()),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    let request = StoreViewRequest {
        prefix: PREFIX,
        from: 12,
        front: 30,
        local_sealed: vec![30],
    };
    let reader = ExecutorStoreViewReader::new(InlineTierExecutor::new(store), layout);
    let scope = TierScope::new();

    let view = reader
        .read(request.clone(), scope.token())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(view.request, request);
    assert_eq!(
        view.listed.keys().copied().collect::<Vec<_>>(),
        vec![0, 10, 20, 30]
    );
    assert_eq!(
        view.logs.keys().copied().collect::<Vec<_>>(),
        vec![10, 20, 30],
        "a HEAD for the segment that holds 12 up to the front, and for each local sealed one"
    );
    assert_eq!(view.manifest, ManifestRead::Read(stored));
    let local = Local::new(30, 11, Some(29));
    let stored_crc32c = |record: ChainRecord| match record {
        ChainRecord::Remote(segment) => ChainRecord::Remote(SegmentRecord {
            crc32c: zeros_crc32c(segment.size),
            ..segment
        }),
        other => other,
    };
    assert_eq!(
        local.rebuild(&view).unwrap().records,
        vec![
            ChainRecord::TrimPoint(11),
            stored_crc32c(remote(10, 19)),
            stored_crc32c(remote(20, 29)),
        ]
    );
}

#[compio::test]
async fn given_a_manifest_above_the_limit_when_reading_the_view_should_not_read_it() {
    let store = MemoryStore::new();
    let layout = KeyLayout::new("iggy").unwrap();
    let size = usize::try_from(MANIFEST_CROSS_CHECK_BYTES_MAX).unwrap() + 1;
    store
        .put(
            &layout.manifest_key(&PREFIX),
            PutBody::from_bytes(vec![0; size]),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    let request = StoreViewRequest {
        prefix: PREFIX,
        from: 0,
        front: 10,
        local_sealed: Vec::new(),
    };

    let view = read_store_view(&store, &layout, request).await.unwrap();

    assert_eq!(
        view.manifest,
        ManifestRead::TooLarge {
            size: MANIFEST_CROSS_CHECK_BYTES_MAX + 1
        }
    );
}

#[compio::test]
async fn given_a_stale_manifest_size_from_a_head_when_reading_the_view_should_read_it_whole() {
    let layout = KeyLayout::new("iggy").unwrap();
    let key = layout.manifest_key(&PREFIX);
    let (ManifestRead::Read(older), ManifestRead::Read(current)) = (
        manifest(&[(0, 9)], None),
        manifest(&[(0, 9), (10, 19)], None),
    ) else {
        unreachable!()
    };
    let store = HeadBeforeReplace {
        store: MemoryStore::new(),
        key: key.clone(),
        size: older.encode().len() as u64,
    };
    store
        .put(
            &key,
            PutBody::from_bytes(current.encode()),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    let request = StoreViewRequest {
        prefix: PREFIX,
        from: 0,
        front: 20,
        local_sealed: Vec::new(),
    };

    let view = read_store_view(&store, &layout, request).await.unwrap();

    assert_eq!(view.manifest, ManifestRead::Read(current));
}

/// A store whose head of `key` reports `size`, as a head that ran before the
/// object was replaced by a longer one.
struct HeadBeforeReplace {
    store: MemoryStore,
    key: String,
    size: u64,
}

impl ObjectStore for HeadBeforeReplace {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        ObjectStore::put(&self.store, key, body, options).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        ObjectStore::get_range(&self.store, key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        let mut meta = ObjectStore::head(&self.store, key).await?;
        if key == self.key {
            meta.size = self.size;
        }
        Ok(meta)
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        ObjectStore::delete(&self.store, keys).await
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        ObjectStore::list(&self.store, request).await
    }
}

fn zeros_crc32c(size: u64) -> u32 {
    crc32c::crc32c(&vec![0; usize::try_from(size).unwrap()])
}

async fn put_segment(store: &MemoryStore, layout: &KeyLayout, start: u64, end: u64) {
    let metadata = metadata(start, end);
    let key = |object| {
        layout.segment_key(&SegmentKey {
            prefix: PREFIX,
            start_offset: start,
            object,
        })
    };
    store
        .put(
            &key(SegmentObject::Log),
            PutBody::from_bytes(vec![0; usize::try_from(metadata.size).unwrap()]),
            &PutOptions {
                metadata: UserMetadata::from(&metadata),
                ..PutOptions::default()
            },
        )
        .await
        .unwrap();
    store
        .put(
            &key(SegmentObject::Index),
            PutBody::from_bytes(vec![0; usize::try_from(INDEX_BYTES).unwrap()]),
            &PutOptions::default(),
        )
        .await
        .unwrap();
}
