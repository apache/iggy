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

//! The fresh tail of a shadow partition: the batches its origin committed
//! above the last manifest the partition applied, as this node found them
//! in the self-describing fresh objects of the origin's store.
//!
//! The tail is what one node knows. It goes through no consensus, every
//! node rebuilds it from the store, and a restart starts it empty. The
//! committed manifest stays the only record of the long-term segments, so a
//! newer one drops the batches of the tail at or below its floor.
//!
//! A reader takes the proof rows alone as the record of what committed:
//!
//! 1. A proof row binds a batch to its offsets. It counts once the batch
//!    rebuilt from it and the rows of the object that holds the blob seals
//!    to the checksum the row carries.
//! 2. The tail serves a batch only when every offset from the floor up to
//!    it is proven. A row past a gap waits, and only a newer manifest fills
//!    the gap: the offsets a writer claimed for its batches are a
//!    prediction and never fill one.
//! 3. A row repeats across objects, and the repeat is the same row. Two
//!    rows that bind one offset to different batches, or bind overlapping
//!    offsets, mean the origin's log diverged. The tail then serves no
//!    fresh batch for good, and the partition keeps serving long-term ones.
//! 4. A batch that no row names never reaches the tail.
//!
//! The objects of a cluster that lost its disks stay in the store after the
//! cluster that recovered it took the root. Their rows count only for the
//! batches that end at or below the cutover the recovery recorded.

use std::collections::{BTreeMap, BTreeSet};

use iggy_binary_protocol::placement::PlacedBatch;
use iggy_object_store::{
    FreshBatchRow, FreshDirectoryRow, FreshIndex, FreshObjectId, FreshProofRow,
};

use crate::proven_batch::{ProvenBatch, ProvenBatchError};

/// The most proof rows a tail holds. Rows past it are dropped, so a
/// partition whose floor stopped far below its newest row waits for the
/// floor rather than for memory.
pub const PEER_FRESH_ROWS_MAX: usize = 65_536;

/// The most objects whose batch rows a tail keeps for proofs that have not
/// come yet. The oldest go first, and a proof that needs one again asks for
/// it.
pub const PEER_FRESH_SOURCES_MAX: usize = 1_024;

/// The most objects a tail asks for at once.
const MISSING_SOURCES_MAX: usize = 16;

/// Which proof rows of an object count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofTrust {
    /// The cluster that owns the origin's root wrote the object.
    Owner,
    /// A cluster that lost its disks wrote it, and the cluster that owns
    /// the root recovered this partition from it: the rows of batches that
    /// end at or below `cutover`.
    Retired { cutover: u64 },
    /// None: the object only lends the rows of its batches.
    None,
}

/// What one fresh object holds of one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerFreshRows {
    pub object_id: FreshObjectId,
    pub row: FreshDirectoryRow,
    /// Each batch with where its blob starts.
    pub batches: Vec<(u64, FreshBatchRow)>,
    pub proofs: Vec<FreshProofRow>,
    pub trust: ProofTrust,
}

impl PeerFreshRows {
    /// The rows of `row` in `index`.
    #[must_use]
    pub fn of(index: &FreshIndex, row: &FreshDirectoryRow, trust: ProofTrust) -> Self {
        Self {
            object_id: index.header().object_id,
            row: *row,
            batches: index
                .batches_of(row)
                .map(|(position, batch)| (position, *batch))
                .collect(),
            proofs: index.proofs_of(row).to_vec(),
            trust,
        }
    }
}

/// What an offer changed in a tail.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerFreshChange {
    /// The tail serves more offsets than before.
    pub grew: bool,
    /// The offset at which two rows disagreed, when this offer found it.
    pub conflict_at: Option<u64>,
}

/// How far the tail is behind the newest proof row it found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerFreshLag {
    pub messages: u64,
    /// By the origin's clock, between the newest proven batch found and the
    /// newest one served, in microseconds.
    pub micros: u64,
}

/// The fresh tail of one shadow partition, see the module docs.
#[derive(Debug, Clone)]
pub struct PeerFreshTail {
    /// The origin's partition, raw, as its objects name it.
    namespace: u64,
    created_revision: u64,
    /// The first offset above the applied floor.
    start: u64,
    /// The rows from `start` on, by the offset where each batch starts.
    rows: BTreeMap<u64, TailRow>,
    /// The rows from `start` to here are rebuilt and follow each other.
    next_offset: u64,
    /// The batch rows that proofs may still name, by object.
    sources: BTreeMap<FreshObjectId, Source>,
    /// Objects the store no longer holds, which nobody asks for again.
    gone: BTreeSet<FreshObjectId>,
    conflict_at: Option<u64>,
    /// The end offset and the timestamp of the newest row taken.
    newest: Option<(u64, u64)>,
}

#[derive(Debug, Clone, Copy)]
struct TailRow {
    proof: FreshProofRow,
    end: u64,
    placed: Option<PlacedBatch>,
}

#[derive(Debug, Clone)]
struct Source {
    row: FreshDirectoryRow,
    /// By where each blob starts.
    batches: BTreeMap<u64, FreshBatchRow>,
}

impl PeerFreshTail {
    /// The empty tail of the origin's partition `namespace` of revision
    /// `created_revision`, above `floor`.
    #[must_use]
    pub const fn new(namespace: u64, created_revision: u64, floor: Option<u64>) -> Self {
        let start = start_above(floor);
        Self {
            namespace,
            created_revision,
            start,
            rows: BTreeMap::new(),
            next_offset: start,
            sources: BTreeMap::new(),
            gone: BTreeSet::new(),
            conflict_at: None,
            newest: None,
        }
    }

    /// Take what one object holds of the partition.
    pub fn offer(&mut self, offered: &PeerFreshRows) -> PeerFreshChange {
        if offered.row.namespace != self.namespace
            || offered.row.created_revision != self.created_revision
        {
            return PeerFreshChange::default();
        }
        let before = self.served_end();
        let had_conflict = self.conflict_at.is_some();
        self.keep_source(offered);
        let cutover = match offered.trust {
            ProofTrust::Owner => Some(u64::MAX),
            ProofTrust::Retired { cutover } => Some(cutover),
            ProofTrust::None => None,
        };
        if let Some(cutover) = cutover {
            for proof in &offered.proofs {
                self.take_proof(proof, cutover);
            }
        }
        self.advance();
        PeerFreshChange {
            grew: self.served_end() > before,
            conflict_at: self.conflict_at.filter(|_| !had_conflict),
        }
    }

    /// The applied floor moved to `floor`: drop what it holds and go on
    /// above it.
    pub fn apply_floor(&mut self, floor: Option<u64>) -> PeerFreshChange {
        let start = start_above(floor);
        if start <= self.start {
            return PeerFreshChange::default();
        }
        let before = self.served_end();
        self.start = start;
        // A row that starts below the floor and ends above it cuts a batch
        // the long-term segments hold whole, so it can never be served.
        self.rows = self.rows.split_off(&start);
        self.next_offset = self.next_offset.max(start);
        self.sources.retain(|_, source| {
            source.row.claimed_offsets().end > start || source.row.batch_count == 0
        });
        self.gone.clear();
        self.advance();
        PeerFreshChange {
            grew: self.served_end() > before,
            conflict_at: None,
        }
    }

    /// The store no longer holds `object_id`, so the proofs that need it
    /// wait for a newer manifest.
    pub fn mark_gone(&mut self, object_id: FreshObjectId) {
        if !self.sources.contains_key(&object_id) {
            self.gone.insert(object_id);
        }
    }

    /// The last offset the tail serves, `None` while it serves none.
    #[must_use]
    pub fn served_end(&self) -> Option<u64> {
        (self.conflict_at.is_none() && self.next_offset > self.start).then(|| self.next_offset - 1)
    }

    /// The first offset above the applied floor.
    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }

    /// Where two rows disagreed, which ended what the tail serves.
    #[must_use]
    pub const fn conflict_at(&self) -> Option<u64> {
        self.conflict_at
    }

    /// The objects whose batch rows the rows that wait need, up to a few.
    #[must_use]
    pub fn missing_sources(&self) -> Vec<FreshObjectId> {
        if self.conflict_at.is_some() {
            return Vec::new();
        }
        let mut missing = Vec::new();
        for row in self.rows.range(self.next_offset..).map(|(_, row)| row) {
            let object = row.proof.object_id;
            if row.placed.is_none()
                && !self.sources.contains_key(&object)
                && !self.gone.contains(&object)
                && !missing.contains(&object)
            {
                missing.push(object);
                if missing.len() == MISSING_SOURCES_MAX {
                    break;
                }
            }
        }
        missing
    }

    /// How far the tail is behind the newest row it took.
    #[must_use]
    pub fn lag(&self) -> PeerFreshLag {
        let Some((newest_end, newest_timestamp)) = self.newest else {
            return PeerFreshLag::default();
        };
        let served = if self.conflict_at.is_some() {
            self.start
        } else {
            self.next_offset
        };
        if newest_end <= served {
            return PeerFreshLag::default();
        }
        let behind_since = self
            .rows
            .range(..served)
            .next_back()
            .or_else(|| self.rows.range(served..).next())
            .map_or(newest_timestamp, |(_, row)| row.proof.base_timestamp);
        PeerFreshLag {
            messages: newest_end - served,
            micros: newest_timestamp.saturating_sub(behind_since),
        }
    }

    /// The batches a poll from `offset` reads, at most `count` of them.
    #[must_use]
    pub fn batches_from_offset(&self, offset: u64, count: usize) -> Vec<PlacedBatch> {
        let Some(last) = self.served_end() else {
            return Vec::new();
        };
        let first = self
            .rows
            .range(..=offset)
            .next_back()
            .filter(|(_, row)| row.end > offset)
            .map_or(offset, |(base, _)| *base);
        self.rows
            .range(first..=last)
            .filter_map(|(_, row)| row.placed)
            .take(count)
            .collect()
    }

    /// The batches a poll from `timestamp` reads, at most `count` of them,
    /// from the last one stamped before it, whose later messages may be
    /// stamped after it.
    #[must_use]
    pub fn batches_from_timestamp(&self, timestamp: u64, count: usize) -> Vec<PlacedBatch> {
        let Some(last) = self.served_end() else {
            return Vec::new();
        };
        let served: Vec<PlacedBatch> = self
            .rows
            .range(self.start..=last)
            .filter_map(|(_, row)| row.placed)
            .collect();
        let first = served
            .partition_point(|batch| batch.header.base_timestamp < timestamp)
            .saturating_sub(1);
        served.into_iter().skip(first).take(count).collect()
    }

    /// The newest timestamp a served batch carries, `None` while the tail
    /// serves none.
    #[must_use]
    pub fn served_timestamp(&self) -> Option<u64> {
        let last = self.served_end()?;
        self.rows
            .range(..=last)
            .next_back()
            .map(|(_, row)| row.proof.base_timestamp)
    }

    fn keep_source(&mut self, offered: &PeerFreshRows) {
        if offered.batches.is_empty() || self.sources.contains_key(&offered.object_id) {
            return;
        }
        self.gone.remove(&offered.object_id);
        if self.sources.len() >= PEER_FRESH_SOURCES_MAX {
            self.sources.pop_first();
        }
        self.sources.insert(
            offered.object_id,
            Source {
                row: offered.row,
                batches: offered.batches.iter().copied().collect(),
            },
        );
    }

    /// Take `proof` when its batch ends at or below `cutover`, unless the
    /// rows held already bind its offsets otherwise.
    fn take_proof(&mut self, proof: &FreshProofRow, cutover: u64) {
        let Some(end) = proof.end_offset().filter(|end| *end > proof.base_offset) else {
            self.conflict(proof.base_offset);
            return;
        };
        // A batch that starts below the floor is the long-term segments'
        // to serve, also when the floor sits inside it.
        let below_floor = proof.base_offset < self.start;
        if below_floor || end > cutover {
            return;
        }
        if let Some((base, held)) = self.rows.range(..=proof.base_offset).next_back() {
            if *base == proof.base_offset {
                if held.proof != *proof {
                    self.conflict(proof.base_offset);
                }
                return;
            }
            if held.end > proof.base_offset {
                self.conflict(proof.base_offset);
                return;
            }
        }
        if let Some((base, _)) = self.rows.range(proof.base_offset + 1..).next()
            && *base < end
        {
            self.conflict(*base);
            return;
        }
        if self.rows.len() >= PEER_FRESH_ROWS_MAX {
            return;
        }
        self.rows.insert(
            proof.base_offset,
            TailRow {
                proof: *proof,
                end,
                placed: None,
            },
        );
        if self.newest.is_none_or(|(newest, _)| end > newest) {
            self.newest = Some((end, proof.base_timestamp));
        }
    }

    /// Rebuild the rows from `next_offset` on while they follow each other
    /// and their sources are held.
    fn advance(&mut self) {
        while self.conflict_at.is_none() {
            let Some(row) = self.rows.get(&self.next_offset).copied() else {
                return;
            };
            if row.placed.is_none() {
                let rebuilt = match self.sources.get(&row.proof.object_id) {
                    Some(source) => source.rebuild(&row.proof),
                    None => return,
                };
                let Ok(placed) = rebuilt else {
                    self.conflict(row.proof.base_offset);
                    return;
                };
                if let Some(held) = self.rows.get_mut(&self.next_offset) {
                    held.placed = Some(placed);
                }
                self.spend_source(&row.proof);
            }
            self.next_offset = row.end;
        }
    }

    /// Forget the batch row a proof used, and the object once none is
    /// left, since a batch commits once.
    fn spend_source(&mut self, proof: &FreshProofRow) {
        if let Some(source) = self.sources.get_mut(&proof.object_id) {
            source.batches.remove(&proof.position);
            if source.batches.is_empty() {
                self.sources.remove(&proof.object_id);
            }
        }
    }

    fn conflict(&mut self, offset: u64) {
        self.conflict_at = Some(self.conflict_at.map_or(offset, |held| held.min(offset)));
    }
}

impl Source {
    fn rebuild(&self, proof: &FreshProofRow) -> Result<PlacedBatch, ProvenBatchError> {
        let batch = self
            .batches
            .get(&proof.position)
            .ok_or(ProvenBatchError::NoBatch {
                namespace: self.row.namespace,
                position: proof.position,
            })?;
        PlacedBatch::try_from(ProvenBatch {
            proof,
            source: &self.row,
            batch,
        })
    }
}

const fn start_above(floor: Option<u64>) -> u64 {
    match floor {
        Some(floor) => floor.saturating_add(1),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader};
    use iggy_binary_protocol::placement::{self, PLACED_BATCH_SIZE, Placement};
    use server_common::sharding::IggyNamespace;

    use super::*;

    const REVISION: u64 = 4096;
    const STARTED_AT: u64 = 1_700_000_000_000_000;
    /// Where the data of every object starts, past an index of some size.
    const DATA_AT: u64 = 1_000;

    fn namespace() -> u64 {
        IggyNamespace::new(7, 1, 0).inner()
    }

    fn id(sequence: u64) -> FreshObjectId {
        FreshObjectId::new(0, 1, STARTED_AT, sequence).unwrap()
    }

    /// An object that holds sends of the partition, before any commits.
    struct Sealed {
        id: FreshObjectId,
        row: FreshDirectoryRow,
        batches: Vec<(u64, FreshBatchRow)>,
    }

    fn directory_row(batches: u32, messages: u32, claimed_first_offset: u64) -> FreshDirectoryRow {
        FreshDirectoryRow {
            namespace: namespace(),
            created_revision: REVISION,
            view: 1,
            first_batch: 0,
            batch_count: batches,
            first_proof: 0,
            proof_count: 0,
            messages,
            data_position: DATA_AT,
            data_length: 0,
            claimed_first_offset,
            proven_below: 0,
        }
    }

    /// Object `sequence` with one send of each count of `counts`, whose
    /// writer claims they commit from `claimed_first_offset` on.
    fn seal(sequence: u64, counts: &[u32], claimed_first_offset: u64) -> Sealed {
        let mut position = DATA_AT;
        let batches = counts
            .iter()
            .map(|count| {
                let batch = FreshBatchRow {
                    message_count: *count,
                    blob_length: 64 * count,
                    origin_timestamp: STARTED_AT + sequence,
                    blob_checksum: 0xb10b ^ sequence,
                    producer: None,
                };
                let at = position;
                position += u64::from(batch.blob_length);
                (at, batch)
            })
            .collect();
        Sealed {
            id: id(sequence),
            row: directory_row(
                u32::try_from(counts.len()).unwrap(),
                counts.iter().sum(),
                claimed_first_offset,
            ),
            batches,
        }
    }

    impl Sealed {
        fn rows(&self, proofs: &[FreshProofRow], trust: ProofTrust) -> PeerFreshRows {
            PeerFreshRows {
                object_id: self.id,
                row: self.row,
                batches: self.batches.clone(),
                proofs: proofs.to_vec(),
                trust,
            }
        }

        fn offered(&self, proofs: &[FreshProofRow]) -> PeerFreshRows {
            self.rows(proofs, ProofTrust::Owner)
        }

        /// The placed batch the partition commits for send `send` at
        /// `base_offset`.
        fn placed(&self, send: usize, base_offset: u64) -> PlacedBatch {
            let (position, batch) = self.batches[send];
            PlacedBatch::new(
                BatchHeader {
                    partition_id: 0,
                    base_offset,
                    base_timestamp: timestamp(base_offset),
                    origin_timestamp: batch.origin_timestamp,
                    batch_length: BATCH_HEADER_SIZE as u64 + u64::from(batch.blob_length),
                    batch_checksum: 0,
                    message_count: batch.message_count,
                    producer: None,
                },
                Placement {
                    object_id: self.id.get(),
                    position,
                    length: u64::from(batch.blob_length),
                    blob_checksum: batch.blob_checksum,
                    proven_below: self.row.proven_below,
                },
            )
        }

        /// The proof row of send `send`, committed at `base_offset`.
        fn proof(&self, send: usize, base_offset: u64) -> FreshProofRow {
            let placed = self.placed(send, base_offset);
            let mut bytes = [0; PLACED_BATCH_SIZE];
            placed.encode_into(&mut bytes);
            FreshProofRow {
                object_id: self.id,
                position: placed.placement.position,
                base_offset,
                base_timestamp: placed.header.base_timestamp,
                message_count: placed.header.message_count,
                placed_checksum: placement::checksum_of(&bytes),
            }
        }
    }

    fn timestamp(offset: u64) -> u64 {
        STARTED_AT + 1_000 * offset
    }

    /// An object that holds proofs of the partition and none of its sends.
    fn rows_only(sequence: u64, proofs: &[FreshProofRow], trust: ProofTrust) -> PeerFreshRows {
        PeerFreshRows {
            object_id: id(sequence),
            row: directory_row(0, 0, 0),
            batches: Vec::new(),
            proofs: proofs.to_vec(),
            trust,
        }
    }

    fn served(tail: &PeerFreshTail) -> Vec<(u64, u32)> {
        tail.batches_from_offset(0, usize::MAX)
            .iter()
            .map(|batch| (batch.header.base_offset, batch.header.message_count))
            .collect()
    }

    #[test]
    fn given_rows_repeated_across_objects_when_offered_should_serve_each_batch_once_at_its_offsets()
    {
        let sends = seal(1, &[2, 3], 10);
        let proofs = [sends.proof(0, 10), sends.proof(1, 12)];
        let mut tail = PeerFreshTail::new(namespace(), REVISION, Some(9));

        let first = tail.offer(&sends.offered(&[]));
        let second = tail.offer(&seal(2, &[1], 15).offered(&proofs));
        let repeated = tail.offer(&rows_only(3, &proofs, ProofTrust::Owner));

        assert!(!first.grew, "a batch no proof names is not served");
        assert!(second.grew);
        assert_eq!(repeated, PeerFreshChange::default());
        assert_eq!(served(&tail), [(10, 2), (12, 3)]);
        assert_eq!(tail.served_end(), Some(14));
        assert_eq!(
            tail.batches_from_offset(13, 10),
            [sends.placed(1, 12)],
            "a poll inside a batch starts at that batch"
        );
        assert_eq!(tail.conflict_at(), None);
    }

    #[test]
    fn given_two_rows_that_bind_one_offset_to_different_batches_when_offered_should_stop_serving_for_good()
     {
        let sends = seal(1, &[2, 2], 10);
        let mut tail = PeerFreshTail::new(namespace(), REVISION, Some(9));
        tail.offer(&sends.offered(&[]));
        tail.offer(&rows_only(2, &[sends.proof(0, 10)], ProofTrust::Owner));
        assert_eq!(tail.served_end(), Some(11));

        let change = tail.offer(&rows_only(3, &[sends.proof(1, 10)], ProofTrust::Owner));

        assert_eq!(change.conflict_at, Some(10));
        assert_eq!(
            tail.served_end(),
            None,
            "no fresh batch is served past a conflict"
        );
        assert_eq!(served(&tail), []);
        let later = tail.offer(&rows_only(4, &[sends.proof(1, 12)], ProofTrust::Owner));
        assert_eq!(later.conflict_at, None, "the alarm is raised once");
        assert_eq!(tail.served_end(), None);
        assert!(!tail.apply_floor(Some(20)).grew);
        assert_eq!(tail.served_end(), None, "a newer floor does not lift it");
    }

    #[test]
    fn given_rows_that_bind_overlapping_offsets_when_offered_should_raise_the_conflict() {
        let sends = seal(1, &[3, 3], 10);
        let mut tail = PeerFreshTail::new(namespace(), REVISION, Some(9));
        tail.offer(&sends.offered(&[sends.proof(0, 10)]));

        let change = tail.offer(&rows_only(2, &[sends.proof(1, 11)], ProofTrust::Owner));

        assert_eq!(change.conflict_at, Some(11));
        assert_eq!(tail.served_end(), None);
    }

    #[test]
    fn given_a_gap_below_proven_rows_when_offered_should_wait_for_a_floor_and_never_fill_it_from_claims()
     {
        let sends = seal(1, &[2, 2, 2], 10);
        let mut tail = PeerFreshTail::new(namespace(), REVISION, Some(9));
        tail.offer(&sends.offered(&[]));

        let change = tail.offer(&rows_only(
            2,
            &[sends.proof(0, 10), sends.proof(2, 14)],
            ProofTrust::Owner,
        ));

        assert!(change.grew);
        assert_eq!(
            served(&tail),
            [(10, 2)],
            "the batch past the gap waits, although its writer claimed the gap"
        );
        assert_eq!(tail.lag().messages, 4);
        assert!(tail.lag().micros > 0);

        let floor = tail.apply_floor(Some(13));

        assert!(floor.grew, "the floor closes the gap");
        assert_eq!(served(&tail), [(14, 2)]);
        assert_eq!(tail.served_end(), Some(15));
        assert_eq!(tail.lag(), PeerFreshLag::default());
    }

    #[test]
    fn given_a_rows_only_object_when_offered_should_take_its_proofs_and_keep_no_source() {
        let sends = seal(1, &[4], 0);
        let mut tail = PeerFreshTail::new(namespace(), REVISION, None);
        tail.offer(&sends.offered(&[]));

        let change = tail.offer(&rows_only(2, &[sends.proof(0, 0)], ProofTrust::Owner));

        assert!(change.grew);
        assert_eq!(served(&tail), [(0, 4)]);
        assert!(tail.sources.is_empty(), "the proven batch is spent");
        assert_eq!(tail.missing_sources(), Vec::<FreshObjectId>::new());
    }

    #[test]
    fn given_a_proof_whose_source_is_not_held_when_offered_should_ask_for_it_and_serve_once_it_comes()
     {
        let sends = seal(1, &[2], 0);
        let gone = seal(2, &[2], 2);
        let mut tail = PeerFreshTail::new(namespace(), REVISION, None);

        tail.offer(&rows_only(
            3,
            &[sends.proof(0, 0), gone.proof(0, 2)],
            ProofTrust::Owner,
        ));

        assert_eq!(tail.served_end(), None);
        assert_eq!(tail.missing_sources(), [sends.id, gone.id]);
        tail.mark_gone(gone.id);
        assert_eq!(
            tail.missing_sources(),
            [sends.id],
            "a gone object is not asked for again"
        );

        let change = tail.offer(&sends.rows(&[], ProofTrust::None));

        assert!(change.grew);
        assert_eq!(served(&tail), [(0, 2)]);
        assert_eq!(tail.missing_sources(), Vec::<FreshObjectId>::new());
        assert!(
            !tail.apply_floor(Some(3)).grew && tail.served_end().is_none(),
            "the floor takes the batch whose object is gone"
        );
    }

    #[test]
    fn given_rows_whose_source_disagrees_when_rebuilding_should_raise_the_conflict() {
        let sends = seal(1, &[2], 0);
        let mut proof = sends.proof(0, 0);
        proof.base_timestamp += 1;
        let mut tail = PeerFreshTail::new(namespace(), REVISION, None);
        tail.offer(&sends.offered(&[]));

        let change = tail.offer(&rows_only(2, &[proof], ProofTrust::Owner));

        assert_eq!(change.conflict_at, Some(0));
        assert_eq!(tail.served_end(), None);
    }

    #[test]
    fn given_objects_of_a_retired_cluster_when_offered_should_take_only_rows_that_end_at_or_below_the_cutover()
     {
        let retired = seal(1, &[2, 2, 2], 10);
        let successor = seal(9, &[2], 16);
        let mut tail = PeerFreshTail::new(namespace(), REVISION, Some(9));
        tail.offer(&retired.rows(&[], ProofTrust::None));
        tail.offer(&successor.offered(&[]));

        let taken = tail.offer(&rows_only(
            2,
            &[
                retired.proof(0, 10),
                retired.proof(1, 12),
                retired.proof(2, 14),
            ],
            ProofTrust::Retired { cutover: 14 },
        ));
        let untrusted = tail.offer(&rows_only(3, &[retired.proof(2, 14)], ProofTrust::None));

        assert!(taken.grew && !untrusted.grew);
        assert_eq!(
            served(&tail),
            [(10, 2), (12, 2)],
            "the batch past the cutover is not the retired cluster's"
        );
        tail.offer(&rows_only(10, &[successor.proof(0, 14)], ProofTrust::Owner));
        assert_eq!(served(&tail), [(10, 2), (12, 2), (14, 2)]);
        assert_eq!(tail.conflict_at(), None);
    }

    #[test]
    fn given_a_newer_floor_when_applied_should_drop_the_batches_at_or_below_it() {
        let sends = seal(1, &[2, 2, 2], 0);
        let mut tail = PeerFreshTail::new(namespace(), REVISION, None);
        tail.offer(&sends.offered(&[]));
        tail.offer(&rows_only(
            2,
            &[sends.proof(0, 0), sends.proof(1, 2), sends.proof(2, 4)],
            ProofTrust::Owner,
        ));

        tail.apply_floor(Some(3));
        let below = tail.offer(&rows_only(3, &[sends.proof(0, 0)], ProofTrust::Owner));

        assert_eq!(served(&tail), [(4, 2)]);
        assert_eq!(tail.start(), 4);
        assert_eq!(
            below,
            PeerFreshChange::default(),
            "a row below the floor is ignored"
        );
        assert_eq!(tail.batches_from_offset(2, 10).len(), 1);
    }

    #[test]
    fn given_rows_of_another_incarnation_when_offered_should_ignore_them() {
        let sends = seal(1, &[2], 0);
        let mut other = sends.offered(&[sends.proof(0, 0)]);
        other.row.created_revision += 1;
        let mut tail = PeerFreshTail::new(namespace(), REVISION, None);

        assert_eq!(tail.offer(&other), PeerFreshChange::default());
        assert_eq!(tail.missing_sources(), Vec::<FreshObjectId>::new());
        assert_eq!(tail.served_end(), None);
    }

    #[test]
    fn given_served_batches_when_polling_by_timestamp_should_start_at_the_last_batch_stamped_before_it()
     {
        let sends = seal(1, &[2, 2, 2], 0);
        let mut tail = PeerFreshTail::new(namespace(), REVISION, None);
        tail.offer(&sends.offered(&[]));
        tail.offer(&rows_only(
            2,
            &[sends.proof(0, 0), sends.proof(1, 2), sends.proof(2, 4)],
            ProofTrust::Owner,
        ));

        let from = |at: u64| -> Vec<u64> {
            tail.batches_from_timestamp(at, 10)
                .iter()
                .map(|batch| batch.header.base_offset)
                .collect()
        };

        assert_eq!(from(timestamp(0)), [0, 2, 4]);
        assert_eq!(from(timestamp(3)), [2, 4]);
        assert_eq!(
            from(timestamp(4)),
            [2, 4],
            "the batch before may hold messages stamped at it"
        );
        assert_eq!(from(timestamp(9)), [4]);
    }
}
