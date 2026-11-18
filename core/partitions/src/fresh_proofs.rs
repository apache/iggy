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

//! The proof rows of a partition that places its batches.
//!
//! A fresh object is stored before its batches are prepared, so it cannot
//! say where they commit. Each replica keeps one row per placed batch it
//! committed, taken in the commit walk, and the primary's shard seals the
//! rows into later objects. Rows come from commits only, never from the
//! order in which a writer stored its objects: a primary that lost its view
//! keeps storing objects, and none of its rows may name a batch that did not
//! commit.
//!
//! A replica drops the rows below the highest `proven_below` that a committed
//! placement claimed, and the rows its long-term segments hold, so it keeps
//! what may not be proven yet and a new primary proves it with no read of
//! the store.

use std::cell::Cell;
use std::collections::VecDeque;

use iggy_object_store::FreshProofRow;

/// The committed placed batches of a partition whose proof may not be in a
/// stored object yet, ascending and contiguous.
#[derive(Debug, Default)]
pub struct FreshProofs {
    rows: VecDeque<FreshProofRow>,
    /// One past the last offset of the newest row taken.
    next_offset: Option<u64>,
    /// The rows hold every committed batch from this offset on. A replica
    /// that missed commits, at boot or through a state transfer, starts
    /// over at the first one it walks.
    complete_from: u64,
    /// The highest `proven_below` among the committed placements.
    proven_floor: u64,
    /// A row arrived on the primary since the shard last asked.
    wanted: Cell<bool>,
    /// The view in which the shard last asked.
    asked_in_view: Cell<Option<u32>>,
}

impl FreshProofs {
    /// Take the row of a batch that committed, whose placement claimed
    /// `proven_below`, with the partition's long-term floor after it.
    pub fn commit(&mut self, row: FreshProofRow, proven_below: u64, long_term_floor: Option<u64>) {
        if self.next_offset != Some(row.base_offset) {
            self.rows.clear();
            self.complete_from = row.base_offset;
        }
        self.next_offset = Some(end_of(&row));
        self.rows.push_back(row);
        self.proven_floor = self.proven_floor.max(proven_below);
        self.prune(long_term_floor);
    }

    /// Drop the rows no reader needs a proof for: those below the highest
    /// claim and those that `long_term_floor` covers.
    pub fn prune(&mut self, long_term_floor: Option<u64>) {
        let long_term_end = long_term_end(long_term_floor);
        while self
            .rows
            .front()
            .is_some_and(|row| row.base_offset < self.proven_floor || end_of(row) <= long_term_end)
        {
            self.rows.pop_front();
        }
    }

    /// The rows from the one whose batch starts at `offset` on, or from the
    /// first one held when that one is gone.
    pub fn rows_from(&self, offset: u64) -> impl Iterator<Item = &FreshProofRow> {
        let first = self.rows.partition_point(|row| row.base_offset < offset);
        self.rows.range(first..)
    }

    /// Where a writer that knows nothing of the stored proofs starts: the
    /// first row held, or past the last one.
    #[must_use]
    pub fn first_unproven(&self) -> u64 {
        self.rows
            .front()
            .map(|row| row.base_offset)
            .or(self.next_offset)
            .unwrap_or(0)
    }

    /// One past the last offset of the newest row taken.
    #[must_use]
    pub const fn next_offset(&self) -> Option<u64> {
        self.next_offset
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The `proven_below` of an object sealed now, by a writer whose rows
    /// below `unstored_from` are all in stored objects: below it, every
    /// committed batch has a proof in a stored object or sits in a long-term
    /// segment.
    #[must_use]
    pub fn proven_below(&self, unstored_from: u64, long_term_floor: Option<u64>) -> u64 {
        let done = self.proven_floor.max(long_term_end(long_term_floor));
        if self.complete_from > done {
            return done;
        }
        unstored_from
            .min(self.next_offset.unwrap_or(done))
            .max(done)
    }

    /// Note that a row arrived on the primary.
    pub(crate) fn want(&self) {
        self.wanted.set(true);
    }

    /// Whether rows wait for the shard of the primary in `view`: one arrived
    /// since the last call, or this is the first call in `view` and rows are
    /// held, as on a replica that just became the primary.
    pub(crate) fn take_wanted(&self, view: u32) -> bool {
        let first_in_view = self.asked_in_view.replace(Some(view)) != Some(view);
        self.wanted.take() || (first_in_view && !self.rows.is_empty())
    }

    /// Forget a row that arrived while this replica does not lead.
    pub(crate) fn unwant(&self) {
        self.wanted.set(false);
        self.asked_in_view.set(None);
    }
}

fn end_of(row: &FreshProofRow) -> u64 {
    row.base_offset.saturating_add(u64::from(row.message_count))
}

fn long_term_end(long_term_floor: Option<u64>) -> u64 {
    long_term_floor.map_or(0, |floor| floor.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_object_store::FreshObjectId;

    fn row(base_offset: u64, message_count: u32) -> FreshProofRow {
        FreshProofRow {
            object_id: FreshObjectId::from_raw(u128::from(base_offset) + 1),
            position: 72,
            base_offset,
            base_timestamp: 1_000 + base_offset,
            message_count,
            placed_checksum: base_offset ^ 0xfeed,
        }
    }

    fn offsets(proofs: &FreshProofs) -> Vec<u64> {
        proofs.rows_from(0).map(|row| row.base_offset).collect()
    }

    /// Three batches of two messages each, at 0, 2 and 4, whose placements
    /// claimed `claims`.
    fn committed(claims: [u64; 3]) -> FreshProofs {
        let mut proofs = FreshProofs::default();
        for (index, claim) in (0..).zip(claims) {
            proofs.commit(row(index * 2, 2), claim, None);
        }
        proofs
    }

    #[test]
    fn given_committed_batches_when_their_placements_claim_nothing_should_keep_every_row() {
        let proofs = committed([0, 0, 0]);

        assert_eq!(offsets(&proofs), [0, 2, 4]);
        assert_eq!(proofs.next_offset(), Some(6));
        assert_eq!(proofs.first_unproven(), 0);
    }

    #[test]
    fn given_committed_placements_that_claim_proofs_when_taking_rows_should_drop_the_rows_below_the_highest_claim()
     {
        let proofs = committed([0, 2, 0]);

        assert_eq!(offsets(&proofs), [2, 4]);
        assert_eq!(proofs.first_unproven(), 2);
        let pruned = committed([0, 0, 4]);
        assert_eq!(offsets(&pruned), [4]);
    }

    #[test]
    fn given_a_long_term_floor_when_pruning_should_drop_only_the_rows_it_covers_whole() {
        let mut proofs = committed([0, 0, 0]);

        proofs.prune(Some(2));
        assert_eq!(
            offsets(&proofs),
            [2, 4],
            "the batch at 2 ends past the floor"
        );
        proofs.prune(Some(3));
        assert_eq!(offsets(&proofs), [4]);
        proofs.prune(Some(5));
        assert!(proofs.is_empty());
        assert_eq!(proofs.first_unproven(), 6);
    }

    #[test]
    fn given_a_commit_that_does_not_follow_the_last_row_when_taking_it_should_start_over() {
        let mut proofs = committed([0, 0, 0]);

        proofs.commit(row(10, 1), 0, None);

        assert_eq!(offsets(&proofs), [10]);
        assert_eq!(proofs.next_offset(), Some(11));
    }

    #[test]
    fn given_rows_from_an_offset_when_reading_should_start_at_that_row_or_at_the_first_held() {
        let proofs = committed([0, 2, 0]);

        assert_eq!(
            proofs
                .rows_from(4)
                .map(|row| row.base_offset)
                .collect::<Vec<_>>(),
            [4]
        );
        assert_eq!(
            proofs
                .rows_from(0)
                .map(|row| row.base_offset)
                .collect::<Vec<_>>(),
            [2, 4]
        );
        assert_eq!(proofs.rows_from(6).count(), 0);
    }

    #[test]
    fn given_a_writer_with_unstored_rows_when_claiming_should_claim_the_first_of_them() {
        let proofs = committed([0, 0, 0]);

        assert_eq!(proofs.proven_below(2, None), 2);
        assert_eq!(proofs.proven_below(0, None), 0);
        assert_eq!(
            proofs.proven_below(u64::MAX, None),
            6,
            "no further than the batches that committed"
        );
    }

    #[test]
    fn given_rows_below_the_floors_when_claiming_should_never_claim_less_than_the_floors() {
        let proofs = committed([0, 2, 0]);

        assert_eq!(proofs.proven_below(0, None), 2, "the highest claim");
        assert_eq!(proofs.proven_below(0, Some(3)), 4, "the long-term floor");
    }

    /// A replica that booted or installed a state transfer holds no row of
    /// the batches before its first commit, which may lack a proof. It
    /// claims no more than the floors say until they pass that commit.
    #[test]
    fn given_rows_that_start_after_a_hole_when_claiming_should_claim_only_what_the_floors_cover() {
        let mut proofs = FreshProofs::default();
        proofs.commit(row(10, 2), 4, None);
        proofs.commit(row(12, 2), 4, None);

        assert_eq!(proofs.proven_below(14, None), 4);
        assert_eq!(proofs.proven_below(14, Some(7)), 8);
        proofs.commit(row(14, 2), 10, None);
        assert_eq!(proofs.proven_below(16, None), 16);
        assert_eq!(proofs.proven_below(12, None), 12);
    }

    #[test]
    fn given_a_partition_that_committed_nothing_when_claiming_should_claim_nothing() {
        let proofs = FreshProofs::default();

        assert_eq!(proofs.proven_below(5, None), 0);
        assert_eq!(proofs.first_unproven(), 0);
    }

    #[test]
    fn given_rows_when_the_shard_asks_should_want_them_once_per_arrival_and_once_per_view() {
        let proofs = committed([0, 0, 0]);

        assert!(proofs.take_wanted(1), "the first ask of a view with rows");
        assert!(!proofs.take_wanted(1));
        proofs.want();
        assert!(proofs.take_wanted(1));
        assert!(!proofs.take_wanted(1));
        assert!(proofs.take_wanted(2), "a new view with rows held");
        proofs.unwant();
        assert!(proofs.take_wanted(2), "asked again after leading stopped");
        assert!(!FreshProofs::default().take_wanted(3), "no rows held");
    }
}
