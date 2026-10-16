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

//! Entries a replica journaled before adopting a newer view, held back from
//! commit and acknowledgement until the view's log proves them.

use iggy_binary_protocol::{CHECKSUM_UNSEALED, PrepareHeader};

/// An unproven window `(floor, through]` installed in `view` and, once known, that
/// view's header at `through + 1`, whose `parent` the window must chain into. The
/// floor is `IggyPartition::fence_floor`: the higher of `commit_min` and the
/// commit point recovered from the WAL, which the fenced walk may still reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewFence {
    pub view: u32,
    pub through: u64,
    pub anchor: Option<PrepareHeader>,
    /// The op the last proof stopped at for lack of an entry. Until it arrives
    /// nothing below it can change, so a proof does not rescan the window.
    pub hole: Option<u64>,
    /// Ticks spent without an anchor, paced by [`crate::IggyPartition::view_fence_probe_due`].
    pub unanchored_ticks: u32,
}

/// What the `parent` chain says about a fenced window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewWindowVerdict {
    /// Every op in the window links into the anchor.
    Proven,
    /// No entry at `op`, so nothing at or below it can be linked yet.
    Hole { op: u64 },
    /// The entry at `op` belongs to a log the view discarded: it is not the one
    /// the op above chains to, or it is not the copy the WAL flush would write.
    Break { op: u64 },
}

/// Walk `(floor, through]` top-down from `anchor_parent`, the checksum the view's
/// entry at `through` must carry.
///
/// An unsealed entry breaks the chain: it names no identity to compare, and its
/// zero `parent` would link every op below it, as [`PrepareHeader::anchors_chain`]
/// refuses for the anchor.
#[must_use]
pub fn view_window_verdict(
    floor: u64,
    through: u64,
    anchor_parent: u128,
    header_at: impl Fn(u64) -> Option<PrepareHeader>,
) -> ViewWindowVerdict {
    let mut expected = anchor_parent;
    for op in (floor + 1..=through).rev() {
        let Some(local) = header_at(op) else {
            return ViewWindowVerdict::Hole { op };
        };
        if local.checksum == CHECKSUM_UNSEALED || local.checksum != expected {
            return ViewWindowVerdict::Break { op };
        }
        expected = local.parent;
    }
    ViewWindowVerdict::Proven
}

#[cfg(test)]
mod tests {
    use super::{ViewWindowVerdict, view_window_verdict};
    use iggy_binary_protocol::{Command, Operation, PrepareHeader};
    use std::collections::BTreeMap;

    const FLOOR: u64 = 3;
    const THROUGH: u64 = 6;

    fn sealed(op: u64, parent: u128, request: u64) -> PrepareHeader {
        let mut header = PrepareHeader {
            command: Command::Prepare,
            operation: Operation::SendMessages,
            op,
            parent,
            request,
            ..Default::default()
        };
        header.checksum = header.identity_checksum();
        header
    }

    /// The view's log over `FLOOR + 1 ..= THROUGH + 1`, each entry chained to the
    /// one below. The last entry is the anchor.
    fn view_log() -> BTreeMap<u64, PrepareHeader> {
        let mut parent = 0;
        (FLOOR + 1..=THROUGH + 1)
            .map(|op| {
                let header = sealed(op, parent, op);
                parent = header.checksum;
                (op, header)
            })
            .collect()
    }

    fn verdict(log: &BTreeMap<u64, PrepareHeader>) -> ViewWindowVerdict {
        let anchor = log[&(THROUGH + 1)];
        view_window_verdict(FLOOR, THROUGH, anchor.parent, |op| log.get(&op).copied())
    }

    #[test]
    fn given_the_views_own_entries_when_proving_should_report_proven() {
        assert_eq!(verdict(&view_log()), ViewWindowVerdict::Proven);
    }

    #[test]
    fn given_an_old_views_entry_under_the_anchor_when_proving_should_report_its_op() {
        let mut log = view_log();
        // Same op, different request: what an isolated old primary journals.
        log.insert(FLOOR + 1, sealed(FLOOR + 1, 0, 99));
        assert_eq!(
            verdict(&log),
            ViewWindowVerdict::Break { op: FLOOR + 1 },
            "a stale entry under the anchor was accepted as the view's"
        );
    }

    #[test]
    fn given_a_hole_under_the_anchor_when_proving_should_report_the_hole_not_a_break() {
        let mut log = view_log();
        log.remove(&(FLOOR + 2));
        log.insert(FLOOR + 1, sealed(FLOOR + 1, 0, 99));
        assert_eq!(
            verdict(&log),
            ViewWindowVerdict::Hole { op: FLOOR + 2 },
            "a hole is missing data, not evidence against the entry below it"
        );
    }

    #[test]
    fn given_an_unsealed_entry_in_the_window_when_proving_should_report_a_break() {
        let mut log = view_log();
        // Unsealed, with the zero parent an unsealed producer leaves: accepting it
        // would link every op below it unchecked.
        let mut unsealed = log[&(FLOOR + 2)];
        unsealed.checksum = 0;
        unsealed.parent = 0;
        log.insert(FLOOR + 2, unsealed);
        log.insert(FLOOR + 1, sealed(FLOOR + 1, 0, 99));
        assert_eq!(verdict(&log), ViewWindowVerdict::Break { op: FLOOR + 2 });
    }

    #[test]
    fn given_an_empty_window_when_proving_should_report_proven() {
        assert_eq!(
            view_window_verdict(THROUGH, THROUGH, 0, |_| None),
            ViewWindowVerdict::Proven
        );
    }
}
