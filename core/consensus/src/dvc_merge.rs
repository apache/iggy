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

//! Merging a `DoViewChange` quorum into the new view's log: for every op that
//! might be uncommitted, does the new view keep it or discard it?
//!
//! Keeping an op that was never committed costs a wasted slot. Discarding one
//! that WAS committed loses acknowledged client data, so the only proof accepted
//! for discarding is a nack quorum: enough replicas stating they never prepared
//! it that a replication quorum provably never formed. Absent that the op is
//! kept, and if no replica offers its body the view does not start. Stalling is
//! visible and recoverable; losing the op is neither.

#[cfg(test)]
use crate::view_change_quorum::DvcSuffix;
use crate::view_change_quorum::{DvcQuorumArray, StoredDvc, dvc_count, dvc_iter};
use iggy_binary_protocol::PrepareHeader;

/// Sizes the merge needs from the replica.
#[derive(Debug, Clone, Copy)]
pub struct MergeQuorums {
    /// `DoViewChange` messages needed before a view may start.
    pub view_change: usize,
    /// Nacks needed to prove an op uncommitted, so it may be discarded.
    pub nack_prepare: usize,
    /// Cluster size, which bounds how many more DVCs could still arrive.
    pub replica_count: usize,
    /// Cluster-wide pipeline ceiling: an op further than this below a sender's head
    /// cannot still be uncommitted, since no node could have kept it in flight.
    ///
    /// NOT this node's configured depth. The bound applies to a *peer's* head op,
    /// and a local depth larger than that peer's manufactures a commit the peer
    /// never made. Every node's depth is pinned below `DVC_HEADERS_MAX`, so the
    /// ceiling holds for all of them.
    pub prepare_queue_ceiling: u64,
}

/// What the collected DVCs say about starting the view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOutcome {
    /// Fewer than `view_change` DVCs so far.
    AwaitingQuorum,
    /// Quorum is in, but some op is neither provably dead nor recoverable and an
    /// unreported replica could still settle it. Wait for them.
    AwaitingRepair {
        /// The op that cannot yet be decided.
        undecided_op: u64,
    },
    /// Every replica reported and an op is still neither provably dead nor
    /// recoverable. No further message changes that: data loss already happened,
    /// and truncating here would turn it from detected into silent.
    Deadlocked {
        /// The op that cannot be decided.
        undecided_op: u64,
    },
    /// The view can start.
    Ready(MergedLog),
}

/// The log the new primary adopts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedLog {
    /// New head op. Below the highest op any canonical sender reported when a
    /// nack quorum proved the ops above it dead.
    pub op_head: u64,
    /// Highest op the quorum proves committed. The merge never discards at or
    /// below this.
    pub commit_max: u64,
    /// Canonical headers for `commit_max..=op_head`, ordered high-to-low op.
    /// The new primary installs these over its own log.
    pub headers: Vec<PrepareHeader>,
    /// Headers non-canonical senders report committed. Installed unconditionally,
    /// because header repair will not cross a gap to reach them later.
    pub committed_elsewhere: Vec<PrepareHeader>,
}

/// Highest op the quorum proves committed.
///
/// Three independent lower bounds, because a single sender's view of the commit
/// point can lag arbitrarily while the cluster's cannot:
/// * each sender's own reported commit,
/// * the `commit` its head prepare carries, stamped by that op's primary,
/// * its head minus the cluster-wide pipeline ceiling, since nothing further back
///   than one pipeline can still be in flight.
///
/// The lowest op in a sender's suffix is deliberately NOT a fourth bound, and
/// re-adding one is a data-loss bug. It would be sound only if the suffix floor
/// were the commit point by construction; here it is computed, and two paths in
/// `build_dvc_suffix` raise it above the sender's commit (ops are 1-based, so
/// commit 0 floors at op 1; the `DVC_HEADERS_MAX` clamp drops the bottom of an
/// over-wide window). Nothing on the wire distinguishes the two.
///
/// Nothing is lost by omitting it: the snapshot is tagged with the `(op, commit)`
/// the header is stamped with and dropped on a mismatch, so the floor either
/// equals `dvc.commit`, already the first bound, or exceeds it, the unsound case.
#[must_use]
pub fn merge_commit_max(quorum: &DvcQuorumArray, prepare_queue_ceiling: u64) -> u64 {
    let mut commit_max = 0;
    for dvc in dvc_iter(quorum) {
        commit_max = commit_max.max(dvc.commit);
        commit_max = commit_max.max(dvc.op.saturating_sub(prepare_queue_ceiling));
        if let Some(head) = dvc.suffix.headers().first() {
            commit_max = commit_max.max(head.commit);
        }
    }
    commit_max
}

/// Highest `log_view` any sender reported.
///
/// Senders at this `log_view` were in every earlier view change, so their headers
/// already reflect the truncations those views decided. That makes them canonical,
/// and a lower-`log_view` sender disagreeing is evidence against its own header.
fn log_view_canonical(quorum: &DvcQuorumArray) -> Option<u32> {
    dvc_iter(quorum).map(|dvc| dvc.log_view).max()
}

/// Per-op tally over the whole quorum.
struct OpVerdict<'a> {
    canonical: Option<&'a PrepareHeader>,
    /// Senders holding the canonical header AND able to serve its body.
    copies: usize,
    nacks: usize,
    /// Canonical senders disagree about this op, so no header here is trustworthy.
    conflict: bool,
}

/// What the canonical senders say about one op.
struct CanonicalAt<'a> {
    header: Option<&'a PrepareHeader>,
    /// Two senders at the canonical `log_view` disagree here.
    conflict: bool,
}

/// The canonical header at `op`, and whether the canonical senders agree.
///
/// Every canonical sender is consulted, not just the first: they were all in
/// normal status in that view and a primary prepares one thing per op, so a
/// disagreement is not a vote but proof that one header is wrong with no way to
/// tell which. The caller treats the op as undecidable.
fn canonical_header_at<'a>(canonical_senders: &[&'a StoredDvc], op: u64) -> CanonicalAt<'a> {
    let mut header: Option<&'a PrepareHeader> = None;
    let mut conflict = false;
    for dvc in canonical_senders {
        let Some(index) = dvc.suffix.index_of(dvc.op, op) else {
            continue;
        };
        let Some(candidate) = dvc.suffix.valid_header_at(index) else {
            continue;
        };
        match header {
            Some(existing) if existing.checksum != candidate.checksum => conflict = true,
            Some(_) => {}
            None => header = Some(candidate),
        }
    }
    CanonicalAt { header, conflict }
}

/// Tally every sender's position on `op`.
fn tally_op<'a>(
    quorum: &'a DvcQuorumArray,
    canonical_senders: &[&'a StoredDvc],
    canonical_log_view: u32,
    op: u64,
) -> OpVerdict<'a> {
    let CanonicalAt {
        header: canonical,
        conflict,
    } = canonical_header_at(canonical_senders, op);
    let mut copies = 0;
    let mut nacks = 0;

    for dvc in dvc_iter(quorum) {
        // The sender's log stops below this op, so it never prepared it.
        if dvc.op < op {
            nacks += 1;
            continue;
        }
        let Some(index) = dvc.suffix.index_of(dvc.op, op) else {
            // The sender said nothing about this op, so it abstains: no nack, no
            // copy. Counting silence as a nack is sound only when every DVC carries
            // headers, since then falling outside a window means being above it.
            // Two cases here are not, and abstaining costs a slower view change
            // where nacking costs data.
            //
            // An empty suffix is not a vote. It comes from a replica with
            // nothing uncommitted, or one whose snapshot no longer matches its
            // log; reading it as
            // agreement with someone else's nack discards a committed op on one real
            // nack plus one silence. And with `dvc.op >= op` established above, a
            // non-empty suffix not covering `op` puts `op` below the sender's window
            // floor, at or below its own commit point, so nacking it is backwards.
            // Only the defensive `DVC_HEADERS_MAX` clamp reaches that.
            continue;
        };

        let held = dvc.suffix.valid_header_at(index);
        if let (Some(held), Some(canonical)) = (held, canonical)
            && dvc.suffix.offers_body(index)
            && held.checksum == canonical.checksum
        {
            copies += 1;
        }

        if dvc.suffix.nacks(index) {
            // Explicit: the sender proves it never prepared this op.
            nacks += 1;
        } else if let Some(held) = held {
            // Only a sender BEHIND the canonical log_view can implicitly nack.
            // Without this, corrupting one canonical header in transit turns every
            // honest sender's correct header into an implicit nack against the
            // garbage: a nack quorum on three replicas. A same-log_view
            // disagreement is evidence, not a vote, and goes through `conflict`.
            let may_nack_implicitly = dvc.log_view < canonical_log_view;
            match canonical {
                // Implicit: the sender holds a DIFFERENT prepare, so not this one.
                Some(canonical) if may_nack_implicitly && held.checksum != canonical.checksum => {
                    nacks += 1;
                }
                // Implicit: no canonical sender holds anything here, so a newer
                // view already truncated this op and the sender holds a corpse.
                None if may_nack_implicitly => nacks += 1,
                _ => {}
            }
        }
    }

    OpVerdict {
        canonical,
        copies,
        nacks,
        conflict,
    }
}

/// Collect the canonical headers for `commit_max..=op_head`, high-to-low.
///
/// Checks the hash chain as it walks: a break means the canonical senders agreed
/// on individual ops but not on one history, which no later step would notice.
fn canonical_headers(
    canonical_senders: &[&StoredDvc],
    op_head: u64,
    commit_max: u64,
) -> Option<Vec<PrepareHeader>> {
    if op_head == 0 {
        return Some(Vec::new());
    }
    let floor = commit_max.max(1);
    let mut headers = Vec::new();
    let mut child: Option<PrepareHeader> = None;
    let mut op = op_head;
    loop {
        let at = canonical_header_at(canonical_senders, op);
        if at.conflict {
            return None;
        }
        let header = *at.header?;
        if let Some(child) = child
            && child.parent != header.checksum
        {
            tracing::error!(
                op,
                child_op = child.op,
                "view-change headers do not hash-chain; refusing to install"
            );
            return None;
        }
        child = Some(header);
        headers.push(header);
        if op <= floor {
            break;
        }
        op -= 1;
    }
    Some(headers)
}

/// Headers that a non-canonical sender reports committed.
///
/// Trusted on that sender's word alone, unlike anything above its commit point:
/// header repair walks the chain backwards and stops at a gap, so an op missing
/// below the new primary's commit point can never be repaired into place and
/// refusing it here strands the log permanently. The claim is already
/// quorum-backed, since a sender cannot report an op committed unless a
/// replication quorum held it.
///
/// `None` when two senders report *different* prepares committed at one op. These
/// install unconditionally, so guessing is least affordable here; as in
/// [`canonical_header_at`], the caller treats it as undecidable.
fn committed_elsewhere(
    quorum: &DvcQuorumArray,
    canonical_log_view: u32,
    already_installed: &[PrepareHeader],
) -> Option<Vec<PrepareHeader>> {
    let mut extra: Vec<PrepareHeader> = Vec::new();
    for dvc in dvc_iter(quorum).filter(|dvc| dvc.log_view < canonical_log_view) {
        for (index, header) in dvc.suffix.headers().iter().enumerate() {
            if header.op > dvc.commit {
                continue;
            }
            if dvc.suffix.valid_header_at(index).is_none() {
                continue;
            }
            if already_installed
                .iter()
                .any(|installed| installed.op == header.op)
            {
                continue;
            }
            if let Some(queued) = extra.iter().find(|queued| queued.op == header.op) {
                if queued.checksum != header.checksum {
                    tracing::error!(
                        op = header.op,
                        "replicas disagree about the prepare committed at op {}; refusing to \
                         choose one to install",
                        header.op
                    );
                    return None;
                }
                continue;
            }
            extra.push(*header);
        }
    }
    Some(extra)
}

/// Decide whether the new view can start, and with what log.
///
/// Walks every op that might be uncommitted, from the proven commit point to the
/// highest a canonical sender reported, stopping at the first proved dead.
#[must_use]
pub fn merge_dvc_quorum(quorum: &DvcQuorumArray, quorums: MergeQuorums) -> MergeOutcome {
    let received = dvc_count(quorum);
    if received < quorums.view_change {
        return MergeOutcome::AwaitingQuorum;
    }

    let Some(canonical_log_view) = log_view_canonical(quorum) else {
        return MergeOutcome::AwaitingQuorum;
    };
    let canonical_senders: Vec<&StoredDvc> = dvc_iter(quorum)
        .filter(|dvc| dvc.log_view == canonical_log_view)
        .collect();
    debug_assert!(
        !canonical_senders.is_empty(),
        "the max log_view must be held by at least one sender"
    );

    let commit_max = merge_commit_max(quorum, quorums.prepare_queue_ceiling);
    let op_head_max = canonical_senders
        .iter()
        .map(|dvc| dvc.op)
        .max()
        .unwrap_or(commit_max)
        .max(commit_max);

    if op_head_max == 0 {
        // Nothing was ever prepared, so nothing to decide. Ops are 1-based, and
        // scanning op 0 reads every sender's absent entry as a nack, manufacturing
        // a nack quorum for an op that does not exist.
        return MergeOutcome::Ready(MergedLog {
            op_head: 0,
            commit_max: 0,
            headers: Vec::new(),
            committed_elsewhere: Vec::new(),
        });
    }

    let mut op_head = op_head_max;
    // Start at the proven commit point, or op 1 when nothing is committed. The
    // commit point is scanned so the adopted log is anchored on a servable header.
    let mut op = commit_max.max(1);
    while op <= op_head_max {
        let verdict = tally_op(quorum, &canonical_senders, canonical_log_view, op);

        if verdict.nacks >= quorums.nack_prepare {
            if op <= commit_max {
                // A nack quorum for a committed op is impossible under quorum
                // intersection, so a peer lied or a bitset is wrong. Refuse the
                // view rather than assert, so one bad peer stalls the group instead
                // of panicking a node into a restart loop.
                tracing::error!(
                    op,
                    commit_max,
                    nacks = verdict.nacks,
                    nack_quorum = quorums.nack_prepare,
                    "nack quorum for an op the quorum proves committed; refusing the view"
                );
                return MergeOutcome::Deadlocked { undecided_op: op };
            }
            op_head = op - 1;
            break;
        }

        if verdict.conflict {
            // Senders all in normal status in the same view disagree about what it
            // prepared here. One header is wrong with no way to tell which, so
            // refuse rather than pick.
            tracing::error!(
                op,
                log_view = canonical_log_view,
                "replicas at the same log_view disagree about op {op}; refusing to choose a \
                 canonical header for it"
            );
            return if received < quorums.replica_count {
                MergeOutcome::AwaitingRepair { undecided_op: op }
            } else {
                MergeOutcome::Deadlocked { undecided_op: op }
            };
        }

        if verdict.canonical.is_none() || verdict.copies == 0 {
            // Neither provably dead nor recoverable. An outstanding replica may
            // hold the body or supply the deciding nack.
            return if received < quorums.replica_count {
                MergeOutcome::AwaitingRepair { undecided_op: op }
            } else {
                tracing::error!(
                    op,
                    canonical = verdict.canonical.is_some(),
                    copies = verdict.copies,
                    nacks = verdict.nacks,
                    nack_quorum = quorums.nack_prepare,
                    "every replica reported and op {op} is neither recoverable nor provably \
                     uncommitted; the view cannot start"
                );
                MergeOutcome::Deadlocked { undecided_op: op }
            };
        }

        op += 1;
    }

    debug_assert!(op_head >= commit_max);
    let Some(headers) = canonical_headers(&canonical_senders, op_head, commit_max) else {
        return MergeOutcome::Deadlocked {
            undecided_op: op_head,
        };
    };
    let Some(committed_elsewhere) = committed_elsewhere(quorum, canonical_log_view, &headers)
    else {
        // Two senders disagree about a committed op. Not a repair problem: these
        // install without a nack quorum, and no message resolves which is true.
        return MergeOutcome::Deadlocked {
            undecided_op: op_head,
        };
    };

    MergeOutcome::Ready(MergedLog {
        op_head,
        commit_max,
        headers,
        committed_elsewhere,
    })
}

/// Build a suffix for a sender that holds every op in `commit..=op` with a
/// servable body. Test helper.
#[cfg(test)]
#[must_use]
pub fn suffix_all_present(headers: Vec<PrepareHeader>) -> DvcSuffix {
    // `1 << 128` overflows, and a full-width suffix is exactly what the clamp
    // produces, so the widest case cannot use the shift. Past the width is left to
    // `DvcSuffix::new`, which rejects it by name rather than as an overflow.
    let count = u32::try_from(headers.len()).unwrap_or(u32::MAX).min(128);
    let mask = u128::MAX.checked_shr(128 - count).unwrap_or(0);
    DvcSuffix::new(headers, 0, mask)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DVC_HEADERS_MAX;
    use crate::view_change_quorum::{dvc_blank, dvc_quorum_array_empty, dvc_record};
    use iggy_binary_protocol::{Command2, Operation};

    /// Three replicas: replication 2, view-change 2, nack 2.
    fn quorums_r3() -> MergeQuorums {
        MergeQuorums {
            view_change: 2,
            nack_prepare: 2,
            replica_count: 3,
            prepare_queue_ceiling: 32,
        }
    }

    /// A prepare whose checksum derives from its op, so the hash chain connects.
    fn prepare(op: u64, view: u32) -> PrepareHeader {
        PrepareHeader {
            command: Command2::Prepare,
            operation: Operation::CreateStream,
            op,
            view,
            checksum: u128::from(op) | (u128::from(view) << 64),
            parent: if op <= 1 {
                0
            } else {
                u128::from(op - 1) | (u128::from(view) << 64)
            },
            // Left at zero so each test drives `commit_max` through the bound it is
            // about; `merge_commit_max` honours this field, covered separately below.
            commit: 0,
            ..Default::default()
        }
    }

    /// Headers for `low..=high`, ordered high-to-low as a suffix requires.
    fn suffix_headers(low: u64, high: u64, view: u32) -> Vec<PrepareHeader> {
        (low..=high).rev().map(|op| prepare(op, view)).collect()
    }

    fn dvc(replica: u8, log_view: u32, op: u64, commit: u64, suffix: DvcSuffix) -> StoredDvc {
        StoredDvc {
            replica,
            log_view,
            op,
            commit,
            suffix,
        }
    }

    #[test]
    fn given_agreeing_quorum_when_merging_should_adopt_the_shared_head() {
        let mut quorum = dvc_quorum_array_empty();
        dvc_record(
            &mut quorum,
            dvc(0, 1, 5, 3, suffix_all_present(suffix_headers(3, 5, 1))),
        );
        dvc_record(
            &mut quorum,
            dvc(1, 1, 5, 3, suffix_all_present(suffix_headers(3, 5, 1))),
        );

        let MergeOutcome::Ready(log) = merge_dvc_quorum(&quorum, quorums_r3()) else {
            panic!("an agreeing quorum must be ready");
        };
        assert_eq!(log.op_head, 5);
        assert_eq!(log.commit_max, 3);
        assert_eq!(
            log.headers.iter().map(|h| h.op).collect::<Vec<_>>(),
            vec![5, 4, 3],
            "headers run high-to-low from the head down to commit_max"
        );
    }

    #[test]
    fn given_nack_quorum_above_commit_when_merging_should_truncate_to_the_nacked_op() {
        // Both survivors hold 1..=3 and never prepared 4, so the head drops to 3.
        let mut quorum = dvc_quorum_array_empty();
        let mut headers = suffix_headers(2, 4, 1);
        headers[0] = dvc_blank(4);
        let nack_op_four = DvcSuffix::new(headers.clone(), 0b001, 0b110);
        dvc_record(&mut quorum, dvc(0, 1, 4, 2, nack_op_four.clone()));
        dvc_record(&mut quorum, dvc(1, 1, 4, 2, nack_op_four));

        let MergeOutcome::Ready(log) = merge_dvc_quorum(&quorum, quorums_r3()) else {
            panic!("a nack quorum must decide the view");
        };
        assert_eq!(log.op_head, 3, "op 4 is provably uncommitted");
        assert!(log.headers.iter().all(|header| header.op <= 3));
    }

    #[test]
    fn given_one_nack_short_of_quorum_when_merging_should_keep_the_op() {
        // Replica 0 never saw op 4; replica 1 holds it and can serve it. One nack
        // is short of the quorum of 2, so op 4 survives.
        let mut quorum = dvc_quorum_array_empty();
        let mut holed = suffix_headers(2, 4, 1);
        holed[0] = dvc_blank(4);
        dvc_record(
            &mut quorum,
            dvc(0, 1, 4, 2, DvcSuffix::new(holed, 0b001, 0b110)),
        );
        dvc_record(
            &mut quorum,
            dvc(1, 1, 4, 2, suffix_all_present(suffix_headers(2, 4, 1))),
        );

        let MergeOutcome::Ready(log) = merge_dvc_quorum(&quorum, quorums_r3()) else {
            panic!("one nack must not decide the view");
        };
        assert_eq!(log.op_head, 4, "a single nack cannot discard op 4");
        assert!(
            log.headers.iter().any(|header| header.op == 4),
            "the surviving op must be installed"
        );
    }

    #[test]
    fn given_committed_op_when_nacked_by_quorum_should_refuse_rather_than_truncate() {
        // A nack quorum at or below the proven commit point is impossible under
        // quorum intersection. If it appears anyway, refuse; never truncate.
        let mut quorum = dvc_quorum_array_empty();
        let blanks = vec![dvc_blank(3)];
        let all_nacked = DvcSuffix::new(blanks, 0b1, 0b0);
        dvc_record(&mut quorum, dvc(0, 1, 3, 3, all_nacked.clone()));
        dvc_record(&mut quorum, dvc(1, 1, 3, 3, all_nacked));

        assert_eq!(
            merge_dvc_quorum(&quorum, quorums_r3()),
            MergeOutcome::Deadlocked { undecided_op: 3 },
            "a committed op must never be truncated"
        );
    }

    #[test]
    fn given_header_without_a_servable_body_when_replicas_outstanding_should_await_repair() {
        // Both senders have op 4's header, neither can serve its body, and replica 2
        // has not reported. A head whose body nobody holds would wedge the view.
        let mut quorum = dvc_quorum_array_empty();
        let headers = suffix_headers(2, 4, 1);
        let header_only = DvcSuffix::new(headers, 0, 0b110);
        dvc_record(&mut quorum, dvc(0, 1, 4, 2, header_only.clone()));
        dvc_record(&mut quorum, dvc(1, 1, 4, 2, header_only));

        assert_eq!(
            merge_dvc_quorum(&quorum, quorums_r3()),
            MergeOutcome::AwaitingRepair { undecided_op: 4 }
        );
    }

    #[test]
    fn given_all_replicas_reported_and_op_undecidable_should_deadlock() {
        let mut quorum = dvc_quorum_array_empty();
        let headers = suffix_headers(2, 4, 1);
        let header_only = DvcSuffix::new(headers, 0, 0b110);
        dvc_record(&mut quorum, dvc(0, 1, 4, 2, header_only.clone()));
        dvc_record(&mut quorum, dvc(1, 1, 4, 2, header_only.clone()));
        dvc_record(&mut quorum, dvc(2, 1, 4, 2, header_only));

        assert_eq!(
            merge_dvc_quorum(&quorum, quorums_r3()),
            MergeOutcome::Deadlocked { undecided_op: 4 },
            "with every replica in, an unrecoverable op stalls the view forever"
        );
    }

    #[test]
    fn given_lower_log_view_sender_when_merging_should_prefer_the_canonical_log() {
        // Replica 1 is at the newer log_view, so its op 4 is canonical and
        // replica 0's stale op 4 counts as an implicit nack against itself.
        let mut quorum = dvc_quorum_array_empty();
        dvc_record(
            &mut quorum,
            dvc(0, 1, 4, 2, suffix_all_present(suffix_headers(2, 4, 1))),
        );
        dvc_record(
            &mut quorum,
            dvc(1, 2, 4, 2, suffix_all_present(suffix_headers(2, 4, 2))),
        );

        let MergeOutcome::Ready(log) = merge_dvc_quorum(&quorum, quorums_r3()) else {
            panic!("the canonical log must win");
        };
        assert_eq!(log.op_head, 4);
        assert!(
            log.headers.iter().all(|header| header.view == 2),
            "installed headers must come from the canonical log_view"
        );
    }

    #[test]
    fn given_committed_header_on_a_stale_sender_should_be_installed_anyway() {
        // Replica 0 is behind on log_view but reports op 2 committed. The canonical
        // window starts at 3, so op 2 is otherwise unreachable across the gap.
        let mut quorum = dvc_quorum_array_empty();
        dvc_record(
            &mut quorum,
            dvc(0, 1, 2, 2, suffix_all_present(suffix_headers(2, 2, 1))),
        );
        dvc_record(
            &mut quorum,
            dvc(1, 2, 4, 3, suffix_all_present(suffix_headers(3, 4, 2))),
        );

        let MergeOutcome::Ready(log) = merge_dvc_quorum(&quorum, quorums_r3()) else {
            panic!("the view must start");
        };
        assert!(
            log.committed_elsewhere.iter().any(|header| header.op == 2),
            "a committed header from a stale sender must still be installed"
        );
    }

    #[test]
    fn given_a_corrupted_canonical_header_should_not_let_honest_senders_nack_it() {
        // Transit corruption of one canonical sender's suffix entry must not turn
        // every other sender's CORRECT header at that op into an implicit nack
        // against the garbage, reaching a nack quorum through the one path that does
        // not verify. Senders at the canonical log_view cannot legitimately disagree,
        // since a primary prepares one thing per op, so it is evidence, not a vote.
        let mut quorum = dvc_quorum_array_empty();

        let mut corrupted = suffix_headers(2, 4, 1);
        corrupted[0].checksum ^= 0xFFFF;
        dvc_record(&mut quorum, dvc(0, 1, 4, 2, suffix_all_present(corrupted)));
        // Two honest senders at the same log_view holding the real op 4.
        dvc_record(
            &mut quorum,
            dvc(1, 1, 4, 2, suffix_all_present(suffix_headers(2, 4, 1))),
        );
        dvc_record(
            &mut quorum,
            dvc(2, 1, 4, 2, suffix_all_present(suffix_headers(2, 4, 1))),
        );

        let outcome = merge_dvc_quorum(&quorum, quorums_r3());
        if let MergeOutcome::Ready(log) = &outcome {
            assert_eq!(
                log.op_head, 4,
                "op 4 is held by two honest senders and must not be discarded"
            );
        }
        assert!(
            !matches!(&outcome, MergeOutcome::Ready(log) if log.op_head < 4),
            "a corrupted canonical header must never authorise truncating op 4, got {outcome:?}"
        );
    }

    #[test]
    fn given_a_mixed_version_quorum_when_one_upgraded_sender_nacks_should_not_truncate() {
        // A silent sender must never count as agreement with someone else's nack.
        // Only replica 1 proves it never held op 4; replica 0 sends no suffix and so
        // says nothing. One real nack is short of the quorum of two, so op 4 has to
        // survive -- reading the empty suffix as a second nack discards it.
        let mut quorum = dvc_quorum_array_empty();
        dvc_record(&mut quorum, dvc(0, 1, 4, 2, DvcSuffix::empty()));
        let mut holed = suffix_headers(2, 4, 1);
        holed[0] = dvc_blank(4);
        dvc_record(
            &mut quorum,
            dvc(1, 1, 4, 2, DvcSuffix::new(holed, 0b001, 0b110)),
        );
        dvc_record(
            &mut quorum,
            dvc(2, 1, 4, 2, suffix_all_present(suffix_headers(2, 4, 1))),
        );

        let MergeOutcome::Ready(log) = merge_dvc_quorum(&quorum, quorums_r3()) else {
            panic!("op 4 is recoverable from replica 2, so the view must start");
        };
        assert_eq!(
            log.op_head, 4,
            "an empty suffix must not stand in for the second nack"
        );
    }

    #[test]
    fn given_a_clamped_suffix_floor_when_merging_should_not_raise_commit_max() {
        // `build_dvc_suffix` clamps a window wider than `DVC_HEADERS_MAX` from below,
        // so its floor stops being the sender's commit point with nothing on the wire
        // saying so. Reading that floor as a commit point marks every op between the
        // real commit and the clamp committed: applied and replied to without a
        // replication quorum, and unreachable by later truncation via `Deadlocked`.
        let mut quorum = dvc_quorum_array_empty();
        // Sender at op 400, commit 200, whose window clamped to 273..=400.
        let clamped = suffix_headers(273, 400, 1);
        assert_eq!(clamped.len(), DVC_HEADERS_MAX);
        dvc_record(
            &mut quorum,
            dvc(0, 1, 400, 200, suffix_all_present(clamped.clone())),
        );
        dvc_record(
            &mut quorum,
            dvc(1, 1, 400, 200, suffix_all_present(clamped)),
        );

        // A ceiling wide enough not to bind, so this asserts the floor rule alone. In
        // production the ceiling is `PREPARE_QUEUE_CEILING`, which already puts
        // `commit_max` at or above any clamped floor.
        assert_eq!(
            merge_commit_max(&quorum, 1000),
            200,
            "a clamped window floor is a scan bound, not a proven commit point"
        );
    }

    #[test]
    fn given_a_sender_at_commit_zero_when_merging_should_not_commit_op_one() {
        // The other floor-raising path: ops are 1-based, so a sender with nothing
        // committed still floors its window at op 1. Every sender says commit 0, so
        // treating op 1 as committed would ack a client for an unheld op.
        let mut quorum = dvc_quorum_array_empty();
        let floored = suffix_headers(1, 1, 1);
        dvc_record(
            &mut quorum,
            dvc(0, 1, 1, 0, suffix_all_present(floored.clone())),
        );
        dvc_record(&mut quorum, dvc(1, 1, 1, 0, suffix_all_present(floored)));

        assert_eq!(
            merge_commit_max(&quorum, 32),
            0,
            "nothing is committed, so the merge must prove nothing committed"
        );
    }

    #[test]
    fn given_disagreeing_committed_elsewhere_headers_should_refuse_the_view() {
        // These headers install unconditionally, with no nack quorum behind them, so
        // first-wins is the defect `canonical_header_at` refuses for the canonical
        // range: only one of two committed claims can be true.
        let mut quorum = dvc_quorum_array_empty();
        // Canonical sender at the higher log_view. Its window starts at the proven
        // commit point, so ops below it come only from a stale sender.
        dvc_record(
            &mut quorum,
            dvc(0, 2, 6, 5, suffix_all_present(suffix_headers(5, 6, 2))),
        );
        // Two senders behind on log_view but level on op, so no nack and no conflict
        // inside the canonical range. They disagree only at op 3, which both report
        // committed; `prepare` derives the checksum from `(op, view)`.
        dvc_record(
            &mut quorum,
            dvc(1, 1, 6, 5, suffix_all_present(suffix_headers(3, 6, 2))),
        );
        let mut divergent = suffix_headers(3, 6, 2);
        *divergent.last_mut().expect("suffix is non-empty") = prepare(3, 5);
        dvc_record(&mut quorum, dvc(2, 1, 6, 5, suffix_all_present(divergent)));

        assert!(
            matches!(
                merge_dvc_quorum(&quorum, quorums_r3()),
                MergeOutcome::Deadlocked { .. }
            ),
            "two committed claims at one op must refuse the view, not pick one"
        );
    }

    #[test]
    fn given_head_header_claiming_a_higher_commit_should_raise_commit_max() {
        // The head prepare's `commit` was stamped by the primary that prepared it, so
        // it proves a commit point even when every sender's own tracking lags.
        // Without it the merge rescans committed ops and could accept nacks for them.
        let mut quorum = dvc_quorum_array_empty();
        let mut headers = suffix_headers(2, 4, 1);
        headers[0].commit = 3;
        dvc_record(
            &mut quorum,
            dvc(0, 1, 4, 2, suffix_all_present(headers.clone())),
        );
        dvc_record(&mut quorum, dvc(1, 1, 4, 2, suffix_all_present(headers)));

        assert_eq!(
            merge_commit_max(&quorum, 32),
            3,
            "the head header's commit field is a commit_max lower bound"
        );
    }
}
