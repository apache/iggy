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

//! Cross-replica committed-log equality.
//!
//! The per-tick checks in [`super::invariants`] catch a single replica
//! contradicting itself, and [`super::oracle`] compares committed metadata against
//! the workload's shadow. Neither compares replicas to EACH OTHER, which is the
//! actual consensus property: two replicas that both committed op N must have
//! committed the same op N.
//!
//! Keeps one canonical commit chain and asserts every replica agrees with it
//! wherever they overlap, in BOTH directions of `(commit_a == commit_b) ==
//! (checksum_a == checksum_b)`: same op implies same prepare, and same prepare
//! implies same op, the second of which catches one prepare committing at two log
//! positions. Also asserts the chain is hash-linked (`header_b.parent ==
//! checksum_a`). Recording which replicas reached each op keeps the check provably
//! non-vacuous: a chain nothing was compared against passes silently.

use crate::Simulator;
use consensus::MetadataHandle;
use iggy_binary_protocol::PrepareHeader;
use journal::Journal;
use std::collections::{BTreeMap, BTreeSet};

/// One op of the canonical committed chain.
#[derive(Debug)]
struct CanonicalCommit {
    /// Identity of the prepare committed at this op. Two replicas disagreeing here
    /// is a divergence: the same log position holds different history.
    ///
    /// The hash link is checked against this rather than a stored `parent`: an
    /// arriving header's `parent` must equal the canonical previous op's `checksum`,
    /// so keeping each entry's own parent would record a value nothing reads.
    checksum: u128,
    /// Replicas observed committing this op, so the check can prove it compared
    /// something rather than passing over an empty chain.
    replicas: BTreeSet<u8>,
}

/// Canonical committed metadata chain, accumulated across ticks.
#[derive(Debug, Default)]
pub struct StateChecker {
    commits: BTreeMap<u64, CanonicalCommit>,
    /// Op each prepare checksum was committed at, the reverse half of `(commit_a ==
    /// commit_b) == (checksum_a == checksum_b)`. `commits` alone gives only the
    /// forward half (same op implies same checksum); without this, the same prepare
    /// appearing at two log positions (a duplicate apply, a misnumbered replay) is
    /// invisible.
    ops_by_checksum: BTreeMap<u128, u64>,
    /// Each replica's commit point as of the last check, so a tick only walks what
    /// is new.
    ///
    /// LOWERED when a replica's commit point drops, which a restart does: the point
    /// is recovered from a lower bound (`SimJournal::recovery_commit_watermark`), so
    /// it re-commits a range it already reported. A high-water mark here would
    /// compare those re-commits against nothing, exactly the syncing case this check
    /// exists for. The cost is re-walking a recovering replica's prefix, bounded by
    /// its own commit point.
    verified_upto: BTreeMap<u8, u64>,
    /// Each replica's metadata incarnation as of the last check.
    ///
    /// A change means everything below the commit point was rebuilt from disk, so it
    /// is not the bytes already compared. Lowering the mark alone misses this: a
    /// replica recovering the SAME point leaves an empty range to walk.
    incarnations: BTreeMap<u8, u128>,
}

impl StateChecker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold every live replica's newly committed metadata ops into the canonical
    /// chain, asserting agreement.
    ///
    /// Committed state only (ops at or below `commit_min`), so a prepare still in
    /// flight is never compared: replicas may disagree about uncommitted tails, and
    /// that is what a view change resolves. Crashed replicas are skipped rather than
    /// dropped, keeping their mark, so a restart re-verifies only what it commits
    /// anew.
    ///
    /// # Panics
    /// On any disagreement about a committed op, or a broken hash chain. The
    /// message names both replicas and the op, and the seed replays the run.
    pub fn check(&mut self, sim: &Simulator, seed: u64) {
        for replica_idx in 0..sim.replica_count {
            if sim.is_crashed(replica_idx) {
                continue;
            }
            let replica = &sim.replicas[usize::from(replica_idx)];
            let Some(consensus) = replica.shards[0].plane.metadata().consensus.as_ref() else {
                continue;
            };
            let committed = consensus.commit_min();
            // Re-walk from the snapshot floor, which bounds the cost: below it the
            // state came from the snapshot and has no header to compare.
            let restarted = self
                .incarnations
                .insert(replica_idx, replica.metadata_incarnation)
                .is_some_and(|previous| previous != replica.metadata_incarnation);
            if restarted {
                self.verified_upto
                    .insert(replica_idx, replica.metadata_journal.snapshot_op());
            }
            let verified = self.verified_upto.get(&replica_idx).copied().unwrap_or(0);
            for op in (verified + 1)..=committed {
                self.verify(replica_idx, replica, op, committed, seed);
            }
            // Again, even when the loop already walked it. A replica whose point does
            // not move is otherwise never compared, while what it holds there can
            // change under it (recovery, repair, transfer install).
            // re-derives the checksum at `commit_min` every tick for this reason.
            if committed > 0 {
                self.verify(replica_idx, replica, committed, committed, seed);
            }
            // Recorded verbatim, NOT maxed: see the field doc.
            self.verified_upto.insert(replica_idx, committed);
        }
    }

    /// Fold one replica's op into the canonical chain, or prove its absence
    /// legitimate.
    fn verify(
        &mut self,
        replica_idx: u8,
        replica: &crate::SimReplica,
        op: u64,
        committed: u64,
        seed: u64,
    ) {
        let Some(header) = journaled_header(replica, op) else {
            assert!(
                absent_header_is_legitimate(replica, op),
                "replica {replica_idx} reports op {op} committed (point {committed}) \
                 but holds no header for it above its snapshot floor {}: a hole in the \
                 committed log (seed={seed:#x})",
                replica.metadata_journal.snapshot_op(),
            );
            return;
        };
        self.record(replica_idx, op, &header, seed);
    }

    /// Number of ops in the canonical chain. Tests assert this is non-zero, so a
    /// green run cannot mean "never compared anything".
    #[must_use]
    pub fn chain_len(&self) -> usize {
        self.commits.len()
    }

    /// Ops witnessed by more than one replica, the only ones that exercised the
    /// equality property: an op seen on a single replica was recorded, not compared.
    #[must_use]
    pub fn ops_compared(&self) -> usize {
        self.commits
            .values()
            .filter(|commit| commit.replicas.len() > 1)
            .count()
    }

    fn record(&mut self, replica_idx: u8, op: u64, header: &PrepareHeader, seed: u64) {
        // Hash-chain link, checked before the identity comparison so a diverged
        // prefix is reported at the op where the chains part rather than at the
        // first op whose contents happen to differ.
        if let Some(previous) = self.commits.get(&(op - 1))
            && header.parent != previous.checksum
        {
            panic!(
                "replica {replica_idx} committed op {op} whose parent {:#x} is not the \
                 canonical op {} checksum {:#x}: its committed history forked below this \
                 op (seed={seed:#x})",
                header.parent,
                op - 1,
                previous.checksum,
            );
        }
        // The reverse half: one prepare, one log position. A checksum turning up at
        // a second op means the same prepare committed twice, which per-op agreement
        // alone would never show.
        match self.ops_by_checksum.get(&header.checksum) {
            Some(&previous_op) => assert_eq!(
                previous_op, op,
                "replica {replica_idx} committed the prepare with checksum {:#x} at op \
                 {op}, but it is already the canonical commit at op {previous_op}: one \
                 prepare committed at two log positions (seed={seed:#x})",
                header.checksum,
            ),
            None => {
                self.ops_by_checksum.insert(header.checksum, op);
            }
        }
        match self.commits.get_mut(&op) {
            Some(canonical) => {
                assert_eq!(
                    canonical.checksum, header.checksum,
                    "replicas disagree on committed op {op}: canonical checksum {:#x} \
                     (committed by {:?}) vs replica {replica_idx}'s {:#x}. Two replicas \
                     committed different history at the same log position (seed={seed:#x})",
                    canonical.checksum, canonical.replicas, header.checksum,
                );
                canonical.replicas.insert(replica_idx);
            }
            None => {
                self.commits.insert(
                    op,
                    CanonicalCommit {
                        checksum: header.checksum,
                        replicas: BTreeSet::from([replica_idx]),
                    },
                );
            }
        }
    }
}

/// Assert every live replica's committed metadata prefix agrees, op for op.
///
/// The quiesce-time counterpart to [`StateChecker::check`]: that one folds ops in
/// as they commit and compares whatever overlaps, while this walks the full
/// committed prefix of every live replica at rest and requires the shorter to be a
/// genuine PREFIX of the longer. A replica may trail, having missed the last commit
/// broadcast, but where it committed anything it must match contiguously.
///
/// Returns how many ops were witnessed by MORE THAN ONE replica, i.e. how many
/// exercised the property. Zero means this proved nothing, so a caller that wants
/// the check to mean something asserts on it: a walk that compared nothing passes
/// exactly like one that compared everything.
///
/// # Panics
/// If two live replicas disagree on any committed op, or a replica's committed
/// prefix has a hole in it.
#[must_use]
pub fn assert_committed_prefixes_agree(sim: &Simulator, seed: u64) -> usize {
    let mut canonical: BTreeMap<u64, (u128, u8)> = BTreeMap::new();
    let mut witnesses: BTreeMap<u64, usize> = BTreeMap::new();
    for replica_idx in 0..sim.replica_count {
        if sim.is_crashed(replica_idx) {
            continue;
        }
        let replica = &sim.replicas[usize::from(replica_idx)];
        let Some(consensus) = replica.shards[0].plane.metadata().consensus.as_ref() else {
            continue;
        };
        let committed = consensus.commit_min();
        for op in 1..=committed {
            let Some(header) = journaled_header(replica, op) else {
                // A PREFIX is what this claims to compare, so a hole cannot be
                // stepped over: skipping it would let a replica missing ops 5..9
                // pass by agreeing on 1..4 and 10.., the shape a bad repair leaves.
                // The one legitimate absence is an op dropped under the snapshot
                // floor.
                assert!(
                    absent_header_is_legitimate(replica, op),
                    "at quiesce replica {replica_idx} reports op {op} committed but holds \
                     no header for it (snapshot floor {}, commit point {committed}): its \
                     committed prefix has a hole (seed={seed:#x})",
                    replica.metadata_journal.snapshot_op(),
                );
                continue;
            };
            if let Some(&(checksum, owner)) = canonical.get(&op) {
                assert_eq!(
                    checksum, header.checksum,
                    "at quiesce replica {replica_idx} and replica {owner} disagree on \
                     committed metadata op {op}: {:#x} vs {checksum:#x} (seed={seed:#x})",
                    header.checksum,
                );
                *witnesses.entry(op).or_insert(1) += 1;
            } else {
                canonical.insert(op, (header.checksum, replica_idx));
                witnesses.insert(op, 1);
            }
        }
    }
    witnesses.values().filter(|&&count| count > 1).count()
}

/// Whether a committed op having no journal header is legitimate rather than a
/// hole.
///
/// One case only: the journal dropped it under its own snapshot floor, which a
/// checkpoint does.
///
/// The commit point itself used to be exempt too, on the grounds that
/// `recovery_commit_watermark` is a lower bound. Unnecessary and unsafe: a checkpoint
/// drains `0..=snapshot_op - 1`, retaining the commit-point header for view-change
/// merging (`checkpoint_drain_retains_the_commit_point_header`), so on a healthy
/// replica it is always there and the exemption only hid a missing committed head.
fn absent_header_is_legitimate(replica: &crate::SimReplica, op: u64) -> bool {
    op <= replica.metadata_journal.snapshot_op()
}

/// The header a replica has journaled at `op`, if any.
///
/// Reads shard 0's retained metadata WAL, which is where the committed metadata
/// log lives; the journal is harness-owned so this also works across a restart.
fn journaled_header(replica: &crate::SimReplica, op: u64) -> Option<PrepareHeader> {
    let slot = usize::try_from(op).ok()?;
    replica.metadata_journal.header(slot).copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::SimClient;
    use crate::packet::PacketSimulatorOptions;

    const SEED: u64 = 0x5C11;

    /// A three-replica cluster with six committed metadata ops behind it.
    fn cluster_with_committed_ops() -> Simulator {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let client_id: u128 = 1;
        let mut sim = Simulator::new(
            3,
            std::iter::once(client_id),
            PacketSimulatorOptions {
                node_count: 3,
                client_count: 1,
                seed: SEED,
                ..PacketSimulatorOptions::default()
            },
        );
        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);
        for sequence in 0..6u32 {
            let msg = client.create_stream(&format!("wl-chk-{sequence}"));
            sim.submit_request(client_id, 0, msg.into_generic());
            for _ in 0..40 {
                sim.step();
            }
        }
        sim
    }

    /// A committed op with no header is a hole, including at the commit point, which
    /// is the one op a checkpoint deliberately retains.
    #[test]
    #[should_panic(expected = "a hole in the committed log")]
    fn a_missing_committed_head_above_the_snapshot_floor_is_a_hole() {
        let sim = cluster_with_committed_ops();
        let committed = sim.replicas[1].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 owns metadata consensus")
            .commit_min();
        assert!(
            committed > sim.replicas[1].metadata_journal.snapshot_op(),
            "the commit point must sit above the snapshot floor or this test is vacuous"
        );
        assert!(
            sim.replicas[1].metadata_journal.forget_op(committed),
            "the head this test removes must have been there"
        );
        StateChecker::new().check(&sim, SEED);
    }

    /// A hole punched below a replica's verified mark is caught after it restarts.
    ///
    /// The mark survives a crash, so without the incarnation reset the walk begins
    /// past the damage. The hole stands in for any way a rebuilt prefix can differ
    /// from the one already compared.
    #[test]
    #[should_panic(expected = "a hole in the committed log")]
    fn a_damaged_prefix_is_rechecked_after_a_restart() {
        let mut sim = cluster_with_committed_ops();
        let mut checker = StateChecker::new();
        checker.check(&sim, SEED);
        assert!(
            checker.chain_len() > 2,
            "the checker verified nothing, so the mark it carries into the restart \
             proves nothing"
        );

        sim.replica_crash(1);
        sim.replica_restart(1);
        for _ in 0..200 {
            sim.step();
        }
        assert!(
            sim.replicas[1].metadata_journal.forget_op(2),
            "op 2 must be journaled for this test to damage anything"
        );
        checker.check(&sim, SEED);
    }

    /// An undamaged prefix passes the recheck: the reset must turn a restart into a
    /// comparison, not into a failure.
    #[test]
    fn an_undamaged_prefix_passes_the_recheck_after_a_restart() {
        let mut sim = cluster_with_committed_ops();
        let mut checker = StateChecker::new();
        checker.check(&sim, SEED);
        let before = checker.chain_len();

        sim.replica_crash(1);
        sim.replica_restart(1);
        for _ in 0..400 {
            sim.step();
        }
        checker.check(&sim, SEED);
        assert!(
            checker.chain_len() >= before,
            "the canonical chain shrank across a restart"
        );
        assert!(
            checker.ops_compared() > 0,
            "no op was witnessed by more than one replica, so the recheck compared \
             nothing"
        );
    }
}
