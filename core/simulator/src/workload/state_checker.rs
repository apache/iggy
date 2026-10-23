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
//! Keeps a canonical metadata chain and a partition chain per incarnation.
//! The partition chains survive fixture deletion, so a lagging replica is checked
//! against commits witnessed before its peers replaced that incarnation.
//!
//! The metadata chain asserts every replica agrees with it
//! wherever they overlap, in BOTH directions of `(commit_a == commit_b) ==
//! (checksum_a == checksum_b)`: same op implies same prepare, and same prepare
//! implies same op, the second of which catches one prepare committing at two log
//! positions. Also asserts the chain is hash-linked (`header_b.parent ==
//! checksum_a`). Recording which replicas reached each op keeps the check provably
//! non-vacuous: a chain nothing was compared against passes silently.
//!
//! A partition prepare's identity leaves its message batch out, so
//! [`assert_applied_data_agrees`] compares the committed batches themselves.

use crate::Simulator;
use consensus::PartitionsHandle;
use iggy_binary_protocol::primitives::consumer::KIND_CONSUMER_GROUP;
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
use iggy_binary_protocol::requests::consumer_offsets::{
    GetConsumerOffsetRequest, split_prepared_offset,
};
use iggy_binary_protocol::requests::partitions::{
    InstallConsumerGroupOwnerRequest, RetireConsumerGroupOwnersRequest,
};
use iggy_binary_protocol::{HEADER_SIZE, Operation, PrepareHeader, WireDecode};
use journal::Journal;
use message_bus::is_auto_commit_client;
use server_common::send_messages::decode_prepare_slice_trusted;
use server_common::sharding::IggyNamespace;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::hash::{DefaultHasher, Hasher};

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
    /// Replicas observed committing this op, each against the replica boot incarnation
    /// it held at the time, so the check can prove it compared something rather
    /// than passing over an empty chain.
    ///
    /// The incarnation is what separates two failure modes the checksum alone
    /// reports identically: two replicas holding different history at one op, and
    /// ONE replica reporting a different header there after a restart. The second
    /// means it applied one entry and recovered another, which is narrower and
    /// wants naming as such.
    witnesses: BTreeMap<u8, u128>,
}

#[derive(Debug)]
struct PartitionProgress {
    incarnation: u64,
    replica_incarnation: u128,
    committed: u64,
    owners: PartitionOwners,
}

#[derive(Debug, Default)]
struct PartitionOwners {
    installed: BTreeMap<u64, ConsumerGroupOwner>,
    retirement: Option<RetireConsumerGroupOwnersRequest>,
}

/// Canonical committed metadata and partition chains, accumulated across ticks.
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
    partition_commits: BTreeMap<(IggyNamespace, u64, u64), CanonicalCommit>,
    partition_verified_upto: BTreeMap<(u8, IggyNamespace), PartitionProgress>,
    /// Canonical partition commits that retire consumer group owners.
    owner_retirements_committed: usize,
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
            let Some(consensus) = sim.metadata_consensus(usize::from(replica_idx)) else {
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
            // A state transfer raises the floor without a restart and keeps the
            // residents below it, one of which can be a prepare a view change
            // discarded. The snapshot superseded them, so the walk starts at the floor
            // here too.
            let verified = self
                .verified_upto
                .get(&replica_idx)
                .copied()
                .unwrap_or(0)
                .max(replica.metadata_journal.snapshot_op());
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

    /// Compare a live replica's new partition commits, retaining earlier
    /// incarnations after their journals disappear. Missing headers are allowed
    /// because the partition repair ring evicts its oldest committed entries.
    ///
    /// # Panics
    /// If one partition incarnation commits different entries at the same op.
    pub(crate) fn check_partition(
        &mut self,
        sim: &Simulator,
        replica_idx: u8,
        namespace: IggyNamespace,
        seed: u64,
    ) {
        let idx = usize::from(replica_idx);
        let (Some(state), Some(incarnation)) = (
            sim.partition_consensus_state(idx, namespace),
            sim.partition_incarnation(idx, namespace),
        ) else {
            self.partition_verified_upto
                .remove(&(replica_idx, namespace));
            return;
        };
        let replica_incarnation = sim.replicas[idx].metadata_incarnation;
        let previous = self
            .partition_verified_upto
            .remove(&(replica_idx, namespace));
        // Recheck the head even without progress, and the retained prefix after a
        // restart, replacement or lower recovered commit point.
        let previous = previous.filter(|previous| {
            previous.incarnation == incarnation
                && previous.replica_incarnation == replica_incarnation
                && previous.committed <= state.commit_min
        });
        let first = previous
            .as_ref()
            .map_or(1, |previous| previous.committed.max(1));
        let mut progress = PartitionProgress {
            incarnation,
            replica_incarnation,
            committed: state.commit_min,
            owners: previous.map_or_else(PartitionOwners::default, |previous| previous.owners),
        };
        let Some(headers) =
            sim.partition_journaled_headers(idx, namespace, first..=state.commit_min)
        else {
            return;
        };
        for (op, header) in headers {
            let identity = header.identity_checksum();
            if header.operation == Operation::RetireConsumerGroupOwners
                && !self
                    .partition_commits
                    .contains_key(&(namespace, incarnation, op))
            {
                self.owner_retirements_committed += 1;
            }
            let canonical = self
                .partition_commits
                .entry((namespace, incarnation, op))
                .or_insert_with(|| CanonicalCommit {
                    checksum: identity,
                    witnesses: BTreeMap::new(),
                });
            assert_eq!(
                identity, canonical.checksum,
                "replica {replica_idx} and witnesses {:?} disagree on committed partition \
                 op {op} of ns {namespace:?} incarnation {incarnation}: \
                 {identity:#x} vs {:#x} (seed={seed:#x})",
                canonical.witnesses, canonical.checksum,
            );
            canonical.witnesses.insert(replica_idx, replica_incarnation);
            if matches!(
                header.operation,
                Operation::InstallConsumerGroupOwner
                    | Operation::RetireConsumerGroupOwners
                    | Operation::StoreConsumerOffset
                    | Operation::DeleteConsumerOffset
            ) {
                let partition = sim.replicas[idx]
                    .partition_shard(namespace)
                    .plane
                    .partitions()
                    .get_by_ns(&namespace)
                    .expect("a journaled partition is hosted");
                let prepare = partition
                    .log
                    .journal()
                    .inner
                    .repair_entry(op)
                    .expect("a retained prepare header has its body");
                check_partition_owner(
                    &mut progress.owners,
                    &header,
                    &prepare.as_slice()[HEADER_SIZE..],
                    seed,
                );
            }
        }
        self.partition_verified_upto
            .insert((replica_idx, namespace), progress);
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
        self.record(
            replica_idx,
            op,
            &header,
            replica.metadata_incarnation,
            committed,
            seed,
        );
    }

    /// Number of ops in the canonical chain. Tests assert this is non-zero, so a
    /// green run cannot mean "never compared anything".
    #[must_use]
    pub fn chain_len(&self) -> usize {
        self.commits.len()
    }

    /// Committed `RetireConsumerGroupOwners` ops, each counted once however many
    /// replicas committed it.
    #[must_use]
    pub const fn owner_retirements_committed(&self) -> usize {
        self.owner_retirements_committed
    }

    /// Ops witnessed by more than one replica, the only ones that exercised the
    /// equality property: an op seen on a single replica was recorded, not compared.
    #[must_use]
    pub fn ops_compared(&self) -> usize {
        self.commits
            .values()
            .filter(|commit| commit.witnesses.len() > 1)
            .count()
    }

    fn record(
        &mut self,
        replica_idx: u8,
        op: u64,
        header: &PrepareHeader,
        incarnation: u128,
        committed: u64,
        seed: u64,
    ) {
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
                // Named separately because the fix differs: a replica disagreeing
                // with its own earlier incarnation applied one entry and recovered
                // another, and no other replica has to be involved.
                let restarted_since = canonical
                    .witnesses
                    .get(&replica_idx)
                    .is_some_and(|&recorded| recorded != incarnation);
                assert!(
                    canonical.checksum == header.checksum,
                    "{} on committed op {op}: canonical checksum {:#x} (witnesses \
                     {:?}) vs replica {replica_idx}'s {:#x} at incarnation \
                     {incarnation}, commit point {committed} (seed={seed:#x})",
                    if restarted_since {
                        "one replica disagrees with its own pre-restart history"
                    } else {
                        "replicas committed different history at the same log position"
                    },
                    canonical.checksum,
                    canonical.witnesses,
                    header.checksum,
                );
                canonical.witnesses.insert(replica_idx, incarnation);
            }
            None => {
                self.commits.insert(
                    op,
                    CanonicalCommit {
                        checksum: header.checksum,
                        witnesses: BTreeMap::from([(replica_idx, incarnation)]),
                    },
                );
            }
        }
    }
}

/// Membership is metadata state; the partition's ordered install barrier is
/// what fences a departed member's later offset writes.
fn check_partition_owner(
    owners: &mut PartitionOwners,
    header: &PrepareHeader,
    body: &[u8],
    seed: u64,
) {
    if header.operation == Operation::InstallConsumerGroupOwner {
        let install = InstallConsumerGroupOwnerRequest::decode_from(body)
            .expect("a committed owner installation decodes");
        if !owners
            .retirement
            .as_ref()
            .is_some_and(|catalog| catalog.retires(install.group_id))
        {
            owners.installed.insert(install.group_id, install.owner);
        }
        return;
    }
    if header.operation == Operation::RetireConsumerGroupOwners {
        let mut catalog = RetireConsumerGroupOwnersRequest::decode_from(body)
            .expect("a committed owner retirement decodes");
        if let Some(previous) = &owners.retirement {
            if catalog.metadata_op < previous.metadata_op {
                return;
            }
            catalog
                .live_group_ids
                .retain(|group| !previous.retires(*group));
        }
        owners.installed.retain(|group, _| !catalog.retires(*group));
        owners.retirement = Some(catalog);
        return;
    }
    let (request, generation) =
        split_prepared_offset(body).expect("a committed offset has its owner token");
    let (request, _) =
        GetConsumerOffsetRequest::decode(request).expect("a committed offset prefix decodes");
    if request.consumer.kind != KIND_CONSUMER_GROUP {
        return;
    }
    let group_id = u64::from(
        request
            .consumer
            .id
            .as_u32()
            .expect("a prepared group id is numeric"),
    );
    let auto_commit = is_auto_commit_client(header.client);
    let cleanup = auto_commit && header.operation == Operation::DeleteConsumerOffset;
    if owners
        .retirement
        .as_ref()
        .is_some_and(|catalog| catalog.retires(group_id))
    {
        assert!(
            cleanup,
            "group offset committed past its retirement fence: group={group_id}, op={}, seed={seed:#x}",
            header.op,
        );
        return;
    }
    // The repair ring may have evicted the install before this checker started.
    if let Some(owner) = owners.installed.get(&group_id) {
        assert!(
            (!owner.is_unassigned() || cleanup)
                && (auto_commit
                    || (owner.client_id, owner.session) == (header.client, header.session))
                && owner.generation == generation,
            "group offset committed past its owner fence: group={group_id}, owner={owner:?}, \
             client={}, session={}, generation={generation}, op={}, seed={seed:#x}",
            header.client,
            header.session,
            header.op,
        );
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
        let Some(consensus) = sim.metadata_consensus(usize::from(replica_idx)) else {
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

/// Assert every live replica's committed partition prefix agrees, op for op.
///
/// Partition-plane counterpart of [`assert_committed_prefixes_agree`]. Unlike the
/// incremental per-tick check, this rechecks the entire retained prefix.
///
/// Two differences from the metadata twin, both from the partition journal evicting
/// its committed prefix as it flushes to segments:
///
/// * A missing header is ordinary, not a hole, so this compares only the ops two
///   replicas both still hold. `evict_prefix` moves a flushed entry to the repair
///   ring rather than dropping it, so that set is the ring plus whatever is still
///   resident: the recently committed tail, where a bad repair or a mis-decided
///   view change lands.
/// * Identity, not the sealed checksum. `restamp_prepare_view` rewrites the view on
///   retransmit, so `identity_checksum` compares the entry, not the delivery.
///
/// Replicas are compared only on one partition incarnation: a recreated topic
/// keeps its namespaces and starts a new log under them.
///
/// Returns ops witnessed by more than one replica, summed over every namespace.
///
/// # Panics
/// If two live replicas hold different entries at the same committed partition op.
#[must_use]
pub fn assert_partition_prefixes_agree(
    sim: &Simulator,
    namespaces: &[IggyNamespace],
    seed: u64,
) -> usize {
    let mut compared = 0;
    for &namespace in namespaces {
        let mut canonical: BTreeMap<(u64, u64), (u128, u8)> = BTreeMap::new();
        let mut witnesses: BTreeMap<(u64, u64), usize> = BTreeMap::new();
        for replica_idx in 0..sim.replica_count {
            if sim.is_crashed(replica_idx) {
                continue;
            }
            let idx = usize::from(replica_idx);
            let (Some(state), Some(incarnation)) = (
                sim.partition_consensus_state(idx, namespace),
                sim.partition_incarnation(idx, namespace),
            ) else {
                continue;
            };
            let Some(headers) =
                sim.partition_journaled_headers(idx, namespace, 1..=state.commit_min)
            else {
                continue;
            };
            for (op, header) in headers {
                let identity = header.identity_checksum();
                if let Some(&(expected, owner)) = canonical.get(&(incarnation, op)) {
                    assert_eq!(
                        identity, expected,
                        "replica {replica_idx} and replica {owner} disagree on \
                         committed partition op {op} of ns {namespace:?} incarnation \
                         {incarnation}: {identity:#x} vs {expected:#x} (seed={seed:#x})",
                    );
                    *witnesses.entry((incarnation, op)).or_insert(1) += 1;
                } else {
                    canonical.insert((incarnation, op), (identity, replica_idx));
                    witnesses.insert((incarnation, op), 1);
                }
            }
        }
        compared += witnesses.values().filter(|&&count| count > 1).count();
    }
    compared
}

/// What [`assert_applied_data_agrees`] compared.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AppliedDataReport {
    /// Namespaces in which at least one committed batch was compared.
    pub namespaces: usize,
    /// Messages in the compared batches, each batch counted once however many
    /// replicas hold it.
    pub records: usize,
}

/// One committed batch as the first replica that holds it applied it.
struct AppliedBatch {
    replica: u8,
    digest: u64,
    base_offset: u64,
    messages: u32,
    witnesses: usize,
}

/// Assert live replicas that committed the same `SendMessages` prepare hold the
/// same applied message data.
///
/// The data twin of [`assert_partition_prefixes_agree`]. A `SendMessages` prepare
/// carries a zero `checksum_body`, so its identity leaves the message batch out,
/// and two replicas can agree on every identity and offset while serving
/// different messages. This hashes the batch each replica retains for a committed
/// op, the bytes a poll reads back and a flush writes, and compares the digests.
///
/// Keyed by incarnation, op and prepare identity, so only replicas that committed
/// the exact same prepare in the same history are compared; an identity
/// divergence is the prefix check's to report. Each replica contributes the ops
/// up to its own `commit_min` that its journal still retains, so the ops compared
/// are the ones at or below the common commit point that both replicas hold.
///
/// # Panics
/// If two live replicas hold different data for one committed prepare, or a
/// committed batch does not decode.
#[must_use]
pub fn assert_applied_data_agrees(
    sim: &Simulator,
    namespaces: &[IggyNamespace],
    seed: u64,
) -> AppliedDataReport {
    let mut report = AppliedDataReport::default();
    for &namespace in namespaces {
        let mut canonical: BTreeMap<(u64, u64, u128), AppliedBatch> = BTreeMap::new();
        for replica_idx in 0..sim.replica_count {
            if sim.is_crashed(replica_idx) {
                continue;
            }
            let Some(partition) = sim.replicas[usize::from(replica_idx)]
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_by_ns(&namespace)
            else {
                continue;
            };
            let incarnation = partition.created_revision();
            let journal = &partition.log.journal().inner;
            for (op, header) in journal.repair_headers_in(1..=partition.consensus().commit_min()) {
                if header.operation != Operation::SendMessages {
                    continue;
                }
                let Some(entry) = journal.repair_entry(op) else {
                    continue;
                };
                let batch = applied_batch(replica_idx, op, entry.as_slice(), seed);
                match canonical.entry((incarnation, op, header.identity_checksum())) {
                    Entry::Vacant(slot) => {
                        slot.insert(batch);
                    }
                    Entry::Occupied(mut slot) => {
                        let expected = slot.get_mut();
                        assert_eq!(
                            batch.digest,
                            expected.digest,
                            "replicas {replica_idx} and {} committed the same prepare at partition \
                             op {op} of ns {namespace:?} incarnation {incarnation} but hold \
                             different applied message data: digest {:#x} vs {:#x}, base offset \
                             {} vs {}, {} vs {} messages (seed={seed:#x})",
                            expected.replica,
                            batch.digest,
                            expected.digest,
                            batch.base_offset,
                            expected.base_offset,
                            batch.messages,
                            expected.messages,
                        );
                        expected.witnesses += 1;
                    }
                }
            }
        }
        let records: usize = canonical
            .values()
            .filter(|batch| batch.witnesses > 1)
            .map(|batch| batch.messages as usize)
            .sum();
        if records > 0 {
            report.namespaces += 1;
            report.records += records;
        }
    }
    report
}

/// Digest of the batch body a committed `SendMessages` entry carries, the batch
/// header included because it holds the stamped offsets and timestamp.
fn applied_batch(replica: u8, op: u64, entry: &[u8], seed: u64) -> AppliedBatch {
    let batch = decode_prepare_slice_trusted(entry).unwrap_or_else(|error| {
        panic!(
            "replica {replica} holds committed partition op {op} whose batch does not \
             decode: {error} (seed={seed:#x})"
        )
    });
    let mut hasher = DefaultHasher::new();
    hasher.write(&entry[HEADER_SIZE..HEADER_SIZE + batch.header.total_size()]);
    AppliedBatch {
        replica,
        digest: hasher.finish(),
        base_offset: batch.header.base_offset,
        messages: batch.message_count(),
        witnesses: 1,
    }
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
    use crate::packet::{Packet, PacketSimulatorOptions, ProcessId};
    use crate::workload::Workload;
    use crate::workload::invariants::Invariants;
    use crate::workload::options::WorkloadOptions;
    use iggy_binary_protocol::requests::consumer_offsets::{
        DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
    };
    use iggy_binary_protocol::{AckLevel, WireConsumer, WireEncode, WireIdentifier};
    use message_bus::AUTO_COMMIT_CLIENT_ID;

    const SEED: u64 = 0x5C11;

    #[test]
    #[should_panic(expected = "group offset committed past its owner fence")]
    fn given_a_successor_install_when_the_departed_owner_commits_should_fail_the_oracle() {
        let mut owners = PartitionOwners::default();
        let original = ConsumerGroupOwner {
            client_id: 1,
            session: 2,
            generation: 3,
        };
        let successor = ConsumerGroupOwner {
            client_id: 4,
            session: 5,
            generation: 6,
        };
        let request = StoreConsumerOffsetRequest {
            consumer: WireConsumer::consumer_group(WireIdentifier::numeric(0)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: Some(0),
            offset: 0,
            ack: AckLevel::Quorum,
        };
        let mut body = request.to_bytes().to_vec();
        body.extend_from_slice(&original.generation.to_le_bytes());
        for (op, owner) in [(1, original), (3, successor)] {
            let install = InstallConsumerGroupOwnerRequest {
                incarnation: 1,
                group_id: 0,
                owner,
                metadata_op: op,
            };
            check_partition_owner(
                &mut owners,
                &PrepareHeader {
                    operation: Operation::InstallConsumerGroupOwner,
                    op,
                    ..Default::default()
                },
                &install.to_bytes(),
                SEED,
            );
            check_partition_owner(
                &mut owners,
                &PrepareHeader {
                    operation: Operation::StoreConsumerOffset,
                    client: original.client_id,
                    session: original.session,
                    op: op + 1,
                    ..Default::default()
                },
                &body,
                SEED,
            );
        }
    }

    #[test]
    #[should_panic(expected = "group offset committed past its owner fence")]
    fn given_an_auto_commit_with_a_stale_generation_when_committed_should_fail_the_oracle() {
        let mut owners = installed_owner();
        check_offset(
            &mut owners,
            AUTO_COMMIT_CLIENT_ID,
            0,
            6,
            Operation::StoreConsumerOffset,
        );
    }

    #[test]
    #[should_panic(expected = "group offset committed past its owner fence")]
    fn given_an_offset_with_a_stale_session_when_committed_should_fail_the_oracle() {
        let mut owners = installed_owner();
        check_offset(&mut owners, 1, 3, 7, Operation::StoreConsumerOffset);
    }

    #[test]
    #[should_panic(expected = "group offset committed past its retirement fence")]
    fn given_a_retired_group_when_an_auto_commit_store_commits_should_fail_the_oracle() {
        let mut owners = retired_owner();
        check_offset(
            &mut owners,
            AUTO_COMMIT_CLIENT_ID,
            0,
            7,
            Operation::StoreConsumerOffset,
        );
    }

    #[test]
    fn given_an_auto_commit_when_current_or_cleaning_retired_offsets_should_pass_the_oracle() {
        let mut owners = installed_owner();
        check_offset(
            &mut owners,
            AUTO_COMMIT_CLIENT_ID,
            0,
            7,
            Operation::StoreConsumerOffset,
        );
        let mut owners = retired_owner();
        check_offset(
            &mut owners,
            AUTO_COMMIT_CLIENT_ID,
            0,
            6,
            Operation::DeleteConsumerOffset,
        );
    }

    fn installed_owner() -> PartitionOwners {
        PartitionOwners {
            installed: BTreeMap::from([(
                0,
                ConsumerGroupOwner {
                    client_id: 1,
                    session: 2,
                    generation: 7,
                },
            )]),
            retirement: None,
        }
    }

    fn retired_owner() -> PartitionOwners {
        let mut owners = installed_owner();
        owners.installed.insert(
            0,
            ConsumerGroupOwner {
                generation: 7,
                ..Default::default()
            },
        );
        for (metadata_op, live_group_ids) in [(8, vec![]), (9, vec![0])] {
            let catalog = RetireConsumerGroupOwnersRequest {
                incarnation: 1,
                metadata_op,
                next_group_id: 1,
                live_group_ids,
            };
            check_partition_owner(
                &mut owners,
                &PrepareHeader {
                    operation: Operation::RetireConsumerGroupOwners,
                    ..Default::default()
                },
                &catalog.to_bytes(),
                SEED,
            );
        }
        assert!(owners.installed.is_empty());
        owners
    }

    fn check_offset(
        owners: &mut PartitionOwners,
        client: u128,
        session: u64,
        generation: u64,
        operation: Operation,
    ) {
        let request = StoreConsumerOffsetRequest {
            consumer: WireConsumer::consumer_group(WireIdentifier::numeric(0)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: Some(0),
            offset: 0,
            ack: AckLevel::Quorum,
        };
        let mut body = if operation == Operation::DeleteConsumerOffset {
            DeleteConsumerOffsetRequest {
                consumer: request.consumer,
                stream_id: request.stream_id,
                topic_id: request.topic_id,
                partition_id: request.partition_id,
                ack: request.ack,
            }
            .to_bytes()
            .to_vec()
        } else {
            request.to_bytes().to_vec()
        };
        body.extend_from_slice(&generation.to_le_bytes());
        check_partition_owner(
            owners,
            &PrepareHeader {
                operation,
                client,
                session,
                op: 10,
                ..Default::default()
            },
            &body,
            SEED,
        );
    }

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
        let committed = sim
            .metadata_consensus(1)
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

    /// A three-replica cluster with committed partition ops flushed to segments.
    ///
    /// The state the partition comparison has to work in: `evict_prefix` clears the
    /// resident header vec on flush, so a resident-only read sees nothing.
    fn cluster_with_flushed_partition_ops() -> (Simulator, IggyNamespace) {
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
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        for sequence in 0..6u32 {
            let msg = client.send_messages(
                namespace,
                &[bytes::Bytes::from(format!("wl-part-{sequence}"))],
            );
            sim.submit_request(client_id, 0, msg.into_generic());
            for _ in 0..60 {
                sim.step();
            }
        }

        for replica_idx in 0..3usize {
            let shard = sim.replicas[replica_idx].partition_shard(namespace);
            let partitions = shard.plane.partitions();
            let config = partitions.config();
            let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                continue;
            };
            futures::executor::block_on(partition.flush_committed_messages(config))
                .expect("the in-memory flush must succeed");
        }
        sim.run_pumps();
        (sim, namespace)
    }

    /// The partition comparison must survive a flush.
    ///
    /// `header_by_op` reads the resident header vec, which `evict_prefix` clears as
    /// the committed prefix flushes, so a resident-only read compares zero ops at
    /// quiescence and the check passes over nothing.
    #[test]
    fn a_flushed_partition_prefix_is_still_compared() {
        let (sim, namespace) = cluster_with_flushed_partition_ops();

        let committed = sim
            .partition_consensus_state(1, namespace)
            .expect("replica 1 hosts the namespace")
            .commit_min;
        assert!(
            committed > 1,
            "the cluster committed {committed} partition op(s), so there is nothing \
             for two replicas to agree on"
        );

        let resident = sim.replicas[1]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .expect("replica 1 hosts the namespace")
            .log
            .journal()
            .inner
            .header_by_op(1);
        assert!(
            resident.is_none(),
            "op 1 is still resident, so this test does not exercise the flushed path"
        );

        let compared = assert_partition_prefixes_agree(&sim, &[namespace], SEED);
        assert!(
            compared > 0,
            "no committed partition op was witnessed by more than one replica after \
             the flush, so the check passed over an empty set"
        );

        let mut checker = StateChecker::new();
        for replica_idx in 0..sim.replica_count {
            checker.check_partition(&sim, replica_idx, namespace, SEED);
        }
        assert!(
            checker
                .partition_commits
                .values()
                .any(|commit| commit.witnesses.len() > 1),
            "the per-tick checker compared no flushed partition op"
        );
    }

    #[test]
    #[should_panic(expected = "disagree on committed partition op")]
    fn given_a_partition_commit_when_its_replica_disappears_should_still_reject_a_later_divergence()
    {
        const DIVERGED: u8 = 1;
        let (mut sim, namespace) = cluster_with_flushed_partition_ops();
        let workload = Workload::new(WorkloadOptions::new(
            SEED,
            sim.replica_count,
            vec![namespace],
        ));
        let mut invariants = Invariants::new();
        invariants.check(&sim, &workload);
        assert!(!invariants.state_checker().partition_commits.is_empty());
        for replica in 0..sim.replica_count {
            if replica != DIVERGED {
                sim.replica_crash(replica);
            }
        }
        let committed = sim
            .partition_consensus_state(usize::from(DIVERGED), namespace)
            .expect("the backup hosts the partition")
            .commit_min;
        change_partition_entry(&sim, DIVERGED, namespace, committed);

        invariants.check(&sim, &workload);
    }

    #[test]
    fn given_two_partition_incarnations_when_their_commits_differ_should_keep_separate_histories() {
        const NEXT: u8 = 1;
        let (sim, namespace) = cluster_with_flushed_partition_ops();
        let mut checker = StateChecker::new();
        checker.check_partition(&sim, NEXT, namespace, SEED);
        let previous = sim
            .partition_incarnation(usize::from(NEXT), namespace)
            .expect("the backup hosts the partition");
        let committed = sim
            .partition_consensus_state(usize::from(NEXT), namespace)
            .expect("the backup hosts the partition")
            .commit_min;
        sim.replicas[usize::from(NEXT)]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_mut_by_ns(&namespace)
            .expect("the backup hosts the partition")
            .set_created_revision(previous + 1);
        change_partition_entry(&sim, NEXT, namespace, committed);

        checker.check_partition(&sim, NEXT, namespace, SEED);

        assert!(
            checker
                .partition_commits
                .contains_key(&(namespace, previous, committed))
        );
        assert!(
            checker
                .partition_commits
                .contains_key(&(namespace, previous + 1, committed))
        );
    }

    #[test]
    #[should_panic(expected = "disagree on committed partition op")]
    fn given_a_checked_partition_when_it_restarts_should_recheck_its_retained_prefix() {
        const RESTARTED: u8 = 1;
        const RECOVERY_TICKS: usize = 400;
        const DAMAGED_OP: u64 = 1;
        let (mut sim, namespace) = cluster_with_flushed_partition_ops();
        let mut checker = StateChecker::new();
        checker.check_partition(&sim, RESTARTED, namespace, SEED);
        assert!(checker.partition_verified_upto[&(RESTARTED, namespace)].committed > DAMAGED_OP);
        sim.replica_crash(RESTARTED);
        sim.replica_restart(RESTARTED);
        for _ in 0..RECOVERY_TICKS {
            sim.step();
        }
        change_partition_entry(&sim, RESTARTED, namespace, DAMAGED_OP);

        checker.check_partition(&sim, RESTARTED, namespace, SEED);
    }

    fn change_partition_entry(sim: &Simulator, replica: u8, namespace: IggyNamespace, op: u64) {
        let partition = sim.replicas[usize::from(replica)]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .expect("the replica hosts the partition");
        let journal = &partition.log.journal().inner;
        let mut entry = journal
            .repair_entry(op)
            .expect("the committed entry is retained")
            .as_slice()
            .to_vec();
        let header_bytes = &mut entry[..size_of::<PrepareHeader>()];
        let mut header: PrepareHeader = bytemuck::checked::pod_read_unaligned(header_bytes);
        header.timestamp += 1;
        header_bytes.copy_from_slice(bytemuck::bytes_of(&header));
        journal.clear_all();
        journal.restore_checkpoint_prepare(
            op,
            server_common::iobuf::Owned::copy_from_slice(&entry).into(),
        );
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

    /// A state transfer keeps the receiver's residents below the floor it installs,
    /// and one of them can be a prepare its view lost: here the op an isolated
    /// primary journaled before it crashed. The snapshot superseded it, so it is not
    /// the receiver's committed history.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_a_transfer_over_a_lost_prepare_when_checked_every_step_should_not_compare_the_superseded_resident()
     {
        const OLD_PRIMARY: u8 = 0;
        const BACKUPS: [u8; 2] = [1, 2];
        // Crosses the checkpoint margin within the sends below, so the survivors
        // compact the prefix the old primary is missing.
        const JOURNAL_SLOTS: usize = 80;
        const SENDS: u32 = 40;
        const STEPS_PER_SEND: usize = 40;
        const ELECTION_STEPS: usize = 4_000;
        const REJOIN_STEPS: usize = 8_000;

        fn drop_every_packet(_: &Packet) -> bool {
            true
        }

        fn run(sim: &mut Simulator, checker: &mut StateChecker, steps: usize) {
            for _ in 0..steps {
                sim.step();
                checker.check(sim, SEED);
            }
        }

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let root = tempfile::tempdir().expect("temp dir for the simulator's snapshots");
        let client_id: u128 = 1;
        let mut sim = Simulator::with_checkpoints(
            3,
            std::iter::once(client_id),
            PacketSimulatorOptions {
                node_count: 3,
                client_count: 1,
                seed: SEED,
                ..PacketSimulatorOptions::default()
            },
            false,
            root.path(),
        );
        sim.set_metadata_journal_slots(JOURNAL_SLOTS);
        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);
        let mut checker = StateChecker::new();
        assert_eq!(sim.metadata_primary_index(), Some(OLD_PRIMARY));

        let lost_op = sim.replicas[usize::from(OLD_PRIMARY)]
            .metadata_journal
            .last_op()
            .unwrap_or(0)
            + 1;
        for backup in BACKUPS {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(OLD_PRIMARY), ProcessId::Replica(backup)) =
                Some(drop_every_packet);
        }
        let lost = client.create_stream("wl-chk-lost");
        sim.submit_request(client_id, OLD_PRIMARY, lost.clone().into_generic());
        run(&mut sim, &mut checker, STEPS_PER_SEND);
        let lost_checksum = journaled_header(&sim.replicas[usize::from(OLD_PRIMARY)], lost_op)
            .expect("the isolated primary never journaled the prepare its backups miss")
            .checksum;
        sim.replica_crash(OLD_PRIMARY);
        for backup in BACKUPS {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(OLD_PRIMARY), ProcessId::Replica(backup)) =
                None;
        }

        let new_primary =
            (0..ELECTION_STEPS)
                .find_map(|_| {
                    run(&mut sim, &mut checker, 1);
                    sim.metadata_primary_index().filter(|&primary| {
                        primary != OLD_PRIMARY
                            && sim.metadata_consensus(usize::from(primary)).is_some_and(
                                |consensus| consensus.status() == consensus::Status::Normal,
                            )
                    })
                })
                .expect("the survivors never elected a primary");
        sim.submit_request(client_id, new_primary, lost.into_generic());
        run(&mut sim, &mut checker, STEPS_PER_SEND);
        for sequence in 0..SENDS {
            let msg = client.create_stream(&format!("wl-chk-{sequence}"));
            sim.submit_request(client_id, new_primary, msg.into_generic());
            run(&mut sim, &mut checker, STEPS_PER_SEND);
        }

        sim.replica_restart(OLD_PRIMARY);
        run(&mut sim, &mut checker, REJOIN_STEPS);

        let rejoined = &sim.replicas[usize::from(OLD_PRIMARY)];
        assert!(
            rejoined.metadata_journal.snapshot_op() > lost_op,
            "the old primary rejoined without a state transfer over op {lost_op}, so no \
             resident was left below a floor"
        );
        assert_eq!(
            journaled_header(rejoined, lost_op).map(|header| header.checksum),
            Some(lost_checksum),
            "the lost prepare is gone from the old primary's WAL, so nothing superseded \
             was left to compare"
        );
        let canonical = checker
            .commits
            .get(&lost_op)
            .expect("the survivors' commit at the lost op was never recorded");
        assert_ne!(
            canonical.checksum, lost_checksum,
            "the survivors committed the lost prepare itself, so the resident is not stale"
        );
        assert!(
            canonical.witnesses.len() > 1,
            "no two survivors witnessed op {lost_op}, so the canonical entry compares nothing"
        );
    }
}
