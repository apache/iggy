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

use hash32::{Hasher, Murmur3Hasher};
use server_common::sharding::{IggyNamespace, PartitionLocation};
use std::hash::Hasher as _;

/// Lookup table that maps partition namespaces to their owning shard.
///
/// Implementations can be:
/// - A shared concurrent map (`DashMap`, papaya, etc.) referenced by all shards.
/// - A per-shard local `HashMap` replica, updated via a
///   broadcast when partitions are created, deleted, or moved.
pub trait ShardsTable {
    /// Returns the shard id that owns `namespace`, or `None` if the
    /// namespace is not yet registered (partition not created or update
    /// hasn't propagated).
    fn shard_for(&self, namespace: IggyNamespace) -> Option<u16>;

    /// Epoch (`Partition::created_revision`) stored on the row, if any.
    /// The reconciler compares it to the committed value to detect a
    /// stale local partition after slab-key reuse.
    fn epoch_for(&self, namespace: IggyNamespace) -> Option<u64>;

    fn insert(&self, namespace: IggyNamespace, location: PartitionLocation);

    fn remove(&self, namespace: &IggyNamespace) -> Option<PartitionLocation>;

    fn namespaces(&self) -> Vec<IggyNamespace>;
}

/// Always-`None` impl. Satisfies the trait for test / simulator paths
/// that never route via the table (e.g. [`crate::IggyShard::without_inbox`]).
/// Do not use in production: the router drops any partition frame whose
/// `shard_for` returns `None`, so a real cluster wired to this impl would
/// silently shed every partition request.
impl ShardsTable for () {
    fn shard_for(&self, _namespace: IggyNamespace) -> Option<u16> {
        None
    }

    fn epoch_for(&self, _namespace: IggyNamespace) -> Option<u64> {
        None
    }

    fn insert(&self, _namespace: IggyNamespace, _location: PartitionLocation) {}

    fn remove(&self, _namespace: &IggyNamespace) -> Option<PartitionLocation> {
        None
    }

    fn namespaces(&self) -> Vec<IggyNamespace> {
        Vec::new()
    }
}

/// Lock-free shards table backed by [`papaya::HashMap`].
pub struct PapayaShardsTable {
    inner: papaya::HashMap<IggyNamespace, PartitionLocation>,
}

impl Default for PapayaShardsTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PapayaShardsTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: papaya::HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: papaya::HashMap::with_capacity(capacity),
        }
    }
}

impl ShardsTable for PapayaShardsTable {
    fn shard_for(&self, namespace: IggyNamespace) -> Option<u16> {
        let guard = self.inner.guard();
        self.inner.get(&namespace, &guard).map(|loc| *loc.shard_id)
    }

    fn epoch_for(&self, namespace: IggyNamespace) -> Option<u64> {
        let guard = self.inner.guard();
        self.inner.get(&namespace, &guard).map(|loc| loc.epoch)
    }

    fn insert(&self, namespace: IggyNamespace, location: PartitionLocation) {
        self.inner.pin().insert(namespace, location);
    }

    fn remove(&self, namespace: &IggyNamespace) -> Option<PartitionLocation> {
        let guard = self.inner.guard();
        self.inner.remove(namespace, &guard).copied()
    }

    /// Snapshot: papaya guards cannot escape the call site.
    fn namespaces(&self) -> Vec<IggyNamespace> {
        let guard = self.inner.guard();
        self.inner.iter(&guard).map(|(ns, _)| *ns).collect()
    }
}

/// The shards of one node that host partitions, and the hash that places a
/// partition namespace on one of them.
///
/// The router, the reconciler and boot recovery must place with the same value,
/// or a partition loads on one shard while its frames route to another. A node
/// places by its own configuration alone: every inbound frame is hashed again
/// by the receiver, and no shard index crosses the wire. Nodes whose shard
/// count or skip flag differ therefore stay correct, but a group can then sit
/// on different shard indexes on the two nodes, so its frames arrive on a link
/// of another shard range and commit batches between them split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionShards {
    count: u16,
    skip_shard_zero: bool,
}

impl PartitionShards {
    /// `count` is the node's shard count. With `skip_shard_zero` and more than
    /// one shard, no partition lands on shard 0, which runs the metadata plane.
    #[must_use]
    pub const fn new(count: u16, skip_shard_zero: bool) -> Self {
        // The config validator and the shard ctors guarantee a shard. A zero
        // count would otherwise surface as an opaque remainder-by-zero panic.
        debug_assert!(count > 0, "shard count must be > 0");
        Self {
            count,
            skip_shard_zero,
        }
    }

    /// Every shard of the node, shard 0 included.
    #[must_use]
    pub const fn shard_count(self) -> u16 {
        self.count
    }

    #[must_use]
    pub const fn skip_shard_zero(self) -> bool {
        self.skip_shard_zero
    }

    /// The shards that own partitions. The per-node partition budgets split
    /// between them only. Boot refuses `skip_shard_zero` on a node with one
    /// shard, and that shard still owns every partition here, so a test shard
    /// built that way cannot divide by zero.
    #[must_use]
    pub const fn owner_count(self) -> u16 {
        if self.skip_shard_zero && self.count > 1 {
            self.count - 1
        } else {
            self.count
        }
    }

    /// The lowest shard that owns partitions. The shards below it own none.
    #[must_use]
    pub const fn first_owner(self) -> u16 {
        self.count - self.owner_count()
    }

    /// The shard that owns the partition at `namespace`.
    #[must_use]
    pub fn shard_for(self, namespace: &IggyNamespace) -> u16 {
        self.shard_for_consensus_ns(namespace.inner())
    }

    /// The shard that owns the consensus group `ns`.
    ///
    /// Consensus control-plane messages (`StartViewChange`, `DoViewChange`,
    /// `StartView`, `Commit`) carry the group's `namespace: u64` raw. A
    /// partition group uses its packed [`IggyNamespace`], and
    /// [`Self::shard_for`] hashes the same value, so a partition and its group
    /// share a shard.
    /// The upper 16 bits of the Murmur3 hash are used because the low bits are
    /// weak for small integer inputs.
    #[must_use]
    pub fn shard_for_consensus_ns(self, ns: u64) -> u16 {
        let mut hasher = Murmur3Hasher::default();
        hasher.write_u64(ns);
        let high = (hasher.finish32() >> 16) as u16;
        self.first_owner() + high % self.owner_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    const SHARD_COUNTS: [u16; 5] = [1, 2, 3, 4, 8];
    /// Enough namespaces that every shard of the largest count gets one.
    const NAMESPACES: u64 = 4096;

    /// Owners under the formula before the skip flag existed, one per entry of
    /// `SHARD_COUNTS`, computed with that code.
    const UNSKIPPED_OWNERS: [(u64, [u16; 5]); 6] = [
        (0, [0, 1, 1, 1, 5]),
        (1, [0, 1, 0, 3, 7]),
        (42, [0, 1, 2, 3, 7]),
        (1 << 20, [0, 1, 1, 1, 1]),
        (0xDEAD_BEEF, [0, 1, 0, 1, 5]),
        (u64::MAX >> 1, [0, 1, 0, 1, 1]),
    ];

    fn owners(shards: PartitionShards) -> BTreeSet<u16> {
        (0..NAMESPACES)
            .map(|ns| shards.shard_for_consensus_ns(ns))
            .collect()
    }

    #[test]
    fn given_skip_and_several_shards_when_placing_should_use_every_shard_but_zero() {
        for count in SHARD_COUNTS.into_iter().filter(|count| *count > 1) {
            let shards = PartitionShards::new(count, true);
            assert_eq!(
                owners(shards),
                (1..count).collect::<BTreeSet<_>>(),
                "{count} shards"
            );
            assert_eq!(shards.owner_count(), count - 1, "{count} shards");
            assert_eq!(shards.first_owner(), 1, "{count} shards");
        }
    }

    #[test]
    fn given_one_shard_when_placing_should_always_pick_shard_zero() {
        for skip_shard_zero in [false, true] {
            let shards = PartitionShards::new(1, skip_shard_zero);
            assert_eq!(
                owners(shards),
                BTreeSet::from([0]),
                "skip_shard_zero = {skip_shard_zero}"
            );
            assert_eq!(
                shards.owner_count(),
                1,
                "skip_shard_zero = {skip_shard_zero}"
            );
            assert_eq!(
                shards.first_owner(),
                0,
                "skip_shard_zero = {skip_shard_zero}"
            );
        }
    }

    #[test]
    fn given_no_skip_when_placing_should_keep_the_unskipped_owners() {
        for (ns, expected) in UNSKIPPED_OWNERS {
            let actual = SHARD_COUNTS
                .map(|count| PartitionShards::new(count, false).shard_for_consensus_ns(ns));
            assert_eq!(actual, expected, "namespace {ns:#x}");
        }
        assert_eq!(
            owners(PartitionShards::new(4, false)),
            BTreeSet::from([0, 1, 2, 3])
        );
    }

    #[test]
    fn given_a_partition_when_placing_should_put_it_on_the_shard_of_its_consensus_group() {
        for count in SHARD_COUNTS {
            for skip_shard_zero in [false, true] {
                let shards = PartitionShards::new(count, skip_shard_zero);
                for partition in 0..64 {
                    let namespace = IggyNamespace::new(1, 2, partition);
                    assert_eq!(
                        shards.shard_for(&namespace),
                        shards.shard_for_consensus_ns(namespace.inner()),
                        "{shards:?}, partition {partition}"
                    );
                }
            }
        }
    }
}
