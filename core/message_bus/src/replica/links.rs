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

//! Placement of the k replica links to each peer.
//!
//! Shard `s` belongs to range `s * k / shards`, and range `j` sends over
//! link `j`. Link `j` to one peer lives on one usable shard of range `j`,
//! picked by the peer slot. Both ends derive placement from configuration
//! alone, and a consensus group has the same shard index on every node, so
//! a group frame leaves from and arrives in the same range.

use crate::MAX_INFLIGHT_REPLICA_HANDSHAKES;
use thiserror::Error;

/// Shard count, links per peer and the shard-0 skip flag of one node.
///
/// Every node of a cluster must run the same link count: the replica
/// handshake refuses a peer whose count differs. Shard counts may differ,
/// because each node places links on its own shards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LinkLayout {
    shards: u16,
    links: u16,
    skip_shard_zero: bool,
}

/// A link count the node cannot run.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum LinkLayoutError {
    #[error("replica_connections_per_peer must be at least 1")]
    NoLinks,
    #[error("replica_connections_per_peer = {links} exceeds the shard count {shards}")]
    MoreLinksThanShards { links: u16, shards: u16 },
    #[error(
        "replica_connections_per_peer = {links} with {peers} peers opens more than \
         {cap} links, the inbound handshake cap",
        cap = MAX_INFLIGHT_REPLICA_HANDSHAKES
    )]
    TooManyLinks { links: u16, peers: usize },
}

impl LinkLayout {
    /// # Errors
    ///
    /// Refuses no links, more links than shards, and more links in total
    /// than [`MAX_INFLIGHT_REPLICA_HANDSHAKES`], which would let a full
    /// redial of every peer fill the inbound handshake cap.
    pub fn new(
        shards: u16,
        links: u16,
        skip_shard_zero: bool,
        peers: usize,
    ) -> Result<Self, LinkLayoutError> {
        if links == 0 {
            return Err(LinkLayoutError::NoLinks);
        }
        if links > shards {
            return Err(LinkLayoutError::MoreLinksThanShards { links, shards });
        }
        if usize::from(links) * peers > MAX_INFLIGHT_REPLICA_HANDSHAKES {
            return Err(LinkLayoutError::TooManyLinks { links, peers });
        }
        Ok(Self {
            shards,
            links,
            skip_shard_zero,
        })
    }

    #[must_use]
    pub const fn links(self) -> u16 {
        self.links
    }

    /// The link that `shard` sends over.
    ///
    /// A shard id at or above the shard count, which only test buses use,
    /// falls into the last range.
    #[must_use]
    pub fn range_of(self, shard: u16) -> u16 {
        let range = u32::from(shard) * u32::from(self.links) / u32::from(self.shards);
        u16::try_from(range).map_or(self.links - 1, |range| range.min(self.links - 1))
    }

    /// The shard that owns `link` to the peer at `peer_slot`.
    ///
    /// Shard 0 hosts the listener, the dialer and the metadata group, so the
    /// skip flag keeps links off it while its range has another shard.
    #[must_use]
    pub fn link_owner(self, link: u16, peer_slot: u8) -> u16 {
        debug_assert!(link < self.links, "link {link} of {}", self.links);
        let first = self.first_of_range(link);
        let end = self.first_of_range(link + 1);
        let first_usable = if first == 0 && self.skip_shard_zero && end > 1 {
            1
        } else {
            first
        };
        first_usable + u16::from(peer_slot) % (end - first_usable)
    }

    /// The lowest shard of range `link`, or the shard count past the last
    /// range: `ceil(link * shards / links)`.
    fn first_of_range(self, link: u16) -> u16 {
        let first = (u32::from(link) * u32::from(self.shards)).div_ceil(u32::from(self.links));
        // `link <= links`, so `first <= shards`.
        u16::try_from(first).unwrap_or(self.shards)
    }
}

/// One link per peer on a single shard: the layout of a bus that never
/// places links itself.
impl Default for LinkLayout {
    fn default() -> Self {
        Self {
            shards: 1,
            links: 1,
            skip_shard_zero: true,
        }
    }
}

/// Index of `peer` among the peers of `self_id`, in ascending id order.
///
/// Replica ids are dense (`0..replica_count`, enforced by the cluster
/// configuration), so the slot skips only the own id.
#[must_use]
pub const fn peer_slot(self_id: u8, peer: u8) -> u8 {
    if peer < self_id { peer } else { peer - 1 }
}

/// One replica link: the peer, the link index and the bundle epoch the
/// link was placed in.
///
/// The epoch is local to the node that placed the link. It never goes on
/// the wire; it only tells a late close of an old link from the links of
/// the bundle that replaced it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaLinkId {
    pub replica: u8,
    pub link: u16,
    pub epoch: u32,
}

/// An installed link as the owner table records it: the shard that owns
/// it and the epoch it was placed in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LinkClaim {
    pub owner: u16,
    pub epoch: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(shards: u16, links: u16) -> LinkLayout {
        LinkLayout::new(shards, links, true, 2).expect("valid layout")
    }

    #[test]
    fn ranges_are_contiguous_for_every_link_count_at_8_shards() {
        let expected: [(u16, [u16; 8]); 4] = [
            (1, [0, 0, 0, 0, 0, 0, 0, 0]),
            (2, [0, 0, 0, 0, 1, 1, 1, 1]),
            (3, [0, 0, 0, 1, 1, 1, 2, 2]),
            (8, [0, 1, 2, 3, 4, 5, 6, 7]),
        ];
        for (links, ranges) in expected {
            let layout = layout(8, links);
            let actual: Vec<u16> = (0..8).map(|shard| layout.range_of(shard)).collect();
            assert_eq!(actual, ranges, "ranges at k = {links}");
        }
    }

    #[test]
    fn owners_match_the_two_link_diagram() {
        // 3 nodes, 8 shards, k = 2: node 0 places A0/A1 (peer 1) on shards
        // 1/4 and B0/B1 (peer 2) on shards 2/5; node 2 places B0/B1 (peer 0)
        // on shards 1/4 and C0/C1 (peer 1) on shards 2/5.
        let layout = layout(8, 2);
        assert_eq!(layout.link_owner(0, peer_slot(0, 1)), 1);
        assert_eq!(layout.link_owner(1, peer_slot(0, 1)), 4);
        assert_eq!(layout.link_owner(0, peer_slot(0, 2)), 2);
        assert_eq!(layout.link_owner(1, peer_slot(0, 2)), 5);
        assert_eq!(layout.link_owner(0, peer_slot(2, 0)), 1);
        assert_eq!(layout.link_owner(1, peer_slot(2, 0)), 4);
        assert_eq!(layout.link_owner(0, peer_slot(2, 1)), 2);
        assert_eq!(layout.link_owner(1, peer_slot(2, 1)), 5);
    }

    #[test]
    fn every_owner_sits_in_the_range_of_its_link() {
        for links in [1, 2, 3, 8] {
            let layout = layout(8, links);
            for link in 0..links {
                for slot in 0..7 {
                    let owner = layout.link_owner(link, slot);
                    assert_eq!(layout.range_of(owner), link, "k = {links}, slot {slot}");
                    assert!(owner != 0 || link == 0, "shard 0 owns only link 0");
                }
            }
        }
    }

    #[test]
    fn shard_zero_owns_a_link_only_alone_in_its_range_or_without_skip() {
        assert_eq!(layout(8, 1).link_owner(0, 0), 1, "k = 1 skips shard 0");
        assert_eq!(
            layout(8, 8).link_owner(0, 0),
            0,
            "range 0 holds only shard 0"
        );
        assert_eq!(
            layout(1, 1).link_owner(0, 1),
            0,
            "a single shard owns every link"
        );
        let no_skip = LinkLayout::new(8, 2, false, 2).expect("valid layout");
        assert_eq!(no_skip.link_owner(0, 0), 0);
        assert_eq!(no_skip.link_owner(0, 3), 3);
    }

    #[test]
    fn peer_slots_skip_the_own_id() {
        assert_eq!(peer_slot(1, 0), 0);
        assert_eq!(peer_slot(1, 2), 1);
        assert_eq!(peer_slot(0, 1), 0);
        assert_eq!(peer_slot(2, 1), 1);
    }

    #[test]
    fn layout_refuses_link_counts_the_node_cannot_run() {
        assert_eq!(
            LinkLayout::new(8, 0, true, 2),
            Err(LinkLayoutError::NoLinks)
        );
        assert_eq!(
            LinkLayout::new(4, 5, true, 2),
            Err(LinkLayoutError::MoreLinksThanShards {
                links: 5,
                shards: 4
            })
        );
        assert_eq!(
            LinkLayout::new(512, 129, true, 2),
            Err(LinkLayoutError::TooManyLinks {
                links: 129,
                peers: 2
            })
        );
        assert!(LinkLayout::new(512, 128, true, 2).is_ok());
    }

    #[test]
    fn test_bus_shard_ids_past_the_shard_count_use_the_last_range() {
        assert_eq!(LinkLayout::default().range_of(5), 0);
        assert_eq!(layout(4, 2).range_of(9), 1);
    }
}
