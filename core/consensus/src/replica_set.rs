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

use crate::REPLICAS_MAX;
use std::{error::Error, fmt};

/// Cluster node ids of one consensus group, in member order.
///
/// Consensus state and frame headers address replicas by member index; only
/// the message bus addresses nodes, so sends translate through this set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaSet {
    nodes: [u8; REPLICAS_MAX],
    len: u8,
}

impl ReplicaSet {
    /// Nodes `0..count`, where member index and node id coincide.
    ///
    /// # Panics
    /// If `count` is zero or above [`REPLICAS_MAX`].
    #[must_use]
    pub fn all(count: u8) -> Self {
        assert!(
            (1..=REPLICAS_MAX).contains(&usize::from(count)),
            "replica set size {count} outside 1..={REPLICAS_MAX}"
        );
        let mut nodes = [0; REPLICAS_MAX];
        for (node, slot) in (0..count).zip(nodes.iter_mut()) {
            *slot = node;
        }
        Self { nodes, len: count }
    }

    /// # Errors
    /// If `nodes` is empty, longer than [`REPLICAS_MAX`], or repeats a node.
    pub fn new(nodes: &[u8]) -> Result<Self, ReplicaSetError> {
        if nodes.is_empty() {
            return Err(ReplicaSetError::Empty);
        }
        if nodes.len() > REPLICAS_MAX {
            return Err(ReplicaSetError::TooMany { len: nodes.len() });
        }
        for (index, node) in nodes.iter().enumerate() {
            if nodes[..index].contains(node) {
                return Err(ReplicaSetError::Duplicate { node: *node });
            }
        }
        let mut members = [0; REPLICAS_MAX];
        members[..nodes.len()].copy_from_slice(nodes);
        Ok(Self {
            nodes: members,
            #[allow(clippy::cast_possible_truncation)]
            len: nodes.len() as u8,
        })
    }

    #[must_use]
    #[expect(
        clippy::len_without_is_empty,
        reason = "construction refuses an empty set"
    )]
    pub const fn len(&self) -> u8 {
        self.len
    }

    #[must_use]
    pub fn nodes(&self) -> &[u8] {
        &self.nodes[..usize::from(self.len)]
    }

    /// Node id of `member`, `None` when the index is outside the group.
    #[must_use]
    pub fn node(&self, member: u8) -> Option<u8> {
        self.nodes().get(usize::from(member)).copied()
    }

    /// Member index of `node`, `None` when the node is not in the group.
    #[must_use]
    pub fn member(&self, node: u8) -> Option<u8> {
        self.nodes()
            .iter()
            .position(|candidate| *candidate == node)
            .and_then(|index| u8::try_from(index).ok())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaSetError {
    Empty,
    TooMany { len: usize },
    Duplicate { node: u8 },
}

impl fmt::Display for ReplicaSetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("replica set has no members"),
            Self::TooMany { len } => write!(
                formatter,
                "replica set has {len} members, above the {REPLICAS_MAX} limit"
            ),
            Self::Duplicate { node } => {
                write!(formatter, "replica set lists node {node} twice")
            }
        }
    }
}

impl Error for ReplicaSetError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_all_nodes_when_translating_should_map_index_to_same_node() {
        let set = ReplicaSet::all(3);

        assert_eq!(set.nodes(), &[0, 1, 2]);
        assert_eq!(set.node(2), Some(2));
        assert_eq!(set.member(1), Some(1));
        assert_eq!(set.node(3), None);
    }

    #[test]
    fn given_subset_when_translating_should_map_both_ways() {
        let set = ReplicaSet::new(&[4, 1, 3]).expect("valid set");

        assert_eq!(set.len(), 3);
        assert_eq!(set.node(0), Some(4));
        assert_eq!(set.node(2), Some(3));
        assert_eq!(set.member(4), Some(0));
        assert_eq!(set.member(3), Some(2));
        assert_eq!(set.member(0), None);
        assert_eq!(set.node(3), None);
    }

    #[test]
    fn given_invalid_nodes_when_building_should_refuse() {
        assert_eq!(ReplicaSet::new(&[]), Err(ReplicaSetError::Empty));
        assert_eq!(
            ReplicaSet::new(&[1, 2, 1]),
            Err(ReplicaSetError::Duplicate { node: 1 })
        );
        let too_many: Vec<u8> = (0..=u8::try_from(REPLICAS_MAX).expect("fits")).collect();
        assert_eq!(
            ReplicaSet::new(&too_many),
            Err(ReplicaSetError::TooMany {
                len: REPLICAS_MAX + 1
            })
        );
    }
}
