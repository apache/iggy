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

use crate::cache::AllocationStrategy;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

pub const DEFAULT_MAX_CONNECTIONS_PER_REPLICA: usize = 8;

pub trait ShardedState {
    type Entry;
    type Delta;

    fn apply(&mut self, delta: Self::Delta);
}

/// Identifies a connection on a specific shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionAssignment {
    pub replica: u8,
    pub shard: u16,
}

/// Maps a source shard to the shard that owns the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardAssignment {
    pub replica: u8,
    pub shard: u16,
    pub conn_shard: u16,
}

/// Changeset for connection-based allocation.
#[derive(Debug, Clone)]
pub enum ConnectionChanges {
    Allocate {
        connections: Vec<ConnectionAssignment>,
        mappings: Vec<ShardAssignment>,
    },
    Deallocate {
        replica: u8,
        connections: Vec<ConnectionAssignment>,
    },
}

/// Round-robin allocator: assigns each new replica to the next shard in
/// sequence. Exactly one owning shard per replica.
///
/// This is the production strategy today. [`LeastLoadedStrategy`] is kept
/// alongside as unwired scaffolding for a future tuning pass.
#[derive(Debug)]
pub struct RoundRobinStrategy {
    total_shards: u16,
    counter: Cell<u16>,
    assigned: RefCell<HashMap<u8, u16>>,
}

impl RoundRobinStrategy {
    #[must_use]
    pub fn new(total_shards: u16) -> Self {
        Self {
            total_shards,
            counter: Cell::new(0),
            assigned: RefCell::new(HashMap::new()),
        }
    }

    fn next_shard(&self) -> u16 {
        let next = self.counter.get();
        self.counter
            .set(next.wrapping_add(1).rem_euclid(self.total_shards.max(1)));
        next.rem_euclid(self.total_shards.max(1))
    }
}

impl AllocationStrategy<ConnectionCache> for RoundRobinStrategy {
    fn allocate(&self, replica: u8) -> Option<<ConnectionCache as ShardedState>::Delta> {
        if self.total_shards == 0 {
            return None;
        }
        if self.assigned.borrow().contains_key(&replica) {
            return None;
        }

        let owner = self.next_shard();
        self.assigned.borrow_mut().insert(replica, owner);

        let connections = vec![ConnectionAssignment {
            replica,
            shard: owner,
        }];
        let mappings = (0..self.total_shards)
            .map(|shard| ShardAssignment {
                replica,
                shard,
                conn_shard: owner,
            })
            .collect();

        Some(ConnectionChanges::Allocate {
            connections,
            mappings,
        })
    }

    fn deallocate(&self, replica: u8) -> Option<<ConnectionCache as ShardedState>::Delta> {
        let owner = self.assigned.borrow_mut().remove(&replica)?;
        let connections = vec![ConnectionAssignment {
            replica,
            shard: owner,
        }];
        Some(ConnectionChanges::Deallocate {
            replica,
            connections,
        })
    }
}

/// Least-loaded allocation strategy for connections.
///
/// NOT wired to production in this revision; see [`RoundRobinStrategy`]. Kept
/// as scaffolding for a future tuning pass that distributes multiple
/// connections per replica across the least-loaded shards. The bugs the
/// earlier version shipped with (non-advancing seed, unfiltered mapping
/// apply) are fixed here so this code does not rot.
#[derive(Debug)]
pub struct LeastLoadedStrategy {
    total_shards: u16,
    max_connections: usize,
    connections_per_shard: RefCell<Vec<(u16, usize)>>,
    replica_to_shards: RefCell<HashMap<u8, HashSet<u16>>>,
    rng_seed: Cell<u64>,
}

impl LeastLoadedStrategy {
    #[must_use]
    pub fn new(total_shards: u16, max_connections: usize, seed: u64) -> Self {
        Self {
            total_shards,
            max_connections,
            connections_per_shard: RefCell::new((0..total_shards).map(|s| (s, 0)).collect()),
            replica_to_shards: RefCell::new(HashMap::new()),
            rng_seed: Cell::new(seed),
        }
    }

    /// Advance the seed via an LCG step so successive allocations see
    /// different shuffle orders without needing external randomness.
    fn next_seed(&self) -> u64 {
        let s = self
            .rng_seed
            .get()
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.rng_seed.set(s);
        s
    }

    fn create_shard_mappings(
        &self,
        mappings: &mut Vec<ShardAssignment>,
        replica: u8,
        mut conn_shards: Vec<u16>,
        seed: u64,
    ) {
        for &shard in &conn_shards {
            mappings.push(ShardAssignment {
                replica,
                shard,
                conn_shard: shard,
            });
        }

        let mut rng = StdRng::seed_from_u64(seed);
        conn_shards.shuffle(&mut rng);

        let mut j = 0;
        for shard in 0..self.total_shards {
            if conn_shards.contains(&shard) {
                continue;
            }
            let conn_idx = j % conn_shards.len();
            mappings.push(ShardAssignment {
                replica,
                shard,
                conn_shard: conn_shards[conn_idx],
            });
            j += 1;
        }
    }

    /// Returns the set of shard ids that own connections for a given replica.
    #[must_use]
    pub fn owning_shards(&self, replica: u8) -> Option<HashSet<u16>> {
        self.replica_to_shards.borrow().get(&replica).cloned()
    }
}

impl AllocationStrategy<ConnectionCache> for LeastLoadedStrategy {
    fn allocate(&self, replica: u8) -> Option<<ConnectionCache as ShardedState>::Delta> {
        if self.replica_to_shards.borrow().contains_key(&replica) {
            return None;
        }

        let mut connections = Vec::new();
        let mut mappings = Vec::new();
        let connections_needed = usize::from(self.total_shards).min(self.max_connections);
        if connections_needed == 0 {
            return None;
        }

        let seed = self.next_seed();
        {
            let mut cps = self.connections_per_shard.borrow_mut();
            let mut rng = StdRng::seed_from_u64(seed);
            cps.shuffle(&mut rng);
            cps.sort_by_key(|(_, count)| *count);
        }

        let mut assigned_shards = HashSet::with_capacity(connections_needed);

        {
            let mut cps = self.connections_per_shard.borrow_mut();
            for i in 0..connections_needed {
                let (shard, count) = cps.get_mut(i).expect("guarded by connections_needed");
                connections.push(ConnectionAssignment {
                    replica,
                    shard: *shard,
                });
                *count += 1;
                assigned_shards.insert(*shard);
            }
        }

        self.replica_to_shards
            .borrow_mut()
            .insert(replica, assigned_shards.clone());

        self.create_shard_mappings(
            &mut mappings,
            replica,
            assigned_shards.into_iter().collect(),
            seed,
        );

        Some(ConnectionChanges::Allocate {
            connections,
            mappings,
        })
    }

    fn deallocate(&self, replica: u8) -> Option<<ConnectionCache as ShardedState>::Delta> {
        let conn_shards = self.replica_to_shards.borrow_mut().remove(&replica)?;

        let mut connections = Vec::new();
        for shard in &conn_shards {
            if let Some((_, count)) = self
                .connections_per_shard
                .borrow_mut()
                .iter_mut()
                .find(|(s, _)| s == shard)
            {
                *count = count.saturating_sub(1);
            }
            connections.push(ConnectionAssignment {
                replica,
                shard: *shard,
            });
        }

        Some(ConnectionChanges::Deallocate {
            replica,
            connections,
        })
    }
}

/// Coordinator that wraps a strategy for a specific sharded state type.
#[derive(Debug)]
pub struct Coordinator<A, SS>
where
    SS: ShardedState,
    A: AllocationStrategy<SS>,
{
    strategy: A,
    _ss: std::marker::PhantomData<SS>,
}

impl<A, SS> Coordinator<A, SS>
where
    SS: ShardedState,
    A: AllocationStrategy<SS>,
{
    pub const fn new(strategy: A) -> Self {
        Self {
            strategy,
            _ss: std::marker::PhantomData,
        }
    }

    pub fn allocate(&self, entry: SS::Entry) -> Option<SS::Delta> {
        self.strategy.allocate(entry)
    }

    pub fn deallocate(&self, entry: SS::Entry) -> Option<SS::Delta> {
        self.strategy.deallocate(entry)
    }
}

#[derive(Debug)]
pub struct ShardedConnections<A, SS>
where
    SS: ShardedState,
    A: AllocationStrategy<SS>,
{
    pub coordinator: Coordinator<A, SS>,
    pub state: SS,
}

impl<A, SS> ShardedConnections<A, SS>
where
    SS: ShardedState,
    A: AllocationStrategy<SS>,
{
    pub fn allocate(&mut self, entry: SS::Entry) -> bool {
        if let Some(delta) = self.coordinator.allocate(entry) {
            self.state.apply(delta);
            true
        } else {
            false
        }
    }

    pub fn deallocate(&mut self, entry: SS::Entry) -> bool {
        if let Some(delta) = self.coordinator.deallocate(entry) {
            self.state.apply(delta);
            true
        } else {
            false
        }
    }
}

/// Per-shard cache of connection state.
///
/// Each shard holds its own `ConnectionCache`. Owning shards have entries in
/// `owns_connection` for replicas whose TCP connections live on this shard.
/// All shards (owning or not) have entries in `connection_map` that tell them
/// which shard owns the connection for each replica.
#[derive(Debug, Default)]
pub struct ConnectionCache {
    pub shard_id: u16,
    /// Replicas for which THIS shard owns a connection. Value is true once
    /// the TCP connection has been established (fd received and registered).
    pub owns_connection: HashMap<u8, bool>,
    /// For every known replica, the shard that owns the connection.
    pub connection_map: HashMap<u8, u16>,
}

impl ConnectionCache {
    #[must_use]
    pub fn is_owner(&self, replica: u8) -> bool {
        self.owns_connection.contains_key(&replica)
    }

    #[must_use]
    pub fn owning_shard(&self, replica: u8) -> Option<u16> {
        self.connection_map.get(&replica).copied()
    }

    pub fn mark_connected(&mut self, replica: u8) {
        if let Some(connected) = self.owns_connection.get_mut(&replica) {
            *connected = true;
        }
    }

    pub fn mark_disconnected(&mut self, replica: u8) {
        if let Some(connected) = self.owns_connection.get_mut(&replica) {
            *connected = false;
        }
    }
}

impl ShardedState for ConnectionCache {
    type Entry = u8;
    type Delta = ConnectionChanges;

    fn apply(&mut self, delta: Self::Delta) {
        let shard_id = self.shard_id;
        match delta {
            ConnectionChanges::Allocate {
                connections,
                mappings,
            } => {
                for conn in connections.iter().filter(|c| c.shard == shard_id) {
                    self.owns_connection.insert(conn.replica, false);
                }
                for mapping in mappings.iter().filter(|m| m.shard == shard_id) {
                    self.connection_map
                        .insert(mapping.replica, mapping.conn_shard);
                }
            }
            ConnectionChanges::Deallocate {
                replica,
                connections,
            } => {
                for conn in connections.iter().filter(|c| c.shard == shard_id) {
                    self.owns_connection.remove(&conn.replica);
                }
                self.connection_map.remove(&replica);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(shard_id: u16) -> ConnectionCache {
        ConnectionCache {
            shard_id,
            ..Default::default()
        }
    }

    #[test]
    fn round_robin_allocates_sequentially() {
        let strategy = RoundRobinStrategy::new(4);
        let replicas: Vec<u8> = (0..8).collect();
        let deltas: Vec<_> = replicas.iter().map(|&r| strategy.allocate(r)).collect();

        let owners: Vec<u16> = deltas
            .iter()
            .map(|d| match d.as_ref().unwrap() {
                ConnectionChanges::Allocate { connections, .. } => connections[0].shard,
                ConnectionChanges::Deallocate { .. } => panic!("expected Allocate"),
            })
            .collect();
        assert_eq!(owners, vec![0, 1, 2, 3, 0, 1, 2, 3]);
    }

    #[test]
    fn round_robin_allocate_duplicate_returns_none() {
        let strategy = RoundRobinStrategy::new(4);
        let _ = strategy.allocate(1).expect("first allocate");
        assert!(strategy.allocate(1).is_none());
    }

    #[test]
    fn round_robin_deallocate_cleans_up() {
        let strategy = RoundRobinStrategy::new(4);
        let mut state = cache(2);
        let delta = strategy.allocate(5).unwrap();
        state.apply(delta);
        let delta = strategy.deallocate(5).unwrap();
        state.apply(delta);
        assert!(state.connection_map.is_empty());
        assert!(state.owns_connection.is_empty());
    }

    #[test]
    fn round_robin_zero_shards_is_none() {
        let strategy = RoundRobinStrategy::new(0);
        assert!(strategy.allocate(0).is_none());
    }

    #[test]
    fn apply_allocate_sets_mapping_on_every_shard_correctly() {
        // This exercises the shard-filter fix: each shard should see the
        // correct owning shard for a replica, regardless of how many other
        // shards were mentioned in the mapping vec.
        let strategy = RoundRobinStrategy::new(4);
        let delta = strategy.allocate(7).unwrap();

        for shard_id in 0..4 {
            let mut state = cache(shard_id);
            state.apply(delta.clone());
            assert_eq!(
                state.connection_map.get(&7).copied(),
                Some(0),
                "shard {shard_id} must map replica 7 to owner 0 (rr first pick)"
            );
            let should_own = shard_id == 0;
            assert_eq!(state.is_owner(7), should_own);
        }
    }

    #[test]
    fn apply_allocate_only_owner_marks_owns_connection() {
        let strategy = RoundRobinStrategy::new(4);
        // Skip once so next owner is shard 1.
        let _ = strategy.allocate(100).unwrap();
        let delta = strategy.allocate(7).unwrap();

        let mut owner = cache(1);
        let mut non_owner = cache(3);
        owner.apply(delta.clone());
        non_owner.apply(delta);

        assert!(owner.is_owner(7));
        assert_eq!(owner.owning_shard(7), Some(1));
        assert!(!non_owner.is_owner(7));
        assert_eq!(non_owner.owning_shard(7), Some(1));
    }

    #[test]
    fn least_loaded_allocate_duplicate_returns_none() {
        let strategy = LeastLoadedStrategy::new(4, 2, 42);
        let _ = strategy.allocate(1);
        assert!(strategy.allocate(1).is_none());
    }

    #[test]
    fn least_loaded_deallocate_cleans_up() {
        let strategy = LeastLoadedStrategy::new(4, 2, 42);
        let mut state = cache(0);
        let delta = strategy.allocate(1).unwrap();
        state.apply(delta);
        assert!(!state.connection_map.is_empty());

        let delta = strategy.deallocate(1).unwrap();
        state.apply(delta);
        assert!(state.connection_map.is_empty());
        assert!(state.owns_connection.is_empty());
    }

    #[test]
    fn least_loaded_max_connections_caps_allocation() {
        let strategy = LeastLoadedStrategy::new(16, 8, 42);
        let delta = strategy.allocate(1).unwrap();
        match &delta {
            ConnectionChanges::Allocate { connections, .. } => {
                assert_eq!(connections.len(), 8);
            }
            ConnectionChanges::Deallocate { .. } => panic!("expected Allocate"),
        }
    }

    #[test]
    fn least_loaded_single_shard_gets_all_connections() {
        let strategy = LeastLoadedStrategy::new(1, 8, 42);
        let delta = strategy.allocate(1).unwrap();
        match &delta {
            ConnectionChanges::Allocate {
                connections,
                mappings,
            } => {
                assert_eq!(connections.len(), 1);
                assert_eq!(mappings.len(), 1);
                assert_eq!(mappings[0].conn_shard, 0);
            }
            ConnectionChanges::Deallocate { .. } => panic!("expected Allocate"),
        }
    }

    #[test]
    fn least_loaded_seed_advances_between_calls() {
        let strategy = LeastLoadedStrategy::new(4, 2, 42);
        let first = strategy.rng_seed.get();
        let _ = strategy.next_seed();
        let second = strategy.rng_seed.get();
        assert_ne!(first, second, "seed must advance on next_seed");
    }
}
