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
use iggy_common::TcpSender;
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
};

const MAX_CONNECTIONS_PER_REPLICA: usize = 8;

// TODO: Move to some common trait location.
pub trait ShardedState {
    type Entry;
    type Delta;

    fn apply(&mut self, delta: Self::Delta);
}

/// Least-loaded allocation strategy for connections
pub struct LeastLoadedStrategy {
    total_shards: usize,
    connections_per_shard: RefCell<Vec<(u16, usize)>>,
    replica_to_shards: RefCell<HashMap<u8, HashSet<u16>>>,
    rng_seed: u64,
}

impl LeastLoadedStrategy {
    pub fn new(total_shards: usize, seed: u64) -> Self {
        Self {
            total_shards,
            connections_per_shard: RefCell::new((0..total_shards).map(|s| (s as u16, 0)).collect()),
            replica_to_shards: RefCell::new(HashMap::new()),
            rng_seed: seed,
        }
    }

    fn create_shard_mappings(
        &self,
        mappings: &mut Vec<ShardAssignment>,
        replica: u8,
        mut conn_shards: Vec<u16>,
    ) {
        for shard in &conn_shards {
            mappings.push(ShardAssignment {
                replica,
                shard: *shard,
                conn_shard: *shard,
            });
        }

        let mut rng = StdRng::seed_from_u64(self.rng_seed);
        conn_shards.shuffle(&mut rng);

        let mut j = 0;
        for shard in 0..self.total_shards {
            let shard = shard as u16;
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
}

/// Identifies a connection on a specific shard
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionAssignment {
    replica: u8,
    shard: u16,
}

/// Maps a source shard to the shard that owns the connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardAssignment {
    replica: u8,
    shard: u16,
    conn_shard: u16,
}

/// Changeset for connection-based allocation
#[derive(Debug, Clone)]
pub enum ConnectionChanges {
    Allocate {
        connections: Vec<ConnectionAssignment>,
        mappings: Vec<ShardAssignment>,
    },
    Deallocate {
        connections: Vec<ConnectionAssignment>,
        mappings: Vec<ConnectionAssignment>,
    },
}

type Delta = <ConnectionCache as ShardedState>::Delta;
impl AllocationStrategy<ConnectionCache> for LeastLoadedStrategy {
    fn allocate(&self, replica: u8) -> Option<Delta> {
        if self.replica_to_shards.borrow().contains_key(&replica) {
            return None;
        }

        let mut connections = Vec::new();
        let mut mappings = Vec::new();
        let connections_needed = self.total_shards.min(MAX_CONNECTIONS_PER_REPLICA);

        let mut rng = StdRng::seed_from_u64(self.rng_seed);
        self.connections_per_shard.borrow_mut().shuffle(&mut rng);
        self.connections_per_shard
            .borrow_mut()
            .sort_by_key(|(_, count)| *count);

        let mut assigned_shards = HashSet::with_capacity(connections_needed);

        for i in 0..connections_needed {
            let mut connections_per_shard = self.connections_per_shard.borrow_mut();
            let (shard, count) = connections_per_shard.get_mut(i).unwrap();
            connections.push(ConnectionAssignment {
                replica,
                shard: *shard,
            });
            *count += 1;
            assigned_shards.insert(*shard);
        }

        self.replica_to_shards
            .borrow_mut()
            .insert(replica, assigned_shards.clone());

        self.create_shard_mappings(
            &mut mappings,
            replica,
            assigned_shards.into_iter().collect(),
        );

        Some(Delta::Allocate {
            connections,
            mappings,
        })
    }

    fn deallocate(&self, replica: u8) -> Option<Delta> {
        let conn_shards = self.replica_to_shards.borrow_mut().remove(&replica)?;

        let mut connections = Vec::new();
        let mut mappings = Vec::new();

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

        for shard in 0..self.total_shards {
            let shard = shard as u16;
            mappings.push(ConnectionAssignment { replica, shard });
        }

        Some(Delta::Deallocate {
            connections,
            mappings,
        })
    }
}

/// Coordinator that wraps a strategy for a specific sharded state type
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
    pub fn new(strategy: A) -> Self {
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
            // TODO: broadcast to other shards.
            self.state.apply(delta);
            true
        } else {
            false
        }
    }

    pub fn deallocate(&mut self, entry: SS::Entry) -> bool {
        if let Some(delta) = self.coordinator.deallocate(entry) {
            // TODO: broadcast to other shards.
            self.state.apply(delta);
            true
        } else {
            false
        }
    }
}

/// Cache for connection state per shard
#[derive(Default)]
pub struct ConnectionCache {
    pub shard_id: u16,
    pub connections: HashMap<u8, Option<Rc<TcpSender>>>,
    pub connection_map: HashMap<u8, u16>,
}

impl ConnectionCache {
    pub fn get_connection(&self, replica: u8) -> Option<Rc<TcpSender>> {
        self.connections.get(&replica).and_then(|opt| opt.clone())
    }

    pub fn get_mapped_shard(&self, replica: u8) -> Option<u16> {
        self.connection_map.get(&replica).copied()
    }
}

impl ShardedState for ConnectionCache {
    type Entry = u8; // replica id
    type Delta = ConnectionChanges;

    fn apply(&mut self, delta: Self::Delta) {
        let shard_id = self.shard_id;
        match delta {
            ConnectionChanges::Allocate {
                connections,
                mappings,
            } => {
                for conn in connections.iter().filter(|c| c.shard == shard_id) {
                    self.connections.insert(conn.replica, None);
                }
                for mapping in &mappings {
                    self.connection_map
                        .insert(mapping.replica, mapping.conn_shard);
                }
            }
            ConnectionChanges::Deallocate {
                connections,
                mappings,
            } => {
                for conn in connections.iter().filter(|c| c.shard == shard_id) {
                    self.connections.remove(&conn.replica);
                }
                for mapping in &mappings {
                    self.connection_map.remove(&mapping.replica);
                }
            }
        }
    }
}
