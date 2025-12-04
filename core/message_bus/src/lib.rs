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
use iggy_common::{SenderKind, TcpSender};
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet}, rc::Rc,
};

const MAX_CONNECTIONS_PER_REPLICA: usize = 8;

/// A cache that can apply changesets
pub trait Cache {
    type ChangeSet;
    
    fn apply(&mut self, changes: Self::ChangeSet);
}

/// Allocation strategy that produces changesets for a specific cache type
/// Generic over C allows the same strategy impl to work with different caches
/// (if they share the same ChangeSet type)
pub trait AllocationStrategy<C> 
where C: Cache
{
    type Resource;

    fn allocate(&self, resource: Self::Resource) -> Option<C::ChangeSet>;
    fn deallocate(&self, resource: Self::Resource) -> Option<C::ChangeSet>;
}

/// Coordinator that wraps a strategy for a specific cache type
pub struct Coordinator<S, C>
where
    C: Cache,
    S: AllocationStrategy<C>,
{
    strategy: S,
    _cache: std::marker::PhantomData<C>,
}

impl<S, C> Coordinator<S, C>
where
    C: Cache,
    S: AllocationStrategy<C>,
{
    pub fn new(strategy: S) -> Self {
        Self {
            strategy,
            _cache: std::marker::PhantomData,
        }
    }

    pub fn allocate(&self, resource: S::Resource) -> Option<C::ChangeSet> {
        self.strategy.allocate(resource)
    }

    pub fn deallocate(&self, resource: S::Resource) -> Option<C::ChangeSet> {
        self.strategy.deallocate(resource)
    }
}

/// Binds a coordinator with a matching cache
pub struct ShardedAllocation<S, C>
where
    C: Cache,
    S: AllocationStrategy<C>,
{
    pub coordinator: Coordinator<S, C>,
    pub cache: C,
}

impl<S, C> ShardedAllocation<S, C>
where
    C: Cache,
    S: AllocationStrategy<C>,
{
    pub fn new(coordinator: Coordinator<S, C>, cache: C) -> Self {
        Self { coordinator, cache }
    }

    pub fn allocate(&mut self, resource: S::Resource) -> bool {
        if let Some(changes) = self.coordinator.allocate(resource) {
            self.cache.apply(changes);
            true
        } else {
            false
        }
    }

    pub fn deallocate(&mut self, resource: S::Resource) -> bool {
        if let Some(changes) = self.coordinator.deallocate(resource) {
            self.cache.apply(changes);
            true
        } else {
            false
        }
    }
}

/// Message bus parameterized by allocation strategy and cache
pub trait MessageBus {
    type Cache: Cache;
    type Strategy: AllocationStrategy<Self::Cache>;
    type ClientId;

    fn send_to_client(&self, client_id: Self::ClientId, data: Vec<u8>) -> Result<(), String>;
}

// ============================================================================
// Concrete Implementation: Connection-based allocation
// ============================================================================

/// Identifies a connection on a specific shard
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionAssignment {
    pub replica: u8,
    pub shard: u16,
}

/// Maps a source shard to the shard that owns the connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardAssignment {
    pub replica: u8,
    pub shard: u16,
    pub conn_shard: u16,
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

/// Cache for connection state per shard
pub struct ConnectionCache {
    shard_id: u16,
    connections: HashMap<u8, Option<Rc<TcpSender>>>,
    connection_map: HashMap<u8, u16>,
}

impl ConnectionCache {
    pub fn new(shard_id: u16) -> Self {
        Self {
            shard_id,
            connections: HashMap::new(),
            connection_map: HashMap::new(),
        }
    }

    pub fn get_connection(&self, replica: u8) -> Option<Rc<TcpSender>> {
        self.connections.get(&replica).and_then(|opt| opt.clone())
    }

    pub fn get_mapped_shard(&self, replica: u8) -> Option<u16> {
        self.connection_map.get(&replica).copied()
    }
}

impl Cache for ConnectionCache {
    type ChangeSet = ConnectionChanges;

    fn apply(&mut self, changes: Self::ChangeSet) {
        let shard_id = self.shard_id;
        match changes {
            ConnectionChanges::Allocate { connections, mappings } => {
                for conn in connections.iter().filter(|c| c.shard == shard_id) {
                    self.connections.insert(conn.replica, None);
                }
                for mapping in &mappings {
                    self.connection_map.insert(mapping.replica, mapping.conn_shard);
                }
            }
            ConnectionChanges::Deallocate { connections, mappings } => {
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

impl AllocationStrategy<ConnectionCache> for LeastLoadedStrategy {
    type Resource = u8;

    fn allocate(&self, replica: Self::Resource) -> Option<ConnectionChanges> {
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

        self.create_shard_mappings(&mut mappings, replica, assigned_shards.into_iter().collect());

        Some(ConnectionChanges::Allocate { connections, mappings })
    }

    fn deallocate(&self, replica: Self::Resource) -> Option<ConnectionChanges> {
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

        Some(ConnectionChanges::Deallocate { connections, mappings })
    }
}

pub struct IggyMessageBus {
    clients: HashMap<u128, SenderKind>,
    replicas: ShardedAllocation<LeastLoadedStrategy, ConnectionCache>,
    shard_id: u16,
}

impl IggyMessageBus {
    pub fn new(total_shards: usize, shard_id: u16, seed: u64) -> Self {
        Self {
            clients: HashMap::new(),
            replicas: ShardedAllocation::new(
                Coordinator::new(LeastLoadedStrategy::new(total_shards, seed)),
                ConnectionCache::new(shard_id),
            ),
            shard_id,
        }
    }

    pub fn add_replica(&mut self, replica: u8) -> bool {
        self.replicas.allocate(replica)
    }

    pub fn remove_replica(&mut self, replica: u8) -> bool {
        self.replicas.deallocate(replica)
    }

    pub fn get_replica_connection(&self, replica: u8) -> Option<Rc<TcpSender>> {
        let mapped_shard = self.replicas.cache.get_mapped_shard(replica)?;
        if mapped_shard == self.shard_id {
            self.replicas.cache.get_connection(replica)
        } else {
            None
        }
    }
}

impl MessageBus for IggyMessageBus {
    type Cache = ConnectionCache;
    type Strategy = LeastLoadedStrategy;
    type ClientId = u128;

    fn send_to_client(&self, _client_id: Self::ClientId, _data: Vec<u8>) -> Result<(), String> {
        // TODO: Implementation
        Ok(())
    }
}
