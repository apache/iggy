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

//! Simulated clusters whose geo topics replicate through their buckets.
//!
//! Each cluster is a [`Simulator`] of its own, with its own executor, clock,
//! network and seeds. No packet crosses clusters: they share only the
//! buckets. Each cluster owns one bucket and reads every other one as a
//! peer. One world step steps every cluster once, in an order that the
//! world's PRNG draws, so a run replays from its seed.

use std::time::Duration;

use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::SeedableRng;
use server_common::Message;

use iggy_binary_protocol::ReplyHeader;

use crate::Simulator;
use crate::bucket::{BucketFaults, ClusterId, SimBucket};
use crate::geo::SimGeo;
use crate::packet::PacketSimulatorOptions;

/// The origin names of the clusters, by cluster index.
pub const ORIGINS: [&str; 2] = ["dc-a", "dc-b"];

/// How a world is built.
#[derive(Debug, Clone)]
pub struct GeoWorldOptions {
    pub seed: u64,
    /// One or two.
    pub clusters: u8,
    /// Replicas per cluster.
    pub replicas: u8,
    /// The clients of each cluster. The clusters share no network, so they
    /// can use the same ids.
    pub clients: Vec<u128>,
    /// The faults of every bucket.
    pub faults: BucketFaults,
    /// How long a peer keeps a deleted topic's shadow after the origin
    /// tombstoned it.
    pub tombstone_grace: Duration,
}

/// Per-stream seeds of a world, drawn from its seed. Each cluster keeps its
/// own [`crate::seeds::SimSeeds`], derived from its seed here.
#[derive(Debug, Clone, Copy)]
pub struct GeoSeeds {
    /// The order in which the clusters step.
    pub world: u64,
    pub clusters: [u64; 2],
    /// The faults of each cluster's bucket.
    pub stores: [u64; 2],
}

impl GeoSeeds {
    /// APPEND fields, never insert, as [`crate::seeds::SimSeeds::derive`]
    /// says.
    #[must_use]
    pub fn derive(seed: u64) -> Self {
        let mut parent = Xoshiro256PlusPlus::seed_from_u64(seed);
        Self {
            world: parent.random(),
            clusters: [parent.random(), parent.random()],
            stores: [parent.random(), parent.random()],
        }
    }
}

/// One or two clusters with geo topics.
pub struct GeoWorld {
    clusters: Vec<Simulator>,
    buckets: Vec<SimBucket>,
    prng: Xoshiro256PlusPlus,
}

impl GeoWorld {
    /// # Panics
    /// When `options` asks for no cluster or for more than two.
    #[must_use]
    pub fn new(options: &GeoWorldOptions) -> Self {
        let count = usize::from(options.clusters);
        assert!((1..=ORIGINS.len()).contains(&count), "one or two clusters");
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let seeds = GeoSeeds::derive(options.seed);
        let buckets: Vec<SimBucket> = (0..count)
            .map(|index| SimBucket::new(cluster_id(index), seeds.stores[index], options.faults))
            .collect();
        let clusters = (0..count)
            .map(|index| {
                // The dispatch shell, because a geo partition takes only the
                // sends that the server's handler hands to the upload path.
                let mut sim = Simulator::with_shards_shell(
                    usize::from(options.replicas),
                    1,
                    options.clients.iter().copied(),
                    PacketSimulatorOptions {
                        node_count: options.replicas,
                        client_count: u8::try_from(options.clients.len())
                            .expect("client count fits u8"),
                        seed: seeds.clusters[index],
                        ..PacketSimulatorOptions::default()
                    },
                );
                let mut geo =
                    SimGeo::new(ORIGINS[index], cluster_id(index), buckets[index].clone())
                        .with_tombstone_grace(options.tombstone_grace);
                for (peer, bucket) in buckets.iter().enumerate() {
                    if peer != index {
                        geo = geo.with_peer(ORIGINS[peer], bucket.clone());
                    }
                }
                sim.enable_geo(geo);
                sim
            })
            .collect();
        Self {
            clusters,
            buckets,
            prng: Xoshiro256PlusPlus::seed_from_u64(seeds.world),
        }
    }

    #[must_use]
    pub const fn cluster_count(&self) -> usize {
        self.clusters.len()
    }

    #[must_use]
    pub fn sim(&self, cluster: usize) -> &Simulator {
        &self.clusters[cluster]
    }

    pub fn sim_mut(&mut self, cluster: usize) -> &mut Simulator {
        &mut self.clusters[cluster]
    }

    #[must_use]
    pub fn bucket(&self, cluster: usize) -> &SimBucket {
        &self.buckets[cluster]
    }

    /// Step every cluster once, in a drawn order, and return each cluster's
    /// replies. Then each cluster learns which cluster owns each peer bucket,
    /// as an identity check that reads it would.
    pub fn step(&mut self) -> Vec<Vec<Message<ReplyHeader>>> {
        let count = self.clusters.len();
        let first = if count > 1 {
            self.prng.random_range(0..count)
        } else {
            0
        };
        let mut replies = vec![Vec::new(); count];
        for offset in 0..count {
            let index = (first + offset) % count;
            replies[index] = self.clusters[index].step();
        }
        self.verify_peers();
        replies
    }

    fn verify_peers(&mut self) {
        let owners: Vec<Option<u128>> = self.clusters.iter().map(Simulator::cluster_uid).collect();
        for cluster in &mut self.clusters {
            cluster.verify_geo_peers(|origin| {
                ORIGINS
                    .iter()
                    .position(|candidate| *candidate == origin)
                    .and_then(|index| owners.get(index).copied().flatten())
            });
        }
    }
}

fn cluster_id(index: usize) -> ClusterId {
    ClusterId(u8::try_from(index).expect("cluster index fits u8"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::GeoSeeds;

    #[test]
    fn given_a_seed_when_deriving_should_give_each_stream_its_own_seed() {
        let seeds = GeoSeeds::derive(0xDEAD_BEEF);
        let all = [
            seeds.world,
            seeds.clusters[0],
            seeds.clusters[1],
            seeds.stores[0],
            seeds.stores[1],
        ];

        assert_eq!(all.iter().copied().collect::<HashSet<_>>().len(), all.len());
        assert_eq!(GeoSeeds::derive(7).world, GeoSeeds::derive(7).world);
    }
}
