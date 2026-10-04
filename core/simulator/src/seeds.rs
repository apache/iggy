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

//! Per-stream PRNG seeds derived from the run's single seed.

use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::SeedableRng;

/// One PRNG seed per independent stream, all derived from the run's single seed.
///
/// Every consumer that draws needs a stream nothing else shares. A shared stream
/// couples them: adding a draw anywhere shifts every later draw everywhere, so
/// enabling crash injection would change the network trace, and a bug found at one
/// seed would vanish once an unrelated draw was added.
///
/// Children are DRAWN from a parent rather than XOR-salted off the seed. Salting
/// needs a distinct constant per stream and silently reuses the parent seed when one
/// is forgotten, which is how the workload and the packet simulator came to share a
/// stream. Drawing needs no constants and cannot hand back a stream without naming
/// it.
///
/// Pure in `seed`, so every caller derives the same children and nothing has to
/// thread a parent PRNG through the constructors.
#[derive(Debug, Clone, Copy)]
pub struct SimSeeds {
    /// Packet delays, drops, replays, partitions and clogs.
    pub network: u64,
    /// Workload op sampling: which action, which entity, which argument.
    pub workload: u64,
    /// Task-poll and timer-fire ordering in `DetExecutor`. Separate so shaking out
    /// order-dependence never perturbs the network or workload traces.
    pub executor: u64,
    /// Which shard of a replica receives each inbound packet, modelling the
    /// coordinator's connection homing. One draw per delivered packet, the
    /// highest-rate stream here.
    pub entry_shard: u64,
    /// Crash and restart scheduling. Separate so a run with both probabilities at
    /// zero draws nothing and replays bit-identically to one with no injector.
    pub faults: u64,
    /// `PacketSimulatorOptions::swarm`'s parameter draw. Separate from `network`
    /// because one seed both picks the network's shape and drives it, and
    /// correlating the loss probability with the loss events would collapse what the
    /// swarm explores to a diagonal of the parameter space.
    pub swarm: u64,
}

impl SimSeeds {
    /// Derive every stream's seed from the run's seed.
    ///
    /// APPEND fields, never insert. Each draw advances the parent, so a field added
    /// in the middle moves every child after it and re-locks every seeded baseline.
    #[must_use]
    pub fn derive(seed: u64) -> Self {
        let mut parent = Xoshiro256PlusPlus::seed_from_u64(seed);
        Self {
            network: parent.random(),
            workload: parent.random(),
            executor: parent.random(),
            entry_shard: parent.random(),
            faults: parent.random(),
            swarm: parent.random(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SimSeeds;
    use std::collections::HashSet;

    /// Every field its own draw. A copy-paste assigning one child twice would
    /// recreate exactly the shared-stream coupling this type exists to prevent, and
    /// nothing else in the harness would fail.
    #[test]
    fn every_stream_gets_its_own_seed() {
        let seeds = SimSeeds::derive(0xDEAD_BEEF);
        let all = [
            seeds.network,
            seeds.workload,
            seeds.executor,
            seeds.entry_shard,
            seeds.faults,
            seeds.swarm,
        ];
        let unique: HashSet<u64> = all.iter().copied().collect();
        assert_eq!(unique.len(), all.len(), "two streams share a seed: {all:?}");
    }

    /// Pure in the seed, which is what lets each constructor derive on its own
    /// instead of threading a parent PRNG through every call site.
    #[test]
    fn derivation_is_a_pure_function_of_the_seed() {
        assert_eq!(SimSeeds::derive(7).workload, SimSeeds::derive(7).workload);
        assert_ne!(SimSeeds::derive(7).workload, SimSeeds::derive(8).workload);
    }
}
