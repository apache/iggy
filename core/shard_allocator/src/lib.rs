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

//! `shard_allocator`: decide which CPU cores each shard lives on.
//!
//! The server makes many shards and wants each one to run on its own
//! core so they do not fight over CPU time. This crate reads the
//! operator's choice ([`CpuAllocation`] from the config), looks at the
//! real machine with `hwloc`, and hands back one [`ShardInfo`] per
//! shard. On Linux it also pins each shard's thread to its core and
//! pins memory to the right NUMA node, so memory stays close and fast.

use cpu_allocation::{CpuAllocation, NumaConfig, allowed_cpus};
use hwlocality::Topology;
use hwlocality::bitmap::SpecializedBitmapRef;
use hwlocality::cpu::cpuset::CpuSet;
use hwlocality::memory::binding::{MemoryBindingFlags, MemoryBindingPolicy};
use hwlocality::object::types::ObjectType::{self, NUMANode};
#[cfg(target_os = "linux")]
use nix::{sched::sched_setaffinity, unistd::Pid};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread::available_parallelism;
use tracing::info;

/// All the ways shard allocation can go wrong: machine has no NUMA,
/// hwloc cannot read the topology, the operator asked for more cores
/// than exist, or the OS refused to pin a thread or its memory.
#[derive(Debug, thiserror::Error)]
pub enum ShardingError {
    #[error("Failed to detect topology: {msg}")]
    TopologyDetection { msg: String },

    #[error("There is no NUMA node on this server")]
    NoNumaNodes,

    #[error("No Topology")]
    NoTopology,

    #[error("Binding Failed")]
    BindingFailed,

    #[error("Insufficient cores on node {node}: requested {requested}, only {available} available")]
    InsufficientCores {
        requested: usize,
        available: usize,
        node: usize,
    },

    #[error(
        "Requested {requested} shard(s) but only {available} CPU core(s) are allowed for this process (affinity/cpuset mask)"
    )]
    AllowedCpusExceeded { requested: usize, available: usize },

    #[error(
        "Configured CPU {cpu} is outside the set of cores allowed for this process (affinity/cpuset mask)"
    )]
    CpuNotAllowed { cpu: usize },

    #[error("Invalid NUMA node: requested {requested}, only available {available} node")]
    InvalidNode { requested: usize, available: usize },

    #[error("Other error: {msg}")]
    Other { msg: String },
}

/// A snapshot of the machine's NUMA layout, read once from `hwloc`.
///
/// Holds how many NUMA nodes there are and, for each node, how many
/// real (physical) cores and how many threads (logical cores) it has.
#[derive(Debug)]
pub struct NumaTopology {
    topology: Topology,
    node_count: usize,
    physical_cores_per_node: Vec<usize>,
    logical_cores_per_node: Vec<usize>,
}

impl NumaTopology {
    /// Ask `hwloc` to read this machine's NUMA layout right now.
    /// Errors if hwloc fails or the machine reports no NUMA nodes.
    pub fn detect() -> Result<NumaTopology, ShardingError> {
        let topology =
            Topology::new().map_err(|e| ShardingError::TopologyDetection { msg: e.to_string() })?;

        let numa_nodes: Vec<_> = topology.objects_with_type(NUMANode).collect();

        let node_count = numa_nodes.len();

        if node_count == 0 {
            return Err(ShardingError::NoNumaNodes);
        }

        let mut physical_cores_per_node = Vec::new();
        let mut logical_cores_per_node = Vec::new();

        for node in numa_nodes {
            let cpuset = node.cpuset().ok_or(ShardingError::TopologyDetection {
                msg: "NUMA node has no CPU set".to_string(),
            })?;

            let logical_cores = cpuset.weight().unwrap_or(0);

            let physical_cores = topology
                .objects_with_type(ObjectType::Core)
                .filter(|core| {
                    if let Some(core_cpuset) = core.cpuset() {
                        !(cpuset & core_cpuset).is_empty()
                    } else {
                        false
                    }
                })
                .count();

            physical_cores_per_node.push(physical_cores);
            logical_cores_per_node.push(logical_cores);
        }

        Ok(Self {
            topology,
            node_count,
            physical_cores_per_node,
            logical_cores_per_node,
        })
    }

    /// How many real cores this node has. Returns `0` if no such node.
    pub fn physical_cores_for_node(&self, node: usize) -> usize {
        self.physical_cores_per_node.get(node).copied().unwrap_or(0)
    }

    /// How many threads (logical cores) this node has, hyperthreads
    /// included. Returns `0` if no such node.
    pub fn logical_cores_for_node(&self, node: usize) -> usize {
        self.logical_cores_per_node.get(node).copied().unwrap_or(0)
    }

    fn filter_physical_cores(&self, node_cpuset: CpuSet) -> CpuSet {
        let mut physical_cpuset = CpuSet::new();
        for core in self.topology.objects_with_type(ObjectType::Core) {
            if let Some(core_cpuset) = core.cpuset() {
                let intersection = node_cpuset.clone() & core_cpuset;
                if !intersection.is_empty()
                    && let Some(first_cpu) = intersection.iter_set().min()
                {
                    physical_cpuset.set(first_cpu)
                }
            }
        }
        physical_cpuset
    }

    fn get_cpuset_for_node(
        &self,
        node_id: usize,
        avoid_hyperthread: bool,
    ) -> Result<CpuSet, ShardingError> {
        let node = self
            .topology
            .objects_with_type(ObjectType::NUMANode)
            .nth(node_id)
            .ok_or(ShardingError::InvalidNode {
                requested: node_id,
                available: self.node_count,
            })?;

        let cpuset_ref = node.cpuset().ok_or(ShardingError::TopologyDetection {
            msg: format!("Node {} has no CPU set", node_id),
        })?;

        let cpuset = SpecializedBitmapRef::to_owned(&cpuset_ref);

        if avoid_hyperthread {
            Ok(self.filter_physical_cores(cpuset))
        } else {
            Ok(cpuset)
        }
    }
}

/// One shard's home: which CPU cores it may run on, and which NUMA
/// node its memory should sit near (`None` means do not pin memory).
#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub cpu_set: HashSet<usize>,
    pub numa_node: Option<usize>,
}

impl ShardInfo {
    /// Pin the calling thread to this shard's cores. On non-Linux this
    /// does nothing (no-op). Empty core set also does nothing.
    pub fn bind_cpu(&self) -> Result<(), ShardingError> {
        #[cfg(target_os = "linux")]
        {
            if self.cpu_set.is_empty() {
                return Ok(());
            }

            let mut cpuset = nix::sched::CpuSet::new();
            for &cpu in &self.cpu_set {
                cpuset.set(cpu).map_err(|_| ShardingError::BindingFailed)?;
            }

            sched_setaffinity(Pid::from_raw(0), &cpuset).map_err(|e| {
                tracing::error!("Failed to set CPU affinity: {:?}", e);
                ShardingError::BindingFailed
            })?;

            info!("Thread bound to CPUs: {:?}", self.cpu_set);
        }

        #[cfg(not(target_os = "linux"))]
        {
            tracing::debug!("CPU affinity binding skipped on non-Linux platform");
        }

        Ok(())
    }

    /// Pin the calling thread's memory to this shard's NUMA node so
    /// allocations stay local and fast. Does nothing if no node is set.
    pub fn bind_memory(&self) -> Result<(), ShardingError> {
        if let Some(node_id) = self.numa_node {
            let topology = Topology::new().map_err(|err| ShardingError::TopologyDetection {
                msg: err.to_string(),
            })?;

            let node = topology
                .objects_with_type(ObjectType::NUMANode)
                .nth(node_id)
                .ok_or(ShardingError::InvalidNode {
                    requested: node_id,
                    available: topology.objects_with_type(ObjectType::NUMANode).count(),
                })?;

            if let Some(nodeset) = node.nodeset() {
                topology
                    .bind_memory(
                        nodeset,
                        MemoryBindingPolicy::Bind,
                        MemoryBindingFlags::THREAD | MemoryBindingFlags::STRICT,
                    )
                    .map_err(|err| {
                        tracing::error!("Failed to bind memory {:?}", err);
                        ShardingError::BindingFailed
                    })?;

                info!("Memory bound to NUMA node {node_id}");
            }
        }

        Ok(())
    }
}

/// One shard per core drawn from `allowed`, each pinned to its own core.
fn pinned_from_allowed(allowed: &[usize], count: usize) -> Result<Vec<ShardInfo>, ShardingError> {
    if count > allowed.len() {
        return Err(ShardingError::AllowedCpusExceeded {
            requested: count,
            available: allowed.len(),
        });
    }

    Ok(allowed
        .iter()
        .take(count)
        .map(|&cpu_id| ShardInfo {
            cpu_set: HashSet::from([cpu_id]),
            numa_node: None,
        })
        .collect())
}

/// `count` shards with no CPU affinity: `bind_cpu` becomes a no-op and the
/// kernel scheduler places shard threads freely. This is the right mode when
/// the process shares its cores with other workloads (e.g. a multi-tenant
/// host slicing CPU via cgroup quotas), where per-process pinning to the same
/// low-numbered cores would pile every tenant onto one core.
fn unpinned(count: usize) -> Vec<ShardInfo> {
    (0..count)
        .map(|_| ShardInfo {
            cpu_set: HashSet::new(),
            numa_node: None,
        })
        .collect()
}

/// Turns the operator's [`CpuAllocation`] choice into a concrete plan
/// of shards. Reads the NUMA topology only when the choice needs it.
pub struct ShardAllocator {
    allocation: CpuAllocation,
    pin_cores: bool,
    topology: Option<Arc<NumaTopology>>,
}

impl ShardAllocator {
    /// Build an allocator for the given choice. Only `NumaAware` reads
    /// the machine topology up front; the simpler modes do not.
    pub fn new(
        allocation: &CpuAllocation,
        pin_cores: bool,
    ) -> Result<ShardAllocator, ShardingError> {
        let topology = if matches!(allocation, CpuAllocation::NumaAware(_)) {
            let numa_topology = NumaTopology::detect()?;

            Some(Arc::new(numa_topology))
        } else {
            None
        };

        Ok(Self {
            allocation: allocation.clone(),
            pin_cores,
            topology,
        })
    }

    /// Produce the final list of shards, one [`ShardInfo`] each, based
    /// on the chosen [`CpuAllocation`]. This is the main entry point.
    pub fn to_shard_assignments(&self) -> Result<Vec<ShardInfo>, ShardingError> {
        match &self.allocation {
            CpuAllocation::All => {
                // `available_parallelism` already accounts for both the
                // affinity mask and any cgroup CPU quota, so a
                // quota-restricted process gets proportionally fewer shards.
                let available_cpus = available_parallelism()
                    .map_err(|err| ShardingError::Other {
                        msg: format!("Failed to get available_parallelism: {:?}", err),
                    })?
                    .get();

                if !self.pin_cores {
                    info!("Using all available CPU cores ({available_cpus} shards, unpinned)");
                    return Ok(unpinned(available_cpus));
                }

                let allowed = allowed_cpus();
                let shard_assignments =
                    pinned_from_allowed(&allowed, available_cpus.min(allowed.len()))?;

                info!(
                    "Using all available CPU cores ({} shards pinned within allowed set {:?})",
                    shard_assignments.len(),
                    allowed
                );

                Ok(shard_assignments)
            }
            CpuAllocation::Count(count) => {
                if !self.pin_cores {
                    info!("Using {count} shard(s), unpinned");
                    return Ok(unpinned(*count));
                }

                let allowed = allowed_cpus();
                let shard_assignments = pinned_from_allowed(&allowed, *count)?;
                let cores: Vec<usize> = shard_assignments
                    .iter()
                    .flat_map(|shard| shard.cpu_set.iter().copied())
                    .collect();

                info!("Using {count} shard(s) with affinity to cores {cores:?}");

                Ok(shard_assignments)
            }
            CpuAllocation::Range(start, end) => {
                if !self.pin_cores {
                    info!(
                        "Using {} shard(s) for range {start}..{end}, unpinned",
                        end - start
                    );
                    return Ok(unpinned(end - start));
                }

                // An explicit range is operator intent for exact cores; honor
                // it as-is but fail fast when a core is outside the allowed
                // set instead of letting `sched_setaffinity` EINVAL later.
                let allowed = allowed_cpus();
                if let Some(cpu) = (*start..*end).find(|cpu| !allowed.contains(cpu)) {
                    return Err(ShardingError::CpuNotAllowed { cpu });
                }

                let shard_assignments = (*start..*end)
                    .map(|cpu_id| ShardInfo {
                        cpu_set: HashSet::from([cpu_id]),
                        numa_node: None,
                    })
                    .collect();

                info!(
                    "Using {} shards with affinity to cores {start}..{end}",
                    end - start
                );

                Ok(shard_assignments)
            }
            CpuAllocation::NumaAware(numa_config) => {
                let topology = self.topology.as_ref().ok_or(ShardingError::NoTopology)?;
                let assignments = self.compute_numa_assignments(topology, numa_config)?;

                if !self.pin_cores {
                    tracing::warn!(
                        "pin_cores = false with a NUMA-aware cpu_allocation: NUMA bindings are ignored"
                    );
                    return Ok(unpinned(assignments.len()));
                }

                Ok(assignments)
            }
        }
    }

    fn compute_numa_assignments(
        &self,
        topology: &NumaTopology,
        numa: &NumaConfig,
    ) -> Result<Vec<ShardInfo>, ShardingError> {
        let nodes = if numa.nodes.is_empty() {
            (0..topology.node_count).collect()
        } else {
            numa.nodes.clone()
        };

        let cores_per_node = if numa.cores_per_node == 0 {
            if numa.avoid_hyperthread {
                topology.physical_cores_for_node(nodes[0])
            } else {
                topology.logical_cores_for_node(nodes[0])
            }
        } else {
            numa.cores_per_node
        };

        let mut shard_infos = Vec::new();

        let node_cpus: Vec<Vec<usize>> = nodes
            .iter()
            .map(|&node_id| {
                let cpuset = topology.get_cpuset_for_node(node_id, numa.avoid_hyperthread)?;
                Ok(cpuset.iter_set().map(usize::from).collect())
            })
            .collect::<Result<_, ShardingError>>()?;

        for (idx, &node_id) in nodes.iter().enumerate() {
            let available_cpus = &node_cpus[idx];

            let cores_to_use: Vec<usize> = available_cpus
                .iter()
                .take(cores_per_node)
                .copied()
                .collect();

            if cores_to_use.len() < cores_per_node {
                return Err(ShardingError::InsufficientCores {
                    requested: cores_per_node,
                    available: available_cpus.len(),
                    node: node_id,
                });
            }

            for cpu_id in cores_to_use {
                shard_infos.push(ShardInfo {
                    cpu_set: HashSet::from([cpu_id]),
                    numa_node: Some(node_id),
                });
            }
        }

        info!(
            "Using {} shards with {} NUMA node, {} cores per node, and avoid hyperthread {}",
            shard_infos.len(),
            nodes.len(),
            cores_per_node,
            numa.avoid_hyperthread
        );

        Ok(shard_infos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_from_allowed_draws_cores_from_the_allowed_set() {
        let allowed = vec![2, 3, 6, 7];
        let shards = pinned_from_allowed(&allowed, 2).unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].cpu_set, HashSet::from([2]));
        assert_eq!(shards[1].cpu_set, HashSet::from([3]));
        assert!(shards.iter().all(|shard| shard.numa_node.is_none()));
    }

    #[test]
    fn pinned_from_allowed_rejects_more_shards_than_allowed_cores() {
        let allowed = vec![0, 1];
        let err = pinned_from_allowed(&allowed, 3).unwrap_err();
        assert!(matches!(
            err,
            ShardingError::AllowedCpusExceeded {
                requested: 3,
                available: 2,
            }
        ));
    }

    #[test]
    fn unpinned_shards_have_empty_cpu_sets() {
        let shards = unpinned(4);
        assert_eq!(shards.len(), 4);
        assert!(shards.iter().all(|shard| shard.cpu_set.is_empty()));
        assert!(shards.iter().all(|shard| shard.numa_node.is_none()));
    }

    #[test]
    fn count_without_pinning_yields_unpinned_shards() {
        let allocator = ShardAllocator::new(&CpuAllocation::Count(1), false).unwrap();
        let shards = allocator.to_shard_assignments().unwrap();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].cpu_set.is_empty());
    }

    #[test]
    fn count_with_pinning_stays_within_the_allowed_set() {
        let allocator = ShardAllocator::new(&CpuAllocation::Count(1), true).unwrap();
        let shards = allocator.to_shard_assignments().unwrap();
        assert_eq!(shards.len(), 1);
        let allowed = allowed_cpus();
        assert!(shards[0].cpu_set.iter().all(|cpu| allowed.contains(cpu)));
    }

    #[test]
    fn range_without_pinning_keeps_shard_count() {
        let allocator = ShardAllocator::new(&CpuAllocation::Range(0, 1), false).unwrap();
        let shards = allocator.to_shard_assignments().unwrap();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].cpu_set.is_empty());
    }

    #[test]
    fn bind_cpu_with_empty_set_is_a_no_op() {
        let shard = ShardInfo {
            cpu_set: HashSet::new(),
            numa_node: None,
        };
        assert!(shard.bind_cpu().is_ok());
    }
}
