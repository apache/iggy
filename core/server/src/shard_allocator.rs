/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use configs::sharding::{CpuAllocation, NumaConfig};
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

    #[error("Invalid NUMA node: requested {requested}, only available {available} node")]
    InvalidNode { requested: usize, available: usize },

    #[error("Other error: {msg}")]
    Other { msg: String },
}

#[derive(Debug)]
pub struct NumaTopology {
    topology: Topology,
    node_count: usize,
    physical_cores_per_node: Vec<usize>,
    logical_cores_per_node: Vec<usize>,
}

impl NumaTopology {
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

    pub fn physical_cores_for_node(&self, node: usize) -> usize {
        self.physical_cores_per_node.get(node).copied().unwrap_or(0)
    }

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

#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub cpu_set: HashSet<usize>,
    pub numa_node: Option<usize>,
}

impl ShardInfo {
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

pub struct ShardAllocator {
    allocation: CpuAllocation,
    topology: Option<Arc<NumaTopology>>,
}

impl ShardAllocator {
    pub fn new(allocation: &CpuAllocation) -> Result<ShardAllocator, ShardingError> {
        let topology = if matches!(allocation, CpuAllocation::NumaAware(_)) {
            let numa_topology = NumaTopology::detect()?;

            Some(Arc::new(numa_topology))
        } else {
            None
        };

        Ok(Self {
            allocation: allocation.clone(),
            topology,
        })
    }

    pub fn to_shard_assignments(&self) -> Result<Vec<ShardInfo>, ShardingError> {
        match &self.allocation {
            CpuAllocation::All => {
                let available_cpus = available_parallelism()
                    .map_err(|err| ShardingError::Other {
                        msg: format!("Failed to get available_parallelism: {:?}", err),
                    })?
                    .get();

                let shard_assignments: Vec<_> = (0..available_cpus)
                    .map(|cpu_id| ShardInfo {
                        cpu_set: HashSet::from([cpu_id]),
                        numa_node: None,
                    })
                    .collect();

                info!(
                    "Using all available CPU cores ({} shards with affinity)",
                    shard_assignments.len()
                );

                Ok(shard_assignments)
            }
            CpuAllocation::Count(count) => {
                let shard_assignments = (0..*count)
                    .map(|cpu_id| ShardInfo {
                        cpu_set: HashSet::from([cpu_id]),
                        numa_node: None,
                    })
                    .collect();

                info!("Using {count} shards with affinity to cores 0..{count}");

                Ok(shard_assignments)
            }
            CpuAllocation::Range(start, end) => {
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
                self.compute_numa_assignments(topology, numa_config)
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
