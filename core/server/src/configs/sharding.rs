/* Licensed to the Apache Software Foundation (ASF) under one
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

use hwlocality::Topology;
use hwlocality::bitmap::SpecializedBitmapRef;
use hwlocality::cpu::cpuset::CpuSet;
use hwlocality::memory::binding::{MemoryBindingFlags, MemoryBindingPolicy};
use hwlocality::object::types::ObjectType::{self, NUMANode};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::available_parallelism;
use tracing::info;

use crate::server_error::ServerError;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct ShardingConfig {
    #[serde(default)]
    pub cpu_allocation: CpuAllocation,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum CpuAllocation {
    #[default]
    All,
    Count(usize),
    Range(usize, usize),
    NumaAware(NumaConfig),
}

/// NUMA specific configuration
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NumaConfig {
    /// Which NUMA nodes to use (empty = auto-detect all)
    pub nodes: Vec<usize>,
    /// Cores per node to use (0 = use all available)
    pub cores_per_node: usize,
    /// skip hyperthread sibling
    pub avoid_hyperthread: bool,
}

impl NumaConfig {
    pub fn validate(&self, topology: &NumaTopology) -> Result<(), ServerError> {
        let available_nodes = topology.node_count;

        if available_nodes == 0 {
            return Err(ServerError::NoNumaNodes);
        }

        for &node in &self.nodes {
            if node >= available_nodes {
                return Err(ServerError::InvalidNode {
                    requested: node,
                    available: available_nodes,
                });
            }
        }

        // Validate core per node
        if self.cores_per_node > 0 {
            for &node in &self.nodes {
                let available_cores = if self.avoid_hyperthread {
                    topology.physical_cores_for_node(node)
                } else {
                    topology.logical_cores_for_node(node)
                };

                info!(
                    "core_per_node: {}, available_cores: {}",
                    self.cores_per_node, available_cores
                );

                if self.cores_per_node > available_cores {
                    return Err(ServerError::InsufficientCores {
                        requested: self.cores_per_node,
                        available: available_cores,
                        node,
                    });
                }
            }
        }

        Ok(())
    }

    // pub fn or_fallback(&self, topology: &NumaTopology) -> CpuAllocation {
    //     if topology.node_count < 2 {
    //         tracing::warn!(
    //             "NUMA requested but only {} node detected, falling back to count",
    //             topology.node_count
    //         );
    //     }
    //
    //     todo!()
    // }
}

impl CpuAllocation {
    fn parse_numa(s: &str) -> Result<CpuAllocation, String> {
        let params = s
            .strip_prefix("numa:")
            .ok_or_else(|| "Numa config must start with 'numa:'".to_string())?;

        if params == "auto" {
            return Ok(CpuAllocation::NumaAware(NumaConfig {
                nodes: vec![],
                cores_per_node: 0,
                avoid_hyperthread: true,
            }));
        }

        let mut nodes = Vec::new();
        let mut cores_per_node = 0;
        let mut avoid_hyperthread = true;

        for param in params.split(';') {
            let kv: Vec<&str> = param.split('=').collect();
            if kv.len() != 2 {
                return Err(format!(
                    "Invalid NUMA parameter: '{param}', only available: 'auto'"
                ));
            }

            match kv[0] {
                "nodes" => {
                    nodes = kv[1]
                        .split(',')
                        .map(|n| {
                            n.parse::<usize>()
                                .map_err(|_| format!("Invalid node number: {n}"))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                }
                "cores" => {
                    cores_per_node = kv[1]
                        .parse::<usize>()
                        .map_err(|_| format!("Invalid cores value: {}", kv[1]))?;
                }
                "no_ht" => {
                    avoid_hyperthread = kv[1]
                        .parse::<bool>()
                        .map_err(|_| format!("Invalid no ht value: {}", kv[1]))?;
                }
                _ => {
                    return Err(format!(
                        "Unknown NUMA parameter: {}, example: numa:nodes=0;cores=4;no_ht=true",
                        kv[0]
                    ));
                }
            }
        }

        Ok(CpuAllocation::NumaAware(NumaConfig {
            nodes,
            cores_per_node,
            avoid_hyperthread,
        }))
    }
}

#[derive(Debug)]
pub struct NumaTopology {
    topology: Topology,
    node_count: usize,
    physical_cores_per_node: Vec<usize>,
    logical_cores_per_node: Vec<usize>,
}

impl NumaTopology {
    pub fn detect() -> Result<NumaTopology, ServerError> {
        let topology =
            Topology::new().map_err(|e| ServerError::TopologyDetection { msg: e.to_string() })?;

        let numa_nodes: Vec<_> = topology.objects_with_type(NUMANode).collect();

        let node_count = numa_nodes.len();

        if node_count == 0 {
            return Err(ServerError::NoNumaNodes);
        }

        let mut physical_cores_per_node = Vec::new();
        let mut logical_cores_per_node = Vec::new();

        for node in numa_nodes {
            let cpuset = node.cpuset().ok_or(ServerError::TopologyDetection {
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
                if !intersection.is_empty() {
                    // Take the minimum (first) CPU ID for consistency
                    if let Some(first_cpu) = intersection.iter_set().min() {
                        physical_cpuset.set(first_cpu)
                    }
                }
            }
        }
        physical_cpuset
    }

    /// Get CPU set for a NUMA node
    fn get_cpuset_for_node(
        &self,
        node_id: usize,
        avoid_hyperthread: bool,
    ) -> Result<CpuSet, ServerError> {
        let node = self
            .topology
            .objects_with_type(ObjectType::NUMANode)
            .nth(node_id)
            .ok_or(ServerError::InvalidNode {
                requested: node_id,
                available: self.node_count,
            })?;

        let cpuset_ref = node.cpuset().ok_or(ServerError::TopologyDetection {
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
    /// CPUs this shard should use
    pub cpu_set: HashSet<usize>,
    /// NUMA node
    pub numa_node: Option<usize>,
}

impl ShardInfo {
    pub fn bind_memory(&self) -> Result<(), ServerError> {
        if let Some(node_id) = self.numa_node {
            let topology = Topology::new().map_err(|err| ServerError::TopologyDetection {
                msg: err.to_string(),
            })?;

            let node = topology
                .objects_with_type(ObjectType::NUMANode)
                .nth(node_id)
                .ok_or(ServerError::InvalidNode {
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
                        ServerError::BindingFailed
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
    pub fn new(allocation: &CpuAllocation) -> Result<ShardAllocator, ServerError> {
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

    pub fn to_shard_assignments(&self) -> Result<Vec<ShardInfo>, ServerError> {
        match &self.allocation {
            CpuAllocation::All => {
                let available_cpus = available_parallelism()
                    .map_err(|err| ServerError::Other {
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
                let topology = self.topology.as_ref().ok_or(ServerError::NoTopology)?;
                self.compute_numa_assignments(topology, numa_config)
            }
        }
    }

    fn compute_numa_assignments(
        &self,
        topology: &NumaTopology,
        numa: &NumaConfig,
    ) -> Result<Vec<ShardInfo>, ServerError> {
        // Determine  which noes to use
        let nodes = if numa.nodes.is_empty() {
            // Auto: use all nodes
            (0..topology.node_count).collect()
        } else {
            numa.nodes.clone()
        };

        // Determine cores per node
        let cores_per_node = if numa.cores_per_node == 0 {
            // Auto: use all available
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
            .collect::<Result<_, ServerError>>()?;

        // For each node, create one shard per core (thread-per-core)
        for (idx, &node_id) in nodes.iter().enumerate() {
            let available_cpus = &node_cpus[idx];

            // Take first core_per_node CPUs from this node
            let cores_to_use: Vec<usize> = available_cpus
                .iter()
                .take(cores_per_node)
                .copied()
                .collect();

            if cores_to_use.len() < cores_per_node {
                return Err(ServerError::InsufficientCores {
                    requested: cores_per_node,
                    available: available_cpus.len(),
                    node: node_id,
                });
            }

            // Create one shard per core
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

impl FromStr for CpuAllocation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all" => Ok(CpuAllocation::All),
            s if s.starts_with("numa:") => Self::parse_numa(s),
            s if s.contains("..") => {
                let parts: Vec<&str> = s.split("..").collect();
                if parts.len() != 2 {
                    return Err(format!("Invalid range format: {s}. Expected 'start..end'"));
                }
                let start = parts[0]
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid start value: {}", parts[0]))?;
                let end = parts[1]
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid end value: {}", parts[1]))?;
                Ok(CpuAllocation::Range(start, end))
            }
            s => {
                let count = s
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid shard count: {s}"))?;
                Ok(CpuAllocation::Count(count))
            }
        }
    }
}

impl Serialize for CpuAllocation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CpuAllocation::All => serializer.serialize_str("all"),
            CpuAllocation::Count(n) => serializer.serialize_u64(*n as u64),
            CpuAllocation::Range(start, end) => {
                serializer.serialize_str(&format!("{start}..{end}"))
            }
            CpuAllocation::NumaAware(numa) => {
                if numa.nodes.is_empty() && numa.cores_per_node == 0 {
                    serializer.serialize_str("numa:auto")
                } else {
                    let nodes_str = numa
                        .nodes
                        .iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(",");

                    let full_str = format!(
                        "numa:nodes={};cores:{};no_ht={}",
                        nodes_str, numa.cores_per_node, numa.avoid_hyperthread
                    );

                    serializer.serialize_str(&full_str)
                }
            }
        }
    }
}

impl<'de> Deserialize<'de> for CpuAllocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum CpuAllocationHelper {
            String(String),
            Number(usize),
        }

        match CpuAllocationHelper::deserialize(deserializer)? {
            CpuAllocationHelper::String(s) => {
                CpuAllocation::from_str(&s).map_err(serde::de::Error::custom)
            }
            CpuAllocationHelper::Number(n) => Ok(CpuAllocation::Count(n)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_all() {
        assert_eq!(CpuAllocation::from_str("all").unwrap(), CpuAllocation::All);
    }

    #[test]
    fn test_parse_count() {
        assert_eq!(
            CpuAllocation::from_str("4").unwrap(),
            CpuAllocation::Count(4)
        );
    }

    #[test]
    fn test_parse_range() {
        assert_eq!(
            CpuAllocation::from_str("2..8").unwrap(),
            CpuAllocation::Range(2, 8)
        );
    }

    #[test]
    fn test_parse_numa_auto() {
        let result = CpuAllocation::from_str("numa:auto").unwrap();
        match result {
            CpuAllocation::NumaAware(numa) => {
                assert!(numa.nodes.is_empty());
                assert_eq!(numa.cores_per_node, 0);
                assert!(numa.avoid_hyperthread);
            }
            _ => panic!("Expected NumaAware"),
        }
    }

    #[test]
    fn test_parse_numa_explicit() {
        let result = CpuAllocation::from_str("numa:nodes=0,1;cores=4;no_ht=true").unwrap();
        match result {
            CpuAllocation::NumaAware(numa) => {
                assert_eq!(numa.nodes, vec![0, 1]);
                assert_eq!(numa.cores_per_node, 4);
                assert!(numa.avoid_hyperthread);
            }
            _ => panic!("Expected NumaAware"),
        }
    }
}
