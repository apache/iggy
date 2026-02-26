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

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::str::FromStr;

use configs::ConfigEnv;

#[derive(Debug, Deserialize, Serialize, Default, ConfigEnv)]
pub struct ShardingConfig {
    #[serde(default)]
    #[config_env(leaf)]
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
                        "numa:nodes={};cores={};no_ht={}",
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

    #[test]
    fn test_numa_explicit_serde_roundtrip() {
        let original = CpuAllocation::NumaAware(NumaConfig {
            nodes: vec![0, 1],
            cores_per_node: 4,
            avoid_hyperthread: true,
        });
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: CpuAllocation = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }
}
