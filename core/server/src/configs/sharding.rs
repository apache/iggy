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
use std::collections::HashSet;
use std::str::FromStr;
use std::thread::available_parallelism;

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
}

impl CpuAllocation {
    pub fn to_shard_set(&self) -> HashSet<usize> {
        match self {
            CpuAllocation::All => {
                let available_cpus = available_parallelism()
                    .expect("Failed to get num of cores")
                    .get();
                (0..available_cpus).collect()
            }
            CpuAllocation::Count(count) => (0..*count).collect(),
            CpuAllocation::Range(start, end) => (*start..*end).collect(),
        }
    }
}

impl FromStr for CpuAllocation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all" => Ok(CpuAllocation::All),
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
