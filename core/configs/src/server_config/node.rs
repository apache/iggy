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

// This node's own client-facing identity, for the cluster-disabled server.

use super::COMPONENT;
use super::cluster::AdvertisedAddress;
use crate::ConfigurationError;
use configs::ConfigEnv;
use iggy_common::{IggyByteSize, Validatable};
use serde::{Deserialize, Serialize};

/// Named to match its roster counterpart: `advertised_address` here and
/// `cluster.nodes[*].advertised_address` there are the same setting for the
/// same question, and an operator moving between the two modes should not have
/// to learn a second spelling.
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// Client-facing address: a literal IP or a DNS hostname. `None` leaves
    /// the server deriving one from its bind address.
    #[serde(default)]
    pub advertised_address: Option<String>,
    /// Free space the node keeps on its data disk. While less is free, the
    /// node refuses new messages for the partitions it leads. `0` disables it.
    #[serde(default = "default_free_space_reserve")]
    #[config_env(leaf)]
    pub free_space_reserve: IggyByteSize,
}

fn default_free_space_reserve() -> IggyByteSize {
    NodeConfig::default().free_space_reserve
}

impl Validatable<ConfigurationError> for NodeConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        let Some(address) = self.advertised_address.as_deref() else {
            return Ok(());
        };

        address.parse::<AdvertisedAddress>().map_err(|error| {
            eprintln!("{COMPONENT} - node.advertised_address '{address}': {error}");
            ConfigurationError::InvalidConfigurationValue
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn advertised(address: &str) -> NodeConfig {
        NodeConfig {
            advertised_address: Some(address.to_owned()),
            ..NodeConfig::default()
        }
    }

    #[test]
    fn default_reserve_matches_the_shipped_config() {
        assert_eq!(
            NodeConfig::default().free_space_reserve.as_bytes_u64(),
            1 << 30
        );
    }

    #[test]
    fn missing_reserve_takes_the_default() {
        let node: NodeConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            node.free_space_reserve,
            NodeConfig::default().free_space_reserve
        );
    }

    #[test]
    fn validate_accepts_an_unset_address() {
        assert!(NodeConfig::default().validate().is_ok());
    }

    #[test]
    fn validate_accepts_a_routable_address() {
        assert!(advertised("203.0.113.10").validate().is_ok());
        assert!(advertised("broker-1.example.com").validate().is_ok());
        assert!(advertised("2001:db8::1").validate().is_ok());
    }

    #[test]
    fn validate_rejects_an_unspecified_address() {
        assert!(advertised("0.0.0.0").validate().is_err());
        assert!(advertised("::").validate().is_err());
    }

    #[test]
    fn validate_rejects_an_unparsable_address() {
        assert!(advertised("broker-1.example.com:8090").validate().is_err());
        assert!(advertised("10.0.0.256").validate().is_err());
        assert!(advertised("").validate().is_err());
    }
}
