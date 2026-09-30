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

//! Environment variable to config path mapping types.

/// Represents a single environment variable to config path mapping.
/// Used by the `ConfigEnv` derive macro to generate compile-time mappings.
#[derive(Debug, Clone, Copy)]
pub struct EnvVarMapping {
    /// The environment variable name (e.g., "IGGY_HTTP_ENABLED")
    pub env_name: &'static str,
    /// The config path (e.g., "http.enabled")
    pub config_path: &'static str,
    /// Whether this field contains secret data
    pub is_secret: bool,
}

/// Compact representation of a configuration environment variable.
///
/// Array indices are represented by `<N>` in `env_name`. `max_elements`
/// contains the expansion limit for each placeholder, from left to right.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvVarTemplate {
    /// Environment variable name, with `<N>` for each array index.
    pub env_name: &'static str,
    /// Maximum element count for each `<N>` placeholder, from left to right.
    pub max_elements: &'static [usize],
}

/// Trait for configuration types that provide environment variable mappings.
/// Implemented automatically by the `#[derive(ConfigEnv)]` macro.
pub trait ConfigEnvMappings {
    /// Returns all environment variable mappings for this config type.
    fn env_mappings() -> &'static [EnvVarMapping];

    /// Returns the compact environment variable templates for this config type.
    fn env_templates() -> &'static [EnvVarTemplate];

    /// Finds a mapping by environment variable name.
    fn find_by_env_name(env_name: &str) -> Option<&'static EnvVarMapping> {
        Self::env_mappings().iter().find(|m| m.env_name == env_name)
    }

    /// Finds a mapping by config path.
    fn find_by_config_path(path: &str) -> Option<&'static EnvVarMapping> {
        Self::env_mappings().iter().find(|m| m.config_path == path)
    }

    /// Returns all valid environment variable names.
    fn all_env_var_names() -> Vec<&'static str> {
        Self::env_mappings().iter().map(|m| m.env_name).collect()
    }

    /// Returns environment variable names marked as secrets.
    fn secret_env_names() -> Vec<&'static str> {
        Self::env_mappings()
            .iter()
            .filter(|m| m.is_secret)
            .map(|m| m.env_name)
            .collect()
    }
}

impl EnvVarTemplate {
    /// Expands this template into every concrete env-var name it represents,
    /// substituting each `<N>` placeholder left-to-right with `0..limit`.
    /// A leaf template (`max_elements: &[]`) expands to itself.
    pub fn expand_names(&self) -> Vec<String> {
        if self.max_elements.is_empty() {
            return vec![self.env_name.to_string()];
        }

        let mut results = vec![self.env_name.to_string()];

        for &limit in self.max_elements {
            let mut next = Vec::new();
            for name in results {
                for i in 0..limit {
                    next.push(name.replacen("<N>", &i.to_string(), 1));
                }
            }
            results = next;
        }

        results
    }
}

#[cfg(test)]
mod consistency_tests {
    use super::*;
    use std::collections::HashSet;
    use crate::server::ServerConfig;
    use crate::cluster::ClusterConfig;
    use crate::McpServerConfig;
    use configs::runtime::ConnectorsRuntimeConfig;
    use configs::connectors::{SinkConfig, SourceConfig};

    #[test]
    fn server_config_templates_and_mappings_align() {
        let expanded: HashSet<String> = ServerConfig::env_templates()
            .iter()
            .flat_map(|t| t.expand_names())
            .collect();
        let mapped: HashSet<String> = ServerConfig::env_mappings()
            .iter()
            .map(|m| m.env_name.to_string())
            .collect();
        assert_eq!(expanded, mapped, "ServerConfig env_templates and env_mappings must align");
    }

    #[test]
    fn cluster_config_templates_and_mappings_align() {
        let expanded: HashSet<String> = ClusterConfig::env_templates()
            .iter()
            .flat_map(|t| t.expand_names())
            .collect();
        let mapped: HashSet<String> = ClusterConfig::env_mappings()
            .iter()
            .map(|m| m.env_name.to_string())
            .collect();
        assert_eq!(expanded, mapped, "ClusterConfig env_templates and env_mappings must align");
    }

    #[test]
    fn mcp_server_config_templates_and_mappings_align() {
        let expanded: HashSet<String> = McpServerConfig::env_templates()
            .iter()
            .flat_map(|t| t.expand_names())
            .collect();
        let mapped: HashSet<String> = McpServerConfig::env_mappings()
            .iter()
            .map(|m| m.env_name.to_string())
            .collect();
        assert_eq!(expanded, mapped, "McpServerConfig env_templates and env_mappings must align");
    }

    #[test]
    fn connectors_runtime_config_templates_and_mappings_align() {
        let expanded: HashSet<String> = ConnectorsRuntimeConfig::env_templates()
            .iter()
            .flat_map(|t| t.expand_names())
            .collect();
        let mapped: HashSet<String> = ConnectorsRuntimeConfig::env_mappings()
            .iter()
            .map(|m| m.env_name.to_string())
            .collect();
        assert_eq!(
            expanded, mapped,
            "ConnectorsRuntimeConfig env_templates and env_mappings must align"
        );
    }

    #[test]
    fn sink_config_templates_and_mappings_align() {
        let expanded: HashSet<String> = SinkConfig::env_templates()
            .iter()
            .flat_map(|t| t.expand_names())
            .collect();
        let mapped: HashSet<String> = SinkConfig::env_mappings()
            .iter()
            .map(|m| m.env_name.to_string())
            .collect();
        assert_eq!(expanded, mapped, "SinkConfig env_templates and env_mappings must align");
    }

    #[test]
    fn source_config_templates_and_mappings_align() {
        let expanded: HashSet<String> = SourceConfig::env_templates()
            .iter()
            .flat_map(|t| t.expand_names())
            .collect();
        let mapped: HashSet<String> = SourceConfig::env_mappings()
            .iter()
            .map(|m| m.env_name.to_string())
            .collect();
        assert_eq!(expanded, mapped, "SourceConfig env_templates and env_mappings must align");
    }
}
