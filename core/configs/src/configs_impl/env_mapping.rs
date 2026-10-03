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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvVarTemplate {
    /// Environment variable name, with `<N>` for each array index.
    pub env_name: &'static str,
    /// Config path, with `<N>` for each array index.
    pub config_path: &'static str,
    /// Whether this field contains secret data.
    pub is_secret: bool,
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
    /// Expands this template into every concrete mapping it represents.
    pub fn expand(&self) -> Vec<EnvVarMapping> {
        let mut results = vec![(self.env_name.to_string(), self.config_path.to_string())];

        for &limit in self.max_elements {
            let mut next = Vec::new();
            for (env_name, config_path) in results {
                for i in 0..limit {
                    let index = i.to_string();
                    next.push((
                        env_name.replacen("<N>", &index, 1),
                        config_path.replacen("<N>", &index, 1),
                    ));
                }
            }
            results = next;
        }

        results
            .into_iter()
            .map(|(env_name, config_path)| EnvVarMapping {
                env_name: Box::leak(env_name.into_boxed_str()),
                config_path: Box::leak(config_path.into_boxed_str()),
                is_secret: self.is_secret,
            })
            .collect()
    }
}

/// Expands compact templates into the mappings consumed by the env provider.
pub fn expand_env_templates(templates: &[EnvVarTemplate]) -> Vec<EnvVarMapping> {
    templates.iter().flat_map(EnvVarTemplate::expand).collect()
}
