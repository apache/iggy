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

/// Trait for configuration types that provide environment variable mappings.
/// Implemented automatically by the `#[derive(ConfigEnv)]` macro.
pub trait ConfigEnvMappings {
    /// Returns all environment variable mappings for this config type.
    fn env_mappings() -> &'static [EnvVarMapping];

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
