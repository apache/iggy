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

//! Configuration module providing flexible configuration loading with file and environment support.
//!
//! This module provides a trait-based configuration system that supports:
//! - Loading configuration from TOML files
//! - Environment variable overrides with complex path resolution
//! - Array and HashMap field overrides
//! - JSON value field handling
//! - Automatic type conversion and validation

mod env_mapping;
mod error;
mod file_provider;
mod parsing;
mod traits;
mod typed_env_provider;

pub use env_mapping::{ConfigEnvMappings, EnvVarMapping};
pub use error::ConfigurationError;
pub use file_provider::FileConfigProvider;
pub use parsing::parse_env_value_to_json;
pub use traits::{ConfigProvider, ConfigurationType};
pub use typed_env_provider::TypedEnvProvider;
