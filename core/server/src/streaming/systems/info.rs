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

use crate::streaming::systems::system::System;
use crate::versioning::SemanticVersion;
use iggy_common::IggyError;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SystemInfo {
    pub version: Version,
    pub migrations: Vec<Migration>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Version {
    pub version: String,
    pub hash: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Migration {
    pub id: u32,
    pub name: String,
    pub hash: String,
    pub applied_at: u64,
}

impl System {
    pub(crate) async fn load_version(&mut self) -> Result<(), IggyError> {
        let current_version = SemanticVersion::current()?;
        let mut system_info;
        let load_system_info = self.storage.info.load().await;
        if load_system_info.is_err() {
            let error = load_system_info.err().unwrap();
            if let IggyError::ResourceNotFound(_) = error {
                info!("System info not found, creating...");
                system_info = SystemInfo::default();
                self.update_system_info(&mut system_info, &current_version)
                    .await?;
            } else {
                return Err(error);
            }
        } else {
            system_info = load_system_info.unwrap();
        }

        info!("Loaded {system_info}.");
        let loaded_version = SemanticVersion::from_str(&system_info.version.version)?;
        if current_version.is_equal_to(&loaded_version) {
            info!("System version {current_version} is up to date.");
        } else if current_version.is_greater_than(&loaded_version) {
            info!(
                "System version {current_version} is greater than {loaded_version}, checking the available migrations..."
            );
            self.update_system_info(&mut system_info, &current_version)
                .await?;
        } else {
            info!(
                "System version {current_version} is lower than {loaded_version}, possible downgrade."
            );
            self.update_system_info(&mut system_info, &current_version)
                .await?;
        }

        Ok(())
    }

    async fn update_system_info(
        &self,
        system_info: &mut SystemInfo,
        version: &SemanticVersion,
    ) -> Result<(), IggyError> {
        system_info.update_version(version);
        self.storage.info.save(system_info).await?;
        Ok(())
    }
}

impl SystemInfo {
    pub fn update_version(&mut self, version: &SemanticVersion) {
        self.version.version = version.to_string();
        let mut hasher = DefaultHasher::new();
        self.version.hash.hash(&mut hasher);
        self.version.hash = hasher.finish().to_string();
    }
}

impl Hash for SystemInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.version.version.hash(state);
        for migration in &self.migrations {
            migration.hash(state);
        }
    }
}

impl Hash for Migration {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "version: {}", self.version)
    }
}

impl Display for SystemInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "system info, {}", self.version)
    }
}
