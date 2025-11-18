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

use crate::versioning::SemanticVersion;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

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
