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

use configs::ConfigEnv;
use iggy_common::IggyDuration;
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;

/// External authentication callout configuration.
///
/// When enabled, login attempts that fail built-in credential verification
/// are forwarded to an external HTTP service. The service returns a grant
/// (with inline permissions or by mapping to an existing Iggy user) or a
/// denial. Built-in users never hit the callout.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct ExternalAuthConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub url: String,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_external_auth_timeout")]
    #[config_env(leaf)]
    pub timeout: IggyDuration,
    #[serde(default)]
    pub forward_credentials: bool,
    /// Reserved user ID for inline-grant sessions. All external auth
    /// inline grants share this single ID; the external principal string
    /// distinguishes sessions. Must not be 0 (root). The replicated
    /// state-machine gate recognizes this ID at boot.
    ///
    /// **Changing this value between restarts is safe.** External auth
    /// sessions are connection-scoped and never persisted, so there is no
    /// stale state referencing the old value. The server validates at boot
    /// that the new value does not collide with any existing user in the
    /// slab. Within a running process the value is immutable (set-once via
    /// `OnceLock`); a hot-reload that attempts a different value panics.
    /// In a cluster, all nodes should use the same value to avoid
    /// operational confusion, though cross-node correctness is not
    /// affected (session state is node-local).
    #[serde(default = "default_external_auth_user_id")]
    #[config_env(leaf)]
    pub user_id: u32,
}

fn default_external_auth_timeout() -> IggyDuration {
    "5 s".parse().expect("hardcoded timeout")
}

const fn default_external_auth_user_id() -> u32 {
    u32::MAX
}
