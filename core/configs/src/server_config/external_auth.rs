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
/// When enabled, login attempts for usernames not found in the local user
/// store are forwarded to an external HTTP service. The service maps the
/// caller to a pre-provisioned Iggy username or denies the login. Local
/// users never hit the callout. The credential (password) is forwarded
/// so the external service can verify the requester.
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
    #[serde(default = "default_max_concurrent_callouts")]
    pub max_concurrent_callouts: u32,
}

impl Default for ExternalAuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: String::new(),
            timeout: default_external_auth_timeout(),
            max_concurrent_callouts: default_max_concurrent_callouts(),
        }
    }
}

fn default_external_auth_timeout() -> IggyDuration {
    IggyDuration::new_from_secs(5)
}

const fn default_max_concurrent_callouts() -> u32 {
    64
}
