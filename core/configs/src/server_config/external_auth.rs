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

/// Strategy when the external auth service is unreachable or returns an error.
#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq, Eq, ConfigEnv)]
#[serde(rename_all = "snake_case")]
pub enum ExternalAuthErrorStrategy {
    #[default]
    Deny,
    Fallback,
}

/// External authentication callout configuration.
///
/// When enabled, login attempts are forwarded to an external HTTP service
/// before (or instead of) built-in credential verification. The service
/// returns a grant (with inline permissions or by mapping to an existing
/// Iggy user) or a denial.
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
    pub on_error: ExternalAuthErrorStrategy,
    #[serde(default)]
    pub forward_credentials: bool,
}

fn default_external_auth_timeout() -> IggyDuration {
    "5 s".parse().expect("hardcoded timeout")
}
