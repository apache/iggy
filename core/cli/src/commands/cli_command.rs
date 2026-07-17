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

use anyhow::{Error, Result};
use async_trait::async_trait;
use iggy_common::Client;

pub static PRINT_TARGET: &str = "iggy::cli::output";

#[async_trait]
pub trait CliCommand {
    fn explain(&self) -> String;
    fn use_tracing(&self) -> bool {
        true
    }
    fn login_required(&self) -> bool {
        true
    }
    fn connection_required(&self) -> bool {
        true
    }
    /// Whether explicitly supplied credentials take precedence over a cached
    /// login-session token. `iggy login` (re)establishes that session, so it
    /// must authenticate with the provided username/password and can recreate
    /// the session even after the cached token expired. Every other command
    /// keeps reusing the cached token so users need not re-enter credentials.
    fn prefer_explicit_credentials(&self) -> bool {
        false
    }
    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error>;
}
