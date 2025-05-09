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

use async_trait::async_trait;
use tracing::{event, Level};

use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use crate::Client;

use super::common::{ContextManager, DEFAULT_CONTEXT_NAME};

pub struct UseContextCmd {
    context_name: String,
}

impl UseContextCmd {
    pub fn new(context_name: String) -> Self {
        Self { context_name }
    }
}

impl Default for UseContextCmd {
    fn default() -> Self {
        UseContextCmd {
            context_name: DEFAULT_CONTEXT_NAME.to_string(),
        }
    }
}

#[async_trait]
impl CliCommand for UseContextCmd {
    fn explain(&self) -> String {
        let context_name = &self.context_name;
        format!("use context {context_name}")
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut context_mgr = ContextManager::default();

        context_mgr
            .set_active_context_key(&self.context_name)
            .await?;

        event!(target: PRINT_TARGET, Level::INFO, "active context set to '{}'", self.context_name);

        return Ok(());
    }
}
