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
use tracing::{Level, event};

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use iggy_common::Client;

use super::common::{ContextConfig, ContextManager};

pub struct CreateContextCmd {
    context_name: String,
    context_config: ContextConfig,
}

impl CreateContextCmd {
    pub fn new(context_name: String, context_config: ContextConfig) -> Self {
        Self {
            context_name,
            context_config,
        }
    }
}

#[async_trait]
impl CliCommand for CreateContextCmd {
    fn explain(&self) -> String {
        let context_name = &self.context_name;
        format!("create context {context_name}")
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
            .create_context(&self.context_name, self.context_config.clone())
            .await?;

        event!(target: PRINT_TARGET, Level::INFO, "context '{}' created successfully", self.context_name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_explain_message() {
        let cmd = CreateContextCmd::new("production".to_string(), ContextConfig::default());
        assert_eq!(cmd.explain(), "create context production");
    }

    #[test]
    fn should_not_require_login() {
        let cmd = CreateContextCmd::new("test".to_string(), ContextConfig::default());
        assert!(!cmd.login_required());
    }

    #[test]
    fn should_not_require_connection() {
        let cmd = CreateContextCmd::new("test".to_string(), ContextConfig::default());
        assert!(!cmd.connection_required());
    }
}
