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

use super::common::ContextManager;

pub struct DeleteContextCmd {
    context_name: String,
}

impl DeleteContextCmd {
    pub fn new(context_name: String) -> Self {
        Self { context_name }
    }
}

#[async_trait]
impl CliCommand for DeleteContextCmd {
    fn explain(&self) -> String {
        let context_name = &self.context_name;
        format!("delete context {context_name}")
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut context_mgr = ContextManager::default();

        context_mgr.delete_context(&self.context_name).await?;

        event!(target: PRINT_TARGET, Level::INFO, "context '{}' deleted successfully", self.context_name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_explain_message() {
        let cmd = DeleteContextCmd::new("production".to_string());
        assert_eq!(cmd.explain(), "delete context production");
    }

    #[test]
    fn should_not_require_login() {
        let cmd = DeleteContextCmd::new("test".to_string());
        assert!(!cmd.login_required());
    }

    #[test]
    fn should_not_require_connection() {
        let cmd = DeleteContextCmd::new("test".to_string());
        assert!(!cmd.connection_required());
    }
}
