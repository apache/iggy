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

use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::users::delete_user::DeleteUser;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct DeleteUserCmd {
    delete_user: DeleteUser,
}

impl DeleteUserCmd {
    pub fn new(user_id: Identifier) -> Self {
        Self {
            delete_user: DeleteUser { user_id },
        }
    }
}

#[async_trait]
impl CliCommand for DeleteUserCmd {
    fn explain(&self) -> String {
        format!("delete user with ID: {}", self.delete_user.user_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .delete_user(&self.delete_user.user_id)
            .await
            .with_context(|| {
                format!(
                    "Problem deleting user with ID: {}",
                    self.delete_user.user_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO, "User with ID: {} deleted", self.delete_user.user_id);

        Ok(())
    }
}
