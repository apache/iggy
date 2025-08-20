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

use crate::Client;
use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use iggy_common::Identifier;
use iggy_common::UserStatus;
use iggy_common::update_user::UpdateUser;
use tracing::{Level, event};

#[derive(Debug, Clone)]
pub enum UpdateUserType {
    Name(String),
    Status(UserStatus),
}

pub struct UpdateUserCmd {
    update_type: UpdateUserType,
    update_user: UpdateUser,
}

impl UpdateUserCmd {
    pub fn new(user_id: Identifier, update_type: UpdateUserType) -> Self {
        let (username, status) = match update_type.clone() {
            UpdateUserType::Name(username) => (Some(username), None),
            UpdateUserType::Status(status) => (None, Some(status)),
        };

        UpdateUserCmd {
            update_type,
            update_user: UpdateUser {
                user_id,
                username,
                status,
            },
        }
    }

    fn get_message(&self) -> String {
        match &self.update_type {
            UpdateUserType::Name(username) => format!("username: {username}"),
            UpdateUserType::Status(status) => format!("status: {status}"),
        }
    }
}

#[async_trait]
impl CliCommand for UpdateUserCmd {
    fn explain(&self) -> String {
        format!(
            "update user with ID: {} with {}",
            self.update_user.user_id,
            self.get_message()
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .update_user(
                &self.update_user.user_id,
                self.update_user.username.as_deref(),
                self.update_user.status,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem updating user with ID: {} with {}",
                    self.update_user.user_id,
                    self.get_message()
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "User with ID: {} updated with {}",
            self.update_user.user_id, self.get_message()
        );

        Ok(())
    }
}
