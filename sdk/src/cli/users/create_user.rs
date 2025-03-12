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
use crate::models::permissions::Permissions;
use crate::models::user_status::UserStatus;
use crate::users::create_user::CreateUser;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct CreateUserCmd {
    create_user: CreateUser,
}

impl CreateUserCmd {
    pub fn new(
        username: String,
        password: String,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Self {
        Self {
            create_user: CreateUser {
                username,
                password,
                status,
                permissions,
            },
        }
    }
}

#[async_trait]
impl CliCommand for CreateUserCmd {
    fn explain(&self) -> String {
        format!(
            "create user with username: {} and password: {}",
            self.create_user.username, self.create_user.password
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_user(
                &self.create_user.username,
                &self.create_user.password,
                self.create_user.status,
                self.create_user.permissions.clone(),
            )
            .await
            .with_context(|| {
                format!(
                    "Problem creating user (username: {} and password: {})",
                    self.create_user.username, self.create_user.password
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "User with username: {} and password: {} created",
            self.create_user.username, self.create_user.password
        );

        Ok(())
    }
}
