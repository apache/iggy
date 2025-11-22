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

use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::prelude::{Identifier, IggyError};
use async_trait::async_trait;
use iggy_binary_protocol::UserClient;
use iggy_common::change_password::ChangePassword;
use iggy_common::create_user::CreateUser;
use iggy_common::login_user::LoginUser;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::update_user::UpdateUser;
use iggy_common::{IdentityInfo, Permissions, UserInfo, UserInfoDetails, UserStatus};

const PATH: &str = "/users";

#[async_trait]
impl UserClient for HttpClient {
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError> {
        let response = self.get(&format!("{PATH}/{user_id}")).await;
        if let Err(error) = response {
            if matches!(error, IggyError::ResourceNotFound(_)) {
                return Ok(None);
            }

            return Err(error);
        }

        let user = response?
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(Some(user))
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        let response = self.get(PATH).await?;
        let users = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(users)
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserInfoDetails, IggyError> {
        let response = self
            .post(
                PATH,
                &CreateUser {
                    username: username.to_string(),
                    password: password.to_string(),
                    status,
                    permissions,
                },
            )
            .await?;
        let user = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(user)
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        self.delete(&format!("{PATH}/{}", &user_id.as_cow_str()))
            .await?;
        Ok(())
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        self.put(
            &format!("{PATH}/{}", &user_id.as_cow_str()),
            &UpdateUser {
                user_id: user_id.clone(),
                username: username.map(|s| s.to_string()),
                status,
            },
        )
        .await?;
        Ok(())
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.put(
            &format!("{PATH}/{}/permissions", &user_id.as_cow_str()),
            &UpdatePermissions {
                user_id: user_id.clone(),
                permissions,
            },
        )
        .await?;
        Ok(())
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.put(
            &format!("{PATH}/{}/password", &user_id.as_cow_str()),
            &ChangePassword {
                user_id: user_id.clone(),
                current_password: current_password.to_string(),
                new_password: new_password.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        let response = self
            .post(
                &format!("{PATH}/login"),
                &LoginUser {
                    username: username.to_string(),
                    password: password.to_string(),
                    version: Some(env!("CARGO_PKG_VERSION").to_string()),
                    context: Some("".to_string()),
                },
            )
            .await?;
        let identity_info = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        self.set_token_from_identity(&identity_info).await?;
        Ok(identity_info)
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        self.delete(&format!("{PATH}/logout")).await?;
        self.set_access_token(None).await;
        Ok(())
    }
}
