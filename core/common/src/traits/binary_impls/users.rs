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

use crate::BinaryClient;
use crate::change_password::ChangePassword;
use crate::create_user::CreateUser;
use crate::delete_user::DeleteUser;
use crate::get_user::GetUser;
use crate::get_users::GetUsers;
use crate::login_user::LoginUser;
use crate::logout_user::LogoutUser;
use crate::traits::binary_auth::fail_if_not_authenticated;
use crate::traits::binary_mapper;
use crate::update_permissions::UpdatePermissions;
use crate::update_user::UpdateUser;
use crate::{
    ClientState, DiagnosticEvent, Identifier, IdentityInfo, IggyError, Permissions, UserClient,
    UserInfo, UserInfoDetails, UserStatus,
};
use secrecy::SecretString;

#[async_trait::async_trait]
impl<B: BinaryClient> UserClient for B {
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetUser {
                user_id: user_id.clone(),
            })
            .await?;
        if response.is_empty() {
            return Ok(None);
        }

        binary_mapper::map_user(response).map(Some)
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self.send_with_response(&GetUsers {}).await?;
        binary_mapper::map_users(response)
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserInfoDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&CreateUser {
                username: username.to_string(),
                password: SecretString::from(password),
                status,
                permissions,
            })
            .await?;
        binary_mapper::map_user(response)
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeleteUser {
            user_id: user_id.clone(),
        })
        .await?;
        Ok(())
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&UpdateUser {
            user_id: user_id.clone(),
            username: username.map(|s| s.to_string()),
            status,
        })
        .await?;
        Ok(())
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&UpdatePermissions {
            user_id: user_id.clone(),
            permissions,
        })
        .await?;
        Ok(())
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&ChangePassword {
            user_id: user_id.clone(),
            current_password: SecretString::from(current_password),
            new_password: SecretString::from(new_password),
        })
        .await?;
        Ok(())
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        let response = self
            .send_with_response(&LoginUser {
                username: username.to_string(),
                password: SecretString::from(password),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
                context: Some("".to_string()),
            })
            .await?;
        self.set_state(ClientState::Authenticated).await;
        self.publish_event(DiagnosticEvent::SignedIn).await;
        binary_mapper::map_identity_info(response)
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&LogoutUser {}).await?;
        self.set_state(ClientState::Connected).await;
        self.publish_event(DiagnosticEvent::SignedOut).await;
        Ok(())
    }
}
