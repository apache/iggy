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

use crate::traits::binary_auth::fail_if_not_authenticated;
use crate::wire_conversions::{identifier_to_wire, permissions_to_wire, users_from_wire};
use crate::{
    BinaryClient, ClientState, DiagnosticEvent, Identifier, IdentityInfo, IggyError, Permissions,
    UserClient, UserInfo, UserInfoDetails, UserStatus,
};
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    CHANGE_PASSWORD_CODE, CREATE_USER_CODE, DELETE_USER_CODE, GET_USER_CODE, GET_USERS_CODE,
    LOGIN_USER_CODE, LOGOUT_USER_CODE, UPDATE_PERMISSIONS_CODE, UPDATE_USER_CODE,
};
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest, CreateUserRequest, DeleteUserRequest, GetUserRequest, GetUsersRequest,
    LoginUserRequest, LogoutUserRequest, UpdatePermissionsRequest, UpdateUserRequest,
};
use iggy_binary_protocol::responses::users::login_user::IdentityResponse;
use iggy_binary_protocol::responses::users::{GetUsersResponse, UserDetailsResponse};

#[async_trait::async_trait]
impl<B: BinaryClient> UserClient for B {
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(user_id)?;
        let response = self
            .send_raw_with_response(
                GET_USER_CODE,
                GetUserRequest { user_id: wire_id }.to_bytes(),
            )
            .await?;
        if response.is_empty() {
            return Ok(None);
        }
        let wire_resp = super::decode_response::<UserDetailsResponse>(&response)?;
        UserInfoDetails::try_from(wire_resp).map(Some)
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(GET_USERS_CODE, GetUsersRequest.to_bytes())
            .await?;
        if response.is_empty() {
            return Ok(Vec::new());
        }
        let wire_resp = super::decode_response::<GetUsersResponse>(&response)?;
        users_from_wire(wire_resp)
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserInfoDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_name = WireName::new(username).map_err(|_| IggyError::InvalidFormat)?;
        let wire_perms = permissions.as_ref().map(permissions_to_wire);
        let response = self
            .send_raw_with_response(
                CREATE_USER_CODE,
                CreateUserRequest {
                    username: wire_name,
                    password: password.to_string(),
                    status: status.as_code(),
                    permissions: wire_perms,
                }
                .to_bytes(),
            )
            .await?;
        let wire_resp = super::decode_response::<UserDetailsResponse>(&response)?;
        UserInfoDetails::try_from(wire_resp)
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(user_id)?;
        self.send_raw_with_response(
            DELETE_USER_CODE,
            DeleteUserRequest { user_id: wire_id }.to_bytes(),
        )
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
        let wire_id = identifier_to_wire(user_id)?;
        let wire_username = username
            .map(WireName::new)
            .transpose()
            .map_err(|_| IggyError::InvalidFormat)?;
        self.send_raw_with_response(
            UPDATE_USER_CODE,
            UpdateUserRequest {
                user_id: wire_id,
                username: wire_username,
                status: status.map(|s| s.as_code()),
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(user_id)?;
        let wire_perms = permissions.as_ref().map(permissions_to_wire);
        self.send_raw_with_response(
            UPDATE_PERMISSIONS_CODE,
            UpdatePermissionsRequest {
                user_id: wire_id,
                permissions: wire_perms,
            }
            .to_bytes(),
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
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(user_id)?;
        self.send_raw_with_response(
            CHANGE_PASSWORD_CODE,
            ChangePasswordRequest {
                user_id: wire_id,
                current_password: current_password.to_string(),
                new_password: new_password.to_string(),
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        let wire_name = WireName::new(username).map_err(|_| IggyError::InvalidFormat)?;
        let response = self
            .send_raw_with_response(
                LOGIN_USER_CODE,
                LoginUserRequest {
                    username: wire_name,
                    password: password.to_string(),
                    version: Some(env!("CARGO_PKG_VERSION").to_string()),
                    context: Some(String::new()),
                }
                .to_bytes(),
            )
            .await?;
        self.set_state(ClientState::Authenticated).await;
        self.publish_event(DiagnosticEvent::SignedIn).await;
        let wire_resp = super::decode_response::<IdentityResponse>(&response)?;
        Ok(IdentityInfo::from(wire_resp))
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_raw_with_response(LOGOUT_USER_CODE, LogoutUserRequest.to_bytes())
            .await?;
        self.set_state(ClientState::Connected).await;
        self.publish_event(DiagnosticEvent::SignedOut).await;
        Ok(())
    }
}
