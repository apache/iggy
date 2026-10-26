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

use crate::client_wrappers::client_wrapper::ClientWrapper;
use async_trait::async_trait;
use iggy_common::{
    Identifier, IdentityInfo, IggyError, Permissions, UserClient, UserCreateOptions, UserInfo,
    UserInfoDetails, UserStatus, UserUpdateOptions,
};

#[async_trait]
impl UserClient for ClientWrapper {
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError> {
        super::dispatch_client!(self, UserClient::get_user(user_id)).await
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        super::dispatch_client!(self, UserClient::get_users()).await
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
        options: &UserCreateOptions,
    ) -> Result<UserInfoDetails, IggyError> {
        super::dispatch_client!(
            self,
            UserClient::create_user(username, password, status, permissions, options)
        )
        .await
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        super::dispatch_client!(self, UserClient::delete_user(user_id)).await
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        options: &UserUpdateOptions,
    ) -> Result<(), IggyError> {
        super::dispatch_client!(self, UserClient::update_user(user_id, options)).await
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        super::dispatch_client!(self, UserClient::update_permissions(user_id, permissions)).await
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        super::dispatch_client!(
            self,
            UserClient::change_password(user_id, current_password, new_password)
        )
        .await
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        super::dispatch_client!(self, UserClient::login_user(username, password)).await
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        super::dispatch_client!(self, UserClient::logout_user()).await
    }
}
