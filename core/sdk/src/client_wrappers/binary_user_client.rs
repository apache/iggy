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

use crate::client_wrappers::client_wrapper::ClientWrapper;
use async_trait::async_trait;
use iggy_binary_protocol::UserClient;
use iggy_common::{
    Identifier, IdentityInfo, IggyError, Permissions, UserInfo, UserInfoDetails, UserStatus,
};

#[async_trait]
impl UserClient for ClientWrapper {
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.get_user(user_id).await,
            ClientWrapper::Http(client) => client.get_user(user_id).await,
            ClientWrapper::Tcp(client) => client.get_user(user_id).await,
            ClientWrapper::Quic(client) => client.get_user(user_id).await,
            ClientWrapper::WebSocket(client) => client.get_user(user_id).await,
        }
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.get_users().await,
            ClientWrapper::Http(client) => client.get_users().await,
            ClientWrapper::Tcp(client) => client.get_users().await,
            ClientWrapper::Quic(client) => client.get_users().await,
            ClientWrapper::WebSocket(client) => client.get_users().await,
        }
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserInfoDetails, IggyError> {
        match self {
            ClientWrapper::Http(client) => {
                client
                    .create_user(username, password, status, permissions)
                    .await
            }
            ClientWrapper::Tcp(client) => {
                client
                    .create_user(username, password, status, permissions)
                    .await
            }
            ClientWrapper::Quic(client) => {
                client
                    .create_user(username, password, status, permissions)
                    .await
            }
            ClientWrapper::Iggy(client) => {
                client
                    .create_user(username, password, status, permissions)
                    .await
            }
            ClientWrapper::WebSocket(client) => {
                client
                    .create_user(username, password, status, permissions)
                    .await
            }
        }
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Http(client) => client.delete_user(user_id).await,
            ClientWrapper::Tcp(client) => client.delete_user(user_id).await,
            ClientWrapper::Quic(client) => client.delete_user(user_id).await,
            ClientWrapper::Iggy(client) => client.delete_user(user_id).await,
            ClientWrapper::WebSocket(client) => client.delete_user(user_id).await,
        }
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Http(client) => client.update_user(user_id, username, status).await,
            ClientWrapper::Tcp(client) => client.update_user(user_id, username, status).await,
            ClientWrapper::Quic(client) => client.update_user(user_id, username, status).await,
            ClientWrapper::Iggy(client) => client.update_user(user_id, username, status).await,
            ClientWrapper::WebSocket(client) => client.update_user(user_id, username, status).await,
        }
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.update_permissions(user_id, permissions).await,
            ClientWrapper::Http(client) => client.update_permissions(user_id, permissions).await,
            ClientWrapper::Tcp(client) => client.update_permissions(user_id, permissions).await,
            ClientWrapper::Quic(client) => client.update_permissions(user_id, permissions).await,
            ClientWrapper::WebSocket(client) => {
                client.update_permissions(user_id, permissions).await
            }
        }
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Http(client) => {
                client
                    .change_password(user_id, current_password, new_password)
                    .await
            }
            ClientWrapper::Tcp(client) => {
                client
                    .change_password(user_id, current_password, new_password)
                    .await
            }
            ClientWrapper::Quic(client) => {
                client
                    .change_password(user_id, current_password, new_password)
                    .await
            }
            ClientWrapper::Iggy(client) => {
                client
                    .change_password(user_id, current_password, new_password)
                    .await
            }
            ClientWrapper::WebSocket(client) => {
                client
                    .change_password(user_id, current_password, new_password)
                    .await
            }
        }
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.login_user(username, password).await,
            ClientWrapper::Http(client) => client.login_user(username, password).await,
            ClientWrapper::Tcp(client) => client.login_user(username, password).await,
            ClientWrapper::Quic(client) => client.login_user(username, password).await,
            ClientWrapper::WebSocket(client) => client.login_user(username, password).await,
        }
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.logout_user().await,
            ClientWrapper::Http(client) => client.logout_user().await,
            ClientWrapper::Tcp(client) => client.logout_user().await,
            ClientWrapper::Quic(client) => client.logout_user().await,
            ClientWrapper::WebSocket(client) => client.logout_user().await,
        }
    }
}
