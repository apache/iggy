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
use crate::prelude::IggyClient;
use async_trait::async_trait;
use iggy_binary_protocol::{Client, UserClient};
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    Identifier, IdentityInfo, IggyError, Permissions, UserInfo, UserInfoDetails, UserStatus,
};
use tracing::info;

#[async_trait]
impl UserClient for IggyClient {
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError> {
        self.client.read().await.get_user(user_id).await
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        self.client.read().await.get_users().await
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserInfoDetails, IggyError> {
        self.client
            .read()
            .await
            .create_user(username, password, status, permissions)
            .await
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        self.client.read().await.delete_user(user_id).await
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .update_user(user_id, username, status)
            .await
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .update_permissions(user_id, permissions)
            .await
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .change_password(user_id, current_password, new_password)
            .await
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        let identity = self
            .client
            .read()
            .await
            .login_user(username, password)
            .await?;

        let should_redirect = {
            let client = self.client.read().await;
            match &*client {
                ClientWrapper::Tcp(tcp_client) => tcp_client.handle_leader_redirection().await?,
                ClientWrapper::Quic(quic_client) => quic_client.handle_leader_redirection().await?,
                ClientWrapper::WebSocket(ws_client) => {
                    ws_client.handle_leader_redirection().await?
                }
                _ => false,
            }
        };

        if should_redirect {
            info!("Redirected to leader, reconnecting and re-authenticating");
            self.connect().await?;
            self.login_user(username, password).await
        } else {
            Ok(identity)
        }
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        self.client.read().await.logout_user().await
    }
}
