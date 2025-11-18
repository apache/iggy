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

use crate::prelude::IggyClient;
use async_trait::async_trait;
use iggy_binary_protocol::PersonalAccessTokenClient;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    IdentityInfo, IggyError, PersonalAccessTokenExpiry, PersonalAccessTokenInfo,
    RawPersonalAccessToken,
};

#[async_trait]
impl PersonalAccessTokenClient for IggyClient {
    async fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
        self.client.read().await.get_personal_access_tokens().await
    }

    async fn create_personal_access_token(
        &self,
        name: &str,
        expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, IggyError> {
        self.client
            .read()
            .await
            .create_personal_access_token(name, expiry)
            .await
    }

    async fn delete_personal_access_token(&self, name: &str) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_personal_access_token(name)
            .await
    }

    async fn login_with_personal_access_token(
        &self,
        token: &str,
    ) -> Result<IdentityInfo, IggyError> {
        self.client
            .read()
            .await
            .login_with_personal_access_token(token)
            .await
    }
}
