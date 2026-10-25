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
    IdentityInfo, IggyError, PersonalAccessTokenClient, PersonalAccessTokenExpiry,
    PersonalAccessTokenInfo, RawPersonalAccessToken,
};

#[async_trait]
impl PersonalAccessTokenClient for ClientWrapper {
    async fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
        super::dispatch_client!(
            self,
            PersonalAccessTokenClient::get_personal_access_tokens()
        )
        .await
    }

    async fn create_personal_access_token(
        &self,
        name: &str,
        expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, IggyError> {
        super::dispatch_client!(
            self,
            PersonalAccessTokenClient::create_personal_access_token(name, expiry)
        )
        .await
    }

    async fn delete_personal_access_token(&self, name: &str) -> Result<(), IggyError> {
        super::dispatch_client!(
            self,
            PersonalAccessTokenClient::delete_personal_access_token(name)
        )
        .await
    }

    async fn login_with_personal_access_token(
        &self,
        token: &str,
    ) -> Result<IdentityInfo, IggyError> {
        super::dispatch_client!(
            self,
            PersonalAccessTokenClient::login_with_personal_access_token(token)
        )
        .await
    }
}
