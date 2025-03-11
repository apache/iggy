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

use crate::client::PersonalAccessTokenClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::models::identity_info::IdentityInfo;
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use crate::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use crate::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
use async_trait::async_trait;

const PATH: &str = "/personal-access-tokens";

#[async_trait]
impl PersonalAccessTokenClient for HttpClient {
    async fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
        let response = self.get(PATH).await?;
        let personal_access_tokens = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(personal_access_tokens)
    }

    async fn create_personal_access_token(
        &self,
        name: &str,
        expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, IggyError> {
        let response = self
            .post(
                PATH,
                &CreatePersonalAccessToken {
                    name: name.to_string(),
                    expiry,
                },
            )
            .await?;
        let personal_access_token = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(personal_access_token)
    }

    async fn delete_personal_access_token(&self, name: &str) -> Result<(), IggyError> {
        self.delete(&format!("{PATH}/{name}")).await?;
        Ok(())
    }

    async fn login_with_personal_access_token(
        &self,
        token: &str,
    ) -> Result<IdentityInfo, IggyError> {
        let response = self
            .post(
                &format!("{PATH}/login"),
                &LoginWithPersonalAccessToken {
                    token: token.to_string(),
                },
            )
            .await?;
        let identity_info: IdentityInfo = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        self.set_token_from_identity(&identity_info).await?;
        Ok(identity_info)
    }
}
