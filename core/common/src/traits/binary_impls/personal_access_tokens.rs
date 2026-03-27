/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
use crate::wire_conversions::personal_access_tokens_from_wire;
use crate::{
    BinaryClient, ClientState, DiagnosticEvent, IdentityInfo, IggyError, PersonalAccessTokenClient,
    PersonalAccessTokenExpiry, PersonalAccessTokenInfo, RawPersonalAccessToken,
};
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    CREATE_PERSONAL_ACCESS_TOKEN_CODE, DELETE_PERSONAL_ACCESS_TOKEN_CODE,
    GET_PERSONAL_ACCESS_TOKENS_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
};
use iggy_binary_protocol::requests::personal_access_tokens::{
    CreatePersonalAccessTokenRequest, DeletePersonalAccessTokenRequest,
    GetPersonalAccessTokensRequest, LoginWithPersonalAccessTokenRequest,
};
use iggy_binary_protocol::responses::personal_access_tokens::create_personal_access_token::RawPersonalAccessTokenResponse;
use iggy_binary_protocol::responses::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokensResponse;
use iggy_binary_protocol::responses::users::login_user::IdentityResponse;

#[async_trait::async_trait]
impl<B: BinaryClient> PersonalAccessTokenClient for B {
    async fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(
                GET_PERSONAL_ACCESS_TOKENS_CODE,
                GetPersonalAccessTokensRequest.to_bytes(),
            )
            .await?;
        if response.is_empty() {
            return Ok(Vec::new());
        }
        let wire_resp = super::decode_response::<GetPersonalAccessTokensResponse>(&response)?;
        Ok(personal_access_tokens_from_wire(wire_resp))
    }

    async fn create_personal_access_token(
        &self,
        name: &str,
        expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_name = WireName::new(name).map_err(|_| IggyError::InvalidFormat)?;
        let response = self
            .send_raw_with_response(
                CREATE_PERSONAL_ACCESS_TOKEN_CODE,
                CreatePersonalAccessTokenRequest {
                    name: wire_name,
                    expiry: u64::from(expiry),
                }
                .to_bytes(),
            )
            .await?;
        let wire_resp = super::decode_response::<RawPersonalAccessTokenResponse>(&response)?;
        Ok(RawPersonalAccessToken::from(wire_resp))
    }

    async fn delete_personal_access_token(&self, name: &str) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_name = WireName::new(name).map_err(|_| IggyError::InvalidFormat)?;
        self.send_raw_with_response(
            DELETE_PERSONAL_ACCESS_TOKEN_CODE,
            DeletePersonalAccessTokenRequest { name: wire_name }.to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn login_with_personal_access_token(
        &self,
        token: &str,
    ) -> Result<IdentityInfo, IggyError> {
        let wire_token = WireName::new(token).map_err(|_| IggyError::InvalidFormat)?;
        let response = self
            .send_raw_with_response(
                LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
                LoginWithPersonalAccessTokenRequest { token: wire_token }.to_bytes(),
            )
            .await?;
        self.set_state(ClientState::Authenticated).await;
        self.publish_event(DiagnosticEvent::SignedIn).await;
        let wire_resp = super::decode_response::<IdentityResponse>(&response)?;
        Ok(IdentityInfo::from(wire_resp))
    }
}
