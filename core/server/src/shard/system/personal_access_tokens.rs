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

use super::COMPONENT;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use crate::streaming::users::user::User;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::IggyExpiry;
use iggy_common::IggyTimestamp;
use iggy_common::PersonalAccessToken;
use tracing::{error, info};

impl IggyShard {
    pub fn get_personal_access_tokens(
        &self,
        user_id: u32,
    ) -> Result<Vec<PersonalAccessToken>, IggyError> {
        let _ = self.get_user(&user_id.try_into()?).error(|e: &IggyError| {
            format!("{COMPONENT} (error: {e}) - failed to get user with id: {user_id}")
        })?;

        info!("Loading personal access tokens for user with ID: {user_id}...",);

        let personal_access_tokens = self.metadata.get_user_personal_access_tokens(user_id);

        info!(
            "Loaded {} personal access tokens for user with ID: {user_id}.",
            personal_access_tokens.len(),
        );
        Ok(personal_access_tokens)
    }

    pub fn create_personal_access_token(
        &self,
        user_id: u32,
        name: &str,
        expiry: IggyExpiry,
    ) -> Result<(PersonalAccessToken, String), IggyError> {
        let _ = self.get_user(&user_id.try_into()?).error(|e: &IggyError| {
            format!("{COMPONENT} (error: {e}) - failed to get user with id: {user_id}")
        })?;

        let max_token_per_user = self.config.personal_access_token.max_tokens_per_user;
        let current_count = self.metadata.user_pat_count(user_id);
        if current_count as u32 >= max_token_per_user {
            error!(
                "User with ID: {user_id} has reached the maximum number of personal access tokens: {max_token_per_user}.",
            );
            return Err(IggyError::PersonalAccessTokensLimitReached(
                user_id,
                max_token_per_user,
            ));
        }

        let (personal_access_token, token) =
            PersonalAccessToken::new(user_id, name, IggyTimestamp::now(), expiry);

        let pat_name = personal_access_token.name.clone();
        if self.metadata.user_has_pat_with_name(user_id, &pat_name) {
            error!("Personal access token: {pat_name} for user with ID: {user_id} already exists.");
            return Err(IggyError::PersonalAccessTokenAlreadyExists(
                pat_name.to_string(),
                user_id,
            ));
        }

        self.writer()
            .add_personal_access_token(user_id, personal_access_token.clone());
        info!("Created personal access token: {pat_name} for user with ID: {user_id}.");

        Ok((personal_access_token, token))
    }

    pub fn delete_personal_access_token(&self, user_id: u32, name: &str) -> Result<(), IggyError> {
        let token_hash =
            self.metadata
                .find_pat_token_hash_by_name(user_id, name)
                .ok_or_else(|| {
                    error!(
                        "Personal access token: {name} for user with ID: {user_id} does not exist.",
                    );
                    IggyError::ResourceNotFound(name.to_owned())
                })?;

        info!("Deleting personal access token: {name} for user with ID: {user_id}...");
        self.writer()
            .delete_personal_access_token(user_id, token_hash);
        info!("Deleted personal access token: {name} for user with ID: {user_id}.");
        Ok(())
    }

    pub fn login_with_personal_access_token(
        &self,
        token: &str,
        session: Option<&Session>,
    ) -> Result<User, IggyError> {
        let token_hash = PersonalAccessToken::hash_token(token);

        let personal_access_token = self
            .metadata
            .get_personal_access_token_by_hash(&token_hash)
            .ok_or_else(|| {
                let redacted_token = if token.len() > 4 {
                    format!("{}****", &token[..4])
                } else {
                    "****".to_string()
                };
                error!("Personal access token: {redacted_token} does not exist.");
                IggyError::ResourceNotFound(token.to_owned())
            })?;

        if personal_access_token.is_expired(IggyTimestamp::now()) {
            error!(
                "Personal access token: {} for user with ID: {} has expired.",
                personal_access_token.name, personal_access_token.user_id
            );
            return Err(IggyError::PersonalAccessTokenExpired(
                (*personal_access_token.name).to_owned(),
                personal_access_token.user_id,
            ));
        }

        let user = self
            .get_user(&personal_access_token.user_id.try_into()?)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to get user with id: {}",
                    personal_access_token.user_id
                )
            })?;
        self.login_user_with_credentials(&user.username, None, session)
    }
}
