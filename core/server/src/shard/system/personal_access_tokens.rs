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
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::session::Session;
use crate::streaming::users::user::User;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::IggyExpiry;
use iggy_common::IggyTimestamp;
use tracing::{error, info};

impl IggyShard {
    pub fn get_personal_access_tokens(
        &self,
        session: &Session,
    ) -> Result<Vec<PersonalAccessToken>, IggyError> {
        self.ensure_authenticated(session)?;
        let user_id = session.get_user_id();
        let user = self.get_user(&user_id.try_into()?).with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get user with id: {user_id}")
        })?;
        info!("Loading personal access tokens for user with ID: {user_id}...",);
        let personal_access_tokens: Vec<_> = user
            .personal_access_tokens
            .iter()
            .map(|pat| pat.clone())
            .collect();

        info!(
            "Loaded {} personal access tokens for user with ID: {user_id}.",
            personal_access_tokens.len(),
        );
        Ok(personal_access_tokens)
    }

    pub fn create_personal_access_token(
        &self,
        session: &Session,
        name: &str,
        expiry: IggyExpiry,
    ) -> Result<(PersonalAccessToken, String), IggyError> {
        self.ensure_authenticated(session)?;
        let user_id = session.get_user_id();
        let identifier = user_id.try_into()?;
        {
            let user = self.get_user(&identifier).with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get user with id: {user_id}")
            })?;
            let max_token_per_user = self.config.personal_access_token.max_tokens_per_user;
            if user.personal_access_tokens.len() as u32 >= max_token_per_user {
                error!(
                    "User with ID: {user_id} has reached the maximum number of personal access tokens: {max_token_per_user}.",
                );
                return Err(IggyError::PersonalAccessTokensLimitReached(
                    user_id,
                    max_token_per_user,
                ));
            }
        }

        let (personal_access_token, token) =
            PersonalAccessToken::new(user_id, name, IggyTimestamp::now(), expiry);
        self.create_personal_access_token_base(personal_access_token.clone())?;
        Ok((personal_access_token, token))
    }

    pub fn create_personal_access_token_bypass_auth(
        &self,
        personal_access_token: PersonalAccessToken,
    ) -> Result<(), IggyError> {
        self.create_personal_access_token_base(personal_access_token)
    }

    fn create_personal_access_token_base(
        &self,
        personal_access_token: PersonalAccessToken,
    ) -> Result<(), IggyError> {
        let user_id = personal_access_token.user_id;
        let name = personal_access_token.name.clone();
        let token_hash = personal_access_token.token.clone();
        let identifier = user_id.try_into()?;
        self.users.with_user_mut(&identifier, |user| {
            if user
                .personal_access_tokens
                .iter()
                .any(|pat| pat.name.as_str() == name.as_str())
            {
                error!("Personal access token: {name} for user with ID: {user_id} already exists.");
                return Err(IggyError::PersonalAccessTokenAlreadyExists(
                    name.to_string(),
                    user_id,
                ));
            }

            user.personal_access_tokens
                .insert(token_hash, personal_access_token);
            info!("Created personal access token: {name} for user with ID: {user_id}.");
            Ok(())
        }).with_error(|error| {
            format!("{COMPONENT} create PAT (error: {error}) - failed to access user with id: {user_id}")
        })??;
        Ok(())
    }

    pub fn delete_personal_access_token(
        &self,
        session: &Session,
        name: &str,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let user_id = session.get_user_id();
        self.delete_personal_access_token_base(user_id, name)
    }

    pub fn delete_personal_access_token_bypass_auth(
        &self,
        user_id: u32,
        name: &str,
    ) -> Result<(), IggyError> {
        self.delete_personal_access_token_base(user_id, name)
    }

    fn delete_personal_access_token_base(&self, user_id: u32, name: &str) -> Result<(), IggyError> {
        self.users.with_user_mut(&user_id.try_into()?, |user| {
            let token = if let Some(pat) = user
                .personal_access_tokens
                .iter()
                .find(|pat| pat.name.as_str() == name)
            {
                pat.token.clone()
            } else {
                error!("Personal access token: {name} for user with ID: {user_id} does not exist.",);
                return Err(IggyError::ResourceNotFound(name.to_owned()));
            };

            info!("Deleting personal access token: {name} for user with ID: {user_id}...");
            user.personal_access_tokens.remove(&token);
            Ok(())
        }).with_error(|error| {
            format!(
                "{COMPONENT} delete PAT (error: {error}) - failed to access user with id: {user_id}"
            )
        })??;
        info!("Deleted personal access token: {name} for user with ID: {user_id}.");
        Ok(())
    }

    pub fn login_with_personal_access_token(
        &self,
        token: &str,
        session: Option<&Session>,
    ) -> Result<User, IggyError> {
        let token_hash = PersonalAccessToken::hash_token(token);
        let users = self.users.values();
        let mut personal_access_token = None;
        for user in &users {
            if let Some(pat) = user.personal_access_tokens.get(&token_hash) {
                personal_access_token = Some(pat);
                break;
            }
        }

        if personal_access_token.is_none() {
            let redacted_token = if token.len() > 4 {
                format!("{}****", &token[..4])
            } else {
                "****".to_string()
            };
            error!("Personal access token: {redacted_token} does not exist.");
            return Err(IggyError::ResourceNotFound(token.to_owned()));
        }

        let personal_access_token = personal_access_token.unwrap();
        if personal_access_token.is_expired(IggyTimestamp::now()) {
            error!(
                "Personal access token: {} for user with ID: {} has expired.",
                personal_access_token.name, personal_access_token.user_id
            );
            return Err(IggyError::PersonalAccessTokenExpired(
                personal_access_token.name.as_str().to_owned(),
                personal_access_token.user_id,
            ));
        }

        let user = self
            .get_user(&personal_access_token.user_id.try_into()?)
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get user with id: {}",
                    personal_access_token.user_id
                )
            })?;
        self.login_user_with_credentials(&user.username, None, session)
    }
}
