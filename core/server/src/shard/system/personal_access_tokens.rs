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

        // Read from SharedMetadata for cross-shard consistency
        let metadata = self.shared_metadata.load();
        let user_meta = metadata
            .users
            .get(&user_id)
            .ok_or_else(|| IggyError::ResourceNotFound(format!("user with ID {user_id}")))?;

        info!("Loading personal access tokens for user with ID: {user_id}...",);
        let personal_access_tokens: Vec<_> = user_meta
            .personal_access_tokens
            .iter()
            .map(|(_, pat_meta)| PersonalAccessToken::from_metadata(user_id, pat_meta))
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
            let user = self.get_user(&identifier).error(|e: &IggyError| {
                format!("{COMPONENT} (error: {e}) - failed to get user with id: {user_id}")
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

    fn create_personal_access_token_base(
        &self,
        personal_access_token: PersonalAccessToken,
    ) -> Result<(), IggyError> {
        let user_id = personal_access_token.user_id;
        let name = personal_access_token.name.clone();
        let token_hash = personal_access_token.token.clone();
        let expiry_at = personal_access_token.expiry_at;

        // Check SharedMetadata for existence (source of truth for cross-shard consistency)
        {
            let metadata = self.shared_metadata.load();
            if let Some(user_meta) = metadata.users.get(&user_id)
                && user_meta.personal_access_tokens.contains_key(name.as_str())
            {
                error!("Personal access token: {name} for user with ID: {user_id} already exists.");
                return Err(IggyError::PersonalAccessTokenAlreadyExists(
                    name.to_string(),
                    user_id,
                ));
            }
        }

        // Create in SharedMetadata first (primary store)
        self.shared_metadata.create_personal_access_token(
            user_id,
            name.to_string(),
            token_hash.to_string(),
            expiry_at,
        )?;

        info!("Created personal access token: {name} for user with ID: {user_id}.");
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

    fn delete_personal_access_token_base(&self, user_id: u32, name: &str) -> Result<(), IggyError> {
        // Check SharedMetadata for existence
        {
            let metadata = self.shared_metadata.load();
            let user_meta = metadata
                .users
                .get(&user_id)
                .ok_or_else(|| IggyError::ResourceNotFound(format!("user with ID {user_id}")))?;
            if !user_meta.personal_access_tokens.contains_key(name) {
                error!("Personal access token: {name} for user with ID: {user_id} does not exist.",);
                return Err(IggyError::ResourceNotFound(name.to_owned()));
            }
        };

        info!("Deleting personal access token: {name} for user with ID: {user_id}...");

        self.shared_metadata
            .delete_personal_access_token(user_id, name)?;

        info!("Deleted personal access token: {name} for user with ID: {user_id}.");
        Ok(())
    }

    pub fn login_with_personal_access_token(
        &self,
        token: &str,
        session: Option<&Session>,
    ) -> Result<User, IggyError> {
        let token_hash = PersonalAccessToken::hash_token(token);

        // Search SharedMetadata for the token by matching token_hash field.
        // PATs are keyed by name, so we must iterate through all values.
        let metadata = self.shared_metadata.load();
        let mut found_user_id = None;
        let mut found_pat_meta = None;

        'outer: for (user_id, user_meta) in metadata.users.iter() {
            for pat_meta in user_meta.personal_access_tokens.values() {
                if pat_meta.token_hash == token_hash {
                    found_user_id = Some(*user_id);
                    found_pat_meta = Some(pat_meta.clone());
                    break 'outer;
                }
            }
        }

        let (user_id, pat_meta) = match (found_user_id, found_pat_meta) {
            (Some(uid), Some(pat)) => (uid, pat),
            _ => {
                let redacted_token = if token.len() > 4 {
                    format!("{}****", &token[..4])
                } else {
                    "****".to_string()
                };
                error!("Personal access token: {redacted_token} does not exist.");
                return Err(IggyError::ResourceNotFound(token.to_owned()));
            }
        };

        // Check expiry
        let personal_access_token = PersonalAccessToken::from_metadata(user_id, &pat_meta);
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
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to get user with id: {}",
                    personal_access_token.user_id
                )
            })?;
        self.login_user_with_credentials(&user.username, None, session)
    }
}
