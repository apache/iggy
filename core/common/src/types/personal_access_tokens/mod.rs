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

use crate::IggyError;
use crate::IggyExpiry;
use crate::IggyTimestamp;
use crate::UserId;
use crate::text::as_base64;
use crate::utils::hash;
use ring::rand::SecureRandom;
use std::sync::Arc;

const SIZE: usize = 50;

#[derive(Clone, Debug)]
pub struct PersonalAccessToken {
    pub user_id: UserId,
    pub name: Arc<str>,
    pub token: Arc<str>,
    pub expiry_at: Option<IggyTimestamp>,
}

impl PersonalAccessToken {
    // Raw token is generated and returned only once
    pub fn new(
        user_id: UserId,
        name: &str,
        now: IggyTimestamp,
        expiry: IggyExpiry,
    ) -> Result<(Self, String), IggyError> {
        let expiry_at = Self::calculate_expiry_at(now, expiry)?;
        let (token, token_hash) = Self::mint_raw_and_hash();
        Ok((
            Self {
                user_id,
                name: Arc::from(name),
                token: Arc::from(token_hash),
                expiry_at,
            },
            token,
        ))
    }

    /// Mint a fresh raw token and its hash without constructing a
    /// `PersonalAccessToken`. The VSR primary calls this in
    /// `maybe_rewrite_pat_request` so the hash is replicated in the prepare
    /// body; replicas decode and store it directly via
    /// [`Self::raw`]. Calling [`Self::new`] inside the state-machine apply
    /// would re-roll `ring::rand` per replica and diverge state.
    #[must_use]
    pub fn mint_raw_and_hash() -> (String, String) {
        let mut buffer: [u8; SIZE] = [0; SIZE];
        let system_random = ring::rand::SystemRandom::new();
        system_random.fill(&mut buffer).unwrap();
        let token = as_base64(&buffer);
        let hash = Self::hash_token(&token);
        (token, hash)
    }

    pub fn raw(
        user_id: UserId,
        name: &str,
        token_hash: &str,
        expiry_at: Option<IggyTimestamp>,
    ) -> Self {
        Self {
            user_id,
            name: Arc::from(name),
            token: Arc::from(token_hash),
            expiry_at,
        }
    }

    pub fn is_expired(&self, now: IggyTimestamp) -> bool {
        match self.expiry_at {
            None => false,
            Some(expiry_at) => expiry_at.as_micros() <= now.as_micros(),
        }
    }

    pub fn hash_token(token: &str) -> String {
        hash::calculate_256(token.as_bytes())
    }

    /// `InvalidPersonalAccessTokenExpiry` when the expiry ends past the last representable timestamp.
    pub fn calculate_expiry_at(
        now: IggyTimestamp,
        expiry: IggyExpiry,
    ) -> Result<Option<IggyTimestamp>, IggyError> {
        match expiry {
            IggyExpiry::ExpireDuration(expiry) => now
                .as_micros()
                .checked_add(expiry.as_micros())
                .map(|expiry_at| Some(IggyTimestamp::from(expiry_at)))
                .ok_or(IggyError::InvalidPersonalAccessTokenExpiry),
            IggyExpiry::NeverExpire | IggyExpiry::ServerDefault => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IggyDuration;
    use crate::IggyTimestamp;

    #[test]
    fn personal_access_token_should_be_created_with_random_secure_value_and_hashed_successfully() {
        let user_id = 1;
        let now = IggyTimestamp::now();
        let name = "test_token";
        let (personal_access_token, raw_token) =
            PersonalAccessToken::new(user_id, name, now, IggyExpiry::NeverExpire)
                .expect("a never-expiring token has no expiry to overflow");
        assert_eq!(&*personal_access_token.name, name);
        assert!(!personal_access_token.token.is_empty());
        assert!(!raw_token.is_empty());
        assert_ne!(&*personal_access_token.token, raw_token);
        assert_eq!(
            &*personal_access_token.token,
            PersonalAccessToken::hash_token(&raw_token)
        );
    }

    #[test]
    fn personal_access_token_should_be_expired_given_passed_expiry() {
        let user_id = 1;
        let now = IggyTimestamp::now();
        let expiry_ms = 10;
        let expiry = IggyExpiry::ExpireDuration(IggyDuration::from(expiry_ms));
        let name = "test_token";
        let (personal_access_token, _) = PersonalAccessToken::new(user_id, name, now, expiry)
            .expect("a 10 microsecond expiry fits the clock");
        let later = IggyTimestamp::from(now.as_micros() + expiry_ms + 1);
        assert!(personal_access_token.is_expired(later));
    }

    #[test]
    fn expiry_at_should_be_refused_given_expiry_past_the_end_of_the_clock() {
        let now = IggyTimestamp::from(1_000);
        let last = IggyExpiry::ExpireDuration(IggyDuration::from(u64::MAX - 1_000));
        let past = IggyExpiry::ExpireDuration(IggyDuration::from(u64::MAX - 999));

        assert_eq!(
            PersonalAccessToken::calculate_expiry_at(now, last),
            Ok(Some(IggyTimestamp::from(u64::MAX)))
        );
        assert_eq!(
            PersonalAccessToken::calculate_expiry_at(now, past),
            Err(IggyError::InvalidPersonalAccessTokenExpiry)
        );
    }
}
