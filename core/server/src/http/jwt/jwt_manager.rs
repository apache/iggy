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

use crate::configs::http::HttpJwtConfig;
use crate::http::jwt::COMPONENT;
use crate::http::jwt::json_web_token::{GeneratedToken, JwtClaims, RevokedAccessToken};
use crate::http::jwt::storage::TokenStorage;
use crate::streaming::persistence::persister::PersisterKind;
use ahash::AHashMap;
use err_trail::ErrContext;
use iggy_common::IggyDuration;
use iggy_common::IggyError;
use iggy_common::IggyExpiry;
use iggy_common::IggyTimestamp;
use iggy_common::UserId;
use iggy_common::locking::IggyRwLock;
use iggy_common::locking::IggyRwLockFn;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation, encode};
use std::sync::Arc;
use tracing::{debug, error, info};

pub struct IssuerOptions {
    pub issuer: String,
    pub audience: String,
    pub access_token_expiry: IggyExpiry,
    pub not_before: IggyDuration,
    pub key: EncodingKey,
    pub algorithm: Algorithm,
}

pub struct ValidatorOptions {
    pub valid_audiences: Vec<String>,
    pub valid_issuers: Vec<String>,
    pub clock_skew: IggyDuration,
    pub key: DecodingKey,
}

pub struct JwtManager {
    issuer: IssuerOptions,
    validator: ValidatorOptions,
    tokens_storage: TokenStorage,
    revoked_tokens: IggyRwLock<AHashMap<String, u64>>,
    validations: AHashMap<Algorithm, Validation>,
}

impl JwtManager {
    pub fn new(
        persister: Arc<PersisterKind>,
        path: &str,
        issuer: IssuerOptions,
        validator: ValidatorOptions,
    ) -> Result<Self, IggyError> {
        let validation = JwtManager::create_validation(
            issuer.algorithm,
            &validator.valid_issuers,
            &validator.valid_audiences,
            validator.clock_skew,
        );

        Ok(Self {
            validations: vec![(issuer.algorithm, validation)].into_iter().collect(),
            issuer,
            validator,
            tokens_storage: TokenStorage::new(persister, path),
            revoked_tokens: IggyRwLock::new(AHashMap::new()),
        })
    }

    pub fn from_config(
        persister: Arc<PersisterKind>,
        path: &str,
        config: &HttpJwtConfig,
    ) -> Result<Self, IggyError> {
        let algorithm = config.get_algorithm()?;
        let issuer = IssuerOptions {
            issuer: config.issuer.clone(),
            audience: config.audience.clone(),
            access_token_expiry: config.access_token_expiry,
            not_before: config.not_before,
            key: config.get_encoding_key().with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get encoding key")
            })?,
            algorithm,
        };
        let validator = ValidatorOptions {
            valid_audiences: config.valid_audiences.clone(),
            valid_issuers: config.valid_issuers.clone(),
            clock_skew: config.clock_skew,
            key: config.get_decoding_key().with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get decoding key")
            })?,
        };
        JwtManager::new(persister, path, issuer, validator)
    }

    fn create_validation(
        algorithm: Algorithm,
        issuers: &[String],
        audiences: &[String],
        clock_skew: IggyDuration,
    ) -> Validation {
        let mut validator = Validation::new(algorithm);
        validator.set_issuer(issuers);
        validator.set_audience(audiences);
        validator.leeway = clock_skew.as_secs() as u64;
        validator
    }

    pub async fn load_revoked_tokens(&self) -> Result<(), IggyError> {
        let revoked_tokens = self.tokens_storage.load_all_revoked_access_tokens().await?;
        let mut tokens = self.revoked_tokens.write().await;
        for token in revoked_tokens {
            tokens.insert(token.id, token.expiry);
        }
        Ok(())
    }

    pub async fn delete_expired_revoked_tokens(&self, now: u64) -> Result<(), IggyError> {
        let mut tokens_to_delete = Vec::new();
        let revoked_tokens = self.revoked_tokens.read().await;
        for (id, expiry) in revoked_tokens.iter() {
            if expiry <= &now {
                tokens_to_delete.push(id.to_string());
            }
        }
        drop(revoked_tokens);

        debug!(
            "Found {} expired revoked access tokens to delete.",
            tokens_to_delete.len()
        );
        if tokens_to_delete.is_empty() {
            return Ok(());
        }

        debug!(
            "Deleting {} expired revoked access tokens...",
            tokens_to_delete.len()
        );
        self.tokens_storage
            .delete_revoked_access_tokens(&tokens_to_delete)
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to delete revoked access tokens, IDs {tokens_to_delete:?}"
                )
            })?;
        let mut revoked_tokens = self.revoked_tokens.write().await;
        for id in tokens_to_delete {
            revoked_tokens.remove(&id);
            info!("Deleted expired revoked access token with ID: {id}")
        }
        Ok(())
    }

    pub fn generate(&self, user_id: UserId) -> Result<GeneratedToken, IggyError> {
        let header = Header::new(self.issuer.algorithm);
        let now = IggyTimestamp::now().to_secs();
        let iat = now;
        let exp = iat
            + (match self.issuer.access_token_expiry {
                IggyExpiry::NeverExpire => 1_000_000_000,
                IggyExpiry::ServerDefault => 0, // This is not a case, as the server default is not allowed here
                IggyExpiry::ExpireDuration(duration) => duration.as_secs(),
            }) as u64;
        let nbf = iat + self.issuer.not_before.as_secs() as u64;
        let claims = JwtClaims {
            jti: uuid::Uuid::now_v7().to_string(),
            sub: user_id,
            aud: self.issuer.audience.to_string(),
            iss: self.issuer.issuer.to_string(),
            iat,
            exp,
            nbf,
        };

        let access_token = encode::<JwtClaims>(&header, &claims, &self.issuer.key);
        if let Err(error) = access_token {
            error!("Cannot generate JWT token. Error: {error}");
            return Err(IggyError::CannotGenerateJwt);
        }

        Ok(GeneratedToken {
            user_id,
            access_token: access_token.unwrap(),
            access_token_expiry: exp,
        })
    }

    // The access token can be refreshed only once and if it is not expired
    pub async fn refresh_token(&self, token: &str) -> Result<GeneratedToken, IggyError> {
        if token.is_empty() {
            return Err(IggyError::InvalidAccessToken);
        }

        let token_header =
            jsonwebtoken::decode_header(token).map_err(|_| IggyError::InvalidAccessToken)?;
        let jwt_claims = self.decode(token, token_header.alg)?;
        let id = jwt_claims.claims.jti;
        let expiry = jwt_claims.claims.exp;
        if self
            .revoked_tokens
            .write()
            .await
            .insert(id.clone(), expiry)
            .is_some()
        {
            return Err(IggyError::InvalidAccessToken);
        }

        self.tokens_storage
            .save_revoked_access_token(&RevokedAccessToken {
                id: id.clone(),
                expiry,
            })
            .await
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to save revoked access token: {id}")
            })?;
        self.generate(jwt_claims.claims.sub)
    }

    pub fn decode(
        &self,
        token: &str,
        algorithm: Algorithm,
    ) -> Result<TokenData<JwtClaims>, IggyError> {
        let validation = self.validations.get(&algorithm);
        if validation.is_none() {
            return Err(IggyError::InvalidJwtAlgorithm(
                Self::map_algorithm_to_string(algorithm),
            ));
        }

        let validation = validation.unwrap();
        match jsonwebtoken::decode::<JwtClaims>(token, &self.validator.key, validation) {
            Ok(claims) => Ok(claims),
            _ => Err(IggyError::Unauthenticated),
        }
    }

    fn map_algorithm_to_string(algorithm: Algorithm) -> String {
        match algorithm {
            Algorithm::HS256 => "HS256",
            Algorithm::HS384 => "HS384",
            Algorithm::HS512 => "HS512",
            Algorithm::RS256 => "RS256",
            Algorithm::RS384 => "RS384",
            Algorithm::RS512 => "RS512",
            _ => "Unknown",
        }
        .to_string()
    }

    pub async fn revoke_token(&self, token_id: &str, expiry: u64) -> Result<(), IggyError> {
        let mut revoked_tokens = self.revoked_tokens.write().await;
        revoked_tokens.insert(token_id.to_string(), expiry);
        self.tokens_storage
            .save_revoked_access_token(&RevokedAccessToken {
                id: token_id.to_string(),
                expiry,
            })
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to save revoked access token: {token_id}"
                )
            })?;
        info!("Revoked access token with ID: {token_id}");
        Ok(())
    }

    pub async fn is_token_revoked(&self, token_id: &str) -> bool {
        let revoked_tokens = self.revoked_tokens.read().await;
        revoked_tokens.contains_key(token_id)
    }
}
