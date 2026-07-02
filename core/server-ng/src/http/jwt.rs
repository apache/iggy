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

//! Minimal JWT issuer/verifier for the shard-0 HTTP listener.
//!
//! Ported from the legacy `server::http::jwt::jwt_manager::JwtManager`, reduced
//! to the issue + verify half: no revoked-token persistence, no JWKS /
//! trusted-issuer path, no refresh. The claim set (`JwtClaims`, including the
//! `jti`) and the `IggyError -> HTTP` grading are reused verbatim so issued
//! tokens and error bodies stay identical to the legacy server.

use std::ops::Range;

use configs::http::HttpJwtConfig;
use iggy_common::{IggyDuration, IggyError, IggyExpiry, IggyTimestamp, UserId};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use server::http::jwt::json_web_token::{Audience, GeneratedToken, JwtClaims};
use server_common::crypto;
use tracing::warn;
use uuid::Uuid;

/// Length window for the random secret minted when no secret is configured.
/// Matches the legacy server so both behave identically on an empty secret.
const GENERATED_SECRET_LEN: Range<usize> = 32..64;

/// Expiry stamp used for a non-expiring token: far enough out to never trip
/// `exp` validation, small enough to fit `u32`. Mirrors the legacy server.
const NEVER_EXPIRE_SECS: u32 = 1_000_000_000;

/// Fixed-lifetime issuer/verifier for HTTP bearer tokens on shard 0.
pub struct JwtManager {
    algorithm: Algorithm,
    issuer: String,
    audience: String,
    access_token_expiry: IggyExpiry,
    not_before: IggyDuration,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtManager {
    /// Build the manager from the `[http.jwt]` config, normalizing an
    /// unconfigured secret the same way the legacy server does (mint a random
    /// ephemeral secret, or mirror whichever half is set).
    ///
    /// # Errors
    ///
    /// Returns [`IggyError`] if the configured algorithm is unsupported or a
    /// secret cannot be turned into an encoding/decoding key.
    pub fn build(config: &HttpJwtConfig) -> Result<Self, IggyError> {
        let config = normalize_secrets(config.clone());
        let algorithm = config.get_algorithm()?;
        let encoding_key = config.get_encoding_key()?;
        let decoding_key = config.get_decoding_key()?;
        let mut validation = Validation::new(algorithm);
        validation.set_issuer(&config.valid_issuers);
        validation.set_audience(&config.valid_audiences);
        validation.leeway = u64::from(config.clock_skew.as_secs());
        // The issuer always stamps `nbf`, so enforce it: a token presented
        // before its not-before instant (beyond `leeway`) is rejected.
        // `jsonwebtoken` leaves `validate_nbf` off by default, so this is
        // explicit; the claim is always present, so it only ever tightens.
        validation.validate_nbf = true;
        Ok(Self {
            algorithm,
            issuer: config.issuer,
            audience: config.audience,
            access_token_expiry: config.access_token_expiry,
            not_before: config.not_before,
            encoding_key,
            decoding_key,
            validation,
        })
    }

    /// Issue a signed access token for `user_id` carrying a unique `jti` plus
    /// issued-at / not-before / expiry stamps derived from the config.
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::CannotGenerateJwt`] if signing fails.
    pub fn generate(&self, user_id: UserId) -> Result<GeneratedToken, IggyError> {
        let header = Header::new(self.algorithm);
        let iat = IggyTimestamp::now().to_secs();
        let expiry_secs = match self.access_token_expiry {
            IggyExpiry::NeverExpire => NEVER_EXPIRE_SECS,
            IggyExpiry::ServerDefault => 0,
            IggyExpiry::ExpireDuration(duration) => duration.as_secs(),
        };
        let exp = iat + u64::from(expiry_secs);
        let nbf = iat + u64::from(self.not_before.as_secs());
        let claims = JwtClaims {
            jti: Uuid::now_v7().to_string(),
            sub: user_id.to_string(),
            aud: Audience::from(self.audience.clone()),
            iss: self.issuer.clone(),
            iat,
            exp,
            nbf,
        };
        let access_token = encode::<JwtClaims>(&header, &claims, &self.encoding_key)
            .map_err(|_| IggyError::CannotGenerateJwt)?;
        Ok(GeneratedToken {
            user_id,
            access_token,
            access_token_expiry: exp,
        })
    }

    /// Verify a token's signature and registered claims, returning its claim
    /// set (the `jti` is what a session table keys on).
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::Unauthenticated`] if the token is malformed,
    /// expired, or fails issuer/audience/signature validation.
    pub fn decode(&self, token: &str) -> Result<JwtClaims, IggyError> {
        decode::<JwtClaims>(token, &self.decoding_key, &self.validation)
            .map(|data| data.claims)
            .map_err(|_| IggyError::Unauthenticated)
    }
}

/// Fill in an unconfigured JWT secret exactly like the legacy HTTP server:
/// both empty -> one random ephemeral secret; one empty -> mirror the other;
/// both set but different under an HMAC algorithm -> warn (they must match).
fn normalize_secrets(mut config: HttpJwtConfig) -> HttpJwtConfig {
    match (
        config.encoding_secret.is_empty(),
        config.decoding_secret.is_empty(),
    ) {
        (true, true) => {
            let secret = crypto::generate_secret(GENERATED_SECRET_LEN);
            let redacted: String = secret.chars().take(3).collect();
            warn!(
                "JWT encoding and decoding secrets are not configured - generated a random secret: {redacted}***. JWT tokens will be invalidated on server restart. Set 'encoding_secret' and 'decoding_secret' in the config to use persistent secrets."
            );
            config.encoding_secret.clone_from(&secret);
            config.decoding_secret = secret;
        }
        (true, false) => {
            warn!(
                "JWT encoding secret is not configured but decoding secret is set - using decoding secret for both. Set 'encoding_secret' in the config to avoid this warning."
            );
            config.encoding_secret = config.decoding_secret.clone();
        }
        (false, true) => {
            warn!(
                "JWT decoding secret is not configured but encoding secret is set - using encoding secret for both. Set 'decoding_secret' in the config to avoid this warning."
            );
            config.decoding_secret = config.encoding_secret.clone();
        }
        (false, false) => {
            if config.encoding_secret != config.decoding_secret
                && config.algorithm.starts_with("HS")
            {
                warn!(
                    "JWT encoding and decoding secrets are different but algorithm is {} (HMAC) - both secrets must be identical for symmetric algorithms.",
                    config.algorithm
                );
            }
        }
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A default config (HS256, matching issuer/audience, empty secrets ->
    /// one ephemeral secret shared by encode and decode) with the two
    /// timing knobs overridden.
    fn config(not_before: &str, clock_skew: &str) -> HttpJwtConfig {
        HttpJwtConfig {
            not_before: not_before.parse().expect("valid duration"),
            clock_skew: clock_skew.parse().expect("valid duration"),
            ..HttpJwtConfig::default()
        }
    }

    #[test]
    fn token_presented_before_its_nbf_is_rejected() {
        // not_before well past the leeway window, so the freshly issued token
        // is not yet valid and decode must reject it.
        let manager = JwtManager::build(&config("3600 s", "5 s")).expect("builds");
        let token = manager.generate(7).expect("issues");
        assert!(
            manager.decode(&token.access_token).is_err(),
            "a token presented before its nbf must be rejected"
        );
    }

    #[test]
    fn token_at_its_nbf_is_accepted() {
        // Default not_before (0s) => nbf == iat, so the token is valid now and
        // enabling nbf validation does not reject a normally issued token.
        let manager = JwtManager::build(&config("0 s", "5 s")).expect("builds");
        let token = manager.generate(7).expect("issues");
        let claims = manager.decode(&token.access_token).expect("valid now");
        assert_eq!(claims.sub, "7");
    }
}
