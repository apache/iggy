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
//! to the issue + verify half: no revoked-token persistence. Self-issued HS256
//! is the common path; with `[[http.jwt.trusted_issuers]]` configured, `decode`
//! also verifies external RS256/EC tokens against the issuer's JWKS
//! ([`super::jwks`]) and remaps their subject onto the configured iggy user.
//! The refresh-token route re-issues by composing `decode_for_refresh` (verify
//! plus a refusal of trusted-issuer tokens) with `generate` at the handler;
//! without a revocation list that refresh is stateless, so the token it was
//! minted from stays valid until its own `exp`. The claim set
//! (`JwtClaims`, including the `jti`) and the `IggyError -> HTTP` grading are
//! reused verbatim so issued tokens and error bodies stay identical to the
//! legacy server.

use std::collections::HashMap;
use std::ops::Range;

use configs::http::{HttpJwtConfig, TrustedIssuerConfig};
use iggy_common::{IggyDuration, IggyError, IggyExpiry, IggyTimestamp, UserId};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use server_common::crypto;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::jwks::JwksClient;

/// Length window for the random secret minted when no secret is configured.
/// Matches the legacy server so both behave identically on an empty secret.
const GENERATED_SECRET_LEN: Range<usize> = 32..64;

/// BLAKE3 `derive_key` context for turning the cluster PSK into the HS256
/// signing key when no JWT secret is configured in cluster mode. A distinct
/// context string keeps this key cryptographically independent from the
/// replica-auth MAC subkey derived from the same PSK. Deliberate coupling:
/// with this fallback a PSK compromise also yields the token-signing key, and
/// rotating the PSK invalidates all bearers; operators who want the domains
/// decoupled configure an explicit `http.jwt` secret, which always wins.
const JWT_KEY_CONTEXT: &str = "apache-iggy server-ng http-jwt v1 psk->hs256-key";

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
    jwks_client: JwksClient,
    /// Trusted external issuers keyed by normalized issuer URL. Empty unless
    /// `[[http.jwt.trusted_issuers]]` is configured, in which case `decode`
    /// takes the JWKS verification path for tokens from these issuers.
    trusted_issuers: HashMap<String, TrustedIssuerConfig>,
}

impl JwtManager {
    /// Build the manager from the `[http.jwt]` config, normalizing an
    /// unconfigured secret the same way the legacy server does (mint a random
    /// ephemeral secret, or mirror whichever half is set) and loading any
    /// configured trusted issuers (keyed by normalized issuer URL).
    ///
    /// `cluster_psk`, when present, replaces the random-mint fallback with a
    /// key derived from the cluster shared secret, so every node verifies
    /// every node's bearers - the invariant follower-to-primary forwarding
    /// depends on. A configured secret always wins over it.
    ///
    /// # Errors
    ///
    /// Returns [`IggyError`] if the configured algorithm is unsupported or a
    /// secret cannot be turned into an encoding/decoding key.
    pub fn build(config: &HttpJwtConfig, cluster_psk: Option<&str>) -> Result<Self, IggyError> {
        let config = normalize_secrets(config.clone(), cluster_psk);
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
        // Validate every trusted issuer at startup so a broken config fails loud
        // instead of silently rejecting every A2A token at first use: an empty
        // issuer or `jwks_url` can never verify, and mapping onto the root user
        // (id 0) would resolve every external subject to root (`decode` guards
        // the root case again at verify time as defense in depth).
        for issuer_config in config.trusted_issuers.iter().flatten() {
            if issuer_config.issuer.is_empty() {
                error!("trusted issuer configured with an empty issuer URL");
                return Err(IggyError::InvalidConfiguration);
            }
            if issuer_config.jwks_url.is_empty() {
                error!(
                    issuer = %issuer_config.issuer,
                    "trusted issuer configured with an empty jwks_url"
                );
                return Err(IggyError::InvalidConfiguration);
            }
            if issuer_config.user_id == 0 {
                error!(
                    issuer = %issuer_config.issuer,
                    "trusted issuer user_id is missing or 0 (must be a non-zero iggy user id)"
                );
                return Err(IggyError::InvalidConfiguration);
            }
        }
        let trusted_issuers = config
            .trusted_issuers
            .iter()
            .flatten()
            .map(|issuer_config| {
                (
                    normalize_issuer_url(&issuer_config.issuer),
                    issuer_config.clone(),
                )
            })
            .collect();
        Ok(Self {
            algorithm,
            issuer: config.issuer,
            audience: config.audience,
            access_token_expiry: config.access_token_expiry,
            not_before: config.not_before,
            encoding_key,
            decoding_key,
            validation,
            jwks_client: JwksClient::default(),
            trusted_issuers,
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
    /// With no trusted issuers this is the self-issued HS256 verify and the
    /// returned future resolves without awaiting. When the token's issuer
    /// matches a configured trusted issuer, verification instead uses the
    /// issuer's JWKS key (awaiting the fetch on a cache miss) and the returned
    /// `sub` is the configured iggy user id, not the external subject.
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::Unauthenticated`] if the token is malformed,
    /// expired, or fails issuer/audience/signature validation.
    pub async fn decode(&self, token: &str) -> Result<JwtClaims, IggyError> {
        // With no trusted issuers this is the original self-issued verifier,
        // resolving without ever awaiting.
        if self.trusted_issuers.is_empty() {
            return self.decode_self_issued(token);
        }

        // Try the self-issued verify first. A trusted-issuer token is signed by
        // the issuer's key, which the self-issued verify rejects at the algorithm
        // check (when self-issued is HS*, the shipped default) or at signature
        // verification (if an operator configures self-issued RS*/ES*) - either
        // way it can never accept an external token; a valid self-issued token
        // returns here and the classification below only runs once it has failed.
        if let Ok(claims) = self.decode_self_issued(token) {
            return Ok(claims);
        }

        // Self-issued verify failed. Classify the token by its (untrusted) issuer
        // claim; if it names a trusted issuer, verify it against that issuer's
        // JWKS key. Everything read here is re-checked under that key below.
        let Ok(insecure) = jsonwebtoken::dangerous::insecure_decode::<JwtClaims>(token) else {
            debug!("token is not self-issued and cannot be parsed to classify its issuer");
            return Err(IggyError::Unauthenticated);
        };
        let normalized_iss = normalize_issuer_url(&insecure.claims.iss);
        let Some(config) = self.trusted_issuers.get(&normalized_iss) else {
            debug!(
                issuer = %insecure.claims.iss,
                "token issuer matches no trusted issuer and failed self-issued verify"
            );
            return Err(IggyError::Unauthenticated);
        };

        // A trusted external subject must never resolve to the root user.
        if config.user_id == 0 {
            error!(
                issuer = %config.issuer,
                "trusted-issuer token cannot map to root user (user_id = 0)"
            );
            return Err(IggyError::Unauthenticated);
        }

        // `insecure.header` is parsed from the same header segment `decode_header`
        // reads, so reuse it for `alg` and `kid` instead of parsing twice.
        let Some(kid) = insecure.header.kid.as_deref() else {
            debug!(
                issuer = %config.issuer,
                "trusted-issuer token has no `kid` header for JWKS key lookup"
            );
            return Err(IggyError::Unauthenticated);
        };
        let decoding_key = match self
            .jwks_client
            .get_key(&config.issuer, &config.jwks_url, kid)
            .await
        {
            Ok(key) => key,
            Err(error) => {
                // Fail closed but loud: a broken `jwks_url` or a rotated-away
                // `kid` otherwise surfaces only as an opaque 401.
                warn!(
                    %error,
                    issuer = %config.issuer,
                    kid,
                    "cannot resolve JWKS key for trusted-issuer token"
                );
                return Err(IggyError::Unauthenticated);
            }
        };

        // The algorithm is taken from the token header (e.g. RS256), not the
        // self-issued HS256; issuer and audience are pinned to the config, and
        // the timing checks mirror the self-issued path (enforce `nbf`, honor
        // the configured clock skew).
        let mut validation = Validation::new(insecure.header.alg);
        validation.set_issuer(std::slice::from_ref(&config.issuer));
        validation.set_audience(std::slice::from_ref(&config.audience));
        validation.validate_nbf = true;
        validation.leeway = self.validation.leeway;
        let mut claims = decode::<JwtClaims>(token, &decoding_key, &validation)
            .map(|data| data.claims)
            .map_err(|error| {
                error!(%error, "trusted-issuer JWT verification failed");
                IggyError::Unauthenticated
            })?;

        // Remap the external subject onto the fixed configured iggy user; the
        // raw external `sub` is discarded (no auto-provisioning).
        claims.sub = config.user_id.to_string();
        Ok(claims)
    }

    /// Verify a token for the refresh route, additionally refusing any token
    /// minted by a trusted external issuer: those carry their own lifecycle (the
    /// issuer controls their `exp`), so exchanging one for a self-issued token
    /// would mint a bearer that outlives the external grant. Mirrors the legacy
    /// `JwtManager::refresh_token` reject.
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::InvalidAccessToken`] for a trusted-issuer token, or
    /// whatever [`Self::decode`] returns for an otherwise invalid one.
    pub async fn decode_for_refresh(&self, token: &str) -> Result<JwtClaims, IggyError> {
        let claims = self.decode(token).await?;
        if self
            .trusted_issuers
            .contains_key(&normalize_issuer_url(&claims.iss))
        {
            error!(issuer = %claims.iss, "refusing to refresh a trusted-issuer token");
            return Err(IggyError::InvalidAccessToken);
        }
        Ok(claims)
    }

    /// Self-issued HS256 verify against the configured secret and validation.
    /// The synchronous common path shared by the fast path and by every
    /// trusted-issuer classification miss.
    fn decode_self_issued(&self, token: &str) -> Result<JwtClaims, IggyError> {
        decode::<JwtClaims>(token, &self.decoding_key, &self.validation)
            .map(|data| data.claims)
            .map_err(|_| IggyError::Unauthenticated)
    }
}

/// Normalize an issuer URL by lowercasing scheme and host while preserving path
/// case, so trusted-issuer lookup is case-insensitive on the parts that are
/// (`https://Example.COM/PATH` -> `https://example.com/PATH`).
fn normalize_issuer_url(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => {
            let scheme = scheme.to_lowercase();
            let (host, path) = rest.find('/').map_or((rest, ""), |idx| rest.split_at(idx));
            format!("{}://{}{}", scheme, host.to_lowercase(), path)
        }
        None => url.trim_end_matches('/').to_lowercase(),
    }
}

/// Fill in an unconfigured JWT secret exactly like the legacy HTTP server:
/// both empty -> one random ephemeral secret; one empty -> mirror the other;
/// both set but different under an HMAC algorithm -> warn (they must match).
fn normalize_secrets(mut config: HttpJwtConfig, cluster_psk: Option<&str>) -> HttpJwtConfig {
    match (
        config.encoding_secret.is_empty(),
        config.decoding_secret.is_empty(),
    ) {
        (true, true) => {
            if let Some(psk) = cluster_psk {
                // Cluster-wide deterministic key: hex of the derived 256-bit
                // subkey, so every node signs and verifies identically and a
                // follower-forwarded bearer is valid on the primary. Hex (not
                // raw bytes) because the secret travels the same String path
                // a configured secret does.
                let derived = blake3::derive_key(JWT_KEY_CONTEXT, psk.as_bytes());
                let secret = blake3::Hash::from_bytes(derived).to_hex().to_string();
                info!(
                    "JWT secrets are not configured - derived a cluster-wide secret from cluster.auth.shared_secret; tokens are valid on every node and rotate with the PSK"
                );
                config.encoding_secret.clone_from(&secret);
                config.decoding_secret = secret;
                return config;
            }
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

#[derive(Debug, Clone)]
pub(in crate::http) enum Audience {
    Single(String),
    Multiple(Vec<String>),
}

impl From<String> for Audience {
    fn from(aud: String) -> Self {
        Self::Single(aud)
    }
}

impl Serialize for Audience {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Single(aud) => serializer.serialize_str(aud),
            Self::Multiple(auds) => auds.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Audience {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AudienceVisitor;

        impl<'de> serde::de::Visitor<'de> for AudienceVisitor {
            type Value = Audience;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or an array of strings")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Audience::Single(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Audience::Single(value))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut auds = Vec::new();
                while let Some(aud) = seq.next_element::<String>()? {
                    auds.push(aud);
                }
                Ok(Audience::Multiple(auds))
            }
        }

        deserializer.deserialize_any(AudienceVisitor)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(in crate::http) struct JwtClaims {
    pub jti: String,
    pub iss: String,
    pub aud: Audience,
    pub sub: String,
    pub iat: u64,
    pub exp: u64,
    pub nbf: u64,
}

#[derive(Debug)]
pub(in crate::http) struct GeneratedToken {
    pub user_id: UserId,
    pub access_token: String,
    pub access_token_expiry: u64,
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

    // The default config has no trusted issuers, so `decode` takes the
    // synchronous self-issued path; any executor can drive the ready future.
    #[tokio::test]
    async fn token_presented_before_its_nbf_is_rejected() {
        // not_before well past the leeway window, so the freshly issued token
        // is not yet valid and decode must reject it.
        let manager = JwtManager::build(&config("3600 s", "5 s"), None).expect("builds");
        let token = manager.generate(7).expect("issues");
        assert!(
            manager.decode(&token.access_token).await.is_err(),
            "a token presented before its nbf must be rejected"
        );
    }

    #[tokio::test]
    async fn token_at_its_nbf_is_accepted() {
        // Default not_before (0s) => nbf == iat, so the token is valid now and
        // enabling nbf validation does not reject a normally issued token.
        let manager = JwtManager::build(&config("0 s", "5 s"), None).expect("builds");
        let token = manager.generate(7).expect("issues");
        let claims = manager
            .decode(&token.access_token)
            .await
            .expect("valid now");
        assert_eq!(claims.sub, "7");
    }

    #[test]
    fn build_rejects_trusted_issuer_mapping_to_root() {
        let jwt = HttpJwtConfig {
            trusted_issuers: Some(vec![TrustedIssuerConfig {
                issuer: "https://external.example".to_string(),
                audience: "iggy".to_string(),
                jwks_url: "https://external.example/.well-known/jwks.json".to_string(),
                user_id: 0,
            }]),
            ..HttpJwtConfig::default()
        };
        match JwtManager::build(&jwt, None) {
            Err(IggyError::InvalidConfiguration) => {}
            Err(other) => panic!("expected InvalidConfiguration, got {other:?}"),
            Ok(_) => panic!("build must reject a trusted issuer mapping to the root user"),
        }
    }

    #[test]
    fn build_rejects_trusted_issuer_with_empty_issuer() {
        let jwt = HttpJwtConfig {
            trusted_issuers: Some(vec![TrustedIssuerConfig {
                issuer: String::new(),
                audience: "iggy".to_string(),
                jwks_url: "https://external.example/.well-known/jwks.json".to_string(),
                user_id: 1,
            }]),
            ..HttpJwtConfig::default()
        };
        match JwtManager::build(&jwt, None) {
            Err(IggyError::InvalidConfiguration) => {}
            Err(other) => panic!("expected InvalidConfiguration, got {other:?}"),
            Ok(_) => panic!("build must reject a trusted issuer with an empty issuer"),
        }
    }

    #[test]
    fn build_rejects_trusted_issuer_with_empty_jwks_url() {
        let jwt = HttpJwtConfig {
            trusted_issuers: Some(vec![TrustedIssuerConfig {
                issuer: "https://external.example".to_string(),
                audience: "iggy".to_string(),
                jwks_url: String::new(),
                user_id: 1,
            }]),
            ..HttpJwtConfig::default()
        };
        match JwtManager::build(&jwt, None) {
            Err(IggyError::InvalidConfiguration) => {}
            Err(other) => panic!("expected InvalidConfiguration, got {other:?}"),
            Ok(_) => panic!("build must reject a trusted issuer with an empty jwks_url"),
        }
    }

    #[test]
    fn normalize_issuer_url_lowercases_scheme_and_host_preserving_path() {
        assert_eq!(
            normalize_issuer_url("HTTPS://Example.COM/PATH"),
            "https://example.com/PATH"
        );
    }

    #[test]
    fn normalize_issuer_url_no_path() {
        assert_eq!(
            normalize_issuer_url("HTTPS://Example.COM"),
            "https://example.com"
        );
    }

    #[test]
    fn normalize_issuer_url_no_scheme_trims_trailing_slash() {
        assert_eq!(normalize_issuer_url("Example.COM/"), "example.com");
    }

    #[test]
    fn normalize_issuer_url_already_normalized_is_stable() {
        assert_eq!(
            normalize_issuer_url("https://example.com/path"),
            "https://example.com/path"
        );
    }
}
