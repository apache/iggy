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

use axum::http::header::HeaderName;
use ring::hmac;
use secrecy::{ExposeSecret, SecretString};
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use subtle::ConstantTimeEq;

use crate::EndpointAuthType;

/// Validates `Authorization: Bearer <token>` in constant time.
pub fn validate_bearer(authorization_header: Option<&str>, expected_token: &SecretString) -> bool {
    let Some(header_value) = authorization_header else {
        return false;
    };
    let Some(presented_token) = strip_bearer(header_value) else {
        return false;
    };
    constant_time_eq(
        presented_token.as_bytes(),
        expected_token.expose_secret().as_bytes(),
    )
}

/// Splits `Bearer <token>`. RFC 7235 makes the scheme case-insensitive, so a
/// sender using `bearer` is legitimate and must not be turned away.
fn strip_bearer(header_value: &str) -> Option<&str> {
    let (scheme, token) = header_value.split_once(' ')?;
    // RFC 9110's grammar is `auth-scheme [ 1*SP token68 ]`, so more than one
    // space is legal and the extra would otherwise fail the compare.
    scheme
        .eq_ignore_ascii_case("Bearer")
        .then(|| token.trim_start_matches(' '))
}

/// Ceilings for the operator-supplied strings that ride an endpoint.
///
/// Unlike a revoke `reason`, which is written once onto a tombstone, these sit
/// on active entries: every `mutate_registry` deep-clones them and every flush
/// re-serializes the whole registry, so they are paid for repeatedly. Without a
/// cap the only limit was the body limit, up to 64 MiB, times `MAX_ENDPOINTS`.
///
/// Generous on purpose. A real HMAC secret is tens of bytes and a header name
/// is shorter still; these refuse abuse without refusing anything an operator
/// would plausibly configure.
pub const MAX_AUTH_SECRET_LEN: usize = 4096;
pub const MAX_HMAC_HEADER_LEN: usize = 256;
pub const MAX_HMAC_PREFIX_LEN: usize = 64;

/// When an endpoint is being admitted, which decides whether the rules that
/// depend on the clock apply.
///
/// The distinction is load-bearing, not bookkeeping. `validate()` runs inside
/// `open()`, so a rule applied there is re-applied on every restart. Most of
/// what makes an endpoint inadmissible is time-invariant and safe to re-check;
/// expiry is not, and re-checking it turns one endpoint's 404 into a failure
/// of the whole instance, taking its other endpoints and its named topic path
/// down with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// A new endpoint arriving through the management API. Every rule applies.
    Creating { now_seconds: u64 },
    /// An endpoint already declared in TOML or restored from state. It was
    /// admissible when it was created; time has passed since, and that alone
    /// must not refuse it.
    Existing,
}

/// Why an endpoint may not be admitted.
///
/// One enum rather than per-caller strings, so the callers cannot drift on the
/// rules they share. Config had already drifted once, admitting an
/// `auth_type: none` carrying a secret that the API refused.
///
/// They do differ on expiry, deliberately: see [`Admission`]. That difference
/// is in the signature rather than in two divergent lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inadmissible {
    /// `auth_type` advertises a second factor with no usable secret behind it.
    MissingSecret,
    /// `auth_type: none` carrying a secret nothing will ever read.
    UnusedSecret,
    /// `hmac_header` is not a valid HTTP header name.
    InvalidHmacHeader,
    /// `expires_at` is already at or past, so the endpoint would 404 forever.
    AlreadyExpired,
    /// `auth_secret` is longer than [`MAX_AUTH_SECRET_LEN`].
    SecretTooLong,
    /// `hmac_header` is longer than [`MAX_HMAC_HEADER_LEN`].
    HmacHeaderTooLong,
    /// `hmac_prefix` is longer than [`MAX_HMAC_PREFIX_LEN`].
    HmacPrefixTooLong,
}

impl Inadmissible {
    /// The operator-facing reason. `&'static str` so it can be an API error
    /// body without an allocation per refused request.
    pub fn message(&self) -> &'static str {
        match self {
            Self::MissingSecret => "a non-empty auth_secret is required",
            Self::UnusedSecret => "auth_type 'none' takes no auth_secret",
            Self::InvalidHmacHeader => "hmac_header is not a valid HTTP header name",
            Self::AlreadyExpired => "expires_at is already past",
            Self::SecretTooLong => "auth_secret is longer than 4096 bytes",
            Self::HmacHeaderTooLong => "hmac_header is longer than 256 bytes",
            Self::HmacPrefixTooLong => "hmac_prefix is longer than 64 bytes",
        }
    }
}

impl Display for Inadmissible {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

/// Whether any operator-supplied string on an endpoint is past its ceiling.
///
/// Shared so the three callers cannot drift: registration and rotation refuse
/// on it, `restore` only warns. It names which field tripped rather than
/// answering yes or no, so a warning can say what to shorten.
pub fn oversized_field(
    auth_secret: &Option<SecretString>,
    hmac_header: &str,
    hmac_prefix: &str,
) -> Option<Inadmissible> {
    if let Some(secret) = auth_secret
        && let Some(oversized) = oversized_secret(secret)
    {
        return Some(oversized);
    }
    if hmac_header.len() > MAX_HMAC_HEADER_LEN {
        return Some(Inadmissible::HmacHeaderTooLong);
    }
    if hmac_prefix.len() > MAX_HMAC_PREFIX_LEN {
        return Some(Inadmissible::HmacPrefixTooLong);
    }
    None
}

/// The ceiling on a secret on its own, for a rotation, which carries nothing
/// else. Shared with [`oversized_field`] so the two cannot disagree.
pub fn oversized_secret(secret: &SecretString) -> Option<Inadmissible> {
    (secret.expose_secret().len() > MAX_AUTH_SECRET_LEN).then_some(Inadmissible::SecretTooLong)
}

/// The one admission rule for an endpoint, whether it arrives from TOML or
/// from the management API.
///
/// Kept here beside [`is_usable_secret`] for the same reason that helper
/// exists: written out at both call sites, the two lists drifted. Config
/// admitted an `auth_type: none` carrying a secret that the API refused, and
/// nothing but a reader comparing the two functions would have caught it.
pub fn admit_endpoint(
    auth_type: EndpointAuthType,
    auth_secret: &Option<SecretString>,
    hmac_header: &str,
    hmac_prefix: &str,
    expires_at: Option<u64>,
    admission: Admission,
) -> Result<(), Inadmissible> {
    if let Some(oversized) = oversized_field(auth_secret, hmac_header, hmac_prefix) {
        return Err(oversized);
    }
    if auth_type == EndpointAuthType::None {
        // `authorize` never reads a secret for an unauthenticated endpoint, so
        // accepting one only persists a credential nothing will ever check.
        if auth_secret.is_some() {
            return Err(Inadmissible::UnusedSecret);
        }
    } else if !is_usable_secret(auth_secret) {
        return Err(Inadmissible::MissingSecret);
    }
    // `HeaderMap::get` answers `None` for a name it cannot parse rather than
    // failing, so an endpoint carrying a malformed one 401s every signed
    // request forever, survives a restart, and gives no clue why.
    if HeaderName::from_str(hmac_header).is_err() {
        return Err(Inadmissible::InvalidHmacHeader);
    }
    // Only at creation. An endpoint that has since expired keeps serving 404
    // on its own path, which is what the README documents and what the active
    // endpoint gauge already reports.
    if let Admission::Creating { now_seconds } = admission
        && expires_at.is_some_and(|expires_at| expires_at <= now_seconds)
    {
        return Err(Inadmissible::AlreadyExpired);
    }
    Ok(())
}

/// Whether a secret is present and non-empty.
///
/// An empty key is valid for HMAC, so accepting one would leave `auth_type`
/// advertising a second factor that anyone holding the URL can compute. Shared
/// so the config path and the management API cannot disagree about it.
pub fn is_usable_secret(secret: &Option<SecretString>) -> bool {
    secret.as_ref().is_some_and(is_usable)
}

/// The same rule for a secret that is always present, such as a rotation's
/// replacement. Delegated to rather than written out, so rotate cannot come to
/// disagree with registration about what an empty secret means.
pub fn is_usable(secret: &SecretString) -> bool {
    !secret.expose_secret().is_empty()
}

/// Compares two secrets without leaking their contents through timing.
pub fn secrets_match(left: &SecretString, right: &SecretString) -> bool {
    constant_time_eq(
        left.expose_secret().as_bytes(),
        right.expose_secret().as_bytes(),
    )
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    // `subtle` is the purpose-built primitive; this used to sign one side and
    // verify the other, which was a workaround for ring deprecating its own
    // comparison helper. Either way a presented token's length is observable,
    // here because unequal lengths answer immediately. That caveat is
    // unchanged and acceptable: length alone does not narrow a secret.
    left.ct_eq(right).into()
}

/// Longest tag any supported algorithm produces: SHA-256 at 32 bytes.
const MAX_TAG_LEN: usize = 32;

/// Derives the verification key for an endpoint, once.
///
/// Called from `RouteTable::build`, so the key is rebuilt whenever the table
/// is, which is on every registry mutation. That is what keeps a rotated
/// secret from being verified against the old key.
pub fn hmac_key(algorithm: hmac::Algorithm, secret: &SecretString) -> hmac::Key {
    hmac::Key::new(algorithm, secret.expose_secret().as_bytes())
}

/// Validates an HMAC signature over the raw request body bytes, never a
/// re-serialized form (whitespace or key-order changes would break the hash).
/// `ring::hmac::verify` compares in constant time.
pub fn validate_hmac(
    body: &[u8],
    signature_header: Option<&str>,
    signature_prefix: &str,
    key: &hmac::Key,
) -> bool {
    let Some(header_value) = signature_header else {
        return false;
    };
    let Some(signature_hex) = header_value.strip_prefix(signature_prefix) else {
        return false;
    };
    // Decoded into a stack buffer: this runs per request, and the tag is at
    // most 32 bytes. An odd or oversized hex string is rejected here rather
    // than allocated for.
    if signature_hex.len() % 2 != 0 || signature_hex.len() / 2 > MAX_TAG_LEN {
        return false;
    }
    let tag_len = signature_hex.len() / 2;
    let mut tag = [0u8; MAX_TAG_LEN];
    if hex::decode_to_slice(signature_hex, &mut tag[..tag_len]).is_err() {
        return false;
    }
    hmac::verify(key, body, &tag[..tag_len]).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "whsec_test_secret";
    const BODY: &[u8] = br#"{"event": "push", "repository": "apache/iggy"}"#;

    fn secret() -> SecretString {
        SecretString::from(SECRET)
    }

    fn github_style_signature(body: &[u8], algorithm: hmac::Algorithm) -> String {
        let key = hmac::Key::new(algorithm, SECRET.as_bytes());
        let tag = hmac::sign(&key, body);
        hex::encode(tag.as_ref())
    }

    #[test]
    fn given_valid_token_when_bearer_validated_should_accept() {
        let header = format!("Bearer {SECRET}");
        assert!(validate_bearer(Some(&header), &secret()));
    }

    #[test]
    fn given_wrong_token_when_bearer_validated_should_reject() {
        assert!(!validate_bearer(Some("Bearer wrong"), &secret()));
    }

    #[test]
    fn given_missing_header_when_bearer_validated_should_reject() {
        assert!(!validate_bearer(None, &secret()));
    }

    #[test]
    fn given_extra_spaces_after_scheme_when_bearer_validated_should_accept() {
        // RFC 9110 allows `1*SP` between the scheme and the token, so the extra
        // space belongs to the separator rather than to the credential.
        let header = format!("Bearer   {SECRET}");
        assert!(validate_bearer(Some(&header), &secret()));
    }

    #[test]
    fn given_lowercase_scheme_when_bearer_validated_should_accept() {
        let header = format!("bearer {SECRET}");
        assert!(
            validate_bearer(Some(&header), &secret()),
            "RFC 7235 makes the auth scheme case-insensitive"
        );
    }

    #[test]
    fn given_wrong_scheme_when_bearer_validated_should_reject() {
        let header = format!("Basic {SECRET}");
        assert!(!validate_bearer(Some(&header), &secret()));
    }

    #[test]
    fn given_valid_sha256_signature_when_hmac_validated_should_accept() {
        let signature = format!("sha256={}", github_style_signature(BODY, hmac::HMAC_SHA256));
        assert!(validate_hmac(
            BODY,
            Some(&signature),
            "sha256=",
            &hmac_key(hmac::HMAC_SHA256, &secret()),
        ));
    }

    #[test]
    fn given_valid_sha1_signature_when_hmac_validated_should_accept() {
        let signature = format!(
            "sha1={}",
            github_style_signature(BODY, hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY)
        );
        assert!(validate_hmac(
            BODY,
            Some(&signature),
            "sha1=",
            &hmac_key(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, &secret()),
        ));
    }

    #[test]
    fn given_tampered_body_when_hmac_validated_should_reject() {
        let signature = format!("sha256={}", github_style_signature(BODY, hmac::HMAC_SHA256));
        assert!(!validate_hmac(
            br#"{"event": "push", "repository": "attacker/repo"}"#,
            Some(&signature),
            "sha256=",
            &hmac_key(hmac::HMAC_SHA256, &secret()),
        ));
    }

    #[test]
    fn given_missing_signature_when_hmac_validated_should_reject() {
        assert!(!validate_hmac(
            BODY,
            None,
            "sha256=",
            &hmac_key(hmac::HMAC_SHA256, &secret()),
        ));
    }

    #[test]
    fn given_wrong_prefix_when_hmac_validated_should_reject() {
        let signature = format!("sha1={}", github_style_signature(BODY, hmac::HMAC_SHA256));
        assert!(!validate_hmac(
            BODY,
            Some(&signature),
            "sha256=",
            &hmac_key(hmac::HMAC_SHA256, &secret()),
        ));
    }

    #[test]
    fn given_oversized_signature_when_hmac_validated_should_reject() {
        // The tag is decoded into a 32 byte stack buffer, so anything longer
        // has to be refused before the decode rather than overrun it.
        let header = format!("sha256={}", "a".repeat((MAX_TAG_LEN + 1) * 2));
        assert!(!validate_hmac(
            BODY,
            Some(&header),
            "sha256=",
            &hmac_key(hmac::HMAC_SHA256, &secret()),
        ));
    }

    #[test]
    fn given_odd_length_signature_when_hmac_validated_should_reject() {
        let header = "sha256=abc".to_string();
        assert!(!validate_hmac(
            BODY,
            Some(&header),
            "sha256=",
            &hmac_key(hmac::HMAC_SHA256, &secret()),
        ));
    }

    #[test]
    fn given_malformed_hex_when_hmac_validated_should_reject() {
        assert!(!validate_hmac(
            BODY,
            Some("sha256=not-hex-at-all"),
            "sha256=",
            &hmac_key(hmac::HMAC_SHA256, &secret()),
        ));
    }

    #[test]
    fn given_empty_prefix_when_hmac_validated_should_accept_raw_hex() {
        let signature = github_style_signature(BODY, hmac::HMAC_SHA256);
        assert!(validate_hmac(
            BODY,
            Some(&signature),
            "",
            &hmac_key(hmac::HMAC_SHA256, &secret()),
        ));
    }
}
