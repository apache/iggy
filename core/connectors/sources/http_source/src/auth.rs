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

use ring::hmac;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

/// HMAC algorithms accepted for signature validation. SHA-256 covers GitHub,
/// Stripe, and most modern providers; SHA-1 exists only for legacy senders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HmacAlgorithm {
    HmacSha256,
    HmacSha1,
}

impl HmacAlgorithm {
    fn ring_algorithm(self) -> hmac::Algorithm {
        match self {
            Self::HmacSha256 => hmac::HMAC_SHA256,
            Self::HmacSha1 => hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
        }
    }
}

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
pub fn strip_bearer(header_value: &str) -> Option<&str> {
    let (scheme, token) = header_value.split_once(' ')?;
    scheme.eq_ignore_ascii_case("Bearer").then_some(token)
}

/// Compares two secrets without leaking their contents through timing.
pub fn secrets_match(left: &SecretString, right: &SecretString) -> bool {
    constant_time_eq(
        left.expose_secret().as_bytes(),
        right.expose_secret().as_bytes(),
    )
}

// ring deprecated its direct comparison helper; `hmac::verify` compares
// tags in constant time, so equal inputs iff the tag over one verifies
// against the other.
/// Built once. The key material is empty and fixed, so rebuilding it per
/// comparison only repeated the block-size padding work on the token path.
static COMPARE_KEY: LazyLock<hmac::Key> = LazyLock::new(|| hmac::Key::new(hmac::HMAC_SHA256, &[]));

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    // Signing one side and verifying the other compares tags of a fixed
    // length, so neither the result nor the timing depends on where the inputs
    // first differ. Total time is still proportional to their length, since
    // each HMAC runs one compression per block, so a presented token's length
    // remains observable. That is the known caveat of this construction and is
    // acceptable here: length alone does not narrow a secret's content.
    let left_tag = hmac::sign(&COMPARE_KEY, left);
    hmac::verify(&COMPARE_KEY, right, left_tag.as_ref()).is_ok()
}

/// Validates an HMAC signature over the raw request body bytes, never a
/// re-serialized form (whitespace or key-order changes would break the hash).
/// `ring::hmac::verify` compares in constant time.
pub fn validate_hmac(
    body: &[u8],
    signature_header: Option<&str>,
    signature_prefix: &str,
    secret: &SecretString,
    algorithm: HmacAlgorithm,
) -> bool {
    let Some(header_value) = signature_header else {
        return false;
    };
    let Some(signature_hex) = header_value.strip_prefix(signature_prefix) else {
        return false;
    };
    let Ok(expected_signature) = hex::decode(signature_hex) else {
        return false;
    };
    let key = hmac::Key::new(
        algorithm.ring_algorithm(),
        secret.expose_secret().as_bytes(),
    );
    hmac::verify(&key, body, &expected_signature).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "whsec_test_secret";
    const BODY: &[u8] = br#"{"event": "push", "repository": "apache/iggy"}"#;

    fn secret() -> SecretString {
        SecretString::from(SECRET)
    }

    fn github_style_signature(body: &[u8], algorithm: HmacAlgorithm) -> String {
        let key = hmac::Key::new(algorithm.ring_algorithm(), SECRET.as_bytes());
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
        let signature = format!(
            "sha256={}",
            github_style_signature(BODY, HmacAlgorithm::HmacSha256)
        );
        assert!(validate_hmac(
            BODY,
            Some(&signature),
            "sha256=",
            &secret(),
            HmacAlgorithm::HmacSha256,
        ));
    }

    #[test]
    fn given_valid_sha1_signature_when_hmac_validated_should_accept() {
        let signature = format!(
            "sha1={}",
            github_style_signature(BODY, HmacAlgorithm::HmacSha1)
        );
        assert!(validate_hmac(
            BODY,
            Some(&signature),
            "sha1=",
            &secret(),
            HmacAlgorithm::HmacSha1,
        ));
    }

    #[test]
    fn given_tampered_body_when_hmac_validated_should_reject() {
        let signature = format!(
            "sha256={}",
            github_style_signature(BODY, HmacAlgorithm::HmacSha256)
        );
        assert!(!validate_hmac(
            br#"{"event": "push", "repository": "attacker/repo"}"#,
            Some(&signature),
            "sha256=",
            &secret(),
            HmacAlgorithm::HmacSha256,
        ));
    }

    #[test]
    fn given_missing_signature_when_hmac_validated_should_reject() {
        assert!(!validate_hmac(
            BODY,
            None,
            "sha256=",
            &secret(),
            HmacAlgorithm::HmacSha256,
        ));
    }

    #[test]
    fn given_wrong_prefix_when_hmac_validated_should_reject() {
        let signature = format!(
            "sha1={}",
            github_style_signature(BODY, HmacAlgorithm::HmacSha256)
        );
        assert!(!validate_hmac(
            BODY,
            Some(&signature),
            "sha256=",
            &secret(),
            HmacAlgorithm::HmacSha256,
        ));
    }

    #[test]
    fn given_malformed_hex_when_hmac_validated_should_reject() {
        assert!(!validate_hmac(
            BODY,
            Some("sha256=not-hex-at-all"),
            "sha256=",
            &secret(),
            HmacAlgorithm::HmacSha256,
        ));
    }

    #[test]
    fn given_empty_prefix_when_hmac_validated_should_accept_raw_hex() {
        let signature = github_style_signature(BODY, HmacAlgorithm::HmacSha256);
        assert!(validate_hmac(
            BODY,
            Some(&signature),
            "",
            &secret(),
            HmacAlgorithm::HmacSha256,
        ));
    }
}
