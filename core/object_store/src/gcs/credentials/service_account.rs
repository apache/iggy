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

//! A service account key file: a JWT that the key signs, exchanged at the
//! token endpoint of the key for an access token.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use http::Method;
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde::Deserialize;
use serde_json::json;

use super::{FetchedToken, parse_token, refusal};
use crate::credentials::{CredentialError, CredentialRequest, CredentialTransport};

/// What the token lets a store do: read, write and delete objects, nothing
/// on the bucket itself.
pub const SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";
/// The longest that Google accepts between `iat` and `exp`.
const ASSERTION_LIFETIME: Duration = Duration::from_hours(1);
const PEM_BEGIN: &str = "-----BEGIN PRIVATE KEY-----";
const PEM_END: &str = "-----END PRIVATE KEY-----";

/// The key of a service account, from its JSON key file. `Debug` shows the
/// account and the id of the key, never the key.
#[derive(Clone, PartialEq, Eq)]
pub struct ServiceAccountKey {
    pub client_email: String,
    /// The id of the key, which the JWT names so that the endpoint finds the
    /// public half without trying each key of the account.
    pub private_key_id: Option<String>,
    /// Where the JWT is exchanged for an access token.
    pub token_uri: String,
    /// The RSA key as PKCS#8 DER.
    pkcs8: Vec<u8>,
}

/// Why a key file cannot sign a token request.
#[derive(Debug, thiserror::Error)]
pub enum KeyFileError {
    #[error("the service account key file {path:?} cannot be read")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("the service account key file {path:?} {reason}")]
    Invalid { path: PathBuf, reason: &'static str },
    /// A file of another kind of credentials, such as the `authorized_user`
    /// file of `gcloud auth application-default login`.
    #[error(
        "the key file {path:?} holds {kind:?} credentials; only a service_account key is supported"
    )]
    Unsupported { path: PathBuf, kind: String },
}

/// The fields of a key file that a token request needs.
#[derive(Deserialize)]
struct KeyFile {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    client_email: Option<String>,
    #[serde(default)]
    private_key: Option<String>,
    #[serde(default)]
    private_key_id: Option<String>,
    #[serde(default)]
    token_uri: Option<String>,
}

/// The error answer of a token endpoint.
#[derive(Deserialize)]
struct EndpointError {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

impl ServiceAccountKey {
    /// The key in the text of the key file at `path`, checked to sign.
    ///
    /// # Errors
    /// [`KeyFileError::Unsupported`] for credentials of another kind, and
    /// [`KeyFileError::Invalid`] for a file without an account, a token
    /// endpoint or an RSA key in PKCS#8 PEM.
    pub fn parse(path: &Path, text: &str) -> Result<Self, KeyFileError> {
        let invalid = |reason| KeyFileError::Invalid {
            path: path.to_path_buf(),
            reason,
        };
        // The error of a parser can quote the file, which holds the key.
        let file: KeyFile = serde_json::from_str(text).map_err(|_| invalid("is not a key file"))?;
        if file.kind != "service_account" {
            return Err(KeyFileError::Unsupported {
                path: path.to_path_buf(),
                kind: file.kind,
            });
        }
        let client_email = file
            .client_email
            .filter(|email| !email.is_empty())
            .ok_or_else(|| invalid("has no client_email"))?;
        let pem = file
            .private_key
            .ok_or_else(|| invalid("has no private_key"))?;
        let pkcs8 = pem_body(&pem).ok_or_else(|| invalid("has no PKCS#8 private_key in PEM"))?;
        RsaKeyPair::from_pkcs8(&pkcs8)
            .map_err(|_| invalid("has a private_key that is not an RSA key that can sign"))?;
        Ok(Self {
            client_email,
            private_key_id: file.private_key_id.filter(|id| !id.is_empty()),
            token_uri: file
                .token_uri
                .filter(|uri| !uri.is_empty())
                .unwrap_or_else(|| DEFAULT_TOKEN_URI.to_owned()),
            pkcs8,
        })
    }

    pub(super) async fn fetch(
        &self,
        transport: &impl CredentialTransport,
        now: SystemTime,
    ) -> Result<FetchedToken, CredentialError> {
        let answer = transport.send(self.token_request(now)?).await?;
        if (200..300).contains(&answer.status) {
            return parse_token(&answer.body, now, "the token endpoint");
        }
        let what = match serde_json::from_slice::<EndpointError>(&answer.body) {
            Ok(EndpointError {
                error,
                error_description: Some(description),
            }) => format!("the token endpoint ({error}: {description})"),
            Ok(EndpointError { error, .. }) => format!("the token endpoint ({error})"),
            Err(_) => "the token endpoint".to_owned(),
        };
        Err(refusal(answer.status, &what))
    }

    fn token_request(&self, now: SystemTime) -> Result<CredentialRequest, CredentialError> {
        Ok(CredentialRequest {
            method: Method::POST,
            url: self.token_uri.clone(),
            headers: vec![(
                "content-type",
                "application/x-www-form-urlencoded".to_owned(),
            )],
            // Both values hold only characters that a form leaves as they
            // are, but for the colons of the grant type.
            body: format!(
                "grant_type={}&assertion={}",
                GRANT_TYPE.replace(':', "%3A"),
                self.assertion(now)?
            ),
        })
    }

    /// The JWT that asks for a token of [`SCOPE`], signed with RS256.
    fn assertion(&self, now: SystemTime) -> Result<String, CredentialError> {
        let issued_at = now
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CredentialError::Refused("the clock is before 1970".to_owned()))?
            .as_secs();
        let mut header = json!({"alg": "RS256", "typ": "JWT"});
        if let Some(key_id) = &self.private_key_id {
            header["kid"] = json!(key_id);
        }
        let claims = json!({
            "iss": self.client_email,
            "scope": SCOPE,
            "aud": self.token_uri,
            "iat": issued_at,
            "exp": issued_at + ASSERTION_LIFETIME.as_secs(),
        });
        let input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(header.to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        );
        let refused = || CredentialError::Refused("the service account key cannot sign".to_owned());
        let key_pair = RsaKeyPair::from_pkcs8(&self.pkcs8).map_err(|_| refused())?;
        let mut signature = vec![0; key_pair.public().modulus_len()];
        key_pair
            .sign(
                &RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                input.as_bytes(),
                &mut signature,
            )
            .map_err(|_| refused())?;
        Ok(format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature)))
    }
}

impl fmt::Debug for ServiceAccountKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceAccountKey")
            .field("client_email", &self.client_email)
            .field("private_key_id", &self.private_key_id)
            .field("token_uri", &self.token_uri)
            .finish_non_exhaustive()
    }
}

/// The DER bytes of the one PKCS#8 key in `pem`.
fn pem_body(pem: &str) -> Option<Vec<u8>> {
    let (_, rest) = pem.split_once(PEM_BEGIN)?;
    let (body, _) = rest.split_once(PEM_END)?;
    let base64: String = body.split_whitespace().collect();
    STANDARD.decode(base64).ok()
}

#[cfg(test)]
pub mod tests {
    use ring::signature::{KeyPair, RSA_PKCS1_2048_8192_SHA256, UnparsedPublicKey};
    use serde_json::Value;

    use super::*;
    use crate::credentials::testing::RecordedTransport;

    /// A 2048-bit RSA key that the TLS and JWT tests of the repository use.
    const TEST_KEY: &str = include_str!("../../../../certs/iggy_key.pem");

    /// The text of a service account key file for `client_email`.
    pub fn key_file_json(client_email: &str) -> String {
        json!({
            "type": "service_account",
            "project_id": "iggy-test",
            "private_key_id": "key-1",
            "private_key": TEST_KEY,
            "client_email": client_email,
            "client_id": "1",
            "token_uri": "https://oauth2.example/token",
        })
        .to_string()
    }

    fn key() -> ServiceAccountKey {
        ServiceAccountKey::parse(
            Path::new("key.json"),
            &key_file_json("iggy@iggy-test.iam.gserviceaccount.com"),
        )
        .unwrap()
    }

    fn json_part(part: &str) -> Value {
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(part).unwrap()).unwrap()
    }

    #[test]
    fn given_key_when_fetching_should_post_a_signed_jwt_for_the_storage_scope() {
        let key = key();
        let transport = RecordedTransport::answering([(
            200,
            r#"{"access_token":"ya29.account","expires_in":3600,"token_type":"Bearer"}"#,
        )]);
        let now = UNIX_EPOCH + Duration::from_hours(500_000);

        let fetched = crate::contract::shard_runtime()
            .block_on(key.fetch(&transport, now))
            .unwrap();

        let requests = transport.requests.borrow();
        let [request] = requests.as_slice() else {
            panic!("one request: {requests:?}");
        };
        assert_eq!(request.method, Method::POST);
        assert_eq!(request.url, "https://oauth2.example/token");
        assert_eq!(
            request.headers,
            [(
                "content-type",
                "application/x-www-form-urlencoded".to_owned()
            )]
        );
        let assertion = request
            .body
            .strip_prefix(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=",
            )
            .unwrap();
        let [header, claims, signature] = assertion.split('.').collect::<Vec<_>>()[..] else {
            panic!("three parts: {assertion}");
        };
        assert_eq!(
            json_part(header),
            json!({"alg": "RS256", "typ": "JWT", "kid": "key-1"})
        );
        assert_eq!(
            json_part(claims),
            json!({
                "iss": "iggy@iggy-test.iam.gserviceaccount.com",
                "scope": SCOPE,
                "aud": "https://oauth2.example/token",
                "iat": 1_800_000_000_u64,
                "exp": 1_800_003_600_u64,
            })
        );
        let key_pair = RsaKeyPair::from_pkcs8(&key.pkcs8).unwrap();
        UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, key_pair.public_key().as_ref())
            .verify(
                format!("{header}.{claims}").as_bytes(),
                &URL_SAFE_NO_PAD.decode(signature).unwrap(),
            )
            .expect("the signature verifies with the public key");
        assert_eq!(fetched.token.secret(), "ya29.account");
        assert_eq!(fetched.expires_at, now + Duration::from_hours(1));
    }

    #[test]
    fn given_endpoint_errors_when_fetching_should_tell_a_refusal_from_an_outage() {
        let fetch = |status, body| {
            crate::contract::shard_runtime().block_on(key().fetch(
                &RecordedTransport::answering([(status, body)]),
                SystemTime::now(),
            ))
        };

        let refused = fetch(
            400,
            r#"{"error":"invalid_grant","error_description":"Invalid JWT Signature."}"#,
        );
        let failing = fetch(503, "");

        assert!(
            matches!(&refused, Err(CredentialError::Refused(reason)) if reason.contains("invalid_grant")),
            "{refused:?}"
        );
        assert!(
            matches!(failing, Err(CredentialError::Unavailable(_))),
            "{failing:?}"
        );
    }

    #[test]
    fn given_files_that_cannot_sign_when_parsing_should_refuse() {
        let path = Path::new("key.json");
        let without = |field: &str| {
            let mut file: Value = serde_json::from_str(&key_file_json("a@b")).unwrap();
            file.as_object_mut().unwrap().remove(field);
            file.to_string()
        };
        let not_rsa = key_file_json("a@b").replace("MII", "AAA");

        assert!(matches!(
            ServiceAccountKey::parse(path, r#"{"type":"authorized_user"}"#),
            Err(KeyFileError::Unsupported { kind, .. }) if kind == "authorized_user"
        ));
        for text in [
            "not json".to_owned(),
            without("client_email"),
            without("private_key"),
            not_rsa,
        ] {
            assert!(
                matches!(
                    ServiceAccountKey::parse(path, &text),
                    Err(KeyFileError::Invalid { .. })
                ),
                "{text}"
            );
        }
        assert_eq!(
            ServiceAccountKey::parse(path, &without("token_uri"))
                .unwrap()
                .token_uri,
            DEFAULT_TOKEN_URI
        );
    }

    #[test]
    fn given_key_when_debug_printing_or_failing_should_show_no_key() {
        let key = key();
        let failure = ServiceAccountKey::parse(
            Path::new("key.json"),
            &key_file_json("a@b").replace("service_account\"", "service_account\" x"),
        );

        let printed = format!("{key:?} {failure:?}");

        assert!(!printed.contains("MII"), "{printed}");
        assert!(printed.contains("key-1"), "{printed}");
    }
}
