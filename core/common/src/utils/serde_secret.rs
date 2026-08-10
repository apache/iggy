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

//! Serde serialization helpers for `SecretString` fields.
//!
//! `SecretString` intentionally does not implement `Serialize`, and that
//! absence is the protection: a struct holding one cannot derive `Serialize`
//! at all. Adding `serialize_with` is therefore what *unblocks* the derive, so
//! reaching for a helper here is a decision to serialize a credential, never a
//! way to avoid it.
//!
//! [`serialize_secret`] and [`serialize_optional_secret`] write the plaintext.
//! Use them only where the plaintext is the point: wire protocol payloads,
//! persisted configs, API responses that expose credentials by design.
//!
//! ```ignore
//! #[serde(serialize_with = "crate::utils::serde_secret::serialize_secret")]
//! pub password: SecretString,
//! ```
//!
//! [`serialize_redacted`] and [`serialize_optional_redacted`] write
//! [`REDACTED`] in place of the value, for a struct that must be serializable
//! for unrelated reasons but whose credential no reader is entitled to.
//!
//! **Redacted output is not a config.** Deserializing it hands back the literal
//! [`REDACTED`] as the secret, silently, so a redact-then-reload round trip
//! replaces the credential with the placeholder instead of failing. Nothing
//! in-tree can reach that today: these helpers have no consumers, and the one
//! persist/reload path round-trips a raw `serde_json::Value` rather than a
//! typed struct. If a consumer ever needs the round trip closed mechanically,
//! the shape that cannot be half-applied is a newtype owning both directions,
//! not a paired `deserialize_with` that a caller can forget to add.
//!
//! If neither applies, leave `serialize_with` off and let the missing impl keep
//! the field unserializable.

use secrecy::{ExposeSecret, SecretString};

/// Placeholder written in place of a redacted secret.
pub const REDACTED: &str = "[REDACTED]";

pub fn serialize_secret<S: serde::Serializer>(
    secret: &SecretString,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(secret.expose_secret())
}

pub fn serialize_optional_secret<S: serde::Serializer>(
    secret: &Option<SecretString>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match secret {
        Some(s) => serializer.serialize_some(s.expose_secret()),
        None => serializer.serialize_none(),
    }
}

/// Writes [`REDACTED`] instead of the secret.
pub fn serialize_redacted<S: serde::Serializer>(
    _secret: &SecretString,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(REDACTED)
}

/// Writes [`REDACTED`] instead of the secret, keeping `None` distinguishable.
///
/// Whether a credential is configured at all is not itself a secret, and
/// collapsing `Some` to `null` would tell a reader the field is unset.
pub fn serialize_optional_redacted<S: serde::Serializer>(
    secret: &Option<SecretString>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match secret {
        Some(_) => serializer.serialize_some(REDACTED),
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct WithSecret {
        #[serde(serialize_with = "serialize_secret")]
        password: SecretString,
    }

    #[derive(Serialize, Deserialize)]
    struct WithOptionalSecret {
        #[serde(serialize_with = "serialize_optional_secret")]
        token: Option<SecretString>,
    }

    #[test]
    fn serialize_secret_preserves_value_in_json() {
        let s = WithSecret {
            password: SecretString::from("my_password"),
        };
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, r#"{"password":"my_password"}"#);
    }

    #[test]
    fn serialize_secret_roundtrips_through_json() {
        let original = WithSecret {
            password: SecretString::from("roundtrip"),
        };
        let json = serde_json::to_string(&original).unwrap();
        let restored: WithSecret = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.password.expose_secret(), "roundtrip");
    }

    #[test]
    fn serialize_optional_secret_with_some_value() {
        let s = WithOptionalSecret {
            token: Some(SecretString::from("tok_123")),
        };
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, r#"{"token":"tok_123"}"#);
    }

    #[test]
    fn serialize_optional_secret_with_none() {
        let s = WithOptionalSecret { token: None };
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, r#"{"token":null}"#);
    }

    #[derive(Serialize)]
    struct WithRedactedSecret {
        #[serde(serialize_with = "serialize_redacted")]
        password: SecretString,
    }

    #[derive(Serialize)]
    struct WithOptionalRedactedSecret {
        #[serde(serialize_with = "serialize_optional_redacted")]
        token: Option<SecretString>,
    }

    #[test]
    fn serialize_redacted_replaces_value_in_json() {
        let s = WithRedactedSecret {
            password: SecretString::from("my_password"),
        };
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, r#"{"password":"[REDACTED]"}"#);
        assert!(!json.contains("my_password"));
    }

    #[test]
    fn serialize_optional_redacted_keeps_some_distinguishable_from_none() {
        let present = WithOptionalRedactedSecret {
            token: Some(SecretString::from("tok_123")),
        };
        let absent = WithOptionalRedactedSecret { token: None };

        let present_json = serde_json::to_string(&present).unwrap();
        assert_eq!(present_json, r#"{"token":"[REDACTED]"}"#);
        assert!(!present_json.contains("tok_123"));
        assert_eq!(
            serde_json::to_string(&absent).unwrap(),
            r#"{"token":null}"#,
            "a configured credential must not read as an unset one"
        );
    }
}
