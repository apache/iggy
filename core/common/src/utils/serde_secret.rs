/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

//! Serde serialization helpers for `SecretString` fields.
//!
//! `SecretString` intentionally does not implement `Serialize` to prevent
//! accidental secret exposure. These helpers are for fields that **must** be
//! serialized (e.g., wire protocol payloads, persisted TOML configs, API
//! responses that already expose credentials by design).
//!
//! Usage:
//! ```ignore
//! #[serde(serialize_with = "crate::utils::serde_secret::serialize_secret")]
//! pub password: SecretString,
//! ```
//!
//! Do **not** add `serialize_with` to fields that should remain redacted in
//! serialized output — rely on `SecretString`'s default behavior instead.

use secrecy::{ExposeSecret, SecretString};

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
}
