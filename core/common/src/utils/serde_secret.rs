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
