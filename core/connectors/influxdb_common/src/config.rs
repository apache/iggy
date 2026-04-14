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

//! Version-selection enums and adapter factory.

use crate::adapter::InfluxDbAdapter;
use crate::v2::V2Adapter;
use crate::v3::V3Adapter;
use tracing::warn;

/// Which InfluxDB wire protocol to use.
///
/// Set via `api_version` in the connector config (TOML / env-var).
///
/// | Config value | Meaning                          |
/// |---|---|
/// | `"v2"` / `"2"` / *(omitted)* | InfluxDB 2.x — Flux, `/api/v2/*`, `Token` auth |
/// | `"v3"` / `"3"` | InfluxDB 3.x — SQL, `/api/v3/*`, `Bearer` auth |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ApiVersion {
    /// InfluxDB 2.x — Flux queries, annotated CSV responses, `Token` auth.
    #[default]
    V2,
    /// InfluxDB 3.x — SQL/InfluxQL queries, JSONL responses, `Bearer` auth.
    V3,
}

impl ApiVersion {
    /// Parse `api_version` from a config string.
    ///
    /// Accepts `"v2"`, `"2"`, `"v3"`, `"3"` (case-insensitive).
    /// Unrecognised values warn and default to [`ApiVersion::V2`].
    pub fn from_config(value: Option<&str>) -> Self {
        match value.map(|v| v.to_ascii_lowercase()).as_deref() {
            Some("v3") | Some("3") => ApiVersion::V3,
            Some("v2") | Some("2") | None => ApiVersion::V2,
            Some(other) => {
                warn!(
                    "Unrecognised api_version {:?}; valid values are \"v2\" or \"v3\". \
                     Defaulting to v2.",
                    other
                );
                ApiVersion::V2
            }
        }
    }

    /// Instantiate the adapter for this API version.
    ///
    /// Returns a `Box<dyn InfluxDbAdapter>` so callers need not know the
    /// concrete type.
    pub fn make_adapter(self) -> Box<dyn InfluxDbAdapter> {
        match self {
            ApiVersion::V2 => Box::new(V2Adapter),
            ApiVersion::V3 => Box::new(V3Adapter),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_is_default() {
        assert_eq!(ApiVersion::from_config(None), ApiVersion::V2);
    }

    #[test]
    fn parses_v2_strings() {
        assert_eq!(ApiVersion::from_config(Some("v2")), ApiVersion::V2);
        assert_eq!(ApiVersion::from_config(Some("V2")), ApiVersion::V2);
        assert_eq!(ApiVersion::from_config(Some("2")), ApiVersion::V2);
    }

    #[test]
    fn parses_v3_strings() {
        assert_eq!(ApiVersion::from_config(Some("v3")), ApiVersion::V3);
        assert_eq!(ApiVersion::from_config(Some("V3")), ApiVersion::V3);
        assert_eq!(ApiVersion::from_config(Some("3")), ApiVersion::V3);
    }

    #[test]
    fn unknown_value_falls_back_to_v2() {
        assert_eq!(ApiVersion::from_config(Some("v4")), ApiVersion::V2);
        assert_eq!(ApiVersion::from_config(Some("auto")), ApiVersion::V2);
        assert_eq!(ApiVersion::from_config(Some("")), ApiVersion::V2);
    }

    #[test]
    fn make_adapter_v2_auth_uses_token_scheme() {
        let adapter = ApiVersion::V2.make_adapter();
        assert_eq!(adapter.auth_header_value("mytoken"), "Token mytoken");
    }

    #[test]
    fn make_adapter_v3_auth_uses_bearer_scheme() {
        let adapter = ApiVersion::V3.make_adapter();
        assert_eq!(adapter.auth_header_value("mytoken"), "Bearer mytoken");
    }
}
