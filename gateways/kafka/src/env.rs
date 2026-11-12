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

//! Parsing shared by every reader of the `IGGY_KAFKA_*` environment surface.
//!
//! In the library rather than in `main`, because the switches that decide whether credentials are
//! protected are read on both sides of that boundary: `main` reads `IGGY_KAFKA_SASL_ENABLED` and
//! `auth` reads `IGGY_KAFKA_IGGY_TLS_ENABLED`. Two copies of the same accept-only-these-spellings
//! rule is one copy away from the two disagreeing.

/// Parses a boolean switch, accepting only the two spellings an operator can check by eye.
///
/// Not `str::parse::<bool>` alone, because the failure mode matters here: a typo in the value of a
/// variable that turns a protection on must stop the process, never quietly leave it serving
/// unauthenticated traffic or sending passwords in the clear.
///
/// # Errors
///
/// Returns a message naming `key` when `raw` is anything but `true` or `false`.
pub fn parse_bool(key: &str, raw: &str) -> Result<bool, String> {
    match raw {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(format!(
            "invalid {key} `{other}`: expected `true` or `false`"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_bool;

    /// A variable that turns authentication or transport security on must fail loudly on a typo,
    /// never default to off. Every spelling a person might reach for is rejected except the two
    /// documented ones.
    #[test]
    fn parse_bool_accepts_only_the_two_documented_spellings() {
        assert_eq!(parse_bool("KEY", "true"), Ok(true));
        assert_eq!(parse_bool("KEY", "false"), Ok(false));
        for typo in [
            "1", "0", "TRUE", "False", "yes", "no", "on", "off", "", " true",
        ] {
            assert!(
                parse_bool("KEY", typo).is_err(),
                "{typo:?} must not be silently treated as a boolean"
            );
        }
    }
}
