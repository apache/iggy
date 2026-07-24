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

//! On-disk schema for shard-0 HTTP write admission.
//!
//! Two in-flight caps previously hardcoded in the server-ng HTTP write
//! path, bounding awaited partition writes (produce / consumer-offset)
//! that each park a handler and pin a buffered body while its
//! decode/encode/HS256 CPU runs on the single core that also pumps
//! consensus:
//!
//! - `max_in_flight_writes` - shard-0-global budget across all sessions.
//! - `max_in_flight_writes_per_session` - one credential's slice of that
//!   budget, so a session that outruns its own commits saturates itself
//!   (429) before it can starve the shared budget.
//!
//! The defaults here are the canonical caps; `core/server-ng`'s HTTP
//! layer reads the configured values at boot, and the lockstep test
//! below pins the shipped `config.toml` defaults to these constants so a
//! default deployment keeps the historical behavior.

use super::COMPONENT_NG;
use crate::ConfigurationError;
use configs::ConfigEnv;
use iggy_common::Validatable;
use serde::{Deserialize, Serialize};

/// Canonical shard-0-global in-flight write budget.
pub const DEFAULT_MAX_IN_FLIGHT_WRITES: u32 = 128;

/// Canonical per-session in-flight write cap.
pub const DEFAULT_MAX_IN_FLIGHT_WRITES_PER_SESSION: u32 = 32;

/// Upper bound on `max_in_flight_writes`. Every awaited write pins a
/// buffered request body (budget x `http.max_request_size`) on the single
/// shard-0 core; four thousand in-flight writes is far past any sane
/// deployment and a likely unit typo.
pub const MAX_IN_FLIGHT_WRITES_CEILING: u32 = 4096;

/// Admission caps for shard-0 HTTP awaited partition writes.
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct HttpAdmissionConfig {
    /// Shard-0-global budget for concurrently awaited partition writes
    /// across every session. Bounds the worst-case buffered bytes
    /// (budget x `http.max_request_size`) and how far admitted HTTP work
    /// can delay the consensus pump (budget x per-request CPU). Refusals
    /// past it are the server-busy 503.
    pub max_in_flight_writes: u32,

    /// Per-session cap on concurrently awaited partition writes, bounding
    /// how much of `max_in_flight_writes` one credential may occupy.
    /// Refusals past it are the too-many-in-flight 429, a session's own
    /// backpressure signal before it can starve the shared budget. Must
    /// not exceed `max_in_flight_writes`.
    pub max_in_flight_writes_per_session: u32,
}

impl Validatable<ConfigurationError> for HttpAdmissionConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.max_in_flight_writes == 0 {
            eprintln!("{COMPONENT_NG} http_admission.max_in_flight_writes must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // One ceiling suffices: per_session may not exceed the global budget
        // (checked below), so this bounds it transitively.
        if self.max_in_flight_writes > MAX_IN_FLIGHT_WRITES_CEILING {
            eprintln!(
                "{COMPONENT_NG} http_admission.max_in_flight_writes ({}) exceeds the maximum ({MAX_IN_FLIGHT_WRITES_CEILING})",
                self.max_in_flight_writes
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.max_in_flight_writes_per_session == 0 {
            eprintln!("{COMPONENT_NG} http_admission.max_in_flight_writes_per_session must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // A per-session cap above the global budget can never bind: the
        // session would always hit the shared 503 first, so the 429 signal
        // is dead. Reject the nonsensical ordering (equal is fine - a
        // single session may then fill the whole budget).
        if self.max_in_flight_writes_per_session > self.max_in_flight_writes {
            eprintln!(
                "{COMPONENT_NG} http_admission.max_in_flight_writes_per_session ({}) must not exceed http_admission.max_in_flight_writes ({})",
                self.max_in_flight_writes_per_session, self.max_in_flight_writes
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(global: u32, per_session: u32) -> HttpAdmissionConfig {
        HttpAdmissionConfig {
            max_in_flight_writes: global,
            max_in_flight_writes_per_session: per_session,
        }
    }

    #[test]
    fn embedded_defaults_match_canonical_caps() {
        // Behavior lockstep: `Default` reads the shipped config.toml, which must
        // carry the caps the HTTP admission path historically hardcoded so a
        // default deployment is unaffected by surfacing them as config.
        let config = HttpAdmissionConfig::default();
        assert_eq!(config.max_in_flight_writes, DEFAULT_MAX_IN_FLIGHT_WRITES);
        assert_eq!(
            config.max_in_flight_writes_per_session,
            DEFAULT_MAX_IN_FLIGHT_WRITES_PER_SESSION
        );
    }

    #[test]
    fn default_impl_validates() {
        assert!(HttpAdmissionConfig::default().validate().is_ok());
    }

    #[test]
    fn rejects_zero_global() {
        assert!(
            cfg(0, DEFAULT_MAX_IN_FLIGHT_WRITES_PER_SESSION)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn rejects_zero_per_session() {
        assert!(cfg(DEFAULT_MAX_IN_FLIGHT_WRITES, 0).validate().is_err());
    }

    #[test]
    fn rejects_per_session_above_global() {
        assert!(cfg(32, 33).validate().is_err());
    }

    #[test]
    fn accepts_per_session_equal_global() {
        // A single session may fill the whole budget; equal is the boundary.
        assert!(cfg(32, 32).validate().is_ok());
    }

    #[test]
    fn rejects_above_ceiling() {
        assert!(
            cfg(
                MAX_IN_FLIGHT_WRITES_CEILING + 1,
                DEFAULT_MAX_IN_FLIGHT_WRITES_PER_SESSION
            )
            .validate()
            .is_err()
        );
    }

    #[test]
    fn accepts_at_ceiling() {
        // Inclusive ceiling; per-session rides under it via per_session <= global.
        assert!(
            cfg(
                MAX_IN_FLIGHT_WRITES_CEILING,
                DEFAULT_MAX_IN_FLIGHT_WRITES_PER_SESSION
            )
            .validate()
            .is_ok()
        );
    }
}
