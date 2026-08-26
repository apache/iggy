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

//! Route table for a shared listener.
//!
//! Every instance joining a listener contributes its named topic path and its
//! secret-path endpoints. The merged table is rebuilt and swapped whole on
//! join, leave, and management mutations, so a request resolves its auth
//! requirements and its destination bridge from a single atomic load.

use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use crate::types::EndpointId;
use crate::{EndpointAuthType, SharedState, StaticEndpointConfig};

/// Lifecycle state of a secret-path endpoint.
///
/// Revocation writes a tombstone rather than deleting the entry: the tombstone
/// persists through a restart, so a stale TOML entry can never resurrect an
/// endpoint an operator revoked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndpointState {
    Active,
    Revoked { reason: String, revoked_at: u64 },
}

/// Where an endpoint came from, which decides whether a TOML edit plus an
/// instance restart or a management API call is the way to change it.
#[derive(Debug, Clone, Copy, Default, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndpointOrigin {
    #[default]
    Static,
    Dynamic,
}

/// A secret-path endpoint served at `POST /e/{endpoint_id}`.
///
/// Persisted verbatim into the runtime state directory, HMAC and bearer
/// secrets included, so endpoints created through the management API survive a
/// restart. That file is as sensitive as the TOML it mirrors, which is why the
/// README requires `chmod 700` on the state path.
/// Fields added after the first release must be APPENDED and carry
/// `#[serde(default)]`. `ConnectorState` uses rmp's compact codec, where a
/// struct is a positional array, so a new field without a default makes every
/// existing state file fail to decode, and `EndpointRegistry::restore` turns a
/// decode failure into "tombstones lost, revoked endpoints served again".
///
/// `auth_type` and `state` deliberately have no default: for those two, a
/// missing element must fail the decode rather than read as an active,
/// unauthenticated endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    pub endpoint_id: EndpointId,
    // No default: an absent auth_type must fail the decode, not silently
    // read as `None` and drop the second factor.
    pub auth_type: EndpointAuthType,
    #[serde(default, serialize_with = "crate::state::serialize_secret_to_state")]
    pub auth_secret: Option<SecretString>,
    #[serde(default)]
    pub hmac_header: String,
    #[serde(default)]
    pub hmac_prefix: String,
    /// Unix seconds; requests arriving at or after this answer 404.
    #[serde(default)]
    pub expires_at: Option<u64>,
    #[serde(default)]
    pub origin: EndpointOrigin,
    // No default: `EndpointState::default()` would be `Active`, so a record
    // short by one element would resurrect a revoked endpoint - turning a
    // loud decode failure into a silent, fail-open one.
    pub state: EndpointState,
    /// Whether this endpoint has been handed to the runtime for persistence.
    /// Reset on every mutation, never itself persisted: a restored endpoint is
    /// durable by definition. Not named `persisted`, because the plugin gets
    /// no acknowledgement that the runtime's write actually landed.
    #[serde(skip)]
    pub submitted: bool,
}

impl Endpoint {
    pub fn is_active(&self) -> bool {
        matches!(self.state, EndpointState::Active)
    }

    /// Whether a request arriving now would be accepted.
    pub fn is_serving(&self, now_seconds: u64) -> bool {
        self.is_active() && !self.is_expired(now_seconds)
    }

    pub fn is_expired(&self, now_seconds: u64) -> bool {
        self.expires_at
            .is_some_and(|expires_at| now_seconds >= expires_at)
    }

    pub fn revoke(&mut self, reason: String, revoked_at: u64) {
        self.state = EndpointState::Revoked { reason, revoked_at };
        self.submitted = false;
        // The handler 404s on a revoked entry before `authorize()` runs, so
        // the secret is already dead weight. Keeping it would serialize a
        // leaked credential into the state file, and nothing compacts
        // tombstones, so revoking *because* it leaked would persist it
        // indefinitely.
        self.auth_secret = None;
    }
}

impl From<&StaticEndpointConfig> for Endpoint {
    fn from(config: &StaticEndpointConfig) -> Self {
        Endpoint {
            endpoint_id: config.endpoint_id.clone(),
            auth_type: config.auth_type,
            auth_secret: config.auth_secret.clone(),
            hmac_header: config.hmac_header.clone(),
            hmac_prefix: config.hmac_prefix.clone(),
            expires_at: config.expires_at,
            origin: EndpointOrigin::Static,
            state: EndpointState::Active,
            // Declared in TOML, so it needs no state file to come back.
            submitted: true,
        }
    }
}

/// Immutable snapshot of every path one shared listener serves.
#[derive(Debug, Default)]
pub struct RouteTable {
    secret_paths: HashMap<EndpointId, RouteEntry>,
    named_paths: HashMap<String, Arc<SharedState>>,
}

/// A resolved secret path: the endpoint's own auth rules plus the instance
/// whose bridge receives the body.
#[derive(Debug)]
pub struct RouteEntry {
    pub instance: Arc<SharedState>,
    pub endpoint: Endpoint,
}

/// Outcome of resolving a secret path. `Revoked` and `Unknown` both answer 404
/// so the table never leaks which endpoints once existed; they stay distinct
/// here so the handler can log and meter them apart.
#[derive(Debug)]
pub enum RouteLookup<'a> {
    Active(&'a RouteEntry),
    /// Carries its entry even though the response hides it, so the request can
    /// still be metered against the instance that owns the endpoint.
    Revoked(&'a RouteEntry),
    Expired(&'a RouteEntry),
    Unknown,
}

/// Two instances claiming the same path. Fails the join rather than letting
/// whichever instance opened last silently steal another's traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteConflict {
    EndpointId {
        endpoint_id: EndpointId,
        held_by: u32,
        claimed_by: u32,
    },
    TopicPath {
        topic_path: String,
        held_by: u32,
        claimed_by: u32,
    },
}

impl RouteTable {
    /// Whether anything is routable. An instance with every endpoint revoked
    /// or expired and no named path serves nothing, however joined it is.
    pub fn is_empty(&self) -> bool {
        self.secret_paths.is_empty() && self.named_paths.is_empty()
    }

    /// Projects every joined instance's registry into one lookup table.
    pub fn build(instances: &[Arc<SharedState>]) -> Result<Self, RouteConflict> {
        let mut table = RouteTable::default();
        for instance in instances {
            if let Some(topic_path) = &instance.config.topic_path {
                match table.named_paths.entry(topic_path.clone()) {
                    Entry::Occupied(occupied) => {
                        return Err(RouteConflict::TopicPath {
                            topic_path: topic_path.clone(),
                            held_by: occupied.get().id,
                            claimed_by: instance.id,
                        });
                    }
                    Entry::Vacant(vacant) => {
                        vacant.insert(Arc::clone(instance));
                    }
                }
            }
            for endpoint in instance.registry().endpoints() {
                match table.secret_paths.entry(endpoint.endpoint_id.clone()) {
                    Entry::Occupied(occupied) => {
                        return Err(RouteConflict::EndpointId {
                            endpoint_id: endpoint.endpoint_id.clone(),
                            held_by: occupied.get().instance.id,
                            claimed_by: instance.id,
                        });
                    }
                    Entry::Vacant(vacant) => {
                        vacant.insert(RouteEntry {
                            instance: Arc::clone(instance),
                            endpoint: endpoint.clone(),
                        });
                    }
                }
            }
        }
        Ok(table)
    }

    pub fn lookup_secret_path(&self, endpoint_id: &str, now_seconds: u64) -> RouteLookup<'_> {
        let Some(entry) = self.secret_paths.get(endpoint_id) else {
            return RouteLookup::Unknown;
        };
        if !entry.endpoint.is_active() {
            return RouteLookup::Revoked(entry);
        }
        if entry.endpoint.is_expired(now_seconds) {
            return RouteLookup::Expired(entry);
        }
        RouteLookup::Active(entry)
    }

    pub fn lookup_named_path(&self, topic_path: &str) -> Option<&Arc<SharedState>> {
        self.named_paths.get(topic_path)
    }

    pub fn secret_path_count(&self) -> usize {
        self.secret_paths.len()
    }

    pub fn named_path_count(&self) -> usize {
        self.named_paths.len()
    }
}

impl Display for RouteConflict {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EndpointId {
                endpoint_id,
                held_by,
                claimed_by,
            } => write!(
                formatter,
                "endpoint_id {} is already served by connector ID: {held_by}, claimed by connector ID: {claimed_by}",
                endpoint_id.log_prefix()
            ),
            Self::TopicPath {
                topic_path,
                held_by,
                claimed_by,
            } => write!(
                formatter,
                "topic_path '{topic_path}' is already served by connector ID: {held_by}, claimed by connector ID: {claimed_by}"
            ),
        }
    }
}

impl std::error::Error for RouteConflict {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ENDPOINT_ONE, ENDPOINT_TWO, endpoint_id, instance};

    const NOW: u64 = 1_800_000_000;

    #[test]
    fn given_two_instances_when_table_built_should_serve_both_paths() {
        let first = instance(1, Some("github"), &[ENDPOINT_ONE]);
        let second = instance(2, Some("stripe"), &[ENDPOINT_TWO]);

        let table = RouteTable::build(&[first, second]).expect("distinct paths must not conflict");

        assert_eq!(table.named_path_count(), 2);
        assert_eq!(table.secret_path_count(), 2);
        assert!(table.lookup_named_path("github").is_some());
        assert!(matches!(
            table.lookup_secret_path(ENDPOINT_TWO, NOW),
            RouteLookup::Active(_)
        ));
    }

    #[test]
    fn given_instance_without_topic_path_when_table_built_should_serve_secret_paths_only() {
        let table = RouteTable::build(&[instance(1, None, &[ENDPOINT_ONE])])
            .expect("secret-path-only instance must build");

        assert_eq!(table.named_path_count(), 0);
        assert_eq!(table.secret_path_count(), 1);
    }

    #[test]
    fn given_duplicate_topic_path_when_table_built_should_reject() {
        let first = instance(1, Some("github"), &[ENDPOINT_ONE]);
        let second = instance(7, Some("github"), &[ENDPOINT_TWO]);

        let conflict = RouteTable::build(&[first, second])
            .expect_err("a stolen topic path must fail the join");

        assert_eq!(
            conflict,
            RouteConflict::TopicPath {
                topic_path: "github".to_string(),
                held_by: 1,
                claimed_by: 7,
            }
        );
    }

    #[test]
    fn given_duplicate_endpoint_id_when_table_built_should_reject() {
        let first = instance(1, Some("github"), &[ENDPOINT_ONE]);
        let second = instance(7, Some("stripe"), &[ENDPOINT_ONE]);

        let conflict = RouteTable::build(&[first, second])
            .expect_err("a stolen endpoint id must fail the join");

        assert_eq!(
            conflict,
            RouteConflict::EndpointId {
                endpoint_id: endpoint_id(ENDPOINT_ONE),
                held_by: 1,
                claimed_by: 7,
            }
        );
        // The rendered form reaches the operator's log through
        // `Error::InvalidConfigValue`, and the id is the credential for a
        // secret-path endpoint.
        let message = conflict.to_string();
        assert!(
            message.contains(&ENDPOINT_ONE[..8]),
            "the message must identify which endpoint collided: {message}"
        );
        assert!(
            !message.contains(ENDPOINT_ONE),
            "but never in full: {message}"
        );
    }

    #[test]
    fn given_unknown_endpoint_id_when_looked_up_should_report_unknown() {
        let table = RouteTable::build(&[instance(1, None, &[ENDPOINT_ONE])]).expect("must build");

        assert!(matches!(
            table.lookup_secret_path(ENDPOINT_TWO, NOW),
            RouteLookup::Unknown
        ));
    }

    #[tokio::test]
    async fn given_revoked_endpoint_when_looked_up_should_report_revoked() {
        let source = instance(1, None, &[ENDPOINT_ONE]);
        source
            .mutate_registry(|registry| registry.revoke(ENDPOINT_ONE, "compromised".to_string(), 1))
            .await;

        let table = RouteTable::build(&[source]).expect("must build");

        assert!(matches!(
            table.lookup_secret_path(ENDPOINT_ONE, NOW),
            RouteLookup::Revoked(_)
        ));
    }

    #[tokio::test]
    async fn given_expired_endpoint_when_looked_up_should_report_expired() {
        let source = instance(1, None, &[ENDPOINT_ONE]);
        source
            .mutate_registry(|registry| {
                registry
                    .endpoint_mut(ENDPOINT_ONE)
                    .expect("static endpoint is registered")
                    .expires_at = Some(NOW);
            })
            .await;

        let table = RouteTable::build(&[source]).expect("must build");

        assert!(matches!(
            table.lookup_secret_path(ENDPOINT_ONE, NOW - 1),
            RouteLookup::Active(_)
        ));
        assert!(matches!(
            table.lookup_secret_path(ENDPOINT_ONE, NOW),
            RouteLookup::Expired(_)
        ));
    }

    #[tokio::test]
    async fn given_revoked_and_expired_endpoint_when_looked_up_should_prefer_revoked() {
        let source = instance(1, None, &[ENDPOINT_ONE]);
        source
            .mutate_registry(|registry| {
                let endpoint = registry
                    .endpoint_mut(ENDPOINT_ONE)
                    .expect("static endpoint is registered");
                endpoint.expires_at = Some(NOW);
                endpoint.revoke("compromised".to_string(), NOW);
            })
            .await;

        let table = RouteTable::build(&[source]).expect("must build");

        assert!(matches!(
            table.lookup_secret_path(ENDPOINT_ONE, NOW + 1),
            RouteLookup::Revoked(_)
        ));
    }
}
