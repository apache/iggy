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
//! requirements and its destination bridge from one wait-free snapshot.

use ring::hmac;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use crate::auth::is_usable_secret;
use crate::state::EndpointRegistry;
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

impl EndpointState {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Revoked { .. } => "revoked",
        }
    }
}

/// Where an endpoint came from, which decides whether a TOML edit plus an
/// instance restart or a management API call is the way to change it.
#[derive(Debug, Clone, Copy, Default, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndpointOrigin {
    #[default]
    Static,
    Dynamic,
}

impl EndpointOrigin {
    /// Deliberately not `Display`: the metrics encoder wants a `&str` and
    /// would allocate one per series on every scrape.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Static => "static",
            Self::Dynamic => "dynamic",
        }
    }
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
    // No default, for symmetry with `state` rather than because it can save
    // us: msgpack compact encodes this struct as a positional array, so a
    // record can only be short from the end, and this sits second of eight.
    // `state` is last, which is why the guard there is the one that works.
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
pub struct RouteEntry {
    pub instance: Arc<SharedState>,
    pub endpoint: Endpoint,
    /// Derived once here rather than per request. The table is rebuilt on
    /// every registry mutation, so a rotated secret cannot be verified against
    /// a stale key.
    pub hmac_key: Option<hmac::Key>,
}

// Hand-written because `hmac::Key` is not `Debug`, and a key has no business
// in a log line even if it were.
impl fmt::Debug for RouteEntry {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RouteEntry")
            .field("instance", &self.instance)
            .field("endpoint", &self.endpoint)
            .field("hmac_key", &self.hmac_key.is_some())
            .finish()
    }
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
    /// Whether any path would accept a request right now.
    ///
    /// `build` inserts revoked and expired endpoints too, so that a leaked id
    /// resolves to a 404 attributed to its owner rather than to `unrouted`.
    /// That makes a bare emptiness check useless for readiness: an instance
    /// whose endpoints have all been revoked would still look routable.
    pub fn serves_anything(&self, now_seconds: u64) -> bool {
        !self.named_paths.is_empty()
            || self
                .secret_paths
                .values()
                .any(|entry| entry.endpoint.is_serving(now_seconds))
    }

    /// Projects every joined instance's registry into one lookup table.
    pub fn build(instances: &[Arc<SharedState>]) -> Result<Self, RouteConflict> {
        Self::build_with(instances, None)
    }

    /// Whether a registration would claim an endpoint id a sibling instance
    /// already serves.
    ///
    /// Id equality only, deliberately. This runs inside `registry_writer`, and
    /// building a table there to answer a yes/no question would clone every
    /// endpoint and derive an `hmac::Key` for every active HMAC endpoint on
    /// every instance sharing the listener, up to `MAX_ENDPOINTS` each. The
    /// real projection happens in `refresh_routes` afterwards, outside the
    /// lock, and remains the authority: a sibling registering the same id is
    /// holding its own writer lock, not this one, so this is a cheap early
    /// refusal rather than a mutual exclusion.
    ///
    /// Only the new id can collide, because every entry already in the registry
    /// was checked the same way when it was admitted.
    pub fn claims_foreign_id(
        instances: &[Arc<SharedState>],
        registering: u32,
        endpoint_id: &EndpointId,
    ) -> Result<(), RouteConflict> {
        for instance in instances {
            if instance.id == registering {
                continue;
            }
            if instance.registry().endpoint(endpoint_id.as_str()).is_some() {
                return Err(RouteConflict::EndpointId {
                    endpoint_id: endpoint_id.clone(),
                    held_by: instance.id,
                    claimed_by: registering,
                });
            }
        }
        Ok(())
    }

    /// As [`RouteTable::build`], but projecting `candidate` in place of the
    /// named instance's own registry.
    ///
    /// Exists so a control-plane mutation can be checked before it is
    /// published. Publishing first and undoing on failure cannot be made
    /// correct: the flush is armed by the same call that publishes, so the
    /// runtime may already have persisted the change this handler is about to
    /// refuse, and an insert that reclaimed tombstones to make room does not
    /// get them back.
    pub fn build_with(
        instances: &[Arc<SharedState>],
        candidate: Option<(u32, &EndpointRegistry)>,
    ) -> Result<Self, RouteConflict> {
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
            let own = instance.registry();
            let registry = match candidate {
                Some((candidate_id, candidate)) if candidate_id == instance.id => candidate,
                _ => own.as_ref(),
            };
            for endpoint in registry.endpoints() {
                match table.secret_paths.entry(endpoint.endpoint_id.clone()) {
                    Entry::Occupied(occupied) => {
                        return Err(RouteConflict::EndpointId {
                            endpoint_id: endpoint.endpoint_id.clone(),
                            held_by: occupied.get().instance.id,
                            claimed_by: instance.id,
                        });
                    }
                    Entry::Vacant(vacant) => {
                        // Gated on `is_usable_secret`, not just presence. An
                        // empty key is valid for HMAC, so `Some("")` builds a
                        // real key that anyone holding the URL can sign for.
                        // `validate()` and `register_endpoint` both refuse one,
                        // but restore deliberately does not fail a whole
                        // instance over one stored value, so this is where an
                        // empty secret arriving from an older build or from the
                        // state store has to fail closed. `authorize` already
                        // rejects every signed request when the key is `None`.
                        let hmac_key = if is_usable_secret(&endpoint.auth_secret) {
                            endpoint
                                .auth_type
                                .hmac_algorithm()
                                .zip(endpoint.auth_secret.as_ref())
                                .map(|(algorithm, secret)| crate::auth::hmac_key(algorithm, secret))
                        } else {
                            None
                        };
                        vacant.insert(RouteEntry {
                            instance: Arc::clone(instance),
                            endpoint: endpoint.clone(),
                            hmac_key,
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
    fn given_a_candidate_registry_when_built_should_project_it_instead_of_the_published_one() {
        // The whole point of the candidate: a registration is checked before it
        // is published, so the endpoint it would add has to be visible to the
        // build. Without the substitution the build sees only what is already
        // published, which cannot conflict with an endpoint that does not exist
        // yet, so the validator would answer Ok to everything and the check
        // would be a no-op nothing could detect.
        let first = instance(1, Some("github"), &[ENDPOINT_ONE]);
        let second = instance(7, Some("stripe"), &[]);

        // Second is about to register an id the first already serves.
        let mut candidate = EndpointRegistry::default();
        assert!(candidate.insert(Endpoint::from(&StaticEndpointConfig {
            endpoint_id: endpoint_id(ENDPOINT_ONE),
            auth_type: EndpointAuthType::None,
            auth_secret: None,
            hmac_header: crate::DEFAULT_HMAC_HEADER.to_string(),
            hmac_prefix: crate::DEFAULT_HMAC_PREFIX.to_string(),
            expires_at: None,
        })));

        let instances = [Arc::clone(&first), Arc::clone(&second)];
        RouteTable::build(&instances).expect("the published registries alone do not conflict");

        let conflict = RouteTable::build_with(&instances, Some((7, &candidate)))
            .expect_err("the candidate steals an id the first instance serves");
        assert_eq!(
            conflict,
            RouteConflict::EndpointId {
                endpoint_id: endpoint_id(ENDPOINT_ONE),
                held_by: 1,
                claimed_by: 7,
            }
        );
    }

    #[test]
    fn given_a_candidate_when_built_should_replace_only_its_own_instances_registry() {
        // The discriminating half. Asserting only the conflict does not pin the
        // substitution: "replace for the named instance", "replace for every
        // instance" and "merge into the existing one" all produce the identical
        // error from the negative case. Only the positive case tells them
        // apart, and "replace for every instance" is the dangerous survivor -
        // it would validate a registration on one instance against a table in
        // which its sibling's real endpoints had been swapped out for the
        // candidate, so a genuine collision would pass.
        let first = instance(1, Some("github"), &[ENDPOINT_ONE]);
        let second = instance(7, Some("stripe"), &[ENDPOINT_TWO]);
        let instances = [Arc::clone(&first), Arc::clone(&second)];

        // An empty candidate for instance 7 removes ENDPOINT_TWO and nothing
        // else. Merging would keep it, giving 2; replacing for every instance
        // would drop ENDPOINT_ONE as well, giving 0.
        let empty = EndpointRegistry::default();
        let table = RouteTable::build_with(&instances, Some((7, &empty)))
            .expect("an empty candidate conflicts with nothing");
        assert_eq!(
            table.secret_path_count(),
            1,
            "only instance 7's registry may be replaced"
        );
        assert!(
            matches!(
                table.lookup_secret_path(ENDPOINT_ONE, NOW),
                RouteLookup::Active(_)
            ),
            "instance 1 keeps serving its own endpoint"
        );
        assert!(matches!(
            table.lookup_secret_path(ENDPOINT_TWO, NOW),
            RouteLookup::Unknown
        ));

        // Both named paths survive either way, so they are asserted separately
        // from the substitution they do not exercise.
        assert_eq!(table.named_path_count(), 2);
    }

    #[test]
    fn given_an_id_a_sibling_serves_when_registering_should_refuse() {
        // What the registration validator actually asks, and all it needs to:
        // does another instance on this listener already serve the id. No
        // clone, no key derivation, and it runs under the registry lock.
        let first = instance(1, Some("github"), &[ENDPOINT_ONE]);
        let second = instance(7, Some("stripe"), &[ENDPOINT_TWO]);
        let instances = [Arc::clone(&first), Arc::clone(&second)];

        let conflict = RouteTable::claims_foreign_id(&instances, 7, &endpoint_id(ENDPOINT_ONE))
            .expect_err("instance 1 already serves it");
        assert_eq!(
            conflict,
            RouteConflict::EndpointId {
                endpoint_id: endpoint_id(ENDPOINT_ONE),
                held_by: 1,
                claimed_by: 7,
            }
        );
    }

    #[test]
    fn given_an_id_only_the_registrant_holds_when_registering_should_allow() {
        // The registrant's own registry must not count against it: the
        // candidate it is validating already contains the id being added, so
        // comparing against itself would refuse every registration.
        let first = instance(1, Some("github"), &[ENDPOINT_ONE]);
        let second = instance(7, Some("stripe"), &[ENDPOINT_TWO]);
        let instances = [Arc::clone(&first), Arc::clone(&second)];

        assert!(
            RouteTable::claims_foreign_id(&instances, 7, &endpoint_id(ENDPOINT_TWO)).is_ok(),
            "an instance may keep serving the ids it already holds"
        );
        assert!(
            RouteTable::claims_foreign_id(&instances, 7, &endpoint_id("c".repeat(32).as_str()))
                .is_ok(),
            "and a genuinely new id collides with nothing"
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
        source.mutate_registry(|registry| {
            registry.revoke(ENDPOINT_ONE, "compromised".to_string(), 1)
        });

        let table = RouteTable::build(&[source]).expect("must build");

        assert!(matches!(
            table.lookup_secret_path(ENDPOINT_ONE, NOW),
            RouteLookup::Revoked(_)
        ));
    }

    #[tokio::test]
    async fn given_expired_endpoint_when_looked_up_should_report_expired() {
        let source = instance(1, None, &[ENDPOINT_ONE]);
        source.mutate_registry(|registry| {
            registry
                .endpoint_mut(ENDPOINT_ONE)
                .expect("static endpoint is registered")
                .expires_at = Some(NOW);
            true
        });

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
        source.mutate_registry(|registry| {
            let endpoint = registry
                .endpoint_mut(ENDPOINT_ONE)
                .expect("static endpoint is registered");
            endpoint.expires_at = Some(NOW);
            endpoint.revoke("compromised".to_string(), NOW);
            true
        });

        let table = RouteTable::build(&[source]).expect("must build");

        assert!(matches!(
            table.lookup_secret_path(ENDPOINT_ONE, NOW + 1),
            RouteLookup::Revoked(_)
        ));
    }
}
