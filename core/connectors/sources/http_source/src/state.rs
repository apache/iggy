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

//! Endpoint registry and its round trip through `ConnectorState`.
//!
//! The registry is per instance and authoritative: the route table is a
//! projection of it, and it is the unit the runtime persists after a
//! successful send. Endpoints created through the management API exist only
//! here, so without this round trip a restart would drop them.
//!
//! Persisting them means writing their bearer tokens and HMAC secrets to the
//! runtime state directory in the clear. That is the same at-rest posture as
//! the TOML those endpoints would otherwise live in; the README requires
//! `chmod 700` on the state path.

use iggy_connector_sdk::{ConnectorState, Error};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use tracing::info;

use crate::routes::{Endpoint, EndpointOrigin};
use crate::types::EndpointId;
use crate::{CONNECTOR_NAME, EndpointAuthType, StaticEndpointConfig};

/// Every secret-path endpoint one instance owns, static and dynamic alike.
///
/// Ordered so the encoding is deterministic; an unordered map would produce
/// different bytes for an identical registry.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EndpointRegistry {
    #[serde(default)]
    endpoints: BTreeMap<EndpointId, Endpoint>,
}

impl EndpointRegistry {
    /// Merges the TOML endpoints with whatever the runtime persisted.
    ///
    /// TOML wins for endpoints it declares: editing a static endpoint and
    /// restarting the instance is the documented way to change it. The one
    /// exception is a revocation tombstone, which always wins, so an operator
    /// who revoked a compromised endpoint does not get it back by restarting
    /// against a TOML file nobody remembered to edit.
    /// Returns `Err` when state existed and could not be decoded. That case
    /// cannot be served: every revocation tombstone lives in the state, so
    /// continuing on the TOML alone would put an endpoint that was revoked for
    /// being compromised straight back on the wire. Failing here surfaces as
    /// `last_error` on the control API, which is the only way an operator
    /// learns the tombstones are unreadable.
    pub fn restore(
        static_endpoints: &[StaticEndpointConfig],
        state: Option<ConnectorState>,
        connector_id: u32,
    ) -> Result<Self, Error> {
        let mut endpoints: BTreeMap<EndpointId, Endpoint> = static_endpoints
            .iter()
            .map(|config| (config.endpoint_id.clone(), Endpoint::from(config)))
            .collect();
        let static_count = endpoints.len();

        let had_state = state.is_some();
        let Some(persisted) = state
            .and_then(|state| state.deserialize::<EndpointRegistry>(CONNECTOR_NAME, connector_id))
        else {
            if had_state {
                return Err(Error::InitError(format!(
                    "Cannot decode the persisted registry for {CONNECTOR_NAME} connector ID: {connector_id}. Refusing to serve {static_count} static endpoints without its revocation tombstones"
                )));
            }
            info!(
                "Started {CONNECTOR_NAME} connector ID: {connector_id} with no persisted registry, static endpoints: {static_count}"
            );
            return Ok(EndpointRegistry { endpoints });
        };

        let mut restored = 0;
        let mut tombstones = 0;
        for (endpoint_id, mut endpoint) in persisted.endpoints {
            endpoint.submitted = true;
            let revoked = !endpoint.is_active();
            match endpoints.entry(endpoint_id) {
                Entry::Occupied(mut occupied) if revoked => {
                    occupied.insert(endpoint);
                    tombstones += 1;
                }
                Entry::Occupied(_) => {}
                Entry::Vacant(vacant) => {
                    vacant.insert(endpoint);
                    if revoked {
                        tombstones += 1;
                    } else {
                        restored += 1;
                    }
                }
            }
        }
        info!(
            "Restored registry for {CONNECTOR_NAME} connector ID: {connector_id}, static endpoints: {static_count}, dynamic endpoints: {restored}, revoked: {tombstones}"
        );

        Ok(EndpointRegistry { endpoints })
    }

    pub fn endpoints(&self) -> impl Iterator<Item = &Endpoint> {
        self.endpoints.values()
    }

    pub fn endpoint(&self, endpoint_id: &str) -> Option<&Endpoint> {
        self.endpoints.get(endpoint_id)
    }

    pub fn endpoint_mut(&mut self, endpoint_id: &str) -> Option<&mut Endpoint> {
        self.endpoints.get_mut(endpoint_id)
    }

    /// Registers a new endpoint, refusing to overwrite an existing one so a
    /// generated-id collision can never silently retarget live traffic.
    pub fn insert(&mut self, endpoint: Endpoint) -> bool {
        match self.endpoints.entry(endpoint.endpoint_id.clone()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(vacant) => {
                vacant.insert(endpoint);
                true
            }
        }
    }

    /// Drops an endpoint outright, as opposed to tombstoning it. Only for
    /// undoing a registration that never became reachable; a live endpoint is
    /// always revoked instead, so the tombstone survives a restart.
    pub fn remove(&mut self, endpoint_id: &str) -> bool {
        self.endpoints.remove(endpoint_id).is_some()
    }

    pub fn revoke(&mut self, endpoint_id: &str, reason: String, revoked_at: u64) -> bool {
        let Some(endpoint) = self.endpoints.get_mut(endpoint_id) else {
            return false;
        };
        if !endpoint.is_active() {
            return false;
        }
        endpoint.revoke(reason, revoked_at);
        true
    }

    /// Flags the whole registry as handed to the runtime for persistence.
    ///
    /// The plugin never learns whether the save itself succeeded: the runtime
    /// writes state only after the batch carrying it lands in Iggy, and no
    /// acknowledgement comes back across the FFI. That is why the flag is
    /// `submitted` rather than `persisted` - it is the strongest claim this
    /// side of the boundary can honestly make.
    pub fn mark_submitted(&mut self) {
        for endpoint in self.endpoints.values_mut() {
            endpoint.submitted = true;
        }
    }

    /// Endpoints that would accept a request right now: neither revoked nor
    /// past their expiry. An expired endpoint is still `Active` in lifecycle
    /// terms but answers 404, so counting it as serving would mislead.
    pub fn serving_count(&self, now_seconds: u64) -> usize {
        self.endpoints
            .values()
            .filter(|endpoint| endpoint.is_serving(now_seconds))
            .count()
    }

    pub fn serving_count_by_origin(&self, origin: EndpointOrigin, now_seconds: u64) -> usize {
        self.endpoints
            .values()
            .filter(|endpoint| endpoint.is_serving(now_seconds) && endpoint.origin == origin)
            .count()
    }

    /// Serving endpoints whose URL is the only thing guarding them.
    pub fn serving_count_without_auth(&self, now_seconds: u64) -> usize {
        self.endpoints
            .values()
            .filter(|endpoint| {
                endpoint.is_serving(now_seconds) && endpoint.auth_type == EndpointAuthType::None
            })
            .count()
    }

    pub fn expired_count(&self, now_seconds: u64) -> usize {
        self.endpoints
            .values()
            .filter(|endpoint| endpoint.is_active() && endpoint.is_expired(now_seconds))
            .count()
    }

    pub fn revoked_count(&self) -> usize {
        self.endpoints
            .values()
            .filter(|endpoint| !endpoint.is_active())
            .count()
    }

    /// Whether every endpoint has been handed to the runtime, which is what
    /// the admin listener reports as `state_submitted`.
    pub fn all_submitted(&self) -> bool {
        self.endpoints.values().all(|endpoint| endpoint.submitted)
    }

    pub fn to_connector_state(&self, connector_id: u32) -> Option<ConnectorState> {
        ConnectorState::serialize(self, CONNECTOR_NAME, connector_id)
    }
}

/// Writes an endpoint secret in the clear, for `ConnectorState` only.
///
/// A local helper rather than the shared
/// [`iggy_common::serde_secret::serialize_optional_secret`] so that the one
/// place in this crate that deliberately writes a secret is greppable and
/// cannot be reached by accident: `HttpSourceConfig` does not implement
/// `Serialize` at all, precisely so a credential cannot leak that way.
pub fn serialize_secret_to_state<S>(
    secret: &Option<SecretString>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match secret {
        Some(secret) => serializer.serialize_some(secret.expose_secret()),
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EndpointAuthType;
    use crate::routes::EndpointState;
    use crate::test_support::{ENDPOINT_ONE, ENDPOINT_TWO, endpoint_id, static_endpoint};

    fn dynamic_endpoint(raw_id: &str) -> Endpoint {
        Endpoint {
            endpoint_id: endpoint_id(raw_id),
            auth_type: EndpointAuthType::Bearer,
            auth_secret: Some(SecretString::from("whsec_dynamic")),
            hmac_header: crate::DEFAULT_HMAC_HEADER.to_string(),
            hmac_prefix: crate::DEFAULT_HMAC_PREFIX.to_string(),
            expires_at: None,
            origin: EndpointOrigin::Dynamic,
            state: EndpointState::Active,
            submitted: false,
        }
    }

    fn registry_state(registry: &EndpointRegistry) -> ConnectorState {
        registry
            .to_connector_state(1)
            .expect("registry must serialize")
    }

    #[test]
    fn given_revoked_endpoint_when_serialized_should_not_carry_the_secret() {
        let mut registry = EndpointRegistry::default();
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));
        assert!(registry.revoke(ENDPOINT_ONE, "compromised".to_string(), 42));

        let ConnectorState(bytes) = registry_state(&registry);

        assert!(
            !bytes
                .windows(b"whsec_dynamic".len())
                .any(|window| window == b"whsec_dynamic"),
            "revoking because a secret leaked must not then persist that secret; the handler 404s before authorize() runs and nothing compacts tombstones"
        );
    }

    #[test]
    fn given_persisted_state_should_restore_dynamic_endpoints() {
        let mut original = EndpointRegistry::default();
        assert!(original.insert(dynamic_endpoint(ENDPOINT_TWO)));

        let restored = EndpointRegistry::restore(&[], Some(registry_state(&original)), 1)
            .expect("the registry must restore");

        let endpoint = restored
            .endpoint(ENDPOINT_TWO)
            .expect("dynamic endpoint must survive the round trip");
        assert_eq!(endpoint.auth_type, EndpointAuthType::Bearer);
        assert_eq!(
            endpoint
                .auth_secret
                .as_ref()
                .map(|secret| secret.expose_secret()),
            Some("whsec_dynamic"),
            "a redacted secret would reject every request the sender signs"
        );
        assert!(endpoint.submitted);
    }

    #[test]
    fn given_no_state_should_start_from_static_config_only() {
        let restored = EndpointRegistry::restore(&[static_endpoint(ENDPOINT_ONE)], None, 1)
            .expect("the registry must restore");

        assert_eq!(restored.endpoints().count(), 1);
        assert!(restored.endpoint(ENDPOINT_ONE).is_some());
        assert!(restored.endpoint(ENDPOINT_TWO).is_none());
    }

    #[test]
    fn given_invalid_state_when_restored_should_refuse_to_serve_static_config() {
        let invalid = ConnectorState(b"not valid msgpack".to_vec());

        let restored =
            EndpointRegistry::restore(&[static_endpoint(ENDPOINT_ONE)], Some(invalid), 1);

        assert!(
            restored.is_err(),
            "state that existed and cannot be decoded has lost every tombstone, so serving the static config would put a revoked endpoint back on the wire"
        );
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let mut original = EndpointRegistry::default();
        assert!(original.insert(dynamic_endpoint(ENDPOINT_ONE)));
        original.revoke(ENDPOINT_ONE, "compromised".to_string(), 42);

        let bytes = rmp_serde::to_vec(&original).expect("registry must serialize");
        let deserialized: EndpointRegistry =
            rmp_serde::from_slice(&bytes).expect("registry must deserialize");

        assert_eq!(
            original.endpoints().count(),
            deserialized.endpoints().count()
        );
        assert_eq!(
            deserialized
                .endpoint(ENDPOINT_ONE)
                .expect("endpoint must survive")
                .state,
            EndpointState::Revoked {
                reason: "compromised".to_string(),
                revoked_at: 42,
            }
        );
    }

    #[test]
    fn serialize_state_helper_should_produce_valid_connector_state() {
        let mut registry = EndpointRegistry::default();
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));

        let bytes = registry
            .to_connector_state(1)
            .expect("registry must serialize")
            .0;

        let restored: EndpointRegistry =
            rmp_serde::from_slice(&bytes).expect("registry must deserialize");
        assert!(restored.endpoint(ENDPOINT_ONE).is_some());
    }

    /// Mirrors `Endpoint`'s wire shape minus the trailing `state`, i.e. what a
    /// writer from before that field existed would have emitted. Built as a
    /// struct rather than by editing bytes, so it stays honest if the fixture
    /// changes.
    #[derive(Serialize)]
    struct EndpointMissingState {
        endpoint_id: String,
        auth_type: String,
        auth_secret: Option<String>,
        hmac_header: String,
        hmac_prefix: String,
        expires_at: Option<u64>,
        origin: String,
    }

    /// `Endpoint` plus one appended, defaulted field: what a FUTURE version
    /// looks like reading today's bytes.
    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct EndpointWithAddedField {
        endpoint_id: String,
        auth_type: String,
        auth_secret: Option<String>,
        hmac_header: String,
        hmac_prefix: String,
        expires_at: Option<u64>,
        origin: String,
        state: EndpointState,
        #[serde(default)]
        added_later: Option<u64>,
    }

    /// The registry is a struct with one field, so under rmp's compact codec
    /// it encodes as a one-element array wrapping the map, not a bare map.
    #[derive(Serialize)]
    struct RegistryMissingState {
        endpoints: BTreeMap<String, EndpointMissingState>,
    }

    #[derive(Deserialize)]
    struct RegistryWithAddedField {
        endpoints: BTreeMap<String, EndpointWithAddedField>,
    }

    #[test]
    fn given_a_field_appended_later_when_old_bytes_are_read_should_still_decode() {
        // The forward-compat contract: append, and give the new field a
        // default. Old state files must keep decoding.
        let mut registry = EndpointRegistry::default();
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));
        let bytes = rmp_serde::to_vec(&registry).expect("registry must serialize");

        let decoded: RegistryWithAddedField =
            rmp_serde::from_slice(&bytes).expect("appending a defaulted field must stay readable");

        assert_eq!(decoded.endpoints.len(), 1);
        assert!(decoded.endpoints[ENDPOINT_ONE].added_later.is_none());
    }

    #[test]
    fn given_a_record_missing_its_state_when_restored_should_fail_closed() {
        // The opposite direction, and the one that matters for security: a
        // record that cannot supply `state` must NOT decode as Active. It
        // previously did, because `state` carried `#[serde(default)]` and
        // `EndpointState::default()` was `Active` - which silently put a
        // revoked endpoint back into service.
        let truncated = RegistryMissingState {
            endpoints: BTreeMap::from([(
                ENDPOINT_ONE.to_string(),
                EndpointMissingState {
                    endpoint_id: ENDPOINT_ONE.to_string(),
                    auth_type: "bearer".to_string(),
                    auth_secret: Some("whsec_dynamic".to_string()),
                    hmac_header: crate::DEFAULT_HMAC_HEADER.to_string(),
                    hmac_prefix: crate::DEFAULT_HMAC_PREFIX.to_string(),
                    expires_at: None,
                    origin: "Dynamic".to_string(),
                },
            )]),
        };
        let bytes = rmp_serde::to_vec(&truncated).expect("the shadow must serialize");

        assert!(
            rmp_serde::from_slice::<EndpointRegistry>(&bytes).is_err(),
            "a record that cannot supply its lifecycle state must fail the decode"
        );

        // Static config carrying the same id is the case that matters: with
        // `&[]` there is nothing to resurrect, so the assertion passed however
        // restore behaved.
        let restored = EndpointRegistry::restore(
            &[static_endpoint(ENDPOINT_ONE)],
            Some(ConnectorState(bytes)),
            1,
        );
        assert!(
            restored.is_err(),
            "and restore must refuse rather than serve the static twin of a record it could not decode"
        );
    }

    #[test]
    fn given_revoked_endpoint_in_state_when_restored_should_keep_tombstone_over_static_config() {
        let mut persisted = EndpointRegistry::default();
        assert!(persisted.insert(dynamic_endpoint(ENDPOINT_ONE)));
        persisted.revoke(ENDPOINT_ONE, "compromised".to_string(), 42);

        let restored = EndpointRegistry::restore(
            &[static_endpoint(ENDPOINT_ONE)],
            Some(registry_state(&persisted)),
            1,
        )
        .expect("the registry must restore");

        let endpoint = restored.endpoint(ENDPOINT_ONE).expect("entry must exist");
        assert!(
            !endpoint.is_active(),
            "a stale TOML entry must not resurrect a revoked endpoint"
        );
    }

    #[test]
    fn given_revoked_dynamic_endpoint_when_restored_should_keep_the_tombstone() {
        // No static counterpart, which is the ordinary case: revoke a
        // dynamically registered endpoint, restart, and the leaked URL must
        // still be dead.
        let mut persisted = EndpointRegistry::default();
        assert!(persisted.insert(dynamic_endpoint(ENDPOINT_TWO)));
        persisted.revoke(ENDPOINT_TWO, "compromised".to_string(), 42);

        let restored = EndpointRegistry::restore(&[], Some(registry_state(&persisted)), 1)
            .expect("the registry must restore");

        let endpoint = restored
            .endpoint(ENDPOINT_TWO)
            .expect("the tombstone must survive the restart, not vanish with it");
        assert!(!endpoint.is_active());
        assert_eq!(restored.serving_count(0), 0);
    }

    #[test]
    fn given_registered_endpoint_when_removed_should_drop_it_without_a_tombstone() {
        let mut registry = EndpointRegistry::default();
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));

        assert!(registry.remove(ENDPOINT_ONE));
        assert!(
            !registry.remove(ENDPOINT_ONE),
            "removing twice must report that there was nothing left to undo"
        );
        assert!(!registry.remove(ENDPOINT_TWO));
        // Outright, not tombstoned: this only undoes a registration that never
        // became reachable, so there is no revocation to preserve.
        assert!(registry.endpoint(ENDPOINT_ONE).is_none());
        assert_eq!(registry.endpoints().count(), 0);
    }

    #[test]
    fn given_active_endpoint_in_both_when_restored_should_prefer_static_config() {
        let mut persisted = EndpointRegistry::default();
        assert!(persisted.insert(dynamic_endpoint(ENDPOINT_ONE)));

        let restored = EndpointRegistry::restore(
            &[static_endpoint(ENDPOINT_ONE)],
            Some(registry_state(&persisted)),
            1,
        )
        .expect("the registry must restore");

        let endpoint = restored.endpoint(ENDPOINT_ONE).expect("entry must exist");
        assert_eq!(
            endpoint.auth_type,
            EndpointAuthType::HmacSha256,
            "editing TOML and restarting is the documented way to change a static endpoint"
        );
    }

    #[test]
    fn given_active_endpoint_when_revoked_twice_should_reject_the_second() {
        let mut registry = EndpointRegistry::default();
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));

        assert!(registry.revoke(ENDPOINT_ONE, "compromised".to_string(), 42));
        assert!(!registry.revoke(ENDPOINT_ONE, "again".to_string(), 43));
        assert!(!registry.revoke(ENDPOINT_TWO, "unknown".to_string(), 44));
    }

    #[test]
    fn given_existing_endpoint_id_when_inserted_should_reject() {
        let mut registry = EndpointRegistry::default();

        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));
        assert!(!registry.insert(dynamic_endpoint(ENDPOINT_ONE)));
        assert_eq!(registry.endpoints().count(), 1);
    }

    #[test]
    fn given_mixed_registry_when_counted_should_exclude_tombstones() {
        let mut registry = EndpointRegistry::default();
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_TWO)));
        registry.revoke(ENDPOINT_TWO, "compromised".to_string(), 42);

        assert_eq!(registry.endpoints().count(), 2);
        assert_eq!(registry.serving_count(0), 1);
    }
}
