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

use crate::auth::{Admission, admit_endpoint};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;
use std::io::Cursor;
use tracing::{info, warn};

use crate::routes::{Endpoint, EndpointOrigin, EndpointState};
use crate::types::{EndpointId, unix_now_seconds};
use crate::{CONNECTOR_NAME, EndpointAuthType, StaticEndpointConfig};

/// Ceiling on how many endpoints one instance's registry will hold.
///
/// Bounds the state file and the per-mutation registry clone against a caller
/// who can register but never has to stop. Far above any real deployment: the
/// README sizes this connector at hundreds of endpoints.
pub const MAX_ENDPOINTS: usize = 10_000;

/// Shape of the persisted state, written into every state file and checked on
/// the way back in.
///
/// This is the migration path, replacing the append-a-defaulted-field trick
/// the endpoint records still use. That trick cannot be applied to the frame:
/// a defaulted field is exactly what lets a shortened blob decode as an empty
/// registry, which is the hole this framing closes. A file carrying any other
/// version is refused rather than guessed at.
const STATE_VERSION: u16 = 1;

/// Why a registration was accepted or refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    /// The generated id already exists. 128 bits of entropy makes this
    /// unreachable in practice; refusing is what stops a collision silently
    /// retargeting live traffic.
    Collision,
    /// At [`MAX_ENDPOINTS`] with nothing reclaimable left.
    Full,
}

/// Every secret-path endpoint one instance owns, static and dynamic alike.
///
/// Ordered so the encoding is deterministic; an unordered map would produce
/// different bytes for an identical registry.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EndpointRegistry {
    // No `#[serde(default)]`. It is the in-memory type's `Default` that gives
    // an empty registry, never a decode: a blob that cannot supply this field
    // is a blob that has lost every revocation tombstone, and defaulting it
    // put the revoked endpoints back on the wire from TOML. The state file
    // goes through `StateFrame`, which refuses that blob outright.
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
    /// a refused `open()`, which `open` logs with the reason. `last_error` on
    /// the control API says only that initialization failed, so the log is
    /// where an operator learns the tombstones are unreadable.
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

        let Some(ConnectorState(blob)) = state else {
            // No state presented, and this arm cannot tell why. The runtime
            // maps a zero-length state file to no state at all, so a genuine
            // first boot and a truncated file arrive here identically, and a
            // truncation means every revocation tombstone is gone while the
            // TOML endpoints below go straight back on the wire with their
            // secrets. That is the fail-open the decode guard exists to
            // refuse, reached without corrupting a single byte, so the one
            // thing this can do is say so where an operator will see it.
            if static_count > 0 {
                warn!(
                    "Started {CONNECTOR_NAME} connector ID: {connector_id} with {static_count} static endpoint(s) and no persisted registry. An empty state file is indistinguishable from a first boot here, so if this instance ever had revocations they are gone and every revoked static endpoint is serving again"
                );
            } else {
                info!(
                    "Started {CONNECTOR_NAME} connector ID: {connector_id} with no persisted registry, static endpoints: {static_count}"
                );
            }
            return Ok(EndpointRegistry { endpoints });
        };

        // `open` logs this before returning it. It is not logged here as well,
        // because `new` holds it until then and a second line would report the
        // same refusal twice.
        let persisted = decode_state_frame(&blob).map_err(|reason| {
            Error::InitError(format!(
                "cannot decode the persisted registry for {CONNECTOR_NAME} connector ID: {connector_id}: {reason}. Refusing to serve {static_count} static endpoints without its revocation tombstones"
            ))
        })?;

        let now_seconds = unix_now_seconds();
        let mut restored = 0;
        let mut tombstones = 0;
        let mut dropped_static = 0;
        for (endpoint_id, mut endpoint) in persisted {
            // The id is both the map key and a field, and both are written
            // out. They cannot disagree here: `decode_state_frame` refuses a
            // frame where they do, rather than preferring one as this used to.
            // Preferring the key repaired the disagreement silently, and that
            // is the wrong direction for the same reason every other guard on
            // this path fails closed: a disagreement means the file is not the
            // one this connector wrote, and the half that split was the half
            // deciding which endpoint a tombstone covers.
            let revoked = !endpoint.is_active();
            // The same rule registration applies, asked here and only warned
            // about. `restore` failing is a hard `open()` failure, so refusing
            // would take a whole instance down over one stored value an
            // operator cannot edit without the state file.
            //
            // Asked rather than re-derived, because the two hand-written checks
            // that used to live here had already drifted from it: they covered
            // the ceilings and a malformed `hmac_header`, and silently skipped
            // an endpoint advertising auth with no usable secret. That one
            // restores active, reads active on the admin API, and rejects every
            // request forever, which is fail-closed and completely silent.
            //
            // `Existing`, so a stored `expires_at` that has merely passed is not
            // reported as a fault: the endpoint answers 404 on its own path and
            // that is the documented behaviour.
            //
            // Skipped for an endpoint whose expiry has already passed. Its
            // secret is cleared below, so on the next restart this rule would
            // report `MissingSecret` and blame the credential for a refusal
            // that expiry already explains.
            if !revoked
                && !endpoint.is_expired(now_seconds)
                && let Err(reason) = admit_endpoint(
                    endpoint.auth_type,
                    &endpoint.auth_secret,
                    &endpoint.hmac_header,
                    &endpoint.hmac_prefix,
                    endpoint.expires_at,
                    Admission::Existing,
                )
            {
                warn!(
                    "Restored endpoint {} for {CONNECTOR_NAME} connector ID: {connector_id}: {reason}; it is served but every request to it will be refused until it is re-registered",
                    endpoint_id.log_prefix()
                );
            }
            match endpoints.entry(endpoint_id) {
                Entry::Occupied(mut occupied) if revoked => {
                    // The map was seeded from TOML, so Occupied means TOML
                    // still declares this id: the tombstone is static whatever
                    // the state file says. Taking the persisted `origin`
                    // verbatim let a `Dynamic` tombstone be the entry that
                    // outranks a live TOML block, and `reclaimable_tombstones`
                    // evicts exactly the dynamic ones, so the next restart
                    // served that endpoint and its secret again. Reclamation is
                    // only safe because a static tombstone is never eligible.
                    endpoint.origin = EndpointOrigin::Static;
                    occupied.insert(endpoint);
                    tombstones += 1;
                }
                Entry::Occupied(_) => {}
                Entry::Vacant(vacant) => {
                    // A static entry reaches this arm only when TOML no longer
                    // declares it. Restoring an active one resurrects an
                    // endpoint the operator decommissioned by deleting it, and
                    // `rotate_secret` then refuses it as static and points at a
                    // TOML entry that is gone, so there is no way back. Its
                    // tombstone still has to survive: re-adding the id to TOML
                    // later must not undo a revocation.
                    if !revoked && endpoint.origin == EndpointOrigin::Static {
                        dropped_static += 1;
                        continue;
                    }
                    vacant.insert(endpoint);
                    if revoked {
                        tombstones += 1;
                    } else {
                        restored += 1;
                    }
                }
            }
        }
        // Same reasoning as `revoke()`: the handler answers 404 on an expired
        // endpoint before `authorize()` runs, so its stored secret is already
        // dead weight, and nothing compacts the registry. Keeping it wrote a
        // credential the operator can no longer use back to disk on every later
        // flush, indefinitely, which is exactly what `revoke()` clears the
        // secret to avoid.
        //
        // The endpoint keeps its slot and stays `Active`. Reclaiming it would
        // make endpoints disappear on a clock condition rather than an operator
        // action.
        //
        // Restore only, deliberately. An endpoint that expires while the
        // instance is running keeps its stored secret until the next restart,
        // because clearing it then would need a clock-driven sweep mutating the
        // registry from `poll()`. `revoke` is what clears it immediately.
        //
        // This clears the in-memory registry and nothing else. It arms no
        // flush, so the state file keeps its copy of the secret until an
        // unrelated mutation writes the registry out, and on an instance that
        // sees none it stays there. Arming the flag here would not fix that
        // either: without a permit a quiet gateway still never flushes, and
        // arming plus notifying would break the contract that a static-only
        // instance writes no state file.
        let mut expired_secrets = 0;
        for endpoint in endpoints.values_mut() {
            if endpoint.is_active()
                && endpoint.is_expired(now_seconds)
                && endpoint.auth_secret.is_some()
            {
                endpoint.auth_secret = None;
                expired_secrets += 1;
            }
        }
        // The third of the three restore-time faults that get a line of their
        // own: an expired endpoint is otherwise silent at startup.
        if expired_secrets > 0 {
            warn!(
                "Cleared the stored secret of {expired_secrets} expired endpoint(s) for {CONNECTOR_NAME} connector ID: {connector_id}; they keep their slot and answer 404 until re-registered"
            );
        }

        // Loud on purpose: these were serving before the restart, and the only
        // trace of them left is this line.
        if dropped_static > 0 {
            warn!(
                "Dropped {dropped_static} persisted static endpoint(s) for {CONNECTOR_NAME} connector ID: {connector_id} that the TOML config no longer declares; they no longer serve"
            );
        }
        info!(
            "Restored registry for {CONNECTOR_NAME} connector ID: {connector_id}, static endpoints: {static_count}, dynamic endpoints: {restored}, revoked: {tombstones}"
        );
        // Warned about, not refused. `validate` rejects a TOML file over the
        // ceiling because an operator can edit that before starting; a state
        // file is not editable without losing every tombstone in it, so
        // failing here would take the instance down over a condition it has no
        // supported way to clear. It serves, and the next registration pays
        // for it: `try_insert` reclaims `len - MAX_ENDPOINTS + 1` tombstones
        // to make room for one endpoint, so an over-full registry discards
        // revocation records in bulk.
        if endpoints.len() > MAX_ENDPOINTS {
            warn!(
                "Restored registry for {CONNECTOR_NAME} connector ID: {connector_id} holds {} endpoints, over the {MAX_ENDPOINTS} ceiling; it serves, but the next registration will reclaim {} revoked entries at once to make room",
                endpoints.len(),
                endpoints.len() - MAX_ENDPOINTS + 1
            );
        }

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

    /// [`Self::try_insert`] for tests that only care whether the registry
    /// changed.
    ///
    /// Test-only on purpose. It collapses `Full` and `Collision` into one
    /// `false`, so production code reaching for it would drop the distinction
    /// between a registry at its ceiling and an id that is already taken, and
    /// answer the caller the same way for both.
    #[cfg(test)]
    pub fn insert(&mut self, endpoint: Endpoint) -> bool {
        self.try_insert(endpoint) == InsertOutcome::Inserted
    }

    /// Registers a new endpoint, reporting why it was refused.
    ///
    /// Nothing evicts tombstones on its own and every mutation clones the whole
    /// registry, so without a ceiling a caller holding the management token can
    /// grow the state file and the per-mutation cost without limit. At the
    /// ceiling the oldest revoked *dynamic* entries are reclaimed first: such an
    /// endpoint exists only here, so dropping it leaves its path answering 404
    /// exactly as its tombstone did. A revoked static one is never reclaimed,
    /// because its tombstone is what outranks a TOML entry that still declares
    /// it.
    ///
    /// Either the whole thing succeeds or nothing is touched, so a refusal
    /// cannot leave the registry partially reclaimed and arm a flush for it.
    pub fn try_insert(&mut self, endpoint: Endpoint) -> InsertOutcome {
        if self.endpoints.contains_key(&endpoint.endpoint_id) {
            return InsertOutcome::Collision;
        }
        if self.endpoints.len() >= MAX_ENDPOINTS {
            let wanted = self.endpoints.len() - MAX_ENDPOINTS + 1;
            let mut reclaimable = self.reclaimable_tombstones();
            if reclaimable.len() < wanted {
                return InsertOutcome::Full;
            }
            reclaimable.truncate(wanted);
            for endpoint_id in reclaimable {
                self.endpoints.remove(&endpoint_id);
            }
        }
        self.endpoints
            .insert(endpoint.endpoint_id.clone(), endpoint);
        InsertOutcome::Inserted
    }

    /// Revoked dynamic entries, oldest revocation first.
    fn reclaimable_tombstones(&self) -> Vec<EndpointId> {
        let mut tombstones: Vec<(u64, EndpointId)> = self
            .endpoints
            .iter()
            .filter_map(|(endpoint_id, endpoint)| match &endpoint.state {
                EndpointState::Revoked { revoked_at, .. }
                    if endpoint.origin == EndpointOrigin::Dynamic =>
                {
                    Some((*revoked_at, endpoint_id.clone()))
                }
                _ => None,
            })
            .collect();
        tombstones.sort_unstable();
        tombstones
            .into_iter()
            .map(|(_, endpoint_id)| endpoint_id)
            .collect()
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

    pub fn to_connector_state(&self, connector_id: u32) -> Option<ConnectorState> {
        let frame = StateFrame {
            version: STATE_VERSION,
            endpoint_count: self.endpoints.len() as u32,
            endpoints: &self.endpoints,
        };
        ConnectorState::serialize(&frame, CONNECTOR_NAME, connector_id)
    }
}

/// The state file's outer frame, and the only shape this connector decodes.
///
/// Generic over the endpoint map so the write side can borrow the live one
/// while the read side owns what it decoded, which keeps a flush from copying
/// the whole registry. MessagePack writes a struct positionally, so both sides
/// agree on a three element shape without the field names reaching the wire.
///
/// `endpoint_count` is redundant with `endpoints.len()` on purpose. It is the
/// only one of the four checks that rejects a blob which is both structurally
/// valid and fully consumed: the registry used to encode as a one element
/// array, so an array holding an empty map decoded as a perfectly good empty
/// registry and no amount of framing arithmetic would have noticed.
#[derive(Serialize, Deserialize)]
struct StateFrame<E> {
    version: u16,
    endpoint_count: u32,
    endpoints: E,
}

/// Why a state blob was refused.
///
/// A variant per check rather than one string, so a test can say which guard
/// fired instead of asserting that something did. Bare `is_err()` would have
/// passed for a frame rejected by the wrong one.
#[derive(Debug, PartialEq, Eq)]
enum FrameError {
    Undecodable(String),
    TrailingBytes {
        consumed: u64,
        len: usize,
    },
    Version(u16),
    CountMismatch {
        declared: u32,
        carried: usize,
    },
    /// Ids carried as `log_prefix`, never whole: a secret-path id is the
    /// credential, and this text reaches a log line.
    MisfiledEndpoint {
        key: String,
        record: String,
    },
}

impl fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Undecodable(error) => write!(formatter, "the frame does not decode ({error})"),
            Self::TrailingBytes { consumed, len } => {
                write!(formatter, "the frame ends after {consumed} of {len} bytes")
            }
            Self::Version(version) => write!(
                formatter,
                "the frame is version {version}, and this connector writes version {STATE_VERSION}"
            ),
            Self::CountMismatch { declared, carried } => write!(
                formatter,
                "the frame declares {declared} endpoint(s) and carries {carried}"
            ),
            Self::MisfiledEndpoint { key, record } => write!(
                formatter,
                "the endpoint filed under {key} carries {record} in its own record"
            ),
        }
    }
}

/// Decodes a state blob, refusing anything that is not exactly one frame.
///
/// `rmp_serde::from_slice` stops at the end of the first value and never
/// checks that it consumed the input. Truncation was never the problem there:
/// a cut blob fails on EOF with or without this, which is why a test built on
/// one proves nothing. The two shapes that did get through were a structurally
/// valid shortening, where the old single-field registry read an empty array
/// as a registry with no endpoints, and an edit that rewrites a value into a
/// smaller one and leaves the rest of the blob unread behind it.
///
/// Reading through a `Cursor` is what makes the consumed length observable at
/// all: `position()` exists only on that flavour of the deserializer, which is
/// why this cannot be done by patching the call in the SDK helper.
///
/// Fail closed, always. Every revocation tombstone lives in this blob, so a
/// decode that guesses is one that puts a compromised endpoint back in service
/// with its secret.
fn decode_state_frame(blob: &[u8]) -> Result<BTreeMap<EndpointId, Endpoint>, FrameError> {
    let mut deserializer = rmp_serde::Deserializer::new(Cursor::new(blob));
    let frame = StateFrame::<BTreeMap<EndpointId, Endpoint>>::deserialize(&mut deserializer)
        .map_err(|error| FrameError::Undecodable(error.to_string()))?;
    let consumed = deserializer.position();
    if consumed != blob.len() as u64 {
        return Err(FrameError::TrailingBytes {
            consumed,
            len: blob.len(),
        });
    }
    if frame.version != STATE_VERSION {
        return Err(FrameError::Version(frame.version));
    }
    if frame.endpoint_count as usize != frame.endpoints.len() {
        return Err(FrameError::CountMismatch {
            declared: frame.endpoint_count,
            carried: frame.endpoints.len(),
        });
    }
    // Every writer keys an endpoint by its own `endpoint_id`, so the two copies
    // agreeing is an invariant and not a coincidence worth repairing. Enforcing
    // it turns the duplication into the one check that catches an edit inside
    // an id: change the key and the tombstone moves to an endpoint nobody
    // revoked, leaving the real one served from TOML; change the field and the
    // record disagrees with where it is filed. A single edit cannot do both, so
    // it cannot stay consistent.
    //
    // `log_prefix`, never the id itself. A secret-path id is the credential.
    if let Some((key, endpoint)) = frame
        .endpoints
        .iter()
        .find(|(key, endpoint)| key.as_str() != endpoint.endpoint_id.as_str())
    {
        return Err(FrameError::MisfiledEndpoint {
            key: key.log_prefix(),
            record: endpoint.endpoint_id.log_prefix(),
        });
    }
    Ok(frame.endpoints)
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
    use crate::auth::{MAX_AUTH_SECRET_LEN, MAX_HMAC_PREFIX_LEN};
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
        }
    }

    /// A static endpoint as it appears *in the state file*, which is where a
    /// TOML-declared endpoint ends up once any flush has run.
    fn persisted_static_endpoint(raw_id: &str) -> Endpoint {
        Endpoint {
            origin: EndpointOrigin::Static,
            ..dynamic_endpoint(raw_id)
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
    }

    /// One ceiling, two answers, and the asymmetry is deliberate. TOML is
    /// something an operator can edit before starting, so an over-full one is
    /// refused. A state file is not editable without losing every tombstone in
    /// it, so an over-full one serves and warns instead of taking the instance
    /// down over a condition with no supported way to clear it.
    #[test]
    fn given_more_endpoints_than_the_ceiling_when_restored_should_serve_them_anyway() {
        // State, not `None`. With no state `restore` returns at its early exit
        // long before the ceiling is looked at, so this asserted nothing about
        // the branch its doc describes and would have passed with the whole
        // post-decode half of `restore` deleted.
        //
        // What it pins is still a decision rather than a fix: nothing here
        // fails if the warning goes away. It is the refusal that must not
        // appear, since a state file over the ceiling cannot be edited down
        // without losing every tombstone in it.
        let statics: Vec<_> = (0..MAX_ENDPOINTS)
            .map(|index| static_endpoint(&format!("{index:032x}")))
            .collect();
        let mut persisted = EndpointRegistry::default();
        assert!(persisted.insert(dynamic_endpoint(ENDPOINT_ONE)));
        let state = persisted
            .to_connector_state(1)
            .expect("registry must serialize");

        let restored = EndpointRegistry::restore(&statics, Some(state), 1)
            .expect("an over-full registry must still serve rather than fail the open");

        assert_eq!(
            restored.endpoints().count(),
            MAX_ENDPOINTS + 1,
            "the merged registry is over the ceiling and every entry still serves"
        );
        assert!(restored.endpoint(ENDPOINT_ONE).is_some());
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
    fn given_a_registry_when_written_to_state_should_read_back_through_the_frame() {
        let mut registry = EndpointRegistry::default();
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));

        let ConnectorState(bytes) = registry
            .to_connector_state(1)
            .expect("registry must serialize");

        let decoded = decode_state_frame(&bytes).expect("what this connector writes must decode");
        assert!(decoded.contains_key(ENDPOINT_ONE));
    }

    /// The four refusals below are one finding: a state blob that has lost its
    /// endpoints must never decode as an empty registry. Every revocation
    /// tombstone lives in that blob, so an empty one puts each revoked
    /// endpoint back on the wire from TOML with its secret. Each test names
    /// which of the four checks is the one doing the work, because no single
    /// check covers every input.
    #[test]
    fn given_the_historic_registry_shape_when_decoded_should_refuse() {
        // What the registry encoded as before the frame existed: a one element
        // array holding the endpoint map, and the bare empty array that read
        // as a registry with no endpoints. Both fail the decode itself, since
        // the frame is three elements and the first of them is not a map.
        assert!(matches!(
            decode_state_frame(&[0x91, 0x80]),
            Err(FrameError::Undecodable(_))
        ));
        assert!(matches!(
            decode_state_frame(&[0x90]),
            Err(FrameError::Undecodable(_))
        ));
    }

    #[test]
    fn given_a_state_blob_with_bytes_appended_when_decoded_should_refuse() {
        // Caught on the consumed length. The frame in front of the extra byte
        // is perfectly valid, and `rmp_serde::from_slice` would return it.
        let mut registry = EndpointRegistry::default();
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));
        let ConnectorState(mut bytes) = registry
            .to_connector_state(1)
            .expect("registry must serialize");
        assert!(decode_state_frame(&bytes).is_ok());

        bytes.push(0xc0);

        assert!(
            matches!(
                decode_state_frame(&bytes),
                Err(FrameError::TrailingBytes { .. })
            ),
            "a blob carrying more than one frame is not a state file this connector wrote"
        );
    }

    #[test]
    fn given_a_frame_whose_count_disagrees_with_its_map_when_decoded_should_refuse() {
        // Caught on the declared count, and nothing else can catch it. This is
        // the successor of the shape that used to survive: the frame is the
        // right arity, every field is present, and the whole blob is consumed.
        // Only the count says the map was replaced with a smaller one.
        let lying = StateFrame {
            version: STATE_VERSION,
            endpoint_count: 2,
            endpoints: BTreeMap::<EndpointId, Endpoint>::new(),
        };
        let bytes = rmp_serde::to_vec(&lying).expect("the shadow must serialize");

        assert_eq!(
            decode_state_frame(&bytes).expect_err("a lying count must be refused"),
            FrameError::CountMismatch {
                declared: 2,
                carried: 0
            }
        );
    }

    /// The property the framing is for, swept rather than sampled: no edit to
    /// a state blob may leave it decoding as a registry that has lost a
    /// tombstone, and no prefix of it may decode at all. A tombstone that
    /// vanishes, or that quietly moves to an id nobody revoked, puts a revoked
    /// endpoint back on the wire from TOML with its secret.
    ///
    /// The id check is what makes the second half hold. Without it 480 of
    /// these mutations produce a frame that is well formed, fully consumed and
    /// honest about its count, carrying the tombstone under an id one
    /// character away from the real one.
    ///
    /// What this still does not claim is authentication. Someone who can write
    /// the file can write a consistent one, and they could read every secret
    /// in it anyway. This catches corruption, not an author.
    #[test]
    fn given_a_mutated_state_blob_when_decoded_should_never_lose_a_tombstone() {
        let mut registry = EndpointRegistry::default();
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));
        assert!(registry.revoke(ENDPOINT_ONE, "compromised".to_string(), 42));
        let ConnectorState(bytes) = registry
            .to_connector_state(1)
            .expect("registry must serialize");
        let baseline = decode_state_frame(&bytes).expect("the baseline must decode");

        for index in 0..bytes.len() {
            for value in 0..=u8::MAX {
                if bytes[index] == value {
                    continue;
                }
                let mut mutated = bytes.clone();
                mutated[index] = value;
                let Ok(decoded) = decode_state_frame(&mutated) else {
                    continue;
                };
                assert_eq!(
                    decoded.len(),
                    baseline.len(),
                    "byte {index} set to {value:#04x} decoded with {} endpoint(s)",
                    decoded.len()
                );
                assert!(
                    decoded
                        .get(ENDPOINT_ONE)
                        .is_some_and(|endpoint| !endpoint.is_active()),
                    "byte {index} set to {value:#04x} decoded without the tombstone for the \
                     endpoint that was revoked"
                );
            }
        }
        for len in 0..bytes.len() {
            assert!(
                decode_state_frame(&bytes[..len]).is_err(),
                "a blob cut at {len} of {} bytes must not decode",
                bytes.len()
            );
        }
    }

    #[test]
    fn given_a_frame_whose_key_and_record_disagree_on_the_id_when_decoded_should_refuse() {
        // One edit cannot change both copies, so a retargeted tombstone always
        // leaves them disagreeing. Built through the map rather than by editing
        // bytes so it stays honest if the encoding changes.
        let mut endpoint = dynamic_endpoint(ENDPOINT_ONE);
        endpoint.state = EndpointState::Revoked {
            reason: "compromised".to_string(),
            revoked_at: 42,
        };
        let lying = StateFrame {
            version: STATE_VERSION,
            endpoint_count: 1,
            endpoints: BTreeMap::from([(endpoint_id(ENDPOINT_TWO), endpoint)]),
        };
        let bytes = rmp_serde::to_vec(&lying).expect("the shadow must serialize");

        assert!(
            matches!(
                decode_state_frame(&bytes),
                Err(FrameError::MisfiledEndpoint { .. })
            ),
            "a tombstone filed under an id its own record does not claim must not be served"
        );
    }

    #[test]
    fn given_a_frame_from_an_unknown_version_when_decoded_should_refuse() {
        let future = StateFrame {
            version: STATE_VERSION + 1,
            endpoint_count: 0,
            endpoints: BTreeMap::<EndpointId, Endpoint>::new(),
        };
        let bytes = rmp_serde::to_vec(&future).expect("the shadow must serialize");

        assert_eq!(
            decode_state_frame(&bytes).expect_err("an unknown version must be refused"),
            FrameError::Version(STATE_VERSION + 1),
            "a shape this connector does not know is refused, not guessed at"
        );
    }

    #[test]
    fn given_the_old_empty_registry_shape_when_restored_should_refuse_to_serve_static_config() {
        // The end the refusal exists for, driven by the shape that actually
        // used to fail open. `[0x90]` is an empty array, which the registry's
        // old single-field struct read as "no endpoints" through
        // `#[serde(default)]`: a valid decode carrying zero tombstones. The
        // TOML below still declares the endpoint the lost tombstone revoked,
        // so serving it is what puts a compromised endpoint back on the wire.
        //
        // Deliberately not a truncated blob. Truncation was always an EOF
        // error, before this framing as well as after, so a test built on one
        // passes at the parent commit and proves nothing.
        let restored = EndpointRegistry::restore(
            &[static_endpoint(ENDPOINT_ONE)],
            Some(ConnectorState(vec![0x90])),
            1,
        );

        assert!(
            restored.is_err(),
            "a registry that cannot be decoded must not be served as its static twin"
        );
    }

    #[test]
    fn given_a_tombstone_flipped_active_when_decoded_should_refuse() {
        // What the consumed-length check is actually for, and the only one of
        // the four that catches this. Rewriting the byte that opens the
        // `state` map turns `Revoked` into `Active`, which is a resurrected
        // endpoint serving with its secret, and leaves the bytes that used to
        // describe the revocation unread behind the frame.
        let mut registry = EndpointRegistry::default();
        assert!(registry.insert(dynamic_endpoint(ENDPOINT_ONE)));
        assert!(registry.revoke(ENDPOINT_ONE, "compromised".to_string(), 42));
        let ConnectorState(bytes) = registry
            .to_connector_state(1)
            .expect("registry must serialize");

        let flipped: Vec<Vec<u8>> = (0..bytes.len())
            .filter_map(|index| {
                let mut mutated = bytes.clone();
                mutated[index] = 0x00;
                let decoded = decode_state_frame(&mutated).ok()?;
                decoded
                    .get(ENDPOINT_ONE)
                    .is_some_and(Endpoint::is_active)
                    .then_some(mutated)
            })
            .collect();

        assert!(
            flipped.is_empty(),
            "{} mutation(s) decoded with the revoked endpoint active again",
            flipped.len()
        );
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
    fn given_an_expired_endpoint_when_restored_should_drop_its_stored_secret() {
        // `revoke()` clears the secret so a leaked credential is not written
        // back forever. An expired endpoint is refused the same way and just as
        // early, before `authorize()` runs, so keeping its secret persisted it
        // exactly as long.
        let mut persisted = EndpointRegistry::default();
        let mut expired = dynamic_endpoint(ENDPOINT_ONE);
        expired.expires_at = Some(1);
        assert!(persisted.insert(expired));

        let restored = EndpointRegistry::restore(&[], Some(registry_state(&persisted)), 1)
            .expect("restore must succeed");

        let endpoint = restored
            .endpoint(ENDPOINT_ONE)
            .expect("the endpoint must keep its slot");
        assert!(
            endpoint.auth_secret.is_none(),
            "an expired endpoint must not carry its secret back into the state file"
        );
        assert!(
            endpoint.is_active(),
            "clearing the secret must not reclaim the slot or make it a tombstone"
        );
    }

    #[test]
    fn given_an_unexpired_endpoint_when_restored_should_keep_its_stored_secret() {
        // The negative half: the clear is scoped to an expiry that has passed,
        // not to having one at all.
        let mut persisted = EndpointRegistry::default();
        let mut live = dynamic_endpoint(ENDPOINT_TWO);
        live.expires_at = Some(unix_now_seconds() + 3600);
        assert!(persisted.insert(live));

        let restored = EndpointRegistry::restore(&[], Some(registry_state(&persisted)), 1)
            .expect("restore must succeed");

        assert!(
            restored
                .endpoint(ENDPOINT_TWO)
                .expect("the endpoint must restore")
                .auth_secret
                .is_some(),
            "an endpoint whose expiry has not passed still needs its secret"
        );
    }

    #[test]
    fn given_a_cleared_expired_secret_when_restored_again_should_stay_admitted() {
        // The interaction the clear creates: the second restore sees an endpoint
        // advertising Bearer with no secret, which is the shape `MissingSecret`
        // exists to report. Expiry explains the refusal already, so it must not
        // be blamed on the credential, and the endpoint must still restore.
        let mut persisted = EndpointRegistry::default();
        let mut expired = dynamic_endpoint(ENDPOINT_ONE);
        expired.expires_at = Some(1);
        assert!(persisted.insert(expired));

        let once = EndpointRegistry::restore(&[], Some(registry_state(&persisted)), 1)
            .expect("the first restore must succeed");
        let twice = EndpointRegistry::restore(&[], Some(registry_state(&once)), 1)
            .expect("the second restore must succeed too");

        let endpoint = twice
            .endpoint(ENDPOINT_ONE)
            .expect("the endpoint must survive both restores");
        assert!(endpoint.auth_secret.is_none());
        assert!(
            endpoint.is_active(),
            "a cleared secret must not turn an expired endpoint into a tombstone"
        );
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
    fn given_static_endpoint_removed_from_config_when_restored_should_not_resurrect_it() {
        // Deleting the block from TOML is how an operator decommissions a
        // static endpoint. The state file still holds it, so restoring it
        // would keep serving a secret the operator believes is gone, and
        // rotate_secret would refuse it as static and point at TOML that no
        // longer describes it.
        let mut persisted = EndpointRegistry::default();
        assert!(persisted.insert(persisted_static_endpoint(ENDPOINT_ONE)));

        let restored = EndpointRegistry::restore(&[], Some(registry_state(&persisted)), 1)
            .expect("the registry must restore");

        assert!(
            restored.endpoint(ENDPOINT_ONE).is_none(),
            "a static endpoint deleted from TOML must not come back from the state file"
        );
    }

    #[test]
    fn given_revoked_static_endpoint_absent_from_config_when_restored_should_keep_the_tombstone() {
        // The other half of the rule above: dropping the entry entirely would
        // mean re-adding the id to TOML later silently undoes the revocation.
        let mut persisted = EndpointRegistry::default();
        assert!(persisted.insert(persisted_static_endpoint(ENDPOINT_ONE)));
        assert!(persisted.revoke(ENDPOINT_ONE, "compromised".to_string(), 42));

        let restored = EndpointRegistry::restore(&[], Some(registry_state(&persisted)), 1)
            .expect("the registry must restore");

        let endpoint = restored
            .endpoint(ENDPOINT_ONE)
            .expect("the tombstone must survive");
        assert!(
            !endpoint.is_active(),
            "a revoked static endpoint must stay revoked even once TOML stops declaring it"
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

    /// Fills the registry with revoked entries of the given origin, so a test
    /// can reach the ceiling without caring about ids.
    fn fill_with_tombstones(registry: &mut EndpointRegistry, count: usize, origin: EndpointOrigin) {
        for index in 0..count {
            let raw_id = format!("{index:032x}");
            let mut endpoint = Endpoint {
                origin,
                ..dynamic_endpoint(ENDPOINT_ONE)
            };
            endpoint.endpoint_id = endpoint_id(&raw_id);
            endpoint.revoke("filler".to_string(), index as u64);
            assert_eq!(
                registry.try_insert(endpoint),
                InsertOutcome::Inserted,
                "filling must not hit the ceiling"
            );
        }
    }

    #[test]
    fn given_ceiling_reached_with_dynamic_tombstones_when_inserted_should_reclaim_the_oldest() {
        let mut registry = EndpointRegistry::default();
        fill_with_tombstones(&mut registry, MAX_ENDPOINTS, EndpointOrigin::Dynamic);

        assert_eq!(
            registry.try_insert(dynamic_endpoint(ENDPOINT_ONE)),
            InsertOutcome::Inserted,
            "a revoked dynamic endpoint exists only here, so reclaiming it leaves its path answering 404 exactly as its tombstone did"
        );
        assert_eq!(
            registry.endpoints.len(),
            MAX_ENDPOINTS,
            "reclaiming must hold the registry at the ceiling, not grow past it"
        );
        assert!(
            registry.endpoint(&format!("{:032x}", 0)).is_none(),
            "the oldest revocation must be the one reclaimed"
        );
    }

    #[test]
    fn given_a_stored_endpoint_with_no_usable_secret_when_restored_should_still_serve() {
        // It advertises a second factor and has none, so `RouteTable::build`
        // derives no key and every request to it is refused. That is correct
        // and it used to be completely silent: the endpoint restored active,
        // the admin API reported it active, and nothing said why the sender
        // was getting 401s. `restore` asks the same rule registration does now,
        // and warns rather than refusing, because failing here takes the whole
        // instance down over one stored entry.
        let mut persisted = EndpointRegistry::default();
        let mut endpoint = dynamic_endpoint(ENDPOINT_ONE);
        endpoint.auth_type = EndpointAuthType::HmacSha256;
        endpoint.auth_secret = None;
        assert!(persisted.insert(endpoint));

        let restored = EndpointRegistry::restore(&[], Some(registry_state(&persisted)), 1)
            .expect("one unusable stored entry must not fail the instance");

        assert!(
            restored
                .endpoint(ENDPOINT_ONE)
                .expect("the endpoint must survive")
                .is_active(),
            "refusing here would take down every other endpoint on the instance"
        );
    }

    #[test]
    fn given_stored_values_past_the_ceilings_when_restored_should_still_serve() {
        // The ceilings are enforced where a caller can be told about them.
        // Refusing here would turn one oversized stored value into a hard
        // `open()` failure for the whole instance, and the operator cannot fix
        // it without hand-editing the state file. It warns instead.
        let mut persisted = EndpointRegistry::default();
        let mut endpoint = dynamic_endpoint(ENDPOINT_ONE);
        endpoint.auth_secret = Some(SecretString::from("x".repeat(MAX_AUTH_SECRET_LEN + 1)));
        endpoint.hmac_prefix = "x".repeat(MAX_HMAC_PREFIX_LEN + 1);
        assert!(persisted.insert(endpoint));

        let restored = EndpointRegistry::restore(&[], Some(registry_state(&persisted)), 1)
            .expect("an oversized stored value must not fail the whole instance");

        assert!(
            restored
                .endpoint(ENDPOINT_ONE)
                .expect("the endpoint must survive")
                .is_active(),
            "it is still the endpoint the operator registered"
        );
    }

    #[test]
    fn given_a_dynamic_tombstone_over_a_toml_id_when_restored_should_become_static() {
        // The tombstone that outranks a live TOML block has to be static, or
        // reclamation evicts it and the next restart serves that endpoint and
        // its secret again. The id was registered dynamically first, revoked,
        // and only then declared in TOML - which is how the persisted entry
        // ends up carrying `Dynamic` while TOML still claims the id.
        let mut persisted = EndpointRegistry::default();
        assert!(persisted.insert(dynamic_endpoint(ENDPOINT_ONE)));
        persisted.revoke(ENDPOINT_ONE, "compromised".to_string(), 42);

        let mut restored = EndpointRegistry::restore(
            &[static_endpoint(ENDPOINT_ONE)],
            Some(registry_state(&persisted)),
            1,
        )
        .expect("the registry must restore");

        let endpoint = restored.endpoint(ENDPOINT_ONE).expect("entry must exist");
        assert!(!endpoint.is_active(), "the revocation must survive");
        assert_eq!(
            endpoint.origin,
            EndpointOrigin::Static,
            "TOML declaring the id is what makes this tombstone static, whatever the state file recorded"
        );

        // The consequence, not just the field: at the ceiling this tombstone
        // must not be the one reclaimed to make room.
        // One short: the restored tombstone already occupies a slot, so this
        // brings the registry to exactly the ceiling.
        fill_with_tombstones(&mut restored, MAX_ENDPOINTS - 1, EndpointOrigin::Static);
        assert_eq!(restored.endpoints.len(), MAX_ENDPOINTS);
        assert_eq!(
            restored.try_insert(dynamic_endpoint(ENDPOINT_TWO)),
            InsertOutcome::Full,
            "reclaiming it would resurrect the TOML endpoint on the next restart"
        );
        assert!(
            !restored
                .endpoint(ENDPOINT_ONE)
                .expect("the tombstone must still be there")
                .is_active()
        );
    }

    #[test]
    fn given_ceiling_reached_with_only_static_tombstones_when_inserted_should_refuse() {
        // A static tombstone is what outranks a TOML entry that still declares
        // the endpoint, so reclaiming one would resurrect it on the next
        // restart. Refusing the registration is the cheaper failure.
        let mut registry = EndpointRegistry::default();
        fill_with_tombstones(&mut registry, MAX_ENDPOINTS, EndpointOrigin::Static);

        assert_eq!(
            registry.try_insert(dynamic_endpoint(ENDPOINT_ONE)),
            InsertOutcome::Full
        );
        assert_eq!(
            registry.endpoints.len(),
            MAX_ENDPOINTS,
            "a refusal must not leave the registry partially reclaimed, or it would arm a flush for a change the caller was told did not happen"
        );
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
