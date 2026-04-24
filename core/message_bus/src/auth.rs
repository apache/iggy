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

//! Bus-layer authenticated handshake (IGGY-112).
//!
//! The replica-plane handshake (`replica_listener::handshake`) and the
//! forthcoming SDK-client plane (Phase 2+) share one envelope, carried in
//! `GenericHeader.reserved_command[0..57]`:
//!
//! ```text
//! [  0 .. 32) auth_tag        BLAKE3 keyed_hash(secret, challenge) full output
//! [ 32 .. 40) auth_timestamp  sender ns since UNIX_EPOCH, little-endian u64
//! [ 40 .. 56) auth_nonce      random u128, little-endian
//! [ 56 .. 57) auth_kind       envelope version tag; 1 = Blake3V1
//! [ 57 ..128) reserved        zero on the wire; zero-checked on receive
//! ```
//!
//! Kept out of `GenericHeader` itself so the 256 B wire layout is
//! preserved (invariant **I3**). Encode / decode operate on a
//! `&mut [u8; 128]` / `&[u8; 128]` view of `GenericHeader.reserved_command`.

use iggy_common::IggyError;
use thiserror::Error;

/// BLAKE3 `keyed_hash` key size, in bytes.
pub const SECRET_SIZE: usize = 32;

/// Byte range of the envelope within `GenericHeader.reserved_command`.
pub(crate) const ENVELOPE_LEN: usize = 57;

/// Acceptance window around `auth_timestamp`, in nanoseconds. 30 s.
pub(crate) const TIMESTAMP_WINDOW_NS: u128 = 30_000_000_000;

/// Per-peer nonce-dedup ring capacity on the acceptor.
///
/// 8 slots × 5-replica clusters × 16 B = 640 B inline; survives the
/// 30 s acceptance window even at the worst-case dial cadence
/// (`reconnect_period = 5 s`). Per-peer (rather than a single global
/// ring) means a flapping or hostile peer cannot evict honest peers'
/// nonce slots, so the verify-order guard in [`verify_envelope`] holds
/// up against an attacker who can reach the listener but lacks the
/// secret. See plan F2.
#[doc(hidden)]
pub const PER_PEER_NONCE_CAPACITY: usize = 8;

/// Plane-tagged 16-byte label prefixed onto every challenge before the MAC.
///
/// Domain separation: same cluster secret produces distinct MACs across
/// the replica plane and the (Phase 2) SDK-client plane, so a captured
/// replica handshake cannot be replayed onto the client plane and vice
/// versa even when both planes share a [`StaticSharedSecret`]. Bump the
/// trailing version digits when the challenge layout itself changes;
/// bump [`AuthKind`] when the MAC algorithm changes.
pub(crate) const LABEL_REPLICA: &[u8; 16] = b"iggy-bus-rep-v01";

/// Plane label for the SDK-client plane (reserved for Phase 2; not
/// exercised on the wire today). Defined now so the cross-plane MAC
/// guard test in this module can exercise both labels and the test
/// catches a regression before Phase 2 wiring lands.
#[allow(dead_code)]
pub(crate) const LABEL_CLIENT: &[u8; 16] = b"iggy-bus-cli-v01";

const KIND_OFFSET: usize = 56;
const TAG_RANGE: std::ops::Range<usize> = 0..32;
const TIMESTAMP_RANGE: std::ops::Range<usize> = 32..40;
const NONCE_RANGE: std::ops::Range<usize> = 40..56;
const RESERVED_RANGE: std::ops::Range<usize> = ENVELOPE_LEN..128;

/// Envelope version tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum AuthKind {
    Blake3V1 = 1,
}

impl AuthKind {
    const fn from_byte(b: u8) -> Result<Self, AuthError> {
        match b {
            1 => Ok(Self::Blake3V1),
            _ => Err(AuthError::UnsupportedKind),
        }
    }
}

/// Fully reconstructed challenge input.
///
/// All fields come from the header (`GenericHeader.cluster`,
/// `GenericHeader.replica`, `GenericHeader.release`) plus the envelope
/// timestamp and nonce. `label` is the plane-tagged domain-separation
/// prefix; pick [`LABEL_REPLICA`] for the consensus plane and
/// [`LABEL_CLIENT`] for the SDK-client plane (Phase 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthChallenge {
    pub cluster: u128,
    pub peer_id: u128,
    pub timestamp_ns: u64,
    pub release: u32,
    pub nonce: u128,
    pub label: &'static [u8; 16],
}

impl AuthChallenge {
    /// Serialize the challenge into a fixed-size buffer. The leading 16 B
    /// are the plane-tagged label, so the same secret cannot be misused
    /// to forge a different plane's MAC even with otherwise-identical
    /// inputs.
    fn encode(&self) -> [u8; 16 + 16 + 16 + 8 + 4 + 16] {
        let mut out = [0u8; 76];
        out[0..16].copy_from_slice(self.label);
        out[16..32].copy_from_slice(&self.cluster.to_le_bytes());
        out[32..48].copy_from_slice(&self.peer_id.to_le_bytes());
        out[48..56].copy_from_slice(&self.timestamp_ns.to_le_bytes());
        out[56..60].copy_from_slice(&self.release.to_le_bytes());
        out[60..76].copy_from_slice(&self.nonce.to_le_bytes());
        out
    }
}

/// MAC over an `AuthChallenge`. Wraps `blake3::Hash` for its
/// constant-time `PartialEq` implementation.
///
/// The inner `blake3::Hash` is `pub(crate)` so downstream callers cannot
/// reach in for byte-level `==` comparisons that would bypass the
/// constant-time `PartialEq` on the wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthTag(pub(crate) blake3::Hash);

impl AuthTag {
    pub(crate) const fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

/// Credential-layer errors surfaced by the verify path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AuthError {
    #[error("timestamp outside acceptance window")]
    TimestampOutOfWindow,
    #[error("nonce replay")]
    NonceReplay,
    #[error("tag mismatch")]
    TagMismatch,
    #[error("unknown peer")]
    UnknownPeer,
    #[error("unsupported envelope version")]
    UnsupportedKind,
    #[error("reserved bytes nonzero")]
    ReservedNonzero,
}

/// Map a credential-layer `AuthError` to the public `IggyError` surface.
///
/// `UnsupportedKind` is routed to `Unauthenticated` rather than
/// `InvalidCommand` so that pre-auth peers in a mixed-version cluster
/// (whose all-zero `reserved_command` decodes to `AuthKind::from_byte(0)
/// -> UnsupportedKind`) surface in the operator's auth-bucket triage,
/// matching the actual deployment failure mode. `ReservedNonzero` keeps
/// the `InvalidCommand` mapping because the peer DID stamp a recognized
/// kind byte and then violated the envelope's reserved-zero contract:
/// that is a genuine protocol bug, not an auth gap.
///
/// Not implemented as `From<AuthError> for IggyError` because the impl
/// would leak into every crate transitively linking `message_bus`, adding
/// an extra `impl From<_> for IggyError` candidate to the orphan-rule set
/// and tripping type inference (`?` ambiguity) in downstream crates.
#[must_use]
pub(crate) const fn to_iggy_error(e: AuthError) -> IggyError {
    match e {
        AuthError::ReservedNonzero => IggyError::InvalidCommand,
        AuthError::UnsupportedKind
        | AuthError::TagMismatch
        | AuthError::UnknownPeer
        | AuthError::TimestampOutOfWindow
        | AuthError::NonceReplay => IggyError::Unauthenticated,
    }
}

/// Signer + verifier of auth envelopes. The bus runs on a single-threaded
/// compio runtime, so no `Send + Sync` bounds.
pub trait TokenSource {
    fn sign(&self, challenge: &AuthChallenge) -> AuthTag;
    /// # Errors
    /// Returns `AuthError::TagMismatch` when `tag` does not match the
    /// MAC produced for `challenge`; may return other variants in
    /// token-source impls that perform additional checks.
    fn verify(&self, challenge: &AuthChallenge, tag: &AuthTag) -> Result<(), AuthError>;
}

/// `TokenSource` that holds a single 32-byte cluster-wide secret.
///
/// All replicas share one secret on the replica plane; the integration-test
/// harness instantiates this with a fixed zero-byte secret, and production
/// bootstraps resolve one via [`crate::auth_config`].
pub struct StaticSharedSecret {
    secret: [u8; SECRET_SIZE],
}

impl StaticSharedSecret {
    #[must_use]
    pub const fn new(secret: [u8; SECRET_SIZE]) -> Self {
        Self { secret }
    }

    #[must_use]
    pub const fn zero() -> Self {
        Self::new([0u8; SECRET_SIZE])
    }
}

impl TokenSource for StaticSharedSecret {
    fn sign(&self, challenge: &AuthChallenge) -> AuthTag {
        let input = challenge.encode();
        AuthTag(blake3::keyed_hash(&self.secret, &input))
    }

    fn verify(&self, challenge: &AuthChallenge, tag: &AuthTag) -> Result<(), AuthError> {
        let expected = self.sign(challenge);
        if expected.0 == tag.0 {
            Ok(())
        } else {
            Err(AuthError::TagMismatch)
        }
    }
}

/// Fixed-capacity ring-buffer nonce dedup. Inline `[u128; CAP]` storage —
/// no heap allocation, deterministic memory footprint.
///
/// Eviction is O(1) via a round-robin cursor; lookup is O(N) over `CAP`
/// entries (linear scan, but `CAP * 16 B` fits a single cache line for
/// `CAP <= 4` and stays L1-resident through `CAP = 256`). Memory bound
/// is `CAP * 16 B + 2 * sizeof(usize)` per ring; per-peer instances cap
/// the cluster-wide cost.
#[doc(hidden)]
#[derive(Debug)]
pub struct NonceRing<const CAP: usize> {
    slots: [u128; CAP],
    len: usize,
    cursor: usize,
}

impl<const CAP: usize> NonceRing<CAP> {
    #[must_use]
    #[doc(hidden)]
    pub const fn new() -> Self {
        Self {
            slots: [0u128; CAP],
            len: 0,
            cursor: 0,
        }
    }

    /// Returns `true` if `nonce` was not present (and has now been
    /// recorded); `false` if it was already seen.
    ///
    /// `slots[..self.len]` is the only range scanned for membership, so
    /// the `[0; CAP]` initial state never collides with a real `0`
    /// nonce: until the ring fills, untouched slots are out-of-range.
    #[doc(hidden)]
    pub fn insert(&mut self, nonce: u128) -> bool {
        if self.slots[..self.len].contains(&nonce) {
            return false;
        }
        if self.len < CAP {
            self.slots[self.len] = nonce;
            self.len += 1;
        } else {
            self.slots[self.cursor] = nonce;
            self.cursor = (self.cursor + 1) % CAP;
        }
        true
    }
}

impl<const CAP: usize> Default for NonceRing<CAP> {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-peer nonce ring with the production [`PER_PEER_NONCE_CAPACITY`].
#[doc(hidden)]
pub type ReplicaNonceRing = NonceRing<PER_PEER_NONCE_CAPACITY>;

/// Encode an auth envelope into `reserved_command`.
///
/// Overwrites the first `ENVELOPE_LEN` bytes and zero-fills the trailing
/// reserved range. Source of the tag is `token_source`; `timestamp_ns`
/// and `nonce` are caller-controlled so the same routine can be used for
/// deterministic tests (fake clock, fixed nonce) and production (system
/// clock, CSPRNG).
pub(crate) fn encode_envelope(
    reserved: &mut [u8; 128],
    token_source: &dyn TokenSource,
    challenge: &AuthChallenge,
) {
    let tag = token_source.sign(challenge);
    reserved[TAG_RANGE].copy_from_slice(tag.as_bytes());
    reserved[TIMESTAMP_RANGE].copy_from_slice(&challenge.timestamp_ns.to_le_bytes());
    reserved[NONCE_RANGE].copy_from_slice(&challenge.nonce.to_le_bytes());
    reserved[KIND_OFFSET] = AuthKind::Blake3V1 as u8;
    for b in &mut reserved[RESERVED_RANGE] {
        *b = 0;
    }
}

/// Parsed view of an envelope's raw bytes. No cryptographic checks.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DecodedEnvelope {
    pub(crate) tag: AuthTag,
    pub(crate) timestamp_ns: u64,
    pub(crate) nonce: u128,
}

/// Decode an envelope and reject malformed layout.
///
/// Rejects unknown envelope kinds and nonzero reserved bytes. Does not
/// check the tag; caller passes the returned `DecodedEnvelope` to
/// [`verify_envelope`] with the full challenge context.
///
/// # Errors
///
/// Returns `AuthError::UnsupportedKind` for unknown envelope versions or
/// `AuthError::ReservedNonzero` for non-zero reserved padding.
pub(crate) fn decode_envelope(reserved: &[u8; 128]) -> Result<DecodedEnvelope, AuthError> {
    AuthKind::from_byte(reserved[KIND_OFFSET])?;
    if reserved[RESERVED_RANGE].iter().any(|&b| b != 0) {
        return Err(AuthError::ReservedNonzero);
    }
    let mut tag_bytes = [0u8; 32];
    tag_bytes.copy_from_slice(&reserved[TAG_RANGE]);
    let mut ts_bytes = [0u8; 8];
    ts_bytes.copy_from_slice(&reserved[TIMESTAMP_RANGE]);
    let mut nonce_bytes = [0u8; 16];
    nonce_bytes.copy_from_slice(&reserved[NONCE_RANGE]);
    Ok(DecodedEnvelope {
        tag: AuthTag(blake3::Hash::from_bytes(tag_bytes)),
        timestamp_ns: u64::from_le_bytes(ts_bytes),
        nonce: u128::from_le_bytes(nonce_bytes),
    })
}

/// Full credential check: timestamp window -> tag verify -> nonce dedup.
///
/// **Order is load-bearing.** A tag-failed envelope must NOT consume a
/// nonce-ring slot, otherwise an off-secret attacker with TCP reachability
/// to the listener could flush the ring with forged-tag Pings (valid
/// timestamps + random nonces) and open a replay window for any captured
/// legit Ping inside the remaining timestamp slack.
///
/// # Errors
///
/// Returns the first `AuthError` triggered by the check sequence.
pub(crate) fn verify_envelope<const CAP: usize>(
    token_source: &dyn TokenSource,
    challenge: &AuthChallenge,
    decoded: &DecodedEnvelope,
    now_ns: u128,
    nonces: &mut NonceRing<CAP>,
) -> Result<(), AuthError> {
    let ts_u128 = u128::from(decoded.timestamp_ns);
    let delta = now_ns.abs_diff(ts_u128);
    if delta > TIMESTAMP_WINDOW_NS {
        return Err(AuthError::TimestampOutOfWindow);
    }
    token_source.verify(challenge, &decoded.tag)?;
    if !nonces.insert(decoded.nonce) {
        return Err(AuthError::NonceReplay);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_challenge() -> AuthChallenge {
        AuthChallenge {
            cluster: 0xdead_beef_1234_5678_9abc_def0_1122_3344,
            peer_id: 7,
            timestamp_ns: 1_700_000_000_000_000_000,
            release: 1,
            nonce: 0x11_2233_4455_6677_8899_aabb_ccdd_eeff,
            label: LABEL_REPLICA,
        }
    }

    #[test]
    fn roundtrip_ok() {
        let source = StaticSharedSecret::new([1u8; SECRET_SIZE]);
        let challenge = sample_challenge();
        let mut reserved = [0u8; 128];
        encode_envelope(&mut reserved, &source, &challenge);

        let decoded = decode_envelope(&reserved).expect("decode");
        let mut ring = NonceRing::<32>::new();
        verify_envelope(
            &source,
            &challenge,
            &decoded,
            u128::from(challenge.timestamp_ns),
            &mut ring,
        )
        .expect("verify");
    }

    #[test]
    fn wrong_secret_rejected() {
        let signer = StaticSharedSecret::new([1u8; SECRET_SIZE]);
        let verifier = StaticSharedSecret::new([2u8; SECRET_SIZE]);
        let challenge = sample_challenge();
        let mut reserved = [0u8; 128];
        encode_envelope(&mut reserved, &signer, &challenge);

        let decoded = decode_envelope(&reserved).unwrap();
        let mut ring = NonceRing::<32>::new();
        let err = verify_envelope(
            &verifier,
            &challenge,
            &decoded,
            u128::from(challenge.timestamp_ns),
            &mut ring,
        )
        .unwrap_err();
        assert_eq!(err, AuthError::TagMismatch);
    }

    #[test]
    fn replay_rejected() {
        let source = StaticSharedSecret::zero();
        let challenge = sample_challenge();
        let mut reserved = [0u8; 128];
        encode_envelope(&mut reserved, &source, &challenge);

        let decoded = decode_envelope(&reserved).unwrap();
        let mut ring = NonceRing::<32>::new();
        let now = u128::from(challenge.timestamp_ns);
        verify_envelope(&source, &challenge, &decoded, now, &mut ring).unwrap();
        let err = verify_envelope(&source, &challenge, &decoded, now, &mut ring).unwrap_err();
        assert_eq!(err, AuthError::NonceReplay);
    }

    #[test]
    fn stale_timestamp_rejected() {
        let source = StaticSharedSecret::zero();
        let challenge = sample_challenge();
        let mut reserved = [0u8; 128];
        encode_envelope(&mut reserved, &source, &challenge);

        let decoded = decode_envelope(&reserved).unwrap();
        let mut ring = NonceRing::<32>::new();
        let now = u128::from(challenge.timestamp_ns) + TIMESTAMP_WINDOW_NS + 1;
        let err = verify_envelope(&source, &challenge, &decoded, now, &mut ring).unwrap_err();
        assert_eq!(err, AuthError::TimestampOutOfWindow);
    }

    #[test]
    fn future_timestamp_rejected() {
        let source = StaticSharedSecret::zero();
        let challenge = sample_challenge();
        let mut reserved = [0u8; 128];
        encode_envelope(&mut reserved, &source, &challenge);

        let decoded = decode_envelope(&reserved).unwrap();
        let mut ring = NonceRing::<32>::new();
        let now = u128::from(challenge.timestamp_ns).saturating_sub(TIMESTAMP_WINDOW_NS + 1);
        let err = verify_envelope(&source, &challenge, &decoded, now, &mut ring).unwrap_err();
        assert_eq!(err, AuthError::TimestampOutOfWindow);
    }

    #[test]
    fn unsupported_kind_rejected() {
        let source = StaticSharedSecret::zero();
        let challenge = sample_challenge();
        let mut reserved = [0u8; 128];
        encode_envelope(&mut reserved, &source, &challenge);
        reserved[KIND_OFFSET] = 99;
        let err = decode_envelope(&reserved).unwrap_err();
        assert_eq!(err, AuthError::UnsupportedKind);
    }

    #[test]
    fn reserved_nonzero_rejected() {
        let source = StaticSharedSecret::zero();
        let challenge = sample_challenge();
        let mut reserved = [0u8; 128];
        encode_envelope(&mut reserved, &source, &challenge);
        reserved[100] = 0xff;
        let err = decode_envelope(&reserved).unwrap_err();
        assert_eq!(err, AuthError::ReservedNonzero);
    }

    #[test]
    fn forged_tag_does_not_consume_ring_slot() {
        // Sender signs with secret A; verifier holds secret B. Verifier's
        // tag check fails. The ring MUST NOT have recorded the nonce -
        // otherwise an attacker without the secret could flood forged
        // Pings to evict honest entries (auth.rs verify_envelope ordering
        // bug; review F1 critical).
        let signer = StaticSharedSecret::new([1u8; SECRET_SIZE]);
        let verifier = StaticSharedSecret::new([2u8; SECRET_SIZE]);
        let challenge = sample_challenge();
        let mut reserved = [0u8; 128];
        encode_envelope(&mut reserved, &signer, &challenge);

        let decoded = decode_envelope(&reserved).unwrap();
        let mut ring = NonceRing::<4>::new();
        let err = verify_envelope(
            &verifier,
            &challenge,
            &decoded,
            u128::from(challenge.timestamp_ns),
            &mut ring,
        )
        .unwrap_err();
        assert_eq!(err, AuthError::TagMismatch);

        // Same nonce now signed correctly with secret B must succeed -
        // proves the failed-tag attempt did NOT burn the ring slot.
        let mut reserved_legit = [0u8; 128];
        encode_envelope(&mut reserved_legit, &verifier, &challenge);
        let decoded_legit = decode_envelope(&reserved_legit).unwrap();
        verify_envelope(
            &verifier,
            &challenge,
            &decoded_legit,
            u128::from(challenge.timestamp_ns),
            &mut ring,
        )
        .expect("legit verify must succeed; nonce slot must have been free");
    }

    #[test]
    fn cross_plane_mac_does_not_verify() {
        // Same secret, same numeric inputs - only label differs. Replica-
        // plane signer must NOT pass client-plane verification (and vice
        // versa). Otherwise a captured replica handshake could be replayed
        // onto the client plane once Phase 2 ships. Review F4.
        let secret = StaticSharedSecret::new([3u8; SECRET_SIZE]);
        let replica_challenge = sample_challenge();
        let mut client_challenge = replica_challenge;
        client_challenge.label = LABEL_CLIENT;

        let mut replica_envelope = [0u8; 128];
        encode_envelope(&mut replica_envelope, &secret, &replica_challenge);
        let decoded = decode_envelope(&replica_envelope).unwrap();
        let mut ring = NonceRing::<4>::new();
        let err = verify_envelope(
            &secret,
            &client_challenge,
            &decoded,
            u128::from(replica_challenge.timestamp_ns),
            &mut ring,
        )
        .unwrap_err();
        assert_eq!(err, AuthError::TagMismatch);
    }

    #[test]
    fn unsupported_kind_maps_to_unauthenticated() {
        // Mixed-version cluster: pre-auth peer sends zeroed
        // reserved_command -> AuthKind::from_byte(0) -> UnsupportedKind.
        // Operator log triage must hit the auth bucket, not the
        // protocol-violation bucket. Review F3.
        assert_eq!(
            to_iggy_error(AuthError::UnsupportedKind),
            IggyError::Unauthenticated
        );
        // ReservedNonzero stays InvalidCommand: peer DID stamp a known
        // kind, then violated the envelope's reserved-zero contract.
        assert_eq!(
            to_iggy_error(AuthError::ReservedNonzero),
            IggyError::InvalidCommand
        );
    }

    #[test]
    fn nonce_ring_fills_and_evicts_fifo() {
        let mut ring = NonceRing::<3>::new();
        assert!(ring.insert(1));
        assert!(ring.insert(2));
        assert!(ring.insert(3));
        assert!(!ring.insert(1));
        assert!(ring.insert(4));
        // Nonce 1 evicted; reinserting it should now succeed again.
        assert!(ring.insert(1));
    }

    #[test]
    fn nonce_ring_zero_initial_does_not_false_positive() {
        // [0; CAP] initial state must NOT match a real `0` nonce until
        // the slot has been written. Edge case for the const-generic
        // refactor (review F2).
        let mut ring = NonceRing::<4>::new();
        assert!(ring.insert(0));
        assert!(!ring.insert(0));
        assert!(ring.insert(1));
        assert!(ring.insert(2));
    }

    #[test]
    fn nonce_ring_default_is_empty() {
        let mut ring = NonceRing::<4>::default();
        assert!(ring.insert(42));
    }
}
