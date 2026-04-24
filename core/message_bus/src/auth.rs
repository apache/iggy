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
pub const ENVELOPE_LEN: usize = 57;

/// Acceptance window around `auth_timestamp`, in nanoseconds. 30 s.
pub const TIMESTAMP_WINDOW_NS: u128 = 30_000_000_000;

/// Default nonce-dedup LRU capacity on the acceptor.
pub const NONCE_LRU_CAPACITY: usize = 256;

const LABEL: &[u8; 16] = b"iggy-bus-auth-v1";

const KIND_OFFSET: usize = 56;
const TAG_RANGE: std::ops::Range<usize> = 0..32;
const TIMESTAMP_RANGE: std::ops::Range<usize> = 32..40;
const NONCE_RANGE: std::ops::Range<usize> = 40..56;
const RESERVED_RANGE: std::ops::Range<usize> = ENVELOPE_LEN..128;

/// Envelope version tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthKind {
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

/// Fully reconstructed challenge input. All fields come from the header
/// (`GenericHeader.cluster`, `GenericHeader.replica`, `GenericHeader.release`)
/// plus the envelope timestamp and nonce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthChallenge {
    pub cluster: u128,
    pub peer_id: u128,
    pub timestamp_ns: u64,
    pub release: u32,
    pub nonce: u128,
}

impl AuthChallenge {
    /// Serialize the challenge into a fixed-size buffer. Domain-separated by
    /// a constant label so the same secret cannot be misused to forge a
    /// different protocol's MAC.
    fn encode(&self) -> [u8; 16 + 16 + 16 + 8 + 4 + 16] {
        let mut out = [0u8; 76];
        out[0..16].copy_from_slice(LABEL);
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthTag(pub blake3::Hash);

impl AuthTag {
    const fn as_bytes(&self) -> &[u8; 32] {
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
/// Not implemented as `From<AuthError> for IggyError` because the impl
/// would leak into every crate transitively linking `message_bus`, adding
/// an extra `impl From<_> for IggyError` candidate to the orphan-rule set
/// and tripping type inference (`?` ambiguity) in downstream crates.
#[must_use]
pub const fn to_iggy_error(e: AuthError) -> IggyError {
    match e {
        AuthError::UnsupportedKind | AuthError::ReservedNonzero => IggyError::InvalidCommand,
        AuthError::TagMismatch
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

/// `TokenSource` that holds a single 32-byte cluster-wide secret. All
/// replicas share one secret on the replica plane; the integration-test
/// harness instantiates this with a fixed zero-byte secret.
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

/// Fixed-capacity ring-buffer nonce dedup.
///
/// Eviction is O(1) via a round-robin cursor; lookup is O(N) over
/// `NONCE_LRU_CAPACITY` entries, which is a 256-wide linear scan on the
/// hot path. Memory footprint is deterministic and bounded regardless of
/// peer behaviour.
#[derive(Debug)]
pub struct NonceRing {
    slots: Vec<u128>,
    cursor: usize,
    capacity: usize,
}

impl NonceRing {
    #[must_use]
    pub const fn new(capacity: usize) -> Self {
        Self {
            slots: Vec::new(),
            cursor: 0,
            capacity,
        }
    }

    /// Returns `true` if `nonce` was not present (and has now been
    /// recorded); `false` if it was already seen.
    pub fn insert(&mut self, nonce: u128) -> bool {
        if self.slots.contains(&nonce) {
            return false;
        }
        if self.slots.len() < self.capacity {
            self.slots.push(nonce);
        } else {
            self.slots[self.cursor] = nonce;
            self.cursor = (self.cursor + 1) % self.capacity;
        }
        true
    }
}

impl Default for NonceRing {
    fn default() -> Self {
        Self::new(NONCE_LRU_CAPACITY)
    }
}

/// Encode an auth envelope into `reserved_command`.
///
/// Overwrites the first `ENVELOPE_LEN` bytes and zero-fills the trailing
/// reserved range. Source of the tag is `token_source`; `timestamp_ns`
/// and `nonce` are caller-controlled so the same routine can be used for
/// deterministic tests (fake clock, fixed nonce) and production (system
/// clock, CSPRNG).
pub fn encode_envelope(
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
pub struct DecodedEnvelope {
    pub tag: AuthTag,
    pub timestamp_ns: u64,
    pub nonce: u128,
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
pub fn decode_envelope(reserved: &[u8; 128]) -> Result<DecodedEnvelope, AuthError> {
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

/// Full credential check: timestamp window -> nonce dedup -> tag.
///
/// # Errors
///
/// Returns the first `AuthError` triggered by the check sequence.
pub fn verify_envelope(
    token_source: &dyn TokenSource,
    challenge: &AuthChallenge,
    decoded: &DecodedEnvelope,
    now_ns: u128,
    nonces: &mut NonceRing,
) -> Result<(), AuthError> {
    let ts_u128 = u128::from(decoded.timestamp_ns);
    let delta = now_ns.abs_diff(ts_u128);
    if delta > TIMESTAMP_WINDOW_NS {
        return Err(AuthError::TimestampOutOfWindow);
    }
    if !nonces.insert(decoded.nonce) {
        return Err(AuthError::NonceReplay);
    }
    token_source.verify(challenge, &decoded.tag)
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
        }
    }

    #[test]
    fn roundtrip_ok() {
        let source = StaticSharedSecret::new([1u8; SECRET_SIZE]);
        let challenge = sample_challenge();
        let mut reserved = [0u8; 128];
        encode_envelope(&mut reserved, &source, &challenge);

        let decoded = decode_envelope(&reserved).expect("decode");
        let mut ring = NonceRing::new(32);
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
        let mut ring = NonceRing::new(32);
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
        let mut ring = NonceRing::new(32);
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
        let mut ring = NonceRing::new(32);
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
        let mut ring = NonceRing::new(32);
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
    fn nonce_ring_fills_and_evicts_fifo() {
        let mut ring = NonceRing::new(3);
        assert!(ring.insert(1));
        assert!(ring.insert(2));
        assert!(ring.insert(3));
        assert!(!ring.insert(1));
        assert!(ring.insert(4));
        // Nonce 1 evicted; reinserting it should now succeed again.
        assert!(ring.insert(1));
    }
}
