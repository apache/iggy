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

//! Replica-to-replica authentication primitives.
//!
//! All crypto for the PSK + BLAKE3 keyed-MAC handshake lives here so the
//! hot transport files (`listener`, `connector`) carry no cipher logic.
//!
//! The handshake is a 3-message mutual challenge-response riding the
//! already-zeroed `reserved_command` bytes of the 256-byte `GenericHeader`.
//! Each message has its own `Command2` discriminant (no Ping/Pong reuse):
//!
//! 1. `ReplicaHello`     dialer -> acceptor: `nonce_d` (no MAC; the dialer has
//!    no acceptor nonce yet);
//! 2. `ReplicaChallenge` acceptor -> dialer: a `status` byte, plus `nonce_a` +
//!    `mac_a` when the status is `Ok` (a nonzero status is a reject, no nonce/MAC);
//! 3. `ReplicaFinish`    dialer -> acceptor: `mac_d` (dialer direction).
//!
//! Each MAC is `keyed_hash(key, DOMAIN_TAG || cluster_id || dialer_id ||
//! acceptor_id || nonce_d || nonce_a || mode || tls_exporter || dir)`.
//! Binding both nonces, the ordered peer pair, and a direction byte
//! defeats replay and reflection. The MAC key is a subkey derived from
//! the configured shared secret, so the raw secret never reaches the
//! wire transcript.
//!
//! `mode || tls_exporter` is the channel binding ([`ChannelBinding`]).
//! On a TLS link both ends derive the exporter from the session's master
//! secret, so a MAC only verifies inside the SAME TLS session: a relay
//! MITM terminating two separate TLS sessions (possible in the
//! accept-any `cluster.tls.self_signed` mode) produces two different
//! exporter values and both MACs fail. The mode byte domain-separates
//! plaintext transcripts (exporter zeroed) from TLS ones.

use blake3::Hash;
use iggy_binary_protocol::RESERVED_COMMAND_LEN;
use iggy_common::IggyError;
use ring::rand::{SecureRandom, SystemRandom};
use zeroize::Zeroizing;

/// Length of each handshake nonce, in bytes.
pub const NONCE_LEN: usize = 32;
/// Length of each handshake MAC, in bytes.
pub const MAC_LEN: usize = 32;
/// Length of the TLS exporter value bound into the MAC transcript, in bytes.
pub const EXPORTER_LEN: usize = 32;
/// rustls `export_keying_material` label for the replica channel binding.
pub const EXPORTER_LABEL: &[u8] = b"iggy replica psk binding";
/// Offset of the `status` byte in a `ReplicaChallenge` frame's `reserved_command`.
///
/// Past the nonce (`[0..32]`) and MAC (`[32..64]`) regions a successful
/// challenge uses. Byte `0` is [`HandshakeStatus::Ok`]; any nonzero value is a
/// reject reason, so a zeroed (`Ok`) challenge carries `nonce_a` + `mac_a` while
/// a reject carries the reason here and leaves the nonce/MAC regions zero.
pub const STATUS_OFFSET: usize = NONCE_LEN + MAC_LEN;

/// Domain separation tag mixed into every MAC transcript.
const DOMAIN_TAG: &[u8] = b"apache-iggy replica-auth v1";
/// Context for deriving the MAC subkey from the configured secret.
const KEY_CONTEXT: &str = "apache-iggy replica-auth v1 psk->mac-key";
/// Direction byte for a MAC produced by the dialer.
const DIR_DIALER: u8 = 1;
/// Direction byte for a MAC produced by the acceptor.
const DIR_ACCEPTOR: u8 = 2;

/// Stable domain-separation cluster id derived from the cluster name.
///
/// Byte-identical across the roster because every node shares the same
/// `cluster.name`. It is a domain-separation tag, not the security gate
/// (the PSK is).
#[must_use]
pub fn cluster_domain_id(name: &str) -> u128 {
    let digest = blake3::hash(name.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

/// Generate a fresh CSPRNG nonce.
///
/// # Errors
///
/// Returns [`IggyError::IoError`] if the system CSPRNG fails (a catastrophic
/// condition). The caller drops the handshake: the dialer's periodic sweep
/// retries it, the acceptor fails the inbound and the peer sees EOF.
pub fn random_nonce() -> Result<[u8; NONCE_LEN], IggyError> {
    let mut nonce = [0u8; NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| IggyError::IoError("replica-auth CSPRNG failure".to_owned()))?;
    Ok(nonce)
}

/// Extract the nonce from `reserved_command[0..32]` of a handshake frame.
#[must_use]
pub fn read_nonce(reserved_command: &[u8; RESERVED_COMMAND_LEN]) -> [u8; NONCE_LEN] {
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&reserved_command[..NONCE_LEN]);
    nonce
}

/// Extract the MAC from `reserved_command[32..64]` of a handshake frame.
#[must_use]
pub fn read_mac(reserved_command: &[u8; RESERVED_COMMAND_LEN]) -> [u8; MAC_LEN] {
    let mut mac = [0u8; MAC_LEN];
    mac.copy_from_slice(&reserved_command[NONCE_LEN..NONCE_LEN + MAC_LEN]);
    mac
}

/// Whether a frame's `reserved_command` carries a handshake nonce (i.e. the
/// peer speaks the authenticated protocol) versus an all-zero legacy frame.
#[must_use]
pub fn has_nonce(reserved_command: &[u8; RESERVED_COMMAND_LEN]) -> bool {
    reserved_command[..NONCE_LEN] != [0u8; NONCE_LEN]
}

/// Outcome of a replica handshake, carried as the `status` byte of a
/// `ReplicaChallenge` frame at [`STATUS_OFFSET`].
///
/// [`Self::Ok`] (byte `0`) means the challenge carries `nonce_a` + `mac_a`. A
/// nonzero reject reason is sent back only to an authenticated, still-waiting
/// dialer so the joining node learns the cause from its own logs instead of
/// seeing a bare connection close. [`Self::AuthRequired`] and
/// [`Self::MacMismatch`] are log-only labels: their peer is not reading a
/// response, so no frame is emitted for them and those bytes never reach the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum HandshakeStatus {
    /// Challenge accepted; `nonce_a` + `mac_a` follow.
    Ok = 0,
    /// A handshake frame carried the wrong command for its position: a non-
    /// `ReplicaHello` first frame, or a non-`ReplicaFinish` finish frame. As a
    /// `read_status` result it also covers byte 1 and any unrecognised byte.
    UnknownCommand = 1,
    /// Frame's cluster id did not match ours.
    ClusterMismatch = 2,
    /// Peer id was out of range or not strictly lower than the acceptor's id.
    DirectionalRule = 3,
    /// Enforcement is on and the peer sent no handshake nonce (log-only).
    AuthRequired = 4,
    /// The dialer's finish MAC did not verify (log-only).
    MacMismatch = 5,
}

impl HandshakeStatus {
    /// Stable lowercase label for structured logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::UnknownCommand => "unknown_command",
            Self::ClusterMismatch => "cluster_mismatch",
            Self::DirectionalRule => "directional_rule",
            Self::AuthRequired => "auth_required",
            Self::MacMismatch => "mac_mismatch",
        }
    }
}

/// Decode the `status` byte of a `ReplicaChallenge` response frame.
///
/// Total: byte `0` is [`HandshakeStatus::Ok`]; `1..=5` map to their reason; any
/// other (garbage) byte is treated as [`HandshakeStatus::UnknownCommand`] so the
/// dialer rejects rather than mistaking it for success. Discriminants must stay
/// in sync with [`HandshakeStatus`]; the `status_round_trips` test enforces it.
#[must_use]
pub const fn read_status(reserved_command: &[u8; RESERVED_COMMAND_LEN]) -> HandshakeStatus {
    match reserved_command[STATUS_OFFSET] {
        0 => HandshakeStatus::Ok,
        2 => HandshakeStatus::ClusterMismatch,
        3 => HandshakeStatus::DirectionalRule,
        4 => HandshakeStatus::AuthRequired,
        5 => HandshakeStatus::MacMismatch,
        // 1 and every unrecognised byte: a reject the dialer must not read as Ok.
        _ => HandshakeStatus::UnknownCommand,
    }
}

/// Channel binding folded into every handshake MAC.
///
/// `Tls` carries the rustls exporter value for the session the
/// handshake frames flow over. Both ends derive it from the TLS master
/// secret (same label, empty context), so a MAC bound to it verifies
/// only inside that exact session; a relay MITM terminates two distinct
/// sessions and both directions' MACs fail. `Plaintext` contributes a
/// zeroed exporter; the mode byte keeps the two domains separate.
#[derive(Clone, Copy)]
pub enum ChannelBinding {
    /// Plaintext replica link; nothing to bind.
    Plaintext,
    /// TLS link with the session's exporter value.
    Tls([u8; EXPORTER_LEN]),
}

impl ChannelBinding {
    const fn mode(self) -> u8 {
        match self {
            Self::Plaintext => 0,
            Self::Tls(_) => 1,
        }
    }

    const fn exporter(&self) -> &[u8; EXPORTER_LEN] {
        match self {
            Self::Plaintext => &[0u8; EXPORTER_LEN],
            Self::Tls(exporter) => exporter,
        }
    }
}

/// The fields bound into a handshake MAC. Identical on both peers.
pub struct Transcript {
    pub cluster_id: u128,
    pub dialer_id: u8,
    pub acceptor_id: u8,
    pub nonce_d: [u8; NONCE_LEN],
    pub nonce_a: [u8; NONCE_LEN],
    pub binding: ChannelBinding,
}

/// Cluster-wide replica authentication context.
///
/// Holds the derived MAC subkey (never the raw secret). Threaded as
/// `Option<ReplicaAuth>`; `None` means auth is disabled and the handshake stays
/// in legacy unauthenticated mode. `Some` means auth is enabled and enforced -
/// a peer that does not complete the handshake is rejected.
#[derive(Clone)]
pub struct ReplicaAuth {
    key: Zeroizing<[u8; 32]>,
}

impl ReplicaAuth {
    /// Derive the MAC subkey from the configured secret material.
    #[must_use]
    pub fn new(secret_material: &[u8]) -> Self {
        Self {
            key: Zeroizing::new(blake3::derive_key(KEY_CONTEXT, secret_material)),
        }
    }

    /// MAC the acceptor sends in the `ReplicaChallenge` frame.
    #[must_use]
    pub fn acceptor_mac(&self, transcript: &Transcript) -> [u8; MAC_LEN] {
        *self.mac(DIR_ACCEPTOR, transcript).as_bytes()
    }

    /// MAC the dialer sends in the finish frame.
    #[must_use]
    pub fn dialer_mac(&self, transcript: &Transcript) -> [u8; MAC_LEN] {
        *self.mac(DIR_DIALER, transcript).as_bytes()
    }

    /// Verify the acceptor's MAC in constant time.
    #[must_use]
    pub fn verify_acceptor_mac(&self, transcript: &Transcript, received: &[u8; MAC_LEN]) -> bool {
        self.mac(DIR_ACCEPTOR, transcript) == Hash::from_bytes(*received)
    }

    /// Verify the dialer's MAC in constant time.
    #[must_use]
    pub fn verify_dialer_mac(&self, transcript: &Transcript, received: &[u8; MAC_LEN]) -> bool {
        self.mac(DIR_DIALER, transcript) == Hash::from_bytes(*received)
    }

    fn mac(&self, dir: u8, transcript: &Transcript) -> Hash {
        let mut hasher = blake3::Hasher::new_keyed(&self.key);
        hasher.update(DOMAIN_TAG);
        hasher.update(&transcript.cluster_id.to_le_bytes());
        hasher.update(&[transcript.dialer_id, transcript.acceptor_id]);
        hasher.update(&transcript.nonce_d);
        hasher.update(&transcript.nonce_a);
        hasher.update(&[transcript.binding.mode()]);
        hasher.update(transcript.binding.exporter());
        hasher.update(&[dir]);
        hasher.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    fn transcript() -> Transcript {
        Transcript {
            cluster_id: 0xDEAD_BEEF,
            dialer_id: 3,
            acceptor_id: 1,
            nonce_d: [0x11; NONCE_LEN],
            nonce_a: [0x22; NONCE_LEN],
            binding: ChannelBinding::Plaintext,
        }
    }

    #[test]
    fn macs_round_trip() {
        let auth = ReplicaAuth::new(SECRET);
        let t = transcript();
        assert!(auth.verify_acceptor_mac(&t, &auth.acceptor_mac(&t)));
        assert!(auth.verify_dialer_mac(&t, &auth.dialer_mac(&t)));
    }

    #[test]
    fn wrong_key_is_rejected() {
        let signer = ReplicaAuth::new(SECRET);
        let attacker = ReplicaAuth::new(b"ffffffffffffffffffffffffffffffff");
        let t = transcript();
        assert!(!attacker.verify_dialer_mac(&t, &signer.dialer_mac(&t)));
    }

    #[test]
    fn direction_is_not_reflectable() {
        // The acceptor's MAC must not validate as a dialer MAC (reflection).
        let auth = ReplicaAuth::new(SECRET);
        let t = transcript();
        assert!(!auth.verify_dialer_mac(&t, &auth.acceptor_mac(&t)));
        assert!(!auth.verify_acceptor_mac(&t, &auth.dialer_mac(&t)));
    }

    #[test]
    fn changed_nonce_is_rejected() {
        // A captured MAC cannot be replayed against a fresh nonce.
        let auth = ReplicaAuth::new(SECRET);
        let t = transcript();
        let mac = auth.dialer_mac(&t);
        let replayed = Transcript {
            nonce_a: [0x33; NONCE_LEN],
            ..transcript()
        };
        assert!(!auth.verify_dialer_mac(&replayed, &mac));
    }

    #[test]
    fn swapped_peer_pair_is_rejected() {
        let auth = ReplicaAuth::new(SECRET);
        let t = transcript();
        let mac = auth.dialer_mac(&t);
        let swapped = Transcript {
            dialer_id: t.acceptor_id,
            acceptor_id: t.dialer_id,
            ..transcript()
        };
        assert!(!auth.verify_dialer_mac(&swapped, &mac));
    }

    #[test]
    fn different_tls_sessions_are_rejected() {
        // The MITM-relay defence: a MAC bound to one TLS session's
        // exporter must not verify against another session's exporter.
        let auth = ReplicaAuth::new(SECRET);
        let session_a = Transcript {
            binding: ChannelBinding::Tls([0xAA; EXPORTER_LEN]),
            ..transcript()
        };
        let session_b = Transcript {
            binding: ChannelBinding::Tls([0xBB; EXPORTER_LEN]),
            ..transcript()
        };
        let mac = auth.dialer_mac(&session_a);
        assert!(auth.verify_dialer_mac(&session_a, &mac));
        assert!(!auth.verify_dialer_mac(&session_b, &mac));
    }

    #[test]
    fn zeroed_exporter_does_not_alias_plaintext() {
        // Mode byte domain separation: a TLS transcript whose exporter is
        // all zeroes must still differ from the plaintext transcript.
        let auth = ReplicaAuth::new(SECRET);
        let plaintext = transcript();
        let tls_zeroed = Transcript {
            binding: ChannelBinding::Tls([0u8; EXPORTER_LEN]),
            ..transcript()
        };
        let mac = auth.dialer_mac(&plaintext);
        assert!(!auth.verify_dialer_mac(&tls_zeroed, &mac));
    }

    #[test]
    fn cluster_domain_id_is_stable_and_distinct() {
        assert_eq!(
            cluster_domain_id("iggy-cluster"),
            cluster_domain_id("iggy-cluster")
        );
        assert_ne!(
            cluster_domain_id("iggy-cluster"),
            cluster_domain_id("other")
        );
    }

    #[test]
    fn random_nonces_differ() {
        assert_ne!(random_nonce().unwrap(), random_nonce().unwrap());
    }

    #[test]
    fn status_round_trips() {
        const ALL: [HandshakeStatus; 6] = [
            HandshakeStatus::Ok,
            HandshakeStatus::UnknownCommand,
            HandshakeStatus::ClusterMismatch,
            HandshakeStatus::DirectionalRule,
            HandshakeStatus::AuthRequired,
            HandshakeStatus::MacMismatch,
        ];
        for status in ALL {
            let mut reserved = [0u8; RESERVED_COMMAND_LEN];
            // Mirror the on-wire write path (build_challenge_message reject).
            reserved[STATUS_OFFSET] = status as u8;
            assert_eq!(read_status(&reserved), status);
        }
    }

    #[test]
    fn ok_status_reads_ok() {
        // nonce[0..32] + mac[32..64] filled, the status byte stays zero.
        let mut reserved = [0u8; RESERVED_COMMAND_LEN];
        reserved[..NONCE_LEN].fill(0xAB);
        reserved[NONCE_LEN..NONCE_LEN + MAC_LEN].fill(0xCD);
        assert_eq!(read_status(&reserved), HandshakeStatus::Ok);
    }

    #[test]
    fn unknown_status_byte_reads_reject() {
        // A garbage status byte must NOT read as Ok (else the dialer would parse
        // a bogus MAC). It collapses to a reject.
        let mut reserved = [0u8; RESERVED_COMMAND_LEN];
        reserved[STATUS_OFFSET] = 200;
        assert_ne!(read_status(&reserved), HandshakeStatus::Ok);
    }
}
