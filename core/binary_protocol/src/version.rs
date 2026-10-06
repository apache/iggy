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

//! Binary protocol versioning.
//!
//! The wire version is an explicit packed semver, maintained manually for
//! stable Iggy releases and independent of server and SDK package versions.
//! It is exchanged during the login/register
//! handshake: clients send [`ClientVersionInfo`] as the body prefix of both
//! login-register request shapes, the server gates on
//! [`is_protocol_compatible`] before touching credentials and advertises
//! its own version in the response. Incompatible clients are rejected with
//! an `EvictionReason::IncompatibleProtocol` frame carrying the accepted
//! range.
//!
//! # Wire spec (language-neutral)
//!
//! Reference for SDKs that do not consume this crate. All integers are
//! little-endian on the wire.
//!
//! ## Packed protocol version
//!
//! A semver `major.minor.patch` packs into one `u32`, 10 bits per
//! component (each must be < 1024):
//!
//! ```text
//! bits 31..30  reserved (zero)
//! bits 29..20  major
//! bits 19..10  minor
//! bits  9..0   patch
//! value = major << 20 | minor << 10 | patch
//! ```
//!
//! Integer order equals semver order. Every component participates in the
//! compatibility check; a patch increment does not imply wire compatibility.
//!
//! ## `ClientVersionInfo` body prefix
//!
//! ```text
//! [protocol_version: u32]
//! [sdk_name_len: u8][sdk_name: UTF-8, 1-255 bytes]
//! [sdk_version_len: u8][sdk_version: UTF-8, 1-255 bytes]
//! ```
//!
//! ## Request framing
//!
//! `ClientVersionInfo` is the leading bytes of the login-register request
//! *body*, which itself rides inside a 256-byte VSR `RequestHeader` (see
//! `consensus::header`): `command` = `Command::Request`, `operation` =
//! `Operation::Register`, client id in `RequestHeader.client`. The client
//! sends no group; the server derives it. A foreign SDK emits that header,
//! then the body starting with this prefix, to reach the gate.
//!
//! ## Login gate
//!
//! The server accepts a client when its packed version is in the inclusive
//! range `IGGY_PROTOCOL_VERSION_MIN..=IGGY_PROTOCOL_VERSION`, including patch.
//! Set both bounds manually for each stable release. Keep them equal unless
//! every released protocol in a wider range is deliberately supported.
//!
//! Compatibility is defined between stable releases. Intermediate development
//! and edge builds may share a protocol number while their layouts change;
//! they require matching client and server builds. Such changes do not require
//! a protocol bump for each commit.
//!
//! ## Rejection frame
//!
//! An incompatible login is answered with a header-only 256-byte
//! `Eviction` frame (`EvictionHeader` in `consensus::header`): `reason`
//! at byte 255 is `IncompatibleProtocol` (14), and the accepted window
//! sits at fixed offsets: `server_protocol_version` (max) at byte 144,
//! `server_protocol_version_min` at byte 148, both packed `u32`.
//!
//! A login body without a decodable `ClientVersionInfo` prefix is
//! rejected with reason `MalformedLogin` (15) instead; the window bytes
//! are zero.

use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_u32_le};
use crate::primitives::identifier::WireName;
use bytes::{BufMut, BytesMut};

/// Bits per packed semver component (each must be < 1024).
const COMPONENT_BITS: u32 = 10;
const COMPONENT_MAX: u32 = (1 << COMPONENT_BITS) - 1;
const PATCH_MASK: u32 = COMPONENT_MAX;

/// Current development protocol version, independent of package versions.
///
/// Version 0.11.1 requires the session-binding secret in login requests.
/// Finalize this number and the minimum during stable release preparation.
/// Released servers using protocol 0.11.0 accept every 0.11.x patch, so an
/// incompatible release needs a new minor version for those servers to reject it.
pub const IGGY_PROTOCOL_VERSION: u32 = pack_protocol_version(0, 11, 1);

/// Oldest protocol version this build accepts at login.
/// Version 0.11.0 has no session-binding secret and is incompatible.
///
/// Keep this equal to the current protocol unless compatibility with older
/// released protocols has been verified. Decide the range per stable release.
pub const IGGY_PROTOCOL_VERSION_MIN: u32 = pack_protocol_version(0, 11, 1);
const _: () =
    assert!(IGGY_PROTOCOL_VERSION_MIN > 0 && IGGY_PROTOCOL_VERSION_MIN <= IGGY_PROTOCOL_VERSION);

/// Range check used by the server-side login gate.
///
/// Packed component order preserves semver ordering, so plain integer
/// comparisons enforce both inclusive bounds, including patch versions.
#[must_use]
pub const fn is_protocol_compatible(client: u32) -> bool {
    client >= IGGY_PROTOCOL_VERSION_MIN && client <= IGGY_PROTOCOL_VERSION
}

/// Pack semver components: `major << 20 | minor << 10 | patch`.
///
/// # Panics
/// At compile time (const context) when any component exceeds 1023.
#[must_use]
pub const fn pack_protocol_version(major: u32, minor: u32, patch: u32) -> u32 {
    assert!(
        major <= COMPONENT_MAX && minor <= COMPONENT_MAX && patch <= COMPONENT_MAX,
        "semver component exceeds 10-bit packing range"
    );
    (major << (2 * COMPONENT_BITS)) | (minor << COMPONENT_BITS) | patch
}

/// Display adapter for a packed protocol version (`major.minor.patch`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolVersion(pub u32);

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.0 >> (2 * COMPONENT_BITS),
            (self.0 >> COMPONENT_BITS) & COMPONENT_MAX,
            self.0 & PATCH_MASK
        )
    }
}

/// Client identity sent as the prefix of every login-register request body.
///
/// Wire format:
/// ```text
/// [protocol_version:u32 LE][sdk_name_len:u8][sdk_name:N][sdk_version_len:u8][sdk_version:N]
/// ```
///
/// `protocol_version` is the packed wire version the client implements;
/// `sdk_version` is the client crate's own version
/// (e.g. the `iggy` crate for the Rust SDK). Encoded first so the server can
/// parse and gate on it regardless of how the rest of the body evolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientVersionInfo {
    pub protocol_version: u32,
    /// SDK identifier, e.g. `rust-sdk`, `go-sdk`.
    pub sdk_name: WireName,
    /// SDK build version, e.g. `0.10.1`.
    pub sdk_version: WireName,
}

impl WireEncode for ClientVersionInfo {
    fn encoded_size(&self) -> usize {
        4 + self.sdk_name.encoded_size() + self.sdk_version.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.protocol_version);
        self.sdk_name.encode(buf);
        self.sdk_version.encode(buf);
    }
}

impl WireDecode for ClientVersionInfo {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let protocol_version = read_u32_le(buf, 0)?;
        let mut pos = 4;
        let (sdk_name, sdk_name_len) = WireName::decode(&buf[pos..])?;
        pos += sdk_name_len;
        let (sdk_version, sdk_version_len) = WireName::decode(&buf[pos..])?;
        pos += sdk_version_len;
        Ok((
            Self {
                protocol_version,
                sdk_name,
                sdk_version,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ClientVersionInfo {
        ClientVersionInfo {
            protocol_version: IGGY_PROTOCOL_VERSION,
            sdk_name: WireName::new("rust-sdk").unwrap(),
            sdk_version: WireName::new("0.10.1").unwrap(),
        }
    }

    #[test]
    fn roundtrip() {
        let info = sample();
        let bytes = info.to_bytes();
        let (decoded, consumed) = ClientVersionInfo::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, info);
    }

    #[test]
    fn encoded_size_matches_output() {
        let info = sample();
        assert_eq!(info.encoded_size(), info.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let bytes = sample().to_bytes();
        for i in 0..bytes.len() {
            assert!(
                ClientVersionInfo::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_layout_protocol_version_first() {
        let bytes = sample().to_bytes();
        assert_eq!(
            u32::from_le_bytes(bytes[..4].try_into().unwrap()),
            IGGY_PROTOCOL_VERSION
        );
        assert_eq!(bytes[4], 8); // sdk_name len
        assert_eq!(&bytes[5..13], b"rust-sdk");
    }

    #[test]
    fn login_without_binding_secret_is_outside_the_compatible_window() {
        assert!(!is_protocol_compatible(pack_protocol_version(0, 11, 0)));
        assert!(is_protocol_compatible(pack_protocol_version(0, 11, 1)));
        assert!(!is_protocol_compatible(pack_protocol_version(0, 11, 1023)));
        assert!(!is_protocol_compatible(pack_protocol_version(0, 12, 0)));
    }

    #[test]
    fn packing_preserves_semver_order() {
        assert!(pack_protocol_version(0, 9, 999) < pack_protocol_version(0, 10, 0));
        assert!(pack_protocol_version(0, 10, 1) < pack_protocol_version(1, 0, 0));
    }

    #[test]
    fn min_requires_the_session_binding_wire_layout() {
        assert_eq!(IGGY_PROTOCOL_VERSION_MIN, pack_protocol_version(0, 11, 1));
    }

    #[test]
    fn compatibility_range_boundaries() {
        assert!(is_protocol_compatible(IGGY_PROTOCOL_VERSION_MIN));
        assert!(is_protocol_compatible(IGGY_PROTOCOL_VERSION));
        assert!(
            !is_protocol_compatible(IGGY_PROTOCOL_VERSION + 1),
            "a newer patch must not pass the current release's protocol gate"
        );
        assert!(!is_protocol_compatible(IGGY_PROTOCOL_VERSION_MIN - 1));
        assert!(!is_protocol_compatible(u32::MAX));
    }

    #[test]
    fn protocol_version_display() {
        assert_eq!(
            ProtocolVersion(pack_protocol_version(0, 10, 1)).to_string(),
            "0.10.1"
        );
    }
}
