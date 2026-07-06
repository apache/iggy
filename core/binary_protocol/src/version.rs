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
//! The protocol version is this crate's own semver, const-parsed from
//! `CARGO_PKG_VERSION` into a packed `u32` so it auto-bumps with releases
//! and stays cheaply comparable. It is exchanged during the login/register
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
//! Integer order equals semver order. The value tracks the
//! `iggy_binary_protocol` crate release; under 0.x a minor bump may break
//! the wire, so the gate is minor-scoped. Past 1.0.0 the gate follows
//! strict semver: major bump = incompatible, minor/patch = compatible.
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
//! `consensus::header`): `command` = `Command2::Request`, `operation` =
//! `Operation::Register`, `namespace` = `METADATA_CONSENSUS_NAMESPACE`,
//! client id in `RequestHeader.client`. A foreign SDK emits that header,
//! then the body starting with this prefix, to reach the gate.
//!
//! ## Login gate
//!
//! The server accepts a client when its packed version is
//! `>= IGGY_PROTOCOL_VERSION_MIN` and its `major.minor` is `<=` the
//! server's. Patch releases never change the wire, so the upper bound
//! ignores patch. `IGGY_PROTOCOL_VERSION_MIN` defaults to the current
//! version with patch zeroed and is widened deliberately when a minor
//! bump stays wire-compatible.
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

/// Current binary protocol version: this crate's semver, packed.
/// Pre-release tags (`-edge.N`) are ignored.
pub const IGGY_PROTOCOL_VERSION: u32 = parse_packed_semver(env!("CARGO_PKG_VERSION"));

/// Oldest protocol version this build still accepts at login: the current
/// version with patch zeroed (patch releases never change the wire).
/// Widen deliberately when a minor bump stays wire-compatible.
///
/// Under 0.x this makes a server minor bump a client flag-day: every
/// prior-minor client is rejected at login until MIN is widened. A rolling
/// upgrade across a minor bump must either widen MIN (when the wire stayed
/// compatible) or accept that old clients fail re-login -- decide per release.
// TODO(hubcio): past 1.0.0 follow strict semver: major bump = incompatible,
// minor/patch = compatible, so MIN derives from the current major instead
// of the current minor. Under 0.x a minor bump may break the wire, so the
// minor-scoped window is correct.
pub const IGGY_PROTOCOL_VERSION_MIN: u32 = IGGY_PROTOCOL_VERSION & !PATCH_MASK;
// A 0.0.x crate version would pack MIN to 0, which `EvictionHeader::validate`
// rejects (window requires min >= 1) -- the server would emit eviction frames
// that fail its own validation.
const _: () = assert!(IGGY_PROTOCOL_VERSION_MIN > 0);

/// Range check used by the server-side login gate.
///
/// Packed component order preserves semver ordering, so plain integer
/// comparisons work. The upper bound ignores patch: patch releases never
/// change the wire, so a newer patch of an accepted minor is compatible.
#[must_use]
pub const fn is_protocol_compatible(client: u32) -> bool {
    client >= IGGY_PROTOCOL_VERSION_MIN
        && (client >> COMPONENT_BITS) <= (IGGY_PROTOCOL_VERSION >> COMPONENT_BITS)
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

/// Const-parse `major.minor.patch[-pre]` into a packed `u32`.
/// Malformed input is a compile error in const context.
const fn parse_packed_semver(version: &str) -> u32 {
    let bytes = version.as_bytes();
    let (major, i) = parse_component(bytes, 0);
    assert!(
        i < bytes.len() && bytes[i] == b'.',
        "expected '.' after major"
    );
    let (minor, j) = parse_component(bytes, i + 1);
    assert!(
        j < bytes.len() && bytes[j] == b'.',
        "expected '.' after minor"
    );
    let (patch, k) = parse_component(bytes, j + 1);
    assert!(
        k == bytes.len() || bytes[k] == b'-' || bytes[k] == b'+',
        "unexpected trailing bytes after patch"
    );
    pack_protocol_version(major, minor, patch)
}

/// Parse a decimal run starting at `start`; returns (value, index past digits).
const fn parse_component(bytes: &[u8], start: usize) -> (u32, usize) {
    assert!(
        start < bytes.len() && bytes[start].is_ascii_digit(),
        "expected digit"
    );
    let mut value: u32 = 0;
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        value = value * 10 + (bytes[i] - b'0') as u32;
        i += 1;
    }
    (value, i)
}

/// Client identity sent as the prefix of every login-register request body.
///
/// Wire format:
/// ```text
/// [protocol_version:u32 LE][sdk_name_len:u8][sdk_name:N][sdk_version_len:u8][sdk_version:N]
/// ```
///
/// `protocol_version` is the packed `iggy_binary_protocol` crate version the
/// client was built against; `sdk_version` is the client crate's own version
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
    fn parses_crate_version_with_prerelease() {
        assert_eq!(
            parse_packed_semver("0.10.1-edge.2"),
            pack_protocol_version(0, 10, 1)
        );
        assert_eq!(parse_packed_semver("1.2.3"), pack_protocol_version(1, 2, 3));
        assert_eq!(
            parse_packed_semver("10.0.0+build.5"),
            pack_protocol_version(10, 0, 0)
        );
    }

    #[test]
    fn packing_preserves_semver_order() {
        assert!(pack_protocol_version(0, 9, 999) < pack_protocol_version(0, 10, 0));
        assert!(pack_protocol_version(0, 10, 1) < pack_protocol_version(1, 0, 0));
    }

    #[test]
    fn min_is_current_with_patch_zeroed() {
        assert_eq!(IGGY_PROTOCOL_VERSION_MIN & PATCH_MASK, 0);
        assert_eq!(
            IGGY_PROTOCOL_VERSION >> COMPONENT_BITS,
            IGGY_PROTOCOL_VERSION_MIN >> COMPONENT_BITS
        );
    }

    #[test]
    fn compatibility_range_boundaries() {
        assert!(is_protocol_compatible(IGGY_PROTOCOL_VERSION_MIN));
        assert!(is_protocol_compatible(IGGY_PROTOCOL_VERSION));
        // Patch never changes the wire: any patch of the current minor passes.
        assert!(is_protocol_compatible(IGGY_PROTOCOL_VERSION | PATCH_MASK));
        // Next minor is outside the window.
        assert!(!is_protocol_compatible(
            ((IGGY_PROTOCOL_VERSION >> COMPONENT_BITS) + 1) << COMPONENT_BITS
        ));
        if IGGY_PROTOCOL_VERSION_MIN > 0 {
            assert!(!is_protocol_compatible(IGGY_PROTOCOL_VERSION_MIN - 1));
        }
    }

    #[test]
    fn protocol_version_display() {
        assert_eq!(
            ProtocolVersion(pack_protocol_version(0, 10, 1)).to_string(),
            "0.10.1"
        );
    }
}
