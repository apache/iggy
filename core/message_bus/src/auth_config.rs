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

//! Boot-time loader for the replica-plane cluster secret.
//!
//! Two environment variables drive the loader; exactly one must be set:
//!
//! - `IGGY_CLUSTER_SECRET` — 64-character hex-encoded 32-byte secret,
//!   passed inline. Convenient for `docker-compose` / `systemd`
//!   `Environment=` but visible in `/proc/$pid/environ`.
//! - `IGGY_CLUSTER_SECRET_FILE` — path to a file whose contents are the
//!   64-character hex secret. Leading / trailing whitespace is trimmed.
//!
//! Both variants decode to the same `[u8; 32]` used by
//! [`crate::auth::StaticSharedSecret`]. `StaticSharedSecret::zero()`
//! remains the integration-test default; production deployments must
//! choose one of the two env vars above.
//!
//! The loader is a one-shot startup event; synchronous `std::fs` is
//! intentional. Do not call it on the data plane.

use std::env;
use std::fs;
use std::rc::Rc;

use thiserror::Error;

use crate::auth::{SECRET_SIZE, StaticSharedSecret, TokenSource};

/// Env var carrying the hex-encoded secret literal.
pub const CLUSTER_SECRET_ENV: &str = "IGGY_CLUSTER_SECRET";

/// Env var carrying the path to a file containing the hex-encoded secret.
pub const CLUSTER_SECRET_FILE_ENV: &str = "IGGY_CLUSTER_SECRET_FILE";

/// Length of the hex-encoded form of a [`SECRET_SIZE`]-byte secret.
pub const HEX_SECRET_LEN: usize = SECRET_SIZE * 2;

/// Failure modes for secret resolution. Surfaced distinctly so operators
/// get a pointed error at boot rather than a generic "invalid config".
#[derive(Debug, Error)]
pub enum SecretLoadError {
    #[error(
        "neither {CLUSTER_SECRET_ENV} nor {CLUSTER_SECRET_FILE_ENV} is set; \
         one is required to boot the replica plane"
    )]
    Missing,
    #[error(
        "both {CLUSTER_SECRET_ENV} and {CLUSTER_SECRET_FILE_ENV} are set; \
         exactly one is required"
    )]
    BothSet,
    #[error("secret has {got} hex characters; expected {HEX_SECRET_LEN}")]
    WrongLength { got: usize },
    #[error("secret contains non-hex character at byte offset {0}")]
    InvalidHex(usize),
    #[error("failed to read secret file {path}")]
    FileRead {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// Resolve the 32-byte secret from the process environment.
///
/// # Errors
///
/// See [`SecretLoadError`].
pub fn load_cluster_secret() -> Result<[u8; SECRET_SIZE], SecretLoadError> {
    let literal = env::var(CLUSTER_SECRET_ENV).ok();
    let file = env::var(CLUSTER_SECRET_FILE_ENV).ok();
    resolve(literal.as_deref(), file.as_deref())
}

/// Resolve the secret and wrap it in a reference-counted
/// [`StaticSharedSecret`]. Call sites that expect
/// `Rc<dyn TokenSource>` get it via unsize coercion:
///
/// ```ignore
/// let ts: Rc<dyn TokenSource> = auth_config::cluster_token_source()?;
/// ```
///
/// # Errors
///
/// See [`SecretLoadError`].
pub fn cluster_token_source() -> Result<Rc<dyn TokenSource>, SecretLoadError> {
    let secret = load_cluster_secret()?;
    Ok(Rc::new(StaticSharedSecret::new(secret)))
}

/// Pure resolution rule, factored out of [`load_cluster_secret`] so tests
/// can drive it without touching the shared process environment (env-var
/// tests race under `cargo test`).
///
/// # Errors
///
/// See [`SecretLoadError`].
pub fn resolve(
    literal: Option<&str>,
    file_path: Option<&str>,
) -> Result<[u8; SECRET_SIZE], SecretLoadError> {
    match (literal, file_path) {
        (Some(hex), None) => decode_hex_secret(hex.trim()),
        (None, Some(path)) => {
            let raw = fs::read_to_string(path).map_err(|source| SecretLoadError::FileRead {
                path: path.to_owned(),
                source,
            })?;
            decode_hex_secret(raw.trim())
        }
        (Some(_), Some(_)) => Err(SecretLoadError::BothSet),
        (None, None) => Err(SecretLoadError::Missing),
    }
}

fn decode_hex_secret(input: &str) -> Result<[u8; SECRET_SIZE], SecretLoadError> {
    if input.len() != HEX_SECRET_LEN {
        return Err(SecretLoadError::WrongLength { got: input.len() });
    }
    let bytes = input.as_bytes();
    let mut out = [0u8; SECRET_SIZE];
    for (i, chunk) in bytes.chunks_exact(2).enumerate() {
        let hi = decode_hex_nibble(chunk[0]).ok_or(SecretLoadError::InvalidHex(i * 2))?;
        let lo = decode_hex_nibble(chunk[1]).ok_or(SecretLoadError::InvalidHex(i * 2 + 1))?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

const fn decode_hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(10 + c - b'a'),
        b'A'..=b'F' => Some(10 + c - b'A'),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    const SAMPLE_BYTES: [u8; SECRET_SIZE] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ];

    #[test]
    fn resolve_literal_happy_path() {
        let got = resolve(Some(SAMPLE_HEX), None).expect("resolve literal");
        assert_eq!(got, SAMPLE_BYTES);
    }

    #[test]
    fn resolve_literal_trims_whitespace() {
        let padded = format!("  {SAMPLE_HEX}\n");
        let got = resolve(Some(&padded), None).expect("resolve literal with whitespace");
        assert_eq!(got, SAMPLE_BYTES);
    }

    #[test]
    fn resolve_literal_uppercase_ok() {
        let upper = SAMPLE_HEX.to_uppercase();
        let got = resolve(Some(&upper), None).expect("resolve uppercase literal");
        assert_eq!(got, SAMPLE_BYTES);
    }

    #[test]
    fn neither_set_is_missing() {
        let err = resolve(None, None).unwrap_err();
        assert!(matches!(err, SecretLoadError::Missing), "got {err:?}");
    }

    #[test]
    fn both_set_is_both_set() {
        let err = resolve(Some(SAMPLE_HEX), Some("/dev/null")).unwrap_err();
        assert!(matches!(err, SecretLoadError::BothSet), "got {err:?}");
    }

    #[test]
    fn wrong_length_is_wrong_length() {
        let short = &SAMPLE_HEX[..SAMPLE_HEX.len() - 2];
        let err = resolve(Some(short), None).unwrap_err();
        match err {
            SecretLoadError::WrongLength { got } => assert_eq!(got, HEX_SECRET_LEN - 2),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn non_hex_is_invalid_hex() {
        let mut bad = SAMPLE_HEX.to_owned();
        // Replace the 5th hex char with a non-hex rune.
        bad.replace_range(4..5, "Z");
        let err = resolve(Some(&bad), None).unwrap_err();
        match err {
            SecretLoadError::InvalidHex(offset) => assert_eq!(offset, 4),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn missing_file_is_file_read() {
        // A path that definitely does not exist; /proc/self/ is real but
        // the basename is a random ULID-shaped string.
        let bogus = "/proc/self/this-path-will-not-exist-01HZZZ";
        let err = resolve(None, Some(bogus)).unwrap_err();
        match err {
            SecretLoadError::FileRead { path, .. } => assert_eq!(path, bogus),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn file_round_trip_with_tempfile() {
        use std::io::Write;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("secret.hex");
        let mut f = fs::File::create(&path).expect("create");
        writeln!(f, "  {SAMPLE_HEX}  ").expect("write");
        drop(f);

        let got =
            resolve(None, Some(path.to_str().expect("utf-8 path"))).expect("resolve from tempfile");
        assert_eq!(got, SAMPLE_BYTES);
    }

    #[test]
    fn cluster_token_source_produces_matching_secret() {
        // Exercise the Rc<dyn TokenSource> return path end-to-end: resolve,
        // sign a challenge with the loader's secret, verify with a
        // hand-built StaticSharedSecret over the same bytes.
        use crate::auth::{AuthChallenge, TokenSource};

        let source = Rc::new(StaticSharedSecret::new(
            resolve(Some(SAMPLE_HEX), None).expect("resolve"),
        )) as Rc<dyn TokenSource>;
        let reference = StaticSharedSecret::new(SAMPLE_BYTES);
        let challenge = AuthChallenge {
            cluster: 1,
            peer_id: 2,
            timestamp_ns: 3,
            release: 4,
            nonce: 5,
        };
        let from_loader = source.sign(&challenge);
        let from_reference = reference.sign(&challenge);
        assert_eq!(from_loader, from_reference);
    }
}
