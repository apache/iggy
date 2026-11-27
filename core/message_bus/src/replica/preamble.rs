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

//! Placement preamble of a replica link.
//!
//! The owner shard of the dialer writes these bytes on the raw socket,
//! before TLS and before the hello. Shard 0 of the acceptor reads exactly
//! them, so the owner shard it delegates to still finds the TLS
//! `ClientHello` (or the plaintext hello) first in the stream. No TLS
//! session ever moves between shards.
//!
//! The bytes are not authenticated. They only pick a shard; the hello that
//! follows names the same peer and link, and the owner shard refuses a
//! mismatch.

use compio::BufResult;
use compio::buf::{IntoInner, IoBuf};
use compio::io::{AsyncReadExt, AsyncWriteExt};
use compio::net::TcpStream;
use std::io;
use std::time::Duration;
use thiserror::Error;

pub const PREAMBLE_LEN: usize = 8;
const PREAMBLE_MAGIC: u32 = u32::from_le_bytes(*b"IGRL");
const PREAMBLE_VERSION: u8 = 1;

/// Wire layout, little-endian like the rest of the wire format:
/// magic `u32`, version `u8`, dialer replica id `u8`, link index `u16`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Preamble {
    pub dialer: u8,
    pub link: u16,
}

/// Why an acceptor closed a connection before delegating it.
#[derive(Debug, Error)]
pub enum PreambleError {
    #[error("replica handshake context not installed on shard 0")]
    NoHandshakeContext,
    #[error("preamble read failed: {0}")]
    Read(#[source] io::Error),
    #[error("no preamble within {0:?}")]
    TimedOut(Duration),
    #[error("not a replica link preamble (magic {0:#010x}), likely a peer on an older build")]
    BadMagic(u32),
    #[error("unsupported preamble version {0}")]
    BadVersion(u8),
    #[error(
        "dialer {dialer} is not a lower replica id of this {replica_count}-node cluster \
         (own id {self_id})"
    )]
    UnknownDialer {
        dialer: u8,
        self_id: u8,
        replica_count: u8,
    },
    #[error("link {link} out of range, this node runs {links} links per peer")]
    LinkOutOfRange { link: u16, links: u16 },
}

impl Preamble {
    #[must_use]
    pub fn encode(self) -> [u8; PREAMBLE_LEN] {
        let mut bytes = [0u8; PREAMBLE_LEN];
        bytes[..4].copy_from_slice(&PREAMBLE_MAGIC.to_le_bytes());
        bytes[4] = PREAMBLE_VERSION;
        bytes[5] = self.dialer;
        bytes[6..].copy_from_slice(&self.link.to_le_bytes());
        bytes
    }

    /// Decode `bytes` for the acceptor `self_id` of a `replica_count`-node
    /// cluster with `links` links per peer. Only a lower id dials.
    ///
    /// # Errors
    ///
    /// Refuses another magic or version, a dialer that is not a lower
    /// replica id, and a link at or above `links`.
    pub const fn admit(
        bytes: [u8; PREAMBLE_LEN],
        self_id: u8,
        replica_count: u8,
        links: u16,
    ) -> Result<Self, PreambleError> {
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if magic != PREAMBLE_MAGIC {
            return Err(PreambleError::BadMagic(magic));
        }
        if bytes[4] != PREAMBLE_VERSION {
            return Err(PreambleError::BadVersion(bytes[4]));
        }
        let dialer = bytes[5];
        if dialer >= self_id || dialer >= replica_count {
            return Err(PreambleError::UnknownDialer {
                dialer,
                self_id,
                replica_count,
            });
        }
        let link = u16::from_le_bytes([bytes[6], bytes[7]]);
        if link >= links {
            return Err(PreambleError::LinkOutOfRange { link, links });
        }
        Ok(Self { dialer, link })
    }
}

/// Write `preamble` on the raw socket of a dialed link.
///
/// # Errors
///
/// Returns the socket error.
#[allow(clippy::future_not_send)]
pub async fn write_preamble(stream: &mut TcpStream, preamble: Preamble) -> io::Result<()> {
    stream.write_all(preamble.encode().to_vec()).await.0
}

/// Read exactly [`PREAMBLE_LEN`] bytes from an accepted socket.
///
/// The read buffer holds exactly that many bytes, so no byte of the TLS
/// `ClientHello` or of the hello leaves the kernel buffer here, and no read
/// stays in flight on the socket once this returns.
///
/// # Errors
///
/// Returns [`PreambleError::TimedOut`] past `timeout` and
/// [`PreambleError::Read`] on a socket error or an early EOF.
#[allow(clippy::future_not_send)]
pub async fn read_preamble(
    stream: &mut TcpStream,
    timeout: Duration,
) -> Result<[u8; PREAMBLE_LEN], PreambleError> {
    let buf = Vec::<u8>::with_capacity(PREAMBLE_LEN).slice(..PREAMBLE_LEN);
    let BufResult(result, buf) = compio::time::timeout(timeout, stream.read_exact(buf))
        .await
        .map_err(|_| PreambleError::TimedOut(timeout))?;
    result.map_err(PreambleError::Read)?;
    let mut bytes = [0u8; PREAMBLE_LEN];
    bytes.copy_from_slice(&buf.into_inner());
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SELF_ID: u8 = 2;
    const REPLICAS: u8 = 3;
    const LINKS: u16 = 4;

    #[test]
    fn preamble_round_trips() {
        let preamble = Preamble { dialer: 1, link: 3 };
        assert_eq!(
            Preamble::admit(preamble.encode(), SELF_ID, REPLICAS, LINKS).unwrap(),
            preamble
        );
    }

    #[test]
    fn preamble_is_little_endian() {
        let bytes = Preamble {
            dialer: 7,
            link: 0x0102,
        }
        .encode();
        assert_eq!(bytes, [b'I', b'G', b'R', b'L', 1, 7, 0x02, 0x01]);
    }

    #[test]
    fn acceptor_refuses_a_foreign_magic_or_version() {
        let mut bytes = Preamble { dialer: 0, link: 0 }.encode();
        bytes[0] = 0x16; // a TLS record type: a dialer that sent no preamble
        assert!(matches!(
            Preamble::admit(bytes, SELF_ID, REPLICAS, LINKS),
            Err(PreambleError::BadMagic(_))
        ));
        let mut bytes = Preamble { dialer: 0, link: 0 }.encode();
        bytes[4] = 2;
        assert!(matches!(
            Preamble::admit(bytes, SELF_ID, REPLICAS, LINKS),
            Err(PreambleError::BadVersion(2))
        ));
    }

    #[test]
    fn acceptor_refuses_a_dialer_that_is_not_a_lower_id() {
        for dialer in [SELF_ID, SELF_ID + 1, u8::MAX] {
            let bytes = Preamble { dialer, link: 0 }.encode();
            assert!(
                matches!(
                    Preamble::admit(bytes, SELF_ID, REPLICAS, LINKS),
                    Err(PreambleError::UnknownDialer { .. })
                ),
                "dialer {dialer}"
            );
        }
    }

    #[test]
    fn acceptor_refuses_a_link_out_of_range() {
        let bytes = Preamble {
            dialer: 0,
            link: LINKS,
        }
        .encode();
        assert!(matches!(
            Preamble::admit(bytes, SELF_ID, REPLICAS, LINKS),
            Err(PreambleError::LinkOutOfRange { link: LINKS, .. })
        ));
    }
}
