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

use crate::WireError;
use bytes::{Bytes, BytesMut};

/// Encode a wire type into a caller-owned buffer.
///
/// Buffer-first design: the caller controls allocation. The `to_bytes()`
/// convenience method allocates when needed, but hot paths can reuse
/// buffers via `encode()` directly.
pub trait WireEncode {
    /// Write the encoded representation into `buf`.
    fn encode(&self, buf: &mut BytesMut);

    /// Return the exact encoded size in bytes.
    fn encoded_size(&self) -> usize;

    /// Convenience: allocate a new [`Bytes`] and encode into it.
    #[must_use]
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.encoded_size());
        self.encode(&mut buf);
        buf.freeze()
    }
}

/// Decode a wire type from a byte slice.
///
/// Takes `&[u8]` instead of [`Bytes`] to avoid requiring reference-counted
/// ownership at the decode boundary.
pub trait WireDecode: Sized {
    /// Decode from `buf`, consuming exactly the bytes needed.
    /// Returns the decoded value and the number of bytes consumed.
    ///
    /// # Errors
    /// Returns `WireError` if the buffer is too short or contains invalid data.
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError>;

    /// Convenience: decode from the entire buffer, ignoring trailing bytes.
    ///
    /// # Errors
    /// Returns `WireError` if decoding fails.
    fn decode_from(buf: &[u8]) -> Result<Self, WireError> {
        Self::decode(buf).map(|(val, _)| val)
    }
}

/// Helper to read a `u8` from `buf` at `offset`.
///
/// # Errors
/// Returns `WireError::UnexpectedEof` if `offset` is out of bounds.
#[inline]
pub fn read_u8(buf: &[u8], offset: usize) -> Result<u8, WireError> {
    buf.get(offset)
        .copied()
        .ok_or_else(|| WireError::UnexpectedEof {
            offset,
            need: 1,
            have: buf.len().saturating_sub(offset),
        })
}

/// Helper to read a `u32` LE from `buf` at `offset`.
///
/// # Errors
/// Returns `WireError::UnexpectedEof` if fewer than 4 bytes remain.
#[allow(clippy::missing_panics_doc)]
#[inline]
pub fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32, WireError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset,
            need: 4,
            have: buf.len().saturating_sub(offset),
        })?;
    let slice = buf
        .get(offset..end)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset,
            need: 4,
            have: buf.len().saturating_sub(offset),
        })?;
    Ok(u32::from_le_bytes(
        slice.try_into().expect("slice is exactly 4 bytes"),
    ))
}

/// Helper to read a `u64` LE from `buf` at `offset`.
///
/// # Errors
/// Returns `WireError::UnexpectedEof` if fewer than 8 bytes remain.
#[allow(clippy::missing_panics_doc)]
#[inline]
pub fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64, WireError> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset,
            need: 8,
            have: buf.len().saturating_sub(offset),
        })?;
    let slice = buf
        .get(offset..end)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset,
            need: 8,
            have: buf.len().saturating_sub(offset),
        })?;
    Ok(u64::from_le_bytes(
        slice.try_into().expect("slice is exactly 8 bytes"),
    ))
}

/// Helper to read a UTF-8 string of `len` bytes from `buf` at `offset`.
///
/// # Errors
/// Returns `WireError::UnexpectedEof` or `WireError::InvalidUtf8` on failure.
#[inline]
pub fn read_str(buf: &[u8], offset: usize, len: usize) -> Result<String, WireError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset,
            need: len,
            have: buf.len().saturating_sub(offset),
        })?;
    let slice = buf
        .get(offset..end)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset,
            need: len,
            have: buf.len().saturating_sub(offset),
        })?;
    std::str::from_utf8(slice)
        .map(str::to_string)
        .map_err(|_| WireError::InvalidUtf8 { offset })
}

/// Helper to read a byte slice of `len` bytes from `buf` at `offset`.
///
/// # Errors
/// Returns `WireError::UnexpectedEof` if fewer than `len` bytes remain.
#[inline]
pub fn read_bytes(buf: &[u8], offset: usize, len: usize) -> Result<&[u8], WireError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset,
            need: len,
            have: buf.len().saturating_sub(offset),
        })?;
    buf.get(offset..end)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset,
            need: len,
            have: buf.len().saturating_sub(offset),
        })
}
