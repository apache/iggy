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

//! Little-endian read cursor shared by the crate's hand-rolled wire codecs
//! (the state-transfer manifest and the client-table wire encoding).
//!
//! Each codec has its own error enum, so the cursor reports the one failure it
//! can produce ([`Truncated`]) and every codec converts it through `From`.
//!
//! [`Truncated`]: Truncated

/// The cursor ran past the end of its slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Truncated;

/// Little-endian cursor over an encoded body.
pub struct LeCursor<'a> {
    bytes: &'a [u8],
}

impl<'a> LeCursor<'a> {
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Bytes not yet consumed. A codec that requires its input fully consumed
    /// checks this after its last field.
    #[must_use]
    pub const fn remaining(&self) -> &'a [u8] {
        self.bytes
    }

    /// # Errors
    /// [`Truncated`] if fewer than `len` bytes remain.
    pub const fn take(&mut self, len: usize) -> Result<&'a [u8], Truncated> {
        if self.bytes.len() < len {
            return Err(Truncated);
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }

    /// # Errors
    /// [`Truncated`] if the slice is exhausted.
    pub fn u8(&mut self) -> Result<u8, Truncated> {
        Ok(self.take(1)?[0])
    }

    /// # Errors
    /// [`Truncated`] if fewer than 4 bytes remain.
    ///
    /// # Panics
    /// Unreachable: the slice is length-checked before the array conversion.
    pub fn u32(&mut self) -> Result<u32, Truncated> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4B")))
    }

    /// # Errors
    /// [`Truncated`] if fewer than 8 bytes remain.
    ///
    /// # Panics
    /// Unreachable: the slice is length-checked before the array conversion.
    pub fn u64(&mut self) -> Result<u64, Truncated> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8B")))
    }

    /// # Errors
    /// [`Truncated`] if fewer than 16 bytes remain.
    ///
    /// # Panics
    /// Unreachable: the slice is length-checked before the array conversion.
    pub fn u128(&mut self) -> Result<u128, Truncated> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().expect("16B")))
    }
}

/// Split a trailing `XxHash3_64` stamp off an encoded body and verify it.
///
/// Both codecs terminate with the same 8-byte trailer over everything before
/// it. Returns the content with the trailer removed.
///
/// # Errors
/// `Err(None)` when the input is too short to hold a trailer, `Err(Some((expected,
/// actual)))` when the stamp does not match; the caller maps both into its own
/// error enum.
///
/// # Panics
/// Unreachable: the split point is derived from the trailer's own size.
pub fn split_verified_trailer(bytes: &[u8]) -> Result<&[u8], Option<(u64, u64)>> {
    let content_len = bytes.len().checked_sub(size_of::<u64>()).ok_or(None)?;
    let (content, trailer) = bytes.split_at(content_len);
    let expected = u64::from_le_bytes(trailer.try_into().expect("trailer is 8 bytes"));
    let actual = crate::state_manifest::state_artifact_checksum(content);
    if expected == actual {
        Ok(content)
    } else {
        Err(Some((expected, actual)))
    }
}
