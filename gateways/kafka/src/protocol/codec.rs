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

//! Low-level Kafka primitive encoders/decoders (ported wire codec).

#![allow(clippy::missing_const_for_fn, clippy::bool_to_int_with_if)]
#![allow(
    clippy::missing_errors_doc,
    clippy::cast_sign_loss,
    clippy::must_use_candidate,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::cast_lossless
)]

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{KafkaProtocolError, Result};

/// Upper bound for Kafka array/collection element counts decoded from the wire.
/// Matches typical broker limits and prevents OOM from adversarial length prefixes.
pub const MAX_COLLECTION_LEN: usize = 65_536;

pub struct Decoder {
    bytes: Bytes,
}

impl Decoder {
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    pub fn remaining(&self) -> usize {
        self.bytes.remaining()
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        self.ensure(1)?;
        Ok(self.bytes.get_u8())
    }

    pub fn read_i8(&mut self) -> Result<i8> {
        self.ensure(1)?;
        Ok(self.bytes.get_i8())
    }

    pub fn read_i16(&mut self) -> Result<i16> {
        self.ensure(2)?;
        Ok(self.bytes.get_i16())
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        self.ensure(4)?;
        Ok(self.bytes.get_i32())
    }

    pub fn read_i64(&mut self) -> Result<i64> {
        self.ensure(8)?;
        Ok(self.bytes.get_i64())
    }

    pub fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_i8()? != 0)
    }

    /// Unsigned varint (Kafka uses this for compact array lengths and tagged-field counts).
    /// Value is encoded with 7 bits per byte, LSB first; the high bit of each byte signals
    /// that more bytes follow.
    pub fn read_varint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        loop {
            let byte = self.read_u8()?;
            if shift == 63 && byte & 0x7E != 0 {
                return Err(KafkaProtocolError::InvalidVarint);
            }
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 64 {
                return Err(KafkaProtocolError::InvalidVarint);
            }
        }
    }

    /// Legacy array length: signed i32 count (must be non-negative).
    pub fn read_i32_array_count(&mut self) -> Result<usize> {
        let n = self.read_i32()?;
        if n < 0 {
            return Err(KafkaProtocolError::InvalidArrayLength(n));
        }
        // Safe: n is in [0, i32::MAX]; i32::MAX (2_147_483_647) fits in usize
        // on all 32-bit and 64-bit platforms this crate targets.
        let count = n as usize;
        if count > MAX_COLLECTION_LEN {
            return Err(KafkaProtocolError::CollectionTooLarge {
                count,
                max: MAX_COLLECTION_LEN,
            });
        }
        Ok(count)
    }

    /// Compact array length: unsigned varint holding `element_count + 1`.
    /// Per the Kafka spec, varint=0 encodes a null (absent) array; treat as empty (0 elements)
    /// so optional fields like `forgotten_topics` are skipped rather than rejected.
    pub fn read_compact_array_count(&mut self) -> Result<usize> {
        let n = self.read_varint()?;
        if n == 0 {
            return Ok(0);
        }
        let count = usize::try_from(n - 1).map_err(|_| KafkaProtocolError::CollectionTooLarge {
            count: MAX_COLLECTION_LEN + 1,
            max: MAX_COLLECTION_LEN,
        })?;
        if count > MAX_COLLECTION_LEN {
            return Err(KafkaProtocolError::CollectionTooLarge {
                count,
                max: MAX_COLLECTION_LEN,
            });
        }
        Ok(count)
    }

    /// Legacy nullable string: i16 length prefix (-1 = null).
    pub fn read_nullable_string(&mut self) -> Result<Option<String>> {
        let len = self.read_i16()?;
        if len < 0 {
            return Ok(None);
        }
        let len = len as usize;
        self.ensure(len)?;
        let s = std::str::from_utf8(&self.bytes.chunk()[..len])
            .map_err(|_| KafkaProtocolError::InvalidUtf8)?
            .to_owned();
        self.bytes.advance(len);
        Ok(Some(s))
    }

    /// Compact nullable string (flexible versions): varint(len+1) prefix, 0 = null.
    pub fn read_compact_nullable_string(&mut self) -> Result<Option<String>> {
        let len_plus_one = self.read_varint()?;
        if len_plus_one == 0 {
            return Ok(None);
        }
        let len = usize::try_from(len_plus_one - 1).map_err(|_| {
            KafkaProtocolError::CollectionTooLarge {
                count: MAX_COLLECTION_LEN + 1,
                max: MAX_COLLECTION_LEN,
            }
        })?;
        self.ensure(len)?;
        let s = std::str::from_utf8(&self.bytes.chunk()[..len])
            .map_err(|_| KafkaProtocolError::InvalidUtf8)?
            .to_owned();
        self.bytes.advance(len);
        Ok(Some(s))
    }

    /// Legacy nullable bytes: i32 length prefix (-1 = null).
    pub fn read_nullable_bytes(&mut self) -> Result<Option<Bytes>> {
        let len = self.read_i32()?;
        if len < 0 {
            return Ok(None);
        }
        let len = len as usize;
        self.ensure(len)?;
        Ok(Some(self.bytes.copy_to_bytes(len)))
    }

    /// Compact nullable bytes (flexible versions): varint(len+1) prefix, 0 = null.
    pub fn read_compact_nullable_bytes(&mut self) -> Result<Option<Bytes>> {
        let len_plus_one = self.read_varint()?;
        if len_plus_one == 0 {
            return Ok(None);
        }
        let len = usize::try_from(len_plus_one - 1).map_err(|_| {
            KafkaProtocolError::CollectionTooLarge {
                count: MAX_COLLECTION_LEN + 1,
                max: MAX_COLLECTION_LEN,
            }
        })?;
        self.ensure(len)?;
        Ok(Some(self.bytes.copy_to_bytes(len)))
    }

    pub fn read_bytes(&mut self, len: usize) -> Result<Bytes> {
        self.ensure(len)?;
        Ok(self.bytes.copy_to_bytes(len))
    }

    /// Skip over a tagged-fields section.  Each field is: tag (varint) + size (varint) + bytes.
    /// A count of 0 is the common case (single byte 0x00).
    pub fn read_tagged_fields(&mut self) -> Result<()> {
        let count = self.read_varint()?;
        if count > MAX_COLLECTION_LEN as u64 {
            return Err(KafkaProtocolError::CollectionTooLarge {
                count: count as usize,
                max: MAX_COLLECTION_LEN,
            });
        }
        let count = count as usize;

        for _ in 0..count {
            self.read_varint()?; // tag number
            let size = usize::try_from(self.read_varint()?).map_err(|_| {
                KafkaProtocolError::CollectionTooLarge {
                    count: MAX_COLLECTION_LEN + 1,
                    max: MAX_COLLECTION_LEN,
                }
            })?;
            self.ensure(size)?;
            self.bytes.advance(size);
        }
        Ok(())
    }

    fn ensure(&self, needed: usize) -> Result<()> {
        let remaining = self.bytes.remaining();
        if remaining < needed {
            return Err(KafkaProtocolError::BufferUnderflow { needed, remaining });
        }
        Ok(())
    }
}

pub struct Encoder {
    bytes: BytesMut,
}

impl Encoder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: BytesMut::with_capacity(capacity),
        }
    }

    pub fn write_u8(&mut self, v: u8) {
        self.bytes.put_u8(v);
    }

    pub fn write_i8(&mut self, v: i8) {
        self.bytes.put_i8(v);
    }

    pub fn write_i16(&mut self, v: i16) {
        self.bytes.put_i16(v);
    }

    pub fn write_i32(&mut self, v: i32) {
        self.bytes.put_i32(v);
    }

    pub fn write_i64(&mut self, v: i64) {
        self.bytes.put_i64(v);
    }

    pub fn write_bool(&mut self, v: bool) {
        self.write_i8(if v { 1 } else { 0 });
    }

    /// Unsigned varint, 7 bits per byte, LSB first.
    pub fn write_varint(&mut self, mut v: u64) {
        loop {
            let byte = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                self.bytes.put_u8(byte);
                return;
            }
            self.bytes.put_u8(byte | 0x80);
        }
    }

    /// Legacy nullable string: i16 length prefix, -1 for null.
    pub fn write_nullable_string(&mut self, v: Option<&str>) -> Result<()> {
        match v {
            None => self.write_i16(-1),
            Some(s) => {
                if s.len() > i16::MAX as usize {
                    return Err(KafkaProtocolError::StringTooLong { length: s.len() });
                }
                self.write_i16(i16::try_from(s.len()).expect("checked above"));
                self.bytes.put_slice(s.as_bytes());
            }
        }
        Ok(())
    }

    /// Infallible variant for response-encoding paths where the string originated from a decoded
    /// Kafka request and is therefore already bounded to `i16::MAX` bytes.
    pub fn write_nullable_string_unchecked(&mut self, v: Option<&str>) {
        match v {
            None => self.write_i16(-1),
            Some(s) => {
                debug_assert!(i16::try_from(s.len()).is_ok());
                self.write_i16(i16::try_from(s.len()).expect("caller guarantees len <= i16::MAX"));
                self.bytes.put_slice(s.as_bytes());
            }
        }
    }

    /// Compact nullable string (flexible versions): varint(len+1), 0 for null.
    pub fn write_compact_nullable_string(&mut self, v: Option<&str>) {
        match v {
            None => self.write_varint(0),
            Some(s) => {
                self.write_varint((s.len() + 1) as u64);
                self.bytes.put_slice(s.as_bytes());
            }
        }
    }

    /// Write a null bytes field (i32 -1). Infallible; use instead of `write_nullable_bytes(None)`.
    pub fn write_null_bytes(&mut self) {
        self.write_i32(-1);
    }

    /// Legacy nullable bytes: i32 length prefix, -1 for null.
    pub fn write_nullable_bytes(&mut self, v: Option<&[u8]>) -> Result<()> {
        match v {
            None => self.write_i32(-1),
            Some(b) => {
                if b.len() > i32::MAX as usize {
                    return Err(KafkaProtocolError::CollectionTooLarge {
                        count: b.len(),
                        max: i32::MAX as usize,
                    });
                }
                self.write_i32(i32::try_from(b.len()).expect("checked above"));
                self.bytes.put_slice(b);
            }
        }
        Ok(())
    }

    /// Compact nullable bytes (flexible versions): varint(len+1), 0 for null.
    pub fn write_compact_nullable_bytes(&mut self, v: Option<&[u8]>) {
        match v {
            None => self.write_varint(0),
            Some(b) => {
                self.write_varint((b.len() + 1) as u64);
                self.bytes.put_slice(b);
            }
        }
    }

    pub fn write_bytes(&mut self, b: &[u8]) {
        self.bytes.put_slice(b);
    }

    /// Write an empty tagged-fields section (single 0x00 byte).
    pub fn write_empty_tagged_fields(&mut self) {
        self.write_varint(0);
    }
    #[must_use]
    pub fn freeze(self) -> Bytes {
        self.bytes.freeze()
    }
}
