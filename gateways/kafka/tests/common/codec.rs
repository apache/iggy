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

//! Minimal Kafka primitive encoder/decoder for test wire fixtures.
//!
//! This is test-only scaffolding, not the gateway's production codec (that decoding/encoding
//! is `kafka_protocol`'s job - see `src/protocol/responses.rs`). It exists here because tests
//! need to hand-build wire bytes `kafka_protocol`'s spec-correct encoder cannot produce
//! (legacy/malformed/version-boundary shapes) and to read gateway response bytes back out at
//! the primitive level, independent of whichever crate encoded them.
#![allow(dead_code, clippy::cast_sign_loss, clippy::missing_const_for_fn)]

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub type Result<T> = std::result::Result<T, String>;

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

    pub fn read_bool(&mut self) -> Result<bool> {
        self.ensure(1)?;
        Ok(self.bytes.get_i8() != 0)
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

    /// Unsigned varint (Kafka uses this for compact array lengths and tagged-field counts).
    pub fn read_varint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        loop {
            self.ensure(1)?;
            let byte = self.bytes.get_u8();
            result |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 64 {
                return Err("varint overflows 64 bits".into());
            }
        }
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
            .map_err(|e| e.to_string())?
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
        let len = usize::try_from(len_plus_one - 1).map_err(|e| e.to_string())?;
        self.ensure(len)?;
        let s = std::str::from_utf8(&self.bytes.chunk()[..len])
            .map_err(|e| e.to_string())?
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

    pub fn read_bytes(&mut self, len: usize) -> Result<Bytes> {
        self.ensure(len)?;
        Ok(self.bytes.copy_to_bytes(len))
    }

    /// Skip over a tagged-fields section: tag (varint) + size (varint) + bytes, repeated.
    pub fn read_tagged_fields(&mut self) -> Result<()> {
        let count = self.read_varint()?;
        for _ in 0..count {
            self.read_varint()?; // tag number
            let size = usize::try_from(self.read_varint()?).map_err(|e| e.to_string())?;
            self.ensure(size)?;
            self.bytes.advance(size);
        }
        Ok(())
    }

    fn ensure(&self, needed: usize) -> Result<()> {
        let remaining = self.bytes.remaining();
        if remaining < needed {
            return Err(format!(
                "buffer underflow: needed {needed}, remaining {remaining}"
            ));
        }
        Ok(())
    }

    /// Decode a Metadata v1 (legacy, non-flexible) response's broker/controller prefix followed
    /// by `expected_count` topics, asserting each topic's error code against `expected_error`
    /// and an empty partitions array, and returning the echoed topic names in order.
    ///
    /// Shared by `api_handler_tests.rs` (in-process) and `server_e2e_tests.rs` (TCP e2e) so the
    /// two layers decode Metadata v1 identically instead of maintaining separate hand-rolled
    /// copies that could silently drift apart.
    pub fn read_metadata_v1_topics(
        &mut self,
        expected_count: i32,
        expected_error: i16,
    ) -> Vec<String> {
        let _brokers_count = self.read_i32().unwrap();
        self.read_i32().unwrap(); // node_id
        self.read_nullable_string().unwrap(); // host
        self.read_i32().unwrap(); // port
        self.read_nullable_string().unwrap(); // rack (v1+)
        self.read_i32().unwrap(); // controller_id (v1+)

        assert_eq!(self.read_i32().unwrap(), expected_count);
        let mut names = Vec::with_capacity(usize::try_from(expected_count).unwrap_or(0));
        for _ in 0..expected_count {
            assert_eq!(
                self.read_i16().unwrap(),
                expected_error,
                "unexpected topic error code"
            );
            names.push(self.read_nullable_string().unwrap().expect("topic name"));
            self.read_bool().unwrap(); // is_internal (v1+)
            assert_eq!(self.read_i32().unwrap(), 0, "empty partitions array");
        }
        names
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

    pub fn write_bool(&mut self, v: bool) {
        self.write_i8(i8::from(v));
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
                self.write_i16(i16::try_from(s.len()).map_err(|e| e.to_string())?);
                self.bytes.put_slice(s.as_bytes());
            }
        }
        Ok(())
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

    /// Legacy nullable bytes: i32 length prefix, -1 for null.
    pub fn write_nullable_bytes(&mut self, v: Option<&[u8]>) -> Result<()> {
        match v {
            None => self.write_i32(-1),
            Some(b) => {
                self.write_i32(i32::try_from(b.len()).map_err(|e| e.to_string())?);
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

    pub fn freeze(self) -> Bytes {
        self.bytes.freeze()
    }
}
