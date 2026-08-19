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

//! Pre-decode bounds guard against unbounded allocation in `kafka_protocol`'s decode path.
//!
//! `kafka_protocol` 0.17 validates every wire-declared array/string/bytes length against zero
//! but never against what remains in the frame (`kafka-protocol-0.17.0/src/protocol/types.rs:988`
//! `Vec::with_capacity(n as usize)` from a wire `Int32`, `:1096` the same from an unsigned
//! varint). A tiny frame that declares a huge count reaches `handle_alloc_error`, which calls
//! `abort()` - not a panic, not catchable, and it takes down every connection on the process, not
//! just the one that sent it. Reproduced: a 20-byte `CreateTopics` v5 frame claiming a huge
//! topics count requests ~481 GB; a 4-byte Metadata v0 frame requests ~143 GiB. No SASL/TLS gates
//! any of `SUPPORTED_RANGES`, so this is reachable by anyone who can `connect()`.
//!
//! This module walks the same field shape `kafka_protocol`'s real decode walks for each of the
//! six accepted message types, but only to validate every length-prefixed field (array count,
//! string length, bytes length, tagged-field size) against what could still fit in the bytes
//! remaining in the frame - it never materializes a value or allocates a collection. Call the
//! matching `validate_*_shape` function before handing the body to `kafka_protocol`.
//!
//! Kept independent of `kafka_protocol`'s own type decoders on purpose: if this walker's field
//! order ever drifts from the real schema, the worst outcome is a false-positive rejection of a
//! legitimate request (caught immediately by the wire-fixture round-trip tests), never a false
//! negative that lets an oversized count back through to the crate.

use bytes::{Buf, Bytes};

use crate::error::{KafkaProtocolError, Result};

/// Same bound the pre-migration hand-rolled codec used: large enough for any real request, small
/// enough that even `MAX_REQUEST_ELEMENTS` identically-sized nested arrays cannot approach a
/// meaningful fraction of memory.
const MAX_COLLECTION_LEN: usize = 65_536;

/// Cumulative cap on the total array elements this guard will walk across one request. A single
/// array's count is capped by [`MAX_COLLECTION_LEN`], but not the product across nested arrays
/// (`topics` x `partitions`) - this budget bounds the sum across the whole frame.
const MAX_REQUEST_ELEMENTS: usize = MAX_COLLECTION_LEN;

struct ShapeCursor {
    bytes: Bytes,
    element_budget: usize,
}

impl ShapeCursor {
    const fn new(body: Bytes) -> Self {
        Self {
            bytes: body,
            element_budget: MAX_REQUEST_ELEMENTS,
        }
    }

    fn remaining(&self) -> usize {
        self.bytes.remaining()
    }

    fn ensure(&self, needed: usize) -> Result<()> {
        let remaining = self.bytes.remaining();
        if remaining < needed {
            return Err(KafkaProtocolError::BufferUnderflow { needed, remaining });
        }
        Ok(())
    }

    fn read_i8(&mut self) -> Result<i8> {
        self.ensure(1)?;
        Ok(self.bytes.get_i8())
    }

    fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_i8()? != 0)
    }

    fn read_i16(&mut self) -> Result<i16> {
        self.ensure(2)?;
        Ok(self.bytes.get_i16())
    }

    fn read_i32(&mut self) -> Result<i32> {
        self.ensure(4)?;
        Ok(self.bytes.get_i32())
    }

    fn read_i64(&mut self) -> Result<i64> {
        self.ensure(8)?;
        Ok(self.bytes.get_i64())
    }

    fn read_varint(&mut self) -> Result<u64> {
        let mut result = 0u64;
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
                return Err(KafkaProtocolError::Malformed(
                    "varint overflows 64 bits".to_string(),
                ));
            }
        }
    }

    fn skip(&mut self, len: usize) -> Result<()> {
        self.ensure(len)?;
        self.bytes.advance(len);
        Ok(())
    }

    fn charge_elements(&mut self, count: usize) -> Result<()> {
        self.element_budget = self.element_budget.checked_sub(count).ok_or_else(|| {
            KafkaProtocolError::Malformed(format!(
                "request element budget exceeded: {count} more requested, {} remaining",
                self.element_budget
            ))
        })?;
        Ok(())
    }

    /// Bounds a just-read array count against [`MAX_COLLECTION_LEN`] and against what could
    /// possibly fit in the remaining bytes - every element needs at least one more byte on the
    /// wire, so `count > remaining()` is already an impossible claim - then debits the cumulative
    /// element budget.
    fn bound_count(&mut self, count: u64) -> Result<usize> {
        let count = usize::try_from(count).map_err(|_| {
            KafkaProtocolError::Malformed(format!(
                "collection length {count} exceeds maximum {MAX_COLLECTION_LEN}"
            ))
        })?;
        if count > MAX_COLLECTION_LEN || count > self.remaining() {
            return Err(KafkaProtocolError::Malformed(format!(
                "collection length {count} exceeds maximum {MAX_COLLECTION_LEN} or remaining {} bytes",
                self.remaining()
            )));
        }
        self.charge_elements(count)?;
        Ok(count)
    }

    fn legacy_array_count(&mut self) -> Result<usize> {
        let n = self.read_i32()?;
        if n < 0 {
            return Err(KafkaProtocolError::Malformed(format!(
                "invalid array length: {n}"
            )));
        }
        self.bound_count(u64::from(n.unsigned_abs()))
    }

    fn legacy_array_count_nullable(&mut self) -> Result<usize> {
        let n = self.read_i32()?;
        if n == -1 {
            return Ok(0);
        }
        if n < 0 {
            return Err(KafkaProtocolError::Malformed(format!(
                "invalid array length: {n}"
            )));
        }
        self.bound_count(u64::from(n.unsigned_abs()))
    }

    fn compact_array_count(&mut self) -> Result<usize> {
        let n = self.read_varint()?;
        if n == 0 {
            return Err(KafkaProtocolError::Malformed(
                "null compact array where a non-null array is required".to_string(),
            ));
        }
        self.bound_count(n - 1)
    }

    fn compact_array_count_nullable(&mut self) -> Result<usize> {
        let n = self.read_varint()?;
        if n == 0 {
            return Ok(0);
        }
        self.bound_count(n - 1)
    }

    /// Legacy string/bytes length caps at `i16`/`i32`, but a non-nullable field with `-1` (or a
    /// nullable field encoded non-null-but-negative) is malformed either way.
    fn legacy_string(&mut self, nullable: bool) -> Result<()> {
        let len = self.read_i16()?;
        if len < 0 {
            return if nullable {
                Ok(())
            } else {
                Err(KafkaProtocolError::Malformed(
                    "null string where a non-null string is required".to_string(),
                ))
            };
        }
        // Safe: len is in [0, i16::MAX], checked above.
        self.skip(len.unsigned_abs().into())
    }

    /// Compact string length is otherwise bounded only by the frame; cap it at
    /// `MAX_COLLECTION_LEN` for parity with the legacy `i16`-length form.
    fn compact_string(&mut self, nullable: bool) -> Result<()> {
        let n = self.read_varint()?;
        if n == 0 {
            return if nullable {
                Ok(())
            } else {
                Err(KafkaProtocolError::Malformed(
                    "null compact string where a non-null string is required".to_string(),
                ))
            };
        }
        let len = usize::try_from(n - 1).map_err(|_| {
            KafkaProtocolError::Malformed(format!(
                "collection length {n} exceeds maximum {MAX_COLLECTION_LEN}"
            ))
        })?;
        if len > MAX_COLLECTION_LEN || len > self.remaining() {
            return Err(KafkaProtocolError::Malformed(format!(
                "collection length {len} exceeds maximum {MAX_COLLECTION_LEN} or remaining {} bytes",
                self.remaining()
            )));
        }
        self.skip(len)
    }

    /// Raw bytes (Produce `records`) can legitimately be large - bounded only by what remains in
    /// the frame, not `MAX_COLLECTION_LEN`.
    fn legacy_bytes(&mut self, nullable: bool) -> Result<()> {
        let len = self.read_i32()?;
        if len < 0 {
            return if nullable {
                Ok(())
            } else {
                Err(KafkaProtocolError::Malformed(
                    "null bytes where non-null bytes are required".to_string(),
                ))
            };
        }
        // Safe: len is in [0, i32::MAX], checked above.
        self.skip(len.unsigned_abs() as usize)
    }

    fn compact_bytes(&mut self, nullable: bool) -> Result<()> {
        let n = self.read_varint()?;
        if n == 0 {
            return if nullable {
                Ok(())
            } else {
                Err(KafkaProtocolError::Malformed(
                    "null bytes where non-null bytes are required".to_string(),
                ))
            };
        }
        let len = usize::try_from(n - 1).map_err(|_| {
            KafkaProtocolError::Malformed(format!(
                "collection length {n} exceeds remaining {} bytes",
                self.remaining()
            ))
        })?;
        self.skip(len)
    }

    fn tagged_fields(&mut self) -> Result<()> {
        let count = self.read_varint()?;
        let count = usize::try_from(count).map_err(|_| {
            KafkaProtocolError::Malformed(format!(
                "collection length {count} exceeds maximum {MAX_COLLECTION_LEN}"
            ))
        })?;
        if count > MAX_COLLECTION_LEN {
            return Err(KafkaProtocolError::Malformed(format!(
                "collection length {count} exceeds maximum {MAX_COLLECTION_LEN}"
            )));
        }
        for _ in 0..count {
            let _tag = self.read_varint()?;
            let size = self.read_varint()?;
            let size = usize::try_from(size).map_err(|_| {
                KafkaProtocolError::Malformed(format!(
                    "tagged field size {size} exceeds remaining {} bytes",
                    self.remaining()
                ))
            })?;
            self.skip(size)?;
        }
        Ok(())
    }
}

/// Mirrors the field order `ProduceRequest::decode` walks for v3+ (the only versions this guard
/// is called for - v0-2 never reach `kafka_protocol` decode, see `api::handle_produce_request`).
///
/// # Errors
///
/// Returns an error when a declared array/string/bytes length cannot fit in the bytes remaining
/// in the frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_produce_shape(version: i16, body: &Bytes) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone());
    let flexible = version >= 9;

    if flexible {
        c.compact_string(true)?;
    } else {
        c.legacy_string(true)?;
    }
    let _acks = c.read_i16()?;
    let _timeout_ms = c.read_i32()?;

    let topics_count = if flexible {
        c.compact_array_count()?
    } else {
        c.legacy_array_count()?
    };
    for _ in 0..topics_count {
        if flexible {
            c.compact_string(false)?;
        } else {
            c.legacy_string(false)?;
        }
        let partitions_count = if flexible {
            c.compact_array_count()?
        } else {
            c.legacy_array_count()?
        };
        for _ in 0..partitions_count {
            let _partition = c.read_i32()?;
            if flexible {
                c.compact_bytes(true)?;
            } else {
                c.legacy_bytes(true)?;
            }
            if flexible {
                c.tagged_fields()?;
            }
        }
        if flexible {
            c.tagged_fields()?;
        }
    }
    if flexible {
        c.tagged_fields()?;
    }
    Ok(())
}

/// Mirrors the field order `FetchRequest::decode` walks.
///
/// # Errors
///
/// Returns an error when a declared array/string/bytes length cannot fit in the bytes remaining
/// in the frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_fetch_shape(version: i16, body: &Bytes) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone());
    let flexible = version >= 12;

    let _replica_id = c.read_i32()?;
    let _max_wait_ms = c.read_i32()?;
    let _min_bytes = c.read_i32()?;
    if version >= 3 {
        let _max_bytes = c.read_i32()?;
    }
    if version >= 4 {
        let _isolation_level = c.read_i8()?;
    }
    if version >= 7 {
        let _session_id = c.read_i32()?;
        let _session_epoch = c.read_i32()?;
    }

    let topics_count = if flexible {
        c.compact_array_count()?
    } else {
        c.legacy_array_count()?
    };
    for _ in 0..topics_count {
        if flexible {
            c.compact_string(false)?;
        } else {
            c.legacy_string(false)?;
        }
        let partitions_count = if flexible {
            c.compact_array_count()?
        } else {
            c.legacy_array_count()?
        };
        for _ in 0..partitions_count {
            let _partition = c.read_i32()?;
            if version >= 9 {
                let _current_leader_epoch = c.read_i32()?;
            }
            let _fetch_offset = c.read_i64()?;
            if version >= 12 {
                let _last_fetched_epoch = c.read_i32()?;
            }
            if version >= 5 {
                let _log_start_offset = c.read_i64()?;
            }
            let _partition_max_bytes = c.read_i32()?;
            if flexible {
                c.tagged_fields()?;
            }
        }
        if flexible {
            c.tagged_fields()?;
        }
    }

    if version >= 7 {
        let forgotten_count = if flexible {
            c.compact_array_count_nullable()?
        } else {
            c.legacy_array_count_nullable()?
        };
        for _ in 0..forgotten_count {
            if flexible {
                c.compact_string(true)?;
                let partitions_count = c.compact_array_count()?;
                for _ in 0..partitions_count {
                    let _partition = c.read_i32()?;
                }
                c.tagged_fields()?;
            } else {
                c.legacy_string(true)?;
                let partitions_count = c.legacy_array_count()?;
                for _ in 0..partitions_count {
                    let _partition = c.read_i32()?;
                }
            }
        }
    }

    if version >= 11 {
        if flexible {
            c.compact_string(true)?;
        } else {
            c.legacy_string(true)?;
        }
    }
    if flexible {
        c.tagged_fields()?;
    }
    Ok(())
}

/// Mirrors the field order `ListOffsetsRequest::decode` walks.
///
/// # Errors
///
/// Returns an error when a declared array/string/bytes length cannot fit in the bytes remaining
/// in the frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_list_offsets_shape(version: i16, body: &Bytes) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone());
    let flexible = version >= 6;

    let _replica_id = c.read_i32()?;
    if version >= 2 {
        let _isolation_level = c.read_i8()?;
    }

    let topics_count = if flexible {
        c.compact_array_count()?
    } else {
        c.legacy_array_count()?
    };
    for _ in 0..topics_count {
        if flexible {
            c.compact_string(false)?;
        } else {
            c.legacy_string(false)?;
        }
        let partitions_count = if flexible {
            c.compact_array_count()?
        } else {
            c.legacy_array_count()?
        };
        for _ in 0..partitions_count {
            let _partition = c.read_i32()?;
            if version >= 4 {
                let _current_leader_epoch = c.read_i32()?;
            }
            let _timestamp = c.read_i64()?;
            if version == 0 {
                let _max_num_offsets = c.read_i32()?;
            }
            if flexible {
                c.tagged_fields()?;
            }
        }
        if flexible {
            c.tagged_fields()?;
        }
    }
    if flexible {
        c.tagged_fields()?;
    }
    Ok(())
}

/// Mirrors the field order `CreateTopicsRequest::decode` walks.
///
/// # Errors
///
/// Returns an error when a declared array/string/bytes length cannot fit in the bytes remaining
/// in the frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_create_topics_shape(version: i16, body: &Bytes) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone());
    let flexible = version >= 5;

    let topics_count = if flexible {
        c.compact_array_count()?
    } else {
        c.legacy_array_count()?
    };
    for _ in 0..topics_count {
        if flexible {
            c.compact_string(false)?;
        } else {
            c.legacy_string(false)?;
        }
        let _num_partitions = c.read_i32()?;
        let _replication_factor = c.read_i16()?;

        let assignments_count = if flexible {
            c.compact_array_count()?
        } else {
            c.legacy_array_count()?
        };
        for _ in 0..assignments_count {
            let _partition_index = c.read_i32()?;
            let replicas_count = if flexible {
                c.compact_array_count()?
            } else {
                c.legacy_array_count()?
            };
            for _ in 0..replicas_count {
                let _broker_id = c.read_i32()?;
            }
            if flexible {
                c.tagged_fields()?;
            }
        }

        let configs_count = if flexible {
            c.compact_array_count()?
        } else {
            c.legacy_array_count()?
        };
        for _ in 0..configs_count {
            if flexible {
                c.compact_string(true)?;
                c.compact_string(true)?;
                c.tagged_fields()?;
            } else {
                c.legacy_string(true)?;
                c.legacy_string(true)?;
            }
        }

        if flexible {
            c.tagged_fields()?;
        }
    }

    let _timeout_ms = c.read_i32()?;
    if version >= 1 {
        let _validate_only = c.read_bool()?;
    }
    if flexible {
        c.tagged_fields()?;
    }
    Ok(())
}

/// Mirrors the field order `MetadataRequest::decode` walks.
///
/// # Errors
///
/// Returns an error when a declared array/string/bytes length cannot fit in the bytes remaining
/// in the frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_metadata_shape(version: i16, body: &Bytes) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone());
    let flexible = version >= 9;

    let topics_count = if flexible {
        c.compact_array_count_nullable()?
    } else {
        c.legacy_array_count_nullable()?
    };
    for _ in 0..topics_count {
        if flexible && version >= 10 {
            c.skip(16)?; // topic_id: 16-byte UUID before name
        }
        if flexible {
            c.compact_string(true)?;
        } else {
            c.legacy_string(true)?;
        }
        if flexible {
            c.tagged_fields()?;
        }
    }

    if version >= 4 {
        let _allow_auto_topic_creation = c.read_bool()?;
    }
    if (8..=10).contains(&version) {
        let _include_cluster_authorized_operations = c.read_bool()?;
    }
    if version >= 8 {
        let _include_topic_authorized_operations = c.read_bool()?;
    }
    if flexible {
        c.tagged_fields()?;
    }
    Ok(())
}

/// Mirrors the field order `ApiVersionsRequest::decode` walks. v0-2 have an empty body (no
/// length-prefixed fields to bound), so this is a no-op below v3.
///
/// # Errors
///
/// Returns an error when a declared string length cannot fit in the bytes remaining in the
/// frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_api_versions_shape(version: i16, body: &Bytes) -> Result<()> {
    if version < 3 {
        return Ok(());
    }
    let mut c = ShapeCursor::new(body.clone());
    c.compact_string(false)?;
    c.compact_string(false)?;
    c.tagged_fields()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two payloads reproduced in review, verbatim: both must be rejected before reaching
    /// `kafka_protocol`, not merely rejected eventually.
    #[test]
    fn create_topics_v5_huge_topics_count_rejected() {
        let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert!(validate_create_topics_shape(5, &body).is_err());
    }

    #[test]
    fn metadata_v0_huge_topics_count_rejected() {
        let body = Bytes::from_static(&[0x7F, 0xFF, 0xFF, 0xFF]);
        assert!(validate_metadata_shape(0, &body).is_err());
    }

    #[test]
    fn metadata_v0_null_array_all_topics_accepted() {
        let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]); // -1: all topics
        assert!(validate_metadata_shape(0, &body).is_ok());
    }

    #[test]
    fn produce_v3_well_formed_empty_topics_accepted() {
        let mut body = Vec::new();
        body.extend_from_slice(&(-1i16).to_be_bytes()); // null transactional_id
        body.extend_from_slice(&1i16.to_be_bytes()); // acks
        body.extend_from_slice(&1000i32.to_be_bytes()); // timeout_ms
        body.extend_from_slice(&0i32.to_be_bytes()); // empty topics
        assert!(validate_produce_shape(3, &Bytes::from(body)).is_ok());
    }

    #[test]
    fn fetch_v4_huge_topics_count_rejected() {
        let mut body = Vec::new();
        body.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
        body.extend_from_slice(&100i32.to_be_bytes()); // max_wait_ms
        body.extend_from_slice(&1i32.to_be_bytes()); // min_bytes
        body.extend_from_slice(&1024i32.to_be_bytes()); // max_bytes (v3+)
        body.push(0); // isolation_level (v4+)
        body.extend_from_slice(&i32::MAX.to_be_bytes()); // topics count: impossible
        assert!(validate_fetch_shape(4, &Bytes::from(body)).is_err());
    }

    #[test]
    fn list_offsets_v6_flexible_huge_topics_count_rejected() {
        let mut body = Vec::new();
        body.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
        body.push(0); // isolation_level
        body.push(0xFF); // varint continuation bytes claiming a huge count
        body.push(0xFF);
        body.push(0xFF);
        body.push(0xFF);
        body.push(0x0F);
        assert!(validate_list_offsets_shape(6, &Bytes::from(body)).is_err());
    }

    #[test]
    fn api_versions_v0_empty_body_accepted() {
        assert!(validate_api_versions_shape(0, &Bytes::new()).is_ok());
    }

    #[test]
    fn api_versions_v3_truncated_rejected() {
        assert!(validate_api_versions_shape(3, &Bytes::new()).is_err());
    }
}
