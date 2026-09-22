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
//! Kept independent of `kafka_protocol`'s own type decoders on purpose, so this walker doesn't
//! inherit a bug from the thing it's guarding against. That independence only delivers a
//! false-positive-only guarantee (a legitimate request gets rejected - caught immediately by the
//! wire-fixture round-trip tests - rather than an oversized count sneaking through) if every
//! primitive reader here consumes the *exact same byte count* the real decoder would for the
//! same field, not merely fields in the same order: a reader that eats more or fewer bytes than
//! the crate's for one field desyncs every field after it, so a small value validated here can
//! correspond to a huge one at the decoder's real (different) position. `read_varint` below
//! documents a real instance of this class of bug and how it's avoided.

use bytes::{Buf, Bytes};

use crate::error::{KafkaProtocolError, Result};

/// Per-array cap: large enough that no single legitimate array (e.g. a topic list) is rejected,
/// small enough that a `Vec::with_capacity` sized from it is trivial memory regardless of element
/// type.
const MAX_COLLECTION_LEN: usize = 65_536;

/// Cumulative cap on the total array elements this guard will walk across one request -
/// deliberately much smaller than [`MAX_COLLECTION_LEN`]. A single array's count is capped by
/// `MAX_COLLECTION_LEN`, but not the *product* across nested arrays (`topics` x `partitions`),
/// and it was that product - not any single array - that let a 393 KB Produce v9 request
/// (1 topic x 65,535 partitions, all within the old 65,536 cumulative budget) produce a
/// 2.16 MB response (5.5x) built by a single synchronous, non-yielding call with no `.await`
/// anywhere in the decode-then-encode path. That's not a crash: it's 10+ ms of one tokio worker
/// thread fully occupied per request, and every worker can be saturated with as few connections
/// as there are worker threads (default = core count), stalling every other connection's I/O
/// while it happens. 4,096 keeps the same request's response under ~135 KB - a small fraction of
/// the default 8 MiB `max_frame_size` - while remaining generous for any real client (a single
/// request with 4,096 total array elements across every nested array combined is already an
/// unusually large batch).
const MAX_REQUEST_ELEMENTS: usize = 4_096;

/// Conservative per-element upper bound on how many bytes one array entry can add to the
/// response, used only to reject before decode when the *projected* response size would exceed
/// the connection's `max_frame_size` - a second, independent bound from [`MAX_REQUEST_ELEMENTS`]
/// alone, since [`MAX_REQUEST_ELEMENTS`] bounds element *count*, not response *bytes*, and a
/// request with few elements but very long echoed strings (e.g. topic names, each individually
/// valid up to [`MAX_COLLECTION_LEN`]) could still be small in count and large in bytes.
/// Measured: `encode_produce_response`'s largest fixed per-partition response entry is ~33 bytes
/// (`index` + `error_code` + `base_offset` + `log_append_time` + `log_start_offset` + empty
/// `record_errors`/`error_message` + tagged field); 64 stays comfortably above every message
/// type's fixed per-element cost without needing a bespoke weight per type.
const RESPONSE_BYTES_PER_ELEMENT: usize = 64;

struct ShapeCursor {
    bytes: Bytes,
    element_budget: usize,
    /// Running total of estimated response bytes this request could produce, charged as the
    /// walk encounters elements (`charge_elements`) and echoed strings (`compact_string`/
    /// `legacy_string` - never bytes fields, since `records`/config values are never echoed back
    /// but can legitimately be large). Never decreases; checked against `max_response_bytes`
    /// on every charge so an oversized projection fails fast instead of finishing the walk.
    estimated_response_bytes: usize,
    max_response_bytes: usize,
}

impl ShapeCursor {
    const fn new(body: Bytes, max_response_bytes: usize) -> Self {
        Self {
            bytes: body,
            element_budget: MAX_REQUEST_ELEMENTS,
            estimated_response_bytes: 0,
            max_response_bytes,
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

    /// Byte-for-byte the same algorithm `kafka_protocol`'s `UnsignedVarInt::decode` uses
    /// (`kafka-protocol-0.17.0/src/protocol/types.rs:119-130`): exactly 5 bytes, `u32`
    /// arithmetic, and no error if the 5th byte still has its continuation bit set - the crate
    /// silently stops and returns whatever accumulated in those 5 bytes.
    ///
    /// This is not a stylistic match, it's load-bearing: an earlier version of this cursor read
    /// up to 10 bytes with a `shift >= 64` overflow check, which is the textbook-correct general
    /// LEB128 reader but *reads more bytes than the real decoder does* for any varint with 6-10
    /// continuation-bit bytes. That desyncs this cursor's position from the crate's for every
    /// field after it - the crate reads its own (differently-positioned) bytes for the next
    /// count field, so a small value validated here can correspond to a huge one there. That
    /// class of bug is exactly the false negative this module's doc claims can't happen; the
    /// guarantee only holds if every field here consumes the identical byte count the real
    /// decoder would.
    fn read_varint(&mut self) -> Result<u64> {
        let mut value: u32 = 0;
        for i in 0..5 {
            self.ensure(1)?;
            let byte = self.bytes.get_u8();
            value |= u32::from(byte & 0x7F) << (i * 7);
            if byte < 0x80 {
                break;
            }
        }
        Ok(u64::from(value))
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
        self.charge_response_bytes(count.saturating_mul(RESPONSE_BYTES_PER_ELEMENT))
    }

    /// Adds `bytes` to the running projected-response-size estimate, rejecting once it would
    /// exceed `max_response_bytes`. See [`RESPONSE_BYTES_PER_ELEMENT`]'s doc for why this exists
    /// alongside, not instead of, the element-count budget.
    fn charge_response_bytes(&mut self, bytes: usize) -> Result<()> {
        self.estimated_response_bytes = self.estimated_response_bytes.saturating_add(bytes);
        if self.estimated_response_bytes > self.max_response_bytes {
            return Err(KafkaProtocolError::Malformed(format!(
                "projected response size {} exceeds max_frame_size {}",
                self.estimated_response_bytes, self.max_response_bytes
            )));
        }
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
    /// Charges the string's own length against the projected-response-size budget, not just its
    /// element slot: every message type this guard covers echoes request strings back in its
    /// response somewhere (topic names, at minimum), so a request with few array elements but
    /// very long strings could still bypass the count-only [`Self::charge_elements`] check.
    /// Deliberately blanket - it also charges strings that are never echoed (e.g. `CreateTopics`
    /// config values), which can only ever reject a legitimate request more eagerly than
    /// necessary, never let an amplifying one through.
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
        let len: usize = len.unsigned_abs().into();
        self.charge_response_bytes(len)?;
        self.skip(len)
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
        self.charge_response_bytes(len)?;
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
pub fn validate_produce_shape(version: i16, body: &Bytes, max_frame_size: usize) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), max_frame_size);
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
pub fn validate_fetch_shape(version: i16, body: &Bytes, max_frame_size: usize) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), max_frame_size);
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
pub fn validate_list_offsets_shape(
    version: i16,
    body: &Bytes,
    max_frame_size: usize,
) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), max_frame_size);
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
pub fn validate_create_topics_shape(
    version: i16,
    body: &Bytes,
    max_frame_size: usize,
) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), max_frame_size);
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
pub fn validate_metadata_shape(version: i16, body: &Bytes, max_frame_size: usize) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), max_frame_size);
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
    // No response-size guard needed: `encode_api_versions_response` takes only
    // `(api_version, error_code)` - the decoded `client_software_name`/`_version` never reach
    // the response, so nothing here can amplify. `usize::MAX` disables the (harmless but
    // pointless) check rather than plumbing `max_frame_size` through for no effect.
    let mut c = ShapeCursor::new(body.clone(), usize::MAX);
    c.compact_string(false)?;
    c.compact_string(false)?;
    c.tagged_fields()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

    /// The two payloads reproduced in review, verbatim: both must be rejected before reaching
    /// `kafka_protocol`, not merely rejected eventually.
    #[test]
    fn create_topics_v5_huge_topics_count_rejected() {
        let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert!(validate_create_topics_shape(5, &body, TEST_MAX_FRAME_SIZE).is_err());
    }

    #[test]
    fn metadata_v0_huge_topics_count_rejected() {
        let body = Bytes::from_static(&[0x7F, 0xFF, 0xFF, 0xFF]);
        assert!(validate_metadata_shape(0, &body, TEST_MAX_FRAME_SIZE).is_err());
    }

    #[test]
    fn metadata_v0_null_array_all_topics_accepted() {
        let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]); // -1: all topics
        assert!(validate_metadata_shape(0, &body, TEST_MAX_FRAME_SIZE).is_ok());
    }

    /// Reviewed POC: a 5-byte varint whose first byte is a 10-byte-reader-only artifact (`0x81`
    /// followed by eight `0x80` continuation bytes then a `0x00` terminator) used to desync this
    /// cursor from `kafka_protocol`'s actual 5-byte-max `UnsignedVarInt::decode` - this guard
    /// would consume all 10 bytes as one (small) varint while the crate consumes only the first
    /// 5, leaving the crate to read `FF FF FF FF 0F` (`u32::MAX`) as the real topics count a few
    /// bytes later, an allocation the guard never saw coming because it was validating the wrong
    /// bytes. `read_varint` now reads at most 5 bytes, exactly matching the crate, so this frame
    /// must be rejected here - not accepted here and crash 5 bytes further into the real decode.
    #[test]
    fn produce_v9_desync_varint_poc_rejected() {
        let body = Bytes::from_static(&[
            0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
            0x00, // 10-byte over-long varint
            0x00, // acks high byte (never reached by the fixed reader)
            0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // u32::MAX topics count, 5-byte varint
            0x01, 0x00,
        ]);
        assert!(validate_produce_shape(9, &body, TEST_MAX_FRAME_SIZE).is_err());
    }

    /// Same desync class as `produce_v9_desync_varint_poc_rejected`, via `CreateTopics`'s own
    /// `compact_string`/`compact_array_count` path.
    #[test]
    fn create_topics_v5_desync_varint_poc_rejected() {
        let body = Bytes::from_static(&[
            0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
            0x00, // 10-byte over-long varint
            0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // u32::MAX topics count, 5-byte varint
        ]);
        assert!(validate_create_topics_shape(5, &body, TEST_MAX_FRAME_SIZE).is_err());
    }

    /// Reviewed POC (C2): 1 topic x 65,535 partitions was within the *old* 65,536 cumulative
    /// budget - no single array count exceeded `MAX_COLLECTION_LEN`, only their product did -
    /// and drove a 393 KB request to a 2.16 MB response built by one synchronous,
    /// non-yielding call. Pins the fix: the same shape, sized to exceed the new 4,096 budget by
    /// one, must be rejected by the cumulative element charge specifically - filler bytes are
    /// present so the earlier "count exceeds remaining bytes" check can't short-circuit before
    /// the budget check runs.
    #[test]
    fn element_budget_rejects_cumulative_amplification_poc() {
        let mut body = Vec::new();
        body.push(0x00); // compact null transactional_id
        body.extend_from_slice(&1i16.to_be_bytes()); // acks
        body.extend_from_slice(&1000i32.to_be_bytes()); // timeout_ms
        body.push(0x02); // one topic (N+1=2)
        body.push(0x02); // topic name length 1 (N+1=2)
        body.push(b't');
        // partitions count varint(4098) = 4097 partitions: one more than the whole request's
        // remaining budget (4096 - 1 already charged for the topic = 4095).
        body.extend_from_slice(&[0x82, 0x20]);
        body.extend(std::iter::repeat_n(0u8, 4097)); // filler, not parsed as elements
        assert!(validate_produce_shape(9, &Bytes::from(body), TEST_MAX_FRAME_SIZE).is_err());
    }

    /// Companion to the element-budget POC above: few elements (well within 4,096) but one
    /// echoed string long enough alone to blow a small `max_frame_size` - the element-count
    /// budget has nothing to say about this shape, only the response-size projection catches
    /// it. Confirms the two guards are covering genuinely different amplification vectors, not
    /// duplicating each other.
    #[test]
    fn response_size_guard_rejects_long_echoed_string_within_element_budget() {
        let mut body = Vec::new();
        body.push(0x02); // one topic (N+1=2)
        let name_len = 2000usize;
        body.extend_from_slice(&[0xD1, 0x0F]); // compact string length varint(2001) = 2000 bytes
        body.extend(std::iter::repeat_n(b'x', name_len));
        body.extend_from_slice(&3i32.to_be_bytes()); // num_partitions
        body.extend_from_slice(&1i16.to_be_bytes()); // replication_factor
        body.push(0x01); // empty assignments (N+1=1)
        body.push(0x01); // empty configs (N+1=1)
        body.push(0x00); // topic tagged fields (empty)
        body.extend_from_slice(&5000i32.to_be_bytes()); // timeout_ms
        body.push(0x01); // validate_only = true
        body.push(0x00); // request tagged fields (empty)

        let small_max_frame = 1000; // smaller than the 2,000-byte topic name alone
        assert!(validate_create_topics_shape(5, &Bytes::from(body), small_max_frame).is_err());
    }

    /// `read_varint` must stop after exactly 5 bytes even when the 5th byte still has its
    /// continuation bit set (`kafka_protocol`'s own behavior - see `read_varint`'s doc) rather
    /// than erroring or reading further; the 6th+ bytes belong to whatever field comes next.
    #[test]
    fn read_varint_stops_at_five_bytes_matching_kafka_protocol() {
        let mut c = ShapeCursor::new(
            Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2A]),
            TEST_MAX_FRAME_SIZE,
        );
        let value = c
            .read_varint()
            .expect("5-byte varint with trailing continuation bit");
        assert_eq!(
            value, 0xFFFF_FFFF,
            "must match kafka_protocol's own u32 truncation"
        );
        assert_eq!(
            c.remaining(),
            1,
            "must consume exactly 5 bytes, leaving the 6th for the next field"
        );
    }

    #[test]
    fn produce_v3_well_formed_empty_topics_accepted() {
        let mut body = Vec::new();
        body.extend_from_slice(&(-1i16).to_be_bytes()); // null transactional_id
        body.extend_from_slice(&1i16.to_be_bytes()); // acks
        body.extend_from_slice(&1000i32.to_be_bytes()); // timeout_ms
        body.extend_from_slice(&0i32.to_be_bytes()); // empty topics
        assert!(validate_produce_shape(3, &Bytes::from(body), TEST_MAX_FRAME_SIZE).is_ok());
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
        assert!(validate_fetch_shape(4, &Bytes::from(body), TEST_MAX_FRAME_SIZE).is_err());
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
        assert!(validate_list_offsets_shape(6, &Bytes::from(body), TEST_MAX_FRAME_SIZE).is_err());
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
