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

//! Encode and decode helpers over `kafka_protocol`'s `Encodable` and `Decodable`.

use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::MetadataRequest;
use kafka_protocol::protocol::{Decodable, Encodable};

use crate::error::{KafkaWireError, Result};

/// Encode a `kafka_protocol` message, mapping its `anyhow::Error` (the crate has no stable
/// decode/encode error taxonomy) to a variant a host can log or answer by closing the connection.
///
/// # Errors
///
/// [`KafkaWireError::Malformed`] when `kafka_protocol` refuses to encode `msg` at `version`.
pub fn encode_message<T: Encodable>(msg: &T, version: i16, capacity: usize) -> Result<Bytes> {
    let mut buf = BytesMut::with_capacity(capacity);
    msg.encode(&mut buf, version)
        .map_err(|e| KafkaWireError::Malformed(e.to_string()))?;
    Ok(buf.freeze())
}

/// Decode `T` from the whole request body and reject unconsumed trailing bytes.
///
/// `kafka_protocol`'s `Decodable` stops once it has read the fields its schema defines; it does
/// not know (or care) whether the caller handed it an exact-length body, so the trailing-bytes
/// check has to live here.
///
/// # Errors
///
/// [`KafkaWireError::Malformed`] when the body does not decode at `version` or leaves bytes
/// behind.
pub fn decode_exhaustive<T: Decodable>(version: i16, mut body: Bytes) -> Result<T> {
    let value =
        T::decode(&mut body, version).map_err(|e| KafkaWireError::Malformed(e.to_string()))?;
    if body.has_remaining() {
        return Err(KafkaWireError::Malformed(
            "unexpected trailing bytes in request body".to_string(),
        ));
    }
    Ok(value)
}

/// Runs `validate` (see [`crate::bounds_guard`]) before [`decode_exhaustive`].
///
/// `kafka_protocol` validates wire-declared array/string lengths as non-negative but never
/// against the bytes actually remaining in the frame, so a tiny frame declaring a huge count
/// drives an allocation attempt that aborts the whole process (`handle_alloc_error`, not a
/// panic - uncatchable, and it kills every connection, not just the offending one). `validate`
/// rejects that class of frame first, on a walk that never allocates a collection.
///
/// # Errors
///
/// Whatever `validate` returns, then the errors of [`decode_exhaustive`].
pub fn decode_guarded<T: Decodable>(
    version: i16,
    body: Bytes,
    validate: impl FnOnce(i16, &Bytes) -> Result<()>,
) -> Result<T> {
    validate(version, &body)?;
    decode_exhaustive(version, body)
}

/// Decode a `MetadataRequest` from the request body.
///
/// Accommodates an encoding defect in librdkafka 2.x (Issue #4828):
/// When requesting metadata for all topics (`total_topic_cnt == 0`) at flexible versions
/// (`ApiVersion >= 9`), librdkafka reserves a 4-byte `i32` array count placeholder
/// (`0x00, 0x00, 0x00, 0x00`) instead of a 1-byte compact array varint null (`0x00`),
/// and omits finalizing or compacting the buffer. This leaves up to 3 trailing zero bytes
/// unconsumed after the request fields are decoded. Real Kafka brokers accept this because
/// their decoders do not check for unconsumed trailing bytes after reading schema fields.
///
/// To maintain malformed-request protection, unconsumed trailing bytes are only permitted when:
/// 1. `version >= 9` (flexible metadata versions where compact array encoding is used)
/// 2. `req.topics.is_none()` (all-topics metadata request)
/// 3. Remaining bytes count is at most 3
/// 4. All remaining bytes are zero (`0x00`)
///
/// In all other cases (e.g. named topics, legacy metadata versions, truncated requests, or
/// non-zero trailing data), trailing bytes are rejected with [`KafkaWireError::Malformed`].
///
/// # Errors
///
/// [`KafkaWireError::Malformed`] when decoding fails or trailing bytes violate the above rules.
pub fn decode_metadata_exhaustive(version: i16, mut body: Bytes) -> Result<MetadataRequest> {
    let req = MetadataRequest::decode(&mut body, version)
        .map_err(|e| KafkaWireError::Malformed(e.to_string()))?;
    if body.has_remaining() {
        let is_librdkafka_all_topics_anomaly = version >= 9
            && req.topics.is_none()
            && body.remaining() <= 3
            && body.as_ref().iter().all(|&b| b == 0);

        if !is_librdkafka_all_topics_anomaly {
            return Err(KafkaWireError::Malformed(
                "unexpected trailing bytes in request body".to_string(),
            ));
        }
    }
    Ok(req)
}

/// Runs `validate` (see [`crate::bounds_guard::validate_metadata_shape`]) before
/// [`decode_metadata_exhaustive`].
///
/// # Errors
///
/// Whatever `validate` returns, then the errors of [`decode_metadata_exhaustive`].
pub fn decode_metadata_guarded(
    version: i16,
    body: Bytes,
    validate: impl FnOnce(i16, &Bytes) -> Result<()>,
) -> Result<MetadataRequest> {
    validate(version, &body)?;
    decode_metadata_exhaustive(version, body)
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::{ListGroupsRequest, MetadataRequest, SaslHandshakeRequest};
    use kafka_protocol::protocol::StrBytes;

    use super::*;
    use crate::bounds_guard::validate_metadata_shape;

    #[test]
    fn decode_exhaustive_accepts_an_exact_body() {
        assert!(decode_exhaustive::<ListGroupsRequest>(0, Bytes::new()).is_ok());
    }

    #[test]
    fn decode_exhaustive_rejects_trailing_bytes() {
        // ListGroups v0 has no fields, so every byte is trailing.
        let error =
            decode_exhaustive::<ListGroupsRequest>(0, Bytes::from_static(&[0x00])).unwrap_err();
        assert!(
            matches!(error, KafkaWireError::Malformed(ref message) if message.contains("trailing")),
            "{error}"
        );
    }

    /// Four bytes declaring `i32::MAX` topics. Decoding them first asks `Vec::with_capacity` for
    /// that many elements, which aborts the whole test binary, so this test cannot pass with the
    /// order flipped.
    #[test]
    fn decode_guarded_validates_before_decoding() {
        let body = Bytes::from_static(&[0x7F, 0xFF, 0xFF, 0xFF]);
        let result = decode_guarded::<MetadataRequest>(0, body, |version, body| {
            validate_metadata_shape(version, body, 8 * 1024 * 1024)
        });
        assert!(result.is_err());
    }

    #[test]
    fn decode_guarded_returns_the_validator_error() {
        let result = decode_guarded::<ListGroupsRequest>(0, Bytes::new(), |_, _| {
            Err(KafkaWireError::Malformed("refused".to_string()))
        });
        assert!(
            matches!(result, Err(KafkaWireError::Malformed(ref message)) if message == "refused")
        );
    }

    /// A legacy string carries a two-byte length, so `kafka_protocol` refuses anything longer
    /// than `i16::MAX` bytes. A field the version lacks is dropped silently instead, so it is no
    /// use here.
    #[test]
    fn encode_message_reports_what_the_encoder_refuses() {
        let mechanism = StrBytes::from_string("m".repeat(usize::try_from(i16::MAX).unwrap() + 1));
        let request = SaslHandshakeRequest::default().with_mechanism(mechanism);
        let error = encode_message(&request, 1, 64).unwrap_err();
        assert!(matches!(error, KafkaWireError::Malformed(_)), "{error}");
    }

    #[test]
    fn decode_metadata_exhaustive_accepts_librdkafka_captured_30_byte_request_body() {
        // Captured 30-byte librdkafka 2.x Metadata v9 all-topics request:
        // Frame prefix: 4 bytes (len 26)
        // Header v2: 18 bytes (ApiKey 3, ApiVersion 9, CorrelationId 1, ClientId "rdkafka", tags 0)
        // Body: 8 bytes (4-byte array count placeholder [0x00; 4], allow_auto 0, inc_cluster 0, inc_topic 0, tags 0)
        // Leaves 3 trailing zero bytes unconsumed.
        let body = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let req = decode_metadata_exhaustive(9, body).unwrap();
        assert!(req.topics.is_none());
        assert!(!req.allow_auto_topic_creation);
        assert!(!req.include_cluster_authorized_operations);
        assert!(!req.include_topic_authorized_operations);
        assert!(req.unknown_tagged_fields.is_empty());
    }

    #[test]
    fn decode_metadata_exhaustive_accepts_standard_5_byte_all_topics_v9() {
        // Standard compliant Kafka Metadata v9 all-topics body:
        // topics: null varint (0x00)
        // allow_auto: false (0x00)
        // inc_cluster: false (0x00)
        // inc_topic: false (0x00)
        // tagged_fields: 0 (0x00)
        let body = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00]);
        let req = decode_metadata_exhaustive(9, body).unwrap();
        assert!(req.topics.is_none());
        assert!(!req.allow_auto_topic_creation);
    }

    #[test]
    fn decode_metadata_exhaustive_accepts_standard_v9_named_topics() {
        // topics: compact array count 2 (1 element)
        // topic name: compact string len 5 ("test")
        // tagged fields on topic: 0
        // allow_auto: false (0x00)
        // inc_cluster: false (0x00)
        // inc_topic: false (0x00)
        // tagged fields: 0 (0x00)
        let body = Bytes::from_static(&[
            0x02, // 1 topic
            0x05, b't', b'e', b's', b't', // compact string "test"
            0x00, // tagged fields for topic
            0x00, // allow_auto
            0x00, // inc_cluster
            0x00, // inc_topic
            0x00, // tagged fields
        ]);
        let req = decode_metadata_exhaustive(9, body).unwrap();
        assert_eq!(req.topics.as_ref().map(Vec::len), Some(1));
    }

    #[test]
    fn decode_metadata_exhaustive_rejects_named_topics_with_trailing_bytes() {
        let body = Bytes::from_static(&[
            0x02, // 1 topic
            0x05, b't', b'e', b's', b't', // compact string "test"
            0x00, // tagged fields for topic
            0x00, // allow_auto
            0x00, // inc_cluster
            0x00, // inc_topic
            0x00, // tagged fields
            0x00, // trailing byte!
        ]);
        let err = decode_metadata_exhaustive(9, body).unwrap_err();
        assert!(matches!(err, KafkaWireError::Malformed(msg) if msg.contains("trailing")));
    }

    #[test]
    fn decode_metadata_exhaustive_rejects_excessive_trailing_bytes() {
        // 4 trailing zero bytes (more than the 3 placeholder bytes)
        let body = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let err = decode_metadata_exhaustive(9, body).unwrap_err();
        assert!(matches!(err, KafkaWireError::Malformed(msg) if msg.contains("trailing")));
    }

    #[test]
    fn decode_metadata_exhaustive_rejects_nonzero_trailing_bytes() {
        let body = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
        let err = decode_metadata_exhaustive(9, body).unwrap_err();
        assert!(matches!(err, KafkaWireError::Malformed(msg) if msg.contains("trailing")));
    }

    #[test]
    fn decode_metadata_exhaustive_rejects_legacy_version_with_trailing_bytes() {
        // Metadata v0 with trailing byte
        let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF, 0x00]);
        let err = decode_metadata_exhaustive(0, body).unwrap_err();
        assert!(matches!(err, KafkaWireError::Malformed(msg) if msg.contains("trailing")));
    }

    #[test]
    fn decode_metadata_guarded_validates_bounds_before_decoding() {
        // Truncated body (< 5 bytes) fails bounds validation with underflow
        let body = Bytes::from_static(&[0x00, 0x00]);
        let result = decode_metadata_guarded(9, body, |v, b| {
            validate_metadata_shape(v, b, 8 * 1024 * 1024)
        });
        assert!(result.is_err());
    }

    #[test]
    fn decode_metadata_guarded_accepts_librdkafka_30_byte_body() {
        let body = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let result = decode_metadata_guarded(9, body, |v, b| {
            validate_metadata_shape(v, b, 8 * 1024 * 1024)
        });
        assert!(result.is_ok());
    }

    #[test]
    fn unrelated_apis_still_reject_trailing_bytes() {
        // ProduceRequest, ListGroupsRequest, etc. continue using decode_exhaustive / decode_guarded
        let body = Bytes::from_static(&[0x00]);
        let result = decode_exhaustive::<ListGroupsRequest>(0, body);
        assert!(matches!(result, Err(KafkaWireError::Malformed(msg)) if msg.contains("trailing")));
    }
}
