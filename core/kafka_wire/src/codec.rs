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
}
