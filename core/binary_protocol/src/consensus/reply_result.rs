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

//! Decoder for the committed-result section that leads a metadata reply body.
//!
//! A metadata op (`Operation::is_metadata`) commits a TigerBeetle-style result
//! section ahead of its typed payload: `[count: u32]` then `count` x
//! `{index: u32, result: u32}`, little-endian. Success is `count == 0` followed
//! by the payload; a committed business rejection is one `{index: 0, result}`
//! entry and no payload. Single-event metadata yields at most one entry today,
//! but the framing is batch-shaped so the format survives if an op ever
//! commits several events.
//!
//! This lives in `binary_protocol` because every client decodes it: the SDK
//! (mapping `result` to `IggyError`) and the simulator both read it, and
//! neither can depend on the server-side `metadata` crate. The encode mirror is
//! `metadata::stm::result::ApplyReply::write_reply_body`; the two share the
//! widths below so they cannot drift.

/// Little-endian width of the leading `[count]` field.
pub const RESULT_COUNT_LEN: usize = 4;

/// Little-endian width of one `{index: u32, result: u32}` entry.
pub const RESULT_ENTRY_LEN: usize = 8;

fn read_u32(reply_body: &[u8], offset: usize) -> Option<u32> {
    reply_body
        .get(offset..offset + 4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
}

/// Leading result code from a reply body, or `None` if it is not a well-formed
/// result section.
///
/// Zero `count` is success (`Some(0)`); nonzero returns the first entry's
/// `result`. `None` if the body is too short for the `count` or for an entry
/// the `count` claims: a truncated rejection must never read as `0`, so the
/// caller treats `None` as malformed, never Ok ("classify never guesses").
#[must_use]
pub fn result_code(reply_body: &[u8]) -> Option<u32> {
    let count = read_u32(reply_body, 0)?;
    if count == 0 {
        Some(0)
    } else {
        // First entry's `result` sits past the count and its leading index; a
        // body that cannot hold it is corruption, not success, so propagate
        // the `None`.
        read_u32(reply_body, RESULT_COUNT_LEN + 4)
    }
}

/// Byte length of the leading result section, i.e. the offset at which the
/// typed payload begins, or `None` if the body is too short to hold the entries
/// the `count` claims.
///
/// On success (`count == 0`) this is [`RESULT_COUNT_LEN`] and the payload
/// follows. The SDK strips this many bytes before decoding the typed response;
/// a `None` here is the same corruption [`result_code`] guards against, never a
/// guessed offset.
#[must_use]
pub fn result_section_len(reply_body: &[u8]) -> Option<usize> {
    let count = read_u32(reply_body, 0)? as usize;
    let len = RESULT_COUNT_LEN + count * RESULT_ENTRY_LEN;
    (reply_body.len() >= len).then_some(len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn success_body(payload: &[u8]) -> Vec<u8> {
        let mut body = 0u32.to_le_bytes().to_vec();
        body.extend_from_slice(payload);
        body
    }

    fn rejection_body(code: u32) -> Vec<u8> {
        let mut body = 1u32.to_le_bytes().to_vec();
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&code.to_le_bytes());
        body
    }

    #[test]
    fn success_decodes_to_zero_and_payload_starts_after_count() {
        let body = success_body(b"payload");
        assert_eq!(result_code(&body), Some(0));
        assert_eq!(result_section_len(&body), Some(RESULT_COUNT_LEN));
        assert_eq!(&body[RESULT_COUNT_LEN..], b"payload");
    }

    #[test]
    fn rejection_decodes_to_first_entry_result() {
        let body = rejection_body(1009);
        assert_eq!(result_code(&body), Some(1009));
        assert_eq!(
            result_section_len(&body),
            Some(RESULT_COUNT_LEN + RESULT_ENTRY_LEN),
        );
    }

    #[test]
    fn malformed_body_is_none_never_zero() {
        // Too short for the count, or claiming an entry it cannot hold: `None`,
        // never `Some(0)`. A rejection silently flipping to success is the bug
        // this guards.
        assert_eq!(result_code(&[]), None);
        assert_eq!(result_section_len(&[]), None);
        assert_eq!(result_code(&1u32.to_le_bytes()), None); // count=1, no entry
        assert_eq!(result_section_len(&1u32.to_le_bytes()), None);
        assert_eq!(result_code(&[1, 0, 0, 0, 0, 0, 0, 0]), None); // result missing
        assert_eq!(result_section_len(&[1, 0, 0, 0, 0, 0, 0, 0]), None);
    }
}
