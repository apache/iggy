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

//! Sans-IO frame codec for the Iggy binary protocol.
//!
//! Encodes and decodes complete request/response frames without any I/O.
//! The transport layer (TCP, QUIC, WebSocket) reads bytes into a buffer,
//! then hands the buffer to these types for zero-copy parsing.
//!
//! When VSR consensus replaces this framing, the transport layer will
//! switch to `consensus::header::GenericHeader` (256-byte fixed header)
//! while the command payload codec stays the same.

use crate::codec::{read_bytes, read_u32_le};
use crate::error::WireError;
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;
use std::num::NonZeroU32;

/// Status code for a successful response.
pub const STATUS_OK: u32 = 0;

/// Decoded request frame. Borrows the payload from the input buffer.
///
/// Wire format: `[length:4 LE][code:4 LE][payload:N]`
/// where `length` = 4 (code size) + N (payload size).
#[derive(Debug)]
pub struct RequestFrame<'a> {
    pub code: u32,
    pub payload: &'a [u8],
}

impl<'a> RequestFrame<'a> {
    /// Size of the frame header: `[length:4][code:4]`.
    pub const HEADER_SIZE: usize = 8;

    /// Validate a frame length field and return the payload size.
    ///
    /// Transport layers that read the length and code fields incrementally
    /// (e.g. compio completion-based I/O) can use this to validate the length
    /// before reading the payload, without buffering the entire frame.
    ///
    /// # Errors
    /// Returns `WireError::Validation` if `frame_length < 4` (too small to
    /// contain even the command code).
    pub fn payload_length(frame_length: u32) -> Result<u32, WireError> {
        frame_length
            .checked_sub(4)
            .ok_or(WireError::Validation(Cow::Borrowed(
                "request frame length must be at least 4 (code size)",
            )))
    }

    /// Construct a frame from pre-parsed header fields and a payload slice.
    ///
    /// Used by transport layers that read the header incrementally (e.g.
    /// compio completion-based I/O) and then read the payload separately.
    #[must_use]
    pub const fn from_parts(code: u32, payload: &'a [u8]) -> Self {
        Self { code, payload }
    }

    /// Decode a request frame from a complete buffer.
    ///
    /// # Errors
    /// Returns `WireError::UnexpectedEof` if the buffer is too short.
    pub fn decode(buf: &'a [u8]) -> Result<(Self, usize), WireError> {
        let frame_length = read_u32_le(buf, 0)?;
        let payload_len = Self::payload_length(frame_length)? as usize;
        let code = read_u32_le(buf, 4)?;
        let payload = read_bytes(buf, Self::HEADER_SIZE, payload_len)?;
        let total = Self::HEADER_SIZE + payload_len;
        Ok((Self { code, payload }, total))
    }

    /// Encode a request frame into `out`.
    ///
    /// Writes `[length:4 LE][code:4 LE][payload]` where length includes
    /// the 4-byte code field.
    ///
    /// # Errors
    /// Returns `WireError::PayloadTooLarge` if payload exceeds u32 capacity.
    pub fn encode(code: u32, payload: &[u8], out: &mut BytesMut) -> Result<(), WireError> {
        let length = payload
            .len()
            .checked_add(4)
            .and_then(|n| u32::try_from(n).ok())
            .ok_or(WireError::PayloadTooLarge {
                size: payload.len(),
                max: u32::MAX as usize - 4,
            })?;
        out.reserve(Self::HEADER_SIZE + payload.len());
        out.put_u32_le(length);
        out.put_u32_le(code);
        out.put_slice(payload);
        Ok(())
    }

    /// Total encoded size for a given payload length.
    ///
    /// Returns `None` if `HEADER_SIZE + payload_len` overflows `usize`.
    #[must_use]
    pub const fn encoded_size(payload_len: usize) -> Option<usize> {
        Self::HEADER_SIZE.checked_add(payload_len)
    }
}

/// Decoded response frame. Borrows the payload from the input buffer.
///
/// Wire format: `[status:4 LE][length:4 LE][payload:N]`
/// where `status` = 0 for success, non-zero for error code.
#[derive(Debug)]
pub struct ResponseFrame<'a> {
    pub status: u32,
    pub payload: &'a [u8],
}

impl<'a> ResponseFrame<'a> {
    /// Size of the frame header: `[status:4][length:4]`.
    pub const HEADER_SIZE: usize = 8;

    /// Decode a response frame from a complete buffer.
    ///
    /// # Errors
    /// Returns `WireError::UnexpectedEof` if the buffer is too short.
    pub fn decode(buf: &'a [u8]) -> Result<(Self, usize), WireError> {
        let status = read_u32_le(buf, 0)?;
        let length = read_u32_le(buf, 4)? as usize;
        let payload = read_bytes(buf, Self::HEADER_SIZE, length)?;
        let total = Self::HEADER_SIZE + length;
        Ok((Self { status, payload }, total))
    }

    /// Encode a successful response with payload.
    ///
    /// # Errors
    /// Returns `WireError::PayloadTooLarge` if payload exceeds u32 capacity.
    pub fn encode_ok(payload: &[u8], out: &mut BytesMut) -> Result<(), WireError> {
        let length = u32::try_from(payload.len()).map_err(|_| WireError::PayloadTooLarge {
            size: payload.len(),
            max: u32::MAX as usize,
        })?;
        out.reserve(Self::HEADER_SIZE + payload.len());
        out.put_u32_le(STATUS_OK);
        out.put_u32_le(length);
        out.put_slice(payload);
        Ok(())
    }

    /// Encode an error response (status code, empty payload).
    pub fn encode_error(status: NonZeroU32, out: &mut BytesMut) {
        out.reserve(Self::HEADER_SIZE);
        out.put_u32_le(status.get());
        out.put_u32_le(0);
    }

    /// Returns `true` if this is a success response.
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        self.status == STATUS_OK
    }

    /// Total encoded size for a given payload length.
    ///
    /// Returns `None` if `HEADER_SIZE + payload_len` overflows `usize`.
    #[must_use]
    pub const fn encoded_size(payload_len: usize) -> Option<usize> {
        Self::HEADER_SIZE.checked_add(payload_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip() {
        let payload = b"hello world";
        let mut buf = BytesMut::with_capacity(RequestFrame::encoded_size(payload.len()).unwrap());
        RequestFrame::encode(42, payload, &mut buf).unwrap();

        let (frame, consumed) = RequestFrame::decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(frame.code, 42);
        assert_eq!(frame.payload, payload);
    }

    #[test]
    fn request_empty_payload() {
        let mut buf = BytesMut::with_capacity(RequestFrame::HEADER_SIZE);
        RequestFrame::encode(1, &[], &mut buf).unwrap();

        let (frame, consumed) = RequestFrame::decode(&buf).unwrap();
        assert_eq!(consumed, 8);
        assert_eq!(frame.code, 1);
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn request_length_field_includes_code() {
        let payload = b"test";
        let mut buf = BytesMut::new();
        RequestFrame::encode(99, payload, &mut buf).unwrap();

        let length = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(length, 4 + 4); // code(4) + payload(4)
    }

    #[test]
    fn request_truncated_header() {
        let buf = [0u8; 7]; // less than HEADER_SIZE
        assert!(RequestFrame::decode(&buf).is_err());
    }

    #[test]
    fn request_truncated_payload() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(104); // length = 104 (code + 100 bytes payload)
        buf.put_u32_le(1); // code
        buf.put_slice(&[0u8; 50]); // only 50 of 100 bytes
        assert!(RequestFrame::decode(&buf).is_err());
    }

    #[test]
    fn request_length_too_small() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(3); // length < 4 (must include code)
        buf.put_u32_le(1);
        assert!(RequestFrame::decode(&buf).is_err());
    }

    #[test]
    fn payload_length_valid() {
        assert_eq!(RequestFrame::payload_length(4).unwrap(), 0);
        assert_eq!(RequestFrame::payload_length(104).unwrap(), 100);
        assert_eq!(
            RequestFrame::payload_length(u32::MAX).unwrap(),
            u32::MAX - 4
        );
    }

    #[test]
    fn payload_length_too_small() {
        assert!(RequestFrame::payload_length(0).is_err());
        assert!(RequestFrame::payload_length(1).is_err());
        assert!(RequestFrame::payload_length(3).is_err());
    }

    #[test]
    fn request_encoded_size() {
        assert_eq!(RequestFrame::encoded_size(0), Some(8));
        assert_eq!(RequestFrame::encoded_size(100), Some(108));
        assert_eq!(RequestFrame::encoded_size(usize::MAX), None);
    }

    #[test]
    fn response_ok_roundtrip() {
        let payload = b"response data";
        let mut buf = BytesMut::with_capacity(ResponseFrame::encoded_size(payload.len()).unwrap());
        ResponseFrame::encode_ok(payload, &mut buf).unwrap();

        let (frame, consumed) = ResponseFrame::decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert!(frame.is_ok());
        assert_eq!(frame.status, 0);
        assert_eq!(frame.payload, payload);
    }

    #[test]
    fn response_ok_empty_payload() {
        let mut buf = BytesMut::new();
        ResponseFrame::encode_ok(&[], &mut buf).unwrap();

        let (frame, consumed) = ResponseFrame::decode(&buf).unwrap();
        assert_eq!(consumed, 8);
        assert!(frame.is_ok());
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn response_error_roundtrip() {
        let mut buf = BytesMut::new();
        ResponseFrame::encode_error(NonZeroU32::new(1001).unwrap(), &mut buf);

        let (frame, consumed) = ResponseFrame::decode(&buf).unwrap();
        assert_eq!(consumed, 8);
        assert!(!frame.is_ok());
        assert_eq!(frame.status, 1001);
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn response_truncated_header() {
        let buf = [0u8; 7];
        assert!(ResponseFrame::decode(&buf).is_err());
    }

    #[test]
    fn response_truncated_payload() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(0); // status OK
        buf.put_u32_le(100); // length = 100
        buf.put_slice(&[0u8; 50]); // only 50 bytes
        assert!(ResponseFrame::decode(&buf).is_err());
    }

    #[test]
    fn response_encoded_size() {
        assert_eq!(ResponseFrame::encoded_size(0), Some(8));
        assert_eq!(ResponseFrame::encoded_size(256), Some(264));
        assert_eq!(ResponseFrame::encoded_size(usize::MAX), None);
    }
}
