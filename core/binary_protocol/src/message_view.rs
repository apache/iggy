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

//! Zero-copy view types for message frames on the wire.
//!
//! These types borrow an existing buffer and provide typed access to
//! message header fields and payload data without copying.

use crate::error::WireError;
use crate::message_layout::{
    MSG_CHECKSUM_OFFSET, MSG_ID_OFFSET, MSG_OFFSET_OFFSET, MSG_ORIGIN_TIMESTAMP_OFFSET,
    MSG_PAYLOAD_LEN_OFFSET, MSG_TIMESTAMP_OFFSET, MSG_USER_HEADERS_LEN_OFFSET,
    WIRE_MESSAGE_HEADER_SIZE,
};

// Private helpers for infallible reads on validated buffers

#[inline]
fn u32_at(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        buf[offset..offset + 4]
            .try_into()
            .expect("slice is exactly 4 bytes"),
    )
}

#[inline]
fn u64_at(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        buf[offset..offset + 8]
            .try_into()
            .expect("slice is exactly 8 bytes"),
    )
}

#[inline]
fn u128_at(buf: &[u8], offset: usize) -> u128 {
    u128::from_le_bytes(
        buf[offset..offset + 16]
            .try_into()
            .expect("slice is exactly 16 bytes"),
    )
}

/// Validate a message frame buffer and return `(total_size, payload_len, user_headers_len)`.
fn validate_frame(buf: &[u8]) -> Result<(usize, usize, usize), WireError> {
    if buf.len() < WIRE_MESSAGE_HEADER_SIZE {
        return Err(WireError::UnexpectedEof {
            offset: 0,
            need: WIRE_MESSAGE_HEADER_SIZE,
            have: buf.len(),
        });
    }

    let user_headers_len = u32_at(buf, MSG_USER_HEADERS_LEN_OFFSET) as usize;
    let payload_len = u32_at(buf, MSG_PAYLOAD_LEN_OFFSET) as usize;

    let total = WIRE_MESSAGE_HEADER_SIZE
        .checked_add(payload_len)
        .and_then(|s| s.checked_add(user_headers_len))
        .ok_or_else(|| WireError::Validation("message frame size overflow".to_string()))?;

    if buf.len() < total {
        return Err(WireError::UnexpectedEof {
            offset: 0,
            need: total,
            have: buf.len(),
        });
    }

    Ok((total, payload_len, user_headers_len))
}

// WireMessageView - immutable zero-copy frame view

/// Borrowed view over a single message frame in a contiguous buffer.
///
/// Validates the buffer on construction; all accessors are infallible.
pub struct WireMessageView<'a> {
    buf: &'a [u8],
    payload_len: usize,
    user_headers_len: usize,
}

impl<'a> WireMessageView<'a> {
    /// Create a view over the first message frame in `buf`.
    ///
    /// # Errors
    /// Returns `WireError` if the buffer is shorter than the header (64 bytes)
    /// or shorter than the full frame indicated by the length fields.
    pub fn new(buf: &'a [u8]) -> Result<Self, WireError> {
        let (total, payload_len, user_headers_len) = validate_frame(buf)?;
        Ok(Self {
            buf: &buf[..total],
            payload_len,
            user_headers_len,
        })
    }

    #[must_use]
    #[inline]
    pub fn checksum(&self) -> u64 {
        u64_at(self.buf, MSG_CHECKSUM_OFFSET)
    }

    #[must_use]
    #[inline]
    pub fn id(&self) -> u128 {
        u128_at(self.buf, MSG_ID_OFFSET)
    }

    #[must_use]
    #[inline]
    pub fn offset(&self) -> u64 {
        u64_at(self.buf, MSG_OFFSET_OFFSET)
    }

    #[must_use]
    #[inline]
    pub fn timestamp(&self) -> u64 {
        u64_at(self.buf, MSG_TIMESTAMP_OFFSET)
    }

    #[must_use]
    #[inline]
    pub fn origin_timestamp(&self) -> u64 {
        u64_at(self.buf, MSG_ORIGIN_TIMESTAMP_OFFSET)
    }

    #[must_use]
    #[inline]
    pub const fn payload_length(&self) -> usize {
        self.payload_len
    }

    #[must_use]
    #[inline]
    pub const fn user_headers_length(&self) -> usize {
        self.user_headers_len
    }

    #[must_use]
    pub fn payload(&self) -> &'a [u8] {
        &self.buf[WIRE_MESSAGE_HEADER_SIZE..WIRE_MESSAGE_HEADER_SIZE + self.payload_len]
    }

    #[must_use]
    pub fn user_headers(&self) -> &'a [u8] {
        let start = WIRE_MESSAGE_HEADER_SIZE + self.payload_len;
        &self.buf[start..start + self.user_headers_len]
    }

    #[must_use]
    #[inline]
    pub const fn total_size(&self) -> usize {
        self.buf.len()
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &'a [u8] {
        self.buf
    }
}

// WireMessageViewMut - mutable zero-copy frame view

/// Mutable view for in-place header patching (offset, timestamp, checksum).
pub struct WireMessageViewMut<'a> {
    buf: &'a mut [u8],
    payload_len: usize,
    user_headers_len: usize,
}

impl<'a> WireMessageViewMut<'a> {
    /// Create a mutable view over the first message frame in `buf`.
    ///
    /// # Errors
    /// Same validation as [`WireMessageView::new`].
    pub fn new(buf: &'a mut [u8]) -> Result<Self, WireError> {
        let (total, payload_len, user_headers_len) = validate_frame(buf)?;
        let (frame, _) = buf.split_at_mut(total);
        Ok(Self {
            buf: frame,
            payload_len,
            user_headers_len,
        })
    }

    // -- Read accessors (same semantics as WireMessageView) --

    #[must_use]
    #[inline]
    pub fn checksum(&self) -> u64 {
        u64_at(self.buf, MSG_CHECKSUM_OFFSET)
    }

    #[must_use]
    #[inline]
    pub fn id(&self) -> u128 {
        u128_at(self.buf, MSG_ID_OFFSET)
    }

    #[must_use]
    #[inline]
    pub fn offset(&self) -> u64 {
        u64_at(self.buf, MSG_OFFSET_OFFSET)
    }

    #[must_use]
    #[inline]
    pub fn timestamp(&self) -> u64 {
        u64_at(self.buf, MSG_TIMESTAMP_OFFSET)
    }

    #[must_use]
    #[inline]
    pub fn origin_timestamp(&self) -> u64 {
        u64_at(self.buf, MSG_ORIGIN_TIMESTAMP_OFFSET)
    }

    #[must_use]
    #[inline]
    pub const fn payload_length(&self) -> usize {
        self.payload_len
    }

    #[must_use]
    #[inline]
    pub const fn user_headers_length(&self) -> usize {
        self.user_headers_len
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.buf[WIRE_MESSAGE_HEADER_SIZE..WIRE_MESSAGE_HEADER_SIZE + self.payload_len]
    }

    #[must_use]
    pub fn user_headers(&self) -> &[u8] {
        let start = WIRE_MESSAGE_HEADER_SIZE + self.payload_len;
        &self.buf[start..start + self.user_headers_len]
    }

    #[must_use]
    #[inline]
    pub const fn total_size(&self) -> usize {
        self.buf.len()
    }

    // -- Typed setters --

    pub fn set_checksum(&mut self, value: u64) {
        self.buf[MSG_CHECKSUM_OFFSET..MSG_CHECKSUM_OFFSET + 8]
            .copy_from_slice(&value.to_le_bytes());
    }

    pub fn set_id(&mut self, value: u128) {
        self.buf[MSG_ID_OFFSET..MSG_ID_OFFSET + 16].copy_from_slice(&value.to_le_bytes());
    }

    pub fn set_offset(&mut self, value: u64) {
        self.buf[MSG_OFFSET_OFFSET..MSG_OFFSET_OFFSET + 8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn set_timestamp(&mut self, value: u64) {
        self.buf[MSG_TIMESTAMP_OFFSET..MSG_TIMESTAMP_OFFSET + 8]
            .copy_from_slice(&value.to_le_bytes());
    }
}

// WireMessageIterator - zero-copy frame iterator

/// Iterates over contiguous message frames, yielding borrowed views.
pub struct WireMessageIterator<'a> {
    buf: &'a [u8],
    pos: usize,
    remaining: u32,
}

impl<'a> WireMessageIterator<'a> {
    #[must_use]
    pub const fn new(buf: &'a [u8], count: u32) -> Self {
        Self {
            buf,
            pos: 0,
            remaining: count,
        }
    }
}

impl<'a> Iterator for WireMessageIterator<'a> {
    type Item = Result<WireMessageView<'a>, WireError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        match WireMessageView::new(&self.buf[self.pos..]) {
            Ok(view) => {
                self.pos += view.total_size();
                self.remaining -= 1;
                Some(Ok(view))
            }
            Err(e) => {
                self.remaining = 0;
                Some(Err(e))
            }
        }
    }
}

// WireMessageIteratorMut - mutable frame iterator (cursor-based)

/// Mutable iterator over contiguous message frames.
///
/// Cannot implement standard `Iterator` because the yielded mutable view's
/// lifetime is tied to `&mut self`. Uses a cursor-based API instead.
pub struct WireMessageIteratorMut<'a> {
    buf: &'a mut [u8],
    pos: usize,
    remaining: u32,
}

impl<'a> WireMessageIteratorMut<'a> {
    pub const fn new(buf: &'a mut [u8], count: u32) -> Self {
        Self {
            buf,
            pos: 0,
            remaining: count,
        }
    }

    /// Advance to the next frame and return a mutable view over it.
    ///
    /// The returned view borrows `self`, so it must be dropped before
    /// calling this method again.
    pub fn next_view_mut(&mut self) -> Option<Result<WireMessageViewMut<'_>, WireError>> {
        if self.remaining == 0 {
            return None;
        }

        // Compute frame size via temporary immutable access (borrow ends at block exit).
        let (total, available) = {
            let rest = &self.buf[self.pos..];
            if rest.len() < WIRE_MESSAGE_HEADER_SIZE {
                self.remaining = 0;
                return Some(Err(WireError::UnexpectedEof {
                    offset: self.pos,
                    need: WIRE_MESSAGE_HEADER_SIZE,
                    have: rest.len(),
                }));
            }
            let user_headers_len = u32_at(rest, MSG_USER_HEADERS_LEN_OFFSET) as usize;
            let payload_len = u32_at(rest, MSG_PAYLOAD_LEN_OFFSET) as usize;
            let Some(total) = WIRE_MESSAGE_HEADER_SIZE
                .checked_add(payload_len)
                .and_then(|s| s.checked_add(user_headers_len))
            else {
                self.remaining = 0;
                return Some(Err(WireError::Validation(
                    "message frame size overflow".to_string(),
                )));
            };
            (total, rest.len())
        };

        if available < total {
            self.remaining = 0;
            return Some(Err(WireError::UnexpectedEof {
                offset: self.pos,
                need: total,
                have: available,
            }));
        }

        let start = self.pos;
        self.pos += total;
        self.remaining -= 1;

        Some(WireMessageViewMut::new(&mut self.buf[start..start + total]))
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_layout::{
        MSG_ID_OFFSET, MSG_ORIGIN_TIMESTAMP_OFFSET, MSG_PAYLOAD_LEN_OFFSET,
        MSG_USER_HEADERS_LEN_OFFSET, WIRE_MESSAGE_HEADER_SIZE,
    };

    fn make_frame(payload: &[u8], user_headers: &[u8], id: u128, origin_ts: u64) -> Vec<u8> {
        let total = WIRE_MESSAGE_HEADER_SIZE + payload.len() + user_headers.len();
        let mut frame = vec![0u8; total];
        frame[MSG_ID_OFFSET..MSG_ID_OFFSET + 16].copy_from_slice(&id.to_le_bytes());
        frame[MSG_ORIGIN_TIMESTAMP_OFFSET..MSG_ORIGIN_TIMESTAMP_OFFSET + 8]
            .copy_from_slice(&origin_ts.to_le_bytes());
        #[allow(clippy::cast_possible_truncation)]
        {
            frame[MSG_USER_HEADERS_LEN_OFFSET..MSG_USER_HEADERS_LEN_OFFSET + 4]
                .copy_from_slice(&(user_headers.len() as u32).to_le_bytes());
            frame[MSG_PAYLOAD_LEN_OFFSET..MSG_PAYLOAD_LEN_OFFSET + 4]
                .copy_from_slice(&(payload.len() as u32).to_le_bytes());
        }
        frame[WIRE_MESSAGE_HEADER_SIZE..WIRE_MESSAGE_HEADER_SIZE + payload.len()]
            .copy_from_slice(payload);
        frame[WIRE_MESSAGE_HEADER_SIZE + payload.len()..].copy_from_slice(user_headers);
        frame
    }

    // -- WireMessageView --

    #[test]
    fn view_rejects_short_buffer() {
        let buf = [0u8; WIRE_MESSAGE_HEADER_SIZE - 1];
        assert!(WireMessageView::new(&buf).is_err());
    }

    #[test]
    fn view_rejects_truncated_payload() {
        let mut buf = vec![0u8; WIRE_MESSAGE_HEADER_SIZE + 5];
        buf[MSG_PAYLOAD_LEN_OFFSET..MSG_PAYLOAD_LEN_OFFSET + 4]
            .copy_from_slice(&10u32.to_le_bytes());
        assert!(WireMessageView::new(&buf).is_err());
    }

    #[test]
    fn view_accessors() {
        let frame = make_frame(b"hello", b"hdr", 42, 999);
        let view = WireMessageView::new(&frame).unwrap();
        assert_eq!(view.checksum(), 0);
        assert_eq!(view.id(), 42);
        assert_eq!(view.offset(), 0);
        assert_eq!(view.timestamp(), 0);
        assert_eq!(view.origin_timestamp(), 999);
        assert_eq!(view.payload_length(), 5);
        assert_eq!(view.user_headers_length(), 3);
        assert_eq!(view.payload(), b"hello");
        assert_eq!(view.user_headers(), b"hdr");
        assert_eq!(view.total_size(), WIRE_MESSAGE_HEADER_SIZE + 5 + 3);
        assert_eq!(view.as_bytes().len(), view.total_size());
    }

    #[test]
    fn view_empty_payload_and_headers() {
        let frame = make_frame(b"", b"", 1, 0);
        let view = WireMessageView::new(&frame).unwrap();
        assert_eq!(view.payload(), b"");
        assert_eq!(view.user_headers(), b"");
        assert_eq!(view.total_size(), WIRE_MESSAGE_HEADER_SIZE);
    }

    #[test]
    fn view_from_larger_buffer() {
        let mut buf = make_frame(b"pay", b"", 1, 0);
        buf.extend_from_slice(b"trailing_garbage");
        let view = WireMessageView::new(&buf).unwrap();
        assert_eq!(view.total_size(), WIRE_MESSAGE_HEADER_SIZE + 3);
        assert_eq!(view.as_bytes().len(), WIRE_MESSAGE_HEADER_SIZE + 3);
    }

    // -- WireMessageViewMut --

    #[test]
    fn view_mut_set_get_roundtrip() {
        let mut frame = make_frame(b"data", b"", 1, 0);
        let mut view = WireMessageViewMut::new(&mut frame).unwrap();

        view.set_checksum(0xDEAD_BEEF);
        view.set_id(0x1234);
        view.set_offset(100);
        view.set_timestamp(200);

        assert_eq!(view.checksum(), 0xDEAD_BEEF);
        assert_eq!(view.id(), 0x1234);
        assert_eq!(view.offset(), 100);
        assert_eq!(view.timestamp(), 200);
        assert_eq!(view.origin_timestamp(), 0);
        assert_eq!(view.payload(), b"data");
    }

    #[test]
    fn view_mut_set_does_not_corrupt_adjacent_fields() {
        let mut frame = make_frame(b"x", b"y", 42, 999);
        {
            let mut view = WireMessageViewMut::new(&mut frame).unwrap();
            view.set_offset(0xFFFF_FFFF_FFFF_FFFF);
        }
        let view = WireMessageView::new(&frame).unwrap();
        assert_eq!(view.id(), 42);
        assert_eq!(view.offset(), 0xFFFF_FFFF_FFFF_FFFF);
        assert_eq!(view.timestamp(), 0);
        assert_eq!(view.origin_timestamp(), 999);
        assert_eq!(view.payload(), b"x");
        assert_eq!(view.user_headers(), b"y");
    }

    // -- WireMessageIterator --

    #[test]
    fn iterator_zero_count() {
        let buf = [];
        let mut iter = WireMessageIterator::new(&buf, 0);
        assert!(iter.next().is_none());
    }

    #[test]
    fn iterator_single_message() {
        let frame = make_frame(b"hello", b"", 1, 100);
        let mut iter = WireMessageIterator::new(&frame, 1);
        let view = iter.next().unwrap().unwrap();
        assert_eq!(view.id(), 1);
        assert_eq!(view.payload(), b"hello");
        assert!(iter.next().is_none());
    }

    #[test]
    fn iterator_multiple_messages() {
        let f1 = make_frame(b"aaa", b"", 1, 100);
        let f2 = make_frame(b"bbb", b"hh", 2, 200);
        let f3 = make_frame(b"c", b"", 3, 300);
        let mut buf = Vec::new();
        buf.extend_from_slice(&f1);
        buf.extend_from_slice(&f2);
        buf.extend_from_slice(&f3);

        let views: Vec<_> = WireMessageIterator::new(&buf, 3)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(views.len(), 3);
        assert_eq!(views[0].id(), 1);
        assert_eq!(views[0].payload(), b"aaa");
        assert_eq!(views[1].id(), 2);
        assert_eq!(views[1].payload(), b"bbb");
        assert_eq!(views[1].user_headers(), b"hh");
        assert_eq!(views[2].id(), 3);
        assert_eq!(views[2].payload(), b"c");
    }

    #[test]
    fn iterator_truncated_mid_frame() {
        let frame = make_frame(b"hello", b"", 1, 0);
        let truncated = &frame[..frame.len() / 2];
        let mut iter = WireMessageIterator::new(truncated, 1);
        assert!(iter.next().unwrap().is_err());
        assert!(iter.next().is_none());
    }

    // -- WireMessageIteratorMut --

    #[test]
    fn iterator_mut_mutate_then_read() {
        let f1 = make_frame(b"aaa", b"", 1, 100);
        let f2 = make_frame(b"bbb", b"", 2, 200);
        let mut buf = Vec::new();
        buf.extend_from_slice(&f1);
        buf.extend_from_slice(&f2);

        let mut iter = WireMessageIteratorMut::new(&mut buf, 2);
        {
            let mut view = iter.next_view_mut().unwrap().unwrap();
            view.set_offset(10);
            view.set_timestamp(1000);
        }
        {
            let mut view = iter.next_view_mut().unwrap().unwrap();
            view.set_offset(20);
            view.set_timestamp(2000);
        }
        assert!(iter.next_view_mut().is_none());

        let views: Vec<_> = WireMessageIterator::new(&buf, 2)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(views[0].offset(), 10);
        assert_eq!(views[0].timestamp(), 1000);
        assert_eq!(views[0].payload(), b"aaa");
        assert_eq!(views[1].offset(), 20);
        assert_eq!(views[1].timestamp(), 2000);
        assert_eq!(views[1].payload(), b"bbb");
    }

    #[test]
    fn iterator_mut_count_matches() {
        let f1 = make_frame(b"a", b"", 1, 0);
        let f2 = make_frame(b"b", b"", 2, 0);
        let mut buf = Vec::new();
        buf.extend_from_slice(&f1);
        buf.extend_from_slice(&f2);

        let mut iter = WireMessageIteratorMut::new(&mut buf, 2);
        let mut count = 0;
        while let Some(Ok(_)) = iter.next_view_mut() {
            count += 1;
        }
        assert_eq!(count, 2);
    }
}
