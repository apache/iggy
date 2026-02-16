/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use super::HeaderValue;
use super::message_header::*;
use crate::BytesSerializable;
use crate::IggyByteSize;
use crate::Sizeable;
use crate::error::IggyError;
use crate::utils::checksum;
use crate::{HeaderKey, IggyMessageHeaderView};
use bytes::{Bytes, BytesMut};
use std::{collections::HashMap, iter::Iterator};

/// A immutable view of a message.
#[derive(Debug)]
pub struct IggyMessageView<'a> {
    buffer: &'a [u8],
    payload_offset: usize,
    user_headers_offset: usize,
}

impl<'a> IggyMessageView<'a> {
    /// Creates a new immutable message view from a buffer.
    ///
    /// Validates that the buffer is large enough to contain the full message
    /// (header + payload + user headers). All subsequent accessors can use
    /// direct indexing because this constructor guarantees the bounds.
    pub fn new(buffer: &'a [u8]) -> Result<Self, IggyError> {
        if buffer.len() < IGGY_MESSAGE_HEADER_SIZE {
            return Err(IggyError::InvalidMessagePayloadLength);
        }
        let header_view = IggyMessageHeaderView::new(&buffer[IGGY_MESSAGE_HEADER_RANGE]);
        let payload_len = header_view.payload_length();
        let user_headers_len = header_view.user_headers_length();
        let total_size = IGGY_MESSAGE_HEADER_SIZE
            .checked_add(payload_len)
            .and_then(|s| s.checked_add(user_headers_len))
            .ok_or(IggyError::InvalidMessagePayloadLength)?;
        if buffer.len() < total_size {
            return Err(IggyError::InvalidMessagePayloadLength);
        }
        let payload_offset = IGGY_MESSAGE_HEADER_SIZE;
        let headers_offset = payload_offset
            .checked_add(payload_len)
            .ok_or(IggyError::InvalidMessagePayloadLength)?;

        Ok(Self {
            buffer,
            payload_offset,
            user_headers_offset: headers_offset,
        })
    }

    /// Returns an immutable header view.
    pub fn header(&self) -> IggyMessageHeaderView<'_> {
        IggyMessageHeaderView::new(&self.buffer[0..IGGY_MESSAGE_HEADER_SIZE])
    }

    /// Returns an immutable slice of the user headers.
    pub fn user_headers(&self) -> Option<&[u8]> {
        let header_length = self.header().user_headers_length();
        if header_length == 0 {
            return None;
        }
        // Bounds guaranteed by new() which validates total message size
        let end_offset = self.user_headers_offset + header_length;
        Some(&self.buffer[self.user_headers_offset..end_offset])
    }

    /// Return instantiated user headers map
    pub fn user_headers_map(&self) -> Result<Option<HashMap<HeaderKey, HeaderValue>>, IggyError> {
        if let Some(headers) = self.user_headers() {
            let headers_bytes = Bytes::copy_from_slice(headers);

            match HashMap::<HeaderKey, HeaderValue>::from_bytes(headers_bytes) {
                Ok(h) => Ok(Some(h)),
                Err(e) => {
                    tracing::error!(
                        "Error parsing headers: {}, header_length={}",
                        e,
                        self.header().user_headers_length()
                    );

                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    /// Returns the size of the entire message.
    pub fn size(&self) -> usize {
        let header_view = self.header();
        IGGY_MESSAGE_HEADER_SIZE + header_view.payload_length() + header_view.user_headers_length()
    }

    /// Returns a reference to the payload portion.
    pub fn payload(&self) -> &[u8] {
        let header_view = self.header();
        let payload_len = header_view.payload_length();
        let end = self.payload_offset + payload_len;
        // Bounds guaranteed by new() which validates total message size
        &self.buffer[self.payload_offset..end]
    }

    /// Calculates the checksum over the message (excluding the checksum field itself).
    /// This should be called only on server side.
    pub fn calculate_checksum(&self) -> u64 {
        let checksum_field_size = size_of::<u64>();
        let size = self.size() - checksum_field_size;
        let end = checksum_field_size + size;
        // Bounds guaranteed by new() which validates total message size
        let data = &self.buffer[checksum_field_size..end];
        checksum::calculate_checksum(data)
    }
}

impl BytesSerializable for IggyMessageView<'_> {
    fn to_bytes(&self) -> Bytes {
        panic!("should not be used")
    }

    fn from_bytes(_bytes: Bytes) -> Result<Self, IggyError> {
        panic!("should not be used")
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(self.buffer);
    }

    fn get_buffer_size(&self) -> usize {
        self.buffer.len()
    }
}

impl Sizeable for IggyMessageView<'_> {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.buffer.len() as u64)
    }
}

/// Iterator over immutable message views in a buffer.
pub struct IggyMessageViewIterator<'a> {
    buffer: &'a [u8],
    position: usize,
}

impl<'a> IggyMessageViewIterator<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }
}

impl<'a> Iterator for IggyMessageViewIterator<'a> {
    type Item = IggyMessageView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.buffer.len() {
            return None;
        }

        let remaining = &self.buffer[self.position..];
        let view = IggyMessageView::new(remaining).ok()?;
        self.position += view.size();
        Some(view)
    }
}
