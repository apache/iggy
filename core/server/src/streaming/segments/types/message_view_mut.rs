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

use super::IggyMessageHeaderViewMut;
use iggy_common::{IGGY_MESSAGE_HEADER_SIZE, IggyMessageHeaderView, calculate_checksum};
use lending_iterator::prelude::*;

/// A mutable view of a message for in-place modifications
#[derive(Debug)]
pub struct IggyMessageViewMut<'a> {
    /// The buffer containing the message
    buffer: &'a mut [u8],
}

impl<'a> IggyMessageViewMut<'a> {
    /// Create a new mutable message view from a buffer
    pub fn new(buffer: &'a mut [u8]) -> Self {
        Self { buffer }
    }

    /// Get an immutable header view
    pub fn header(&self) -> IggyMessageHeaderView<'_> {
        let hdr_slice = &self.buffer[0..IGGY_MESSAGE_HEADER_SIZE];
        IggyMessageHeaderView::new(hdr_slice)
    }

    /// Get an ephemeral mutable header view for reading/writing
    pub fn header_mut(&mut self) -> IggyMessageHeaderViewMut<'_> {
        let hdr_slice = &mut self.buffer[0..IGGY_MESSAGE_HEADER_SIZE];
        IggyMessageHeaderViewMut::new(hdr_slice)
    }

    /// Returns the size of the entire message (header + payload + user headers).
    pub fn size(&self) -> usize {
        let hdr_view = self.header();

        IGGY_MESSAGE_HEADER_SIZE + hdr_view.payload_length() + hdr_view.user_headers_length()
    }

    /// Convenience method to update the checksum field in the header
    pub fn update_checksum(&mut self) {
        let checksum_field_size = size_of::<u64>(); // Skip checksum field for checksum calculation
        let size = self.size() - checksum_field_size;
        let data = &self.buffer[checksum_field_size..checksum_field_size + size];
        let checksum = calculate_checksum(data);
        self.header_mut().set_checksum(checksum);
    }
}

/// Iterator over mutable message views in a buffer
pub struct IggyMessageViewMutIterator<'a> {
    buffer: &'a mut [u8],
    position: usize,
}

impl<'a> IggyMessageViewMutIterator<'a> {
    pub fn new(buffer: &'a mut [u8]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }
}

#[gat]
impl LendingIterator for IggyMessageViewMutIterator<'_> {
    type Item<'next> = IggyMessageViewMut<'next>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let buffer_len = self.buffer.len();
        if self.position >= buffer_len {
            return None;
        }

        if self.position + IGGY_MESSAGE_HEADER_SIZE > self.buffer.len() {
            tracing::error!(
                "Buffer too small for message header at position {}, buffer len: {}",
                self.position,
                self.buffer.len()
            );
            self.position = self.buffer.len();
            return None;
        }

        let buffer_slice = &mut self.buffer[self.position..];
        let view = IggyMessageViewMut::new(buffer_slice);

        let message_size = view.size();
        if message_size == 0 {
            tracing::error!(
                "Message size is 0 at position {}, preventing infinite loop",
                self.position
            );
            self.position = buffer_len;
            return None;
        }

        self.position += message_size;
        Some(view)
    }
}
