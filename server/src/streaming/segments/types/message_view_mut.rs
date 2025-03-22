use super::IggyMessageHeaderViewMut;
use gxhash::gxhash64;
use iggy::prelude::*;
use lending_iterator::prelude::*;
use std::ops::Range;

/// A mutable view of a message for in-place modifications
#[derive(Debug)]
pub struct IggyMessageViewMut<'a> {
    /// The buffer containing the message
    buffer: &'a mut [u8],
    /// Payload offset
    payload_offset: usize,
}

impl<'a> IggyMessageViewMut<'a> {
    /// Create a new mutable message view from a buffer
    pub fn new(buffer: &'a mut [u8]) -> Self {
        let (payload_len, headers_len) = {
            let hdr_slice = &buffer[0..IGGY_MESSAGE_HEADER_SIZE as usize];
            let hdr_view = IggyMessageHeaderView::new(hdr_slice);
            (hdr_view.payload_length(), hdr_view.headers_length())
        };
        Self {
            buffer,
            payload_offset: IGGY_MESSAGE_HEADER_SIZE as usize,
        }
    }

    /// Get an immutable header view
    pub fn msg_header(&self) -> IggyMessageHeaderView<'_> {
        let hdr_slice = &self.buffer[0..IGGY_MESSAGE_HEADER_SIZE as usize];
        IggyMessageHeaderView::new(hdr_slice)
    }

    /// Get an ephemeral mutable header view for reading/writing
    pub fn msg_header_mut(&mut self) -> IggyMessageHeaderViewMut<'_> {
        let hdr_slice = &mut self.buffer[0..IGGY_MESSAGE_HEADER_SIZE as usize];
        IggyMessageHeaderViewMut::new(hdr_slice)
    }

    /// Returns the size of the entire message (header + payload + user headers).
    pub fn size(&self) -> usize {
        // TODO(hubcio): remove unwraps()
        let hdr_view = self.msg_header();
        (IGGY_MESSAGE_HEADER_SIZE + hdr_view.payload_length() + hdr_view.headers_length()) as usize
    }

    /// Get the byte range this message occupies in the buffer
    pub fn range(&self) -> Range<usize> {
        let end = self.payload_offset + self.size();
        self.payload_offset..end
    }

    /// Convenience to update the checksum field in the header
    pub fn update_checksum(&mut self) {
        let start = 8; // Skip checksum field for checksum calculation
        let size = self.size() - 8;
        let data = &self.buffer[start..start + size];

        let checksum = gxhash64(data, 0);

        let mut hdr_view = self.msg_header_mut();
        hdr_view.set_checksum(checksum);
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

        // Make sure we have enough bytes for at least a header
        if self.position + IGGY_MESSAGE_HEADER_SIZE as usize > self.buffer.len() {
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

        // Safety check: Make sure we're advancing
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
