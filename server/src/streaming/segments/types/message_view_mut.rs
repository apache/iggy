use super::IggyMessageHeaderViewMut;
use iggy::prelude::*;
use iggy::utils::checksum;
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

        // TODO(hubcio): field is 64 bits, but checksum is 32 bits (crc32fast)
        // in future, we should change it to some cryptographic safe hash function
        let checksum = checksum::calculate(data);

        self.header_mut().set_checksum(checksum as u64);
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
