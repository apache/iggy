use super::message_header::*;
use super::message_header_view::IggyMessageHeaderView;
use crate::error::IggyError;
use crate::models::messaging::header::HeaderKey;
use crate::models::messaging::header::HeaderValue;
use crate::prelude::BytesSerializable;
use std::collections::HashMap;
use std::iter::Iterator;

/// A immutable view of a message.
#[derive(Debug)]
pub struct IggyMessageView<'a> {
    buffer: &'a [u8],
    payload_offset: usize,
    headers_offset: usize,
}

impl<'a> IggyMessageView<'a> {
    /// Creates a new immutable message view from a buffer.
    pub fn new(buffer: &'a [u8]) -> Self {
        let header_view = IggyMessageHeaderView::new(&buffer[IGGY_MESSAGE_HEADER_RANGE]);
        let payload_len = header_view.payload_length() as usize;
        let payload_offset = IGGY_MESSAGE_HEADER_SIZE as usize;
        let headers_offset = IGGY_MESSAGE_HEADER_SIZE as usize + payload_len;

        Self {
            buffer,
            payload_offset,
            headers_offset,
        }
    }

    /// Returns an immutable header view.
    pub fn msg_header(&self) -> IggyMessageHeaderView<'_> {
        IggyMessageHeaderView::new(&self.buffer[0..IGGY_MESSAGE_HEADER_SIZE as usize])
    }

    /// Returns an immutable slice of the user headers.
    pub fn headers(&self) -> &[u8] {
        &self.buffer[self.headers_offset..]
    }

    /// Returns the size of the entire message.
    pub fn size(&self) -> usize {
        let header_view = self.msg_header();
        IGGY_MESSAGE_HEADER_SIZE as usize
            + header_view.payload_length() as usize
            + header_view.headers_length() as usize
    }

    /// Returns a reference to the payload portion.
    pub fn payload(&self) -> &[u8] {
        let header_view = self.msg_header();
        let payload_len = header_view.payload_length() as usize;
        &self.buffer[self.payload_offset..self.payload_offset + payload_len]
    }

    /// Validates that the message view is properly formatted and has valid data.
    pub fn validate(&self) -> Result<(), IggyError> {
        if self.buffer.len() < IGGY_MESSAGE_HEADER_SIZE as usize {
            return Err(IggyError::InvalidMessagePayloadLength);
        }

        let header = self.msg_header();
        let payload_len = header.payload_length() as usize;
        let headers_len = header.headers_length() as usize;
        let total_size = IGGY_MESSAGE_HEADER_SIZE as usize + payload_len + headers_len;

        if self.buffer.len() < total_size {
            return Err(IggyError::InvalidMessagePayloadLength);
        }
        Ok(())
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
        let view = IggyMessageView::new(remaining);
        self.position += view.size();
        Some(view)
    }
}
