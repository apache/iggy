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

//! Zero-copy consensus message wrapper.
//!
//! `Message<H>` wraps a `Bytes` buffer and provides typed access to the
//! header via `bytemuck` pointer cast (zero allocation, zero copy).

use crate::consensus::{
    Command2, ConsensusError, ConsensusHeader, GenericHeader, PrepareHeader, PrepareOkHeader,
    RequestHeader,
};
use bytes::Bytes;
use std::marker::PhantomData;

/// Trait for types that provide access to a consensus header.
pub trait ConsensusMessage<H: ConsensusHeader> {
    fn header(&self) -> &H;
}

impl<H: ConsensusHeader> ConsensusMessage<H> for Message<H> {
    fn header(&self) -> &H {
        let header_bytes = &self.buffer[..size_of::<H>()];
        bytemuck::checked::try_from_bytes(header_bytes)
            .expect("header validated at construction time")
    }
}

/// Zero-copy message wrapping a `Bytes` buffer with a typed header.
///
/// The header is accessed via `bytemuck::try_from_bytes` - a pointer cast,
/// not a deserialization step. The body is the slice after the header,
/// sized according to `header.size()`.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct Message<H: ConsensusHeader> {
    buffer: Bytes,
    _marker: PhantomData<H>,
}

impl<H: ConsensusHeader> Message<H> {
    /// Create a message from a buffer, validating the header.
    ///
    /// # Errors
    /// Returns `ConsensusError` if the buffer is too small or the header is invalid.
    pub fn from_bytes(buffer: Bytes) -> Result<Self, ConsensusError> {
        if buffer.len() < size_of::<H>() {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command2::Reserved,
            });
        }

        // TODO(hubcio): bytemuck::checked::try_from_bytes requires the buffer to be aligned
        // to the target type's alignment. Consensus headers contain u128 fields (16-byte
        // alignment), but Bytes from Vec<u8> only guarantees 8-byte alignment. The checked
        // variant returns Err on misalignment (no UB), but production code can fail on
        // misaligned buffers. The original iggy_common code has the same limitation.
        // Fix by either using pod_read_unaligned (copies header) or ensuring aligned
        // allocation at the network receive layer.
        let header_bytes = &buffer[..size_of::<H>()];
        let header = bytemuck::checked::try_from_bytes::<H>(header_bytes)
            .map_err(|_| ConsensusError::InvalidBitPattern)?;

        header.validate()?;

        let header_size = header.size() as usize;
        if buffer.len() < header_size {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command2::Reserved,
            });
        }

        Ok(Self {
            buffer,
            _marker: PhantomData,
        })
    }

    /// Create a zero-initialized message of the given size.
    /// The caller must initialize the header fields.
    ///
    /// # Panics
    /// Panics if `size` is smaller than the header size.
    #[must_use]
    pub fn new(size: usize) -> Self {
        assert!(
            size >= size_of::<H>(),
            "size must be at least header size ({})",
            size_of::<H>()
        );
        Self {
            buffer: Bytes::from(vec![0u8; size]),
            _marker: PhantomData,
        }
    }

    /// Access the typed header (zero-copy pointer cast).
    ///
    /// Re-validates the bit pattern on each call because headers contain
    /// `CheckedBitPattern` enums (`Command2`, `Operation`) which prevent
    /// using the unchecked `bytemuck::try_from_bytes` (requires `Pod`).
    /// The cost is a few enum discriminant range checks per access.
    /// The buffer is immutable (`Bytes`) so validation cannot fail after
    /// successful construction.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    #[inline]
    pub fn header(&self) -> &H {
        let header_bytes = &self.buffer[..size_of::<H>()];
        bytemuck::checked::try_from_bytes(header_bytes)
            .expect("header validated at construction time")
    }

    /// Message body (everything after the header).
    #[must_use]
    #[inline]
    pub fn body(&self) -> &[u8] {
        let header_size = size_of::<H>();
        let total_size = self.header().size() as usize;
        if total_size > header_size {
            &self.buffer[header_size..total_size]
        } else {
            &[]
        }
    }

    /// Message body as zero-copy [`Bytes`].
    #[must_use]
    #[inline]
    pub fn body_bytes(&self) -> Bytes {
        let header_size = size_of::<H>();
        let total_size = self.header().size() as usize;
        if total_size > header_size {
            self.buffer.slice(header_size..total_size)
        } else {
            Bytes::new()
        }
    }

    /// Complete message bytes (header + body).
    #[must_use]
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        let total_size = self.header().size() as usize;
        &self.buffer[..total_size]
    }

    /// Consume into the underlying buffer.
    #[must_use]
    #[inline]
    pub fn into_inner(self) -> Bytes {
        self.buffer
    }

    /// Convert to a generic message (type-erase the header).
    #[must_use]
    pub fn into_generic(self) -> Message<GenericHeader> {
        // SAFETY: Message<H> and Message<GenericHeader> have identical layout
        // (only PhantomData differs). GenericHeader accepts any command.
        unsafe { Message::from_buffer_unchecked(self.buffer) }
    }

    /// View as a generic message without consuming.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn as_generic(&self) -> &Message<GenericHeader> {
        // SAFETY: #[repr(C)] guarantees field layout. Message<H> and
        // Message<GenericHeader> differ only in PhantomData (ZST).
        unsafe { &*std::ptr::from_ref(self).cast::<Message<GenericHeader>>() }
    }

    /// Try to reinterpret as a different header type.
    ///
    /// # Errors
    /// Returns `ConsensusError` if the header command doesn't match or validation fails.
    pub fn try_into_typed<T: ConsensusHeader>(self) -> Result<Message<T>, ConsensusError> {
        if self.buffer.len() < size_of::<T>() {
            return Err(ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: Command2::Reserved,
            });
        }

        let generic = self.as_generic();
        if generic.header().command != T::COMMAND {
            return Err(ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: generic.header().command,
            });
        }

        let header_bytes = &self.buffer[..size_of::<T>()];
        let header = bytemuck::checked::try_from_bytes::<T>(header_bytes)
            .map_err(|_| ConsensusError::InvalidBitPattern)?;
        header.validate()?;

        Ok(unsafe { Message::<T>::from_buffer_unchecked(self.buffer) })
    }

    /// Try to borrow as a different header type without consuming.
    ///
    /// # Errors
    /// Returns `ConsensusError` if the header command doesn't match or validation fails.
    pub fn try_as_typed<T: ConsensusHeader>(&self) -> Result<&Message<T>, ConsensusError> {
        if self.buffer.len() < size_of::<T>() {
            return Err(ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: Command2::Reserved,
            });
        }

        let generic = self.as_generic();
        if generic.header().command != T::COMMAND {
            return Err(ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: generic.header().command,
            });
        }

        let header_bytes = &self.buffer[..size_of::<T>()];
        bytemuck::checked::try_from_bytes::<T>(header_bytes)
            .map_err(|_| ConsensusError::InvalidBitPattern)?
            .validate()?;

        // SAFETY: Message<H> and Message<T> have identical layout (#[repr(C)],
        // differ only in PhantomData). Header validated above.
        Ok(unsafe { &*std::ptr::from_ref(self).cast::<Message<T>>() })
    }

    /// Transmute the header to a different type, preserving the buffer.
    ///
    /// The callback receives the old header (by value) and a mutable reference
    /// to the new (zeroed) header, allowing field-by-field initialization.
    ///
    /// # Panics
    /// Panics if `size_of::<H>() != size_of::<T>()`.
    #[must_use]
    pub fn transmute_header<T: ConsensusHeader>(self, f: impl FnOnce(H, &mut T)) -> Message<T> {
        assert_eq!(size_of::<H>(), size_of::<T>());
        let old_header = *self.header();
        let buffer = self.into_inner();
        // TODO: This mutates through Bytes via cast_mut(), which violates aliasing rules.
        // The original iggy_common code has the same pattern. Once iggy_common is migrated
        // to use iggy_binary_protocol types, fix both by using Bytes::try_into_mut() ->
        // mutate BytesMut -> freeze(). Tracked as part of the consensus migration.
        unsafe {
            let ptr = buffer.as_ptr().cast_mut();
            let slice = std::slice::from_raw_parts_mut(ptr, size_of::<T>());
            slice.fill(0);
            let new_header =
                bytemuck::checked::try_from_bytes_mut(slice).expect("zeroed bytes are valid");
            f(old_header, new_header);
        }
        Message {
            buffer,
            _marker: PhantomData,
        }
    }

    /// Create without validation. Private.
    ///
    /// # Safety
    /// Buffer must already be validated for the target header type.
    #[inline]
    const unsafe fn from_buffer_unchecked(buffer: Bytes) -> Self {
        Self {
            buffer,
            _marker: PhantomData,
        }
    }
}

/// Type-erased message bag for dispatching incoming consensus messages.
#[derive(Debug)]
pub enum MessageBag {
    Request(Message<RequestHeader>),
    Prepare(Message<PrepareHeader>),
    PrepareOk(Message<PrepareOkHeader>),
}

impl MessageBag {
    #[must_use]
    pub fn command(&self) -> Command2 {
        match self {
            Self::Request(m) => m.header().command,
            Self::Prepare(m) => m.header().command,
            Self::PrepareOk(m) => m.header().command,
        }
    }

    #[must_use]
    pub fn size(&self) -> u32 {
        match self {
            Self::Request(m) => m.header().size(),
            Self::Prepare(m) => m.header().size(),
            Self::PrepareOk(m) => m.header().size(),
        }
    }

    #[must_use]
    pub fn operation(&self) -> crate::consensus::Operation {
        match self {
            Self::Request(m) => m.header().operation,
            Self::Prepare(m) => m.header().operation,
            Self::PrepareOk(m) => m.header().operation,
        }
    }
}

impl<T: ConsensusHeader> TryFrom<Message<T>> for MessageBag {
    type Error = ConsensusError;

    fn try_from(value: Message<T>) -> Result<Self, Self::Error> {
        let command = value.as_generic().header().command;
        let buffer = value.into_inner();

        match command {
            Command2::Request => {
                let msg = unsafe { Message::<RequestHeader>::from_buffer_unchecked(buffer) };
                Ok(Self::Request(msg))
            }
            Command2::Prepare => {
                let msg = unsafe { Message::<PrepareHeader>::from_buffer_unchecked(buffer) };
                Ok(Self::Prepare(msg))
            }
            Command2::PrepareOk => {
                let msg = unsafe { Message::<PrepareOkHeader>::from_buffer_unchecked(buffer) };
                Ok(Self::PrepareOk(msg))
            }
            other => Err(ConsensusError::InvalidCommand {
                expected: Command2::Reserved,
                found: other,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Message;
    use crate::consensus::{Command2, GenericHeader, RequestHeader};
    use bytes::{Bytes, BytesMut};

    #[test]
    fn generic_message_from_zeroed_buffer() {
        let mut buf = BytesMut::zeroed(384);
        let header =
            bytemuck::checked::try_from_bytes_mut::<GenericHeader>(&mut buf[..256]).unwrap();
        header.size = 384;
        header.cluster = 42;

        let msg = Message::<GenericHeader>::from_bytes(buf.freeze()).unwrap();
        assert_eq!(msg.header().cluster, 42);
        assert_eq!(msg.header().size, 384);
        assert_eq!(msg.body().len(), 128);
    }

    #[test]
    fn request_message_roundtrip() {
        let mut buf = BytesMut::zeroed(320);
        let header =
            bytemuck::checked::try_from_bytes_mut::<RequestHeader>(&mut buf[..256]).unwrap();
        header.size = 320;
        header.command = Command2::Request;
        header.client = 12345;
        header.operation = crate::consensus::Operation::CreateStream;

        let msg = Message::<RequestHeader>::from_bytes(buf.freeze()).unwrap();
        assert_eq!(msg.header().client, 12345);
        assert_eq!(
            msg.header().operation,
            crate::consensus::Operation::CreateStream
        );
        assert_eq!(msg.body().len(), 64);
    }

    #[test]
    fn too_small_buffer_rejected() {
        let buf = Bytes::from(vec![0u8; 100]); // < 256
        assert!(Message::<GenericHeader>::from_bytes(buf).is_err());
    }

    #[test]
    fn into_generic_and_back() {
        let mut buf = BytesMut::zeroed(256);
        let header =
            bytemuck::checked::try_from_bytes_mut::<RequestHeader>(&mut buf[..256]).unwrap();
        header.size = 256;
        header.command = Command2::Request;

        let msg = Message::<RequestHeader>::from_bytes(buf.freeze()).unwrap();
        let generic = msg.into_generic();
        assert_eq!(generic.header().command, Command2::Request);

        let back = generic.try_into_typed::<RequestHeader>().unwrap();
        assert_eq!(back.header().command, Command2::Request);
    }

    #[test]
    fn transmute_header_generic_to_request() {
        let mut buf = BytesMut::zeroed(320);
        let header =
            bytemuck::checked::try_from_bytes_mut::<GenericHeader>(&mut buf[..256]).unwrap();
        header.size = 320;
        header.cluster = 99;

        let generic = Message::<GenericHeader>::from_bytes(buf.freeze()).unwrap();
        let request = generic.transmute_header(|old, new: &mut RequestHeader| {
            new.size = old.size;
            new.command = Command2::Request;
            new.cluster = old.cluster;
            new.client = 42;
            new.operation = crate::consensus::Operation::CreateStream;
        });
        assert_eq!(request.header().command, Command2::Request);
        assert_eq!(request.header().client, 42);
        assert_eq!(request.header().cluster, 99);
        assert_eq!(request.header().size, 320);
        assert_eq!(request.body().len(), 64);
    }

    #[test]
    fn wrong_type_conversion_fails() {
        let mut buf = BytesMut::zeroed(256);
        let header =
            bytemuck::checked::try_from_bytes_mut::<GenericHeader>(&mut buf[..256]).unwrap();
        header.size = 256;
        header.command = Command2::Reserved; // not Request

        let msg = Message::<GenericHeader>::from_bytes(buf.freeze()).unwrap();
        assert!(msg.try_into_typed::<RequestHeader>().is_err());
    }
}
