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

use crate::types::consensus::header::{
    self, CommitHeader, ConsensusHeader, GenericHeader, PrepareHeader, PrepareOkHeader, ReplyHeader,
};
use bytes::Bytes;
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct Message<H: ConsensusHeader> {
    buffer: Bytes,
    _marker: PhantomData<H>,
}

impl<H> Message<H>
where
    H: ConsensusHeader,
{
    /// Create a new message from a buffer.
    ///
    /// # Safety
    ///
    /// The buffer must:
    /// - be at least `size_of::<H>()` bytes long
    /// - contain a valid header at the start
    /// - be properly aligned for type H
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - buffer is too small for the header
    /// - buffer is too small for the size specified in the header
    /// - header validation fails
    #[allow(unused)]
    pub fn from_bytes(buffer: Bytes) -> Result<Self, header::ConsensusError> {
        // verify minimum size
        if buffer.len() < size_of::<H>() {
            return Err(header::ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: header::Command2::Reserved,
            });
        }

        let message = Self {
            buffer,
            _marker: PhantomData,
        };

        // validate the header
        message.header().validate()?;

        // verify buffer size matches header.size
        let header_size = message.header().size() as usize;
        if message.buffer.len() < header_size {
            return Err(header::ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: header::Command2::Reserved,
            });
        }

        Ok(message)
    }

    /// Create a new message with a specific size, initializing the buffer with zeros.
    ///
    /// The header will be zeroed and must be initialized by the caller.
    #[allow(unused)]
    pub fn new(size: usize) -> Self {
        assert!(size >= size_of::<H>(), "Size must be at least header size");
        let buffer = Bytes::from(vec![0u8; size]);
        Self {
            buffer,
            _marker: PhantomData,
        }
    }

    /// Transmute the header to a different type, using the provided function to modify the new header.
    pub fn transmute_header<T: ConsensusHeader>(self, f: impl FnOnce(H, &mut T)) -> Message<T> {
        assert_eq!(size_of::<H>(), size_of::<T>());

        // Copy old header to stack to avoid UB.
        let old_header = *self.header();

        // Safety: We ensured that size_of::<H>() == size_of::<T>()
        // On top of that, there is going to be only one reference to buffer during this function call
        // so no other references can observe the mutation.
        // In the future we can replace the `Bytes` buffer with something that does not allow sharing between different threads.
        let buffer = self.into_inner();
        unsafe {
            let ptr = buffer.as_ptr() as *mut u8;
            let slice = std::slice::from_raw_parts_mut(ptr, size_of::<T>());
            let new_header = bytemuck::from_bytes_mut(slice);
            f(old_header, new_header);
        }

        Message {
            buffer,
            _marker: PhantomData,
        }
    }

    /// Get a reference to the header using zero-copy access.
    ///
    /// This uses `bytemuck::from_bytes` to cast the buffer to the header type
    /// without any copying or allocation.
    #[inline]
    #[allow(unused)]
    pub fn header(&self) -> &H {
        let header_bytes = &self.buffer[..size_of::<H>()];
        bytemuck::from_bytes(header_bytes)
    }

    /// Get a reference to the message body (everything after the header).
    ///
    /// Returns an empty slice if there is no body.
    #[inline]
    #[allow(unused)]
    pub fn body(&self) -> &[u8] {
        let header_size = size_of::<H>();
        let total_size = self.header().size() as usize;

        if total_size > header_size {
            &self.buffer[header_size..total_size]
        } else {
            &[]
        }
    }

    /// Get the message body as zero-copy `Bytes`.
    ///
    /// Returns an empty `Bytes` if there is no body.
    #[inline]
    #[allow(unused)]
    pub fn body_bytes(&self) -> Bytes {
        let header_size = size_of::<H>();
        let total_size = self.header().size() as usize;

        if total_size > header_size {
            self.buffer.slice(header_size..total_size)
        } else {
            Bytes::new()
        }
    }

    /// Get the complete message as bytes (header + body).
    #[inline]
    #[allow(unused)]
    pub fn as_bytes(&self) -> &[u8] {
        let total_size = self.header().size() as usize;
        &self.buffer[..total_size]
    }

    /// Convert into the underlying buffer.
    #[inline]
    #[allow(unused)]
    pub fn into_inner(self) -> Bytes {
        self.buffer
    }

    /// Create a message from a buffer without validation.
    ///
    /// # Safety
    ///
    /// This is private and skips validation. Use only when:
    /// - The buffer is already validated
    /// - If doing a zero-cost type conversion (like to GenericHeader)
    #[inline]
    #[allow(unused)]
    unsafe fn from_buffer_unchecked(buffer: Bytes) -> Self {
        Self {
            buffer,
            _marker: PhantomData,
        }
    }

    /// Convert to a generic message (erasing the specific header type).
    ///
    /// This allows treating any message as a generic message for common operations.
    #[allow(unused)]
    pub fn into_generic(self) -> Message<header::GenericHeader> {
        unsafe { Message::from_buffer_unchecked(self.buffer) }
    }

    /// Get a reference to this message as a generic message.
    #[allow(unused)]
    pub fn as_generic(&self) -> &Message<header::GenericHeader> {
        // SAFETY: Message<H> and Message<GenericHeader> have the same memory layout
        // because they only differ in the PhantomData type parameter
        unsafe { &*(self as *const Self as *const Message<header::GenericHeader>) }
    }

    /// Try to convert this message to a different header type.
    ///
    /// This validates that the command in the header matches the target type's
    /// expected command before performing the conversion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The command doesn't match the target type
    /// - The target header validation fails
    #[allow(unused)]
    pub fn try_into_typed<T>(self) -> Result<Message<T>, header::ConsensusError>
    where
        T: ConsensusHeader,
    {
        if self.buffer.len() < size_of::<T>() {
            return Err(header::ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: header::Command2::Reserved,
            });
        }

        let generic = self.as_generic();
        if generic.header().command != T::COMMAND {
            return Err(header::ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: generic.header().command,
            });
        }

        let new_message = unsafe { Message::<T>::from_buffer_unchecked(self.buffer) };

        new_message.header().validate()?;

        Ok(new_message)
    }

    /// Try to get a reference to this message as a different header type.
    ///
    /// This is similar to `try_into_typed` but borrows instead of consuming.
    #[allow(unused)]
    pub fn try_as_typed<T>(&self) -> Result<&Message<T>, header::ConsensusError>
    where
        T: ConsensusHeader,
    {
        // check buffer size
        if self.buffer.len() < size_of::<T>() {
            return Err(header::ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: header::Command2::Reserved,
            });
        }

        // check the command matches
        let generic = self.as_generic();
        if generic.header().command != T::COMMAND {
            return Err(header::ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: generic.header().command,
            });
        }

        let typed_message = unsafe { &*(self as *const Self as *const Message<T>) };

        // validate the header
        typed_message.header().validate()?;

        Ok(typed_message)
    }
}

#[derive(Debug)]
#[allow(unused)]
pub enum MessageBag {
    Generic(Message<GenericHeader>),
    Prepare(Message<PrepareHeader>),
    PrepareOk(Message<PrepareOkHeader>),
    Commit(Message<CommitHeader>),
    Reply(Message<ReplyHeader>),
}

impl MessageBag {
    #[allow(unused)]
    pub fn command(&self) -> header::Command2 {
        match self {
            MessageBag::Generic(message) => message.header().command,
            MessageBag::Prepare(message) => message.header().command,
            MessageBag::PrepareOk(message) => message.header().command,
            MessageBag::Commit(message) => message.header().command,
            MessageBag::Reply(message) => message.header().command,
        }
    }

    #[allow(unused)]
    pub fn size(&self) -> u32 {
        match self {
            MessageBag::Generic(message) => message.header().size(),
            MessageBag::Prepare(message) => message.header().size(),
            MessageBag::PrepareOk(message) => message.header().size(),
            MessageBag::Commit(message) => message.header().size(),
            MessageBag::Reply(message) => message.header().size(),
        }
    }
}

impl<T> From<Message<T>> for MessageBag
where
    T: ConsensusHeader,
{
    fn from(value: Message<T>) -> Self {
        let command = value.as_generic().header().command;

        let buffer = value.into_inner();

        // SAFETY: All Message<H> types have identical memory layout (only PhantomData differs).
        // We've validated the command when the original message was created.
        match command {
            header::Command2::Prepare => {
                let msg =
                    unsafe { Message::<header::PrepareHeader>::from_buffer_unchecked(buffer) };
                MessageBag::Prepare(msg)
            }
            header::Command2::Commit => {
                let msg = unsafe { Message::<header::CommitHeader>::from_buffer_unchecked(buffer) };
                MessageBag::Commit(msg)
            }
            header::Command2::Reply => {
                let msg = unsafe { Message::<header::ReplyHeader>::from_buffer_unchecked(buffer) };
                MessageBag::Reply(msg)
            }
            _ => unreachable!(
                "For now we only support Prepare, Commit, and Reply. In the future we will support more commands. Command2: {command:?}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    trait MessageFactory: ConsensusHeader + Sized {
        fn create_test() -> Message<Self>;
    }

    impl MessageFactory for header::GenericHeader {
        fn create_test() -> Message<Self> {
            let header_size = size_of::<Self>();
            let body_size = 128;
            let total_size = header_size + body_size;

            let mut buffer = BytesMut::zeroed(total_size);

            let header = bytemuck::from_bytes_mut::<Self>(&mut buffer[..header_size]);

            header.checksum = 123456;
            header.cluster = 12345;
            header.size = total_size as u32;
            header.command = header::Command2::Reserved;

            for (i, item) in buffer
                .iter_mut()
                .enumerate()
                .take(total_size)
                .skip(header_size)
            {
                *item = (i % 256) as u8;
            }

            Message::<Self>::from_bytes(buffer.freeze()).unwrap()
        }
    }

    impl MessageFactory for header::PrepareHeader {
        fn create_test() -> Message<Self> {
            let header_size = size_of::<Self>();
            let body_size = 64;
            let total_size = header_size + body_size;

            let mut buffer = BytesMut::zeroed(total_size);

            let header = bytemuck::from_bytes_mut::<Self>(&mut buffer[..header_size]);

            header.checksum = 123456;
            header.checksum_body = 789012;
            header.cluster = 12345;
            header.size = total_size as u32;
            header.view = 1;
            header.command = header::Command2::Prepare;
            header.replica = 1;
            header.op = 100;
            header.commit = 99;
            header.timestamp = 1234567890;
            header.operation = header::Operation::CreateStream;

            Message::<Self>::from_bytes(buffer.freeze()).unwrap()
        }
    }

    impl MessageFactory for header::CommitHeader {
        fn create_test() -> Message<Self> {
            let header_size = size_of::<Self>();
            let total_size = 256;

            let mut buffer = BytesMut::zeroed(total_size);

            let header = bytemuck::from_bytes_mut::<Self>(&mut buffer[..header_size]);

            header.checksum = 123456;
            header.cluster = 12345;
            header.size = 256;
            header.view = 1;
            header.command = header::Command2::Commit;
            header.replica = 2;
            header.commit = 50;

            Message::<Self>::from_bytes(buffer.freeze()).unwrap()
        }
    }

    impl MessageFactory for header::ReplyHeader {
        fn create_test() -> Message<Self> {
            let header_size = size_of::<Self>();
            let body_size = 32;
            let total_size = header_size + body_size;

            let mut buffer = BytesMut::zeroed(total_size);

            let header = bytemuck::from_bytes_mut::<Self>(&mut buffer[..header_size]);

            header.checksum = 123456;
            header.cluster = 12345;
            header.size = total_size as u32;
            header.view = 1;
            header.command = header::Command2::Reply;
            header.replica = 3;
            header.op = 100;
            header.commit = 99;
            header.operation = header::Operation::CreateStream;

            Message::<Self>::from_bytes(buffer.freeze()).unwrap()
        }
    }

    #[test]
    fn test_message_creation_and_access() {
        let message = header::GenericHeader::create_test();

        assert_eq!(message.header().cluster, 12345);
        assert_eq!(message.header().command, header::Command2::Reserved);
        assert_eq!(
            message.body().len(),
            message.header().size() as usize - size_of::<header::GenericHeader>()
        );

        let body = message.body();
        let header_size = size_of::<header::GenericHeader>();
        for (i, &byte) in body.iter().enumerate() {
            let expected = ((i + header_size) % 256) as u8;
            assert_eq!(byte, expected);
        }
    }

    #[test]
    fn test_message_conversion() {
        let prepare_message = header::PrepareHeader::create_test();

        let original_bytes = prepare_message.as_bytes().to_vec();

        let generic_message = prepare_message.into_generic();
        assert_eq!(generic_message.header().command, header::Command2::Prepare);

        let prepare_again: Message<header::PrepareHeader> =
            generic_message.try_into_typed().unwrap();

        assert_eq!(prepare_again.header().op, 100);
        assert_eq!(prepare_again.header().view, 1);
        assert_eq!(prepare_again.header().cluster, 12345);

        let roundtrip_bytes = prepare_again.as_bytes().to_vec();

        assert_eq!(
            original_bytes, roundtrip_bytes,
            "Bytes should be identical after round-trip conversion"
        );
    }

    #[test]
    fn test_message_bag_from_prepare() {
        let prepare = header::PrepareHeader::create_test();
        let bag = MessageBag::from(prepare);

        assert_eq!(bag.command(), header::Command2::Prepare);
        assert!(matches!(bag, MessageBag::Prepare(_)));
        assert!(!matches!(bag, MessageBag::Commit(_)));
        assert!(!matches!(bag, MessageBag::Reply(_)));
        assert!(!matches!(bag, MessageBag::Generic(_)));
    }

    #[test]
    fn test_message_bag_from_commit() {
        let commit = header::CommitHeader::create_test();
        let bag = MessageBag::from(commit);

        assert_eq!(bag.command(), header::Command2::Commit);
        assert!(!matches!(bag, MessageBag::Prepare(_)));
        assert!(matches!(bag, MessageBag::Commit(_)));
        assert!(!matches!(bag, MessageBag::Reply(_)));
        assert!(!matches!(bag, MessageBag::Generic(_)));
    }

    #[test]
    fn test_message_bag_from_reply() {
        let reply = header::ReplyHeader::create_test();
        let bag = MessageBag::from(reply);

        assert_eq!(bag.command(), header::Command2::Reply);
        assert!(!matches!(bag, MessageBag::Prepare(_)));
        assert!(!matches!(bag, MessageBag::Commit(_)));
        assert!(matches!(bag, MessageBag::Reply(_)));
        assert!(!matches!(bag, MessageBag::Generic(_)));
    }
}

// TODO: Header generic
// TODO: We will have to impl something like this (NOT ONE TO ONE JUST A SKETCH) as we will use `bytemuck`:
/*
// Generic Message type
#[repr(C)]
pub struct Message<H: Header = GenericHeader> {
    buffer: AlignedBuffer<ALIGNED_TO_HEADER_SIZE>,
    _phantom: PhantomData<H>,
}

// Trait that all headers must implement
pub trait Header: Sized {
    const COMMAND: Command2;

    fn size(&self) -> u32;
    fn command(&self) -> Command2;
    fn checksum(&self) -> u128;
}

// Command2 enum (simplified)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Command2 {
    reserved = 0,

    ping = 1,
    pong = 2,

    request = 5,
    prepare = 6,
    prepare_ok = 7,
    reply = 8,
    commit = 9,

    start_view_change = 10,
    do_view_change = 11,
    start_view = 24,
    // ... etc
}

// Generic header for base Message
#[repr(C)]
pub struct GenericHeader {
    checksum: u128,
    command: Command2,
    size: u32,
    // ... other fields
}

// first 128 bytes GenericHeader (checksum, command, size, etc....)
// second 128 bytes specific header (PrepareHeader, CommitHeader, etc....)

impl Header for GenericHeader {
    const COMMAND: Command2 = Command::Reserved;

    fn size(&self) -> u32 { self.size }
    fn command(&self) -> Command2 { self.command }
    fn checksum(&self) -> u128 { self.checksum }
}

// Specific header types
#[repr(C)]
pub struct PrepareHeader {
    checksum: u128,
    command: Command2,
    size: u32,
    // ... prepare-specific fields
    view: u32,
    op: u64,
    commit: u64,
}

impl Header for PrepareHeader {
    const COMMAND: Command2 = Command::Prepare;

    fn size(&self) -> u32 { self.size }
    fn command(&self) -> Command2 { self.command }
    fn checksum(&self) -> u128 { self.checksum }
}
*/

// And then for Message impl
/*
impl<H: Header> Message<H> {
    // Access header (no stored pointer needed!)
    pub fn header(&self) -> &H {
        assert!(size_of::<H>() <= ALIGNED_TO_HEADER_SIZE);
        unsafe { &*(self.buffer.as_ptr() as *const H) }
    }

    pub fn header_mut(&mut self) -> &mut H {
        unsafe { &mut *(self.buffer.as_mut_ptr() as *mut H) }
    }

    pub fn body(&self) -> &[u8] {
        let header_size = size_of::<H>();
        let total_size = self.header().size() as usize;
        &self.buffer[header_size..total_size]
    }

    pub fn body_mut(&mut self) -> &mut [u8] {
        let header_size = size_of::<H>();
        let total_size = self.header().size() as usize;
        &mut self.buffer.as_mut()[header_size..total_size]
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer[..self.header().size() as usize]
    }

    pub fn base(&self) -> &Message<GenericHeader> {
        unsafe { &*(self as *const Self as *const Message<GenericHeader>) }
    }

    pub fn base_mut(&mut self) -> &mut Message<GenericHeader> {
        unsafe { &mut *(self as *mut Self as *mut Message<GenericHeader>) }
    }

    pub fn try_into<T: Header>(self) -> Option<Message<T>> {
        if self.header().command() == T::COMMAND {
            Some(unsafe { std::mem::transmute(self) })
        } else {
            None
        }
    }
}
*/

// Then for the `Request` header we will have to store an `Operation` enum
/*
 Define your operation enum
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    // Metadata operations
    CreateTopic = 1,
    DeleteTopic = 2,
    ListTopics = 3,
    CreatePartition = 4,

    // Partition operations
    Produce = 100,
    Consume = 101,
    Fetch = 102,
}

// Specific header types
#[repr(C)]
pub struct PrepareHeader {
    checksum: u128,
    command: Command2,
    size: u32,
    // ... prepare-specific fields
    view: u32,
    op: u64,
    commit: u64,

    // second 128 bytes
    operation: Operation,
}
*/

// Which will have an method that returns discriminator between Metadata and Partition requests

// TODO: Fill this enum
// #[expect(unused)]
// pub enum MessageBag {
//     Void,
// }

// impl<H> From<Message<H>> for MessageBag
// where
//     H: ConsensusHeader,
// {
//     fn from(_value: Message<H>) -> Self {
//         MessageBag::Void
//     }
// }
