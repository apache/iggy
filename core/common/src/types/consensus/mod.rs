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

use std::marker::PhantomData;

use bytes::Bytes;

use crate::types::consensus::header::ConsensusHeader;

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
    const COMMAND: Command;

    fn size(&self) -> u32;
    fn command(&self) -> Command;
    fn checksum(&self) -> u128;
}

// Command enum (simplified)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Command {
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
    command: Command,
    size: u32,
    // ... other fields
}

// first 128 bytes GenericHeader (checksum, command, size, etc....)
// second 128 bytes specific header (PrepareHeader, CommitHeader, etc....)

impl Header for GenericHeader {
    const COMMAND: Command = Command::Reserved;

    fn size(&self) -> u32 { self.size }
    fn command(&self) -> Command { self.command }
    fn checksum(&self) -> u128 { self.checksum }
}

// Specific header types
#[repr(C)]
pub struct PrepareHeader {
    checksum: u128,
    command: Command,
    size: u32,
    // ... prepare-specific fields
    view: u32,
    op: u64,
    commit: u64,
}

impl Header for PrepareHeader {
    const COMMAND: Command = Command::Prepare;

    fn size(&self) -> u32 { self.size }
    fn command(&self) -> Command { self.command }
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
    command: Command,
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
#[expect(unused)]
pub enum MessageBag {
    Void,
}

#[expect(unused)]
pub struct Message<H>
where
    H: ConsensusHeader,
{
    buffer: Bytes,
    _h: PhantomData<H>,
}

impl<H> From<Message<H>> for MessageBag
where
    H: ConsensusHeader,
{
    fn from(_value: Message<H>) -> Self {
        MessageBag::Void
    }
}

pub(crate) mod header;
