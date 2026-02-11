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

use crate::bus::SharedMemBus;
use bytes::Bytes;
use consensus::VsrConsensus;
use iggy_common::header::PrepareHeader;
use iggy_common::message::Message;
use journal::{Journal, JournalHandle, Storage};
use metadata::stm::consumer_group::ConsumerGroups;
use metadata::stm::stream::Streams;
use metadata::stm::user::Users;
use metadata::{IggyMetadata, MuxStateMachine, variadic};
use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashMap;

/// In-memory storage backend for testing/simulation
#[derive(Debug, Default)]
pub struct MemStorage {
    data: RefCell<Vec<u8>>,
    offset: Cell<u64>,
}

impl Storage for MemStorage {
    type Buffer = Vec<u8>;

    async fn write(&self, buf: Self::Buffer) -> usize {
        let len = buf.len();
        self.data.borrow_mut().extend_from_slice(&buf);
        self.offset.set(self.offset.get() + len as u64);
        len
    }

    async fn read(&self, offset: usize, mut buffer: Self::Buffer) -> Self::Buffer {
        let data = self.data.borrow();
        let end = offset + buffer.len();
        if offset < data.len() && end <= data.len() {
            buffer[..].copy_from_slice(&data[offset..end]);
        }
        buffer
    }
}

// TODO: Replace with actual Journal, the only thing that we will need to change is the `Storage` impl for an in-memory one.
/// Generic in-memory journal implementation for testing/simulation
pub struct SimJournal<S: Storage> {
    storage: S,
    headers: UnsafeCell<HashMap<u64, PrepareHeader>>,
    offsets: UnsafeCell<HashMap<u64, usize>>,
}

impl<S: Storage + Default> Default for SimJournal<S> {
    fn default() -> Self {
        Self {
            storage: S::default(),
            headers: UnsafeCell::new(HashMap::new()),
            offsets: UnsafeCell::new(HashMap::new()),
        }
    }
}

impl<S: Storage> std::fmt::Debug for SimJournal<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimJournal")
            .field("storage", &"<Storage>")
            .field("headers", &"<UnsafeCell>")
            .field("offsets", &"<UnsafeCell>")
            .finish()
    }
}

impl<S: Storage<Buffer = Vec<u8>>> Journal<S> for SimJournal<S> {
    type Header = PrepareHeader;
    type Entry = Message<PrepareHeader>;
    type HeaderRef<'a>
        = &'a PrepareHeader
    where
        Self: 'a;

    // TODO(hubcio): validate that the caller's checksum matches the stored
    // header - currently this looks up by op only, ignoring the checksum.
    // A real journal implementation must reject mismatches.
    async fn entry(&self, header: &Self::Header) -> Option<Self::Entry> {
        let headers = unsafe { &*self.headers.get() };
        let offsets = unsafe { &*self.offsets.get() };

        let header = headers.get(&header.op)?;
        let offset = *offsets.get(&header.op)?;

        let buffer = vec![0; header.size as usize];
        let buffer = self.storage.read(offset, buffer).await;
        let message =
            Message::from_bytes(Bytes::from(buffer)).expect("simulator: bytes should be valid");
        Some(message)
    }

    fn previous_header(&self, header: &Self::Header) -> Option<Self::HeaderRef<'_>> {
        if header.op == 0 {
            return None;
        }
        unsafe { &*self.headers.get() }.get(&(header.op - 1))
    }

    async fn append(&self, entry: Self::Entry) {
        let header = *entry.header();
        let message_bytes = entry.as_bytes();

        let bytes_written = self.storage.write(message_bytes.to_vec()).await;

        let current_offset = unsafe { &mut *self.offsets.get() }
            .values()
            .last()
            .cloned()
            .unwrap_or_default();

        unsafe { &mut *self.headers.get() }.insert(header.op, header);
        unsafe { &mut *self.offsets.get() }.insert(header.op, current_offset + bytes_written);
    }

    fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>> {
        let headers = unsafe { &*self.headers.get() };
        headers.get(&(idx as u64))
    }
}

impl JournalHandle for SimJournal<MemStorage> {
    type Storage = MemStorage;
    type Target = SimJournal<MemStorage>;

    fn handle(&self) -> &Self::Target {
        self
    }
}

#[derive(Debug, Default)]
pub struct SimSnapshot {}

/// Type aliases for simulator metadata
pub type SimMuxStateMachine = MuxStateMachine<variadic!(Users, Streams, ConsumerGroups)>;
pub type SimMetadata = IggyMetadata<
    VsrConsensus<SharedMemBus>,
    SimJournal<MemStorage>,
    SimSnapshot,
    SimMuxStateMachine,
>;

/// Type alias for simulator partitions
pub type ReplicaPartitions = partitions::IggyPartitions<VsrConsensus<SharedMemBus>>;
