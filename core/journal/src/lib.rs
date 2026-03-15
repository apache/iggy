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

use std::ops::Deref;

pub trait Journal<S>
where
    S: Storage,
{
    type Header;
    type Entry;
    type HeaderRef<'a>: Deref<Target = Self::Header>
    where
        Self: 'a;

    fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>>;
    fn previous_header(&self, header: &Self::Header) -> Option<Self::HeaderRef<'_>>;

    fn append(&self, entry: Self::Entry) -> impl Future<Output = ()>;
    fn entry(&self, header: &Self::Header) -> impl Future<Output = Option<Self::Entry>>;
}

// TODO: Move to other crate.
pub trait Storage {
    type Buffer;

    fn write(&self, buf: Self::Buffer) -> impl Future<Output = usize>;
    // TODO: Get rid of the `len` usize, we need to do changes in `Simulator` in order to support that.
    // Maybe we should go back to passing in the `Buffer` again, but I am not sure how to handle it in the `Partitions Journal`, since we use in-memory impl
    // which extracts the buffer out of the `Vec<Message>` and we don't need to allocate a new buffer.
    fn read(&self, offset: usize, len: usize) -> impl Future<Output = Self::Buffer>;
}

pub trait JournalHandle {
    type Storage: Storage;
    type Target: Journal<Self::Storage>;

    fn handle(&self) -> &Self::Target;
}
