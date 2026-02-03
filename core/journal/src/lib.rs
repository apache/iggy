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

// TODO: We already have a `Journal` trait inside of the `storage` module `journal.rs` file.
// But the interface was designed for partition log, not an generic journal.
pub trait Journal<S>
where
    S: Storage,
{
    type Header;
    type Entry;

    fn header(&self, idx: usize) -> Option<&Self::Header>;
    fn previous_header(&self, header: &Self::Header) -> Option<&Self::Header>;

    fn append(&self, entry: Self::Entry) -> impl Future<Output = ()>;
    fn entry(&self, header: &Self::Header) -> impl Future<Output = Option<Self::Entry>>;
}

// TODO: Move to other crate.
pub trait Storage {
    type Buffer;

    fn write(&self, buf: Self::Buffer) -> impl Future<Output = usize>;
    fn read(&self, offset: usize, buffer: Self::Buffer) -> impl Future<Output = Self::Buffer>;
}

pub trait JournalHandle {
    type Storage: Storage;
    type Target: Journal<Self::Storage>;

    fn handle(&self) -> &Self::Target;
}
