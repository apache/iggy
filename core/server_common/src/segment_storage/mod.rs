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

mod index_reader;
mod index_writer;
mod messages_reader;
mod messages_writer;

use iggy_common::IggyError;
use std::rc::Rc;

pub use index_reader::IndexReader;
pub use index_writer::IndexWriter;
pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;

unsafe impl Send for SegmentStorage {}

#[derive(Debug, Clone, Default)]
pub struct SegmentStorage {
    pub messages_writer: Option<Rc<MessagesWriter>>,
    pub messages_reader: Option<Rc<MessagesReader>>,
    pub index_writer: Option<Rc<IndexWriter>>,
    pub index_reader: Option<Rc<IndexReader>>,
}

impl SegmentStorage {
    pub async fn new(
        messages_path: &str,
        index_path: &str,
        messages_size: u64,
        indexes_size: u64,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let size = Rc::new(std::sync::atomic::AtomicU64::new(messages_size));
        let indexes_size = Rc::new(std::sync::atomic::AtomicU64::new(indexes_size));
        let messages_writer = Rc::new(MessagesWriter::new(messages_path, size, file_exists).await?);

        let index_writer = Rc::new(IndexWriter::new(index_path, indexes_size, file_exists).await?);

        if file_exists {
            messages_writer.fsync().await?;
            index_writer.fsync().await?;
        }

        let messages_reader = Rc::new(MessagesReader::new(messages_path).await?);
        let index_reader = Rc::new(IndexReader::new(index_path).await?);
        Ok(Self {
            messages_writer: Some(messages_writer),
            messages_reader: Some(messages_reader),
            index_writer: Some(index_writer),
            index_reader: Some(index_reader),
        })
    }

    pub fn shutdown(&mut self) -> (Option<Rc<MessagesWriter>>, Option<Rc<IndexWriter>>) {
        let messages_writer = self.messages_writer.take();
        let index_writer = self.index_writer.take();
        (messages_writer, index_writer)
    }

    pub fn segment_and_index_paths(&self) -> (Option<String>, Option<String>) {
        let index_path = self.index_reader.as_ref().map(|reader| reader.path());
        let segment_path = self.messages_reader.as_ref().map(|reader| reader.path());
        (segment_path, index_path)
    }
}
