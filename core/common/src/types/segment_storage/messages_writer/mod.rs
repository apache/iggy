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

pub mod buffered;
pub mod direct;

use std::{rc::Rc, sync::atomic::AtomicU64};

pub use buffered::BufferedMessagesWriter;
pub use direct::DirectMessagesWriter;

use crate::{
    IggyByteSize, IggyError, IggyMessagesBatch, types::segment_storage::direct_file::SharedTail,
};

#[derive(Debug)]
pub enum MessagesWriterBackend {
    Direct(Box<DirectMessagesWriter>),
    Buffered(BufferedMessagesWriter),
}

#[derive(Debug)]
pub struct MessagesWriter {
    backend: MessagesWriterBackend,
}

impl MessagesWriter {
    pub async fn new(
        file_path: &str,
        messages_size_bytes: Rc<AtomicU64>,
        fsync: bool,
        file_exists: bool,
        direct_io_enabled: bool,
        direct_io_dsync: bool,
    ) -> Result<Self, IggyError> {
        let backend = if direct_io_enabled {
            MessagesWriterBackend::Direct(Box::new(
                DirectMessagesWriter::new(
                    file_path,
                    messages_size_bytes,
                    file_exists,
                    direct_io_dsync,
                )
                .await?,
            ))
        } else {
            MessagesWriterBackend::Buffered(
                BufferedMessagesWriter::new(file_path, messages_size_bytes, fsync, file_exists)
                    .await?,
            )
        };

        Ok(Self { backend })
    }

    pub async fn save_frozen_batches(
        &self,
        batches: &[IggyMessagesBatch],
    ) -> Result<IggyByteSize, IggyError> {
        match &self.backend {
            MessagesWriterBackend::Direct(w) => w.save_frozen_batches(batches).await,
            MessagesWriterBackend::Buffered(w) => w.save_frozen_batches(batches).await,
        }
    }

    pub async fn flush(&self) -> Result<(), IggyError> {
        match &self.backend {
            MessagesWriterBackend::Direct(w) => w.flush().await,
            MessagesWriterBackend::Buffered(w) => w.fsync().await,
        }
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        match &self.backend {
            MessagesWriterBackend::Direct(_) => Ok(()),
            MessagesWriterBackend::Buffered(w) => w.fsync().await,
        }
    }

    pub fn try_get_shared_tail(&self) -> Option<Rc<SharedTail>> {
        match &self.backend {
            MessagesWriterBackend::Direct(w) => Some(w.shared_tail()),
            MessagesWriterBackend::Buffered(_) => None,
        }
    }

    pub fn size_counter(&self) -> Rc<AtomicU64> {
        match &self.backend {
            MessagesWriterBackend::Direct(w) => w.size_counter(),
            MessagesWriterBackend::Buffered(w) => w.size_counter(),
        }
    }

    pub async fn flush_and_truncate(&self) -> Result<(), IggyError> {
        match &self.backend {
            MessagesWriterBackend::Direct(w) => w.flush_and_truncate().await,
            MessagesWriterBackend::Buffered(w) => w.fsync().await,
        }
    }

    pub fn path(&self) -> String {
        match &self.backend {
            MessagesWriterBackend::Direct(w) => w.path(),
            MessagesWriterBackend::Buffered(w) => w.path(),
        }
    }
}
