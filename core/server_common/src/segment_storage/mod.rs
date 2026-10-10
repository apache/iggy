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
mod messages_reader;

use crate::fatal::NoteDescriptorExhaustion;
use compio::fs::OpenOptions;
use err_trail::ErrContext;
use iggy_common::IggyError;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use tracing::{error, trace};

use crate::fs_utils::preallocate_file;

pub use index_reader::IndexReader;
pub use messages_reader::MessagesReader;

unsafe impl Send for SegmentStorage {}

/// The files of one segment. It holds no file descriptor: the readers keep
/// only a path, and the writers that append to the tail segment open their
/// own files.
#[derive(Debug, Clone, Default)]
pub struct SegmentStorage {
    /// Write cursor of the messages file, shared with the writer that appends
    /// to it. `None` for a sealed segment and for messages that the WAL owns.
    pub messages_size: Option<Rc<AtomicU64>>,
    pub messages_reader: Option<Rc<MessagesReader>>,
    /// Write cursor of the index file, shared with the writer that appends to
    /// it. `None` for a sealed segment.
    pub index_size: Option<Rc<AtomicU64>>,
    pub index_reader: Option<Rc<IndexReader>>,
}

impl SegmentStorage {
    /// The WAL owns message writes; this storage exposes only their committed prefix.
    pub async fn with_read_only_messages(
        messages_path: &str,
        index_path: &str,
        indexes_size: u64,
        file_exists: bool,
        preallocate_size: Option<u64>,
    ) -> Result<Self, IggyError> {
        let messages_reader = if file_exists && preallocate_size.is_none() {
            MessagesReader::new(messages_path).await?
        } else {
            let messages_file = compio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(!file_exists)
                .truncate(false)
                .open(messages_path)
                .await
                .note_descriptor_exhaustion(|| format!("opening {messages_path}"))
                .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.to_owned()))?;
            let mut changed = !file_exists;
            if let Some(size) = preallocate_size
                && messages_file
                    .metadata()
                    .await
                    .map_err(|_| IggyError::CannotReadFileMetadata)?
                    .len()
                    == 0
            {
                preallocate_file(&messages_file, Path::new(messages_path), size).await;
                changed = true;
            }
            if changed {
                messages_file
                    .sync_all()
                    .await
                    .map_err(|_| IggyError::CannotSyncFile)?;
            }
            MessagesReader::from_validated_path(messages_path)
        };
        prepare_for_writes(index_path, "index", indexes_size, file_exists).await?;
        Ok(Self {
            messages_size: None,
            messages_reader: Some(Rc::new(messages_reader)),
            index_size: Some(Rc::new(AtomicU64::new(indexes_size))),
            index_reader: Some(Rc::new(IndexReader::from_validated_path(index_path))),
        })
    }

    pub async fn new(
        messages_path: &str,
        index_path: &str,
        messages_size: u64,
        indexes_size: u64,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        prepare_for_writes(messages_path, "messages", messages_size, file_exists).await?;
        prepare_for_writes(index_path, "index", indexes_size, file_exists).await?;
        let messages_reader = Rc::new(MessagesReader::from_validated_path(messages_path));
        let index_reader = Rc::new(IndexReader::from_validated_path(index_path));
        Ok(Self {
            messages_size: Some(Rc::new(AtomicU64::new(messages_size))),
            messages_reader: Some(messages_reader),
            index_size: Some(Rc::new(AtomicU64::new(indexes_size))),
            index_reader: Some(index_reader),
        })
    }

    /// Drop the write cursors of a segment that takes no more writes.
    pub fn seal(&mut self) {
        self.messages_size = None;
        self.index_size = None;
    }

    pub fn segment_and_index_paths(&self) -> (Option<String>, Option<String>) {
        let index_path = self.index_reader.as_ref().map(|reader| reader.path());
        let segment_path = self.messages_reader.as_ref().map(|reader| reader.path());
        (segment_path, index_path)
    }
}

/// Open a segment file for reads and writes, then close it. The open proves
/// that the readers and the appending writer can open the file. A new file is
/// created empty. An existing file must be `expected_size` bytes long, and it
/// is synced, so the truncation of recovery is durable.
///
/// The appending writer opens its own descriptor, so keeping this one open
/// would cost a descriptor per segment and serve nothing.
async fn prepare_for_writes(
    path: &str,
    file_kind: &str,
    expected_size: u64,
    file_exists: bool,
) -> Result<(), IggyError> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    // `file_exists = false` asserts a fresh start; truncate so a
    // stale file from a partial prior attempt doesn't survive.
    if !file_exists {
        options.truncate(true);
    }
    let file = options
        .open(path)
        .await
        .note_descriptor_exhaustion(|| format!("opening {path}"))
        .error(|e: &std::io::Error| format!("Failed to open {file_kind} file: {path}. {e}"))
        .map_err(|_| IggyError::CannotReadFile)?;
    if !file_exists {
        trace!("Created {file_kind} file: {path}");
        return Ok(());
    }

    let actual_size = file
        .metadata()
        .await
        .error(|e: &std::io::Error| {
            format!("Failed to get metadata of {file_kind} file: {path}. {e}")
        })
        .map_err(|_| IggyError::CannotReadFileMetadata)?
        .len();
    // The caller seeds the size counter from recovered, validated bounds and
    // recovery truncates the file to them. A divergent on-disk length means
    // appending would resurrect or shear bytes those bounds exclude, so refuse
    // the open. See `IggyError::SegmentSizeMismatchAtOpen`.
    if actual_size != expected_size {
        error!(
            "{file_kind} file size on disk: {actual_size} does not match expected size: {expected_size}, file: {path}"
        );
        return Err(IggyError::SegmentSizeMismatchAtOpen(
            actual_size,
            expected_size,
        ));
    }
    file.sync_all()
        .await
        .error(|e: &std::io::Error| format!("Failed to fsync {file_kind} file: {path}. {e}"))
        .map_err(|_| IggyError::CannotWriteToFile)?;
    trace!("Checked {file_kind} file: {path}, size: {actual_size}");
    Ok(())
}
