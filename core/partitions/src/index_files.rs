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

//! Where the polls of remote segments keep the `.index` objects that they
//! fetched, next to the partition's local files.

use std::io;

use bytes::Bytes;
use compio::io::AsyncWriteAtExt;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_common::IggyError;
use server_common::fatal::NoteDescriptorExhaustion;

use crate::iggy_index_reader::IggyIndexReader;
use crate::state_transfer::fsync_dir;

/// The files that hold the fetched indexes of remote segments.
///
/// A file that is written, renamed into place and whose directory is synced
/// stays readable. A file that a crash cut short is never read: the name a
/// reader opens only ever names a whole index.
pub trait IndexFiles {
    /// Create the file at `path`, or empty the one there.
    fn create<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<Box<dyn IndexFile>>>;

    /// Move the file at `source` to `target`, in place of any file there.
    fn rename<'a>(&'a self, source: &'a str, target: &'a str)
    -> LocalBoxFuture<'a, io::Result<()>>;

    /// Make the names in `directory` durable.
    fn sync_directory<'a>(&'a self, directory: &'a str) -> LocalBoxFuture<'a, io::Result<()>>;

    /// Remove the file at `path`.
    fn remove<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<()>>;

    /// Open the index at `path` for reading.
    fn open<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, Result<IggyIndexReader, IggyError>>;
}

/// One index file that a fetch writes.
pub trait IndexFile {
    /// Write `bytes` at `position`.
    fn write_at(&mut self, bytes: Bytes, position: u64) -> LocalBoxFuture<'_, io::Result<()>>;

    /// Make what was written durable.
    fn sync(&mut self) -> LocalBoxFuture<'_, io::Result<()>>;
}

/// The node's local disk.
pub struct DiskIndexFiles;

impl IndexFiles for DiskIndexFiles {
    fn create<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<Box<dyn IndexFile>>> {
        async move {
            let file = compio::fs::File::create(path)
                .await
                .note_descriptor_exhaustion(|| format!("creating {path}"))?;
            Ok(Box::new(DiskIndexFile(file)) as Box<dyn IndexFile>)
        }
        .boxed_local()
    }

    fn rename<'a>(
        &'a self,
        source: &'a str,
        target: &'a str,
    ) -> LocalBoxFuture<'a, io::Result<()>> {
        compio::fs::rename(source, target).boxed_local()
    }

    fn sync_directory<'a>(&'a self, directory: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        fsync_dir(directory).boxed_local()
    }

    fn remove<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        compio::fs::remove_file(path).boxed_local()
    }

    fn open<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, Result<IggyIndexReader, IggyError>> {
        IggyIndexReader::new(path).boxed_local()
    }
}

struct DiskIndexFile(compio::fs::File);

impl IndexFile for DiskIndexFile {
    fn write_at(&mut self, bytes: Bytes, position: u64) -> LocalBoxFuture<'_, io::Result<()>> {
        async move {
            let compio::BufResult(written, _) = self.0.write_all_at(bytes, position).await;
            written
        }
        .boxed_local()
    }

    fn sync(&mut self) -> LocalBoxFuture<'_, io::Result<()>> {
        self.0.sync_all().boxed_local()
    }
}
