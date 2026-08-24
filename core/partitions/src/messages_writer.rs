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

use compio::{
    fs::{File, OpenOptions},
    io::AsyncWriteAtExt,
};
use iggy_common::{IggyByteSize, IggyError};
use server_common::iobuf::Frozen;
use std::{
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::{error, warn};

#[cfg(target_os = "linux")]
use nix::fcntl::{FallocateFlags, fallocate};

const MAX_IOV_COUNT: usize = 1024;

#[derive(Debug)]
pub struct MessagesWriter {
    file_path: String,
    file: File,
    messages_size_bytes: Rc<AtomicU64>,
    fsync: bool,
}

impl MessagesWriter {
    /// Creates a messages writer backed by the segment file at `file_path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, synchronized, or queried for
    /// metadata, or if the on-disk length does not match the seeded size counter.
    pub async fn new(
        file_path: &str,
        messages_size_bytes: Rc<AtomicU64>,
        fsync: bool,
        file_exists: bool,
        preallocate_size: Option<IggyByteSize>,
    ) -> Result<Self, IggyError> {
        let mut opts = OpenOptions::new();
        opts.write(true);
        if !file_exists {
            opts.create(true);
        }
        let file = opts
            .open(file_path)
            .await
            .map_err(|_| IggyError::CannotReadFile)?;

        if let Some(preallocate_size) = preallocate_size {
            preallocate_file(&file, file_path, preallocate_size.as_bytes_u64());
        }

        if file_exists {
            file.sync_all()
                .await
                .map_err(|_| IggyError::CannotWriteToFile)?;

            let actual_messages_size = file
                .metadata()
                .await
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            // Refusal rationale documented on `IggyError::SegmentSizeMismatchAtOpen`.
            let expected_messages_size = messages_size_bytes.load(Ordering::Relaxed);
            if actual_messages_size != expected_messages_size {
                error!(
                    target: "iggy.partitions.storage",
                    file = file_path,
                    on_disk_size = actual_messages_size,
                    expected_size = expected_messages_size,
                    "segment messages file size does not match the seeded size at open"
                );
                return Err(IggyError::SegmentSizeMismatchAtOpen(
                    actual_messages_size,
                    expected_messages_size,
                ));
            }
        }

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            messages_size_bytes,
            fsync,
        })
    }

    /// Appends a batch of frozen message buffers to the segment file.
    ///
    /// # Errors
    ///
    /// Returns an error if any chunk cannot be written or synced to disk.
    pub async fn save_frozen_batches<const ALIGN: usize>(
        &self,
        buffers: &[Frozen<ALIGN>],
    ) -> Result<IggyByteSize, IggyError> {
        let messages_size: u64 = buffers.iter().map(|buffer| buffer.len() as u64).sum();

        if messages_size == 0 {
            return Ok(IggyByteSize::from(0));
        }

        let position = self.messages_size_bytes.load(Ordering::Relaxed);
        write_frozen_chunked(&self.file, &self.file_path, position, buffers).await?;

        if self.fsync {
            self.fsync().await?;
        }

        self.messages_size_bytes
            .fetch_add(messages_size, Ordering::Release);

        Ok(IggyByteSize::from(messages_size))
    }

    /// Roll the in-memory write cursor back by `bytes`, undoing the advance of a
    /// `save_frozen_batches` whose batch was written but whose companion index
    /// save then failed. The committed prefix stays resident and is
    /// re-persisted on the next `commit_messages`; rewinding the cursor makes
    /// that retry overwrite the same region instead of appending a second copy
    /// of the committed batch. Crash-safe without truncating: the rewound
    /// bytes are whole batch records past the last index entry, so boot
    /// recovery's indexed walk absorbs them and its truncate is a no-op.
    pub(crate) fn rewind(&self, bytes: u64) {
        debug_assert!(
            bytes <= self.messages_size_bytes.load(Ordering::Relaxed),
            "rewind underflow: bytes ({bytes}) exceeds the write cursor"
        );
        self.messages_size_bytes.fetch_sub(bytes, Ordering::Release);
    }

    #[must_use]
    pub fn path(&self) -> String {
        self.file_path.clone()
    }

    /// Flushes buffered segment file contents to disk.
    ///
    /// Uses `fdatasync` (data only): segment files are append-only and the
    /// size change is tracked in datasync metadata on Linux, so the inode
    /// metadata fsync of `sync_all` adds latency without correctness gain.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be synchronized.
    pub async fn fsync(&self) -> Result<(), IggyError> {
        self.file
            .sync_data()
            .await
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn preallocate_file(file: &File, file_path: &str, len: u64) {
    let Ok(len) = i64::try_from(len) else {
        warn!(
            target: "iggy.partitions.storage",
            file = file_path,
            preallocate_len = len,
            "file preallocation size is unsupported, using buffered allocation"
        );
        return;
    };

    // Runs INLINE on the shard thread, deliberately. `server_common::executor`
    // sets `thread_pool_limit(0)` on the shard proactor, so `spawn_blocking`
    // has no worker to park a task on and compio panics the shard outright with
    // "the thread pool is needed but no worker thread is running". (That limit
    // is skipped on macOS/aarch64, where the pool does exist -- see the FIXME
    // there -- so the panic is Linux-and-most-targets, not universal. This arm
    // is Linux-only regardless.)
    //
    // The cost is acceptable only because of what this call is: a metadata-only
    // extent reservation, microseconds on the local filesystems this option
    // exists for, and an immediate `EOPNOTSUPP` where the filesystem cannot do
    // it. Where it can genuinely block -- NFSv4.2 `ALLOCATE`, FUSE, a badly
    // fragmented extent tree forcing a journal commit -- it stalls the whole
    // core, not one partition, because nothing here yields. Preallocation is
    // opt-in per topic at creation for that reason; on such a deployment,
    // create topics without `preallocate_segments` rather than reintroducing a
    // pool the shard runtime does not have.
    if let Err(error) = fallocate(file, FallocateFlags::FALLOC_FL_KEEP_SIZE, 0, len) {
        warn!(
            target: "iggy.partitions.storage",
            file = file_path,
            preallocate_len = len,
            %error,
            "file preallocation failed, using buffered allocation"
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn preallocate_file(_file: &File, file_path: &str, _len: u64) {
    warn!(
        target: "iggy.partitions.storage",
        file = file_path,
        "file preallocation is unavailable on this platform, using buffered allocation"
    );
}

async fn write_frozen_chunked<const ALIGN: usize>(
    file: &File,
    file_path: &str,
    mut position: u64,
    buffers: &[Frozen<ALIGN>],
) -> Result<(), IggyError> {
    for chunk in buffers.chunks(MAX_IOV_COUNT) {
        let chunk_size: usize = chunk.iter().map(Frozen::len).sum();
        let chunk_vec: Vec<_> = chunk.to_vec();

        let (result, _) = (&*file)
            .write_vectored_all_at(chunk_vec, position)
            .await
            .into();
        result.map_err(|err| {
            error!(
                target: "iggy.partitions.storage",
                file = file_path,
                write_position = position,
                %err,
                "failed to write frozen messages to segment file"
            );
            IggyError::CannotWriteToFile
        })?;

        position += chunk_size as u64;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[compio::test]
    async fn preallocated_file_keeps_logical_length() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("segment.log");
        let writer = MessagesWriter::new(
            path.to_str().unwrap(),
            Rc::new(AtomicU64::new(0)),
            false,
            false,
            Some(IggyByteSize::from(1024 * 1024_u64)),
        )
        .await
        .unwrap();

        assert_eq!(writer.file.metadata().await.unwrap().len(), 0);
    }

    #[compio::test]
    async fn given_seeded_size_diverging_from_disk_when_opening_existing_file_should_return_size_mismatch_error()
     {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("segment.log");
        std::fs::write(&path, [7u8; 128]).unwrap();

        let result = MessagesWriter::new(
            path.to_str().unwrap(),
            Rc::new(AtomicU64::new(129)),
            false,
            true,
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(IggyError::SegmentSizeMismatchAtOpen(128, 129))
        ));
    }
}
