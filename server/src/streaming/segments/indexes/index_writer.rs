use super::{Index, INDEX_SIZE};
use error_set::ErrContext;
use iggy::error::IggyError;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};
use tracing::trace;

/// A dedicated struct for writing to the index file.
#[derive(Debug)]
pub struct SegmentIndexWriter {
    file_path: String,
    file: File,
    index_size_bytes: Arc<AtomicU64>,
    fsync: bool,
}

impl SegmentIndexWriter {
    /// Opens the index file in write mode.
    pub async fn new(
        file_path: &str,
        index_size_bytes: Arc<AtomicU64>,
        fsync: bool,
    ) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)
            .await
            .with_error_context(|error| format!("Failed to open index file: {file_path}. {error}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        let _ = file.sync_all().await.with_error_context(|error| {
            format!("Failed to fsync index file after creation: {file_path}. {error}",)
        });

        let actual_index_size = file
            .metadata()
            .await
            .with_error_context(|error| {
                format!("Failed to get metadata of index file: {file_path}. {error}")
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        index_size_bytes.store(actual_index_size, Ordering::Release);

        trace!("Opened index file for writing: {file_path}, size: {actual_index_size}");

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            index_size_bytes,
            fsync,
        })
    }

    /// Append multiple index records to the index file in a single operation.
    pub async fn save_indexes(&mut self, indexes: &[Index]) -> Result<(), IggyError> {
        if indexes.is_empty() {
            return Ok(());
        }

        let mut buf = vec![0u8; INDEX_SIZE as usize * indexes.len()];

        for (i, index) in indexes.iter().enumerate() {
            let start = i * INDEX_SIZE as usize;
            buf[start..start + 4].copy_from_slice(&index.offset.to_le_bytes());
            buf[start + 4..start + 8].copy_from_slice(&index.position.to_le_bytes());
            buf[start + 8..start + 16].copy_from_slice(&index.timestamp.to_le_bytes());
        }

        self.file
            .write_all(&buf)
            .await
            .with_error_context(|error| {
                format!(
                    "Failed to write {} indexes to file: {}. {error}",
                    indexes.len(),
                    self.file_path
                )
            })
            .map_err(|_| IggyError::CannotSaveIndexToSegment)?;

        if self.fsync {
            let _ = self.fsync().await;
        }

        self.index_size_bytes
            .fetch_add(INDEX_SIZE as u64 * indexes.len() as u64, Ordering::Release);

        Ok(())
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        self.file
            .sync_all()
            .await
            .with_error_context(|error| {
                format!("Failed to fsync index file: {}. {error}", self.file_path)
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }
}
