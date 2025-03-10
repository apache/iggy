use super::{index::Index, INDEX_SIZE};
use error_set::ErrContext;
use iggy::error::IggyError;
use std::{
    fs::{File, OpenOptions},
    io::ErrorKind,
    os::unix::fs::FileExt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::task::spawn_blocking;
use tracing::{error, trace};

/// A dedicated struct for reading from the index file.
#[derive(Debug)]
pub struct SegmentIndexReader {
    file_path: String,
    file: Arc<File>,
    index_size_bytes: Arc<AtomicU64>,
}

impl SegmentIndexReader {
    /// Opens the index file in read-only mode.
    pub async fn new(file_path: &str, index_size_bytes: Arc<AtomicU64>) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .with_error_context(|error| format!("Failed to open index file: {file_path}. {error}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        let actual_index_size = file
            .metadata()
            .with_error_context(|error| {
                format!("Failed to get metadata of index file: {file_path}. {error}")
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        index_size_bytes.store(actual_index_size, Ordering::Release);

        trace!("Opened index file for reading: {file_path}, size: {actual_index_size}",);
        Ok(Self {
            file_path: file_path.to_string(),
            file: Arc::new(file),
            index_size_bytes,
        })
    }

    /// Loads all indexes from the index file.
    pub async fn load_all_indexes_impl(&self) -> Result<Vec<Index>, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            trace!("Index file {} is empty.", self.file_path);
            return Ok(Vec::new());
        }

        let buf = match self.read_at(0, file_size).await {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(Vec::new()),
            Err(error) => {
                error!(
                    "Error reading batch header at offset 0 in file {}: {error}",
                    self.file_path
                );
                return Err(IggyError::CannotReadFile);
            }
        };

        let indexes: Vec<Index> = buf
            .chunks_exact(INDEX_SIZE as usize)
            .map(parse_index)
            .collect::<Result<Vec<_>, IggyError>>()
            .with_error_context(|error| {
                format!(
                    "Failed to parse indexes in file {}: {error}",
                    self.file_path
                )
            })?;
        if indexes.len() as u32 != file_size / INDEX_SIZE {
            error!(
                "Loaded {} indexes from disk, expected {}, file {} is probably corrupted!",
                indexes.len(),
                file_size / INDEX_SIZE,
                self.file_path
            );
        }
        Ok(indexes)
    }

    pub async fn load_index_for_timestamp_impl(
        &self,
        timestamp: u64,
    ) -> Result<Option<Index>, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            trace!("Index file {} is empty.", self.file_path);
            return Ok(Some(Index::default()));
        }

        let total_indexes = file_size / INDEX_SIZE;
        trace!(
            "Searching for timestamp {} in {} indexes",
            timestamp,
            total_indexes
        );

        let mut left = 0;
        let mut right = total_indexes - 1;
        let mut result: Option<Index> = None;

        while left <= right {
            let mid = left + (right - left) / 2;
            let buf = match self.read_at(mid * INDEX_SIZE, INDEX_SIZE).await {
                Ok(buf) => buf,
                Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
                Err(error) => {
                    error!(
                        "Error reading index at position {} in file {}: {error}",
                        mid, self.file_path
                    );
                    return Err(IggyError::CannotReadFile);
                }
            };
            let current = parse_index(&buf)?;
            match current.timestamp.cmp(&timestamp) {
                std::cmp::Ordering::Equal => {
                    return Ok(Some(current));
                }
                std::cmp::Ordering::Less => {
                    result = Some(current);
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        return Ok(result.or(Some(Index::default())));
                    }
                    right = mid - 1;
                }
            }
        }
        trace!("Index for timestamp {timestamp}: {result:?}");
        Ok(result)
    }

    /// Returns the size of the index file in bytes.
    fn file_size(&self) -> u32 {
        self.index_size_bytes.load(Ordering::Acquire) as u32
    }

    /// Reads a specified number of bytes from the index file at a given offset.
    async fn read_at(&self, offset: u32, len: u32) -> Result<Vec<u8>, std::io::Error> {
        let file = self.file.clone();
        spawn_blocking(move || {
            let mut buf = vec![0u8; len as usize];
            file.read_exact_at(&mut buf, offset as u64)?;
            Ok(buf)
        })
        .await?
    }

    /// Gets the nth index from the index file.
    ///
    /// The index position is 0-based (first index is at position 0).
    /// Returns None if the specified position is out of bounds.
    pub async fn load_nth_index(&self, position: u32) -> Result<Option<Index>, IggyError> {
        let file_size = self.file_size();
        let total_indexes = file_size / INDEX_SIZE;

        if position >= total_indexes {
            trace!(
                "Index position {} is out of bounds. Total indexes: {}",
                position,
                total_indexes
            );
            return Ok(None);
        }

        let offset = position * INDEX_SIZE;

        let buf = match self.read_at(offset, INDEX_SIZE).await {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => {
                error!(
                    "Error reading index at position {} (offset {}) in file {}: {error}",
                    position, offset, self.file_path
                );
                return Err(IggyError::CannotReadFile);
            }
        };

        let index = parse_index(&buf).with_error_context(|error| {
            format!(
                "Failed to parse index at position {} (offset {}) in file {}: {error}",
                position, offset, self.file_path
            )
        })?;

        Ok(Some(index))
    }
}

/// Parses an index from a byte slice.
fn parse_index(chunk: &[u8]) -> Result<Index, IggyError> {
    let offset = u32::from_le_bytes(
        chunk[0..4]
            .try_into()
            .with_error_context(|error| format!("Failed to parse index offset: {error}"))
            .map_err(|_| IggyError::CannotReadIndexOffset)?,
    );
    let position = u32::from_le_bytes(
        chunk[4..8]
            .try_into()
            .with_error_context(|error| format!("Failed to parse index position: {error}"))
            .map_err(|_| IggyError::CannotReadIndexPosition)?,
    );
    let timestamp = u64::from_le_bytes(
        chunk[8..16]
            .try_into()
            .with_error_context(|error| format!("Failed to parse index timestamp: {error}"))
            .map_err(|_| IggyError::CannotReadIndexTimestamp)?,
    );
    Ok(Index {
        offset,
        position,
        timestamp,
    })
}
