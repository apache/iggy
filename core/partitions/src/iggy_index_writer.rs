use compio::fs::{File, OpenOptions};
use compio::io::AsyncWriteAtExt;
use iggy_common::IggyError;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::trace;

#[derive(Debug)]
pub struct IggyIndexWriter {
    file_path: String,
    file: File,
    index_size_bytes: Rc<AtomicU64>,
}

impl IggyIndexWriter {
    pub async fn new(
        file_path: &str,
        index_size_bytes: Rc<AtomicU64>,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true);
        let file = opts
            .open(file_path)
            .await
            .map_err(|_| IggyError::CannotReadFile)?;

        if file_exists {
            let _ = file.sync_all().await;

            let actual_index_size = file
                .metadata()
                .await
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            index_size_bytes.store(actual_index_size, Ordering::Relaxed);
        }

        let size = index_size_bytes.load(Ordering::Relaxed);
        trace!(
            "Opened sparse index file for writing: {file_path}, size: {}",
            size
        );

        Ok(Self {
            file_path: file_path.to_owned(),
            file,
            index_size_bytes,
        })
    }

    pub async fn save_indexes(&self, indexes: Vec<u8>) -> Result<(), IggyError> {
        if indexes.is_empty() {
            return Ok(());
        }

        let len = indexes.len();
        let position = self.index_size_bytes.load(Ordering::Relaxed);
        let file = &self.file;
        (&*file)
            .write_all_at(indexes, position)
            .await
            .0
            .map_err(|_| IggyError::CannotSaveIndexToSegment)?;

        self.index_size_bytes
            .fetch_add(len as u64, Ordering::Release);

        self.fsync().await?;

        trace!("Saved {len} sparse index bytes to file: {}", self.file_path);
        Ok(())
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        self.file
            .sync_all()
            .await
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }
}
