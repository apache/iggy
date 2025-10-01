use iggy_common::IggyError;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;

use crate::configs::system::SystemConfig;
use crate::streaming::segments::{
    indexes::{IndexReader, IndexWriter},
    messages::{MessagesReader, MessagesWriter},
};

unsafe impl Send for Storage {}

#[derive(Debug, Clone)]
pub struct Storage {
    pub messages_writer: Option<Rc<MessagesWriter>>,
    pub messages_reader: Option<Rc<MessagesReader>>,
    pub index_writer: Option<Rc<IndexWriter>>,
    pub index_reader: Option<Rc<IndexReader>>,
}

impl Storage {
    pub async fn new(
        messages_path: &str,
        index_path: &str,
        messages_size: u64,
        indexes_size: u64,
        log_fsync: bool,
        index_fsync: bool,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let size = Rc::new(AtomicU64::new(messages_size));
        let indexes_size = Rc::new(AtomicU64::new(indexes_size));
        let messages_writer = Rc::new(
            MessagesWriter::new(messages_path, size.clone(), log_fsync, file_exists).await?,
        );

        let index_writer = Rc::new(
            IndexWriter::new(index_path, indexes_size.clone(), index_fsync, file_exists).await?,
        );

        if file_exists {
            messages_writer.fsync().await?;
            index_writer.fsync().await?;
        }

        let messages_reader = Rc::new(MessagesReader::new(messages_path, size).await?);
        let index_reader = Rc::new(IndexReader::new(index_path, indexes_size).await?);
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
}

/// Creates a new storage for the specified partition with the given start offset
pub async fn create_segment_storage(
    config: &SystemConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    messages_size: u64,
    indexes_size: u64,
    start_offset: u64,
) -> Result<Storage, IggyError> {
    let messages_path =
        config.get_messages_file_path(stream_id, topic_id, partition_id, start_offset);
    let index_path = config.get_index_path(stream_id, topic_id, partition_id, start_offset);
    let log_fsync = config.partition.enforce_fsync;
    let index_fsync = config.partition.enforce_fsync;
    let file_exists = false;

    Storage::new(
        &messages_path,
        &index_path,
        messages_size,
        indexes_size,
        log_fsync,
        index_fsync,
        file_exists,
    )
    .await
}
