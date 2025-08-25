/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use super::indexes::*;
use super::messages::*;
use super::messages_accumulator::MessagesAccumulator;
use crate::configs::system::SystemConfig;
use crate::streaming::segments::*;
use compio::fs::remove_file;
use error_set::ErrContext;
use iggy_common::INDEX_SIZE;
use iggy_common::IggyByteSize;
use iggy_common::IggyError;
use iggy_common::IggyExpiry;
use iggy_common::IggyTimestamp;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::error;
use tracing::{info, warn};

const SIZE_16MB: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub struct Segment {;
    pub(super) stream_id: u32,
    pub(super) topic_id: u32,
    pub(super) partition_id: u32,
    pub(super) start_offset: u64,
    pub(super) start_timestamp: u64, // first message timestamp
    pub(super) end_timestamp: u64,   // last message timestamp
    pub(super) end_offset: u64,
    pub(super) index_path: String,
    pub(super) messages_path: String,
    pub(super) last_index_position: u32,
    pub(super) max_size_bytes: IggyByteSize,
    pub(super) size_of_parent_stream: Arc<AtomicU64>,
    pub(super) size_of_parent_topic: Arc<AtomicU64>,
    pub(super) size_of_parent_partition: Arc<AtomicU64>,
    pub(super) messages_count_of_parent_stream: Arc<AtomicU64>,
    pub(super) messages_count_of_parent_topic: Arc<AtomicU64>,
    pub(super) messages_count_of_parent_partition: Arc<AtomicU64>,
    pub(super) is_closed: bool,
    pub(super) messages_writer: Option<MessagesWriter>,
    pub(super) messages_reader: Option<MessagesReader>,
    pub(super) index_writer: Option<IndexWriter>,
    pub(super) index_reader: Option<IndexReader>,
    pub(super) message_expiry: IggyExpiry,
    pub(super) accumulator: MessagesAccumulator,
    pub(super) config: Arc<SystemConfig>,
    pub(super) indexes: Option<IggyIndexesMut>,
    pub(super) messages_size: Arc<AtomicU64>,
    pub(super) indexes_size: Arc<AtomicU64>,
}

impl Segment {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        start_offset: u64,
        config: Arc<SystemConfig>,
        message_expiry: IggyExpiry,
        size_of_parent_stream: Arc<AtomicU64>,
        size_of_parent_topic: Arc<AtomicU64>,
        size_of_parent_partition: Arc<AtomicU64>,
        messages_count_of_parent_stream: Arc<AtomicU64>,
        messages_count_of_parent_topic: Arc<AtomicU64>,
        messages_count_of_parent_partition: Arc<AtomicU64>,
        fresh: bool, // `fresh` means created and persisted in this runtime, in other words it's set to false when loading from disk
    ) -> Segment {
        let path = config.get_segment_path(
            stream_id as usize,
            topic_id as usize,
            partition_id as usize,
            start_offset,
        );
        let messages_path = Self::get_messages_file_path(&path);
        let index_path = Self::get_index_path(&path);
        let message_expiry = match message_expiry {
            IggyExpiry::ServerDefault => config.segment.message_expiry,
            _ => message_expiry,
        };
        Segment {
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            start_timestamp: IggyTimestamp::now().as_micros(),
            end_timestamp: IggyTimestamp::now().as_micros(),
            end_offset: start_offset,
            messages_path,
            index_path,
            last_index_position: 0,
            max_size_bytes: config.segment.size,
            message_expiry,
            indexes: None,
            accumulator: MessagesAccumulator::default(),
            is_closed: false,
            messages_writer: None,
            messages_reader: None,
            index_writer: None,
            index_reader: None,
            size_of_parent_stream,
            size_of_parent_partition,
            size_of_parent_topic,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
            config,
            messages_size: Arc::new(AtomicU64::new(0)),
            indexes_size: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Load the segment state from disk.
    pub async fn load_from_disk(&mut self) -> Result<(), IggyError> {
        if self.messages_reader.is_none() || self.index_reader.is_none() {
            self.initialize_writing(true).await?;
            self.initialize_reading().await?;
        }

        let log_size_bytes = self.messages_size.load(Ordering::Acquire);
        info!(
            "Loading segment from disk: messages_file_path: {}, index_path: {}, log_size: {}",
            self.messages_path,
            self.index_path,
            IggyByteSize::from(log_size_bytes)
        );

        self.last_index_position = log_size_bytes as _;

        let loaded_indexes = self
            .index_reader
            .as_ref()
            .unwrap()
            .load_all_indexes_from_disk()
            .await
            .with_error_context(|error| format!("Failed to load indexes for {self}. {error}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        info!(
            "Loaded {} indexes for segment with start offset: {}, end offset: {}, and partition with ID: {}, topic with ID: {}, and stream with ID: {}.",
            self.indexes.as_ref().map_or(0, |idx| idx.count()),
            self.start_offset,
            self.end_offset,
            self.partition_id,
            self.topic_id,
            self.stream_id
        );

        let last_index_offset = if loaded_indexes.is_empty() {
            0_u64
        } else {
            loaded_indexes.last().unwrap().offset() as u64
        };

        self.end_offset = self.start_offset + last_index_offset;

        info!(
            "Loaded {} indexes for segment with start offset: {}, end offset: {}, and partition with ID: {}, topic with ID: {}, and stream with ID: {}.",
            self.indexes.as_ref().map_or(0, |idx| idx.count()),
            self.start_offset,
            self.end_offset,
            self.partition_id,
            self.topic_id,
            self.stream_id
        );

        if self.is_full().await {
            self.is_closed = true;
        }

        let messages_count = self.get_messages_count() as u64;

        info!(
            "Loaded segment with log file of size {} ({} messages) for start offset {}, end offset: {}, and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
            IggyByteSize::from(log_size_bytes),
            messages_count,
            self.start_offset,
            self.end_offset,
            self.partition_id,
            self.topic_id,
            self.stream_id
        );

        self.size_of_parent_stream
            .fetch_add(log_size_bytes, Ordering::SeqCst);
        self.size_of_parent_topic
            .fetch_add(log_size_bytes, Ordering::SeqCst);
        self.size_of_parent_partition
            .fetch_add(log_size_bytes, Ordering::SeqCst);
        self.messages_count_of_parent_stream
            .fetch_add(messages_count, Ordering::SeqCst);
        self.messages_count_of_parent_topic
            .fetch_add(messages_count, Ordering::SeqCst);
        self.messages_count_of_parent_partition
            .fetch_add(messages_count, Ordering::SeqCst);

        Ok(())
    }

    /// Open segment.
    pub async fn open(&mut self) -> Result<(), IggyError> {
        info!(
            "Saving segment with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
            self.start_offset, self.partition_id, self.topic_id, self.stream_id
        );
        self.initialize_writing(false).await?;
        self.initialize_reading().await?;
        info!(
            "Saved segment log file with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
            self.start_offset, self.partition_id, self.topic_id, self.stream_id
        );
        Ok(())
    }

    pub async fn initialize_writing(&mut self, file_exists: bool) -> Result<(), IggyError> {
        let log_fsync = self.config.partition.enforce_fsync;
        let index_fsync = self.config.partition.enforce_fsync;

        let messages_writer = MessagesWriter::new(
            &self.messages_path,
            self.messages_size.clone(),
            log_fsync,
            file_exists,
        )
        .await?;

        let index_writer = IndexWriter::new(
            &self.index_path,
            self.indexes_size.clone(),
            index_fsync,
            file_exists,
        )
        .await?;

        self.messages_writer = Some(messages_writer);
        self.index_writer = Some(index_writer);
        Ok(())
    }

    pub async fn initialize_reading(&mut self) -> Result<(), IggyError> {
        let messages_reader =
            MessagesReader::new(&self.messages_path, self.messages_size.clone()).await?;
        self.messages_reader = Some(messages_reader);

        let index_reader = IndexReader::new(&self.index_path, self.indexes_size.clone()).await?;
        self.index_reader = Some(index_reader);

        Ok(())
    }

    pub async fn is_full(&self) -> bool {
        if self.get_messages_size() >= self.max_size_bytes {
            return true;
        }

        self.is_expired(IggyTimestamp::now()).await
    }

    pub async fn is_expired(&self, now: IggyTimestamp) -> bool {
        if !self.is_closed {
            return false;
        }

        match self.message_expiry {
            IggyExpiry::NeverExpire => false,
            IggyExpiry::ServerDefault => false,
            IggyExpiry::ExpireDuration(expiry) => {
                let last_messages = self.get_messages_by_offset(self.end_offset, 1).await;
                if last_messages.is_err() {
                    return false;
                }

                let last_messages = last_messages.unwrap();
                if last_messages.is_empty() {
                    return false;
                }

                let last_message = last_messages.iter().last().unwrap().iter().last().unwrap();
                let last_message_timestamp = last_message.header().timestamp();
                last_message_timestamp + expiry.as_micros() <= now.as_micros()
            }
        }
    }

    pub async fn shutdown_reading(&mut self) {
        if let Some(log_reader) = self.messages_reader.take() {
            drop(log_reader);
        }
        if let Some(index_reader) = self.index_reader.take() {
            drop(index_reader);
        }
    }

    pub async fn shutdown_writing(&mut self) {
        if let Some(log_writer) = self.messages_writer.take() {
            //TODO: Fixme not sure whether we should spawn a task here.
            compio::runtime::spawn(async move {
                let _ = log_writer.fsync().await;
            })
            .detach();
        } else {
            warn!(
                "Log writer already closed when calling close() for {}",
                self
            );
        }

        if let Some(index_writer) = self.index_writer.take() {
            //TODO: Fixme not sure whether we should spawn a task here.
            compio::runtime::spawn(async move {
                let _ = index_writer.fsync().await;
                drop(index_writer)
            })
            .detach();
        } else {
            warn!("Index writer already closed when calling close()");
        }
    }

    pub async fn delete(&mut self) -> Result<(), IggyError> {
        let segment_size = self.get_messages_size();
        let segment_count_of_messages = self.get_messages_count() as u64;
        info!(
            "Deleting segment of size {segment_size} ({segment_count_of_messages} messages) with start offset: {} for partition with ID: {} for stream with ID: {} and topic with ID: {}...",
            self.start_offset, self.partition_id, self.stream_id, self.topic_id,
        );

        self.shutdown_reading().await;

        if !self.is_closed {
            self.shutdown_writing().await;
        }

        let _ = remove_file(&self.messages_path)
            .await
            .with_error_context(|error| {
                format!("Failed to delete log file: {}. {error}", self.messages_path)
            });
        let _ = remove_file(&self.index_path)
            .await
            .with_error_context(|error| {
                format!("Failed to delete index file: {}. {error}", self.index_path)
            });

        let segment_size_bytes = segment_size.as_bytes_u64();
        self.size_of_parent_stream
            .fetch_sub(segment_size_bytes, Ordering::SeqCst);
        self.size_of_parent_topic
            .fetch_sub(segment_size_bytes, Ordering::SeqCst);
        self.size_of_parent_partition
            .fetch_sub(segment_size_bytes, Ordering::SeqCst);
        self.messages_count_of_parent_stream
            .fetch_sub(segment_count_of_messages, Ordering::SeqCst);
        self.messages_count_of_parent_topic
            .fetch_sub(segment_count_of_messages, Ordering::SeqCst);
        self.messages_count_of_parent_partition
            .fetch_sub(segment_count_of_messages, Ordering::SeqCst);

        info!(
            "Deleted segment of size {segment_size} with start offset: {} for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            self.start_offset, self.partition_id, self.stream_id, self.topic_id,
        );

        Ok(())
    }

    fn get_messages_file_path(path: &str) -> String {
        format!("{path}.{LOG_EXTENSION}")
    }

    fn get_index_path(path: &str) -> String {
        format!("{path}.{INDEX_EXTENSION}")
    }

    pub fn update_message_expiry(&mut self, message_expiry: IggyExpiry) {
        self.message_expiry = message_expiry;
    }

    /// Ensure indexes are initialized with proper capacity
    pub fn ensure_indexes(&mut self) {
        if self.indexes.is_none() {
            let capacity = SIZE_16MB / INDEX_SIZE;
            self.indexes = Some(IggyIndexesMut::with_capacity(capacity, 0));
        }
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn start_offset(&self) -> u64 {
        self.start_offset
    }

    pub fn end_offset(&self) -> u64 {
        self.end_offset
    }

    pub fn end_timestamp(&self) -> u64 {
        self.end_timestamp
    }

    pub fn index_file_path(&self) -> &str {
        &self.index_path
    }

    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    pub fn messages_file_path(&self) -> &str {
        &self.messages_path
    }

    /// Explicitly drop the old indexes to ensure memory is freed
    pub fn drop_indexes(&mut self) {
        self.indexes = None;
    }
}

impl std::fmt::Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Segment {{ stream_id: {}, topic_id: {}, partition_id: {}, start_offset: {}, end_offset: {}, size_bytes: {}, last_index_position: {}, max_size_bytes: {}, closed: {} }}",
            self.stream_id,
            self.topic_id,
            self.partition_id,
            self.start_offset,
            self.end_offset,
            self.get_messages_size(),
            self.last_index_position,
            self.max_size_bytes,
            self.is_closed
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::cache_indexes::CacheIndexesConfig;
    use crate::configs::system::SegmentConfig;
    use crate::streaming::utils::MemoryPool;
    use iggy_common::IggyDuration;

    #[tokio::test]
    async fn should_be_created_given_valid_parameters() {
        let stream_id = 1usize;
        let topic_id = 2usize;
        let partition_id = 3usize;
        let start_offset = 0;
        let config = Arc::new(SystemConfig::default());
        let path = config.get_segment_path(stream_id, topic_id, partition_id, start_offset);
        let messages_file_path = Segment::get_messages_file_path(&path);
        let index_path = Segment::get_index_path(&path);
        let message_expiry = IggyExpiry::ExpireDuration(IggyDuration::from(10));
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let size_of_parent_topic = Arc::new(AtomicU64::new(0));
        let size_of_parent_partition = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_topic = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_partition = Arc::new(AtomicU64::new(0));
        MemoryPool::init_pool(config.clone());

        let segment = Segment::create(
            stream_id as u32,
            topic_id as u32,
            partition_id as u32,
            start_offset,
            config,
            message_expiry,
            size_of_parent_stream,
            size_of_parent_topic,
            size_of_parent_partition,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
            true,
        );

        assert_eq!(segment.stream_id, stream_id as u32);
        assert_eq!(segment.topic_id, topic_id as u32);
        assert_eq!(segment.partition_id, partition_id as u32);
        assert_eq!(segment.start_offset(), start_offset);
        assert_eq!(segment.end_offset(), 0);
        assert_eq!(segment.get_messages_size(), 0);
        assert_eq!(segment.messages_file_path(), messages_file_path);
        assert_eq!(segment.index_file_path(), index_path);
        assert_eq!(segment.message_expiry, message_expiry);
        assert!(segment.indexes.is_none());
        assert!(!segment.is_closed());
        assert!(!segment.is_full().await);
    }

    #[tokio::test]
    async fn should_not_initialize_indexes_cache_when_disabled() {
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig {
            segment: SegmentConfig {
                cache_indexes: CacheIndexesConfig::None,
                ..Default::default()
            },
            ..Default::default()
        });
        let message_expiry = IggyExpiry::NeverExpire;
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let size_of_parent_topic = Arc::new(AtomicU64::new(0));
        let size_of_parent_partition = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_topic = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_partition = Arc::new(AtomicU64::new(0));
        MemoryPool::init_pool(config.clone());

        let segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            config,
            message_expiry,
            size_of_parent_stream,
            size_of_parent_topic,
            size_of_parent_partition,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
            true,
        );

        assert!(segment.indexes.is_none());
    }
}
