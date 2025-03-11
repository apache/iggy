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
use super::logs::*;
use crate::configs::system::SystemConfig;
use crate::streaming::batching::batch_accumulator::BatchAccumulator;
use crate::streaming::segments::*;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::timestamp::IggyTimestamp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::remove_file;
use tracing::{info, warn};

#[derive(Debug)]
pub struct Segment {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub start_offset: u64,
    pub start_timestamp: u64, // first message timestamp
    pub end_offset: u64,
    pub end_timestamp: u64, // last message timestamp
    pub current_offset: u64,
    pub index_path: String,
    pub log_path: String,
    pub size_bytes: IggyByteSize,
    pub last_index_position: u32,
    pub max_size_bytes: IggyByteSize,
    pub size_of_parent_stream: Arc<AtomicU64>,
    pub size_of_parent_topic: Arc<AtomicU64>,
    pub size_of_parent_partition: Arc<AtomicU64>,
    pub messages_count_of_parent_stream: Arc<AtomicU64>,
    pub messages_count_of_parent_topic: Arc<AtomicU64>,
    pub messages_count_of_parent_partition: Arc<AtomicU64>,
    pub is_closed: bool,
    pub(super) log_writer: Option<SegmentLogWriter>,
    pub(super) log_reader: Option<SegmentLogReader>,
    pub(super) index_writer: Option<SegmentIndexWriter>,
    pub(super) index_reader: Option<SegmentIndexReader>,
    pub message_expiry: IggyExpiry,
    pub unsaved_messages: Option<BatchAccumulator>,
    pub config: Arc<SystemConfig>,
    pub indexes: Option<Vec<Index>>,
    pub(super) log_size_bytes: Arc<AtomicU64>,
    pub(super) index_size_bytes: Arc<AtomicU64>,
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
    ) -> Segment {
        let path = config.get_segment_path(stream_id, topic_id, partition_id, start_offset);
        let log_path = Self::get_log_path(&path);
        let index_path = Self::get_index_path(&path);
        let message_expiry = match message_expiry {
            IggyExpiry::ServerDefault => config.segment.message_expiry,
            _ => message_expiry,
        };
        let indexes = match config.segment.cache_indexes {
            true => Some(Vec::new()),
            false => None,
        };

        Segment {
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            start_timestamp: IggyTimestamp::now().as_micros(),
            end_offset: 0,
            end_timestamp: IggyTimestamp::now().as_micros(),
            current_offset: start_offset,
            log_path,
            index_path,
            size_bytes: IggyByteSize::from(0),
            last_index_position: 0,
            max_size_bytes: config.segment.size,
            message_expiry,
            indexes,
            unsaved_messages: None,
            is_closed: false,
            log_writer: None,
            log_reader: None,
            index_writer: None,
            index_reader: None,
            size_of_parent_stream,
            size_of_parent_partition,
            size_of_parent_topic,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
            config,
            log_size_bytes: Arc::new(AtomicU64::new(0)),
            index_size_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Load the segment state from disk.
    pub async fn load_from_disk(&mut self) -> Result<(), IggyError> {
        info!(
            "Loading segment from disk: log_path: {}, index_path: {}",
            self.log_path, self.index_path
        );

        if self.log_reader.is_none() || self.index_reader.is_none() {
            self.initialize_writing().await?;
            self.initialize_reading().await?;
        }

        let log_size_bytes = self.log_size_bytes.load(Ordering::Acquire);
        info!("Log file size: {}", IggyByteSize::from(log_size_bytes));

        // TODO(hubcio): in future, remove size_bytes and use only atomic log_size_bytes everywhere
        self.size_bytes = IggyByteSize::from(log_size_bytes);
        self.last_index_position = log_size_bytes as _;

        self.indexes = Some(
            self.index_reader
                .as_ref()
                .unwrap()
                .load_all_indexes_impl()
                .await
                .with_error_context(|error| format!("Failed to load indexes for {self}. {error}"))
                .map_err(|_| IggyError::CannotReadFile)?,
        );

        let last_index_offset = if self.indexes.as_ref().unwrap().is_empty() {
            0_u64
        } else {
            self.indexes.as_ref().unwrap().last().unwrap().offset as u64
        };

        self.current_offset = self.start_offset + last_index_offset;

        info!("Loaded {} indexes for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
              self.indexes.as_ref().unwrap().len(),
              self.start_offset,
              self.partition_id,
              self.topic_id,
              self.stream_id);

        if !self.config.segment.cache_indexes {
            self.indexes = None;
        }

        if self.is_full().await {
            self.is_closed = true;
        }

        let messages_count = self.get_messages_count();

        info!(
            "Loaded segment with log file of size {} ({} messages) for start offset {}, current offset: {}, and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
            IggyByteSize::from(log_size_bytes),
            messages_count,
            self.start_offset,
            self.current_offset,
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

    /// Save the segment state to disk.
    pub async fn persist(&mut self) -> Result<(), IggyError> {
        info!("Saving segment with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
            self.start_offset, self.partition_id, self.topic_id, self.stream_id);
        self.initialize_writing().await?;
        self.initialize_reading().await?;
        info!("Saved segment log file with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
            self.start_offset, self.partition_id, self.topic_id, self.stream_id);
        Ok(())
    }

    pub async fn initialize_writing(&mut self) -> Result<(), IggyError> {
        // TODO(hubcio): consider splitting enforce_fsync for index/log to separate entries in config
        let log_fsync = self.config.partition.enforce_fsync;
        let index_fsync = self.config.partition.enforce_fsync;

        let server_confirmation = self.config.segment.server_confirmation;
        let max_file_operation_retries = self.config.state.max_file_operation_retries;
        let retry_delay = self.config.state.retry_delay;

        let log_writer = SegmentLogWriter::new(
            &self.log_path,
            self.log_size_bytes.clone(),
            log_fsync,
            server_confirmation,
            max_file_operation_retries,
            retry_delay,
        )
        .await?;

        let index_writer =
            SegmentIndexWriter::new(&self.index_path, self.index_size_bytes.clone(), index_fsync)
                .await?;

        self.log_writer = Some(log_writer);
        self.index_writer = Some(index_writer);
        Ok(())
    }

    pub async fn initialize_reading(&mut self) -> Result<(), IggyError> {
        let log_reader = SegmentLogReader::new(&self.log_path, self.log_size_bytes.clone()).await?;
        // TODO(hubcio): there is no need to store open fd for reader if we have index cache enabled
        let index_reader =
            SegmentIndexReader::new(&self.index_path, self.index_size_bytes.clone()).await?;

        self.log_reader = Some(log_reader);
        self.index_reader = Some(index_reader);
        Ok(())
    }

    pub async fn is_full(&self) -> bool {
        if self.size_bytes >= self.max_size_bytes {
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
                let last_messages = self.get_messages_by_offset(self.current_offset, 1).await;
                if last_messages.is_err() {
                    return false;
                }

                let last_messages = last_messages.unwrap();
                if last_messages.is_empty() {
                    return false;
                }

                let last_message = &last_messages[0];
                let last_message_timestamp = last_message.timestamp;
                last_message_timestamp + expiry.as_micros() <= now.as_micros()
            }
        }
    }

    pub async fn shutdown_reading(&mut self) {
        if let Some(log_reader) = self.log_reader.take() {
            drop(log_reader);
        }
        if let Some(index_reader) = self.index_reader.take() {
            drop(index_reader);
        }
    }

    pub async fn shutdown_writing(&mut self) {
        if let Some(log_writer) = self.log_writer.take() {
            tokio::spawn(async move {
                let _ = log_writer.fsync().await;
                log_writer.shutdown_persister_task().await;
            });
        } else {
            warn!(
                "Log writer already closed when calling close() for {}",
                self
            );
        }

        if let Some(index_writer) = self.index_writer.take() {
            tokio::spawn(async move {
                let _ = index_writer.fsync().await;
                drop(index_writer)
            });
        } else {
            warn!("Index writer already closed when calling close()");
        }
    }

    pub async fn delete(&mut self) -> Result<(), IggyError> {
        let segment_size = self.size_bytes;
        let segment_count_of_messages = self.get_messages_count();
        info!(
            "Deleting segment of size {segment_size} with start offset: {} for partition with ID: {} for stream with ID: {} and topic with ID: {}...",
            self.start_offset, self.partition_id, self.stream_id, self.topic_id,
        );

        self.shutdown_reading().await;

        if !self.is_closed {
            self.shutdown_writing().await;
        }

        let _ = remove_file(&self.log_path)
            .await
            .with_error_context(|error| {
                format!("Failed to delete log file: {}. {error}", self.log_path)
            });
        let _ = remove_file(&self.index_path)
            .await
            .with_error_context(|error| {
                format!("Failed to delete index file: {}. {error}", self.index_path)
            });

        let segment_size_bytes = self.size_bytes.as_bytes_u64();
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

    fn get_log_path(path: &str) -> String {
        format!("{}.{}", path, LOG_EXTENSION)
    }

    fn get_index_path(path: &str) -> String {
        format!("{}.{}", path, INDEX_EXTENSION)
    }
}

impl std::fmt::Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Segment {{ stream ID: {}, topic ID: {}, partition_id: {}, start_offset: {}, end_offset: {}, current_offset: {}, size_bytes: {}, last_index_position: {}, max_size_bytes: {}, closed: {} }}",
            self.stream_id,
            self.topic_id,
            self.partition_id,
            self.start_offset,
            self.end_offset,
            self.current_offset,
            self.size_bytes,
            self.last_index_position,
            self.max_size_bytes,
            self.is_closed
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::SegmentConfig;
    use iggy::utils::duration::IggyDuration;

    #[tokio::test]
    async fn should_be_created_given_valid_parameters() {
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig::default());
        let path = config.get_segment_path(stream_id, topic_id, partition_id, start_offset);
        let log_path = Segment::get_log_path(&path);
        let index_path = Segment::get_index_path(&path);
        let message_expiry = IggyExpiry::ExpireDuration(IggyDuration::from(10));
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let size_of_parent_topic = Arc::new(AtomicU64::new(0));
        let size_of_parent_partition = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_topic = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_partition = Arc::new(AtomicU64::new(0));

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
        );

        assert_eq!(segment.stream_id, stream_id);
        assert_eq!(segment.topic_id, topic_id);
        assert_eq!(segment.partition_id, partition_id);
        assert_eq!(segment.start_offset, start_offset);
        assert_eq!(segment.current_offset, 0);
        assert_eq!(segment.end_offset, 0);
        assert_eq!(segment.size_bytes, 0);
        assert_eq!(segment.log_path, log_path);
        assert_eq!(segment.index_path, index_path);
        assert_eq!(segment.message_expiry, message_expiry);
        assert!(segment.unsaved_messages.is_none());
        assert!(segment.indexes.is_some());
        assert!(!segment.is_closed);
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
                cache_indexes: false,
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
        );

        assert!(segment.indexes.is_none());
    }
}
