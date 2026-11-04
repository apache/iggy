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

use super::*;

use bytes::Bytes;
use iggy_common::{IggyByteSize, PartitionStats};
use server_common::send_messages::{
    COMMAND_HEADER_SIZE, IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    calculate_batch_checksum,
};
use server_common::sharding::IggyNamespace;
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;

pub const STREAM_ID: usize = 1;
pub const TOPIC_ID: usize = 1;
pub const PARTITION_ID: usize = 1;
pub const SEGMENT_MAX_SIZE: u64 = 16 * 1024 * 1024;
pub const FIXTURE_TIMESTAMP: u64 = 1_700_000_000_000_000;
pub const GARBAGE: [u8; 384] = [0xAB; 384];

const HEADER_BATCH_LENGTH_OFFSET: usize = 32;
const HEADER_MESSAGE_COUNT_OFFSET: usize = 48;

pub fn test_config(tmp: &TempDir) -> PartitionsConfig {
    PartitionsConfig {
        messages_required_to_save: iggy_common::DEFAULT_MESSAGES_REQUIRED_TO_SAVE,
        size_of_messages_required_to_save: IggyByteSize::from(
            iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
        ),
        validate_checksum: true,
        segment_size: IggyByteSize::from(SEGMENT_MAX_SIZE),
        preallocate_segments: iggy_common::DEFAULT_PREALLOCATE_SEGMENTS,
        key_context: Arc::default(),
        path_layout: PartitionPathLayout {
            streams_root: tmp.path().join("streams").to_string_lossy().into_owned(),
        },
    }
}

pub fn prepare_partition_dir(config: &PartitionsConfig) -> String {
    let partition_path = config.get_partition_path(STREAM_ID, TOPIC_ID, PARTITION_ID);
    fs::create_dir_all(&partition_path).expect("create partition dir");
    partition_path
}

pub fn zero_padded_record(claimed_batch_length: u64, sequence: u32) -> Vec<u8> {
    let mut record = vec![0u8; COMMAND_HEADER_SIZE];
    record[HEADER_BATCH_LENGTH_OFFSET..HEADER_BATCH_LENGTH_OFFSET + 8]
        .copy_from_slice(&claimed_batch_length.to_le_bytes());
    record[HEADER_MESSAGE_COUNT_OFFSET..HEADER_MESSAGE_COUNT_OFFSET + 4]
        .copy_from_slice(&sequence.to_le_bytes());
    record
}

pub fn encoded_batch(base_offset: u64, message_count: usize) -> Vec<u8> {
    encoded_batch_with_payload(
        base_offset,
        message_count,
        &Bytes::from_static(b"segment-recovery-fixture"),
    )
}

pub fn encoded_batch_with_payload(
    base_offset: u64,
    message_count: usize,
    payload: &Bytes,
) -> Vec<u8> {
    encoded_batch_stamped(base_offset, message_count, payload, PARTITION_ID as u64)
}

pub fn encoded_batch_stamped(
    base_offset: u64,
    message_count: usize,
    payload: &Bytes,
    partition_id: u64,
) -> Vec<u8> {
    let mut messages = IggyMessages::with_capacity(message_count);
    for _ in 0..message_count {
        messages.push(IggyMessage {
            header: IggyMessageHeader {
                origin_timestamp: FIXTURE_TIMESTAMP,
                ..IggyMessageHeader::default()
            },
            payload: payload.clone(),
            user_headers: None,
        });
    }
    let namespace = IggyNamespace::new(STREAM_ID, TOPIC_ID, PARTITION_ID);
    let SendMessagesOwned { mut header, blob } =
        SendMessagesOwned::from_messages(namespace, &messages).expect("encode fixture batch");
    header.partition_id = partition_id;
    header.base_offset = base_offset;
    header.base_timestamp = FIXTURE_TIMESTAMP;
    header.batch_checksum = calculate_batch_checksum(&header, &blob);
    let mut record = vec![0u8; header.total_size()];
    header.encode_into(&mut record[..COMMAND_HEADER_SIZE]);
    record[COMMAND_HEADER_SIZE..].copy_from_slice(&blob);
    record
}

pub fn index_entry(offset: u64, position: u64) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&offset.to_le_bytes());
    entry.extend_from_slice(&FIXTURE_TIMESTAMP.to_le_bytes());
    entry.extend_from_slice(&position.to_le_bytes());
    entry
}

pub fn write_segment(
    config: &PartitionsConfig,
    start_offset: u64,
    log: &[u8],
    index: &[u8],
) -> (String, String) {
    let messages_path = config.get_messages_path(STREAM_ID, TOPIC_ID, PARTITION_ID, start_offset);
    let index_path = config.get_index_path(STREAM_ID, TOPIC_ID, PARTITION_ID, start_offset);
    fs::write(&messages_path, log).expect("write log fixture");
    fs::write(&index_path, index).expect("write index fixture");
    (messages_path, index_path)
}

pub async fn recover(
    config: &PartitionsConfig,
) -> Result<Vec<RecoveredSegment>, PartitionRecoveryError> {
    recover_under_fsync(config, false).await
}

pub async fn recover_under_fsync(
    config: &PartitionsConfig,
    durable_segments: bool,
) -> Result<Vec<RecoveredSegment>, PartitionRecoveryError> {
    load_persisted_segments(
        config,
        IggyNamespace::new(STREAM_ID, TOPIC_ID, PARTITION_ID),
        IggyByteSize::from(SEGMENT_MAX_SIZE),
        durable_segments,
        &PartitionStats::default(),
    )
    .await
}

pub fn bytes_of(path: &str) -> Vec<u8> {
    fs::read(path).expect("read fixture file")
}
