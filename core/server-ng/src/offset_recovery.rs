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

//! server-ng-owned consumer offset recovery.
//!
//! Forked from `server::streaming::partitions::storage` (the legacy
//! `load_consumer_offsets` / `load_consumer_group_offsets`) so server-ng
//! owns the loaders for the offset files its own persistence path writes,
//! without depending on the legacy `server` crate. One file per consumer (numeric
//! file name = consumer id) holding a little-endian `u64` offset then a checksum over
//! it; see [`partitions::offset_storage`]. The legacy server stays compatible both
//! ways: it reads the first eight bytes and stops, and a file it wrote itself decodes
//! here as unchecksummed.

use iggy_common::{ConsumerGroupId, ConsumerKind, ConsumerOffset, IggyError};
use partitions::offset_storage::{OffsetRecord, decode_offset_record};
use std::sync::atomic::AtomicU64;
use tracing::{error, trace, warn};

const COMPONENT: &str = "STREAMING_PARTITIONS";

pub fn load_consumer_offsets(path: &str) -> Result<Vec<ConsumerOffset>, IggyError> {
    trace!("Loading consumer offsets from path: {path}...");
    let Ok(dir_entries) = std::fs::read_dir(path) else {
        return Err(IggyError::CannotReadConsumerOffsets(path.to_owned()));
    };

    let mut consumer_offsets = Vec::new();
    for dir_entry in dir_entries {
        let dir_entry = match dir_entry {
            Ok(entry) => entry,
            Err(e) => {
                warn!(
                    "Failed to read directory entry in consumer offsets path: {path}, \
                     error: {e}, skipping."
                );
                continue;
            }
        };

        let metadata = match dir_entry.metadata() {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "Failed to read metadata for entry in consumer offsets path: {path}, \
                     error: {e}, skipping."
                );
                continue;
            }
        };

        if metadata.is_dir() {
            continue;
        }

        let name = dir_entry.file_name().to_string_lossy().to_string();
        let Ok(consumer_id) = name.parse::<u32>() else {
            warn!(
                "Unexpected non-numeric consumer offset file: '{}', skipping.",
                name
            );
            continue;
        };

        let path = dir_entry.path();
        let Some(path) = path.to_str().map(str::to_owned) else {
            error!("Invalid consumer ID path for file with name: '{}'.", name);
            continue;
        };

        let Some(offset) = read_offset_file(&path, "consumer offset") else {
            continue;
        };

        consumer_offsets.push(ConsumerOffset {
            kind: ConsumerKind::Consumer,
            consumer_id,
            offset,
            path,
        });
    }

    consumer_offsets.sort_by_key(|consumer_offset| consumer_offset.consumer_id);
    Ok(consumer_offsets)
}

pub fn load_consumer_group_offsets(
    path: &str,
) -> Result<Vec<(ConsumerGroupId, ConsumerOffset)>, IggyError> {
    trace!("Loading consumer group offsets from path: {path}...");
    let Ok(dir_entries) = std::fs::read_dir(path) else {
        return Err(IggyError::CannotReadConsumerOffsets(path.to_owned()));
    };

    let mut consumer_group_offsets = Vec::new();
    for dir_entry in dir_entries {
        let dir_entry = match dir_entry {
            Ok(entry) => entry,
            Err(e) => {
                warn!(
                    "Failed to read directory entry in consumer group offsets path: {path}, \
                     error: {e}, skipping."
                );
                continue;
            }
        };

        let metadata = match dir_entry.metadata() {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "Failed to read metadata for entry in consumer group offsets path: {path}, \
                     error: {e}, skipping."
                );
                continue;
            }
        };

        if metadata.is_dir() {
            continue;
        }

        let name = dir_entry.file_name().to_string_lossy().to_string();
        let Ok(raw_consumer_group_id) = name.parse::<u32>() else {
            warn!(
                "Unexpected non-numeric consumer group offset file: '{}', skipping.",
                name
            );
            continue;
        };
        let consumer_group_id = ConsumerGroupId(raw_consumer_group_id as usize);

        let path = dir_entry.path();
        let Some(path) = path.to_str().map(str::to_owned) else {
            error!(
                "Invalid consumer group offset path for file with name: '{}'.",
                name
            );
            continue;
        };

        let Some(offset) = read_offset_file(&path, "consumer group offset") else {
            continue;
        };

        let consumer_offset = ConsumerOffset {
            kind: ConsumerKind::ConsumerGroup,
            consumer_id: raw_consumer_group_id,
            offset,
            path,
        };

        consumer_group_offsets.push((consumer_group_id, consumer_offset));
    }

    Ok(consumer_group_offsets)
}

fn read_offset_file(path: &str, offset_kind: &'static str) -> Option<AtomicU64> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(
                "{COMPONENT} (error: {e}) - failed to read offset file, \
                 path: {path}, skipping."
            );
            return None;
        }
    };
    match decode_offset_record(&bytes) {
        OffsetRecord::Value { offset, .. } => Some(AtomicU64::new(offset)),
        OffsetRecord::Torn => {
            warn!(
                "{COMPONENT} - failed to read {offset_kind} from file (truncated), \
                 path: {path}, skipping."
            );
            None
        }
        // Skipped rather than loaded: resuming from a cursor provably not the one
        // written reads as ordinary redelivery or a gap, never as corruption.
        OffsetRecord::Corrupt {
            offset,
            expected,
            found,
        } => {
            error!(
                "{COMPONENT} - {offset_kind} file failed its checksum \
                 (offset: {offset}, expected: {expected}, found: {found}), \
                 path: {path}, skipping."
            );
            None
        }
    }
}
