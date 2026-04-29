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

use iggy_common::{ConsumerKind, IggyError};
use server::streaming::partitions::storage::{load_consumer_group_offsets, load_consumer_offsets};
use std::path::Path;
use std::sync::atomic::Ordering;

fn write_offset_file(dir: &Path, name: &str, offset: u64) {
    std::fs::write(dir.join(name), offset.to_le_bytes()).unwrap();
}

#[test]
fn load_consumer_offsets_valid_files() {
    let dir = tempfile::tempdir().unwrap();
    write_offset_file(dir.path(), "1", 100);
    write_offset_file(dir.path(), "2", 200);
    write_offset_file(dir.path(), "3", 300);

    let offsets = load_consumer_offsets(dir.path().to_str().unwrap()).unwrap();

    assert_eq!(offsets.len(), 3);
    assert_eq!(offsets[0].consumer_id, 1);
    assert_eq!(offsets[0].offset.load(Ordering::Relaxed), 100);
    assert_eq!(offsets[0].kind, ConsumerKind::Consumer);
    assert_eq!(offsets[1].consumer_id, 2);
    assert_eq!(offsets[1].offset.load(Ordering::Relaxed), 200);
    assert_eq!(offsets[2].consumer_id, 3);
    assert_eq!(offsets[2].offset.load(Ordering::Relaxed), 300);
}

#[test]
fn load_consumer_offsets_skips_non_numeric_files() {
    let dir = tempfile::tempdir().unwrap();
    write_offset_file(dir.path(), ".DS_Store", 0);
    write_offset_file(dir.path(), "backup.bak", 0);
    write_offset_file(dir.path(), "1", 42);

    let offsets = load_consumer_offsets(dir.path().to_str().unwrap()).unwrap();

    assert_eq!(offsets.len(), 1);
    assert_eq!(offsets[0].consumer_id, 1);
    assert_eq!(offsets[0].offset.load(Ordering::Relaxed), 42);
}

#[test]
fn load_consumer_offsets_empty_dir() {
    let dir = tempfile::tempdir().unwrap();

    let offsets = load_consumer_offsets(dir.path().to_str().unwrap()).unwrap();

    assert!(offsets.is_empty());
}

#[test]
fn load_consumer_offsets_skips_directories() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("123")).unwrap();
    write_offset_file(dir.path(), "1", 77);

    let offsets = load_consumer_offsets(dir.path().to_str().unwrap()).unwrap();

    assert_eq!(offsets.len(), 1);
    assert_eq!(offsets[0].consumer_id, 1);
    assert_eq!(offsets[0].offset.load(Ordering::Relaxed), 77);
}

#[test]
fn load_consumer_offsets_nonexistent_dir() {
    let result = load_consumer_offsets("/tmp/nonexistent_iggy_test_dir_12345");

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        IggyError::CannotReadConsumerOffsets(_)
    ));
}

#[test]
fn load_consumer_group_offsets_valid_files() {
    let dir = tempfile::tempdir().unwrap();
    write_offset_file(dir.path(), "1", 500);
    write_offset_file(dir.path(), "2", 600);

    let offsets = load_consumer_group_offsets(dir.path().to_str().unwrap()).unwrap();

    assert_eq!(offsets.len(), 2);
    for (group_id, offset) in &offsets {
        assert_eq!(offset.kind, ConsumerKind::ConsumerGroup);
        assert_eq!(offset.consumer_id, group_id.0 as u32);
    }
    let ids: Vec<u32> = offsets.iter().map(|(_, co)| co.consumer_id).collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
}

#[test]
fn load_consumer_group_offsets_skips_non_numeric_files() {
    let dir = tempfile::tempdir().unwrap();
    write_offset_file(dir.path(), ".DS_Store", 0);
    write_offset_file(dir.path(), "notes.txt", 0);
    write_offset_file(dir.path(), "5", 999);

    let offsets = load_consumer_group_offsets(dir.path().to_str().unwrap()).unwrap();

    assert_eq!(offsets.len(), 1);
    assert_eq!(offsets[0].0.0, 5);
    assert_eq!(offsets[0].1.consumer_id, 5);
    assert_eq!(offsets[0].1.offset.load(Ordering::Relaxed), 999);
}

#[test]
fn load_consumer_group_offsets_empty_dir() {
    let dir = tempfile::tempdir().unwrap();

    let offsets = load_consumer_group_offsets(dir.path().to_str().unwrap()).unwrap();

    assert!(offsets.is_empty());
}

#[test]
fn load_consumer_group_offsets_nonexistent_dir() {
    let result = load_consumer_group_offsets("/tmp/nonexistent_iggy_test_dir_12345");

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        IggyError::CannotReadConsumerOffsets(_)
    ));
}
