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

use super::{
    Chain, ChainError, ChainRecord, EvictionFailure, EvictionJob, HEADER_SIZE, HashSource,
    LiveChain, ObjectPrefix, RECORD_SIZE, SegmentChain, SegmentDigest, SegmentHashes,
    SegmentRecord, append_record, chain_path, create_chain, decode_chain, encode_header,
    hash_segment_file, read_chain,
};
use crate::state_transfer::ChunkHasher;

const PREFIX: ObjectPrefix = ObjectPrefix {
    stream_id: 3,
    topic_id: 5,
    partition_id: 7,
    created_revision: 11,
};

fn hashes_of(bytes: &[u8]) -> SegmentHashes {
    SegmentHashes {
        blake3: *blake3::hash(bytes).as_bytes(),
        crc32c: crc32c::crc32c(bytes),
    }
}

const fn segment(start_offset: u64, source: HashSource) -> SegmentRecord {
    SegmentRecord {
        start_offset,
        end_offset: start_offset + 9,
        start_timestamp: 100,
        end_timestamp: 200,
        max_timestamp: 250,
        size: 4096,
        incarnation: 11,
        hash: [7; 32],
        crc32c: 0x1234_5678,
        source,
    }
}

fn records() -> [ChainRecord; 3] {
    [
        ChainRecord::TrimPoint(0),
        ChainRecord::Sealed(segment(0, HashSource::CommitPath)),
        ChainRecord::Remote(segment(10, HashSource::Disk)),
    ]
}

fn chain_bytes(records: &[ChainRecord]) -> Vec<u8> {
    let mut bytes = encode_header(&PREFIX).to_vec();
    for record in records {
        bytes.extend_from_slice(&record.encode());
    }
    bytes
}

#[test]
fn given_records_when_encoded_and_decoded_should_round_trip() {
    let records = records();
    let chain = decode_chain(&chain_bytes(&records)).unwrap();
    assert_eq!(
        chain,
        Chain {
            prefix: PREFIX,
            records: records.to_vec(),
            valid_len: (HEADER_SIZE + 3 * RECORD_SIZE) as u64,
            torn_tail: false,
        }
    );
}

#[test]
fn given_a_torn_last_record_when_decoded_should_keep_the_whole_ones() {
    let records = records();
    let bytes = chain_bytes(&records);
    let mut garbled = bytes.clone();
    let last = garbled.len() - 1;
    garbled[last] ^= 1;
    for torn in [
        &bytes[..bytes.len() - 1],
        &bytes[..=(bytes.len() - RECORD_SIZE)],
        &garbled,
    ] {
        let chain = decode_chain(torn).unwrap();
        assert!(chain.torn_tail);
        assert_eq!(chain.records, records[..2]);
        assert_eq!(chain.valid_len, (HEADER_SIZE + 2 * RECORD_SIZE) as u64);
    }
}

#[test]
fn given_any_flipped_byte_in_a_middle_record_when_decoded_should_refuse_the_chain() {
    let bytes = chain_bytes(&records());
    for at in HEADER_SIZE..HEADER_SIZE + RECORD_SIZE {
        let mut flipped = bytes.clone();
        flipped[at] ^= 1;
        assert!(
            matches!(decode_chain(&flipped), Err(ChainError::Record { index: 0 })),
            "a flip at byte {at} must not read as a valid record"
        );
    }
}

#[test]
fn given_a_damaged_or_short_header_when_decoded_should_refuse_the_chain() {
    let bytes = chain_bytes(&[]);
    for at in 0..HEADER_SIZE {
        let mut flipped = bytes.clone();
        flipped[at] ^= 1;
        assert!(matches!(decode_chain(&flipped), Err(ChainError::Header)));
    }
    assert!(matches!(
        decode_chain(&bytes[..HEADER_SIZE - 1]),
        Err(ChainError::Header)
    ));
}

#[compio::test]
async fn given_a_created_chain_when_records_are_appended_should_read_them_back() {
    let directory = tempfile::tempdir().unwrap();
    let directory = directory.path().to_string_lossy().into_owned();
    assert!(read_chain(&directory).await.unwrap().is_none());
    create_chain(&directory, &PREFIX).await.unwrap();

    let path = chain_path(&directory);
    let mut at = HEADER_SIZE as u64;
    for record in records() {
        append_record(&path, at, &record).await.unwrap();
        at += RECORD_SIZE as u64;
    }
    let chain = read_chain(&directory).await.unwrap().unwrap();
    assert_eq!(chain.prefix, PREFIX);
    assert_eq!(chain.records, records());
    assert_eq!(chain.valid_len, at);
}

#[compio::test]
async fn given_a_torn_tail_when_appending_at_the_valid_length_should_overwrite_it() {
    let directory = tempfile::tempdir().unwrap();
    let directory = directory.path().to_string_lossy().into_owned();
    create_chain(&directory, &PREFIX).await.unwrap();
    let path = chain_path(&directory);
    let [first, second, _] = records();
    append_record(&path, HEADER_SIZE as u64, &first)
        .await
        .unwrap();
    let mut torn = std::fs::read(&path).unwrap();
    torn.extend_from_slice(&second.encode()[..RECORD_SIZE / 2]);
    std::fs::write(&path, &torn).unwrap();

    let chain = read_chain(&directory).await.unwrap().unwrap();
    assert!(chain.torn_tail);
    append_record(&path, chain.valid_len, &second)
        .await
        .unwrap();

    let chain = read_chain(&directory).await.unwrap().unwrap();
    assert!(!chain.torn_tail);
    assert_eq!(chain.records, [first, second]);
}

/// A file spanning more than one read chunk hashes as the whole bytes do, and
/// a shorter size hashes only its prefix.
/// The next eviction starts only after a directory sync made this unlink
/// durable, so a sync that fails must fail the eviction, even on the retry
/// that finds the `.log` already gone.
#[compio::test]
async fn given_a_log_already_unlinked_when_the_directory_cannot_be_synced_should_fail_the_eviction()
{
    let directory = tempfile::tempdir().unwrap();
    let evict = |messages_path: std::path::PathBuf| EvictionJob {
        start_offset: 0,
        append: None,
        messages_path: Some(messages_path.to_string_lossy().into_owned()),
    };

    let unsynced = evict(directory.path().join("missing").join("0.log"))
        .execute()
        .await;
    assert_eq!(unsynced.outcome, Err(EvictionFailure::Sync));

    let log = directory.path().join("0.log");
    std::fs::write(&log, b"log").unwrap();
    assert_eq!(evict(log.clone()).execute().await.outcome, Ok(()));
    assert!(!log.exists());
    assert_eq!(evict(log).execute().await.outcome, Ok(()));
}

#[compio::test]
async fn given_a_segment_file_when_hashed_from_disk_should_match_its_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("00000000000000000000.log");
    let bytes: Vec<u8> = (0..9 << 20)
        .map(|index: u32| index.to_le_bytes()[0])
        .collect();
    std::fs::write(&path, &bytes).unwrap();
    let path = path.to_string_lossy().into_owned();

    let whole = hash_segment_file(&path, bytes.len() as u64).await.unwrap();
    assert_eq!(whole, hashes_of(&bytes));
    let prefix = hash_segment_file(&path, 1000).await.unwrap();
    assert_eq!(prefix, hashes_of(&bytes[..1000]));
    assert!(
        hash_segment_file(&path, bytes.len() as u64 + 1)
            .await
            .is_err()
    );
}

#[test]
fn given_a_floor_advance_when_encoded_should_decode_to_the_same_record() {
    let advance = ChainRecord::Advanced(crate::tiered_floor::AdvanceTieredFloor {
        start: 10,
        up_to: 19,
        hash: [5; 32],
    });
    let mut bytes = encode_header(&PREFIX).to_vec();
    bytes.extend_from_slice(&advance.encode());
    assert_eq!(decode_chain(&bytes).unwrap().records, [advance]);

    // A byte this build never writes in an advance refuses the record.
    let stray = HEADER_SIZE + 40;
    bytes[stray] = 1;
    assert_eq!(decode_chain(&bytes).unwrap().records, Vec::new());
}

#[test]
fn given_advances_waiting_for_segments_when_one_seals_should_settle_only_those_it_holds() {
    let advanced = |up_to| {
        ChainRecord::Advanced(crate::tiered_floor::AdvanceTieredFloor {
            start: up_to,
            up_to,
            hash: [7; 32],
        })
    };
    let waiting = |chain: &SegmentChain| chain.advances.keys().copied().collect::<Vec<_>>();
    let empty = Chain {
        prefix: PREFIX,
        records: Vec::new(),
        valid_len: HEADER_SIZE as u64,
        torn_tail: false,
    };
    let mut chain = SegmentChain::load(String::new(), LiveChain::new(&empty), None);
    // The first ends its segment, the second ends inside the next one, and
    // the third is two segments ahead.
    for up_to in [9, 14, 29] {
        assert_eq!(chain.note_appended(&advanced(up_to)), Vec::<u64>::new());
    }

    let first = ChainRecord::Sealed(segment(0, HashSource::CommitPath));
    assert_eq!(chain.note_appended(&first), Vec::<u64>::new());
    assert!(chain.evictable(0, 9));
    assert_eq!(waiting(&chain), [14, 29]);

    let second = ChainRecord::Sealed(segment(10, HashSource::CommitPath));
    assert_eq!(chain.note_appended(&second), [14]);
    assert!(!chain.evictable(10, 19));
    assert_eq!(waiting(&chain), [29]);

    assert_eq!(
        chain.note_appended(&ChainRecord::TrimPoint(29)),
        Vec::<u64>::new()
    );
    assert_eq!(waiting(&chain), Vec::<u64>::new());
    assert_eq!(chain.diverged().iter().copied().collect::<Vec<_>>(), [14]);
}

#[test]
fn given_a_digest_when_fed_in_pieces_should_cover_and_hash_the_whole_bytes() {
    let mut digest = SegmentDigest::new(20);
    digest.update(b"segment ");
    digest.update(b"bytes");
    assert!(digest.covers(20, 13));
    assert!(!digest.covers(20, 12));
    assert!(!digest.covers(0, 13));
    assert_eq!(digest.finalize(), hashes_of(b"segment bytes"));
}
