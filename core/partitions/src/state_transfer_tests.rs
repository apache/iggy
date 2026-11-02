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
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;

#[test]
fn given_a_prefix_read_that_stops_without_a_result_should_land_an_error_and_wake() {
    let result = Rc::new(RefCell::new(None));
    let woken = Rc::new(Cell::new(false));
    let io = Rc::new(PartitionIoQuiescence::default());
    assert!(io.begin_local_work());
    let landing = Landing {
        io: Rc::clone(&io),
        result: Rc::clone(&result),
        wake: {
            let woken = Rc::clone(&woken);
            move || woken.set(true)
        },
        stopped: prefix_read_stopped,
    };

    drop(landing);

    assert!(
        matches!(result.borrow().as_ref(), Some(Err(_))),
        "the pull must see a failed read and fetch the whole artifact"
    );
    assert!(woken.get(), "the partition must resume the pull");
    assert!(
        io.is_interrupted(),
        "teardown must keep interrupted I/O fenced"
    );
}

#[test]
fn given_segment_read_error_when_classifying_should_tell_local_faults_from_stale_offers() {
    const EIO: i32 = 5;

    assert!(matches!(
        SegmentLoadError::classify(std::io::Error::from_raw_os_error(EIO)),
        SegmentLoadError::LocalFault(_)
    ));
    assert!(matches!(
        SegmentLoadError::classify(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        SegmentLoadError::LocalFault(_)
    ));
    assert!(matches!(
        SegmentLoadError::classify(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
        SegmentLoadError::Stale(_)
    ));
}

#[compio::test]
async fn given_transient_offset_io_failure_when_retried_should_succeed_without_exhausting_budget() {
    let attempts = std::cell::Cell::new(0);
    let result = retry_offset_mutation(|| {
        attempts.set(attempts.get() + 1);
        std::future::ready(if attempts.get() == 1 { Err(()) } else { Ok(7) })
    })
    .await;
    assert_eq!(result, Ok(7));
    assert_eq!(attempts.get(), 2);
}

#[compio::test]
async fn given_persistent_offset_io_failure_when_retried_should_stop_at_attempt_limit() {
    let attempts = std::cell::Cell::new(0);
    let result = retry_offset_mutation(|| {
        attempts.set(attempts.get() + 1);
        std::future::ready(Err::<(), _>(7))
    })
    .await;
    assert_eq!(result, Err(7));
    assert_eq!(attempts.get(), OFFSET_IO_ATTEMPTS);
}

fn table() -> ConsumerOffsetsWire {
    ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 43,
        emptied_at: 0,
        consumers: vec![(1, 10), (7, 42)],
        groups: vec![(2, 5)],
        external_groups: Vec::new(),
        dedup: vec![
            dedup_entry(11, 4, 90),
            dedup_entry(usize::MAX as u128 + 5, 9, 91),
        ],
        retirement_paid_through: 0,
    }
}

fn dedup_entry(client: u128, watermark: u64, latest_commit: u64) -> DedupWatermark {
    let header = ReplyHeader {
        command: iggy_binary_protocol::Command::Reply,
        operation: Operation::StoreConsumerOffset,
        client,
        request: watermark,
        commit: latest_commit,
        size: u32::try_from(size_of::<ReplyHeader>()).unwrap(),
        ..Default::default()
    };
    DedupWatermark {
        session: 1,
        reply: bytemuck::bytes_of(&header).to_vec(),
        client,
        user_id: 1,
        watermark,
        latest_commit,
        committed_window: 0b1011,
    }
}

#[test]
fn given_zero_frontier_transfer_when_capacity_is_uncommitted_should_keep_local_limit() {
    const LOCAL_CAPACITY: usize = 2;
    const FIRST_COMMITTED_CAPACITY: usize = 3;
    let wire = ConsumerOffsetsWire {
        dedup_capacity: 0,
        ..Default::default()
    };
    let decoded = ConsumerOffsetsWire::decode(&wire.encode()).unwrap();
    let mut restored = checked_retry_table(&decoded, 0, LOCAL_CAPACITY).unwrap();
    assert!(!restored.capacity_committed());
    assert_eq!(restored.capacity(), LOCAL_CAPACITY);
    restored.commit_capacity(FIRST_COMMITTED_CAPACITY).unwrap();
    assert_eq!(restored.capacity(), FIRST_COMMITTED_CAPACITY);
    assert!(checked_retry_table(&decoded, 1, LOCAL_CAPACITY).is_err());
    let with_receipt = ConsumerOffsetsWire {
        owner_retirement: None,
        dedup: vec![dedup_entry(1, 1, 1)],
        ..wire
    };
    assert!(ConsumerOffsetsWire::decode(&with_receipt.encode()).is_err());
}

#[test]
fn given_offset_table_when_encoded_should_round_trip() {
    let encoded = table().encode();
    assert_eq!(
        ConsumerOffsetsWire::decode(&encoded).expect("round trip"),
        table()
    );
}

#[test]
fn given_history_and_owner_evidence_when_restored_should_validate_order_and_identity() {
    const INCARNATION: u64 = 7;
    let mut wire = table();
    wire.required_metadata_frontier = 13;
    wire.history_transition = Some(InstalledPartitionHistory {
        transition: TransitionPartitionHistoryRequest {
            incarnation: INCARNATION,
            metadata_op: 12,
        },
        partition_op: 6,
    });
    wire.owners.push(InstalledConsumerGroupOwner {
        installation: InstallConsumerGroupOwnerRequest {
            incarnation: INCARNATION,
            group_id: 2,
            owner: iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner {
                client_id: 11,
                session: 4,
                generation: 9,
            },
            metadata_op: 13,
        },
        partition_op: 5,
    });
    wire.owner_retirement = Some(
        iggy_binary_protocol::requests::partitions::RetireConsumerGroupOwnersRequest {
            incarnation: INCARNATION,
            metadata_op: 13,
            next_group_id: 4,
            live_group_ids: vec![2],
        },
    );
    let restored = ConsumerOffsetsWire::decode(&wire.encode()).unwrap();
    assert_eq!(wire, restored);
    assert!(restored.validate_owners(INCARNATION, 91).is_ok());
    assert_eq!(
        restored.validate_owners(INCARNATION + 1, 91),
        Err(ConsumerOffsetsWireError::InvalidOwnership)
    );
    assert_eq!(
        restored.validate_owners(INCARNATION, 5),
        Err(ConsumerOffsetsWireError::InvalidOwnership)
    );

    let mut invalid = wire.clone();
    invalid
        .owner_retirement
        .as_mut()
        .unwrap()
        .live_group_ids
        .clear();
    assert_eq!(
        invalid.validate_owners(INCARNATION, 91),
        Err(ConsumerOffsetsWireError::InvalidOwnership)
    );
    invalid = wire.clone();
    invalid.owner_retirement.as_mut().unwrap().metadata_op += 1;
    assert_eq!(
        ConsumerOffsetsWire::decode(&invalid.encode()),
        Err(ConsumerOffsetsWireError::InvalidOwnership)
    );
    // The delete fence is terminal, so no owner shares or follows its op.
    for partition_op in [6, 7] {
        invalid = wire.clone();
        invalid.owners[0].partition_op = partition_op;
        assert_eq!(
            invalid.validate_owners(INCARNATION, 91),
            Err(ConsumerOffsetsWireError::InvalidOwnership)
        );
    }
    invalid = wire.clone();
    invalid.owners[0].installation.incarnation += 1;
    assert_eq!(
        invalid.validate_owners(INCARNATION, 91),
        Err(ConsumerOffsetsWireError::InvalidOwnership)
    );
    invalid = wire.clone();
    invalid.owners.push(invalid.owners[0]);
    assert_eq!(
        ConsumerOffsetsWire::decode(&invalid.encode()),
        Err(ConsumerOffsetsWireError::InvalidOwnership)
    );
    invalid = wire;
    invalid.required_metadata_frontier = 11;
    assert_eq!(
        ConsumerOffsetsWire::decode(&invalid.encode()),
        Err(ConsumerOffsetsWireError::InvalidOwnership)
    );
}

#[test]
fn transferred_prepare_checksum_is_covered_by_the_artifact() {
    let mut table = table();
    table.prepare_checksum = Some(u128::MAX - 7);
    let bytes = table.encode();
    assert_eq!(
        ConsumerOffsetsWire::decode(&bytes)
            .unwrap()
            .prepare_checksum,
        table.prepare_checksum
    );
    let mut corrupt = bytes;
    let position = corrupt.len() - 9;
    corrupt[position] ^= 1;
    assert!(matches!(
        ConsumerOffsetsWire::decode(&corrupt),
        Err(ConsumerOffsetsWireError::ChecksumMismatch { .. })
    ));
}

#[test]
fn given_empty_table_when_encoded_should_round_trip() {
    let empty = ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 0,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
    };
    let encoded = empty.encode();
    assert_eq!(
        ConsumerOffsetsWire::decode(&encoded).expect("round trip"),
        empty
    );
}

#[test]
fn given_emptied_table_when_encoded_should_round_trip_the_emptied_op() {
    let emptied = ConsumerOffsetsWire {
        emptied_at: 9,
        consumers: Vec::new(),
        groups: Vec::new(),
        ..table()
    };
    assert_eq!(
        ConsumerOffsetsWire::decode(&emptied.encode()).expect("round trip"),
        emptied
    );
}

/// Version 7 had no emptied-at op after the next offset. Read in place, its
/// later fields would land one word early.
#[test]
fn given_version_7_offsets_artifact_when_decoded_should_reject_it() {
    const RELEASED_VERSION: u8 = 7;
    const EMPTIED_AT_START: usize =
        CONSUMER_OFFSETS_MAGIC.len() + size_of::<u8>() + size_of::<u64>();
    let encoded = table().encode();
    let mut released = encoded[..encoded.len() - size_of::<u64>()].to_vec();
    released.drain(EMPTIED_AT_START..EMPTIED_AT_START + size_of::<u64>());
    released[CONSUMER_OFFSETS_MAGIC.len()] = RELEASED_VERSION;
    let trailer = state_artifact_checksum(&released);
    released.extend_from_slice(&trailer.to_le_bytes());

    assert_eq!(
        ConsumerOffsetsWire::decode(&released),
        Err(ConsumerOffsetsWireError::UnsupportedVersion {
            version: RELEASED_VERSION
        })
    );
}

#[test]
fn given_flipped_bit_when_decoded_should_reject_checksum() {
    let mut encoded = table().encode();
    encoded[6] ^= 1;
    assert!(matches!(
        ConsumerOffsetsWire::decode(&encoded),
        Err(ConsumerOffsetsWireError::ChecksumMismatch { .. })
    ));
}

#[test]
fn given_truncated_bytes_when_decoded_should_reject() {
    let encoded = table().encode();
    for len in 0..encoded.len() {
        assert!(
            ConsumerOffsetsWire::decode(&encoded[..len]).is_err(),
            "strict prefix of {len} bytes must fail closed"
        );
    }
}

#[test]
fn given_unknown_version_when_decoded_should_reject() {
    for version in [CONSUMER_OFFSETS_VERSION - 1, CONSUMER_OFFSETS_VERSION + 1] {
        let mut wrong = table().encode();
        wrong[CONSUMER_OFFSETS_MAGIC.len()] = version;
        let content_len = wrong.len() - size_of::<u64>();
        let trailer = state_artifact_checksum(&wrong[..content_len]);
        wrong[content_len..].copy_from_slice(&trailer.to_le_bytes());
        assert_eq!(
            ConsumerOffsetsWire::decode(&wrong),
            Err(ConsumerOffsetsWireError::UnsupportedVersion { version })
        );
    }
}

#[test]
fn given_entry_in_every_section_when_encoded_should_match_the_version_pins() {
    // Nothing else fails when a layout changes and its version bump is
    // forgotten, and a peer on another release then reads the artifact out
    // of place. One entry per section makes each length cover the header,
    // every count field and every entry stride. Changing a length is the
    // reminder to change its version. The trailing `retirement_paid_through`
    // is optional on decode, so it kept version 8 readable without a bump.
    const WITHOUT_EXTERNAL_LEN: usize = 563;
    const WITH_EXTERNAL_LEN: usize = 575;

    const INCARNATION: u64 = 1;
    let with_external = ConsumerOffsetsWire {
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 3,
        prepare_checksum: Some(1),
        checkpoint_prepare: Vec::new(),
        next_offset: 0,
        emptied_at: 0,
        consumers: vec![(1, 0)],
        groups: vec![(1, 0)],
        external_groups: vec![(1, 0)],
        dedup: vec![dedup_entry(1, 0, 0)],
        owners: vec![InstalledConsumerGroupOwner {
            installation: InstallConsumerGroupOwnerRequest {
                incarnation: INCARNATION,
                group_id: 1,
                owner: ConsumerGroupOwner {
                    client_id: 1,
                    session: 1,
                    generation: 1,
                },
                metadata_op: 2,
            },
            partition_op: 1,
        }],
        history_transition: Some(InstalledPartitionHistory {
            transition: TransitionPartitionHistoryRequest {
                incarnation: INCARNATION,
                metadata_op: 3,
            },
            partition_op: 2,
        }),
        owner_retirement: Some(RetireConsumerGroupOwnersRequest {
            incarnation: INCARNATION,
            metadata_op: 3,
            next_group_id: 2,
            live_group_ids: vec![1],
        }),
        retirement_paid_through: 0,
    };
    let without_external = ConsumerOffsetsWire {
        external_groups: Vec::new(),
        ..with_external.clone()
    };

    for (wire, len) in [
        (&without_external, WITHOUT_EXTERNAL_LEN),
        (&with_external, WITH_EXTERNAL_LEN),
    ] {
        let encoded = wire.encode();
        assert_eq!(
            (encoded[CONSUMER_OFFSETS_MAGIC.len()], encoded.len()),
            (CONSUMER_OFFSETS_VERSION, len),
            "a consumer-offsets layout changed; bump its version with it"
        );
    }
    assert_eq!(
        CONSUMER_OFFSETS_VERSION, 8,
        "a consumer-offsets version moved; confirm its layout moved with it"
    );
}

/// Legacy artifacts omit session-qualified receipts and cannot restore retry protection.
#[test]
fn given_server_0_9_0_artifact_when_decoded_should_reject() {
    let wire = ConsumerOffsetsWire {
        prepare_checksum: Some(u128::MAX - 7),
        checkpoint_prepare: vec![1, 2, 3],
        ..table()
    };
    let mut released = Vec::new();
    released.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
    released.push(1);
    // 0.9.0 wrote its purge generation here.
    released.extend_from_slice(&0u64.to_le_bytes());
    released.extend_from_slice(&wire.next_offset.to_le_bytes());
    for count in [wire.consumers.len(), wire.groups.len(), wire.dedup.len()] {
        released.extend_from_slice(&u32::try_from(count).unwrap().to_le_bytes());
    }
    for (id, offset) in wire.consumers.iter().chain(&wire.groups) {
        released.extend_from_slice(&id.to_le_bytes());
        released.extend_from_slice(&offset.to_le_bytes());
    }
    for entry in &wire.dedup {
        released.extend_from_slice(&entry.client.to_le_bytes());
        released.extend_from_slice(&entry.watermark.to_le_bytes());
        released.extend_from_slice(&entry.latest_commit.to_le_bytes());
        released.extend_from_slice(&entry.user_id.to_le_bytes());
        released.extend_from_slice(&entry.committed_window.to_le_bytes());
    }
    released.push(1);
    released.extend_from_slice(&wire.prepare_checksum.unwrap().to_le_bytes());
    released.extend_from_slice(
        &u32::try_from(wire.checkpoint_prepare.len())
            .unwrap()
            .to_le_bytes(),
    );
    released.extend_from_slice(&wire.checkpoint_prepare);
    let trailer = state_artifact_checksum(&released);
    released.extend_from_slice(&trailer.to_le_bytes());

    assert_eq!(
        ConsumerOffsetsWire::decode(&released),
        Err(ConsumerOffsetsWireError::UnsupportedVersion { version: 1 })
    );
}

#[test]
fn given_external_groups_when_encoded_should_round_trip() {
    let wire = ConsumerOffsetsWire {
        external_groups: vec![(2, 0), (9, 1 << 40)],
        ..table()
    };

    assert_eq!(ConsumerOffsetsWire::decode(&wire.encode()), Ok(wire));
}

#[test]
fn given_foreign_magic_when_decoded_should_reject() {
    let mut wrong = table().encode();
    // Rewrite the magic and re-seal so only the magic check can fire.
    wrong[0] = b'X';
    let content_len = wrong.len() - size_of::<u64>();
    let trailer = state_artifact_checksum(&wrong[..content_len]);
    wrong[content_len..].copy_from_slice(&trailer.to_le_bytes());
    assert_eq!(
        ConsumerOffsetsWire::decode(&wrong),
        Err(ConsumerOffsetsWireError::BadMagic)
    );
}

#[test]
fn given_unordered_dedup_clients_when_decoded_should_reject() {
    let unordered = ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 0,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: vec![dedup_entry(9, 1, 1), dedup_entry(4, 2, 2)],
        retirement_paid_through: 0,
    };
    assert_eq!(
        ConsumerOffsetsWire::decode(&unordered.encode()),
        Err(ConsumerOffsetsWireError::NonAscendingClient { client: 4 })
    );
}

#[test]
fn given_reserved_client_in_dedup_when_decoded_should_reject() {
    let reserved = ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 0,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: vec![dedup_entry(0, 1, 1), dedup_entry(4, 2, 2)],
        retirement_paid_through: 0,
    };
    assert_eq!(
        ConsumerOffsetsWire::decode(&reserved.encode()),
        Err(ConsumerOffsetsWireError::ReservedClient)
    );
}

#[test]
fn given_unknown_artifact_formats_when_decoded_should_reject() {
    for magic in [b"BAD1", b"ICO9"] {
        let mut bytes = table().encode();
        bytes[..4].copy_from_slice(magic);
        let length = bytes.len() - 8;
        let checksum = state_artifact_checksum(&bytes[..length]);
        bytes[length..].copy_from_slice(&checksum.to_le_bytes());
        assert_eq!(
            ConsumerOffsetsWire::decode(&bytes),
            Err(ConsumerOffsetsWireError::BadMagic)
        );
    }
}

#[test]
fn given_dedup_count_past_ceiling_when_decoded_should_reject_before_allocating() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
    bytes.push(CONSUMER_OFFSETS_VERSION);
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&(u32::try_from(CLIENTS_TABLE_SLOT_MAX).unwrap()).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&(CONSUMER_OFFSETS_ENTRIES_MAX + 1).to_le_bytes());
    let trailer = state_artifact_checksum(&bytes);
    bytes.extend_from_slice(&trailer.to_le_bytes());
    assert_eq!(
        ConsumerOffsetsWire::decode(&bytes),
        Err(ConsumerOffsetsWireError::TooManyEntries {
            section: "dedup",
            count: CONSUMER_OFFSETS_ENTRIES_MAX + 1,
            max: u32::try_from(CLIENTS_TABLE_SLOT_MAX).unwrap(),
        })
    );
}

#[test]
fn given_dedup_count_exceeding_bytes_when_decoded_should_reject_as_truncated() {
    // Under the ceiling but past the bytes present: the stride guard is
    // what stops a ~30 byte artifact reserving megabytes.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
    bytes.push(CONSUMER_OFFSETS_VERSION);
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&(u32::try_from(CLIENTS_TABLE_SLOT_MAX).unwrap()).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&1_000u32.to_le_bytes());
    let trailer = state_artifact_checksum(&bytes);
    bytes.extend_from_slice(&trailer.to_le_bytes());
    assert_eq!(
        ConsumerOffsetsWire::decode(&bytes),
        Err(ConsumerOffsetsWireError::Truncated)
    );
}

#[test]
fn given_count_past_ceiling_when_decoded_should_reject_before_allocating() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
    bytes.push(CONSUMER_OFFSETS_VERSION);
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&(u32::try_from(CLIENTS_TABLE_SLOT_MAX).unwrap()).to_le_bytes());
    bytes.extend_from_slice(&(CONSUMER_OFFSETS_ENTRIES_MAX + 1).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    let trailer = state_artifact_checksum(&bytes);
    bytes.extend_from_slice(&trailer.to_le_bytes());
    assert_eq!(
        ConsumerOffsetsWire::decode(&bytes),
        Err(ConsumerOffsetsWireError::TooManyEntries {
            section: "consumers",
            count: CONSUMER_OFFSETS_ENTRIES_MAX + 1,
            max: CONSUMER_OFFSETS_ENTRIES_MAX,
        })
    );
}

#[test]
fn given_owner_count_past_ceiling_when_decoded_should_reject_before_allocating() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
    bytes.push(CONSUMER_OFFSETS_VERSION);
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&(u32::try_from(CLIENTS_TABLE_SLOT_MAX).unwrap()).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&(CONSUMER_OFFSETS_ENTRIES_MAX + 1).to_le_bytes());
    let trailer = state_artifact_checksum(&bytes);
    bytes.extend_from_slice(&trailer.to_le_bytes());
    assert_eq!(
        ConsumerOffsetsWire::decode(&bytes),
        Err(ConsumerOffsetsWireError::TooManyEntries {
            section: "owners",
            count: CONSUMER_OFFSETS_ENTRIES_MAX + 1,
            max: CONSUMER_OFFSETS_ENTRIES_MAX,
        })
    );
}

#[test]
fn given_duplicate_or_unordered_ids_when_decoded_should_reject() {
    let duplicate = ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 0,
        emptied_at: 0,
        consumers: vec![(5, 1), (5, 2)],
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
    };
    assert_eq!(
        ConsumerOffsetsWire::decode(&duplicate.encode()),
        Err(ConsumerOffsetsWireError::NonAscendingId {
            section: "consumers",
            id: 5,
        })
    );
    let unordered = ConsumerOffsetsWire {
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 0,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: vec![(9, 1), (4, 2)],
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
    };
    assert_eq!(
        ConsumerOffsetsWire::decode(&unordered.encode()),
        Err(ConsumerOffsetsWireError::NonAscendingId {
            section: "groups",
            id: 4,
        })
    );
}

#[test]
fn given_trailing_bytes_when_decoded_should_reject() {
    let mut padded = table().encode();
    let content_len = padded.len() - size_of::<u64>();
    padded.truncate(content_len);
    padded.push(0);
    let trailer = state_artifact_checksum(&padded);
    padded.extend_from_slice(&trailer.to_le_bytes());
    assert_eq!(
        ConsumerOffsetsWire::decode(&padded),
        Err(ConsumerOffsetsWireError::TrailingBytes { extra: 1 }),
        "bytes past the last section must fail closed"
    );
}

#[test]
fn given_an_artifact_without_paid_through_when_decoded_should_read_it_as_zero() {
    let paid = ConsumerOffsetsWire {
        retirement_paid_through: 9,
        ..table()
    };
    let encoded = paid.encode();
    assert_eq!(ConsumerOffsetsWire::decode(&encoded), Ok(paid));
    let mut legacy = encoded[..encoded.len() - 2 * size_of::<u64>()].to_vec();
    let trailer = state_artifact_checksum(&legacy);
    legacy.extend_from_slice(&trailer.to_le_bytes());
    assert_eq!(ConsumerOffsetsWire::decode(&legacy), Ok(table()));
}

#[test]
fn given_offset_count_above_transfer_ceiling_when_validated_should_reject() {
    assert!(validate_consumer_offset_transfer_count(ConsumerKind::Consumer, 4, 4).is_ok());
    assert!(matches!(
        validate_consumer_offset_transfer_count(ConsumerKind::ConsumerGroup, 5, 4),
        Err(PartitionTransferUnavailable::ConsumerOffsetsTooLarge {
            kind: ConsumerKind::ConsumerGroup,
            count: 5,
            max: 4,
        })
    ));
    let error = validate_consumer_offset_transfer_count(ConsumerKind::Consumer, 5, 4)
        .expect_err("count above ceiling");
    assert!(
        !error.transient(),
        "an artifact that cannot fit the decoder will not heal by retrying"
    );
}
