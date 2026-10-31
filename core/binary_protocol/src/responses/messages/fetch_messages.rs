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

//! Codec for the `FetchMessages` reply.
//!
//! The section table comes first, so a client can send its next fetch before
//! it decodes any record. The records of each section follow the table in
//! table order, as stored, in the record format of a `PollMessages` reply.

use crate::codec::{WireDecode, read_u32_le, read_u64_le};
use crate::framing::STATUS_OK;
use crate::primitives::partition_history::PartitionContext;
use crate::{MAX_PARTITIONS_PER_REQUEST, WireError};
use bytes::{BufMut, Bytes, BytesMut};
use std::cmp::Ordering;

/// Size of the section count that opens the reply.
pub const FETCH_SECTIONS_COUNT_SIZE: usize = size_of::<u32>();
/// Size of one table entry: `partition_id(4) + status(4) + current_offset(8)`,
/// `messages_count(4) + context(24) + next_offset(8) + records_len(4)`.
pub const FETCH_SECTION_HEADER_SIZE: usize =
    4 * size_of::<u32>() + 2 * size_of::<u64>() + PartitionContext::ENCODED_SIZE;

/// One entry of the section table.
///
/// `current_offset`, `messages_count` and `context` mean what they mean in a
/// `PollMessages` reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchMessagesSectionHeader {
    pub partition_id: u32,
    /// [`STATUS_OK`] when the partition was read. Any other value is the
    /// `IggyError` code of this target, and the section holds no records.
    pub status: u32,
    pub current_offset: u64,
    pub messages_count: u32,
    pub context: PartitionContext,
    /// Where the next fetch of this partition starts.
    pub next_offset: u64,
    pub records_len: u32,
}

impl FetchMessagesSectionHeader {
    /// The entry bytes, for a reply head that is written in place.
    #[must_use]
    pub fn to_le_bytes(&self) -> [u8; FETCH_SECTION_HEADER_SIZE] {
        let mut bytes = [0; FETCH_SECTION_HEADER_SIZE];
        let mut entry = &mut bytes[..];
        entry.put_u32_le(self.partition_id);
        entry.put_u32_le(self.status);
        entry.put_u64_le(self.current_offset);
        entry.put_u32_le(self.messages_count);
        entry.put_slice(&self.context.to_le_bytes());
        entry.put_u64_le(self.next_offset);
        entry.put_u32_le(self.records_len);
        bytes
    }

    const fn is_error_with_records(&self) -> bool {
        self.status != STATUS_OK && self.records_len != 0
    }
}

impl WireDecode for FetchMessagesSectionHeader {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let partition_id = read_u32_le(buf, 0)?;
        let mut pos = size_of::<u32>();
        let status = read_u32_le(buf, pos)?;
        pos += size_of::<u32>();
        let current_offset = read_u64_le(buf, pos)?;
        pos += size_of::<u64>();
        let messages_count = read_u32_le(buf, pos)?;
        pos += size_of::<u32>();
        let (context, n) = PartitionContext::decode(&buf[pos..])?;
        pos += n;
        let next_offset = read_u64_le(buf, pos)?;
        pos += size_of::<u64>();
        let records_len = read_u32_le(buf, pos)?;
        pos += size_of::<u32>();
        Ok((
            Self {
                partition_id,
                status,
                current_offset,
                messages_count,
                context,
                next_offset,
                records_len,
            },
            pos,
        ))
    }
}

/// One section of a reply and its records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchMessagesSection<'a> {
    pub header: FetchMessagesSectionHeader,
    /// Batch records as stored, `header.records_len` bytes.
    pub records: &'a [u8],
}

/// Borrowed `FetchMessages` reply. Does not own message data.
///
/// Wire format:
/// ```text
/// [sections_count:4 LE] sections_count x
///   [partition_id:4 LE][status:4 LE][current_offset:8 LE][messages_count:4 LE]
///   [context:24][next_offset:8 LE][records_len:4 LE]
/// [records of section 1][records of section 2]...
/// ```
///
/// A request with `n` targets gets at most `n` sections, so a reply holds at
/// most `FetchMessagesRequest::reply_limit` bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchMessagesResponse<'a> {
    pub sections: Vec<FetchMessagesSection<'a>>,
}

impl<'a> FetchMessagesResponse<'a> {
    /// Size of the head of a reply with `sections` sections: the count and
    /// the table.
    #[must_use]
    pub const fn head_size(sections: usize) -> usize {
        FETCH_SECTIONS_COUNT_SIZE + sections * FETCH_SECTION_HEADER_SIZE
    }

    /// Decode a reply body. Borrows the buffer.
    /// [`FetchMessagesSectionIterator`] reads the same sections without
    /// allocating.
    ///
    /// # Errors
    /// The errors of [`FetchMessagesSectionIterator::new`].
    pub fn decode(buf: &'a [u8]) -> Result<Self, WireError> {
        Ok(Self {
            sections: FetchMessagesSectionIterator::new(buf)?.collect(),
        })
    }

    /// Writes the count and the table into `head`, so the records can follow
    /// as stored fragments, in table order and without a copy.
    ///
    /// # Errors
    /// Returns `WireError::Validation` when `head` is not
    /// [`Self::head_size`] bytes, for more sections than a request has
    /// targets, or for an error section with records.
    pub fn write_head(
        headers: &[FetchMessagesSectionHeader],
        head: &mut [u8],
    ) -> Result<(), WireError> {
        let count = u32::try_from(headers.len())
            .ok()
            .filter(|count| *count <= MAX_PARTITIONS_PER_REQUEST);
        let Some(count) = count else {
            return Err(WireError::Validation(
                "fetch reply has more sections than a request has targets".into(),
            ));
        };
        if head.len() != Self::head_size(headers.len()) {
            return Err(WireError::Validation(
                "fetch reply head does not match its table".into(),
            ));
        }
        if headers
            .iter()
            .any(FetchMessagesSectionHeader::is_error_with_records)
        {
            return Err(WireError::Validation(
                "fetch error section carries records".into(),
            ));
        }
        let (count_bytes, table) = head.split_at_mut(FETCH_SECTIONS_COUNT_SIZE);
        count_bytes.copy_from_slice(&count.to_le_bytes());
        let (entries, _) = table.as_chunks_mut::<FETCH_SECTION_HEADER_SIZE>();
        for (entry, header) in entries.iter_mut().zip(headers) {
            *entry = header.to_le_bytes();
        }
        Ok(())
    }

    /// The whole reply in one buffer, for tests and fakes. A server writes
    /// the head with [`Self::write_head`] and sends the records as stored.
    ///
    /// # Errors
    /// The errors of [`Self::write_head`], and a section whose `records_len`
    /// is not the length of its records.
    pub fn to_bytes(&self) -> Result<Bytes, WireError> {
        let headers = self
            .sections
            .iter()
            .map(|section| {
                if section.header.records_len as usize == section.records.len() {
                    Ok(section.header)
                } else {
                    Err(WireError::Validation(
                        "fetch section records_len differs from its records".into(),
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut buf = BytesMut::zeroed(Self::head_size(headers.len()));
        Self::write_head(&headers, &mut buf)?;
        for section in &self.sections {
            buf.extend_from_slice(section.records);
        }
        Ok(buf.freeze())
    }
}

/// The sections of a reply in table order, read in place. [`Self::new`]
/// proves the layout of the whole reply before the first section.
#[derive(Debug, Clone)]
pub struct FetchMessagesSectionIterator<'a> {
    table: std::slice::Iter<'a, [u8; FETCH_SECTION_HEADER_SIZE]>,
    records: &'a [u8],
}

impl<'a> FetchMessagesSectionIterator<'a> {
    /// Walks a reply body without allocating. Borrows the buffer.
    ///
    /// # Errors
    /// Returns `WireError` for a table that is truncated or names more
    /// sections than a request has targets, an error section with records, or
    /// records that do not fill the rest of the body exactly.
    pub fn new(buf: &'a [u8]) -> Result<Self, WireError> {
        let count = read_u32_le(buf, 0)?;
        if count > MAX_PARTITIONS_PER_REQUEST {
            return Err(WireError::Validation(
                "fetch reply has more sections than a request has targets".into(),
            ));
        }
        let Some((head, records)) =
            buf.split_at_checked(FetchMessagesResponse::head_size(count as usize))
        else {
            return Err(WireError::Validation(
                "fetch reply ends inside its section table".into(),
            ));
        };
        let (table, _) = head[FETCH_SECTIONS_COUNT_SIZE..].as_chunks::<FETCH_SECTION_HEADER_SIZE>();
        let mut records_len = 0_usize;
        for entry in table {
            let (header, _) = FetchMessagesSectionHeader::decode(entry)?;
            if header.is_error_with_records() {
                return Err(WireError::Validation(
                    "fetch error section carries records".into(),
                ));
            }
            records_len = records_len.saturating_add(header.records_len as usize);
        }
        match records_len.cmp(&records.len()) {
            Ordering::Greater => Err(WireError::Validation(
                "fetch section records run past the reply".into(),
            )),
            Ordering::Less => Err(WireError::Validation(
                "fetch reply has bytes past its sections".into(),
            )),
            Ordering::Equal => Ok(Self {
                table: table.iter(),
                records,
            }),
        }
    }
}

impl<'a> Iterator for FetchMessagesSectionIterator<'a> {
    type Item = FetchMessagesSection<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // `new` decoded every entry and proved that the records fill the body,
        // so neither step can fail.
        let (header, _) = FetchMessagesSectionHeader::decode(self.table.next()?).ok()?;
        let (records, rest) = self.records.split_at_checked(header.records_len as usize)?;
        self.records = rest;
        Some(FetchMessagesSection { header, records })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.table.len(), Some(self.table.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{
        BATCH_HEADER_SIZE, BATCH_RESERVED_OFFSET, BatchHeader, calculate_batch_checksum,
        decode_batch_slice,
    };
    use crate::requests::messages::{
        FetchMessagesRequest, FetchMessagesTarget, MIN_DEFERRED_POLL_MAX_BYTES,
        MIN_DEFERRED_POLL_REPLY_MARGIN_US, RawMessage, SendMessagesEncoder,
    };
    use crate::responses::messages::poll_messages::PolledBatchesIterator;
    use crate::{WireConsumer, WireIdentifier, WirePartitioning, WirePollingStrategy};

    /// `IggyError::NotPrimary`, the error a backup gives for a read.
    const NOT_PRIMARY: u32 = 89;

    fn served(partition_id: u32, records: &[u8]) -> FetchMessagesSection<'_> {
        FetchMessagesSection {
            header: FetchMessagesSectionHeader {
                partition_id,
                status: STATUS_OK,
                current_offset: 99,
                messages_count: 2,
                context: PartitionContext {
                    incarnation: 3,
                    owner_generation: 4,
                    metadata_op: 5,
                },
                next_offset: 42,
                records_len: u32::try_from(records.len()).unwrap(),
            },
            records,
        }
    }

    fn failed(partition_id: u32, status: u32) -> FetchMessagesSection<'static> {
        FetchMessagesSection {
            header: FetchMessagesSectionHeader {
                status,
                messages_count: 0,
                records_len: 0,
                ..served(partition_id, &[]).header
            },
            records: &[],
        }
    }

    fn response(sections: Vec<FetchMessagesSection<'_>>) -> FetchMessagesResponse<'_> {
        FetchMessagesResponse { sections }
    }

    #[test]
    fn roundtrip_and_reject_truncation() {
        let records = [1u8, 2, 3, 4, 5, 6, 7];
        for sections in [
            Vec::new(),
            vec![failed(1, NOT_PRIMARY)],
            vec![
                served(4, &records[..3]),
                failed(5, 5009),
                served(6, &[]),
                served(7, &records[3..]),
            ],
        ] {
            let response = response(sections);
            let bytes = response.to_bytes().unwrap();
            let records_len: usize = response
                .sections
                .iter()
                .map(|section| section.records.len())
                .sum();
            assert_eq!(
                bytes.len(),
                FetchMessagesResponse::head_size(response.sections.len()) + records_len
            );
            assert_eq!(FetchMessagesResponse::decode(&bytes).unwrap(), response);
            for end in 0..bytes.len() {
                assert!(
                    FetchMessagesResponse::decode(&bytes[..end]).is_err(),
                    "accepted truncation at {end}"
                );
            }
        }
    }

    #[test]
    fn walk_reads_each_section_in_place() {
        let records = [1u8, 2, 3, 4, 5];
        let response = response(vec![
            served(4, &records[..2]),
            failed(5, NOT_PRIMARY),
            served(6, &records[2..]),
        ]);
        let bytes = response.to_bytes().unwrap();
        let mut walk = FetchMessagesSectionIterator::new(&bytes).unwrap();
        assert_eq!(walk.size_hint(), (3, Some(3)));

        let first = walk.next().unwrap();
        assert_eq!(first, response.sections[0]);
        assert!(std::ptr::eq(
            first.records.as_ptr(),
            bytes[FetchMessagesResponse::head_size(3)..].as_ptr()
        ));
        assert_eq!(walk.size_hint(), (2, Some(2)));
        assert_eq!(walk.by_ref().collect::<Vec<_>>(), response.sections[1..]);
        assert_eq!(walk.next(), None);
        assert_eq!(walk.size_hint(), (0, Some(0)));
    }

    #[test]
    fn section_header_layout() {
        let header = served(0x0102_0304, &[0; 9]).header;
        let bytes = header.to_le_bytes();
        assert_eq!(bytes.len(), 56, "the table entry is part of the protocol");
        assert_eq!(&bytes[..4], &0x0102_0304u32.to_le_bytes());
        assert_eq!(&bytes[4..8], &STATUS_OK.to_le_bytes());
        assert_eq!(&bytes[8..16], &99u64.to_le_bytes());
        assert_eq!(&bytes[16..20], &2u32.to_le_bytes());
        assert_eq!(&bytes[20..44], &header.context.to_le_bytes());
        assert_eq!(&bytes[44..52], &42u64.to_le_bytes());
        assert_eq!(&bytes[52..], &9u32.to_le_bytes());
        assert_eq!(
            FetchMessagesSectionHeader::decode(&bytes).unwrap(),
            (header, bytes.len())
        );
    }

    #[test]
    fn records_must_fill_the_body_exactly() {
        let records = [7u8; 6];
        let bytes = response(vec![served(1, &records[..2]), served(2, &records[2..])])
            .to_bytes()
            .unwrap();
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(FetchMessagesResponse::decode(&trailing).is_err());

        let last_records_len = FetchMessagesResponse::head_size(2) - size_of::<u32>();
        for records_len in [5u32, u32::MAX] {
            let mut past_the_body = bytes.to_vec();
            past_the_body[last_records_len..last_records_len + size_of::<u32>()]
                .copy_from_slice(&records_len.to_le_bytes());
            assert!(
                FetchMessagesResponse::decode(&past_the_body).is_err(),
                "accepted records_len {records_len}"
            );
        }
    }

    #[test]
    fn rejects_more_sections_than_a_request_has_targets() {
        let mut bytes = BytesMut::new();
        bytes.put_u32_le(MAX_PARTITIONS_PER_REQUEST + 1);
        bytes.resize(
            FetchMessagesResponse::head_size(MAX_PARTITIONS_PER_REQUEST as usize + 1),
            0,
        );
        assert!(FetchMessagesResponse::decode(&bytes).is_err());

        let headers = vec![failed(1, NOT_PRIMARY).header; MAX_PARTITIONS_PER_REQUEST as usize + 1];
        let mut head = vec![0; FetchMessagesResponse::head_size(headers.len())];
        assert!(FetchMessagesResponse::write_head(&headers, &mut head).is_err());
    }

    #[test]
    fn error_sections_carry_no_records() {
        let mut header = failed(1, NOT_PRIMARY).header;
        header.records_len = 1;
        let mut head = vec![0; FetchMessagesResponse::head_size(1)];
        assert!(FetchMessagesResponse::write_head(&[header], &mut head).is_err());

        header.status = STATUS_OK;
        FetchMessagesResponse::write_head(&[header], &mut head).unwrap();
        head[FETCH_SECTIONS_COUNT_SIZE + size_of::<u32>()..][..size_of::<u32>()]
            .copy_from_slice(&NOT_PRIMARY.to_le_bytes());
        head.push(0);
        assert!(FetchMessagesResponse::decode(&head).is_err());
    }

    #[test]
    fn head_is_written_in_place_before_the_records() {
        let records = [9u8; 5];
        let response = response(vec![served(1, &records[..2]), served(2, &records[2..])]);
        let headers: Vec<_> = response
            .sections
            .iter()
            .map(|section| section.header)
            .collect();
        let mut head = vec![0; FetchMessagesResponse::head_size(headers.len())];
        FetchMessagesResponse::write_head(&headers, &mut head).unwrap();
        let bytes = response.to_bytes().unwrap();
        assert_eq!(&bytes[..head.len()], head.as_slice());
        assert_eq!(&bytes[head.len()..], &records);

        for wrong in [head.len() - 1, head.len() + 1] {
            let mut head = vec![0; wrong];
            assert!(FetchMessagesResponse::write_head(&headers, &mut head).is_err());
        }
    }

    #[test]
    fn to_bytes_rejects_a_records_len_that_differs_from_the_records() {
        let mut section = served(1, &[1, 2, 3]);
        section.header.records_len = 2;
        assert!(response(vec![section]).to_bytes().is_err());
    }

    #[test]
    fn the_largest_reply_fills_the_reply_limit() {
        let targets = 3;
        let max_bytes = 2 * MIN_DEFERRED_POLL_MAX_BYTES;
        let request = FetchMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            wait_us: 1,
            request_timeout_us: 1 + MIN_DEFERRED_POLL_REPLY_MARGIN_US,
            min_count: 1,
            count: 1,
            max_bytes: u32::try_from(max_bytes).unwrap(),
            auto_commit: false,
            targets: (1..=targets)
                .map(|partition_id| FetchMessagesTarget {
                    partition_id,
                    strategy: WirePollingStrategy::next(),
                    max_bytes: u32::try_from(max_bytes).unwrap(),
                    incarnation: 1,
                    owner_generation: 0,
                })
                .collect(),
        };
        request.validate().unwrap();
        let records = vec![0; max_bytes + 1];
        let largest = |records| {
            let mut sections = vec![served(1, records)];
            sections.extend((2..=targets).map(|partition_id| failed(partition_id, NOT_PRIMARY)));
            response(sections).to_bytes().unwrap().len()
        };
        assert_eq!(largest(&records[..max_bytes]), request.reply_limit());
        assert!(largest(&records) > request.reply_limit());
    }

    /// One stored batch of partition 1 with the message `hello` at offset 41,
    /// stamped as the server stores it.
    fn stored_record() -> Vec<u8> {
        let mut send = BytesMut::new();
        SendMessagesEncoder::encode(
            &mut send,
            &WireIdentifier::numeric(1),
            &WireIdentifier::named("orders").unwrap(),
            &WirePartitioning::PartitionId(1),
            &[RawMessage {
                id: 1,
                origin_timestamp: 1_700_000_000_000_000,
                headers: None,
                payload: b"hello",
            }],
        )
        .unwrap();
        let metadata_len = read_u32_le(&send, 0).unwrap() as usize;
        let mut record = send[size_of::<u32>() + metadata_len..].to_vec();
        let mut stored = BatchHeader::decode(&record).unwrap();
        stored.partition_id = 1;
        stored.base_offset = 41;
        stored.base_timestamp = 1_700_000_000_000_100;
        stored.batch_checksum = calculate_batch_checksum(&stored, &record[BATCH_HEADER_SIZE..]);
        stored.encode_into(&mut record);
        decode_batch_slice(&record).unwrap();
        record
    }

    #[test]
    fn golden_reply() {
        let record = stored_record();
        let response = response(vec![
            FetchMessagesSection {
                header: FetchMessagesSectionHeader {
                    partition_id: 1,
                    status: STATUS_OK,
                    current_offset: 41,
                    messages_count: 1,
                    context: PartitionContext {
                        incarnation: 3,
                        owner_generation: 9,
                        metadata_op: 12,
                    },
                    next_offset: 42,
                    records_len: u32::try_from(record.len()).unwrap(),
                },
                records: &record,
            },
            FetchMessagesSection {
                header: FetchMessagesSectionHeader {
                    partition_id: 2,
                    status: NOT_PRIMARY,
                    current_offset: 0,
                    messages_count: 0,
                    context: PartitionContext::default(),
                    next_offset: 0,
                    records_len: 0,
                },
                records: &[],
            },
        ]);
        let bytes = response.to_bytes().unwrap();
        let decoded = FetchMessagesResponse::decode(&bytes).unwrap();
        let messages = PolledBatchesIterator::new(decoded.sections[0].records)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(
            (messages[0].offset, messages[0].payload),
            (41, &b"hello"[..])
        );
        let hex = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .concat();
        let empty_context = "00".repeat(PartitionContext::ENCODED_SIZE);
        let reserved = "00".repeat(BATCH_HEADER_SIZE - BATCH_RESERVED_OFFSET);
        let expected = [
            "02000000",
            "01000000",
            "00000000",
            "2900000000000000",
            "01000000",
            "0300000000000000",
            "0900000000000000",
            "0c00000000000000",
            "2a00000000000000",
            "35010000",
            "02000000",
            "59000000",
            "0000000000000000",
            "00000000",
            &empty_context,
            "0000000000000000",
            "00000000",
            "0100000000000000",
            "2900000000000000",
            "64401e18240a0600",
            "00401e18240a0600",
            "3501000000000000",
            "bd2d787d3cdb745c",
            "01000000",
            &reserved,
            "8e5ca59edbbdbfb9",
            "01000000000000000000000000000000",
            "00000000",
            "00000000",
            "00000000",
            "05000000",
            "0000000000000000",
            "68656c6c6f",
        ]
        .concat();
        assert_eq!(hex, expected);
    }
}
