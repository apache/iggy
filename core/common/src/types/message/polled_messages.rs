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

use crate::{
    Checkpoint, IggyMessage, IggyMessageHeader, PollingKind, PollingStrategy, ReaderStamp,
    error::IggyError,
};
use bytes::Bytes;
use iggy_binary_protocol::WireDecode;
use iggy_binary_protocol::batch::{
    BATCH_HEADER_SIZE, BATCH_MESSAGE_HEADER_SIZE, BatchHeader, BatchMessageHeader,
};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::responses::messages::PollMessagesResponseHeader;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use tracing::error;

/// The wrapper on top of the collection of messages that are polled from the partition.
/// It consists of the following fields:
/// - `partition_id`: the identifier of the partition.
/// - `current_offset`: the current offset of the partition.
/// - `count`: the count of messages.
/// - `messages`: the collection of messages.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessages {
    /// Partition incarnation and owner captured by the accepted poll.
    context: PartitionContext,
    /// The identifier of the partition. An empty reply can carry
    /// [`RESYNC_REQUIRED_PARTITION_SENTINEL`](crate::RESYNC_REQUIRED_PARTITION_SENTINEL) instead
    /// of a real id when the server fenced a stale group assignment.
    pub partition_id: u32,
    /// The current offset of the partition.
    pub current_offset: u64,
    /// The count of messages.
    pub count: u32,
    /// The collection of messages.
    pub messages: Vec<IggyMessage>,
    /// The start of the request that read the batch, for the continuation of an empty batch.
    /// The HTTP JSON form has no such field.
    #[serde(skip)]
    request_start: Option<PollingStrategy>,
    /// Set by the SDK reader, and copied into each checkpoint of the batch.
    #[serde(skip)]
    stamp: Option<ReaderStamp>,
    /// The served records a binary reply carried, kept for the record of a message that does
    /// not decrypt. A batch decoded from JSON has none.
    #[serde(skip)]
    records: Option<Bytes>,
}

impl PolledMessages {
    pub fn empty() -> Self {
        Self::new(PartitionContext::default(), 0, 0, 0, Vec::new())
    }

    /// A batch that no request read, so it has no request start.
    #[doc(hidden)]
    pub fn new(
        context: PartitionContext,
        partition_id: u32,
        current_offset: u64,
        count: u32,
        messages: Vec<IggyMessage>,
    ) -> Self {
        Self {
            context,
            partition_id,
            current_offset,
            count,
            messages,
            request_start: None,
            stamp: None,
            records: None,
        }
    }

    /// [`Self::new`] with the served records the batch was decoded from.
    #[doc(hidden)]
    pub fn with_records(
        context: PartitionContext,
        partition_id: u32,
        current_offset: u64,
        count: u32,
        messages: Vec<IggyMessage>,
        records: Bytes,
    ) -> Self {
        Self {
            records: Some(records),
            ..Self::new(context, partition_id, current_offset, count, messages)
        }
    }

    #[doc(hidden)]
    pub fn context(&self) -> PartitionContext {
        self.context
    }

    /// Records the start of the request that read a batch decoded from JSON.
    #[doc(hidden)]
    pub fn set_request_start(&mut self, start: &PollingStrategy) {
        self.request_start = Some(*start);
    }

    #[doc(hidden)]
    pub fn stamp(&self) -> Option<ReaderStamp> {
        self.stamp
    }

    #[doc(hidden)]
    pub fn set_stamp(&mut self, stamp: ReaderStamp) {
        self.stamp = Some(stamp);
    }

    /// The position after the last message, to commit the whole batch. `None` for an empty
    /// batch.
    pub fn checkpoint(&self) -> Option<Checkpoint> {
        self.messages
            .last()
            .map(|message| self.checkpoint_of(message))
    }

    /// The position after `messages[index]`, to commit a processed part of the batch. `None`
    /// for an index out of range.
    pub fn checkpoint_at(&self, index: usize) -> Option<Checkpoint> {
        self.messages
            .get(index)
            .map(|message| self.checkpoint_of(message))
    }

    /// The position after the batch, for the next read of its partition.
    ///
    /// After the last message, the continuation is `Offset(last + 1)`. An empty batch keeps the
    /// start of its request, so a start can take more than one read:
    /// - `Offset(n)` stays `Offset(n)`.
    /// - `First` becomes `Offset(0)`, because the server reads both from offset 0.
    /// - `Timestamp(t)` keeps `t`.
    /// - `Last` stays `Last`.
    /// - `Next` stays `Next`, and the next read resolves it again from the stored offset.
    ///
    /// An empty batch without a recorded start, such as a batch that the caller deserialized,
    /// continues with `Next`.
    ///
    /// The continuation belongs to the partition of the batch, and a read of another partition
    /// refuses it. Serde drops that partition, so a deserialized continuation fits every
    /// partition: read it only from the partition of its batch. It carries the context of the
    /// batch, so a read after a change of the partition history returns
    /// [`IggyError::HistoryUnavailable`].
    ///
    /// An empty batch with a zero context is the reply of a partition that could not serve the
    /// read, and it names no partition history. Its continuation takes nothing from the reply:
    /// it is the start of the request as it was, or `Next` without a recorded start.
    pub fn continuation(&self) -> PollingStrategy {
        let start = match (self.messages.last(), self.request_start) {
            (Some(last), _) => PollingStrategy::offset(last.header.offset.saturating_add(1)),
            // The server refuses a read that carries the zero context with `HistoryUnavailable`.
            (None, start) if self.context == PartitionContext::default() => {
                return start.unwrap_or_else(PollingStrategy::next);
            }
            (None, Some(start)) if start.kind == PollingKind::First => PollingStrategy::offset(0),
            (None, Some(start)) => start,
            (None, None) => PollingStrategy::next(),
        };
        PollingStrategy::continuation(self.partition_id, start.kind, start.value, self.context)
    }

    fn checkpoint_of(&self, message: &IggyMessage) -> Checkpoint {
        Checkpoint::stamped(
            self.partition_id,
            message.header.offset,
            self.context,
            self.stamp,
        )
    }
}

impl PolledMessages {
    /// Decode a `PollMessages` response body: the 40-byte prefix followed by
    /// the served batch records (`[256B batch header][frames]`, deltas
    /// resolved against the stamped bases). `strategy` is the start of the
    /// request, which the [continuation](Self::continuation) of an empty batch keeps.
    ///
    /// # Errors
    /// [`IggyError::InvalidNumberEncoding`] on a short prefix;
    /// [`IggyError::InvalidMessagePayloadLength`] on a malformed record;
    /// [`IggyError::InvalidMessagesCount`] when the records hold another count than the prefix.
    pub fn from_bytes(bytes: Bytes, strategy: &PollingStrategy) -> Result<Self, IggyError> {
        Self::decode(bytes, strategy).map_err(|error| error.error)
    }

    /// [`Self::from_bytes`], keeping the records of a reply that does not decode.
    #[doc(hidden)]
    pub fn decode(bytes: Bytes, strategy: &PollingStrategy) -> Result<Self, DecodeError> {
        let (header, consumed) = PollMessagesResponseHeader::decode(&bytes)
            .map_err(|_| DecodeError::without_record(IggyError::InvalidNumberEncoding))?;
        let PollMessagesResponseHeader {
            partition_id,
            current_offset,
            messages_count: count,
            context,
        } = header;
        let records = bytes.slice(consumed..);
        let messages = messages_from_batches(records.clone(), count)?;

        Ok(Self {
            context,
            partition_id,
            current_offset,
            count,
            messages,
            request_start: Some(*strategy),
            stamp: None,
            records: Some(records),
        })
    }

    /// The record of the message at `index` as the reply carried it, for a message that does
    /// not decrypt. `None` for a batch decoded from JSON, which carries no records.
    #[doc(hidden)]
    #[must_use]
    pub fn undecoded_record(&self, index: usize) -> Option<UndecodedRecord> {
        record_at(self.records.as_ref()?, index)
    }
}

/// A record of a reply that does not decode, with what the reply carried for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndecodedRecord {
    /// The offset of the record, known when its frame decoded and a later step failed, such as
    /// a payload past the batch, a checksum or a decryption. `None` when the framing itself is
    /// malformed, so no record can be isolated.
    pub offset: Option<u64>,
    /// The record as the reply carried it: its frame header, payload and user headers, before
    /// any decryption. For malformed framing, the rest of the records from where decoding
    /// stopped. For an HTTP reply that is not JSON, the response body.
    pub bytes: Bytes,
}

/// A reply that does not decode, with its undecoded record when the reply carried one.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct DecodeError {
    pub error: IggyError,
    /// Boxed, so a result of the decode stays small.
    pub record: Option<Box<UndecodedRecord>>,
}

impl DecodeError {
    /// A failure before any record was read, or of a reply that carries none.
    #[must_use]
    pub const fn without_record(error: IggyError) -> Self {
        Self {
            error,
            record: None,
        }
    }
}

/// The record of the message at `index` of the served `records`, as the reply carried it.
#[doc(hidden)]
#[must_use]
pub fn record_at(records: &Bytes, index: usize) -> Option<UndecodedRecord> {
    let record = Records::new(records).nth(index)?.ok()?;
    Some(UndecodedRecord {
        offset: Some(record.header.offset),
        bytes: records.slice(record.frame),
    })
}

/// Walk the served batch records, resolving each frame's deltas to absolute
/// values. Payload and user-header `Bytes` are zero-copy slices of the
/// response buffer.
pub(super) fn messages_from_batches(
    buffer: Bytes,
    count: u32,
) -> Result<Vec<IggyMessage>, DecodeError> {
    let whole = |error| DecodeError {
        error,
        record: Some(Box::new(UndecodedRecord {
            offset: None,
            bytes: buffer.clone(),
        })),
    };
    if count as usize > buffer.len() / BATCH_MESSAGE_HEADER_SIZE {
        return Err(whole(IggyError::InvalidMessagesCount));
    }
    let mut messages = Vec::with_capacity(count as usize);
    for record in Records::new(&buffer) {
        let record = record?;
        if messages.len() == count as usize {
            return Err(DecodeError {
                error: IggyError::InvalidMessagesCount,
                record: Some(Box::new(UndecodedRecord {
                    offset: Some(record.header.offset),
                    bytes: buffer.slice(record.frame),
                })),
            });
        }
        let user_headers = record.user_headers.map(|range| buffer.slice(range));
        messages.push(IggyMessage {
            header: record.header,
            payload: buffer.slice(record.payload),
            user_headers,
        });
    }
    if messages.len() != count as usize {
        return Err(whole(IggyError::InvalidMessagesCount));
    }
    Ok(messages)
}

/// One record of a reply: its resolved header and where its parts lie in the buffer.
struct Record {
    header: IggyMessageHeader,
    frame: Range<usize>,
    payload: Range<usize>,
    user_headers: Option<Range<usize>>,
}

/// The records of a reply, batch by batch. Ends at the first record that does not decode,
/// with the bytes from there on.
struct Records<'a> {
    buffer: &'a Bytes,
    position: usize,
    /// The batch being walked and where it ends.
    batch: Option<(BatchHeader, usize)>,
    cursor: usize,
}

impl<'a> Records<'a> {
    const fn new(buffer: &'a Bytes) -> Self {
        Self {
            buffer,
            position: 0,
            batch: None,
            cursor: 0,
        }
    }

    fn malformed(&mut self, from: usize, offset: Option<u64>) -> DecodeError {
        self.position = self.buffer.len();
        self.batch = None;
        DecodeError {
            error: IggyError::InvalidMessagePayloadLength,
            record: Some(Box::new(UndecodedRecord {
                offset,
                bytes: self.buffer.slice(from..),
            })),
        }
    }

    fn frame(&mut self, batch: BatchHeader, batch_end: usize) -> Result<Record, DecodeError> {
        let cursor = self.cursor;
        let frame = BatchMessageHeader::decode(&self.buffer[cursor..batch_end]).map_err(
            |decode_error| {
                error!("Failed to decode polled message frame: {decode_error}");
                self.malformed(cursor, None)
            },
        )?;
        let offset = batch.base_offset + u64::from(frame.offset_delta);
        let payload_start = cursor + BATCH_MESSAGE_HEADER_SIZE;
        let payload_end = payload_start + frame.payload_length as usize;
        let user_headers_end = payload_end + frame.user_headers_length as usize;
        if user_headers_end > batch_end {
            let mut error = self.malformed(cursor, Some(offset));
            if let Some(record) = &mut error.record {
                record.bytes = self.buffer.slice(cursor..batch_end);
            }
            return Err(error);
        }
        self.cursor = user_headers_end;
        Ok(Record {
            header: IggyMessageHeader {
                checksum: frame.checksum,
                id: frame.id,
                offset,
                // Broker append time is stamped once per batch; the
                // per-message delta applies to `origin_timestamp` only.
                timestamp: batch.base_timestamp,
                origin_timestamp: batch.origin_timestamp + u64::from(frame.timestamp_delta),
                user_headers_length: frame.user_headers_length,
                payload_length: frame.payload_length,
                reserved: 0,
            },
            frame: cursor..user_headers_end,
            payload: payload_start..payload_end,
            user_headers: (frame.user_headers_length > 0).then_some(payload_end..user_headers_end),
        })
    }
}

impl Iterator for Records<'_> {
    type Item = Result<Record, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((batch, batch_end)) = self.batch {
                if self.cursor < batch_end {
                    return Some(self.frame(batch, batch_end));
                }
                self.batch = None;
                self.position = batch_end;
            }
            if self.position >= self.buffer.len() {
                return None;
            }
            let position = self.position;
            let batch = match BatchHeader::decode(&self.buffer[position..]) {
                Ok(batch) => batch,
                Err(decode_error) => {
                    error!("Failed to decode polled batch header: {decode_error}");
                    return Some(Err(self.malformed(position, None)));
                }
            };
            let Some(batch_end) = position
                .checked_add(batch.total_size())
                .filter(|end| *end <= self.buffer.len())
            else {
                return Some(Err(self.malformed(position, None)));
            };
            self.cursor = position + BATCH_HEADER_SIZE;
            self.batch = Some((batch, batch_end));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IggyTimestamp, RESYNC_REQUIRED_PARTITION_SENTINEL};
    use iggy_binary_protocol::WireEncode;

    const PARTITION: u32 = 1;
    const CURRENT_OFFSET: u64 = 5;
    const CONTEXT: PartitionContext = PartitionContext {
        incarnation: 3,
        owner_generation: 9,
        metadata_op: 12,
    };
    /// Today's JSON form of an empty batch of `PARTITION` under `CONTEXT`.
    const EMPTY_BATCH_JSON: &str = r#"{"context":{"incarnation":3,"owner_generation":9,"metadata_op":12},"partition_id":1,"current_offset":5,"count":0,"messages":[]}"#;

    fn message_at(offset: u64) -> IggyMessage {
        IggyMessage {
            header: IggyMessageHeader {
                offset,
                ..Default::default()
            },
            payload: Bytes::new(),
            user_headers: None,
        }
    }

    fn batch(offsets: &[u64]) -> PolledMessages {
        PolledMessages::new(
            CONTEXT,
            PARTITION,
            CURRENT_OFFSET,
            u32::try_from(offsets.len()).unwrap(),
            offsets.iter().copied().map(message_at).collect(),
        )
    }

    fn empty_reply(start: &PollingStrategy) -> PolledMessages {
        let header = PollMessagesResponseHeader {
            partition_id: PARTITION,
            current_offset: CURRENT_OFFSET,
            messages_count: 0,
            context: CONTEXT,
        };
        PolledMessages::from_bytes(header.to_bytes(), start).unwrap()
    }

    fn continuation(kind: PollingKind, value: u64) -> PollingStrategy {
        PollingStrategy::continuation(PARTITION, kind, value, CONTEXT)
    }

    #[test]
    fn given_an_empty_reply_when_continuing_should_keep_the_start_of_its_request() {
        const OFFSET: u64 = 7;
        const TIMESTAMP: u64 = 1_000;
        let cases = [
            (
                PollingStrategy::offset(OFFSET),
                continuation(PollingKind::Offset, OFFSET),
            ),
            (
                PollingStrategy::first(),
                continuation(PollingKind::Offset, 0),
            ),
            (
                PollingStrategy::timestamp(IggyTimestamp::from(TIMESTAMP)),
                continuation(PollingKind::Timestamp, TIMESTAMP),
            ),
            (PollingStrategy::last(), continuation(PollingKind::Last, 0)),
            (PollingStrategy::next(), continuation(PollingKind::Next, 0)),
            (
                continuation(PollingKind::Offset, OFFSET),
                continuation(PollingKind::Offset, OFFSET),
            ),
        ];

        for (start, expected) in cases {
            assert_eq!(empty_reply(&start).continuation(), expected, "{start:?}");
        }
    }

    #[test]
    fn given_a_batch_with_messages_when_continuing_should_start_after_the_last_message() {
        let mut polled = batch(&[4, 5]);
        polled.set_request_start(&PollingStrategy::last());

        let next = polled.continuation();

        assert_eq!(next, continuation(PollingKind::Offset, 6));
        assert_eq!(next.partition_id(), Some(PARTITION));
        assert_eq!(next.context(), Some(CONTEXT));
    }

    #[test]
    fn given_an_empty_batch_without_a_start_when_continuing_should_continue_with_next() {
        assert_eq!(
            batch(&[]).continuation(),
            continuation(PollingKind::Next, 0)
        );
    }

    /// The server answers a partition that cannot serve the read with an empty reply and a zero
    /// context, under the re-sync sentinel for a fenced group member.
    #[test]
    fn given_a_fail_fast_empty_reply_when_continuing_should_repeat_the_start_of_its_request() {
        const OFFSET: u64 = 7;
        let fail_fast = |partition_id, start: &PollingStrategy| {
            let header = PollMessagesResponseHeader {
                partition_id,
                current_offset: 0,
                messages_count: 0,
                context: PartitionContext::default(),
            };
            PolledMessages::from_bytes(header.to_bytes(), start).unwrap()
        };

        for start in [
            PollingStrategy::offset(OFFSET),
            PollingStrategy::first(),
            PollingStrategy::last(),
            PollingStrategy::next(),
            continuation(PollingKind::Offset, OFFSET),
        ] {
            for partition_id in [PARTITION, RESYNC_REQUIRED_PARTITION_SENTINEL] {
                assert_eq!(
                    fail_fast(partition_id, &start).continuation(),
                    start,
                    "{start:?} from partition {partition_id}"
                );
            }
        }
        assert_eq!(
            PolledMessages::empty().continuation(),
            PollingStrategy::next(),
            "a batch without a start"
        );
    }

    #[test]
    fn given_a_batch_when_taking_checkpoints_should_follow_the_delivered_messages() {
        let polled = batch(&[4, 5]);
        let after = |offset| Some(Checkpoint::new(PARTITION, offset, CONTEXT));

        assert_eq!(polled.checkpoint(), after(5));
        assert_eq!(polled.checkpoint_at(0), after(4));
        assert_eq!(polled.checkpoint_at(1), after(5));
        assert_eq!(polled.checkpoint_at(2), None);
        let empty = batch(&[]);
        assert_eq!(empty.checkpoint(), None);
        assert_eq!(empty.checkpoint_at(0), None);
    }

    #[test]
    fn given_a_batch_when_serialized_should_keep_the_json_form_without_its_start() {
        let decoded = empty_reply(&PollingStrategy::offset(CURRENT_OFFSET));
        assert_eq!(serde_json::to_string(&decoded).unwrap(), EMPTY_BATCH_JSON);

        let deserialized: PolledMessages = serde_json::from_str(EMPTY_BATCH_JSON).unwrap();

        assert_eq!(
            (
                deserialized.context(),
                deserialized.partition_id,
                deserialized.current_offset,
                deserialized.count,
                deserialized.messages.len()
            ),
            (CONTEXT, PARTITION, CURRENT_OFFSET, 0, 0)
        );
        assert_eq!(
            deserialized.continuation(),
            continuation(PollingKind::Next, 0),
            "a deserialized batch has no start"
        );
        assert_eq!(
            serde_json::to_string(&deserialized).unwrap(),
            EMPTY_BATCH_JSON
        );
    }

    #[test]
    fn rejects_impossible_count_before_allocating_messages() {
        let prefix = PollMessagesResponseHeader {
            partition_id: 0,
            current_offset: 0,
            messages_count: u32::MAX,
            context: PartitionContext::default(),
        }
        .to_bytes();
        assert!(matches!(
            PolledMessages::from_bytes(prefix, &PollingStrategy::next()),
            Err(IggyError::InvalidMessagesCount)
        ));
    }
}
