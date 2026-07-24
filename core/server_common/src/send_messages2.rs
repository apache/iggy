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

use crate::consensus_message::{MESSAGE_ALIGN, Message};
use crate::iobuf::Owned;
use crate::sharding::IggyNamespace;
use bytes::{Bytes, BytesMut};
use iggy_binary_protocol::{PrepareHeader, RequestHeader};
use iggy_common::{EncryptorKind, INDEX_SIZE, IggyError, random_id};
use std::hash::Hasher;
use twox_hash::XxHash3_64;

pub const COMMAND_HEADER_SIZE: usize = 256;
pub const PREPARE_SPLIT_POINT: usize = 512;
const MESSAGE_HEADER_SIZE: usize = 48;
const LEGACY_MESSAGE_HEADER_SIZE: usize = 64;
const BATCH_CHECKSUM_OFFSET: usize = 40;
const MESSAGE_COUNT_OFFSET: usize = 48;
const MAX_TIMESTAMP_DELTA_MICROS: u64 = u32::MAX as u64;

#[derive(Debug, Clone, Copy, Default)]
pub struct SendMessages2Header {
    pub partition_id: u64,
    pub base_offset: u64,
    pub base_timestamp: u64,
    pub origin_timestamp: u64,
    pub batch_length: u64,
    pub batch_checksum: u64,
    pub message_count: u32,
}

impl SendMessages2Header {
    pub const fn new(
        partition_id: u64,
        origin_timestamp: u64,
        batch_length: u64,
        message_count: u32,
    ) -> Self {
        Self {
            partition_id,
            base_offset: 0,
            base_timestamp: 0,
            origin_timestamp,
            batch_length,
            batch_checksum: 0,
            message_count,
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IggyError> {
        if bytes.len() < COMMAND_HEADER_SIZE {
            return Err(IggyError::InvalidCommand);
        }

        let batch_length = read_u64(bytes, 32)?;
        if batch_length < COMMAND_HEADER_SIZE as u64 {
            return Err(IggyError::InvalidCommand);
        }

        Ok(Self {
            partition_id: read_u64(bytes, 0)?,
            base_offset: read_u64(bytes, 8)?,
            base_timestamp: read_u64(bytes, 16)?,
            origin_timestamp: read_u64(bytes, 24)?,
            batch_length,
            batch_checksum: read_u64(bytes, BATCH_CHECKSUM_OFFSET)?,
            message_count: read_u32(bytes, MESSAGE_COUNT_OFFSET)?,
        })
    }

    pub fn encode_into(&self, bytes: &mut [u8]) {
        assert!(bytes.len() >= COMMAND_HEADER_SIZE);
        bytes[..COMMAND_HEADER_SIZE].fill(0);
        bytes[0..8].copy_from_slice(&self.partition_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.base_offset.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.base_timestamp.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.origin_timestamp.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.batch_length.to_le_bytes());
        bytes[BATCH_CHECKSUM_OFFSET..BATCH_CHECKSUM_OFFSET + 8]
            .copy_from_slice(&self.batch_checksum.to_le_bytes());
        bytes[MESSAGE_COUNT_OFFSET..MESSAGE_COUNT_OFFSET + 4]
            .copy_from_slice(&self.message_count.to_le_bytes());
    }

    pub fn total_size(&self) -> usize {
        usize::try_from(self.batch_length).expect("batch length exceeds usize::MAX")
    }

    pub fn blob_len(&self) -> Result<usize, IggyError> {
        usize::try_from(
            self.batch_length
                .checked_sub(COMMAND_HEADER_SIZE as u64)
                .ok_or(IggyError::InvalidCommand)?,
        )
        .map_err(|_| IggyError::InvalidCommand)
    }

    pub fn into_frozen(self) -> FrozenBatchHeader {
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(COMMAND_HEADER_SIZE);
        self.encode_into(buffer.as_mut_slice());
        buffer.into()
    }

    #[must_use]
    pub fn checksum_for_blob(&self, blob: &[u8]) -> u64 {
        calculate_batch_checksum(self, blob)
    }
}

#[derive(Debug, Clone)]
pub struct SendMessages2Owned {
    pub header: SendMessages2Header,
    pub blob: Bytes,
}

impl SendMessages2Owned {
    pub fn from_messages(
        namespace: IggyNamespace,
        messages: &IggyMessages2,
    ) -> Result<Self, IggyError> {
        let message_count = messages.count();
        let mut origin_timestamp = u64::MAX;
        for message in messages {
            origin_timestamp = origin_timestamp.min(message.header.origin_timestamp);
        }

        if origin_timestamp == u64::MAX {
            origin_timestamp = 0;
        }

        let mut blob = BytesMut::new();
        for (index, message) in messages.iter().enumerate() {
            let id = if message.header.id == 0 {
                random_id::get_uuid()
            } else {
                message.header.id
            };
            let offset_delta = u32::try_from(index).map_err(|_| IggyError::InvalidCommand)?;
            let timestamp_delta = message
                .header
                .origin_timestamp
                .checked_sub(origin_timestamp)
                .ok_or(IggyError::InvalidCommand)?;
            if timestamp_delta > MAX_TIMESTAMP_DELTA_MICROS {
                return Err(IggyError::InvalidMessageTimestampDelta(timestamp_delta));
            }
            let timestamp_delta =
                u32::try_from(timestamp_delta).map_err(|_| IggyError::InvalidCommand)?;
            let user_headers = message.user_headers.as_deref().unwrap_or_default();
            let user_headers_length =
                u32::try_from(user_headers.len()).map_err(|_| IggyError::InvalidCommand)?;
            let payload_length =
                u32::try_from(message.payload.len()).map_err(|_| IggyError::InvalidCommand)?;

            let mut header = [0u8; MESSAGE_HEADER_SIZE];
            header[8..24].copy_from_slice(&id.to_le_bytes());
            header[24..28].copy_from_slice(&offset_delta.to_le_bytes());
            header[28..32].copy_from_slice(&timestamp_delta.to_le_bytes());
            header[32..36].copy_from_slice(&user_headers_length.to_le_bytes());
            header[36..40].copy_from_slice(&payload_length.to_le_bytes());

            let msg_start = blob.len();
            blob.extend_from_slice(&header);
            blob.extend_from_slice(&message.payload);
            blob.extend_from_slice(user_headers);
            let checksum = XxHash3_64::oneshot(&blob[msg_start + 8..]);
            blob[msg_start..msg_start + 8].copy_from_slice(&checksum.to_le_bytes());
        }

        let blob = blob.freeze();
        let mut header = SendMessages2Header::new(
            namespace.partition_id() as u64,
            origin_timestamp,
            u64::try_from(COMMAND_HEADER_SIZE + blob.len())
                .map_err(|_| IggyError::InvalidCommand)?,
            message_count,
        );
        header.batch_checksum = calculate_batch_checksum(&header, &blob);

        Ok(Self { header, blob })
    }

    pub fn encode_request(
        self,
        mut request_header: RequestHeader,
    ) -> Result<Message<RequestHeader>, IggyError> {
        let total_size = std::mem::size_of::<RequestHeader>() + self.header.total_size();
        // The converted body differs in size from the legacy wire body the
        // header described; a stale `size` truncates the rebuilt blob for
        // every downstream slice (stamping, journal reads).
        request_header.size = u32::try_from(total_size).map_err(|_| IggyError::InvalidCommand)?;
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total_size);
        let bytes = buffer.as_mut_slice();
        bytes[0..std::mem::size_of::<RequestHeader>()]
            .copy_from_slice(bytemuck::bytes_of(&request_header));
        self.header.encode_into(
            &mut bytes[std::mem::size_of::<RequestHeader>()
                ..std::mem::size_of::<RequestHeader>() + COMMAND_HEADER_SIZE],
        );
        bytes[PREPARE_SPLIT_POINT..PREPARE_SPLIT_POINT + self.blob.len()]
            .copy_from_slice(&self.blob);

        Message::try_from(buffer).map_err(|_| IggyError::InvalidCommand)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IggyMessage2Header {
    pub checksum: u64,
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub user_headers_length: u32,
    pub payload_length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IggyMessage2 {
    pub header: IggyMessage2Header,
    pub payload: Bytes,
    pub user_headers: Option<Bytes>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IggyMessages2 {
    messages: Vec<IggyMessage2>,
}

impl IggyMessages2 {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            messages: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, message: IggyMessage2) {
        self.messages.push(message);
    }

    #[must_use]
    pub fn count(&self) -> u32 {
        u32::try_from(self.messages.len()).unwrap_or(u32::MAX)
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    #[must_use]
    pub fn first_offset(&self) -> Option<u64> {
        self.messages.first().map(|message| message.header.offset)
    }

    #[must_use]
    pub fn last_offset(&self) -> Option<u64> {
        self.messages.last().map(|message| message.header.offset)
    }

    #[must_use]
    pub fn limit(self, count: u32) -> Self {
        let mut messages = self.messages;
        messages.truncate(usize::try_from(count).unwrap_or(usize::MAX));
        Self { messages }
    }

    pub fn iter(&self) -> std::slice::Iter<'_, IggyMessage2> {
        self.messages.iter()
    }
}

impl IntoIterator for IggyMessages2 {
    type Item = IggyMessage2;
    type IntoIter = std::vec::IntoIter<IggyMessage2>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.into_iter()
    }
}

impl<'a> IntoIterator for &'a IggyMessages2 {
    type Item = &'a IggyMessage2;
    type IntoIter = std::slice::Iter<'a, IggyMessage2>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.iter()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SendMessages2Ref<'a> {
    pub header: SendMessages2Header,
    blob: &'a [u8],
}

#[allow(dead_code)]
impl<'a> SendMessages2Ref<'a> {
    pub const fn iter(&self) -> SendMessages2Iterator<'a> {
        SendMessages2Iterator {
            blob: self.blob,
            position: 0,
        }
    }

    pub const fn iter_with_offsets(&self) -> SendMessages2IteratorWithOffsets<'a> {
        SendMessages2IteratorWithOffsets {
            blob: self.blob,
            position: 0,
        }
    }

    pub const fn blob(&self) -> &'a [u8] {
        self.blob
    }

    pub const fn message_count(&self) -> u32 {
        self.header.message_count
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct SendMessages2MessageHeader {
    pub checksum: u64,
    pub id: u128,
    pub offset_delta: u32,
    /// Microsecond delta from `SendMessages2Header::origin_timestamp`.
    ///
    /// This is stored in `u32`, which limits a single batch to roughly
    /// 71.6 minutes of origin timestamp span.
    pub timestamp_delta: u32,
    pub user_headers_length: u32,
    pub payload_length: u32,
}

impl SendMessages2MessageHeader {
    fn decode(bytes: &[u8]) -> Result<Self, IggyError> {
        if bytes.len() < MESSAGE_HEADER_SIZE {
            return Err(IggyError::InvalidCommand);
        }

        let reserved = read_u64(bytes, 40)?;
        if reserved != 0 {
            return Err(IggyError::InvalidCommand);
        }

        Ok(Self {
            checksum: read_u64(bytes, 0)?,
            id: read_u128(bytes, 8)?,
            offset_delta: read_u32(bytes, 24)?,
            timestamp_delta: read_u32(bytes, 28)?,
            user_headers_length: read_u32(bytes, 32)?,
            payload_length: read_u32(bytes, 36)?,
        })
    }

    const fn total_size(&self) -> usize {
        MESSAGE_HEADER_SIZE + self.user_headers_length as usize + self.payload_length as usize
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct SendMessages2MessageView<'a> {
    pub header: SendMessages2MessageHeader,
    pub user_headers: &'a [u8],
    pub payload: &'a [u8],
}

#[allow(dead_code)]
pub struct SendMessages2Iterator<'a> {
    blob: &'a [u8],
    position: usize,
}

impl<'a> Iterator for SendMessages2Iterator<'a> {
    type Item = SendMessages2MessageView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.blob.len() {
            return None;
        }

        let header = SendMessages2MessageHeader::decode(&self.blob[self.position..]).ok()?;
        let start = self.position + MESSAGE_HEADER_SIZE;
        let payload_end = start + header.payload_length as usize;
        let headers_end = payload_end + header.user_headers_length as usize;
        let payload = self.blob.get(start..payload_end)?;
        let user_headers = self.blob.get(payload_end..headers_end)?;
        self.position += header.total_size();
        Some(SendMessages2MessageView {
            header,
            user_headers,
            payload,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SendMessages2MessageViewWithOffsets<'a> {
    pub message: SendMessages2MessageView<'a>,
    pub start: usize,
    pub end: usize,
}

pub struct SendMessages2IteratorWithOffsets<'a> {
    blob: &'a [u8],
    position: usize,
}

impl<'a> Iterator for SendMessages2IteratorWithOffsets<'a> {
    type Item = SendMessages2MessageViewWithOffsets<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.blob.len() {
            return None;
        }

        let start = self.position;
        let header = SendMessages2MessageHeader::decode(&self.blob[self.position..]).ok()?;
        let message_start = self.position + MESSAGE_HEADER_SIZE;
        let payload_end = message_start + header.payload_length as usize;
        let headers_end = payload_end + header.user_headers_length as usize;
        let payload = self.blob.get(message_start..payload_end)?;
        let user_headers = self.blob.get(payload_end..headers_end)?;
        self.position += header.total_size();
        Some(SendMessages2MessageViewWithOffsets {
            message: SendMessages2MessageView {
                header,
                user_headers,
                payload,
            },
            start,
            end: self.position,
        })
    }
}

pub(crate) type FrozenBatchHeader = crate::iobuf::Frozen<MESSAGE_ALIGN>;

/// Re-encode a canonical `SendMessages2` request with every message's payload
/// and user headers encrypted, per-message checksums and lengths recomputed,
/// and the batch header (length + checksum) restamped.
///
/// Runs ONCE, on the primary at ingestion (after [`convert_request_message`]
/// canonicalized the wire form), so the ciphertext is what replicates: every
/// replica journals and persists identical bytes, and the poll path decrypts
/// uniformly regardless of which replica or tier served the fragment.
///
/// # Errors
///
/// [`IggyError::InvalidCommand`] on an undecodable batch; encryption errors
/// propagate from the encryptor.
pub fn encrypt_batch_request(
    message: Message<RequestHeader>,
    encryptor: &EncryptorKind,
) -> Result<Message<RequestHeader>, IggyError> {
    let request_header = *message.header();
    let total_size = request_header.size as usize;
    let body = &message.as_slice()[std::mem::size_of::<RequestHeader>()..total_size];
    let batch = decode_batch_slice(body)?;

    let mut blob = BytesMut::with_capacity(batch.blob().len() * 2);
    for view in batch.iter() {
        let encrypted_payload = encryptor.encrypt(view.payload)?;
        let encrypted_user_headers = if view.user_headers.is_empty() {
            None
        } else {
            Some(encryptor.encrypt(view.user_headers)?)
        };
        let user_headers: &[u8] = encrypted_user_headers.as_deref().unwrap_or_default();
        let payload_length =
            u32::try_from(encrypted_payload.len()).map_err(|_| IggyError::InvalidCommand)?;
        let user_headers_length =
            u32::try_from(user_headers.len()).map_err(|_| IggyError::InvalidCommand)?;

        let mut header = [0u8; MESSAGE_HEADER_SIZE];
        header[8..24].copy_from_slice(&view.header.id.to_le_bytes());
        header[24..28].copy_from_slice(&view.header.offset_delta.to_le_bytes());
        header[28..32].copy_from_slice(&view.header.timestamp_delta.to_le_bytes());
        header[32..36].copy_from_slice(&user_headers_length.to_le_bytes());
        header[36..40].copy_from_slice(&payload_length.to_le_bytes());
        let msg_start = blob.len();
        blob.extend_from_slice(&header);
        blob.extend_from_slice(&encrypted_payload);
        blob.extend_from_slice(user_headers);
        let checksum = XxHash3_64::oneshot(&blob[msg_start + 8..]);
        blob[msg_start..msg_start + 8].copy_from_slice(&checksum.to_le_bytes());
    }

    let blob = blob.freeze();
    let mut header = batch.header;
    header.batch_length =
        u64::try_from(COMMAND_HEADER_SIZE + blob.len()).map_err(|_| IggyError::InvalidCommand)?;
    header.batch_checksum = calculate_batch_checksum(&header, &blob);

    SendMessages2Owned { header, blob }.encode_request(request_header)
}

/// Whether the legacy transcode stamps a batch checksum onto its output.
///
/// The recompute is a full-blob `XxHash3` pass, needed only when a reader
/// validates the transcoded batch before [`stamp_prepare_for_persistence`]
/// recomputes it: the encrypt ingest path re-decodes the canonicalized batch
/// (`encrypt_batch_request`'s validating decode, then the second `convert` its
/// output re-enters as the canonical-vs-legacy discriminator). The partition
/// ingest path has no such reader, so it skips the pass and the checksum stays
/// zero until stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumMode {
    /// Compute the batch checksum over the transcoded blob.
    Compute,
    /// Leave the batch checksum zero; `stamp_prepare_for_persistence` fills it.
    Skip,
}

pub fn convert_request_message(
    namespace: IggyNamespace,
    message: Message<RequestHeader>,
    checksum: ChecksumMode,
) -> Result<Message<RequestHeader>, IggyError> {
    let request_header = *message.header();
    let total_size = request_header.size as usize;
    let body = &message.as_slice()[std::mem::size_of::<RequestHeader>()..total_size];
    if decode_batch_slice(body).is_ok() {
        return Ok(message);
    }
    transcode_legacy_request(namespace, body, request_header, checksum)
}

/// Transcode a legacy `SendMessages` request body directly into the canonical
/// `[RequestHeader][256B SendMessages2Header][blob]` form, writing each message
/// record straight into the final aligned buffer.
///
/// Fused replacement for the `from_legacy_request(..).encode_request(..)`
/// two-step: a size walk over the legacy input sizes the single output
/// allocation, then a write walk lays down each canonical record in place. This
/// drops the intermediate blob allocation and the full-blob copy the two-step
/// paid. Output bytes are identical to that path.
///
/// `checksum` selects whether the output carries a batch checksum (see
/// [`ChecksumMode`]); [`ChecksumMode::Skip`] leaves it zero for the partition
/// ingest path, where stamp recomputes it.
fn transcode_legacy_request(
    namespace: IggyNamespace,
    body: &[u8],
    mut request_header: RequestHeader,
    checksum: ChecksumMode,
) -> Result<Message<RequestHeader>, IggyError> {
    let (message_count, messages) = legacy_messages_slice(body)?;
    let mut parsed = Vec::with_capacity(message_count as usize);
    let mut origin_timestamp = u64::MAX;
    let mut cursor = 0usize;
    let mut blob_len = 0usize;

    while cursor < messages.len() && parsed.len() < message_count as usize {
        let legacy = LegacyMessageRef::decode(&messages[cursor..])?;
        origin_timestamp = origin_timestamp.min(legacy.origin_timestamp);
        cursor += legacy.total_size;
        blob_len = blob_len
            .checked_add(MESSAGE_HEADER_SIZE + legacy.payload.len() + legacy.user_headers.len())
            .ok_or(IggyError::InvalidCommand)?;
        parsed.push(legacy);
    }

    if parsed.len() != message_count as usize || cursor != messages.len() {
        return Err(IggyError::InvalidCommand);
    }

    if origin_timestamp == u64::MAX {
        origin_timestamp = 0;
    }

    let header_size = std::mem::size_of::<RequestHeader>();
    let batch_length = COMMAND_HEADER_SIZE
        .checked_add(blob_len)
        .ok_or(IggyError::InvalidCommand)?;
    let total_size = header_size
        .checked_add(batch_length)
        .ok_or(IggyError::InvalidCommand)?;
    request_header.size = u32::try_from(total_size).map_err(|_| IggyError::InvalidCommand)?;

    let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total_size);
    let bytes = buffer.as_mut_slice();
    bytes[0..header_size].copy_from_slice(bytemuck::bytes_of(&request_header));

    let mut write = PREPARE_SPLIT_POINT;
    for (index, legacy) in parsed.iter().enumerate() {
        let id = if legacy.id == 0 {
            random_id::get_uuid()
        } else {
            legacy.id
        };
        let offset_delta = u32::try_from(index).map_err(|_| IggyError::InvalidCommand)?;
        let timestamp_delta = legacy
            .origin_timestamp
            .checked_sub(origin_timestamp)
            .ok_or(IggyError::InvalidCommand)?;
        if timestamp_delta > MAX_TIMESTAMP_DELTA_MICROS {
            return Err(IggyError::InvalidMessageTimestampDelta(timestamp_delta));
        }
        let timestamp_delta =
            u32::try_from(timestamp_delta).map_err(|_| IggyError::InvalidCommand)?;
        let user_headers_length =
            u32::try_from(legacy.user_headers.len()).map_err(|_| IggyError::InvalidCommand)?;
        let payload_length =
            u32::try_from(legacy.payload.len()).map_err(|_| IggyError::InvalidCommand)?;

        let mut header = [0u8; MESSAGE_HEADER_SIZE];
        header[8..24].copy_from_slice(&id.to_le_bytes());
        header[24..28].copy_from_slice(&offset_delta.to_le_bytes());
        header[28..32].copy_from_slice(&timestamp_delta.to_le_bytes());
        header[32..36].copy_from_slice(&user_headers_length.to_le_bytes());
        header[36..40].copy_from_slice(&payload_length.to_le_bytes());
        let msg_start = write;
        bytes[write..write + MESSAGE_HEADER_SIZE].copy_from_slice(&header);
        write += MESSAGE_HEADER_SIZE;
        bytes[write..write + legacy.payload.len()].copy_from_slice(legacy.payload);
        write += legacy.payload.len();
        bytes[write..write + legacy.user_headers.len()].copy_from_slice(legacy.user_headers);
        write += legacy.user_headers.len();
        // The cover is [msg_start + 8 .. write], including the 8 reserved zero
        // header bytes. This relies on the stack header being zero-initialized,
        // not on the output buffer being pre-zeroed.
        let checksum = XxHash3_64::oneshot(&bytes[msg_start + 8..write]);
        bytes[msg_start..msg_start + 8].copy_from_slice(&checksum.to_le_bytes());
    }

    let mut command = SendMessages2Header::new(
        namespace.partition_id() as u64,
        origin_timestamp,
        batch_length as u64,
        message_count,
    );
    if checksum == ChecksumMode::Compute {
        command.batch_checksum = calculate_batch_checksum(
            &command,
            &bytes[PREPARE_SPLIT_POINT..PREPARE_SPLIT_POINT + blob_len],
        );
    }
    command.encode_into(&mut bytes[header_size..header_size + COMMAND_HEADER_SIZE]);

    Message::try_from(buffer).map_err(|_| IggyError::InvalidCommand)
}

/// Decode one batch slice (`[256B command header][blob]`), validating the
/// batch checksum. The persisted segment-file record and the request slice
/// share this layout, so both decode through here.
pub fn decode_batch_slice(body: &[u8]) -> Result<SendMessages2Ref<'_>, IggyError> {
    if body.len() < COMMAND_HEADER_SIZE {
        return Err(IggyError::InvalidCommand);
    }

    let header = SendMessages2Header::decode(&body[..COMMAND_HEADER_SIZE])?;
    let blob_len = header.blob_len()?;
    if body.len() < header.total_size() {
        return Err(IggyError::InvalidCommand);
    }

    let blob = &body[COMMAND_HEADER_SIZE..COMMAND_HEADER_SIZE + blob_len];
    let expected_checksum = calculate_batch_checksum(&header, blob);
    if header.batch_checksum != expected_checksum {
        return Err(IggyError::InvalidBatchChecksum(
            header.batch_checksum,
            expected_checksum,
            header.base_offset,
        ));
    }

    Ok(SendMessages2Ref { header, blob })
}

/// Decode a `Prepare` message from a slice of bytes, validating the batch
/// checksum.
///
/// `bytes` must be 16-byte aligned (`PrepareHeader` has `u128` fields). Source
/// from `Frozen<MESSAGE_ALIGN>` / `Owned<MESSAGE_ALIGN>` / `Message<H>`.
/// Misalignment: `debug_assert!` in debug; `InvalidCommand` in release.
///
/// # Errors
///
/// `IggyError::InvalidCommand` on: short buffer, bad bit pattern, `size`
/// outside `[header_size, bytes.len()]`, short/checksum-mismatched body.
pub fn decode_prepare_slice(bytes: &[u8]) -> Result<SendMessages2Ref<'_>, IggyError> {
    decode_prepare_slice_inner(bytes, true)
}

/// Like [`decode_prepare_slice`] but skips the full-blob batch-checksum
/// recomputation, extracting only the header meta. Every cheap structural check
/// (length, 16-byte alignment, `size` bounds, blob length) is still enforced.
///
/// INVARIANT: `bytes` MUST be node-local self-stamped -
/// [`stamp_prepare_for_persistence`] recomputed the batch checksum over the
/// exact blob on THIS node - or already integrity-checked at their network
/// ingress. There is no consensus-layer blob validation: the `PrepareHeader`
/// integrity fields are inert zeros. A replicated `SendMessages` prepare is
/// gated per-message on receipt by [`verify_received_send_messages`], and a
/// repaired prepare is validated via [`decode_prepare_slice`]; both run BEFORE
/// the bytes reach any trusted decode. NEVER call this on unvalidated network
/// bytes - it would let a corrupted blob pass undetected. The `XxHash3` pass
/// over a ~1 MiB blob dominates produce-path CPU, so trusted call sites that
/// only read header meta skip it.
///
/// # Errors
///
/// Same structural errors as [`decode_prepare_slice`], minus
/// `InvalidBatchChecksum`.
pub fn decode_prepare_slice_trusted(bytes: &[u8]) -> Result<SendMessages2Ref<'_>, IggyError> {
    decode_prepare_slice_inner(bytes, false)
}

fn decode_prepare_slice_inner(
    bytes: &[u8],
    validate_checksum: bool,
) -> Result<SendMessages2Ref<'_>, IggyError> {
    let header_size = std::mem::size_of::<PrepareHeader>();
    if bytes.len() < header_size {
        return Err(IggyError::InvalidCommand);
    }

    // Bytemuck enforces alignment in release (maps to InvalidCommand below);
    // debug_assert surfaces the contract violation early in dev.
    debug_assert_eq!(
        bytes
            .as_ptr()
            .align_offset(std::mem::align_of::<PrepareHeader>()),
        0,
        "decode_prepare_slice: bytes must be at least 16-byte aligned",
    );

    let prepare = bytemuck::checked::try_from_bytes::<PrepareHeader>(&bytes[..header_size])
        .map_err(|_| IggyError::InvalidCommand)?;
    let total_size = prepare.size as usize;
    // Wire-controllable `size`: reject < header_size to avoid slice OOB below.
    if total_size < header_size || bytes.len() < total_size {
        return Err(IggyError::InvalidCommand);
    }

    let body = &bytes[header_size..total_size];
    if body.len() < COMMAND_HEADER_SIZE {
        return Err(IggyError::InvalidCommand);
    }

    let header = SendMessages2Header::decode(&body[..COMMAND_HEADER_SIZE])?;
    let blob = &body[COMMAND_HEADER_SIZE..];
    let blob_len = header.blob_len()?;
    if body.len() < header.total_size() {
        return Err(IggyError::InvalidCommand);
    }

    let blob = &blob[..blob_len];
    if validate_checksum {
        let expected_checksum = calculate_batch_checksum(&header, blob);
        if header.batch_checksum != expected_checksum {
            return Err(IggyError::InvalidBatchChecksum(
                header.batch_checksum,
                expected_checksum,
                header.base_offset,
            ));
        }
    }

    Ok(SendMessages2Ref { header, blob })
}

pub fn stamp_prepare_for_persistence(
    mut message: Message<PrepareHeader>,
    base_offset: u64,
    base_timestamp: u64,
) -> Result<(Message<PrepareHeader>, SendMessages2Header, u32), IggyError> {
    let total_size = message.header().size as usize;
    let bytes = message.as_mut_slice();
    if bytes.len() < PREPARE_SPLIT_POINT || total_size < PREPARE_SPLIT_POINT {
        return Err(IggyError::InvalidCommand);
    }

    let header_offset = std::mem::size_of::<PrepareHeader>();
    let mut command =
        SendMessages2Header::decode(&bytes[header_offset..header_offset + COMMAND_HEADER_SIZE])?;
    command.base_offset = base_offset;
    command.base_timestamp = base_timestamp;
    let blob = &bytes[PREPARE_SPLIT_POINT..total_size];
    command.batch_checksum = calculate_batch_checksum(&command, blob);
    command.encode_into(&mut bytes[header_offset..header_offset + COMMAND_HEADER_SIZE]);
    Ok((message, command, command.message_count))
}

/// Verify every per-message checksum in a received `SendMessages` prepare.
///
/// The FIRST blob-integrity check on the replicated path: the `PrepareHeader`
/// integrity fields are inert zeros and the batch checksum is recomputed
/// locally at stamp, so transit corruption of a message body would otherwise
/// reach apply undetected. Backups call this before journaling a replicated
/// prepare; on a mismatch the caller fails closed (drop, no `PrepareOk`) and the
/// primary retransmits on prepare-timeout.
///
/// The decode is structural only (no batch-checksum recompute). Each message's
/// stored checksum is recomputed over its stamp-invariant cover
/// (`header[8..48] || payload || user_headers`), which excludes the 256B command
/// header, so it holds whether or not this node has stamped `base_offset` /
/// `base_timestamp` yet (a received prepare is pre-stamp).
///
/// # Errors
///
/// [`IggyError::InvalidCommand`] if the records do not tile `message_count`
/// exactly (a length-field corruption desyncs the walk);
/// [`IggyError::InvalidMessageChecksum`] on the first per-message mismatch.
pub fn verify_received_send_messages(bytes: &[u8]) -> Result<(), IggyError> {
    let batch = decode_prepare_slice_trusted(bytes)?;
    let blob = batch.blob();
    let mut verified = 0u32;
    let mut covered = 0usize;
    for framed in batch.iter_with_offsets() {
        // Cover (`header[8..48] || payload || user_headers`) hashed raw from
        // the blob, not rebuilt from decoded fields, so it is byte-exact with
        // the encoder's.
        let expected = XxHash3_64::oneshot(&blob[framed.start + 8..framed.end]);
        if expected != framed.message.header.checksum {
            return Err(IggyError::InvalidMessageChecksum(
                framed.message.header.checksum,
                expected,
                batch.header.base_offset + u64::from(framed.message.header.offset_delta),
            ));
        }
        verified += 1;
        covered = framed.end;
    }
    if verified != batch.message_count() || covered != blob.len() {
        return Err(IggyError::InvalidCommand);
    }
    Ok(())
}

fn legacy_messages_slice(body: &[u8]) -> Result<(u32, &[u8]), IggyError> {
    if body.len() < 4 {
        return Err(IggyError::InvalidCommand);
    }

    let metadata_length = read_u32(body, 0)? as usize;
    let metadata_end = 4usize
        .checked_add(metadata_length)
        .ok_or(IggyError::InvalidCommand)?;
    if metadata_end < 4 || body.len() < metadata_end {
        return Err(IggyError::InvalidCommand);
    }

    let message_count = read_u32(body, metadata_end - 4)?;
    let indexes_len = usize::try_from(message_count)
        .ok()
        .and_then(|count| count.checked_mul(INDEX_SIZE))
        .ok_or(IggyError::InvalidCommand)?;
    let messages_start = metadata_end
        .checked_add(indexes_len)
        .ok_or(IggyError::InvalidCommand)?;
    if body.len() < messages_start {
        return Err(IggyError::InvalidCommand);
    }

    Ok((message_count, &body[messages_start..]))
}

#[derive(Clone, Copy)]
struct LegacyMessageRef<'a> {
    id: u128,
    origin_timestamp: u64,
    user_headers: &'a [u8],
    payload: &'a [u8],
    total_size: usize,
}

impl<'a> LegacyMessageRef<'a> {
    fn decode(bytes: &'a [u8]) -> Result<Self, IggyError> {
        if bytes.len() < LEGACY_MESSAGE_HEADER_SIZE {
            return Err(IggyError::InvalidCommand);
        }

        let user_headers_length = read_u32(bytes, 48)? as usize;
        let payload_length = read_u32(bytes, 52)? as usize;
        let total_size = LEGACY_MESSAGE_HEADER_SIZE
            .checked_add(payload_length)
            .and_then(|size| size.checked_add(user_headers_length))
            .ok_or(IggyError::InvalidCommand)?;
        if bytes.len() < total_size {
            return Err(IggyError::InvalidCommand);
        }

        let payload_start = LEGACY_MESSAGE_HEADER_SIZE;
        let payload_end = payload_start + payload_length;
        let headers_end = payload_end + user_headers_length;

        Ok(Self {
            id: read_u128(bytes, 8)?,
            origin_timestamp: read_u64(bytes, 40)?,
            user_headers: &bytes[payload_end..headers_end],
            payload: &bytes[payload_start..payload_end],
            total_size,
        })
    }
}

fn calculate_batch_checksum(header: &SendMessages2Header, blob: &[u8]) -> u64 {
    let mut hasher = XxHash3_64::new();
    hasher.write(&header.partition_id.to_le_bytes());
    hasher.write(&header.base_offset.to_le_bytes());
    hasher.write(&header.base_timestamp.to_le_bytes());
    hasher.write(&header.origin_timestamp.to_le_bytes());
    hasher.write(&header.batch_length.to_le_bytes());
    hasher.write(&header.message_count.to_le_bytes());
    hasher.write(blob);
    hasher.finish()
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, IggyError> {
    bytes
        .get(offset..offset + 4)
        .and_then(|slice| slice.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(IggyError::InvalidNumberEncoding)
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, IggyError> {
    bytes
        .get(offset..offset + 8)
        .and_then(|slice| slice.try_into().ok())
        .map(u64::from_le_bytes)
        .ok_or(IggyError::InvalidNumberEncoding)
}

fn read_u128(bytes: &[u8], offset: usize) -> Result<u128, IggyError> {
    bytes
        .get(offset..offset + 16)
        .and_then(|slice| slice.try_into().ok())
        .map(u128::from_le_bytes)
        .ok_or(IggyError::InvalidNumberEncoding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Command2, Operation};
    use iggy_common::Aes256GcmEncryptor;

    fn aligned_prepare_bytes(size: u32) -> Owned<MESSAGE_ALIGN> {
        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(std::mem::size_of::<PrepareHeader>());
        let header: &mut PrepareHeader =
            bytemuck::checked::try_from_bytes_mut(owned.as_mut_slice())
                .expect("zeroed bytes form a valid PrepareHeader");
        header.command = Command2::Prepare;
        header.size = size;
        owned
    }

    /// A checksum-consistent `Prepare`: `[PrepareHeader][256B batch header][blob]`
    /// with `batch_checksum` stamped over the final header fields + `blob`.
    fn valid_prepare_bytes(blob: &[u8]) -> Owned<MESSAGE_ALIGN> {
        let header_size = std::mem::size_of::<PrepareHeader>();
        let batch_length = COMMAND_HEADER_SIZE + blob.len();
        let total = header_size + batch_length;
        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(total);
        {
            let prepare: &mut PrepareHeader =
                bytemuck::checked::try_from_bytes_mut(&mut owned.as_mut_slice()[..header_size])
                    .expect("zeroed bytes form a valid PrepareHeader");
            prepare.command = Command2::Prepare;
            prepare.size = u32::try_from(total).expect("prepare size fits u32");
        }

        let mut command = SendMessages2Header::new(7, 123, batch_length as u64, 3);
        command.base_offset = 10;
        command.base_timestamp = 20;
        command.batch_checksum = command.checksum_for_blob(blob);

        let bytes = owned.as_mut_slice();
        command.encode_into(&mut bytes[header_size..header_size + COMMAND_HEADER_SIZE]);
        bytes[header_size + COMMAND_HEADER_SIZE..].copy_from_slice(blob);
        owned
    }

    #[test]
    fn decode_prepare_slice_trusted_matches_validating_for_valid_batch() {
        // The trusted variant must surface byte-identical header meta to the
        // validating decode for a checksum-consistent batch; only the full-blob
        // hash pass is skipped.
        let blob = vec![0x5Au8; 4096];
        let owned = valid_prepare_bytes(&blob);

        let validated = decode_prepare_slice(owned.as_slice()).expect("valid batch decodes");
        let trusted =
            decode_prepare_slice_trusted(owned.as_slice()).expect("valid batch decodes trusted");

        assert_eq!(validated.header.base_offset, trusted.header.base_offset);
        assert_eq!(
            validated.header.base_timestamp,
            trusted.header.base_timestamp
        );
        assert_eq!(
            validated.header.origin_timestamp,
            trusted.header.origin_timestamp
        );
        assert_eq!(validated.header.batch_length, trusted.header.batch_length);
        assert_eq!(validated.message_count(), trusted.message_count());
        assert_eq!(validated.header.total_size(), trusted.header.total_size());
        assert_eq!(validated.blob(), trusted.blob());
    }

    #[test]
    fn decode_prepare_slice_trusted_skips_batch_checksum() {
        // A blob mutated after stamping fails the validating decode but passes
        // the trusted one: exactly why the trusted variant is confined to
        // locally-produced bytes (see its doc invariant).
        let blob = vec![0x11u8; 512];
        let mut owned = valid_prepare_bytes(&blob);
        let corrupt_index = owned.as_slice().len() - 1;
        owned.as_mut_slice()[corrupt_index] ^= 0xFF;

        assert!(
            matches!(
                decode_prepare_slice(owned.as_slice()),
                Err(IggyError::InvalidBatchChecksum(..))
            ),
            "validating decode must reject a corrupted blob",
        );
        assert!(
            decode_prepare_slice_trusted(owned.as_slice()).is_ok(),
            "trusted decode skips the batch-checksum recomputation",
        );
    }

    #[test]
    fn decode_prepare_slice_size_below_header_size_does_not_panic() {
        // Regression: without the `total_size < header_size` guard,
        // `&bytes[256..size]` panics for any size < 256.
        for adversarial_size in [0u32, 255] {
            let owned = aligned_prepare_bytes(adversarial_size);
            let result = decode_prepare_slice(owned.as_slice());
            assert!(
                matches!(result, Err(IggyError::InvalidCommand)),
                "size={adversarial_size} must be rejected, got {result:?}",
            );
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "must be at least 16-byte aligned")]
    fn decode_prepare_slice_debug_asserts_on_misaligned_input() {
        // `Vec<u8>` requests align=1; glibc returns a base that is a
        // multiple of 16, so `&buf[1..]` has offset 1 mod 16, eliably
        // misaligned.
        let buf: Vec<u8> = vec![0u8; std::mem::size_of::<PrepareHeader>() + 1];
        let misaligned = &buf[1..];
        assert_ne!(
            misaligned.as_ptr().align_offset(16),
            0,
            "test setup: allocator returned non-16k base",
        );
        let _ = decode_prepare_slice(misaligned);
    }

    fn sample_messages() -> IggyMessages2 {
        let mut messages = IggyMessages2::with_capacity(2);
        messages.push(IggyMessage2 {
            header: IggyMessage2Header {
                id: 7,
                origin_timestamp: 1_000,
                ..Default::default()
            },
            payload: Bytes::from_static(b"first-payload"),
            user_headers: None,
        });
        messages.push(IggyMessage2 {
            header: IggyMessage2Header {
                id: 8,
                origin_timestamp: 1_050,
                ..Default::default()
            },
            payload: Bytes::from_static(b"second-payload"),
            user_headers: Some(Bytes::from_static(b"user-header-bytes")),
        });
        messages
    }

    /// `[PrepareHeader][256B batch header][blob]` carrying real per-message
    /// records + checksums from the production encoder, left pre-stamp
    /// (`base_offset` / `base_timestamp` zero) as a follower receives it.
    fn prepare_with_messages(messages: &IggyMessages2) -> Owned<MESSAGE_ALIGN> {
        let namespace = IggyNamespace::new(1, 1, 7);
        let owned =
            SendMessages2Owned::from_messages(namespace, messages).expect("build send batch");
        let header_size = std::mem::size_of::<PrepareHeader>();
        let total = header_size + owned.header.total_size();
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total);
        {
            let prepare: &mut PrepareHeader =
                bytemuck::checked::try_from_bytes_mut(&mut buffer.as_mut_slice()[..header_size])
                    .expect("zeroed bytes form a valid PrepareHeader");
            prepare.command = Command2::Prepare;
            prepare.size = u32::try_from(total).expect("prepare size fits u32");
        }
        let bytes = buffer.as_mut_slice();
        owned
            .header
            .encode_into(&mut bytes[header_size..header_size + COMMAND_HEADER_SIZE]);
        bytes[PREPARE_SPLIT_POINT..PREPARE_SPLIT_POINT + owned.blob.len()]
            .copy_from_slice(&owned.blob);
        buffer
    }

    #[test]
    fn verify_received_send_messages_accepts_clean_batch() {
        let owned = prepare_with_messages(&sample_messages());
        verify_received_send_messages(owned.as_slice())
            .expect("a clean batch passes the receive gate");
    }

    #[test]
    fn verify_received_send_messages_rejects_flipped_payload_byte() {
        let mut owned = prepare_with_messages(&sample_messages());
        // First payload begins right after the first message's 48B header.
        let payload_index = PREPARE_SPLIT_POINT + MESSAGE_HEADER_SIZE;
        owned.as_mut_slice()[payload_index] ^= 0xFF;
        assert!(
            matches!(
                verify_received_send_messages(owned.as_slice()),
                Err(IggyError::InvalidMessageChecksum(..))
            ),
            "a flipped payload byte must fail the per-message checksum",
        );
    }

    #[test]
    fn verify_received_send_messages_rejects_flipped_stored_checksum() {
        let mut owned = prepare_with_messages(&sample_messages());
        // The first message's stored checksum is the first 8 bytes of the blob.
        owned.as_mut_slice()[PREPARE_SPLIT_POINT] ^= 0xFF;
        assert!(
            matches!(
                verify_received_send_messages(owned.as_slice()),
                Err(IggyError::InvalidMessageChecksum(..))
            ),
            "a flipped stored checksum must fail the per-message check",
        );
    }

    #[test]
    fn checksum_oneshot_matches_streaming_reference() {
        // Formula pin: the per-message checksum is XxHash3-64 (default seed)
        // over `header[8..48] || payload || user_headers` as one byte stream.
        // The encoders hash the concatenation in a single oneshot pass; this
        // streaming reference feeds the same parts separately. Both must agree
        // for every shape, or checksums at rest stop verifying.
        fn streaming_reference(header_tail: &[u8], payload: &[u8], user_headers: &[u8]) -> u64 {
            let mut hasher = XxHash3_64::new();
            hasher.write(header_tail);
            hasher.write(payload);
            hasher.write(user_headers);
            hasher.finish()
        }

        let header_tail: Vec<u8> = (0u8..40).collect();
        let kilobyte: Vec<u8> = (0..1024u32).map(|index| (index % 251) as u8).collect();
        let cases: &[(&[u8], &[u8])] = &[
            (&[], &[]),
            (b"payload-bytes", &[]),
            (b"payload-bytes", b"user-header-bytes"),
            (&kilobyte, &[]),
            (&kilobyte, &kilobyte[..7]),
            (&kilobyte[..1023], &kilobyte[..7]),
        ];
        for (payload, user_headers) in cases {
            let mut concatenated =
                Vec::with_capacity(header_tail.len() + payload.len() + user_headers.len());
            concatenated.extend_from_slice(&header_tail);
            concatenated.extend_from_slice(payload);
            concatenated.extend_from_slice(user_headers);
            assert_eq!(
                XxHash3_64::oneshot(&concatenated),
                streaming_reference(&header_tail, payload, user_headers),
                "oneshot must match the streaming reference for payload {} B, user headers {} B",
                payload.len(),
                user_headers.len(),
            );
        }
    }

    /// Legacy `SendMessages` request body: `[metadata_len=4][message_count]`
    /// then `count` skipped index slots, then the 64B-header legacy records.
    fn legacy_send_messages_body(messages: &IggyMessages2) -> Vec<u8> {
        let count = messages.count();
        let mut body = Vec::new();
        body.extend_from_slice(&4u32.to_le_bytes());
        body.extend_from_slice(&count.to_le_bytes());
        body.extend_from_slice(&vec![0u8; count as usize * INDEX_SIZE]);
        for message in messages.iter() {
            let user_headers = message.user_headers.as_deref().unwrap_or_default();
            let mut header = [0u8; LEGACY_MESSAGE_HEADER_SIZE];
            header[8..24].copy_from_slice(&message.header.id.to_le_bytes());
            header[40..48].copy_from_slice(&message.header.origin_timestamp.to_le_bytes());
            header[48..52].copy_from_slice(&(user_headers.len() as u32).to_le_bytes());
            header[52..56].copy_from_slice(&(message.payload.len() as u32).to_le_bytes());
            body.extend_from_slice(&header);
            body.extend_from_slice(&message.payload);
            body.extend_from_slice(user_headers);
        }
        body
    }

    fn legacy_request_message(body: &[u8]) -> Message<RequestHeader> {
        let header_size = std::mem::size_of::<RequestHeader>();
        let total = header_size + body.len();
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total);
        {
            let header: &mut RequestHeader =
                bytemuck::checked::try_from_bytes_mut(&mut buffer.as_mut_slice()[..header_size])
                    .expect("zeroed bytes form a valid RequestHeader");
            header.command = Command2::Request;
            header.operation = Operation::SendMessages;
            header.client = 1;
            header.session = 1;
            header.request = 1;
            header.size = u32::try_from(total).expect("size fits u32");
        }
        buffer.as_mut_slice()[header_size..].copy_from_slice(body);
        Message::try_from(buffer).expect("legacy request message is valid")
    }

    #[test]
    fn convert_request_message_transcodes_legacy_to_canonical_bytes() {
        // Golden: the fused legacy transcode must emit the exact canonical batch
        // the native builder (`from_messages`) produces for the same messages -
        // command header + blob, byte for byte. Explicit non-zero ids keep it
        // deterministic (no `random_id` substitution).
        let namespace = IggyNamespace::new(1, 1, 3);
        let messages = sample_messages();

        let owned =
            SendMessages2Owned::from_messages(namespace, &messages).expect("build canonical batch");
        let mut expected_body = vec![0u8; COMMAND_HEADER_SIZE + owned.blob.len()];
        owned
            .header
            .encode_into(&mut expected_body[..COMMAND_HEADER_SIZE]);
        expected_body[COMMAND_HEADER_SIZE..].copy_from_slice(&owned.blob);

        let legacy = legacy_request_message(&legacy_send_messages_body(&messages));
        let converted = convert_request_message(namespace, legacy, ChecksumMode::Compute)
            .expect("legacy body transcodes");
        let header_size = std::mem::size_of::<RequestHeader>();
        let actual_body = &converted.as_slice()[header_size..converted.header().size as usize];

        assert_eq!(
            actual_body, expected_body,
            "legacy transcode must be byte-identical to the canonical native batch",
        );

        // And the emitted batch is self-consistent: it validates through the
        // batch-checksum decode and yields the original messages.
        let decoded = decode_batch_slice(actual_body).expect("transcoded batch checksum is valid");
        assert_eq!(decoded.message_count(), messages.count());
        let payloads: Vec<&[u8]> = decoded.iter().map(|view| view.payload).collect();
        assert_eq!(
            payloads,
            vec![&b"first-payload"[..], &b"second-payload"[..]]
        );
    }

    #[test]
    fn convert_request_message_skip_leaves_batch_checksum_zero_until_stamp() {
        // The partition ingest path passes Skip: the transcoded batch must carry
        // a zero checksum (stamp fills it) and be otherwise byte-identical to the
        // Compute output - the flag toggles nothing but that one hash.
        let namespace = IggyNamespace::new(1, 1, 3);
        let messages = sample_messages();
        let body = legacy_send_messages_body(&messages);
        let header_size = std::mem::size_of::<RequestHeader>();

        let computed = convert_request_message(
            namespace,
            legacy_request_message(&body),
            ChecksumMode::Compute,
        )
        .expect("compute transcode");
        let skipped =
            convert_request_message(namespace, legacy_request_message(&body), ChecksumMode::Skip)
                .expect("skip transcode");

        let computed_body = &computed.as_slice()[header_size..computed.header().size as usize];
        let skipped_body = &skipped.as_slice()[header_size..skipped.header().size as usize];

        let skipped_header = SendMessages2Header::decode(&skipped_body[..COMMAND_HEADER_SIZE])
            .expect("decode skipped header");
        assert_eq!(
            skipped_header.batch_checksum, 0,
            "skip leaves the batch checksum zero until stamp",
        );

        // Patch only the 8-byte batch_checksum field into the skipped body; it
        // must then equal the computed body, proving nothing else diverges.
        let mut patched = skipped_body.to_vec();
        patched[BATCH_CHECKSUM_OFFSET..BATCH_CHECKSUM_OFFSET + 8]
            .copy_from_slice(&computed_body[BATCH_CHECKSUM_OFFSET..BATCH_CHECKSUM_OFFSET + 8]);
        assert_eq!(
            patched.as_slice(),
            computed_body,
            "skip and compute differ only in the batch_checksum field",
        );
    }

    #[test]
    fn encrypt_ingest_path_stays_canonical_through_flag_split() {
        // Mirror the plane encrypt ingest sequence: convert(Compute) -> the
        // validating decode encrypt performs on its input -> encrypt -> the
        // validating decode the second convert performs as its discriminator ->
        // convert(Skip) (the partition convert), which sees an already-canonical
        // batch and returns it unchanged. Every decode must succeed.
        let namespace = IggyNamespace::new(1, 1, 3);
        let messages = sample_messages();
        let header_size = std::mem::size_of::<RequestHeader>();

        let legacy = legacy_request_message(&legacy_send_messages_body(&messages));
        let canonical = convert_request_message(namespace, legacy, ChecksumMode::Compute)
            .expect("pre-encrypt transcode");
        let canonical_body = &canonical.as_slice()[header_size..canonical.header().size as usize];
        decode_batch_slice(canonical_body).expect("encrypt input decode validates the checksum");

        let encryptor =
            EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::new(&[7u8; 32]).expect("valid 32B key"));
        let encrypted = encrypt_batch_request(canonical, &encryptor).expect("encrypt batch");
        let encrypted_body: Vec<u8> =
            encrypted.as_slice()[header_size..encrypted.header().size as usize].to_vec();
        decode_batch_slice(&encrypted_body)
            .expect("encrypt output drives the 2nd-convert discriminator");

        let repassed = convert_request_message(namespace, encrypted, ChecksumMode::Skip)
            .expect("second convert passes the canonical batch");
        let repassed_body = &repassed.as_slice()[header_size..repassed.header().size as usize];
        assert_eq!(
            repassed_body,
            encrypted_body.as_slice(),
            "an already-canonical encrypted batch passes the partition convert untouched",
        );
    }
}
