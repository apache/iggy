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

            let checksum = calculate_checksum_parts(&header[8..], &message.payload, user_headers);
            header[0..8].copy_from_slice(&checksum.to_le_bytes());

            blob.extend_from_slice(&header);
            blob.extend_from_slice(&message.payload);
            blob.extend_from_slice(user_headers);
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

    pub fn from_legacy_request(namespace: IggyNamespace, body: &[u8]) -> Result<Self, IggyError> {
        let (message_count, messages) = legacy_messages_slice(body)?;
        let mut parsed = Vec::with_capacity(message_count as usize);
        let mut origin_timestamp = u64::MAX;
        let mut cursor = 0usize;

        while cursor < messages.len() && parsed.len() < message_count as usize {
            let legacy = LegacyMessageRef::decode(&messages[cursor..])?;
            origin_timestamp = origin_timestamp.min(legacy.origin_timestamp);
            cursor += legacy.total_size;
            parsed.push(legacy);
        }

        if parsed.len() != message_count as usize || cursor != messages.len() {
            return Err(IggyError::InvalidCommand);
        }

        if origin_timestamp == u64::MAX {
            origin_timestamp = 0;
        }

        let mut blob = BytesMut::with_capacity(messages.len());
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

            let checksum =
                calculate_checksum_parts(&header[8..], legacy.payload, legacy.user_headers);
            header[0..8].copy_from_slice(&checksum.to_le_bytes());

            blob.extend_from_slice(&header);
            blob.extend_from_slice(legacy.payload);
            blob.extend_from_slice(legacy.user_headers);
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
        let checksum = calculate_checksum_parts(&header[8..], &encrypted_payload, user_headers);
        header[0..8].copy_from_slice(&checksum.to_le_bytes());

        blob.extend_from_slice(&header);
        blob.extend_from_slice(&encrypted_payload);
        blob.extend_from_slice(user_headers);
    }

    let blob = blob.freeze();
    let mut header = batch.header;
    header.batch_length =
        u64::try_from(COMMAND_HEADER_SIZE + blob.len()).map_err(|_| IggyError::InvalidCommand)?;
    header.batch_checksum = calculate_batch_checksum(&header, &blob);

    SendMessages2Owned { header, blob }.encode_request(request_header)
}

pub fn convert_request_message(
    namespace: IggyNamespace,
    message: Message<RequestHeader>,
) -> Result<Message<RequestHeader>, IggyError> {
    let request_header = *message.header();
    let total_size = request_header.size as usize;
    let body = &message.as_slice()[std::mem::size_of::<RequestHeader>()..total_size];
    if decode_batch_slice(body).is_ok() {
        return Ok(message);
    }
    SendMessages2Owned::from_legacy_request(namespace, body)?.encode_request(request_header)
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

/// Decode a `Prepare` message from a slice of bytes.
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

// Hash in storage order: header tail, payload, user headers (the message
// sections follow the legacy wire layout).
fn calculate_checksum_parts(header_tail: &[u8], payload: &[u8], user_headers: &[u8]) -> u64 {
    let mut hasher = XxHash3_64::new();
    hasher.write(header_tail);
    hasher.write(payload);
    hasher.write(user_headers);
    hasher.finish()
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
    use iggy_binary_protocol::Command2;

    fn aligned_prepare_bytes(size: u32) -> Owned<MESSAGE_ALIGN> {
        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(std::mem::size_of::<PrepareHeader>());
        let header: &mut PrepareHeader =
            bytemuck::checked::try_from_bytes_mut(owned.as_mut_slice())
                .expect("zeroed bytes form a valid PrepareHeader");
        header.command = Command2::Prepare;
        header.size = size;
        owned
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
}
