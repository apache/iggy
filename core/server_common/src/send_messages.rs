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

//! Server-side handling of the canonical message batch (layout in
//! [`iggy_binary_protocol::batch`]).
//!
//! Producers put the batch on the wire themselves; admission
//! ([`convert_request_message`]) verifies the producer's checksums, strips the
//! routing metadata, and stamps the partition. From there the same bytes are
//! journaled, replicated, persisted, and served back to polls.

use crate::consensus_message::{MESSAGE_ALIGN, Message};
use crate::iobuf::Owned;
use crate::sharding::IggyNamespace;
use bytes::{Bytes, BytesMut};
use iggy_binary_protocol::batch;
use iggy_binary_protocol::requests::messages::SendMessagesHeader as SendMessagesMetadata;
use iggy_binary_protocol::{PrepareHeader, RoutedRequestHeader, WireDecode, WireError};
use iggy_common::{EncryptorKind, IggyError, random_id};
use twox_hash::XxHash3_64;

pub use iggy_binary_protocol::batch::{
    BATCH_CHECKSUM_OFFSET, BATCH_HEADER_SIZE, BATCH_MESSAGE_HEADER_SIZE, BatchHeader,
    BatchIntegrity, BatchIterator, BatchIteratorWithOffsets, BatchMessageHeader, BatchMessageView,
    BatchMessageViewWithOffsets, BatchRef, MAX_TIMESTAMP_DELTA_MICROS, calculate_batch_checksum,
};

/// Size of the batch header at the front of a `SendMessages` prepare body.
pub const COMMAND_HEADER_SIZE: usize = BATCH_HEADER_SIZE;

/// Offset of the blob inside a prepare frame:
/// `[256B PrepareHeader][256B batch header][blob]`.
pub const PREPARE_SPLIT_POINT: usize = 512;

/// Map a wire-level batch error onto the typed `IggyError` the server paths
/// key on. The two integrity variants keep their payloads; every structural
/// error is an invalid command.
fn batch_error(error: &WireError) -> IggyError {
    match *error {
        WireError::InvalidBatchChecksum {
            stored,
            computed,
            base_offset,
        } => IggyError::InvalidBatchChecksum(stored, computed, base_offset),
        WireError::InvalidMessageChecksum {
            stored,
            computed,
            offset,
        } => IggyError::InvalidMessageChecksum(stored, computed, offset),
        _ => IggyError::InvalidCommand,
    }
}

/// Decode one batch record, verifying the batch checksum and every
/// per-message checksum.
///
/// See [`batch::decode_batch_slice`]; this wrapper types the error for the
/// server paths.
///
/// # Errors
/// [`IggyError::InvalidCommand`] for a short or inconsistent record, and
/// [`IggyError::InvalidBatchChecksum`] / [`IggyError::InvalidMessageChecksum`]
/// on an integrity mismatch.
pub fn decode_batch_slice(body: &[u8]) -> Result<BatchRef<'_>, IggyError> {
    batch::decode_batch_slice(body).map_err(|error| batch_error(&error))
}

/// [`decode_batch_slice`] with the integrity level chosen by the caller.
///
/// The one caller that passes anything but [`BatchIntegrity::Verify`] is the
/// disk poll under its operator knob; layout checks are not optional either
/// way.
///
/// # Errors
/// See [`decode_batch_slice`].
pub fn decode_batch_slice_with(
    body: &[u8],
    integrity: BatchIntegrity,
) -> Result<BatchRef<'_>, IggyError> {
    batch::decode_batch_slice_with(body, integrity).map_err(|error| batch_error(&error))
}

#[derive(Debug, Clone)]
pub struct SendMessagesOwned {
    pub header: BatchHeader,
    pub blob: Bytes,
}

impl SendMessagesOwned {
    pub fn from_messages(
        namespace: IggyNamespace,
        messages: &IggyMessages,
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

            let mut header = [0u8; BATCH_MESSAGE_HEADER_SIZE];
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
        let mut header = BatchHeader::new(
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
        mut request_header: RoutedRequestHeader,
    ) -> Result<Message<RoutedRequestHeader>, IggyError> {
        let total_size = std::mem::size_of::<RoutedRequestHeader>() + self.header.total_size();
        // The rebuilt body differs in size from the wire body the header
        // described; a stale `size` truncates the blob for every downstream
        // slice (stamping, journal reads).
        request_header.size = u32::try_from(total_size).map_err(|_| IggyError::InvalidCommand)?;
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total_size);
        let bytes = buffer.as_mut_slice();
        bytes[0..std::mem::size_of::<RoutedRequestHeader>()]
            .copy_from_slice(bytemuck::bytes_of(&request_header));
        self.header.encode_into(
            &mut bytes[std::mem::size_of::<RoutedRequestHeader>()
                ..std::mem::size_of::<RoutedRequestHeader>() + COMMAND_HEADER_SIZE],
        );
        bytes[PREPARE_SPLIT_POINT..PREPARE_SPLIT_POINT + self.blob.len()]
            .copy_from_slice(&self.blob);

        Message::try_from(buffer).map_err(|_| IggyError::InvalidCommand)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IggyMessageHeader {
    pub checksum: u64,
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub user_headers_length: u32,
    pub payload_length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IggyMessage {
    pub header: IggyMessageHeader,
    pub payload: Bytes,
    pub user_headers: Option<Bytes>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IggyMessages {
    messages: Vec<IggyMessage>,
}

impl IggyMessages {
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

    pub fn push(&mut self, message: IggyMessage) {
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

    pub fn iter(&self) -> std::slice::Iter<'_, IggyMessage> {
        self.messages.iter()
    }
}

impl IntoIterator for IggyMessages {
    type Item = IggyMessage;
    type IntoIter = std::vec::IntoIter<IggyMessage>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.into_iter()
    }
}

impl<'a> IntoIterator for &'a IggyMessages {
    type Item = &'a IggyMessage;
    type IntoIter = std::slice::Iter<'a, IggyMessage>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.iter()
    }
}

/// Encode `header` into its own frozen 256-byte buffer (e.g. the rewritten
/// header fragment of a server-sliced poll batch).
#[must_use]
pub fn frozen_batch_header(header: &BatchHeader) -> crate::iobuf::Frozen<MESSAGE_ALIGN> {
    let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(COMMAND_HEADER_SIZE);
    header.encode_into(buffer.as_mut_slice());
    buffer.into()
}

/// Re-encode a canonical `SendMessages` request with every message's payload
/// and user headers encrypted, per-message checksums and lengths recomputed,
/// and the batch header (length + checksum) restamped.
///
/// Runs ONCE, on the primary at ingestion (after [`convert_request_message`]
/// admitted the wire form), so the ciphertext is what replicates: every
/// replica journals and persists identical bytes, and the poll path decrypts
/// uniformly regardless of which replica or tier served the fragment.
///
/// # Errors
///
/// [`IggyError::InvalidCommand`] on an undecodable batch; encryption errors
/// propagate from the encryptor.
pub fn encrypt_batch_request(
    message: Message<RoutedRequestHeader>,
    encryptor: &EncryptorKind,
) -> Result<Message<RoutedRequestHeader>, IggyError> {
    let request_header = *message.header();
    let total_size = request_header.size as usize;
    let body = &message.as_slice()[std::mem::size_of::<RoutedRequestHeader>()..total_size];
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

        let mut header = [0u8; BATCH_MESSAGE_HEADER_SIZE];
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

    SendMessagesOwned { header, blob }.encode_request(request_header)
}

/// Rebuild one stored batch record with every message's payload and user
/// headers decrypted: the poll reply's single decrypt point, mirroring
/// [`encrypt_batch_request`]. Lengths, per-message checksums, and the batch
/// header (length + checksum) are restamped over the plaintext.
///
/// Framing is validated layout-only: the read path already applied its
/// integrity knob to the stored bytes, and a server-sliced fragment's header
/// was rewritten checksum-consistent at slice time.
///
/// # Errors
/// [`IggyError::InvalidCommand`] on a malformed record;
/// [`IggyError::CannotDecryptData`] when a section fails to decrypt.
pub fn decrypt_batch_record(
    record: &[u8],
    encryptor: &EncryptorKind,
) -> Result<Vec<u8>, IggyError> {
    let batch = decode_batch_slice_with(record, BatchIntegrity::LayoutOnly)?;
    if record.len() != batch.header.total_size() {
        return Err(IggyError::InvalidCommand);
    }

    let mut blob = BytesMut::with_capacity(batch.blob().len());
    for view in batch.iter() {
        let payload = encryptor
            .decrypt(view.payload)
            .map_err(|_| IggyError::CannotDecryptData)?;
        let decrypted_user_headers = if view.user_headers.is_empty() {
            None
        } else {
            Some(
                encryptor
                    .decrypt(view.user_headers)
                    .map_err(|_| IggyError::CannotDecryptData)?,
            )
        };
        let user_headers: &[u8] = decrypted_user_headers.as_deref().unwrap_or_default();
        let payload_length = u32::try_from(payload.len()).map_err(|_| IggyError::InvalidCommand)?;
        let user_headers_length =
            u32::try_from(user_headers.len()).map_err(|_| IggyError::InvalidCommand)?;

        let mut header = [0u8; BATCH_MESSAGE_HEADER_SIZE];
        header[8..24].copy_from_slice(&view.header.id.to_le_bytes());
        header[24..28].copy_from_slice(&view.header.offset_delta.to_le_bytes());
        header[28..32].copy_from_slice(&view.header.timestamp_delta.to_le_bytes());
        header[32..36].copy_from_slice(&user_headers_length.to_le_bytes());
        header[36..40].copy_from_slice(&payload_length.to_le_bytes());
        let msg_start = blob.len();
        blob.extend_from_slice(&header);
        blob.extend_from_slice(&payload);
        blob.extend_from_slice(user_headers);
        let checksum = XxHash3_64::oneshot(&blob[msg_start + 8..]);
        blob[msg_start..msg_start + 8].copy_from_slice(&checksum.to_le_bytes());
    }

    let mut header = batch.header;
    header.batch_length =
        u64::try_from(COMMAND_HEADER_SIZE + blob.len()).map_err(|_| IggyError::InvalidCommand)?;
    header.batch_checksum = calculate_batch_checksum(&header, &blob);

    let mut out = vec![0u8; COMMAND_HEADER_SIZE + blob.len()];
    header.encode_into(&mut out[..COMMAND_HEADER_SIZE]);
    out[COMMAND_HEADER_SIZE..].copy_from_slice(&blob);
    Ok(out)
}

/// Whether admission stamps a batch checksum onto its output.
///
/// The recompute is an `XxHash3` batch-checksum pass, needed only when a reader
/// validates the admitted batch before [`stamp_prepare_for_persistence`]
/// recomputes it: the encrypt ingest path re-decodes the admitted batch
/// (`encrypt_batch_request`'s validating decode, then the second `convert` its
/// output re-enters as the canonical-batch fast path). The partition ingest
/// path has no such reader, so it skips the pass and the checksum stays zero
/// until stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumMode {
    /// Compute the batch checksum for the admitted batch.
    Compute,
    /// Leave the batch checksum zero; `stamp_prepare_for_persistence` fills it.
    Skip,
}

/// Admit a `SendMessages` request into the pipeline form
/// `[RoutedRequestHeader][256B batch header][blob]`.
///
/// Two input shapes reach this:
/// - The wire form `[metadata][batch]` from a producer. The metadata section
///   is validated and stripped, the batch is checksum-verified exactly as the
///   producer hashed it (`partition_id` still zero), and the partition is
///   stamped afterwards.
/// - A body that already IS one canonical batch: the encrypt ingest path
///   re-enters here with its own output. Detected first via the
///   checksum-verified decode - a wire-form body cannot pass it, since its
///   leading metadata bytes cannot form a batch whose length and checksum
///   both match.
///
/// Either way the batch must fill the body exactly: `size` and `batch_length`
/// are independent client-supplied fields, and a suffix past `batch_length`
/// is covered by no checksum, still rides the buffer to disk, and desyncs the
/// segment walk that advances by `batch_length`.
///
/// # Errors
/// [`IggyError::InvalidCommand`] on a malformed body, an empty batch, or a
/// metadata/batch count mismatch; typed checksum errors from the validating
/// decode.
pub fn convert_request_message(
    namespace: IggyNamespace,
    message: Message<RoutedRequestHeader>,
    checksum: ChecksumMode,
) -> Result<Message<RoutedRequestHeader>, IggyError> {
    let request_header = *message.header();
    let total_size = request_header.size as usize;
    let body = &message.as_slice()[std::mem::size_of::<RoutedRequestHeader>()..total_size];

    if let Ok(batch) = decode_batch_slice(body) {
        if batch.message_count() == 0 || body.len() != batch.header.total_size() {
            return Err(IggyError::InvalidCommand);
        }
        return Ok(message);
    }

    admit_wire_request(namespace, body, request_header, checksum)
}

/// Validate a producer's `[metadata][batch]` body and rebuild it as the
/// pipeline form with the partition stamped.
fn admit_wire_request(
    namespace: IggyNamespace,
    body: &[u8],
    mut request_header: RoutedRequestHeader,
    checksum: ChecksumMode,
) -> Result<Message<RoutedRequestHeader>, IggyError> {
    if body.len() < 4 {
        return Err(IggyError::InvalidCommand);
    }
    let metadata_length = u32::from_le_bytes(
        body[..4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    ) as usize;
    let batch_start = 4usize
        .checked_add(metadata_length)
        .ok_or(IggyError::InvalidCommand)?;
    if body.len() < batch_start {
        return Err(IggyError::InvalidCommand);
    }

    let (metadata, consumed) = SendMessagesMetadata::decode(&body[4..batch_start])
        .map_err(|_| IggyError::InvalidCommand)?;
    if consumed != metadata_length {
        return Err(IggyError::InvalidCommand);
    }

    let batch_bytes = &body[batch_start..];
    let batch = decode_batch_slice(batch_bytes)?;
    if batch.message_count() == 0
        || batch.message_count() != metadata.messages_count
        || batch_bytes.len() != batch.header.total_size()
    {
        return Err(IggyError::InvalidCommand);
    }

    let header_size = std::mem::size_of::<RoutedRequestHeader>();
    let total_size = header_size + batch.header.total_size();
    request_header.size = u32::try_from(total_size).map_err(|_| IggyError::InvalidCommand)?;
    let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total_size);
    let bytes = buffer.as_mut_slice();
    bytes[0..header_size].copy_from_slice(bytemuck::bytes_of(&request_header));
    bytes[header_size..total_size].copy_from_slice(batch_bytes);

    // The producer hashed `partition_id = 0`; stamp the resolved partition
    // and restamp (or clear, for the stamp-fills-it path) the batch checksum.
    let mut stamped = batch.header;
    stamped.partition_id = namespace.partition_id() as u64;
    stamped.batch_checksum = match checksum {
        ChecksumMode::Compute => {
            calculate_batch_checksum(&stamped, &bytes[PREPARE_SPLIT_POINT..total_size])
        }
        ChecksumMode::Skip => 0,
    };
    stamped.encode_into(&mut bytes[header_size..header_size + COMMAND_HEADER_SIZE]);

    Message::try_from(buffer).map_err(|_| IggyError::InvalidCommand)
}

/// Decode a `Prepare` message from a slice of bytes, validating the batch
/// checksum and every per-message checksum.
///
/// `bytes` must be 16-byte aligned (`PrepareHeader` has `u128` fields). Source
/// from `Frozen<MESSAGE_ALIGN>` / `Owned<MESSAGE_ALIGN>` / `Message<H>`.
/// Misalignment: `debug_assert!` in debug; `InvalidCommand` in release.
///
/// # Errors
///
/// `IggyError::InvalidCommand` on a short buffer, bad bit pattern, `size`
/// outside `[header_size, bytes.len()]`, a `size` that does not describe the
/// batch exactly, or frames that do not tile the batch;
/// `InvalidBatchChecksum` / `InvalidMessageChecksum` on an integrity mismatch.
pub fn decode_prepare_slice(bytes: &[u8]) -> Result<BatchRef<'_>, IggyError> {
    decode_prepare_slice_inner(bytes, true)
}

/// Like [`decode_prepare_slice`] but skips the per-message checksum
/// verification and batch-checksum recompute, extracting only the header meta.
/// Every cheap structural check (length, 16-byte alignment, `size` bounds, and
/// `size` describing the batch exactly) is still enforced.
///
/// INVARIANT: `bytes` MUST be node-local self-stamped -
/// [`stamp_prepare_for_persistence`] recomputed the batch checksum over the
/// exact blob on the local node - or already integrity-checked at network
/// ingress. There is no consensus-layer blob validation: the `PrepareHeader`
/// integrity fields are inert zeros. Replicated and repaired prepares are
/// validated via [`decode_prepare_slice`] before the bytes reach any trusted
/// decode. Calling this on unvalidated network bytes would let a corrupted blob
/// pass undetected. The full-body per-message checksum pass dominates
/// produce-path CPU, so trusted call sites that only read header meta skip it.
///
/// # Errors
///
/// Same structural errors as [`decode_prepare_slice`], minus
/// `InvalidBatchChecksum` and `InvalidMessageChecksum`.
pub fn decode_prepare_slice_trusted(bytes: &[u8]) -> Result<BatchRef<'_>, IggyError> {
    decode_prepare_slice_inner(bytes, false)
}

fn decode_prepare_slice_inner(
    bytes: &[u8],
    validate_checksum: bool,
) -> Result<BatchRef<'_>, IggyError> {
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

    let header =
        BatchHeader::decode(&body[..COMMAND_HEADER_SIZE]).map_err(|error| batch_error(&error))?;
    let blob_len = header.blob_len().map_err(|error| batch_error(&error))?;
    // Exact, not a lower bound: a prepare frame IS one batch, so bytes past
    // `batch_length` belong to nobody - no checksum covers them, yet the flush
    // writes them, desyncing the segment walk. Readers walking a multi-batch
    // chunk use `decode_batch_slice`, which bounds the blob by design.
    if body.len() != header.total_size() {
        return Err(IggyError::InvalidCommand);
    }

    let blob = &body[COMMAND_HEADER_SIZE..COMMAND_HEADER_SIZE + blob_len];
    let batch = BatchRef::new(header, blob);
    if validate_checksum {
        let expected_checksum = batch::verify_and_recompute_batch_checksum(&batch)
            .map_err(|error| batch_error(&error))?;
        if header.batch_checksum != expected_checksum {
            return Err(IggyError::InvalidBatchChecksum(
                header.batch_checksum,
                expected_checksum,
                header.base_offset,
            ));
        }
    }

    Ok(batch)
}

pub fn stamp_prepare_for_persistence(
    mut message: Message<PrepareHeader>,
    base_offset: u64,
    base_timestamp: u64,
) -> Result<(Message<PrepareHeader>, BatchHeader, u32), IggyError> {
    let total_size = message.header().size as usize;
    let bytes = message.as_mut_slice();
    if bytes.len() < PREPARE_SPLIT_POINT || total_size < PREPARE_SPLIT_POINT {
        return Err(IggyError::InvalidCommand);
    }

    let header_offset = std::mem::size_of::<PrepareHeader>();
    let mut command =
        BatchHeader::decode(&bytes[header_offset..header_offset + COMMAND_HEADER_SIZE])
            .map_err(|error| batch_error(&error))?;
    command.base_offset = base_offset;
    command.base_timestamp = base_timestamp;
    let blob = &bytes[PREPARE_SPLIT_POINT..total_size];
    command.batch_checksum = calculate_batch_checksum(&command, blob);
    command.encode_into(&mut bytes[header_offset..header_offset + COMMAND_HEADER_SIZE]);
    Ok((message, command, command.message_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::requests::messages::{RawMessage, SendMessagesEncoder};
    use iggy_binary_protocol::{Command, Operation, WireIdentifier, WirePartitioning};
    use iggy_common::Aes256GcmEncryptor;
    use std::hash::Hasher;

    fn aligned_prepare_bytes(size: u32) -> Owned<MESSAGE_ALIGN> {
        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(std::mem::size_of::<PrepareHeader>());
        let header: &mut PrepareHeader =
            bytemuck::checked::try_from_bytes_mut(owned.as_mut_slice())
                .expect("zeroed bytes form a valid PrepareHeader");
        header.command = Command::Prepare;
        header.size = size;
        owned
    }

    /// Assemble an already-stamped batch into a `Prepare`:
    /// `[PrepareHeader][256B batch header][blob]`, copying `owned`'s header and
    /// blob verbatim. Shared by every real-batch fixture.
    fn prepare_from_owned(owned: &SendMessagesOwned) -> Owned<MESSAGE_ALIGN> {
        let header_size = std::mem::size_of::<PrepareHeader>();
        let total = header_size + owned.header.total_size();
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total);
        {
            let prepare: &mut PrepareHeader =
                bytemuck::checked::try_from_bytes_mut(&mut buffer.as_mut_slice()[..header_size])
                    .expect("zeroed bytes form a valid PrepareHeader");
            prepare.command = Command::Prepare;
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

    /// A checksum-consistent STAMPED `Prepare` carrying real per-message records,
    /// stamped at a non-zero `base_offset` / `base_timestamp` with a
    /// `batch_checksum` over the final header fields + per-message checksum fields.
    fn valid_prepare_bytes() -> Owned<MESSAGE_ALIGN> {
        let namespace = IggyNamespace::new(1, 1, 7);
        let mut owned = SendMessagesOwned::from_messages(namespace, &sample_messages())
            .expect("build send batch");
        owned.header.base_offset = 10;
        owned.header.base_timestamp = 20;
        owned.header.batch_checksum = owned.header.checksum_for_blob(&owned.blob);
        prepare_from_owned(&owned)
    }

    #[test]
    fn decode_prepare_slice_trusted_matches_validating_for_valid_batch() {
        // The trusted variant must surface byte-identical header meta to the
        // validating decode for a checksum-consistent batch; only the
        // per-message and batch-checksum passes are skipped.
        let owned = valid_prepare_bytes();

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
        // A stored batch_checksum mutated after stamping fails the validating
        // decode but passes the trusted one: exactly why the trusted variant is
        // confined to locally-produced bytes (see its doc invariant).
        let mut owned = valid_prepare_bytes();
        let corrupt_index = std::mem::size_of::<PrepareHeader>() + BATCH_CHECKSUM_OFFSET;
        owned.as_mut_slice()[corrupt_index] ^= 0xFF;

        assert!(
            matches!(
                decode_prepare_slice(owned.as_slice()),
                Err(IggyError::InvalidBatchChecksum(..))
            ),
            "validating decode must reject a mutated batch checksum",
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
        // multiple of 16, so `&buf[1..]` has offset 1 mod 16, reliably
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

    fn sample_messages() -> IggyMessages {
        let mut messages = IggyMessages::with_capacity(2);
        messages.push(IggyMessage {
            header: IggyMessageHeader {
                id: 7,
                origin_timestamp: 1_000,
                ..Default::default()
            },
            payload: Bytes::from_static(b"first-payload"),
            user_headers: None,
        });
        messages.push(IggyMessage {
            header: IggyMessageHeader {
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
    /// records and checksums from the production encoder, with the initial zero
    /// base offset and timestamp.
    fn prepare_with_messages(messages: &IggyMessages) -> Owned<MESSAGE_ALIGN> {
        let namespace = IggyNamespace::new(1, 1, 7);
        let owned =
            SendMessagesOwned::from_messages(namespace, messages).expect("build send batch");
        prepare_from_owned(&owned)
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

    #[test]
    fn batch_checksum_pins_header_fields_then_message_checksum_fields() {
        // Formula pin for the batch checksum: XxHash3-64 (default seed) streaming
        // over the six batch header meta fields (LE, in field order) then each
        // message's stored 8-byte checksum field in message order - never the
        // bodies. This reference walks the blob by the KNOWN input message sizes,
        // independent of the production frame decoder, and must equal what the
        // encoder stamped, or a stamp will not verify against a read-back
        // recompute.
        let namespace = IggyNamespace::new(1, 1, 7);
        let messages = sample_messages();
        let mut owned =
            SendMessagesOwned::from_messages(namespace, &messages).expect("build batch");
        owned.header.base_offset = 100;
        owned.header.base_timestamp = 200;
        owned.header.batch_checksum = owned.header.checksum_for_blob(&owned.blob);

        let mut hasher = XxHash3_64::new();
        hasher.write(&owned.header.partition_id.to_le_bytes());
        hasher.write(&owned.header.base_offset.to_le_bytes());
        hasher.write(&owned.header.base_timestamp.to_le_bytes());
        hasher.write(&owned.header.origin_timestamp.to_le_bytes());
        hasher.write(&owned.header.batch_length.to_le_bytes());
        hasher.write(&owned.header.message_count.to_le_bytes());
        let mut frame_start = 0usize;
        for message in messages.iter() {
            hasher.write(&owned.blob[frame_start..frame_start + 8]);
            let user_headers = message.user_headers.as_deref().unwrap_or_default();
            frame_start += BATCH_MESSAGE_HEADER_SIZE + message.payload.len() + user_headers.len();
        }
        let reference = hasher.finish();

        assert_eq!(
            frame_start,
            owned.blob.len(),
            "reference walk must consume the whole blob",
        );
        assert_eq!(
            owned.header.batch_checksum, reference,
            "batch checksum must equal hash(6 header fields || per-message checksum fields)",
        );
    }

    #[test]
    fn decode_batch_slice_rejects_body_corruption_with_intact_checksum_field() {
        // Equal-integrity: the batch value binds bodies only through the
        // per-message checksum fields, so a flipped body byte that leaves the
        // 8-byte checksum field intact keeps the batch value matching. The
        // validating decode must still reject it via the per-message verify -
        // the sole at-rest read-back check (the poll disk walk) decodes
        // through here.
        let namespace = IggyNamespace::new(1, 1, 7);
        let owned =
            SendMessagesOwned::from_messages(namespace, &sample_messages()).expect("build batch");
        let mut body = vec![0u8; COMMAND_HEADER_SIZE + owned.blob.len()];
        owned.header.encode_into(&mut body[..COMMAND_HEADER_SIZE]);
        body[COMMAND_HEADER_SIZE..].copy_from_slice(&owned.blob);

        decode_batch_slice(&body).expect("the clean batch decodes");

        // First payload byte sits right after the command header and the first
        // message's 48B frame header, leaving that frame's checksum field intact.
        let payload_index = COMMAND_HEADER_SIZE + BATCH_MESSAGE_HEADER_SIZE;
        body[payload_index] ^= 0xFF;
        assert!(
            matches!(
                decode_batch_slice(&body),
                Err(IggyError::InvalidMessageChecksum(..))
            ),
            "body corruption with an intact checksum field must fail the per-message verify",
        );
    }

    #[test]
    fn decode_prepare_slice_rejects_body_corruption_with_intact_checksum_field() {
        // The same equal-integrity guarantee at the resident/repair validating
        // decode, plus proof that the batch value alone is blind to it.
        let mut owned = prepare_with_messages(&sample_messages());
        decode_prepare_slice(owned.as_slice()).expect("the clean prepare decodes");

        let payload_index = PREPARE_SPLIT_POINT + BATCH_MESSAGE_HEADER_SIZE;
        owned.as_mut_slice()[payload_index] ^= 0xFF;
        assert!(
            matches!(
                decode_prepare_slice(owned.as_slice()),
                Err(IggyError::InvalidMessageChecksum(..))
            ),
            "body corruption with an intact checksum field must fail the per-message verify",
        );
        assert!(
            decode_prepare_slice_trusted(owned.as_slice()).is_ok(),
            "the intact checksum field leaves the batch value matching, so trusted still passes",
        );
    }

    /// Wire-form `SendMessages` body built by the production client encoder:
    /// `[metadata][256B batch header][blob]` with producer-computed checksums
    /// and `partition_id = 0`.
    fn wire_send_messages_body(messages: &IggyMessages) -> Vec<u8> {
        let raw: Vec<RawMessage<'_>> = messages
            .iter()
            .map(|message| RawMessage {
                id: message.header.id,
                origin_timestamp: message.header.origin_timestamp,
                headers: message.user_headers.as_deref(),
                payload: &message.payload,
            })
            .collect();
        let stream_id = WireIdentifier::numeric(1);
        let topic_id = WireIdentifier::numeric(1);
        let partitioning = WirePartitioning::Balanced;
        let mut buf = BytesMut::with_capacity(SendMessagesEncoder::encoded_size(
            &stream_id,
            &topic_id,
            &partitioning,
            &raw,
        ));
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &raw)
            .expect("wire body encodes");
        buf.to_vec()
    }

    fn wire_request_message(body: &[u8]) -> Message<RoutedRequestHeader> {
        let header_size = std::mem::size_of::<RoutedRequestHeader>();
        let total = header_size + body.len();
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total);
        {
            let header: &mut RoutedRequestHeader =
                bytemuck::checked::try_from_bytes_mut(&mut buffer.as_mut_slice()[..header_size])
                    .expect("zeroed bytes form a valid RoutedRequestHeader");
            header.command = Command::Request;
            header.operation = Operation::SendMessages;
            header.client = 1;
            header.session = 1;
            header.request = 1;
            header.size = u32::try_from(total).expect("size fits u32");
        }
        buffer.as_mut_slice()[header_size..].copy_from_slice(body);
        Message::try_from(buffer).expect("wire request message is valid")
    }

    #[test]
    fn convert_request_message_rejects_empty_batches() {
        let namespace = IggyNamespace::new(1, 1, 3);
        let messages = IggyMessages::with_capacity(0);
        let canonical = SendMessagesOwned::from_messages(namespace, &messages)
            .expect("build empty canonical batch");
        let mut canonical_body = vec![0; canonical.header.total_size()];
        canonical
            .header
            .encode_into(&mut canonical_body[..COMMAND_HEADER_SIZE]);
        let wire_body = wire_send_messages_body(&messages);

        for mode in [ChecksumMode::Compute, ChecksumMode::Skip] {
            let canonical_result =
                convert_request_message(namespace, wire_request_message(&canonical_body), mode);
            assert!(matches!(canonical_result, Err(IggyError::InvalidCommand)));

            let wire_result =
                convert_request_message(namespace, wire_request_message(&wire_body), mode);
            assert!(matches!(wire_result, Err(IggyError::InvalidCommand)));
        }
    }

    #[test]
    fn convert_request_message_admits_wire_body_and_stamps_partition() {
        // Golden: admitting the producer's wire body must yield the exact
        // canonical batch the server-side builder (`from_messages`) produces
        // for the same messages - command header + blob, byte for byte.
        // Explicit non-zero ids keep it deterministic.
        let namespace = IggyNamespace::new(1, 1, 3);
        let messages = sample_messages();

        let owned =
            SendMessagesOwned::from_messages(namespace, &messages).expect("build canonical batch");
        let mut expected_body = vec![0u8; COMMAND_HEADER_SIZE + owned.blob.len()];
        owned
            .header
            .encode_into(&mut expected_body[..COMMAND_HEADER_SIZE]);
        expected_body[COMMAND_HEADER_SIZE..].copy_from_slice(&owned.blob);

        let wire = wire_request_message(&wire_send_messages_body(&messages));
        let converted = convert_request_message(namespace, wire, ChecksumMode::Compute)
            .expect("wire body admits");
        let header_size = std::mem::size_of::<RoutedRequestHeader>();
        let actual_body = &converted.as_slice()[header_size..converted.header().size as usize];

        assert_eq!(
            actual_body, expected_body,
            "admitted wire body must be byte-identical to the server-built canonical batch",
        );

        // And the admitted batch is self-consistent: it validates through the
        // batch-checksum decode and yields the original messages.
        let decoded = decode_batch_slice(actual_body).expect("admitted batch checksum is valid");
        assert_eq!(decoded.header.partition_id, 3);
        assert_eq!(decoded.message_count(), messages.count());
        let payloads: Vec<&[u8]> = decoded.iter().map(|view| view.payload).collect();
        assert_eq!(
            payloads,
            vec![&b"first-payload"[..], &b"second-payload"[..]]
        );
    }

    #[test]
    fn convert_request_message_rejects_tampered_wire_body() {
        // A flipped payload byte invalidates the producer's per-message
        // checksum; admission must refuse the batch instead of stamping it.
        let namespace = IggyNamespace::new(1, 1, 3);
        let mut body = wire_send_messages_body(&sample_messages());
        let last = body.len() - 1;
        body[last] ^= 0xFF;
        let result =
            convert_request_message(namespace, wire_request_message(&body), ChecksumMode::Skip);
        assert!(matches!(result, Err(IggyError::InvalidMessageChecksum(..))));
    }

    #[test]
    fn convert_request_message_rejects_metadata_count_mismatch() {
        let namespace = IggyNamespace::new(1, 1, 3);
        let mut body = wire_send_messages_body(&sample_messages());
        // The metadata count is the 4 bytes right before the batch header:
        // metadata = [stream][topic][partitioning][count].
        let metadata_length =
            u32::from_le_bytes(body[..4].try_into().expect("4-byte slice")) as usize;
        let count_offset = 4 + metadata_length - 4;
        body[count_offset..count_offset + 4].copy_from_slice(&9u32.to_le_bytes());
        let result =
            convert_request_message(namespace, wire_request_message(&body), ChecksumMode::Skip);
        assert!(matches!(result, Err(IggyError::InvalidCommand)));
    }

    #[test]
    fn convert_request_message_skip_leaves_batch_checksum_zero_until_stamp() {
        // The partition ingest path passes Skip: the admitted batch must carry
        // a zero checksum (stamp fills it) and be otherwise byte-identical to
        // the Compute output - the flag toggles nothing but that one hash.
        let namespace = IggyNamespace::new(1, 1, 3);
        let messages = sample_messages();
        let body = wire_send_messages_body(&messages);
        let header_size = std::mem::size_of::<RoutedRequestHeader>();

        let computed = convert_request_message(
            namespace,
            wire_request_message(&body),
            ChecksumMode::Compute,
        )
        .expect("compute admission");
        let skipped =
            convert_request_message(namespace, wire_request_message(&body), ChecksumMode::Skip)
                .expect("skip admission");

        let computed_body = &computed.as_slice()[header_size..computed.header().size as usize];
        let skipped_body = &skipped.as_slice()[header_size..skipped.header().size as usize];

        let skipped_header = BatchHeader::decode(&skipped_body[..COMMAND_HEADER_SIZE])
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
        let header_size = std::mem::size_of::<RoutedRequestHeader>();

        let wire = wire_request_message(&wire_send_messages_body(&messages));
        let canonical = convert_request_message(namespace, wire, ChecksumMode::Compute)
            .expect("pre-encrypt admission");
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

    /// Junk suffixes that must be refused at both ingest boundaries: one below a
    /// frame header (the frame walk stops on a short read) and one frame-sized
    /// but undecodable (`reserved != 0`). Neither is covered by any checksum, so
    /// a walk that stops at the last decodable frame cannot see them.
    const TRAILING_JUNK_CASES: [&[u8]; 2] = [&[0xAA], &[0xFF; 64]];

    /// Canonical `SendMessages` request carrying `junk` past `batch_length`, with
    /// `RoutedRequestHeader.size` inflated to cover it. `size` and `batch_length` are
    /// independent wire fields, so a non-conforming client can emit this.
    fn canonical_request_with_trailing_bytes(junk: &[u8]) -> Message<RoutedRequestHeader> {
        let namespace = IggyNamespace::new(1, 1, 3);
        let owned =
            SendMessagesOwned::from_messages(namespace, &sample_messages()).expect("build batch");
        let header_size = std::mem::size_of::<RoutedRequestHeader>();
        let total = header_size + owned.header.total_size() + junk.len();
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total);
        {
            let header: &mut RoutedRequestHeader =
                bytemuck::checked::try_from_bytes_mut(&mut buffer.as_mut_slice()[..header_size])
                    .expect("zeroed bytes form a valid RoutedRequestHeader");
            header.command = Command::Request;
            header.operation = Operation::SendMessages;
            header.client = 1;
            header.session = 1;
            header.request = 1;
            header.size = u32::try_from(total).expect("size fits u32");
        }
        let bytes = buffer.as_mut_slice();
        owned
            .header
            .encode_into(&mut bytes[header_size..header_size + COMMAND_HEADER_SIZE]);
        let blob_end = PREPARE_SPLIT_POINT + owned.blob.len();
        bytes[PREPARE_SPLIT_POINT..blob_end].copy_from_slice(&owned.blob);
        bytes[blob_end..].copy_from_slice(junk);
        Message::try_from(buffer).expect("request message is valid")
    }

    /// A `Prepare` whose `size` covers `junk` past `batch_length`.
    fn prepare_with_trailing_bytes(junk: &[u8]) -> Owned<MESSAGE_ALIGN> {
        let namespace = IggyNamespace::new(1, 1, 7);
        let owned =
            SendMessagesOwned::from_messages(namespace, &sample_messages()).expect("build batch");
        let header_size = std::mem::size_of::<PrepareHeader>();
        let total = header_size + owned.header.total_size() + junk.len();
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total);
        {
            let prepare: &mut PrepareHeader =
                bytemuck::checked::try_from_bytes_mut(&mut buffer.as_mut_slice()[..header_size])
                    .expect("zeroed bytes form a valid PrepareHeader");
            prepare.command = Command::Prepare;
            prepare.size = u32::try_from(total).expect("prepare size fits u32");
        }
        let bytes = buffer.as_mut_slice();
        owned
            .header
            .encode_into(&mut bytes[header_size..header_size + COMMAND_HEADER_SIZE]);
        let blob_end = PREPARE_SPLIT_POINT + owned.blob.len();
        bytes[PREPARE_SPLIT_POINT..blob_end].copy_from_slice(&owned.blob);
        bytes[blob_end..].copy_from_slice(junk);
        buffer
    }

    #[test]
    fn convert_request_message_rejects_canonical_batch_with_trailing_bytes() {
        // Client ingest boundary. Accepting the request would carry the suffix
        // into the journal and onto disk: the flush writes the whole frame while
        // every reader advances by `batch_length`, so the segment walk lands
        // inside the junk and every later batch becomes unreadable.
        let namespace = IggyNamespace::new(1, 1, 3);
        for junk in TRAILING_JUNK_CASES {
            for mode in [ChecksumMode::Compute, ChecksumMode::Skip] {
                let message = canonical_request_with_trailing_bytes(junk);
                let result = convert_request_message(namespace, message, mode);
                assert!(
                    matches!(result, Err(IggyError::InvalidCommand)),
                    "{} trailing bytes ({mode:?}) must be rejected, got {result:?}",
                    junk.len(),
                );
            }
        }
    }

    #[test]
    fn convert_request_message_accepts_exact_canonical_batch() {
        // The same builder with no suffix must still pass untouched, so the
        // rejection above is the suffix and not the fixture.
        let namespace = IggyNamespace::new(1, 1, 3);
        let message = canonical_request_with_trailing_bytes(&[]);
        let expected = message.as_slice().to_vec();
        let converted = convert_request_message(namespace, message, ChecksumMode::Skip)
            .expect("an exact canonical batch passes untouched");
        assert_eq!(converted.as_slice(), expected.as_slice());
    }

    #[test]
    fn decode_prepare_slice_rejects_trailing_bytes_past_batch_length() {
        // Replica ingest must reject bytes beyond `batch_length` because no
        // per-message checksum covers them.
        for junk in TRAILING_JUNK_CASES {
            let owned = prepare_with_trailing_bytes(junk);
            assert!(
                matches!(
                    decode_prepare_slice(owned.as_slice()),
                    Err(IggyError::InvalidCommand)
                ),
                "{} trailing bytes must fail the validating decode",
                junk.len(),
            );
        }
    }
}
