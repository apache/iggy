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

//! Reply frame builders and readers shared by the dispatch spine and the HTTP
//! plane.
//!
//! Every reply echoes its request through `ReplyHeader`; the builders here
//! wrap a body in that echo (empty, denied, result-framed, `PolledMessages`
//! vectored or flattened), and the readers grade a committed reply's header
//! and result section for the write paths.

use crate::shell::{ShellBus, ShellShard};
use bytes::{Bytes, BytesMut};
use consensus::{MetadataHandle, VsrConsensus};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::consensus::{RESULT_COUNT_LEN, result_code};
use iggy_binary_protocol::responses::personal_access_tokens::RawPersonalAccessTokenResponse;
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::{
    Command, GenericHeader, IGGY_PROTOCOL_VERSION, ReplyHeader, RoutedRequestHeader, WireEncode,
    WireName,
};
use iggy_common::{EncryptorKind, IggyError};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::BusMessage;
use partitions::{Fragment, PollFragments};
use server_common::iobuf::{Frozen, Owned};
use server_common::send_messages;
use server_common::{MESSAGE_ALIGN, Message, ResponseBacking, ResponseFragments};
use std::rc::Rc;
use tracing::warn;

pub fn build_empty_reply(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
) -> Message<ReplyHeader> {
    build_reply_with_body(request_header, client_id, session, commit, 0, |_| {})
}

/// Build an empty reply that denies a dispatch-time authorization check: the
/// same request echo as [`build_empty_reply`] but with `ReplyHeader.status`
/// set to the rule's error code -- the request-level failure channel the SDK
/// peeks before any body decode. Every deny frame shares this shape (empty
/// body, nonzero status); op carries the builder's session argument like every
/// reply, and only the partition primary's pre-pipeline deny pins it to 0,
/// stamped through `consensus::build_deny_reply_from_request`.
pub fn build_deny_reply(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    status: u32,
) -> Message<ReplyHeader> {
    let mut reply = build_empty_reply(request_header, client_id, session, commit);
    let header_len = std::mem::size_of::<ReplyHeader>();
    let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
        &mut reply.as_mut_slice()[..header_len],
    )
    .expect("empty reply header is a valid ReplyHeader");
    header.status = status;
    reply
}

/// Server build version advertised in the login-register response.
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build a metadata reply carrying `payload` behind a success result section.
///
/// The SDK strips a result section off exactly the replies whose operation is
/// [`iggy_binary_protocol::Operation::is_result_framed`] (every metadata op plus the
/// four consumer-offset ops), and a non-empty `Register`, which it handles on its
/// own. For those, a payload missing the leading zero count has its first four bytes
/// eaten as a result count, and the decode fails or, worse, succeeds on the shifted
/// remainder: the raw-PAT reply shipped once without the prefix and broke the SDK.
///
/// The only way to BUILD a result-framed success body, though not the only path to a
/// success reply with one: [`build_reply_from_bytes`] passes a committed body
/// through, framed or not according to the operation. Framing a reply whose operation
/// is not result-framed breaks decoding just as badly, so the choice belongs with the
/// operation rather than here.
fn build_result_framed_reply(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    payload: &impl WireEncode,
) -> Message<ReplyHeader> {
    let mut encoded = BytesMut::with_capacity(payload.encoded_size());
    payload.encode(&mut encoded);
    build_reply_with_body(
        request_header,
        client_id,
        session,
        commit,
        RESULT_COUNT_LEN + encoded.len(),
        |out| {
            let (count, body) = out.split_at_mut(RESULT_COUNT_LEN);
            count.copy_from_slice(&0u32.to_le_bytes());
            body.copy_from_slice(&encoded);
        },
    )
}

pub fn build_login_register_reply(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    user_id: u32,
) -> Message<ReplyHeader> {
    // A transient Register instead ships a `[count=1][index=0]
    // [TransientNotCommitted]` frame (`build_transient_reply`), which the SDK
    // decodes and replays.
    let payload = LoginRegisterResponse {
        user_id,
        session,
        server_protocol_version: IGGY_PROTOCOL_VERSION,
        server_version: WireName::new(SERVER_VERSION).expect("SERVER_VERSION is 1-255 bytes"),
    };
    build_result_framed_reply(request_header, client_id, session, commit, &payload)
}

pub fn build_reply_from_bytes(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    body: &Bytes,
) -> Message<ReplyHeader> {
    build_reply_with_body(
        request_header,
        client_id,
        session,
        commit,
        body.len(),
        |out| out.copy_from_slice(body),
    )
}

/// The reply body past the generic header, bounded by the header's `size`
/// rather than by the buffer length: `size` is the frame's authoritative
/// extent, so a short frame reads as "no result section" instead of into
/// allocation padding.
#[must_use]
pub fn reply_body(reply: &Message<GenericHeader>) -> &[u8] {
    let size = reply.header().size as usize;
    reply
        .as_slice()
        .get(std::mem::size_of::<ReplyHeader>()..size)
        .unwrap_or_default()
}

/// The header of a SUCCESSFULLY COMMITTED metadata reply, or `None` when the
/// frame promises the caller nothing.
///
/// Three checks, in this order, and both callers need all three:
///
/// - an eviction is an `EvictionHeader` whose bytes would cast cleanly as a
///   `ReplyHeader`, so the command is checked FIRST: casting it would both
///   swallow the eviction and grade it as a commit;
/// - a request-level denial names itself in `ReplyHeader.status`, the channel
///   the SDK peeks before body decode (see [`build_deny_reply`]);
/// - a nonzero result section is a rejection, transient or committed. Every
///   reply here is result-framed (`Operation::is_result_framed` covers the
///   metadata ops; the partition plane grades through
///   `classify_partition_reply` instead), so a missing section is a malformed
///   frame, not a bare payload.
///
/// The read-your-writes floor and the raw-PAT splice both hang off exactly
/// this predicate - the floor must not advance on a frame that committed
/// nothing, and the token must not be grafted onto a rejection body - so they
/// share one implementation rather than two that have to stay in step.
///
/// A frame too short to hold a header, or one whose header will not cast, is
/// `None` with a warning: it is malformed, and the alternative is a panic on
/// the reply path.
#[must_use]
pub fn committed_reply_header(reply: &Message<GenericHeader>) -> Option<&ReplyHeader> {
    if reply.header().command != Command::Reply {
        return None;
    }
    let Some(bytes) = reply.as_slice().get(..std::mem::size_of::<ReplyHeader>()) else {
        warn!(
            size = reply.header().size,
            "metadata reply shorter than its own header"
        );
        return None;
    };
    let header = match bytemuck::checked::try_from_bytes::<ReplyHeader>(bytes) {
        Ok(header) => header,
        Err(error) => {
            warn!(?error, "metadata reply header failed to cast");
            return None;
        }
    };
    if header.status != 0 || result_code(reply_body(reply)) != Some(0) {
        return None;
    }
    Some(header)
}

/// The transient variant of a reply-shaped pre-consensus rejection frame
/// (`[count=1][index=0][code]`, see `build_result_rejection_reply`), or `None`
/// for a committed outcome. Either transient means the op did not commit, so
/// the write path must replay the same request id rather than grade it as a
/// committed result or advance the session gate. The two codes are kept
/// distinct because they exhaust differently: `TransientNotAccepted` never
/// entered the pipeline and is safe to re-issue anywhere, while
/// `TransientNotCommitted` may still commit and only a same-session same-id
/// replay is safe.
///
/// Lives here rather than in the HTTP reply module both planes' write paths
/// grade through: the dispatch spine needs it too, and importing it from
/// `http` would close a module cycle.
#[must_use]
pub fn transient_code(reply: &Message<GenericHeader>) -> Option<IggyError> {
    match result_code(reply_body(reply)) {
        Some(code) if code == IggyError::TransientNotCommitted.as_code() => {
            Some(IggyError::TransientNotCommitted)
        }
        Some(code) if code == IggyError::TransientNotAccepted.as_code() => {
            Some(IggyError::TransientNotAccepted)
        }
        _ => None,
    }
}

/// If a raw PAT token was minted (`CreatePersonalAccessToken`) and the commit
/// succeeded, replace the committed reply -- whose body is empty because the
/// raw token never entered consensus -- with a `RawPersonalAccessTokenResponse`,
/// reusing the confirmed commit position from the committed reply. Otherwise
/// (no token, a committed business rejection, or an eviction frame) the
/// committed reply passes through unchanged.
pub fn build_raw_pat_reply(
    request_header: &RoutedRequestHeader,
    committed: Message<GenericHeader>,
    raw_token: Option<String>,
) -> Result<Message<GenericHeader>, IggyError> {
    let Some(raw) = raw_token else {
        return Ok(committed);
    };
    // Only a genuine committed success gets the secret spliced in. An eviction
    // frame (a `CreatePersonalAccessToken` whose session died between bind and
    // request), a request-level denial, and a rejection result section all pass
    // through untouched, so the client decodes the typed outcome - or, for a
    // transient, replays - instead of having a raw token grafted onto a
    // rejection body whose hash never committed.
    let Some(commit) = committed_reply_header(&committed).map(|header| header.commit) else {
        return Ok(committed);
    };
    let token = WireName::new(raw.as_str()).map_err(|_| IggyError::InvalidFormat)?;
    let response = RawPersonalAccessTokenResponse { token };
    let reply = build_result_framed_reply(
        request_header,
        request_header.client,
        request_header.session,
        commit,
        &response,
    );
    Ok(reply.into_generic())
}

pub fn build_reply_with_body(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    body_len: usize,
    write_body: impl FnOnce(&mut [u8]),
) -> Message<ReplyHeader> {
    let header_len = std::mem::size_of::<ReplyHeader>();
    let total_size = header_len + body_len;
    let size = u32::try_from(total_size).expect("reply size must fit into u32");
    let mut reply = Message::<ReplyHeader>::new(total_size);
    let header = reply_header(request_header, client_id, session, commit, size);
    reply.as_mut_slice()[..header_len].copy_from_slice(bytemuck::bytes_of(&header));
    write_body(&mut reply.as_mut_slice()[header_len..total_size]);
    reply
}

/// The header of a `size`-byte reply frame answering `request_header`.
fn reply_header(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    size: u32,
) -> ReplyHeader {
    ReplyHeader {
        client: client_id,
        op: session,
        commit,
        ..ReplyHeader::echoing(request_header, size)
    }
}

pub fn current_metadata_commit<B, MJ, S, SB>(shard: &Rc<ShellShard<B, MJ, S, SB>>) -> u64
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .map_or(0, VsrConsensus::commit_max)
}

/// Body head of a `PolledMessages` reply:
/// `[partition_id:4][current_offset:8][count:4]`, before the batch records.
const POLLED_HEAD_LEN: usize = 16;

/// Build the `PolledMessages` reply for the wire as a vectored frame: one
/// buffer holding the reply header and the body head, then the poll
/// fragments as they are. The record bytes are never copied or gathered;
/// their reply encoding IS the storage encoding (see
/// [`build_polled_messages_body`]), so `count` comes from walking the batch
/// headers in place. At-rest decryption is the one case that must rewrite
/// records, and it takes the flattening builder instead.
pub fn build_polled_messages_reply(
    request_header: &RoutedRequestHeader,
    commit: u64,
    partition_id: u32,
    current_offset: u64,
    fragments: PollFragments,
    encryptor: Option<&EncryptorKind>,
) -> Result<BusMessage, IggyError> {
    let client_id = request_header.client;
    let session = request_header.session;
    if encryptor.is_some() {
        let body = build_polled_messages_body(partition_id, current_offset, fragments, encryptor)?;
        let reply = build_reply_from_bytes(request_header, client_id, session, commit, &body);
        return Ok(reply.into_generic().into_frozen().into());
    }

    let mut frames = ResponseFragments::with_capacity(fragments.len() + 1);
    frames.extend(fragments.into_iter().map(Fragment::into_frozen));
    let count = polled_message_count(&frames)?;
    let records_len: usize = frames.iter().map(Frozen::len).sum();

    let header_len = std::mem::size_of::<ReplyHeader>();
    let size = u32::try_from(header_len + POLLED_HEAD_LEN + records_len)
        .map_err(|_| IggyError::InvalidCommand)?;
    let header = reply_header(request_header, client_id, session, commit, size);
    let mut head = Owned::<MESSAGE_ALIGN>::zeroed(header_len + POLLED_HEAD_LEN);
    let (header_bytes, body_head) = head.as_mut_slice().split_at_mut(header_len);
    header_bytes.copy_from_slice(bytemuck::bytes_of(&header));
    body_head[..4].copy_from_slice(&partition_id.to_le_bytes());
    body_head[4..12].copy_from_slice(&current_offset.to_le_bytes());
    body_head[12..].copy_from_slice(&count.to_le_bytes());
    frames.insert(0, head.into());

    // Re-checks the header and that the fragments cover `size`.
    Message::<ReplyHeader, ResponseBacking>::try_from(frames)
        .map(Message::into_inner)
        .map_err(|_| IggyError::InvalidCommand)
}

/// Sum of `message_count` over the batch records spanning `fragments`, read
/// from each batch header in place. Rejects a stream that is not a whole
/// number of batches, as [`build_polled_messages_body`] does.
fn polled_message_count(fragments: &[Frozen<MESSAGE_ALIGN>]) -> Result<u32, IggyError> {
    let mut cursor = FragmentCursor::new(fragments);
    let mut count = 0u32;
    let mut header = [0u8; send_messages::COMMAND_HEADER_SIZE];
    while !cursor.is_exhausted() {
        cursor.read_exact(&mut header)?;
        let batch =
            send_messages::BatchHeader::decode(&header).map_err(|_| IggyError::InvalidCommand)?;
        cursor.skip(batch.blob_len().map_err(|_| IggyError::InvalidCommand)?)?;
        count = count
            .checked_add(batch.message_count)
            .ok_or(IggyError::InvalidCommand)?;
    }
    Ok(count)
}

/// Byte cursor over the virtual concatenation of `fragments`. Rests on an
/// unread byte or at the end of the stream, never inside an exhausted
/// fragment, so a batch header split across fragments reads the same as one
/// stored whole.
struct FragmentCursor<'a> {
    fragments: &'a [Frozen<MESSAGE_ALIGN>],
    index: usize,
    offset: usize,
}

impl<'a> FragmentCursor<'a> {
    fn new(fragments: &'a [Frozen<MESSAGE_ALIGN>]) -> Self {
        let mut cursor = Self {
            fragments,
            index: 0,
            offset: 0,
        };
        cursor.settle();
        cursor
    }

    const fn is_exhausted(&self) -> bool {
        self.index == self.fragments.len()
    }

    fn read_exact(&mut self, out: &mut [u8]) -> Result<(), IggyError> {
        let mut filled = 0;
        while filled < out.len() {
            let available = self.available()?;
            let take = available.len().min(out.len() - filled);
            out[filled..filled + take].copy_from_slice(&available[..take]);
            filled += take;
            self.advance(take);
        }
        Ok(())
    }

    fn skip(&mut self, mut len: usize) -> Result<(), IggyError> {
        while len > 0 {
            let take = self.available()?.len().min(len);
            len -= take;
            self.advance(take);
        }
        Ok(())
    }

    /// Unread bytes of the current fragment; `Err` past the end of the stream.
    fn available(&self) -> Result<&'a [u8], IggyError> {
        self.fragments
            .get(self.index)
            .map(|fragment| &fragment.as_slice()[self.offset..])
            .ok_or(IggyError::InvalidCommand)
    }

    fn advance(&mut self, len: usize) {
        self.offset += len;
        self.settle();
    }

    /// Step past the current fragment once it is used up, and past empty ones.
    fn settle(&mut self) {
        while let Some(fragment) = self.fragments.get(self.index) {
            if self.offset < fragment.len() {
                break;
            }
            self.offset = 0;
            self.index += 1;
        }
    }
}

/// Build the `PolledMessages` reply body from the owning shard's poll
/// fragments, gathered into one buffer.
///
/// Fragments carry the stored batch records (a 256-byte batch header plus
/// `[48B header][payload][user_headers]` frames, deltas resolved against the
/// stamped bases) and are served to the client as they are - the reply's
/// message encoding IS the storage encoding. The one rewrite left is at-rest
/// decryption: stored sections are ciphertext, and this reply is the single
/// decrypt point, so encrypted records are rebuilt over the plaintext.
///
/// The binary transports reply through [`build_polled_messages_reply`], which
/// ships the fragments without gathering them; this builder serves the
/// decrypt path and the HTTP handler, which decodes the body into JSON.
///
/// Body layout: `[partition_id:4][current_offset:8][count:4][batch records...]`.
pub fn build_polled_messages_body(
    partition_id: u32,
    current_offset: u64,
    fragments: PollFragments,
    encryptor: Option<&EncryptorKind>,
) -> Result<Bytes, IggyError> {
    // Body head: [partition_id:4][current_offset:8][count:4]. `count` sits at
    // COUNT_OFFSET and is backpatched once the walk below knows it.
    const HEAD_LEN: usize = 16;
    const COUNT_OFFSET: usize = 12;
    // Batches may arrive split across fragments (rewritten batch header +
    // sliced blob); concatenate into one stream before walking records.
    let mut stream: Vec<u8> = Vec::new();
    for fragment in fragments {
        let frozen = fragment.into_frozen();
        stream.extend_from_slice(frozen.as_slice());
    }

    let mut body: Vec<u8> = Vec::with_capacity(HEAD_LEN + stream.len());
    body.extend_from_slice(&partition_id.to_le_bytes());
    body.extend_from_slice(&current_offset.to_le_bytes());
    body.extend_from_slice(&[0u8; 4]); // count placeholder, backpatched below
    let mut count: u32 = 0;
    let mut position = 0usize;
    while position < stream.len() {
        let batch = send_messages::BatchHeader::decode(&stream[position..])
            .map_err(|_| IggyError::InvalidCommand)?;
        let batch_end = position
            .checked_add(batch.total_size())
            .ok_or(IggyError::InvalidCommand)?;
        if batch_end > stream.len() {
            return Err(IggyError::InvalidCommand);
        }
        let record = &stream[position..batch_end];
        if let Some(encryptor) = encryptor {
            let decrypted = send_messages::decrypt_batch_record(record, encryptor)?;
            body.extend_from_slice(&decrypted);
        } else {
            body.extend_from_slice(record);
        }
        count = count
            .checked_add(batch.message_count)
            .ok_or(IggyError::InvalidCommand)?;
        position = batch_end;
    }

    body[COUNT_OFFSET..HEAD_LEN].copy_from_slice(&count.to_le_bytes());
    Ok(Bytes::from(body))
}

/// Build the `ConsumerOffsetResponse` reply body:
/// `[partition_id:4][current_offset:8][stored_offset:8]`.
pub fn build_consumer_offset_body(
    partition_id: u32,
    current_offset: u64,
    stored_offset: u64,
) -> Bytes {
    let mut body = Vec::with_capacity(20);
    body.extend_from_slice(&partition_id.to_le_bytes());
    body.extend_from_slice(&current_offset.to_le_bytes());
    body.extend_from_slice(&stored_offset.to_le_bytes());
    Bytes::from(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Operation, WireDecode};
    use iggy_common::Aes256GcmEncryptor;
    use server_common::send_messages::{
        BatchHeader, COMMAND_HEADER_SIZE, IggyMessage, IggyMessageHeader, IggyMessages,
        PREPARE_SPLIT_POINT, SendMessagesOwned, encrypt_batch_request, frozen_batch_header,
    };
    use server_common::sharding::IggyNamespace;

    fn pat_request_header() -> RoutedRequestHeader {
        let zeroed = [0u8; std::mem::size_of::<RoutedRequestHeader>()];
        let mut header = *bytemuck::checked::try_from_bytes::<RoutedRequestHeader>(&zeroed)
            .expect("zeroed bytes form a valid RoutedRequestHeader");
        header.command = Command::Request;
        header.operation = Operation::CreatePersonalAccessToken;
        header.client = 42;
        header.session = 7;
        header.request = 3;
        header
    }

    #[test]
    fn login_register_reply_carries_the_success_result_prefix() {
        // The other `build_result_framed_reply` caller. The SDK strips the result
        // section off every metadata reply, so a payload emitted without the prefix
        // loses its first four bytes to a phantom result count -- the decode break
        // the raw-PAT reply shipped once. Pin it on both callers, not just the one
        // that regressed.
        let mut header = pat_request_header();
        header.operation = Operation::Register;
        let reply = build_login_register_reply(&header, 42, 7, 9, 5);

        let header_len = std::mem::size_of::<ReplyHeader>();
        let body = &reply.as_slice()[header_len..reply.header().size as usize];
        assert_eq!(result_code(body), Some(0));

        let payload = LoginRegisterResponse::decode_from(&body[RESULT_COUNT_LEN..])
            .expect("login-register payload decodes past the result section");
        assert_eq!(payload.user_id, 5);
        assert_eq!(payload.session, 7);
    }

    /// A committed metadata reply whose body is the given result section
    /// (`[count][{index, result}]*`), as the commit path emits it.
    fn committed_reply(result_body: &[u8]) -> Message<GenericHeader> {
        let request_header = pat_request_header();
        build_reply_from_bytes(
            &request_header,
            42,
            7,
            9,
            &Bytes::copy_from_slice(result_body),
        )
        .into_generic()
    }

    #[test]
    fn raw_pat_reply_splices_token_into_a_committed_success() {
        let success = committed_reply(&0u32.to_le_bytes());
        let reply =
            build_raw_pat_reply(&pat_request_header(), success, Some("raw-token".to_owned()))
                .expect("splice succeeds");
        let header_len = std::mem::size_of::<ReplyHeader>();
        let body = &reply.as_slice()[header_len..reply.header().size as usize];
        // Framed like every committed metadata reply: the SDK reads the result
        // section first, then decodes the token payload past it.
        assert_eq!(result_code(body), Some(0));
        let response = RawPersonalAccessTokenResponse::decode_from(&body[RESULT_COUNT_LEN..])
            .expect("token body decodes");
        assert_eq!(response.token.as_str(), "raw-token");
    }

    #[test]
    fn raw_pat_reply_passes_a_committed_rejection_through_untouched() {
        let rejection_code =
            IggyError::PersonalAccessTokenAlreadyExists(String::new(), 0).as_code();
        let mut result_body = Vec::new();
        result_body.extend_from_slice(&1u32.to_le_bytes());
        result_body.extend_from_slice(&0u32.to_le_bytes());
        result_body.extend_from_slice(&rejection_code.to_le_bytes());
        let rejection = committed_reply(&result_body);
        let original = rejection.as_slice().to_vec();

        let reply = build_raw_pat_reply(
            &pat_request_header(),
            rejection,
            Some("raw-token".to_owned()),
        )
        .expect("pass-through succeeds");
        assert_eq!(
            reply.as_slice(),
            original.as_slice(),
            "a committed rejection must not be rewritten into a token reply"
        );
    }

    #[test]
    fn raw_pat_reply_without_a_token_passes_through() {
        let success = committed_reply(&0u32.to_le_bytes());
        let original = success.as_slice().to_vec();
        let reply =
            build_raw_pat_reply(&pat_request_header(), success, None).expect("pass-through");
        assert_eq!(reply.as_slice(), original.as_slice());
    }

    // Vectored `PolledMessages` replies against the flattening builder as the
    // byte-for-byte oracle.
    const POLL_PARTITION_ID: u32 = 9;
    const POLL_CURRENT_OFFSET: u64 = 1_234;
    const POLL_COMMIT: u64 = 17;

    fn poll_request_header() -> RoutedRequestHeader {
        pat_request_header()
    }

    /// A stored batch record over an opaque blob. Both builders decode only
    /// the 256-byte batch header, so the blob needs no message framing.
    fn batch_record(base_offset: u64, message_count: u32, blob: &[u8]) -> Frozen<MESSAGE_ALIGN> {
        let batch_length = u64::try_from(COMMAND_HEADER_SIZE + blob.len()).expect("fits u64");
        let mut header =
            BatchHeader::new(u64::from(POLL_PARTITION_ID), 5, batch_length, message_count);
        header.base_offset = base_offset;
        let mut bytes = vec![0u8; COMMAND_HEADER_SIZE + blob.len()];
        header.encode_into(&mut bytes[..COMMAND_HEADER_SIZE]);
        bytes[COMMAND_HEADER_SIZE..].copy_from_slice(blob);
        Owned::<MESSAGE_ALIGN>::copy_from_slice(&bytes).into()
    }

    /// The wire bytes the flattening builder ships for `fragments`.
    fn flattened_reply(fragments: PollFragments, encryptor: Option<&EncryptorKind>) -> Vec<u8> {
        let header = poll_request_header();
        let body = build_polled_messages_body(
            POLL_PARTITION_ID,
            POLL_CURRENT_OFFSET,
            fragments,
            encryptor,
        )
        .expect("flattening builder accepts the fragments");
        build_reply_from_bytes(&header, header.client, header.session, POLL_COMMIT, &body)
            .into_generic()
            .into_frozen()
            .as_slice()
            .to_vec()
    }

    fn vectored_reply(
        fragments: PollFragments,
        encryptor: Option<&EncryptorKind>,
    ) -> Result<BusMessage, IggyError> {
        build_polled_messages_reply(
            &poll_request_header(),
            POLL_COMMIT,
            POLL_PARTITION_ID,
            POLL_CURRENT_OFFSET,
            fragments,
            encryptor,
        )
    }

    /// The vectored reply must be byte-identical to the flattened one and
    /// ship exactly `fragment_count` buffers.
    fn assert_vectored_matches_flattened(fragments: PollFragments, fragment_count: usize) {
        let expected = flattened_reply(fragments.clone(), None);
        let reply =
            vectored_reply(fragments, None).expect("vectored builder accepts the fragments");
        assert_eq!(reply.fragments().len(), fragment_count);
        assert_eq!(reply.total_len(), expected.len());
        assert_eq!(reply.into_contiguous().as_slice(), expected.as_slice());
    }

    fn polled_count(reply: &[u8]) -> u32 {
        let count_at = std::mem::size_of::<ReplyHeader>() + 12;
        u32::from_le_bytes(reply[count_at..count_at + 4].try_into().expect("4 bytes"))
    }

    #[test]
    fn polled_reply_single_fragment_matches_flattened_builder() {
        let record = batch_record(0, 3, &[0xAB; 100]);
        let fragments = PollFragments::from_iter([Fragment::whole(record)]);
        assert_vectored_matches_flattened(fragments.clone(), 2);

        let reply = vectored_reply(fragments, None)
            .expect("reply")
            .into_contiguous();
        let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
            &reply.as_slice()[..std::mem::size_of::<ReplyHeader>()],
        )
        .expect("reply header decodes");
        assert_eq!(header.size as usize, reply.len());
        assert_eq!(header.client, 42);
        assert_eq!(header.op, 7);
        assert_eq!(header.commit, POLL_COMMIT);
        assert_eq!(polled_count(reply.as_slice()), 3);
    }

    #[test]
    fn polled_reply_split_batch_matches_flattened_builder() {
        // The journal slices a partially selected batch into a rewritten header
        // plus a blob slice, exactly how `push_selected_batch_fragments` does.
        let source = batch_record(10, 4, &[0x11; 400]);
        let (start, end) = (100, 300);
        let batch_length = u64::try_from(COMMAND_HEADER_SIZE + (end - start)).expect("fits u64");
        let mut rewritten = BatchHeader::new(u64::from(POLL_PARTITION_ID), 5, batch_length, 2);
        rewritten.base_offset = 10;
        let fragments = PollFragments::from_iter([
            Fragment::whole(frozen_batch_header(&rewritten)),
            Fragment::slice(
                source,
                COMMAND_HEADER_SIZE + start,
                COMMAND_HEADER_SIZE + end,
            ),
        ]);
        assert_vectored_matches_flattened(fragments.clone(), 3);
        let reply = vectored_reply(fragments, None)
            .expect("reply")
            .into_contiguous();
        assert_eq!(polled_count(reply.as_slice()), 2);
    }

    #[test]
    fn polled_reply_multiple_batches_counts_every_header() {
        let first = batch_record(0, 1, &[0x01; 50]);
        let second = batch_record(1, 4, &[0x02; 700]);
        let third = batch_record(5, 7, &[0x03; 20]);
        // `second` arrives cut mid-header so the count walk has to read a batch
        // header spanning two fragments.
        let fragments = PollFragments::from_iter([
            Fragment::whole(first),
            Fragment::slice(second.clone(), 0, 100),
            Fragment::slice(second.clone(), 100, second.len()),
            Fragment::whole(third),
        ]);
        assert_vectored_matches_flattened(fragments.clone(), 5);
        let reply = vectored_reply(fragments, None)
            .expect("reply")
            .into_contiguous();
        assert_eq!(polled_count(reply.as_slice()), 12);
    }

    #[test]
    fn polled_reply_empty_poll_is_the_head_alone() {
        assert_vectored_matches_flattened(PollFragments::new(), 1);
        let reply = vectored_reply(PollFragments::new(), None)
            .expect("reply")
            .into_contiguous();
        assert_eq!(
            reply.len(),
            std::mem::size_of::<ReplyHeader>() + POLLED_HEAD_LEN
        );
        assert_eq!(polled_count(reply.as_slice()), 0);
    }

    #[test]
    fn polled_reply_rejects_a_truncated_record() {
        let record = batch_record(0, 3, &[0xAB; 100]);
        let truncated =
            PollFragments::from_iter([Fragment::slice(record, 0, COMMAND_HEADER_SIZE + 99)]);
        assert!(matches!(
            build_polled_messages_body(
                POLL_PARTITION_ID,
                POLL_CURRENT_OFFSET,
                truncated.clone(),
                None
            ),
            Err(IggyError::InvalidCommand)
        ));
        assert!(matches!(
            vectored_reply(truncated, None),
            Err(IggyError::InvalidCommand)
        ));
    }

    /// A stored record encrypted the way the primary encrypts at ingestion.
    fn encrypted_record(encryptor: &EncryptorKind) -> Frozen<MESSAGE_ALIGN> {
        let namespace = IggyNamespace::new(1, 1, 3);
        let mut messages = IggyMessages::with_capacity(2);
        for (id, payload) in [(7u128, &b"first-payload"[..]), (8, &b"second-payload"[..])] {
            messages.push(IggyMessage {
                header: IggyMessageHeader {
                    id,
                    origin_timestamp: 1_000,
                    ..Default::default()
                },
                payload: Bytes::copy_from_slice(payload),
                user_headers: None,
            });
        }
        let owned = SendMessagesOwned::from_messages(namespace, &messages).expect("build batch");
        let header_size = std::mem::size_of::<RoutedRequestHeader>();
        let total = header_size + owned.header.total_size();
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
        bytes[PREPARE_SPLIT_POINT..].copy_from_slice(&owned.blob);
        let canonical = Message::try_from(buffer).expect("request message is valid");
        let encrypted = encrypt_batch_request(canonical, encryptor).expect("encrypt batch");
        let record = &encrypted.as_slice()[header_size..encrypted.header().size as usize];
        Owned::<MESSAGE_ALIGN>::copy_from_slice(record).into()
    }

    #[test]
    fn polled_reply_encrypted_records_take_the_flattening_path() {
        let encryptor =
            EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::new(&[7u8; 32]).expect("valid 32B key"));
        let fragments = PollFragments::from_iter([Fragment::whole(encrypted_record(&encryptor))]);
        let expected = flattened_reply(fragments.clone(), Some(&encryptor));
        let reply = vectored_reply(fragments, Some(&encryptor)).expect("decrypting reply");
        assert_eq!(reply.fragments().len(), 1);
        assert_eq!(reply.into_contiguous().as_slice(), expected.as_slice());
        assert_eq!(polled_count(&expected), 2);
    }
}
