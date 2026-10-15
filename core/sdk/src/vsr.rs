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

use crate::session::ConsensusSession;
use crate::session_owner::SessionOwner;
use bytes::Bytes;
use iggy_binary_protocol::codes::{LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE};
use iggy_binary_protocol::consensus::{
    Command, EvictionHeader, EvictionReason, GenericHeader, HEADER_SIZE, Operation, ReplyHeader,
    RequestHeader, operation_for_code, read_size_field, result_code, result_section_len,
};
use iggy_common::{IggyError, eviction_reason_to_error};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Each retry of a [`IggyError::LifecycleBusy`] refusal is a new request that commits another
/// refusal, so the pause between retries doubles up to this cap.
pub(crate) const LIFECYCLE_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(1);

/// The id of every request without a session. A bound id is never 0, and a
/// link sends one unbound request at a time, so no reply to an unbound
/// request can answer another request.
const UNBOUND_REQUEST_ID: u64 = 0;

/// A later refusal cannot establish whether an earlier attempt committed.
#[derive(Default)]
pub(crate) struct RetryOutcome {
    uncertain: bool,
}

impl RetryOutcome {
    pub(crate) fn for_reconnect(code: u32, error: &IggyError) -> Self {
        Self {
            uncertain: !replay_after_session_reset_is_safe(code, error),
        }
    }

    pub(crate) fn observe(&mut self, error: IggyError) -> IggyError {
        if matches!(error, IggyError::TransientNotCommitted) {
            self.uncertain = true;
        }
        if self.uncertain
            && matches!(
                error,
                IggyError::TransientNotAccepted
                    | IggyError::HistoryUnavailable
                    | IggyError::Unauthorized
                    | IggyError::Unauthenticated
                    | IggyError::StaleClient
                    | IggyError::NotConnected
                    | IggyError::CannotEstablishConnection
            )
        {
            IggyError::TransientNotCommitted
        } else {
            error
        }
    }

    /// [`Self::observe`] for an attempt that failed on the link. A replay
    /// follows an attempt that reached the server, so a replay that never left
    /// still leaves the request in flight.
    pub(crate) fn observe_link_error(&mut self, error: IggyError, replay: bool) -> IggyError {
        if replay && matches!(error, IggyError::NotConnected) {
            return IggyError::Disconnected;
        }
        self.observe(error)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RetainedRequest {
    pub(crate) header: Option<RequestHeader>,
    pub(crate) context: iggy_binary_protocol::primitives::partition_history::PartitionContext,
}

impl RetainedRequest {
    pub(crate) const fn new(
        context: iggy_binary_protocol::primitives::partition_history::PartitionContext,
    ) -> Self {
        Self {
            header: None,
            context,
        }
    }

    pub(crate) fn encode(
        &mut self,
        session: &mut ConsensusSession,
        code: u32,
        payload: &Bytes,
    ) -> Result<RequestHeader, IggyError> {
        if let Some(header) = self.header {
            validate_retained_header(&header, session)?;
            return Ok(header);
        }
        let mut header = encode_request_header(session, code, payload)?.0;
        self.context.stamp(&mut header);
        self.header = Some(header);
        Ok(header)
    }
}

pub(crate) fn encode_request_header(
    session: &mut ConsensusSession,
    code: u32,
    payload: &Bytes,
) -> Result<(RequestHeader, usize), IggyError> {
    let (operation, request_id, session_id) = match code {
        LOGIN_REGISTER_CODE | LOGIN_REGISTER_WITH_PAT_CODE => {
            (Operation::Register, session.register_request_id(), 0)
        }
        _ => {
            let operation = operation_for_code(code);
            // NonReplicated ops (ping, reads) bypass server-side dedup, and
            // `ClientTable` admits any id above its watermark
            // (client_table.rs: "There is no `RequestGap`"), so a read can
            // spend an id. A bound session spends one per read, so a reply
            // names exactly one request on a link that matches by id.
            //
            // Unbound, they are sessionless on the server (routed by
            // transport id; protected codes are auth-gated server-side), so
            // they carry session 0, which keeps the legacy "ping works
            // without auth" contract. Replicated ops still require a bound
            // session for dedup and fail fast here.
            if operation == Operation::NonReplicated {
                match session.session() {
                    Some(session_id) => (operation, session.next_request_id()?, session_id),
                    None => (operation, UNBOUND_REQUEST_ID, 0),
                }
            } else {
                // Partition dedup needs each new write to carry a distinct id.
                // The metadata watermark tolerates the resulting gaps
                // (`client_table.rs`: "There is no `RequestGap`").
                let session_id = session.session().ok_or(IggyError::Unauthenticated)?;
                (operation, session.next_request_id()?, session_id)
            }
        }
    };
    // The header rules (checksum stamp, size, reserved code) live in the
    // protocol crate so every client and the cross-SDK fixtures share them;
    // only the session identity is decided here.
    let header =
        RequestHeader::for_request(code, session.client_id(), request_id, session_id, payload)
            .map_err(|_| IggyError::InvalidConfiguration)?;
    debug_assert_eq!(header.operation, operation);
    Ok((header, header.size as usize))
}

/// Whether replaying `code` after reconnecting with a new session cannot
/// apply it twice.
pub(crate) fn replay_after_session_reset_is_safe(code: u32, error: &IggyError) -> bool {
    matches!(code, LOGIN_REGISTER_CODE | LOGIN_REGISTER_WITH_PAT_CODE)
        || matches!(
            error,
            IggyError::NotConnected
                | IggyError::TransientNotAccepted
                | IggyError::CannotEstablishConnection
                | IggyError::Unauthenticated
        )
        || matches!(
            operation_for_code(code),
            Operation::NonReplicated | Operation::Logout
        )
}

/// A transport failure or a lagging replica can be retried on another endpoint.
pub(crate) fn resume_error_is_retryable(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::Disconnected
            | IggyError::EmptyResponse
            | IggyError::NotConnected
            | IggyError::CannotEstablishConnection
            | IggyError::TcpError
            | IggyError::QuicError
            | IggyError::ConnectionClosed
            | IggyError::WebSocketSendError
            | IggyError::WebSocketReceiveError
            | IggyError::TransientNotAccepted
    )
}

/// A resumed session can replay its exact request. A replacement session can
/// replay only an operation whose outcome proves it was never admitted.
pub(crate) fn retain_replay_header(
    header: &mut RetainedRequest,
    session_owner: &SessionOwner,
    code: u32,
    error: &IggyError,
) -> Result<(), IggyError> {
    let session = session_owner.session()?;
    if header.header.as_ref().is_some_and(|header| {
        header.client == session.client_id()
            && (header.session == session.session().unwrap_or(0)
                || header.operation == Operation::Register)
    }) {
        return Ok(());
    }
    if replay_after_session_reset_is_safe(code, error) {
        header.header = None;
        Ok(())
    } else {
        Err(IggyError::TransientNotCommitted)
    }
}

pub(crate) fn validate_retained_header(
    header: &RequestHeader,
    session: &ConsensusSession,
) -> Result<(), IggyError> {
    if header.client != session.client_id()
        || (header.operation != Operation::Register
            && header.session != session.session().unwrap_or(0))
    {
        return Err(IggyError::TransientNotCommitted);
    }
    Ok(())
}

pub(crate) fn response_size(header: &[u8]) -> Result<usize, IggyError> {
    let size = read_size_field(header).ok_or(IggyError::InvalidCommand)? as usize;
    if size < HEADER_SIZE {
        return Err(IggyError::InvalidCommand);
    }
    Ok(size)
}

/// Track metadata acknowledgments without adding coordinator traffic to warm polls.
pub(crate) fn observe_metadata_reply(watermark: &AtomicU64, header: &[u8; HEADER_SIZE]) {
    if peek_command(header) != Command::Reply {
        return;
    }
    let Ok(operation) = read_operation(header) else {
        return;
    };
    if operation == Operation::NonReplicated || operation.is_partition() {
        return;
    }
    const COMMIT_OFFSET: usize = std::mem::offset_of!(ReplyHeader, commit);
    watermark.fetch_max(read_u64_field(header, COMMIT_OFFSET), Ordering::Release);
}

/// The request a reply answers, read by wire offset like [`read_operation`].
pub(crate) fn reply_request_id(header_bytes: &[u8; HEADER_SIZE]) -> u64 {
    const REQUEST_OFFSET: usize = std::mem::offset_of!(ReplyHeader, request);
    read_u64_field(header_bytes, REQUEST_OFFSET)
}

/// A metadata reply header that carries `commit`.
#[cfg(test)]
pub(crate) fn metadata_reply_header(commit: u64) -> [u8; HEADER_SIZE] {
    let reply = ReplyHeader {
        command: Command::Reply,
        operation: Operation::DeleteTopic,
        size: u32::try_from(HEADER_SIZE).unwrap(),
        commit,
        ..Default::default()
    };
    let mut header = [0; HEADER_SIZE];
    header.copy_from_slice(bytemuck::bytes_of(&reply));
    header
}

/// Decode a reply whose header and body arrive as separate buffers.
///
/// Also surfaces session-terminal `Command::Eviction` frames as typed
/// errors: callers waiting on a Reply for an unbound session would otherwise
/// hit a read-timeout because the SDK previously only accepted
/// `Command::Reply`. Returns the body slice on a normal Reply, or maps the
/// eviction reason to an `IggyError` so the request fails fast.
pub(crate) fn decode_reply(
    header_bytes: &[u8; HEADER_SIZE],
    body: Bytes,
) -> Result<Bytes, IggyError> {
    match peek_command(header_bytes) {
        Command::Eviction => Err(decode_eviction(header_bytes)),
        Command::Reply => {
            let expected_body = response_size(header_bytes)? - HEADER_SIZE;
            if body.len() < expected_body {
                return Err(IggyError::InvalidCommand);
            }
            if let Some(error) = read_reply_status(header_bytes) {
                return Err(error);
            }
            let operation = read_operation(header_bytes)?;
            split_metadata_result(operation, body.slice(..expected_body))
        }
        _ => Err(IggyError::InvalidCommand),
    }
}

/// Peek `ReplyHeader.status`, the pre-commit deny channel shared by every deny
/// frame: dispatch-time denies (authorization and a malformed user-password
/// body) via `build_deny_reply`, and partition-primary admission rejects via
/// `consensus::build_deny_reply_from_request`. Read before any body decode:
/// a nonzero status maps to its [`IggyError`] and fails the request, an empty
/// or partial body notwithstanding. `None` (status 0) lets the reply flow to
/// the body/result-section decode. Read by wire offset for the same
/// misalignment reason as [`read_operation`]; a committed metadata reply stamps
/// 0 here and carries its business rejection in the result section (see
/// `split_metadata_result`).
fn read_reply_status(header_bytes: &[u8; HEADER_SIZE]) -> Option<IggyError> {
    const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&header_bytes[STATUS_OFFSET..STATUS_OFFSET + 4]);
    let status = u32::from_le_bytes(bytes);
    (status != 0).then(|| IggyError::from_code(status))
}

/// [`decode_reply`] as the transports grade a refusal. `NotPrimary` takes the
/// `TransientNotAccepted` path: neither was admitted, and the transports find
/// the primary by moving to another replica either way.
pub(crate) fn decode_response_split(
    header_bytes: &[u8; HEADER_SIZE],
    body: Bytes,
) -> Result<Bytes, IggyError> {
    decode_reply(header_bytes, body).map_err(|error| match error {
        IggyError::NotPrimary => IggyError::TransientNotAccepted,
        error => error,
    })
}

/// Read the [`Operation`] discriminant from a reply header by wire offset.
/// Mirrors [`peek_command`]: a full `ReplyHeader` struct cast is avoided
/// because the response buffers are not 16-aligned, so the leading `u128`
/// fields would make `try_from_bytes::<ReplyHeader>` fail on an unlucky
/// placement. One validated byte is enough to drive `split_metadata_result`.
fn read_operation(header_bytes: &[u8; HEADER_SIZE]) -> Result<Operation, IggyError> {
    const OPERATION_OFFSET: usize = std::mem::offset_of!(ReplyHeader, operation);
    bytemuck::checked::try_from_bytes::<Operation>(
        &header_bytes[OPERATION_OFFSET..=OPERATION_OFFSET],
    )
    .copied()
    .map_err(|_| IggyError::InvalidCommand)
}

/// Strip the result section from metadata, consumer-offset write, and
/// non-empty Register replies. Success carries `count == 0` then the payload;
/// a business or transient rejection carries an error code in a result entry.
/// A result section can report a pre-commit rejection, so its presence alone
/// does not prove commitment.
///
/// Reads, SendMessages, and Logout pass through untouched. An empty Register
/// body passes through to fail the typed login decode. A malformed result
/// section maps to `InvalidCommand` rather than decoding a rejection as `Ok`.
fn split_metadata_result(operation: Operation, body: Bytes) -> Result<Bytes, IggyError> {
    // Register (login/register) replies are result-framed too, so a transient
    // login decodes to `TransientNotCommitted` and the SDK replays it. The one
    // exception is a terminal failure, which ships an empty body (no result
    // section) and is passed through to fail the typed `LoginRegisterResponse`
    // decode. `Operation::is_result_framed` is the shared source of truth with
    // the server-side encode sites; the Register empty-body-is-terminal nuance
    // is the one SDK-side addition. Other reads, data-plane ops, and Logout
    // carry no result section and pass through untouched.
    let result_framed =
        operation.is_result_framed() || (operation == Operation::Register && !body.is_empty());
    if !result_framed {
        return Ok(body);
    }
    match result_code(&body) {
        Some(0) => {
            let payload_start = result_section_len(&body).ok_or(IggyError::InvalidCommand)?;
            Ok(body.slice(payload_start..))
        }
        Some(code) => Err(IggyError::from_code(code)),
        None => Err(IggyError::InvalidCommand),
    }
}

/// `Command` lives at a fixed offset shared by every consensus header
/// (Reply, Eviction, Prepare, ...), so a byte read is enough to discriminate
/// the frame.
pub(crate) fn peek_command(header_bytes: &[u8; HEADER_SIZE]) -> Command {
    const COMMAND_OFFSET: usize = std::mem::offset_of!(GenericHeader, command);
    match header_bytes[COMMAND_OFFSET] {
        x if x == Command::Reply as u8 => Command::Reply,
        x if x == Command::Eviction as u8 => Command::Eviction,
        _ => Command::Reserved,
    }
}

/// Map a session-terminal Eviction frame to a typed error. Fields are read
/// by wire offset instead of an `EvictionHeader` struct cast: the response
/// buffers are not 16-aligned (the header holds `u128`s, so the cast would
/// fail on an unlucky buffer placement), and reading raw lets the SDK apply
/// the same window sanity check as `EvictionHeader::validate` rather than
/// trusting the remote frame.
pub(crate) fn decode_eviction(header_bytes: &[u8; HEADER_SIZE]) -> IggyError {
    const REASON_OFFSET: usize = std::mem::offset_of!(EvictionHeader, reason);
    const VERSION_OFFSET: usize = std::mem::offset_of!(EvictionHeader, server_protocol_version);
    const VERSION_MIN_OFFSET: usize =
        std::mem::offset_of!(EvictionHeader, server_protocol_version_min);

    let Ok(&reason) = bytemuck::checked::try_from_bytes::<EvictionReason>(
        &header_bytes[REASON_OFFSET..=REASON_OFFSET],
    ) else {
        return IggyError::Unauthenticated;
    };
    eviction_reason_to_error(
        reason,
        read_window_field(header_bytes, VERSION_OFFSET),
        read_window_field(header_bytes, VERSION_MIN_OFFSET),
    )
}

fn read_window_field(header_bytes: &[u8; HEADER_SIZE], offset: usize) -> u32 {
    let mut value = [0u8; 4];
    value.copy_from_slice(&header_bytes[offset..offset + 4]);
    u32::from_le_bytes(value)
}

fn read_u64_field(header_bytes: &[u8; HEADER_SIZE], offset: usize) -> u64 {
    let mut value = [0; size_of::<u64>()];
    value.copy_from_slice(&header_bytes[offset..offset + size_of::<u64>()]);
    u64::from_le_bytes(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::ConsensusSession;
    use bytes::{BufMut, BytesMut};
    use iggy_binary_protocol::codes::{
        CREATE_STREAM_CODE, GET_STREAM_CODE, LOGOUT_USER_CODE, PING_CODE, SEND_MESSAGES_CODE,
    };
    use iggy_binary_protocol::consensus::NON_REPLICATED_CODE_RANGE;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::users::LoginRegisterRequest;
    use iggy_binary_protocol::version::IGGY_PROTOCOL_VERSION;
    use iggy_binary_protocol::{ClientVersionInfo, WireEncode, WireName, WireOptions};
    use iggy_common::calculate_checksum;
    use secrecy::SecretString;

    #[test]
    fn later_refusals_preserve_an_earlier_uncertain_outcome() {
        for refusal in [IggyError::Unauthorized, IggyError::HistoryUnavailable] {
            for initial in [IggyError::TransientNotCommitted, IggyError::Disconnected] {
                let mut outcome = RetryOutcome::for_reconnect(SEND_MESSAGES_CODE, &initial);
                assert_eq!(
                    outcome.observe(refusal.clone()),
                    IggyError::TransientNotCommitted
                );
            }
            assert_eq!(RetryOutcome::default().observe(refusal.clone()), refusal);
        }
    }

    #[test]
    fn given_a_replay_when_the_link_fails_should_keep_the_outcome_unknown() {
        assert_eq!(
            RetryOutcome::default().observe_link_error(IggyError::NotConnected, false),
            IggyError::NotConnected
        );
        assert_eq!(
            RetryOutcome::default().observe_link_error(IggyError::NotConnected, true),
            IggyError::Disconnected
        );
        let mut uncertain = RetryOutcome::default();
        uncertain.observe(IggyError::TransientNotCommitted);
        assert_eq!(
            uncertain.observe_link_error(IggyError::Unauthenticated, true),
            IggyError::TransientNotCommitted
        );
    }

    #[test]
    fn repeated_login_preserves_the_bound_session_and_request_sequence() {
        let request = LoginRegisterRequest {
            bind_secret: iggy_binary_protocol::requests::users::login_register::BindSecret::new(
                Box::new([0x5a; 32]),
            ),
            version_info: ClientVersionInfo {
                protocol_version: IGGY_PROTOCOL_VERSION,
                sdk_name: WireName::new("rust-sdk").unwrap(),
                sdk_version: WireName::new("1.0.0").unwrap(),
            },
            username: WireName::new("admin").unwrap(),
            password: SecretString::from("secret"),
            client_context: None,
        };

        let mut session = ConsensusSession::with_client_id(7);
        RetainedRequest::default()
            .encode(&mut session, LOGIN_REGISTER_CODE, &request.to_bytes())
            .unwrap();
        session.bind(42).unwrap();

        let next_request = session.next_request_id().unwrap();
        let header = RetainedRequest::default()
            .encode(&mut session, LOGIN_REGISTER_CODE, &request.to_bytes())
            .unwrap();
        assert_eq!(header.operation, Operation::Register);
        assert_eq!(header.request, 0);
        assert_eq!(header.session, 0);
        assert!(session.is_bound());
        assert_eq!(session.session(), Some(42));
        assert_eq!(session.next_request_id().unwrap(), next_request + 1);
    }

    #[test]
    fn eviction_incompatible_protocol_decodes_to_typed_error() {
        use iggy_binary_protocol::version::IGGY_PROTOCOL_VERSION_MIN;

        // Header lands at a guaranteed-misaligned address (aligned start +1):
        // eviction decode reads fields by offset and must not care.
        #[repr(C, align(16))]
        struct Misaligner([u8; HEADER_SIZE + 1]);

        let header = EvictionHeader::incompatible_protocol(
            0,
            0,
            0,
            0xCAFE,
            IGGY_PROTOCOL_VERSION,
            IGGY_PROTOCOL_VERSION_MIN,
        );
        let mut raw = Misaligner([0; HEADER_SIZE + 1]);
        raw.0[1..].copy_from_slice(bytemuck::bytes_of(&header));
        let shifted: &[u8; HEADER_SIZE] = raw.0[1..].try_into().unwrap();

        let result = decode_response_split(shifted, Bytes::new());
        assert!(matches!(
            result,
            Err(IggyError::IncompatibleProtocolVersion(client, min, max))
                if client == IGGY_PROTOCOL_VERSION
                    && min == IGGY_PROTOCOL_VERSION_MIN
                    && max == IGGY_PROTOCOL_VERSION
        ));
    }

    #[test]
    fn eviction_with_invalid_window_degrades_to_unauthenticated() {
        for (server_max, server_min) in [(1, 0), (1, 2)] {
            let mut header = EvictionHeader::incompatible_protocol(0, 0, 0, 0xCAFE, 1, 1);
            header.server_protocol_version = server_max;
            header.server_protocol_version_min = server_min;
            let mut buf = [0u8; HEADER_SIZE];
            buf.copy_from_slice(bytemuck::bytes_of(&header));

            let result = decode_response_split(&buf, Bytes::new());
            assert!(
                matches!(result, Err(IggyError::Unauthenticated)),
                "window [{server_min}, {server_max}] must not surface as typed error"
            );
        }
    }

    #[test]
    fn reply_with_nonzero_status_surfaces_as_typed_error() {
        for error in [IggyError::Unauthorized, IggyError::RequestTooOld] {
            let header = ReplyHeader {
                command: Command::Reply,
                operation: Operation::SendMessages,
                size: HEADER_SIZE as u32,
                status: error.as_code(),
                ..Default::default()
            };
            let mut buffer = [0u8; HEADER_SIZE];
            buffer.copy_from_slice(bytemuck::bytes_of(&header));
            assert!(
                matches!(decode_response_split(&buffer, Bytes::new()), Err(result) if result == error),
                "an empty denial body must preserve {error}"
            );
        }
    }

    #[test]
    fn given_not_primary_refusal_when_decoded_should_keep_not_primary() {
        let not_primary = IggyError::NotPrimary.as_code();
        let header = ReplyHeader {
            command: Command::Reply,
            operation: Operation::SendMessages,
            size: HEADER_SIZE as u32,
            status: not_primary,
            ..Default::default()
        };
        let mut buffer = [0u8; HEADER_SIZE];
        buffer.copy_from_slice(bytemuck::bytes_of(&header));
        assert_eq!(
            decode_reply(&buffer, Bytes::new()).unwrap_err(),
            IggyError::NotPrimary
        );
        assert_eq!(
            split_metadata_result(Operation::CreateStream, rejection_body(not_primary))
                .unwrap_err(),
            IggyError::NotPrimary
        );
        assert_eq!(
            decode_response_split(&buffer, Bytes::new()).unwrap_err(),
            IggyError::TransientNotAccepted
        );
    }

    #[test]
    fn reply_with_zero_status_passes_body_through() {
        // status 0 is the ok channel: a non-metadata reply returns its body.
        let header = ReplyHeader {
            command: Command::Reply,
            operation: Operation::NonReplicated,
            size: (HEADER_SIZE + 3) as u32,
            ..Default::default()
        };
        let mut buf = [0u8; HEADER_SIZE];
        buf.copy_from_slice(bytemuck::bytes_of(&header));
        let out = decode_response_split(&buf, Bytes::from_static(b"abc")).unwrap();
        assert_eq!(&out[..], b"abc");
    }

    #[test]
    fn replicated_request_increments_request_counter() {
        let mut session = ConsensusSession::with_client_id(42);
        let _ = session.register_request_id();
        session.bind(99).unwrap();
        let payload = CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
        }
        .to_bytes();

        let first = RetainedRequest::default()
            .encode(&mut session, CREATE_STREAM_CODE, &payload)
            .unwrap();
        let second = RetainedRequest::default()
            .encode(&mut session, CREATE_STREAM_CODE, &payload)
            .unwrap();

        assert_eq!(first.request, 1);
        assert_eq!(second.request, 2);
        assert_eq!(second.session, 99);
    }

    #[test]
    fn request_checksum_is_stamped_for_metadata_but_not_partition_operations() {
        // Metadata dedup compares the stamp against cached replies. Partition
        // dedup checks request ids without stamps; NonReplicated bypasses dedup.
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();
        let payload = Bytes::from_static(b"payload");

        let metadata = RetainedRequest::default()
            .encode(&mut session, CREATE_STREAM_CODE, &payload)
            .unwrap();
        assert_eq!(
            metadata.request_checksum,
            u128::from(calculate_checksum(&payload)),
        );

        let partition = RetainedRequest::default()
            .encode(&mut session, SEND_MESSAGES_CODE, &payload)
            .unwrap();
        assert_eq!(partition.request_checksum, 0);

        let ping = RetainedRequest::default()
            .encode(&mut session, PING_CODE, &Bytes::new())
            .unwrap();
        assert_eq!(ping.request_checksum, 0);
    }

    #[test]
    fn partition_request_consumes_the_request_counter() {
        // Dedup identity requires each send to carry a distinct id, so
        // partition ops advance the counter exactly like metadata ops and
        // the two planes interleave on one sequence.
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();
        let payload = Bytes::from_static(b"batch");

        let first = RetainedRequest::default()
            .encode(&mut session, SEND_MESSAGES_CODE, &payload)
            .unwrap();
        let second = RetainedRequest::default()
            .encode(&mut session, SEND_MESSAGES_CODE, &payload)
            .unwrap();
        assert_eq!(first.request, 1);
        assert_eq!(second.request, 2);

        let metadata_payload = CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
        }
        .to_bytes();
        let metadata = RetainedRequest::default()
            .encode(&mut session, CREATE_STREAM_CODE, &metadata_payload)
            .unwrap();
        assert_eq!(metadata.request, 3);
    }

    #[test]
    fn ping_uses_non_replicated_operation() {
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();
        let header = RetainedRequest::default()
            .encode(&mut session, PING_CODE, &Bytes::new())
            .unwrap();

        assert_eq!(header.operation, Operation::NonReplicated);
        assert_eq!(
            u32::from_le_bytes(
                header.reserved[NON_REPLICATED_CODE_RANGE]
                    .try_into()
                    .unwrap()
            ),
            PING_CODE
        );
        assert_eq!(header.session, 99);
    }

    #[test]
    fn given_a_bound_session_when_encoding_two_reads_should_take_two_new_request_ids() {
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();

        let (first, _) =
            encode_request_header(&mut session, GET_STREAM_CODE, &Bytes::new()).unwrap();
        let (second, _) =
            encode_request_header(&mut session, GET_STREAM_CODE, &Bytes::new()).unwrap();

        assert_eq!((first.request, second.request), (1, 2));
        assert_eq!(session.current_request_id(), 3);
    }

    #[test]
    fn given_an_unbound_session_when_pinging_should_send_request_0_and_session_0() {
        let mut session = ConsensusSession::with_client_id(42);

        let (header, _) = encode_request_header(&mut session, PING_CODE, &Bytes::new()).unwrap();
        session.bind(99).unwrap();
        let first_bound = session.next_request_id().unwrap();

        assert_eq!((header.request, header.session), (0, 0));
        assert_ne!(header.request, first_bound);
    }

    #[test]
    fn logout_uses_replicated_logout_operation() {
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();
        let header = RetainedRequest::default()
            .encode(&mut session, LOGOUT_USER_CODE, &Bytes::new())
            .unwrap();

        assert_eq!(header.operation, Operation::Logout);
        assert_eq!(header.request, 1);
        assert_eq!(header.session, 99);
    }

    #[test]
    fn read_only_request_uses_non_replicated_operation() {
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();
        let header = RetainedRequest::default()
            .encode(&mut session, GET_STREAM_CODE, &Bytes::new())
            .unwrap();

        assert_eq!(header.operation, Operation::NonReplicated);
        assert_eq!(
            u32::from_le_bytes(
                header.reserved[NON_REPLICATED_CODE_RANGE]
                    .try_into()
                    .unwrap()
            ),
            GET_STREAM_CODE
        );
        assert_eq!(header.session, 99);
    }

    #[test]
    fn unknown_code_encodes_as_non_replicated_and_carries_the_code() {
        // An extended server may implement codes this SDK build has never heard
        // of. The registry is not a capability list, so the request must reach
        // the server rather than fail at encode time.
        const UNKNOWN_CODE: u32 = 60_000;
        assert!(
            iggy_binary_protocol::dispatch::lookup_command(UNKNOWN_CODE).is_none(),
            "test needs a code absent from COMMAND_TABLE"
        );

        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();
        let header = RetainedRequest::default()
            .encode(&mut session, UNKNOWN_CODE, &Bytes::new())
            .unwrap();

        assert_eq!(header.operation, Operation::NonReplicated);
        assert_eq!(
            u32::from_le_bytes(
                header.reserved[NON_REPLICATED_CODE_RANGE]
                    .try_into()
                    .unwrap()
            ),
            UNKNOWN_CODE
        );
    }

    #[test]
    fn no_replicated_command_ever_resolves_to_non_replicated() {
        // The safety asymmetry that makes forwarding unknown codes acceptable:
        // an unknown code is the server's business, but a *known* replicated
        // command sent as non-replicated would apply on one node only and
        // silently diverge the replicas. Swept over the whole registry so a
        // future entry cannot regress it.
        for meta in iggy_binary_protocol::dispatch::COMMAND_TABLE {
            if !meta.is_replicated() {
                continue;
            }
            assert_ne!(
                operation_for_code(meta.code),
                Operation::NonReplicated,
                "replicated command {} ({}) must never encode as NonReplicated",
                meta.name,
                meta.code
            );
        }
    }

    #[test]
    fn metadata_success_reply_strips_result_section_and_returns_payload() {
        let mut body = BytesMut::new();
        body.put_u32_le(0); // count = 0 (success)
        body.put_slice(b"payload");
        let payload = split_metadata_result(Operation::CreateStream, body.freeze()).unwrap();
        assert_eq!(&payload[..], b"payload");
    }

    #[test]
    fn metadata_rejection_reply_maps_committed_code_to_iggy_error() {
        let mut body = BytesMut::new();
        body.put_u32_le(1); // count = 1 (one rejection entry)
        body.put_u32_le(0); // index
        body.put_u32_le(IggyError::StreamIdNotFound(Default::default()).as_code());
        let err = split_metadata_result(Operation::DeleteStream, body.freeze()).unwrap_err();
        assert_eq!(
            err.as_code(),
            IggyError::StreamIdNotFound(Default::default()).as_code()
        );
    }

    #[test]
    fn metadata_reply_with_truncated_section_is_invalid_command_never_ok() {
        // count claims an entry the body cannot hold: corruption, never a
        // rejection silently flipping to success.
        let mut body = BytesMut::new();
        body.put_u32_le(1);
        let err = split_metadata_result(Operation::CreateStream, body.freeze()).unwrap_err();
        assert!(matches!(err, IggyError::InvalidCommand));
    }

    #[test]
    fn non_metadata_reply_passes_through_without_a_result_section() {
        // Reads / partition-plane / Register-Logout replies carry no section.
        let body = Bytes::from_static(b"raw-non-metadata-body");
        let out = split_metadata_result(Operation::NonReplicated, body.clone()).unwrap();
        assert_eq!(out, body);
    }

    /// `[count = 1][index = 0][result = code]` -- the single-entry rejection
    /// shape (`ApplyReply::write_reply_body` / `build_transient_reply`).
    fn rejection_body(code: u32) -> Bytes {
        let mut body = Vec::with_capacity(12);
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&code.to_le_bytes());
        Bytes::from(body)
    }

    /// `[count = 0]` followed by the payload -- the result-framed success shape.
    fn success_body(payload: &[u8]) -> Bytes {
        let mut body = Vec::with_capacity(4 + payload.len());
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(payload);
        Bytes::from(body)
    }

    #[test]
    fn delete_consumer_offset_rejection_decodes_to_terminal_error() {
        let code = IggyError::ConsumerOffsetNotFound(0).as_code();
        let result = split_metadata_result(Operation::DeleteConsumerOffset, rejection_body(code));
        assert_eq!(
            result.unwrap_err().as_code(),
            code,
            "delete rejection must surface as the typed error, not decode as Ok"
        );
    }

    #[test]
    fn store_consumer_offset_rejection_decodes_to_terminal_error() {
        let code = IggyError::InvalidOffset(42).as_code();
        let result = split_metadata_result(Operation::StoreConsumerOffset, rejection_body(code));
        assert_eq!(result.unwrap_err().as_code(), code);
    }

    #[test]
    fn consumer_offset_success_strips_the_empty_result_section() {
        let out = split_metadata_result(Operation::StoreConsumerOffset, success_body(b"")).unwrap();
        assert!(out.is_empty());
        let out =
            split_metadata_result(Operation::DeleteConsumerOffset, success_body(b"")).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn metadata_transient_code_decodes_to_transient_not_committed() {
        let code = IggyError::TransientNotCommitted.as_code();
        let result = split_metadata_result(Operation::CreateStream, rejection_body(code));
        assert!(matches!(
            result.unwrap_err(),
            IggyError::TransientNotCommitted
        ));
    }

    #[test]
    fn metadata_success_returns_payload_after_result_section() {
        let out = split_metadata_result(Operation::CreateStream, success_body(b"payload")).unwrap();
        assert_eq!(out.as_ref(), b"payload");
    }

    #[test]
    fn send_messages_body_is_never_interpreted_as_a_result_section() {
        // A data-plane reply whose first bytes happen to look like a rejection
        // must pass through untouched: `SendMessages` is not result-framed.
        let body = rejection_body(IggyError::InvalidOffset(1).as_code());
        let out = split_metadata_result(Operation::SendMessages, body.clone()).unwrap();
        assert_eq!(out, body);
    }
}
