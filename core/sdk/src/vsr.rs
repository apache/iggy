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
    // The header rules (request identifier, size, reserved code) live in the
    // protocol crate so every client and the cross-SDK fixtures share them;
    // only the session identity is decided here.
    let mut header =
        RequestHeader::for_request(code, session.client_id(), request_id, session_id, payload)
            .map_err(|_| IggyError::InvalidConfiguration)?;
    if operation == Operation::Register {
        header.request_checksum = session.register_request_identifier()?;
    }
    debug_assert_eq!(header.operation, operation);
    Ok((header, header.size as usize))
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
    decode_reply_outcome(header_bytes, body, None)?.result
}

/// Keeps a well-formed business rejection distinct from an invalid frame.
#[derive(Debug)]
pub(crate) struct ReplyOutcome {
    pub(crate) result: Result<Bytes, IggyError>,
    pub(crate) committed: bool,
}

pub(crate) fn decode_reply_for(
    request: &RequestHeader,
    header_bytes: &[u8; HEADER_SIZE],
    body: Bytes,
) -> Result<ReplyOutcome, IggyError> {
    decode_reply_outcome(header_bytes, body, Some(request))
}

fn decode_reply_outcome(
    header_bytes: &[u8; HEADER_SIZE],
    body: Bytes,
    request: Option<&RequestHeader>,
) -> Result<ReplyOutcome, IggyError> {
    match peek_command(header_bytes) {
        Command::Eviction => Err(decode_eviction(header_bytes)),
        Command::Reply => {
            let expected_body = response_size(header_bytes)? - HEADER_SIZE;
            if body.len() != expected_body
                || request.is_some_and(|request| reply_request_id(header_bytes) != request.request)
            {
                return Err(IggyError::InvalidCommand);
            }
            if let Some(error) = read_reply_status(header_bytes) {
                return Ok(ReplyOutcome {
                    result: Err(error),
                    committed: false,
                });
            }
            let header = bytemuck::checked::try_pod_read_unaligned::<ReplyHeader>(header_bytes)
                .map_err(|_| IggyError::InvalidCommand)?;
            let mut outcome = decode_result(header.operation, body)?;
            if let Some(request) = request.filter(|_| header.has_committed_result()) {
                let matches_request = header.client == request.client
                    && header.request_checksum == request.request_checksum
                    // The primary resolves some commands into an internal
                    // operation before committing their prepare.
                    && header.operation.request_operation() == request.operation
                    && header.partition_incarnation == request.partition_incarnation
                    && header.op != 0
                    && header.op == header.commit;
                if !matches_request {
                    return Err(IggyError::InvalidCommand);
                }
                outcome.committed = !matches!(
                    outcome.result,
                    Err(IggyError::TransientNotCommitted
                        | IggyError::TransientNotAccepted
                        | IggyError::NotPrimary)
                );
            }
            Ok(outcome)
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
fn decode_result(operation: Operation, body: Bytes) -> Result<ReplyOutcome, IggyError> {
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
    let result = if !result_framed {
        Ok(body)
    } else {
        match result_code(&body) {
            Some(0) => {
                let payload_start = result_section_len(&body).ok_or(IggyError::InvalidCommand)?;
                Ok(body.slice(payload_start..))
            }
            Some(code) => Err(IggyError::from_code(code)),
            None => return Err(IggyError::InvalidCommand),
        }
    };
    Ok(ReplyOutcome {
        result,
        committed: false,
    })
}

#[cfg(test)]
fn split_metadata_result(operation: Operation, body: Bytes) -> Result<Bytes, IggyError> {
    decode_result(operation, body)?.result
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

/// Map an Eviction frame to its public error. Fields are read
/// by wire offset instead of an `EvictionHeader` struct cast: the response
/// buffers are not 16-aligned (the header holds `u128`s, so the cast would
/// fail on an unlucky buffer placement), and reading raw lets the SDK apply
/// the same window sanity check as `EvictionHeader::validate` rather than
/// trusting the remote frame.
pub(crate) fn decode_eviction(header_bytes: &[u8; HEADER_SIZE]) -> IggyError {
    const VERSION_OFFSET: usize = std::mem::offset_of!(EvictionHeader, server_protocol_version);
    const VERSION_MIN_OFFSET: usize =
        std::mem::offset_of!(EvictionHeader, server_protocol_version_min);

    let Some(reason) = eviction_reason(header_bytes) else {
        return IggyError::Unauthenticated;
    };
    eviction_reason_to_error(
        reason,
        read_window_field(header_bytes, VERSION_OFFSET),
        read_window_field(header_bytes, VERSION_MIN_OFFSET),
    )
}

pub(crate) fn eviction_reason(header: &[u8; HEADER_SIZE]) -> Option<EvictionReason> {
    const REASON_OFFSET: usize = std::mem::offset_of!(EvictionHeader, reason);
    bytemuck::checked::try_from_bytes::<EvictionReason>(&header[REASON_OFFSET..=REASON_OFFSET])
        .ok()
        .copied()
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
        CREATE_PARTITIONS_CODE, CREATE_STREAM_CODE, CREATE_TOPIC_CODE, DELETE_SEGMENTS_CODE,
        GET_STREAM_CODE, LOGOUT_USER_CODE, PING_CODE, SEND_MESSAGES_CODE, UPDATE_RESOURCE_CODE,
    };
    use iggy_binary_protocol::consensus::{NON_REPLICATED_CODE_RANGE, PrepareHeader};
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::users::LoginRegisterRequest;
    use iggy_binary_protocol::version::IGGY_PROTOCOL_VERSION;
    use iggy_binary_protocol::{ClientVersionInfo, WireEncode, WireName, WireOptions};
    use iggy_common::calculate_checksum;
    use secrecy::SecretString;

    fn committed_header(request: &RequestHeader, body: &[u8]) -> ReplyHeader {
        ReplyHeader::from_prepare(
            &PrepareHeader {
                client: request.client,
                request: request.request,
                request_checksum: request.request_checksum,
                operation: request.operation,
                partition_incarnation: request.partition_incarnation,
                op: 7,
                ..Default::default()
            },
            u32::try_from(HEADER_SIZE + body.len()).unwrap(),
        )
    }

    fn decode_for_test(
        request: &RequestHeader,
        reply: &ReplyHeader,
        body: Bytes,
    ) -> Result<ReplyOutcome, IggyError> {
        decode_reply_for(request, bytemuck::bytes_of(reply).try_into().unwrap(), body)
    }

    #[test]
    fn given_rewritten_committed_results_when_decoded_should_prove_only_the_matching_operation() {
        let cases = [
            (CREATE_TOPIC_CODE, Operation::CreateTopicWithAssignments),
            (
                CREATE_PARTITIONS_CODE,
                Operation::CreatePartitionsWithAssignments,
            ),
            (DELETE_SEGMENTS_CODE, Operation::TruncatePartition),
        ];
        for (code, committed_operation) in cases {
            let request = RequestHeader::for_request(code, 1, 2, 3, b"request").unwrap();
            for result_code in [0, IggyError::Unauthorized.as_code()] {
                let body = rejection_body(result_code);
                let mut reply = committed_header(&request, &body);
                reply.operation = committed_operation;
                let outcome = decode_for_test(&request, &reply, body.clone()).unwrap();
                assert!(outcome.committed, "{code}: {committed_operation:?}");
                assert_eq!(outcome.result.is_ok(), result_code == 0);
                for unrelated in cases.map(|(_, operation)| operation) {
                    if unrelated == committed_operation {
                        continue;
                    }
                    reply.operation = unrelated;
                    assert!(matches!(
                        decode_for_test(&request, &reply, body.clone()),
                        Err(IggyError::InvalidCommand)
                    ));
                }
            }
        }
    }

    #[test]
    fn given_resolved_update_reply_when_correlating_should_accept_only_matching_request_identity() {
        let request =
            RequestHeader::for_request(UPDATE_RESOURCE_CODE, 1, 2, 3, b"request").unwrap();
        let body = rejection_body(0);
        for resolved in [
            Operation::UpdateStreamResolved,
            Operation::UpdateTopicResolved,
            Operation::UpdateUserResolved,
        ] {
            let mut reply = committed_header(&request, &body);
            reply.operation = resolved;
            let outcome = decode_for_test(&request, &reply, body.clone()).unwrap();
            assert!(outcome.committed, "{resolved:?}");
            assert!(outcome.result.is_ok(), "{resolved:?}");
            let mismatches: [fn(&mut ReplyHeader); 3] = [
                |reply| reply.client ^= 1,
                |reply| reply.request_checksum ^= 1,
                |reply| reply.operation = Operation::CreateTopicWithAssignments,
            ];
            for mismatch in mismatches {
                let mut unrelated = reply;
                mismatch(&mut unrelated);
                assert!(
                    matches!(
                        decode_for_test(&request, &unrelated, body.clone()),
                        Err(IggyError::InvalidCommand)
                    ),
                    "{resolved:?}"
                );
            }
        }
    }

    #[test]
    fn given_a_committed_rejection_when_identity_or_marker_differs_should_not_prove_the_result() {
        let request = RequestHeader::for_request(CREATE_STREAM_CODE, 1, 2, 3, b"request").unwrap();
        assert_ne!(request.request_checksum, request.checksum);
        let body = rejection_body(IggyError::StreamNameAlreadyExists("".to_owned()).as_code());
        let reply = committed_header(&request, &body);
        assert!(
            decode_for_test(&request, &reply, body.clone())
                .unwrap()
                .committed
        );
        for marker in [0, ReplyHeader::COMMITTED_RESULT + 1] {
            let mut unproved = reply;
            unproved.reserved[0] = marker;
            let outcome = decode_for_test(&request, &unproved, body.clone()).unwrap();
            assert!(!outcome.committed);
            assert!(outcome.result.is_err());
        }
        let mismatches: [fn(&mut ReplyHeader); 6] = [
            |reply| reply.client ^= 1,
            |reply| reply.request_checksum ^= 1,
            |reply| reply.operation = Operation::DeleteStream,
            |reply| reply.partition_incarnation ^= 1,
            |reply| reply.op = 0,
            |reply| reply.commit += 1,
        ];

        for mismatch in mismatches {
            let mut changed = reply;
            mismatch(&mut changed);
            assert!(
                matches!(
                    decode_for_test(&request, &changed, body.clone()),
                    Err(IggyError::InvalidCommand)
                ),
                "{changed:?}"
            );
        }
        let mut wrong_request = reply;
        wrong_request.request += 1;
        assert!(matches!(
            decode_for_test(&request, &wrong_request, body),
            Err(IggyError::InvalidCommand)
        ));
    }

    #[test]
    fn given_a_marked_reply_when_result_is_malformed_should_not_decode_as_a_committed_rejection() {
        let request = RequestHeader::for_request(CREATE_STREAM_CODE, 1, 2, 3, b"request").unwrap();
        let valid = rejection_body(IggyError::InvalidCommand.as_code());
        let reply = committed_header(&request, &valid);
        let outcome = decode_for_test(&request, &reply, valid.clone()).unwrap();
        assert!(outcome.committed);
        assert_eq!(outcome.result, Err(IggyError::InvalidCommand));
        let mut truncated = valid.to_vec();
        truncated[..size_of::<u32>()].copy_from_slice(&2u32.to_le_bytes());

        assert!(matches!(
            decode_for_test(&request, &reply, Bytes::from(truncated)),
            Err(IggyError::InvalidCommand)
        ));
        assert!(matches!(
            decode_for_test(&request, &reply, valid.slice(..valid.len() - 1)),
            Err(IggyError::InvalidCommand)
        ));
    }

    #[test]
    fn given_a_marked_transient_or_status_denial_when_decoded_should_not_prove_completion() {
        let request = RequestHeader::for_request(CREATE_STREAM_CODE, 1, 2, 3, b"request").unwrap();
        for error in [
            IggyError::TransientNotCommitted,
            IggyError::TransientNotAccepted,
            IggyError::NotPrimary,
        ] {
            let body = rejection_body(error.as_code());
            let reply = committed_header(&request, &body);
            let outcome = decode_for_test(&request, &reply, body).unwrap();
            assert!(!outcome.committed);
            assert_eq!(outcome.result, Err(error));
        }
        let mut reply = committed_header(&request, &[]);
        reply.status = IggyError::Unauthorized.as_code();
        let outcome = decode_for_test(&request, &reply, Bytes::new()).unwrap();
        assert!(!outcome.committed);
        assert_eq!(outcome.result, Err(IggyError::Unauthorized));
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
        encode_request_header(&mut session, LOGIN_REGISTER_CODE, &request.to_bytes()).unwrap();
        session.bind(42).unwrap();

        let next_request = session.next_request_id().unwrap();
        let header = encode_request_header(&mut session, LOGIN_REGISTER_CODE, &request.to_bytes())
            .unwrap()
            .0;
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

        let result = decode_reply(shifted, Bytes::new());
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

            let result = decode_reply(&buf, Bytes::new());
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
                matches!(decode_reply(&buffer, Bytes::new()), Err(result) if result == error),
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
        let out = decode_reply(&buf, Bytes::from_static(b"abc")).unwrap();
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

        let first = encode_request_header(&mut session, CREATE_STREAM_CODE, &payload)
            .unwrap()
            .0;
        let second = encode_request_header(&mut session, CREATE_STREAM_CODE, &payload)
            .unwrap()
            .0;

        assert_eq!(first.request, 1);
        assert_eq!(second.request, 2);
        assert_eq!(second.session, 99);
    }

    #[test]
    fn request_checksum_is_stamped_for_metadata_but_not_partition_operations() {
        // Metadata replies carry the request identifier. Partition operations
        // leave it zero and NonReplicated bypasses dedup.
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();
        let payload = Bytes::from_static(b"payload");

        let metadata = encode_request_header(&mut session, CREATE_STREAM_CODE, &payload)
            .unwrap()
            .0;
        assert_ne!(metadata.request_checksum, 0);
        assert_ne!(
            metadata.request_checksum,
            u128::from(calculate_checksum(&payload)),
        );

        let repeated = encode_request_header(&mut session, CREATE_STREAM_CODE, &payload)
            .unwrap()
            .0;
        assert_ne!(metadata.request_checksum, repeated.request_checksum);
        let request_frame = crate::links::Frame {
            header: metadata,
            body: payload.clone(),
        };
        let replay_frame = request_frame.clone();
        assert_eq!(
            replay_frame.header.request_checksum,
            request_frame.header.request_checksum
        );

        let partition = encode_request_header(&mut session, SEND_MESSAGES_CODE, &payload)
            .unwrap()
            .0;
        assert_eq!(partition.request_checksum, 0);

        let ping = encode_request_header(&mut session, PING_CODE, &Bytes::new())
            .unwrap()
            .0;
        assert_eq!(ping.request_checksum, 0);
    }

    #[test]
    fn given_register_retry_when_encoding_again_should_keep_request_identifier() {
        let mut session = ConsensusSession::with_client_id(42);
        let payload = Bytes::from_static(b"login-body");
        let first = encode_request_header(&mut session, LOGIN_REGISTER_CODE, &payload)
            .expect("first register")
            .0;
        let retry = encode_request_header(&mut session, LOGIN_REGISTER_CODE, &payload)
            .expect("register retry")
            .0;
        assert_ne!(first.request_checksum, 0);
        assert_eq!(retry.request_checksum, first.request_checksum);
        assert_eq!(retry.request, first.request);
    }

    #[test]
    fn partition_request_consumes_the_request_counter() {
        // Dedup identity requires each send to carry a distinct id, so
        // partition ops advance the counter exactly like metadata ops and
        // the two planes interleave on one sequence.
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();
        let payload = Bytes::from_static(b"batch");

        let first = encode_request_header(&mut session, SEND_MESSAGES_CODE, &payload)
            .unwrap()
            .0;
        let second = encode_request_header(&mut session, SEND_MESSAGES_CODE, &payload)
            .unwrap()
            .0;
        assert_eq!(first.request, 1);
        assert_eq!(second.request, 2);

        let metadata_payload = CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
        }
        .to_bytes();
        let metadata = encode_request_header(&mut session, CREATE_STREAM_CODE, &metadata_payload)
            .unwrap()
            .0;
        assert_eq!(metadata.request, 3);
    }

    #[test]
    fn ping_uses_non_replicated_operation() {
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();
        let header = encode_request_header(&mut session, PING_CODE, &Bytes::new())
            .unwrap()
            .0;

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
        let header = encode_request_header(&mut session, LOGOUT_USER_CODE, &Bytes::new())
            .unwrap()
            .0;

        assert_eq!(header.operation, Operation::Logout);
        assert_eq!(header.request, 1);
        assert_eq!(header.session, 99);
    }

    #[test]
    fn read_only_request_uses_non_replicated_operation() {
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99).unwrap();
        let header = encode_request_header(&mut session, GET_STREAM_CODE, &Bytes::new())
            .unwrap()
            .0;

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
        let header = encode_request_header(&mut session, UNKNOWN_CODE, &Bytes::new())
            .unwrap()
            .0;

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
