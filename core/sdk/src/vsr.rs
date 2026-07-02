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
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::codec::WireDecode;
use iggy_binary_protocol::codes::{
    DELETE_CONSUMER_OFFSET_2_CODE, DELETE_CONSUMER_OFFSET_CODE, DELETE_SEGMENTS_CODE,
    LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE, LOGOUT_USER_CODE, SEND_MESSAGES_CODE,
    STORE_CONSUMER_OFFSET_2_CODE, STORE_CONSUMER_OFFSET_CODE,
};
use iggy_binary_protocol::consensus::{
    Command2, EvictionHeader, EvictionReason, GenericHeader, HEADER_SIZE, Operation, ReplyHeader,
    RequestHeader, read_size_field, result_code, result_section_len,
};
use iggy_binary_protocol::namespace::{
    MAX_PARTITIONS, MAX_STREAMS, MAX_TOPICS, METADATA_CONSENSUS_NAMESPACE, PARTITION_MASK,
    PARTITION_SHIFT, STREAM_MASK, STREAM_SHIFT, TOPIC_MASK, TOPIC_SHIFT,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffset2Request, DeleteConsumerOffsetRequest, StoreConsumerOffset2Request,
    StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::SendMessagesHeader;
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::version::IGGY_PROTOCOL_VERSION;
use iggy_binary_protocol::{WireIdentifier, WirePartitioning};
use iggy_common::IggyError;

const NON_REPLICATED_CODE_RANGE: std::ops::Range<usize> = 0..4;

// TODO(vsr): transparent retry-after-disconnect can weaken at-most-once semantics
// for replicated writes. The transports currently disconnect, reconnect, and encode
// the retry from the current ConsensusSession. If disconnect created a fresh VSR
// client/session, the retried request gets a new (client_id, request_id) tuple, so
// server-side deduplication cannot match a mutation that may already have committed
// before the transport failure. TigerBeetle avoids session resume and relies on
// idempotency for requests retried from a new client session. For Iggy, either stop
// transparent retries for replicated mutations after VSR session reset, add explicit
// session resume/rebind semantics, or add a protocol-level idempotency key that is
// independent of (client_id, request_id).
pub(crate) fn encode_contiguous_request(
    session: &mut ConsensusSession,
    code: u32,
    payload: &Bytes,
) -> Result<Bytes, IggyError> {
    let (header, total_size) = encode_request_header(session, code, payload)?;
    let mut request = BytesMut::with_capacity(total_size);
    request.put_slice(bytemuck::bytes_of(&header));
    request.put_slice(payload);
    Ok(request.freeze())
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
            let operation = operation_for_code(code)?;
            // NonReplicated ops (ping, reads) bypass server-side dedup --
            // `ClientTable` only tracks request_ids for replicated ops. If
            // they consumed the monotonic counter, the next replicated
            // request would skip an id and the primary's `request_preflight`
            // would see a `RequestGap` and silently drop it. Read the
            // current id without advancing; the server ignores it for
            // NonReplicated.
            //
            // They are also sessionless on the server (routed by transport
            // id; protected codes are auth-gated server-side), so send with
            // session 0 when unregistered -- preserving the legacy "ping
            // works without auth" contract. Replicated ops still require a
            // bound session for dedup and fail fast here.
            if operation == Operation::NonReplicated {
                (
                    operation,
                    session.current_request_id(),
                    session.session().unwrap_or(0),
                )
            } else if operation.is_partition() {
                // Partition ops replicate in their own per-partition group,
                // which is at-least-once with no `ClientTable` dedup -- the
                // metadata table never records their request ids. Consuming
                // the counter here would gap the NEXT metadata op's id and
                // `request_preflight` would silently drop it (`RequestGap`).
                let session_id = session.session().ok_or(IggyError::Unauthenticated)?;
                (operation, session.current_request_id(), session_id)
            } else {
                let session_id = session.session().ok_or(IggyError::Unauthenticated)?;
                (operation, session.next_request_id(), session_id)
            }
        }
    };
    let namespace = namespace_for_request(code, payload, operation)?;
    let total_size = HEADER_SIZE
        .checked_add(payload.len())
        .ok_or(IggyError::InvalidConfiguration)?;
    let size = u32::try_from(total_size).map_err(|_| IggyError::InvalidConfiguration)?;
    let mut reserved = [0; 52];
    if operation == Operation::NonReplicated {
        reserved[NON_REPLICATED_CODE_RANGE].copy_from_slice(&code.to_le_bytes());
    }
    let header = RequestHeader {
        command: Command2::Request,
        operation,
        size,
        client: session.client_id(),
        request: request_id,
        session: session_id,
        namespace,
        // Zeroed: the field is "informational" -- the server copies it into
        // `ReplyHeader.timestamp` for RTT but nothing else reads it. Paying
        // a `clock_gettime` syscall per encoded request (formerly held the
        // `consensus_session` lock too) for an unused field is waste.
        // Reintroduce a real stamp here when an RTT consumer actually wires
        // it up.
        timestamp: 0,
        reserved,
        ..Default::default()
    };

    Ok((header, total_size))
}

fn operation_for_code(code: u32) -> Result<Operation, IggyError> {
    if code == LOGOUT_USER_CODE {
        return Ok(Operation::Logout);
    }

    if let Some(operation) = Operation::from_command_code(code) {
        return Ok(operation);
    }

    match iggy_binary_protocol::dispatch::lookup_command(code) {
        Some(meta) if !meta.is_replicated() => Ok(Operation::NonReplicated),
        Some(_) => Err(IggyError::UnknownReplicatedCommand(code)),
        None => Err(IggyError::InvalidCommand),
    }
}

pub(crate) fn response_size(header: &[u8]) -> Result<usize, IggyError> {
    let size = read_size_field(header).ok_or(IggyError::InvalidCommand)? as usize;
    if size < HEADER_SIZE {
        return Err(IggyError::InvalidCommand);
    }
    Ok(size)
}

pub(crate) fn decode_response(response: Bytes) -> Result<Bytes, IggyError> {
    if response.len() < HEADER_SIZE {
        return Err(IggyError::EmptyResponse);
    }

    let header_bytes: &[u8; HEADER_SIZE] = response[..HEADER_SIZE]
        .try_into()
        .map_err(|_| IggyError::InvalidCommand)?;
    match peek_command(header_bytes) {
        Command2::Eviction => Err(decode_eviction(header_bytes)),
        Command2::Reply => {
            let total_size = response_size(header_bytes)?;
            if response.len() < total_size {
                return Err(IggyError::InvalidCommand);
            }
            if let Some(error) = read_reply_status(header_bytes) {
                return Err(error);
            }
            let operation = read_operation(header_bytes)?;
            split_metadata_result(operation, response.slice(HEADER_SIZE..total_size))
        }
        _ => Err(IggyError::InvalidCommand),
    }
}

/// Decode a reply when the header and body have been read into separate
/// buffers. Saves the 64B header `put_slice` that `decode_response` would
/// otherwise perform when callers concatenate header + body before decoding.
///
/// Also surfaces session-terminal `Command2::Eviction` frames as typed
/// errors: callers waiting on a Reply for an unbound session would otherwise
/// hit a read-timeout because the SDK previously only accepted
/// `Command2::Reply`. Returns the body slice on a normal Reply, or maps the
/// eviction reason to an `IggyError` so the request fails fast.
pub(crate) fn decode_response_split(
    header_bytes: &[u8; HEADER_SIZE],
    body: Bytes,
) -> Result<Bytes, IggyError> {
    match peek_command(header_bytes) {
        Command2::Eviction => Err(decode_eviction(header_bytes)),
        Command2::Reply => {
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

/// Peek `ReplyHeader.status`, the request-level error channel decided at
/// dispatch time (authorization denial today). Read before any body decode:
/// a nonzero status maps to its [`IggyError`] and fails the request, an empty
/// or partial body notwithstanding. `None` (status 0) lets the reply flow to
/// the body/result-section decode. Read by wire offset for the same
/// misalignment reason as [`read_operation`]; metadata replies always stamp 0,
/// so this is inert for them.
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

/// Interpret the committed result section that leads a metadata reply body.
///
/// Metadata ops ([`Operation::is_metadata`]) commit a result
/// section ahead of their typed payload (encode mirror:
/// `metadata::stm::result::ApplyReply::write_reply_body`): success carries
/// `count == 0` then the payload; a committed business rejection carries one
/// `{index, result}` entry and no payload. Strip the section on success and map
/// a nonzero committed code to its [`IggyError`] -- the result discriminants
/// share the `IggyError` code space, so this is the same [`IggyError::from_code`]
/// mapping the legacy transport applies to a status word.
///
/// Reads, the partition data plane, and Register/Logout carry no result section
/// and pass through untouched. A metadata body that is not a well-formed result
/// section is corruption, never a silent success, so it maps to `InvalidCommand`
/// rather than risk a rejection decoding as `Ok`.
fn split_metadata_result(operation: Operation, body: Bytes) -> Result<Bytes, IggyError> {
    if !operation.is_metadata() {
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

/// `Command2` lives at a fixed offset shared by every consensus header
/// (Reply, Eviction, Prepare, ...), so a byte read is enough to discriminate
/// the frame.
fn peek_command(header_bytes: &[u8; HEADER_SIZE]) -> Command2 {
    const COMMAND_OFFSET: usize = std::mem::offset_of!(GenericHeader, command);
    match header_bytes[COMMAND_OFFSET] {
        x if x == Command2::Reply as u8 => Command2::Reply,
        x if x == Command2::Eviction as u8 => Command2::Eviction,
        _ => Command2::Reserved,
    }
}

/// Map a session-terminal Eviction frame to a typed error. Fields are read
/// by wire offset instead of an `EvictionHeader` struct cast: the response
/// buffers are not 16-aligned (the header holds `u128`s, so the cast would
/// fail on an unlucky buffer placement), and reading raw lets the SDK apply
/// the same window sanity check as `EvictionHeader::validate` rather than
/// trusting the remote frame.
fn decode_eviction(header_bytes: &[u8; HEADER_SIZE]) -> IggyError {
    const REASON_OFFSET: usize = std::mem::offset_of!(EvictionHeader, reason);
    const VERSION_OFFSET: usize = std::mem::offset_of!(EvictionHeader, server_protocol_version);
    const VERSION_MIN_OFFSET: usize =
        std::mem::offset_of!(EvictionHeader, server_protocol_version_min);

    let Ok(&reason) = bytemuck::checked::try_from_bytes::<EvictionReason>(
        &header_bytes[REASON_OFFSET..=REASON_OFFSET],
    ) else {
        return IggyError::Unauthenticated;
    };
    match reason {
        EvictionReason::InvalidCredentials => IggyError::InvalidCredentials,
        EvictionReason::InvalidToken => IggyError::InvalidPersonalAccessToken,
        EvictionReason::UserInactive
        | EvictionReason::SessionError
        | EvictionReason::NoSession
        | EvictionReason::SessionTooLow
        | EvictionReason::SessionReleaseMismatch => IggyError::Unauthenticated,
        EvictionReason::StaleClient => IggyError::StaleClient,
        EvictionReason::IncompatibleProtocol => {
            let server_max = read_window_field(header_bytes, VERSION_OFFSET);
            let server_min = read_window_field(header_bytes, VERSION_MIN_OFFSET);
            if server_min == 0 || server_max < server_min {
                return IggyError::Unauthenticated;
            }
            IggyError::IncompatibleProtocolVersion(IGGY_PROTOCOL_VERSION, server_min, server_max)
        }
        EvictionReason::MalformedLogin => IggyError::InvalidFormat,
        _ => IggyError::InvalidCommand,
    }
}

fn read_window_field(header_bytes: &[u8; HEADER_SIZE], offset: usize) -> u32 {
    let mut value = [0u8; 4];
    value.copy_from_slice(&header_bytes[offset..offset + 4]);
    u32::from_le_bytes(value)
}

fn namespace_for_request(
    code: u32,
    payload: &Bytes,
    operation: Operation,
) -> Result<u64, IggyError> {
    // Control-plane requests target the metadata replica (shard 0). The
    // router's `route_typed` only short-circuits to shard 0 when the
    // namespace value equals `METADATA_CONSENSUS_NAMESPACE`; sending plain
    // `0` falls into `route_consensus_control` which hashes the namespace
    // and lands a Register on a peer shard whose `submit_register_in_process`
    // panics ("consensus only exists on shard 0").
    if operation == Operation::Register || operation == Operation::Logout {
        return Ok(METADATA_CONSENSUS_NAMESPACE);
    }
    if operation == Operation::NonReplicated || operation.is_metadata() {
        return Ok(0);
    }

    let namespace = match code {
        SEND_MESSAGES_CODE => {
            if payload.len() < 4 {
                return Err(IggyError::InvalidCommand);
            }
            let metadata_length = u32::from_le_bytes(
                payload[..4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ) as usize;
            if payload.len() < 4 + metadata_length {
                return Err(IggyError::InvalidCommand);
            }
            let header = SendMessagesHeader::decode_from(&payload[4..4 + metadata_length])
                .map_err(|_| IggyError::InvalidCommand)?;
            namespace_from_partitioning(&header.stream_id, &header.topic_id, &header.partitioning)?
        }
        STORE_CONSUMER_OFFSET_CODE => {
            let request = StoreConsumerOffsetRequest::decode_from(payload)
                .map_err(|_| IggyError::InvalidCommand)?;
            namespace_from_partition(&request.stream_id, &request.topic_id, request.partition_id)?
        }
        DELETE_CONSUMER_OFFSET_CODE => {
            let request = DeleteConsumerOffsetRequest::decode_from(payload)
                .map_err(|_| IggyError::InvalidCommand)?;
            namespace_from_partition(&request.stream_id, &request.topic_id, request.partition_id)?
        }
        STORE_CONSUMER_OFFSET_2_CODE => {
            let request = StoreConsumerOffset2Request::decode_from(payload)
                .map_err(|_| IggyError::InvalidCommand)?;
            namespace_from_partition(&request.stream_id, &request.topic_id, request.partition_id)?
        }
        DELETE_CONSUMER_OFFSET_2_CODE => {
            let request = DeleteConsumerOffset2Request::decode_from(payload)
                .map_err(|_| IggyError::InvalidCommand)?;
            namespace_from_partition(&request.stream_id, &request.topic_id, request.partition_id)?
        }
        DELETE_SEGMENTS_CODE => {
            let request = DeleteSegmentsRequest::decode_from(payload)
                .map_err(|_| IggyError::InvalidCommand)?;
            namespace_from_partition(
                &request.stream_id,
                &request.topic_id,
                Some(request.partition_id),
            )?
        }
        _ => return Err(IggyError::FeatureUnavailable),
    };

    Ok(namespace)
}

fn namespace_from_partitioning(
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partitioning: &WirePartitioning,
) -> Result<u64, IggyError> {
    let WirePartitioning::PartitionId(partition_id) = partitioning else {
        return Err(IggyError::FeatureUnavailable);
    };
    namespace_from_partition(stream_id, topic_id, Some(*partition_id))
}

fn namespace_from_partition(
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
) -> Result<u64, IggyError> {
    let partition_id = partition_id.ok_or(IggyError::InvalidIdentifier)?;
    let Some(stream_id) = stream_id.as_u32() else {
        return Ok(0);
    };
    let Some(topic_id) = topic_id.as_u32() else {
        return Ok(0);
    };
    validate_namespace_field(stream_id, MAX_STREAMS)?;
    validate_namespace_field(topic_id, MAX_TOPICS)?;
    validate_namespace_field(partition_id, MAX_PARTITIONS)?;
    Ok(pack_namespace(
        stream_id as usize,
        topic_id as usize,
        partition_id as usize,
    ))
}

fn validate_namespace_field(value: u32, exclusive_max: usize) -> Result<(), IggyError> {
    let value = usize::try_from(value).map_err(|_| IggyError::InvalidIdentifier)?;
    if value >= exclusive_max {
        return Err(IggyError::InvalidIdentifier);
    }
    Ok(())
}

fn pack_namespace(stream_id: usize, topic_id: usize, partition_id: usize) -> u64 {
    ((stream_id as u64) & STREAM_MASK) << STREAM_SHIFT
        | ((topic_id as u64) & TOPIC_MASK) << TOPIC_SHIFT
        | ((partition_id as u64) & PARTITION_MASK) << PARTITION_SHIFT
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::ConsensusSession;
    use iggy_binary_protocol::codes::{
        CREATE_STREAM_CODE, GET_STREAM_CODE, LOGOUT_USER_CODE, PING_CODE,
    };
    use iggy_binary_protocol::requests::messages::SendMessagesHeader;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::users::LoginRegisterRequest;
    use iggy_binary_protocol::{ClientVersionInfo, WireEncode, WireName};
    use secrecy::SecretString;

    fn decode_request_header(bytes: &Bytes) -> RequestHeader {
        *bytemuck::checked::try_from_bytes::<RequestHeader>(&bytes[..HEADER_SIZE]).unwrap()
    }

    #[test]
    fn register_request_uses_zero_request_and_session() {
        let mut session = ConsensusSession::with_client_id(7);
        let request = LoginRegisterRequest {
            version_info: ClientVersionInfo {
                protocol_version: IGGY_PROTOCOL_VERSION,
                sdk_name: WireName::new("rust-sdk").unwrap(),
                sdk_version: WireName::new("1.0.0").unwrap(),
            },
            username: WireName::new("admin").unwrap(),
            password: SecretString::from("secret"),
            client_context: None,
        };

        let bytes =
            encode_contiguous_request(&mut session, LOGIN_REGISTER_CODE, &request.to_bytes())
                .unwrap();
        let header = decode_request_header(&bytes);

        assert_eq!(header.operation, Operation::Register);
        assert_eq!(header.request, 0);
        assert_eq!(header.session, 0);
        assert_eq!(header.client, 7);
        // Register is routed to the metadata replica (shard 0). The router's
        // namespace==METADATA short-circuit needs the sentinel, not 0.
        assert_eq!(header.namespace, METADATA_CONSENSUS_NAMESPACE);
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
        // A dispatch-time authorization denial rides `ReplyHeader.status`; the
        // decode funnel surfaces it as the typed error before any body decode,
        // even though the deny body is empty.
        let header = ReplyHeader {
            command: Command2::Reply,
            size: HEADER_SIZE as u32,
            status: IggyError::Unauthorized.as_code(),
            ..Default::default()
        };
        let mut buf = [0u8; HEADER_SIZE];
        buf.copy_from_slice(bytemuck::bytes_of(&header));
        let result = decode_response_split(&buf, Bytes::new());
        assert!(matches!(result, Err(IggyError::Unauthorized)));
    }

    #[test]
    fn reply_with_zero_status_passes_body_through() {
        // status 0 is the ok channel: a non-metadata reply returns its body.
        let header = ReplyHeader {
            command: Command2::Reply,
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
        session.bind(99);
        let payload = CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
        }
        .to_bytes();

        let first = encode_contiguous_request(&mut session, CREATE_STREAM_CODE, &payload).unwrap();
        let second = encode_contiguous_request(&mut session, CREATE_STREAM_CODE, &payload).unwrap();

        assert_eq!(decode_request_header(&first).request, 1);
        assert_eq!(decode_request_header(&second).request, 2);
        assert_eq!(decode_request_header(&second).session, 99);
        assert_eq!(decode_request_header(&second).namespace, 0);
    }

    #[test]
    fn ping_uses_non_replicated_operation() {
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99);
        let bytes = encode_contiguous_request(&mut session, PING_CODE, &Bytes::new()).unwrap();
        let header = decode_request_header(&bytes);

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
        assert_eq!(header.namespace, 0);
    }

    #[test]
    fn logout_uses_replicated_logout_operation() {
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99);
        let bytes =
            encode_contiguous_request(&mut session, LOGOUT_USER_CODE, &Bytes::new()).unwrap();
        let header = decode_request_header(&bytes);

        assert_eq!(header.operation, Operation::Logout);
        assert_eq!(header.request, 1);
        assert_eq!(header.session, 99);
        // Logout, like Register, is routed to shard 0 via the metadata
        // sentinel rather than namespace 0.
        assert_eq!(header.namespace, METADATA_CONSENSUS_NAMESPACE);
    }

    #[test]
    fn read_only_request_uses_non_replicated_operation() {
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(99);
        let bytes =
            encode_contiguous_request(&mut session, GET_STREAM_CODE, &Bytes::new()).unwrap();
        let header = decode_request_header(&bytes);

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
    fn namespace_defers_named_identifiers_to_server_resolution() {
        let stream = WireIdentifier::named("stream").unwrap();
        let topic = WireIdentifier::numeric(1);
        let namespace = namespace_from_partition(&stream, &topic, Some(0)).unwrap();
        assert_eq!(namespace, 0);
    }

    #[test]
    fn namespace_rejects_out_of_range_fields() {
        let stream = WireIdentifier::numeric(MAX_STREAMS as u32);
        let topic = WireIdentifier::numeric(1);
        let err = namespace_from_partition(&stream, &topic, Some(0)).unwrap_err();
        assert!(matches!(err, IggyError::InvalidIdentifier));

        let stream = WireIdentifier::numeric(1);
        let partition_id = u32::try_from(MAX_PARTITIONS).unwrap();
        let err = namespace_from_partition(&stream, &topic, Some(partition_id)).unwrap_err();
        assert!(matches!(err, IggyError::InvalidIdentifier));
    }

    #[test]
    fn send_messages_with_numeric_partition_builds_namespace() {
        let header = SendMessagesHeader {
            stream_id: WireIdentifier::numeric(2),
            topic_id: WireIdentifier::numeric(3),
            partitioning: WirePartitioning::PartitionId(4),
            messages_count: 0,
        };
        let mut payload = BytesMut::new();
        payload.put_u32_le(header.metadata_length() as u32);
        header.encode(&mut payload);

        let namespace = namespace_for_request(
            SEND_MESSAGES_CODE,
            &payload.freeze(),
            Operation::SendMessages,
        )
        .unwrap();
        assert_eq!((namespace >> STREAM_SHIFT) & STREAM_MASK, 2);
        assert_eq!((namespace >> TOPIC_SHIFT) & TOPIC_MASK, 3);
        assert_eq!((namespace >> PARTITION_SHIFT) & PARTITION_MASK, 4);
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
}
