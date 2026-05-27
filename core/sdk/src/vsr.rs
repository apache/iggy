/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::session::ConsensusSession;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::codec::WireDecode;
use iggy_binary_protocol::codes::{
    DELETE_CONSUMER_OFFSET_2_CODE, DELETE_CONSUMER_OFFSET_CODE, DELETE_SEGMENTS_CODE,
    LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE, LOGOUT_USER_CODE, SEND_MESSAGES_CODE,
    STORE_CONSUMER_OFFSET_2_CODE, STORE_CONSUMER_OFFSET_CODE,
};
use iggy_binary_protocol::consensus::{
    Command2, EvictionHeader, EvictionReason, HEADER_SIZE, Operation, ReplyHeader, RequestHeader,
    read_size_field,
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
            let session_id = session.session().ok_or(IggyError::Unauthenticated)?;
            (operation, session.next_request_id(), session_id)
        }
    };
    let namespace = namespace_for_request(code, payload, operation)?;
    let total_size = HEADER_SIZE
        .checked_add(payload.len())
        .ok_or(IggyError::InvalidConfiguration)?;
    let size = u32::try_from(total_size).map_err(|_| IggyError::InvalidConfiguration)?;
    let mut reserved = [0; 56];
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

    let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(&response[..HEADER_SIZE])
        .map_err(|_| IggyError::InvalidCommand)?;
    if header.command != Command2::Reply {
        return Err(IggyError::InvalidCommand);
    }

    let total_size = header.size as usize;
    if total_size < HEADER_SIZE || response.len() < total_size {
        return Err(IggyError::InvalidCommand);
    }

    Ok(response.slice(HEADER_SIZE..total_size))
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
    let command = peek_command(header_bytes);
    if command == Command2::Eviction {
        if let Ok(header) = bytemuck::checked::try_from_bytes::<EvictionHeader>(header_bytes) {
            return Err(map_eviction_reason(header.reason));
        }
        return Err(IggyError::Unauthenticated);
    }

    let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(header_bytes)
        .map_err(|_| IggyError::InvalidCommand)?;
    if header.command != Command2::Reply {
        return Err(IggyError::InvalidCommand);
    }

    let total_size = header.size as usize;
    if total_size < HEADER_SIZE {
        return Err(IggyError::InvalidCommand);
    }
    let expected_body = total_size - HEADER_SIZE;
    if body.len() < expected_body {
        return Err(IggyError::InvalidCommand);
    }
    Ok(body.slice(..expected_body))
}

/// `Command2` lives at a fixed offset shared by every consensus header
/// (Reply, Eviction, Prepare, ...), so a byte read is enough to discriminate
/// the frame before paying for full `bytemuck` validation.
fn peek_command(header_bytes: &[u8; HEADER_SIZE]) -> Command2 {
    const COMMAND_OFFSET: usize = 60;
    match header_bytes[COMMAND_OFFSET] {
        x if x == Command2::Reply as u8 => Command2::Reply,
        x if x == Command2::Eviction as u8 => Command2::Eviction,
        _ => Command2::Reserved,
    }
}

fn map_eviction_reason(reason: EvictionReason) -> IggyError {
    match reason {
        EvictionReason::InvalidCredentials => IggyError::InvalidCredentials,
        EvictionReason::InvalidToken => IggyError::InvalidPersonalAccessToken,
        EvictionReason::UserInactive => IggyError::Unauthenticated,
        EvictionReason::SessionError
        | EvictionReason::NoSession
        | EvictionReason::SessionTooLow
        | EvictionReason::SessionReleaseMismatch => IggyError::Unauthenticated,
        _ => IggyError::InvalidCommand,
    }
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
    use iggy_binary_protocol::{WireEncode, WireName};
    use secrecy::SecretString;

    fn decode_request_header(bytes: &Bytes) -> RequestHeader {
        *bytemuck::checked::try_from_bytes::<RequestHeader>(&bytes[..HEADER_SIZE]).unwrap()
    }

    #[test]
    fn register_request_uses_zero_request_and_session() {
        let mut session = ConsensusSession::with_client_id(7);
        let request = LoginRegisterRequest {
            username: WireName::new("admin").unwrap(),
            password: SecretString::from("secret"),
            version: None,
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
}
