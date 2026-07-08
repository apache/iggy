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

//! Committed-reply decoding and classification: result-section grading, typed
//! payload decoders, partition-reply status classification, and the eviction /
//! login error mappings the write and auth paths render through.

use iggy_binary_protocol::consensus::{
    Command2, EvictionHeader, HEADER_SIZE, result_code, result_section_len,
};
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::ConsumerGroupDetailsResponse;
use iggy_binary_protocol::responses::personal_access_tokens::RawPersonalAccessTokenResponse;
use iggy_binary_protocol::responses::streams::get_stream::GetStreamResponse;
use iggy_binary_protocol::responses::topics::get_topic::GetTopicResponse;
use iggy_binary_protocol::responses::users::get_user::UserDetailsResponse;
use iggy_binary_protocol::{GenericHeader, ReplyHeader, WireDecode};
use iggy_common::{
    ConsumerGroupDetails, IggyError, StreamDetails, TopicDetails, UserInfoDetails,
    eviction_reason_to_error,
};
use message_bus::BusMessage;
use server_common::Message;

use crate::http::error::{PartitionWriteError, WriteError};
use crate::login_register::LoginRegisterError;

/// Discriminate a partition write reply. Partition replies carry no result
/// section (success and denial are both empty-bodied), so the discriminators
/// live in the header. `status` is read first: a nonzero value is the typed
/// pre-commit denial (the partition primary's delete-of-missing-offset
/// rejection, or a dispatch-time authorization denial) and renders through
/// the legacy `IggyError -> status` map. With status 0, the reply's `op`
/// splits the rest: a committed reply is built from its prepare header, whose
/// op (the partition group's commit number) is always >= 1, while the
/// pre-dispatch gate failures reply through `build_empty_reply` with 0 in
/// that field. The gate reply cannot name which entity was missing, hence the
/// generic legacy 404 body.
pub(in crate::http) fn classify_partition_reply(
    reply: &BusMessage,
) -> Result<(), PartitionWriteError> {
    let header = reply
        .as_slice()
        .get(..HEADER_SIZE)
        .and_then(|bytes| bytemuck::checked::try_from_bytes::<ReplyHeader>(bytes).ok())
        .ok_or(PartitionWriteError::Rejected(IggyError::InvalidCommand))?;
    if header.command != Command2::Reply {
        return Err(PartitionWriteError::Rejected(IggyError::InvalidCommand));
    }
    if header.status != 0 {
        return Err(PartitionWriteError::Rejected(IggyError::from_code(
            header.status,
        )));
    }
    if header.op == 0 {
        return Err(PartitionWriteError::NotFound);
    }
    Ok(())
}

/// Classify a committed reply's leading result section and return the typed
/// payload slice on success. Mirrors the SDK's `split_metadata_result`:
/// `Some(0)` is success and the payload follows the result section; a nonzero
/// first result is a committed business rejection carrying an `IggyError` code
/// (e.g. a duplicate token name); a missing or short section is a malformed
/// committed reply, mapped to an error rather than a false success. Shared by
/// [`submit_write`] and [`create_pat`] so a committed rejection can never render
/// as a 2xx.
pub(in crate::http) fn committed_payload(
    reply: &Message<GenericHeader>,
) -> Result<&[u8], WriteError> {
    let reply_body = reply_body(reply);
    match result_code(reply_body) {
        Some(0) => {
            let payload_start = result_section_len(reply_body)
                .ok_or(WriteError::Rejected(IggyError::InvalidCommand))?;
            reply_body
                .get(payload_start..)
                .ok_or(WriteError::Rejected(IggyError::InvalidCommand))
        }
        Some(code) => Err(WriteError::Rejected(IggyError::from_code(code))),
        None => Err(WriteError::Rejected(IggyError::InvalidCommand)),
    }
}

/// True when a reply-shaped frame is the primary's pre-consensus
/// `TransientNotCommitted` rejection (`[count=1][index=0][57]`, see
/// `build_result_rejection_reply`): the op did not commit, so the write path
/// must replay the same request id rather than grade it as a committed
/// result or advance the session gate.
pub(in crate::http) fn is_transient_not_committed(reply: &Message<GenericHeader>) -> bool {
    result_code(reply_body(reply)) == Some(IggyError::TransientNotCommitted.as_code())
}

/// The reply body past the generic header, bounded by the header's `size`.
fn reply_body(reply: &Message<GenericHeader>) -> &[u8] {
    let size = reply.header().size as usize;
    reply.as_slice().get(HEADER_SIZE..size).unwrap_or_default()
}

/// Decode the `GetStreamResponse` payload of a committed create-stream reply into
/// `StreamDetails`. `payload` is the slice past the result section that
/// [`submit_write`] already validated as a success.
pub(in crate::http) fn decode_stream_details(payload: &[u8]) -> Result<StreamDetails, WriteError> {
    let response = GetStreamResponse::decode_from(payload)
        .map_err(|_| WriteError::Rejected(IggyError::InvalidCommand))?;
    StreamDetails::try_from(response).map_err(WriteError::Rejected)
}

/// Decode the `GetTopicResponse` payload of a committed create-topic reply into
/// `TopicDetails`. `payload` is the slice past the result section that
/// [`submit_write`] already validated as a success.
pub(in crate::http) fn decode_topic_details(payload: &[u8]) -> Result<TopicDetails, WriteError> {
    let response = GetTopicResponse::decode_from(payload)
        .map_err(|_| WriteError::Rejected(IggyError::InvalidCommand))?;
    TopicDetails::try_from(response).map_err(WriteError::Rejected)
}

/// Decode the `UserDetailsResponse` payload of a committed create-user reply into
/// `UserInfoDetails`. `payload` is the slice past the result section that
/// [`submit_write`] already validated as a success.
pub(in crate::http) fn decode_user_details(payload: &[u8]) -> Result<UserInfoDetails, WriteError> {
    let response = UserDetailsResponse::decode_from(payload)
        .map_err(|_| WriteError::Rejected(IggyError::InvalidCommand))?;
    UserInfoDetails::try_from(response).map_err(WriteError::Rejected)
}

/// Decode the `ConsumerGroupDetailsResponse` payload of a committed
/// create-consumer-group reply into `ConsumerGroupDetails`. `payload` is the
/// slice past the result section that [`submit_write`] already validated as a
/// success. The wire-to-domain conversion is infallible.
pub(in crate::http) fn decode_consumer_group_details(
    payload: &[u8],
) -> Result<ConsumerGroupDetails, WriteError> {
    let response = ConsumerGroupDetailsResponse::decode_from(payload)
        .map_err(|_| WriteError::Rejected(IggyError::InvalidCommand))?;
    Ok(ConsumerGroupDetails::from(response))
}

/// Extract the raw one-time token from a [`build_raw_pat_reply`] output. The
/// spliced reply is framed like any committed metadata reply (a success result
/// section, then the `RawPersonalAccessTokenResponse`) so the SDK's
/// `split_metadata_result` can decode it; strip the section the same way here.
pub(in crate::http) fn decode_raw_pat_token(
    reply: &Message<GenericHeader>,
) -> Result<String, WriteError> {
    let payload = committed_payload(reply)?;
    let response = RawPersonalAccessTokenResponse::decode_from(payload)
        .map_err(|_| WriteError::Rejected(IggyError::InvalidCommand))?;
    Ok(response.token.to_string())
}

/// Map an eviction frame to the same typed [`IggyError`] the SDK's
/// `decode_eviction` produces, so an HTTP caller sees the identical status a TCP
/// caller would (session-terminal reasons render as 401 -> re-authenticate).
/// Reuses the shared [`EvictionHeader`] primitive rather than hand-decoding
/// offsets; an unreadable frame falls back to re-authentication.
pub(in crate::http) fn eviction_error(reply: &Message<GenericHeader>) -> IggyError {
    let Some(eviction) = reply
        .as_slice()
        .get(..HEADER_SIZE)
        .and_then(|bytes| bytemuck::checked::try_from_bytes::<EvictionHeader>(bytes).ok())
    else {
        return IggyError::Unauthenticated;
    };
    eviction_reason_to_error(
        eviction.reason,
        eviction.server_protocol_version,
        eviction.server_protocol_version_min,
    )
}

/// Grade a credential-verification failure onto the `IggyError` the legacy
/// HTTP error map already maps to a status + body, so `CustomError` renders
/// what the SDKs are tested against. `verify_*` only yield the first three
/// variants; the tail is unreachable but kept terminal (401) for the
/// `#[non_exhaustive]` enum.
pub(in crate::http) const fn login_error_to_iggy(error: &LoginRegisterError) -> IggyError {
    match error {
        LoginRegisterError::InvalidCredentials => IggyError::InvalidCredentials,
        LoginRegisterError::InvalidToken => IggyError::InvalidPersonalAccessToken,
        LoginRegisterError::UserInactive => IggyError::UserInactive,
        _ => IggyError::Unauthenticated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use iggy_binary_protocol::Operation;
    use iggy_binary_protocol::PrepareHeader;

    use crate::responses::{
        NonReplicatedResponse, build_deny_reply, build_empty_reply, build_reply_from_bytes,
        build_reply_with_body,
    };

    use crate::http::wire::build_request_message;

    // The dispatch-time deny frame: an empty body plus `ReplyHeader.status` set
    // to the rule's error code, the request-level channel the SDK peeks before
    // body decode. `build_empty_reply` (status 0) is the ok-shaped counterpart.
    #[test]
    fn deny_reply_stamps_status_over_an_empty_body() {
        let request = build_request_message(Operation::SendMessages, 42, 7, 3, &[]);
        let status = IggyError::Unauthorized.as_code();
        let reply = build_deny_reply(request.header(), 42, 0, 9, status);
        assert_eq!(
            reply.header().size as usize,
            HEADER_SIZE,
            "deny body is empty (header-only)"
        );
        assert_eq!(
            reply.header().status,
            status,
            "status carries the deny code"
        );
        assert_ne!(status, 0, "a deny status is nonzero so the SDK peek fires");
        assert_eq!(reply.header().command, Command2::Reply);
        // Echoes the request so the SDK routes it back to the waiting slot.
        assert_eq!(reply.header().request, request.header().request);
        assert_eq!(reply.header().operation, request.header().operation);
    }

    // Enforces the status contract: every ok-path reply builder leaves status
    // 0, so a future builder that leaks a nonzero reserved tail (or sets
    // status by mistake) trips here rather than silently shipping a fake
    // denial. The deny builders (build_deny_reply here, the partition
    // primary's consensus::build_deny_reply_from_request) are the only
    // nonzero-status writers.
    #[test]
    fn only_the_deny_builder_emits_a_nonzero_status() {
        let request = build_request_message(Operation::CreateStream, 42, 7, 3, &[]);
        let header = request.header();
        let commit = 9;
        let body = Bytes::from_static(b"body");

        // server-ng reply builders, all funnelled through build_reply_with_body.
        for status in [
            build_reply_with_body(header, 42, 7, commit, 0, |_| {})
                .header()
                .status,
            build_empty_reply(header, 42, 7, commit).header().status,
            build_reply_from_bytes(header, 42, 7, commit, &body)
                .header()
                .status,
            NonReplicatedResponse::Empty
                .into_reply(header, 42, 7, commit)
                .header()
                .status,
            NonReplicatedResponse::Bytes(body.clone())
                .into_reply(header, 42, 7, commit)
                .header()
                .status,
        ] {
            assert_eq!(status, 0, "ok-path reply builders must leave status 0");
        }

        // The consensus plane's committed-reply builder (plane_helpers path).
        let prepare = PrepareHeader {
            command: Command2::Prepare,
            operation: Operation::SendMessages,
            client: 42,
            op: 1,
            request: 1,
            ..Default::default()
        };
        let committed = consensus::build_reply_message(&prepare, &Bytes::new());
        assert_eq!(
            committed.header().status,
            0,
            "committed replies carry status 0"
        );

        // The intentional deny writer stamps nonzero.
        assert_ne!(
            build_deny_reply(header, 42, 7, commit, IggyError::Unauthorized.as_code())
                .header()
                .status,
            0,
            "the deny builder must stamp a nonzero status"
        );
    }

    fn frozen(reply: Message<iggy_binary_protocol::ReplyHeader>) -> BusMessage {
        reply.into_generic().into_frozen()
    }

    #[test]
    fn committed_partition_reply_classifies_as_success() {
        let prepare = PrepareHeader {
            command: Command2::Prepare,
            operation: Operation::SendMessages,
            client: 42,
            op: 1,
            request: 1,
            ..Default::default()
        };
        let reply = frozen(consensus::build_reply_message(&prepare, &Bytes::new()));
        assert!(classify_partition_reply(&reply).is_ok());
    }

    /// The 204 path of the offset write routes: a committed
    /// `StoreConsumerOffset2` reply (op >= 1) classifies as success.
    #[test]
    fn committed_offset_write_reply_classifies_as_success() {
        let prepare = PrepareHeader {
            command: Command2::Prepare,
            operation: Operation::StoreConsumerOffset2,
            client: 42,
            op: 3,
            request: 2,
            ..Default::default()
        };
        let reply = frozen(consensus::build_reply_message(&prepare, &Bytes::new()));
        assert!(classify_partition_reply(&reply).is_ok());
    }

    #[test]
    fn gate_failure_empty_reply_classifies_as_not_found() {
        let request = build_request_message(Operation::SendMessages, 42, 7, 1, &[]);
        let reply = frozen(build_empty_reply(request.header(), 42, 0, 9));
        assert!(matches!(
            classify_partition_reply(&reply),
            Err(PartitionWriteError::NotFound)
        ));
    }

    #[test]
    fn non_reply_frame_classifies_as_rejected() {
        let request = build_request_message(Operation::SendMessages, 42, 7, 1, &[]);
        assert!(matches!(
            classify_partition_reply(&request.into_generic().into_frozen()),
            Err(PartitionWriteError::Rejected(IggyError::InvalidCommand))
        ));
    }

    /// The pre-consensus retry frame must classify as transient so the write
    /// path replays the same request id instead of advancing the session gate
    /// and grading it as a committed rejection (which rendered as a terminal
    /// HTTP error).
    #[test]
    fn transient_rejection_frame_classifies_as_transient() {
        let request = build_request_message(Operation::CreateStream, 42, 7, 3, &[]);
        let reply = consensus::build_result_rejection_reply(
            request.header(),
            9,
            IggyError::TransientNotCommitted.as_code(),
        )
        .into_generic();
        assert!(is_transient_not_committed(&reply));
        assert!(matches!(
            committed_payload(&reply),
            Err(WriteError::Rejected(IggyError::TransientNotCommitted))
        ));
    }

    /// Genuine committed outcomes must advance the gate, so neither a success
    /// reply nor a committed business rejection may trip the replay
    /// classification.
    #[test]
    fn committed_replies_do_not_classify_as_transient() {
        let request = build_request_message(Operation::CreateStream, 42, 7, 3, &[]);
        // `[count=1][index=0][result=0]` success section, then the payload.
        let mut body = Vec::new();
        for word in [1u32, 0, 0] {
            body.extend_from_slice(&word.to_le_bytes());
        }
        body.extend_from_slice(b"payload");
        let success =
            build_reply_from_bytes(request.header(), 42, 7, 9, &Bytes::from(body)).into_generic();
        assert!(!is_transient_not_committed(&success));
        let Ok(payload) = committed_payload(&success) else {
            panic!("success section must grade ok");
        };
        assert_eq!(payload, b"payload");

        let rejected = consensus::build_result_rejection_reply(
            request.header(),
            9,
            IggyError::UserAlreadyExists.as_code(),
        )
        .into_generic();
        assert!(!is_transient_not_committed(&rejected));
        assert!(matches!(
            committed_payload(&rejected),
            Err(WriteError::Rejected(IggyError::UserAlreadyExists))
        ));
    }

    /// The partition primary's typed pre-commit deny (delete of a missing
    /// consumer offset) rides `ReplyHeader.status` and must classify as the
    /// mapped `IggyError`, not as the generic op-0 not-found.
    #[test]
    fn status_bearing_deny_reply_classifies_as_typed_rejection() {
        let request = build_request_message(Operation::DeleteConsumerOffset2, 42, 7, 1, &[]);
        let mut deny = build_empty_reply(request.header(), 42, 0, 9);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut deny.as_mut_slice()[..HEADER_SIZE],
        )
        .expect("empty reply header is valid");
        header.status = IggyError::ConsumerOffsetNotFound(0).as_code();
        assert!(matches!(
            classify_partition_reply(&frozen(deny)),
            Err(PartitionWriteError::Rejected(
                IggyError::ConsumerOffsetNotFound(_)
            ))
        ));
    }
}
