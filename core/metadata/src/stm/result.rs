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

//! Committed result codes for metadata state-machine ops.
//!
//! One closed enum per op, keyed off the `IggyError` `#[repr(u32)]` code space
//! so an SDK can reuse its `IggyError` mapping. Discriminants are append-only,
//! never reused (the wire-stability discipline of `Operation`/`IggyError`).
//!
//! Codes are carrier-agnostic: a metadata op is single-event today, so its code
//! rides the reply body as a one-entry (see [`ApplyReply::write_reply_body`]),
//! never a header field. If ops ever batch
//! the section gains `{index, result}` entries and these enums survive unchanged,
//! so no variant may encode a carrier assumption (no index, no framing).

use bytes::{Bytes, BytesMut};
use iggy_binary_protocol::Operation;
use iggy_binary_protocol::consensus::{RESULT_COUNT_LEN, RESULT_ENTRY_LEN};

/// Decode side of the result section, shared with every client (SDK, simulator)
/// via `binary_protocol`. Re-exported so server-side callers keep one import.
pub use iggy_binary_protocol::consensus::result_code;

/// Outcome of applying one committed metadata op.
///
/// `code == 0` is success, `body` the typed reply payload. Nonzero `code` is a
/// committed business rejection with empty `body`: the op committed as a
/// deterministic no-op on every replica, so a retry replays the cached reply
/// (error included) idempotently.
#[derive(Debug, Clone, Default)]
pub struct ApplyReply {
    pub code: u32,
    pub body: Bytes,
}

impl ApplyReply {
    /// Successful apply carrying the typed reply body.
    #[must_use]
    pub const fn ok(body: Bytes) -> Self {
        Self { code: 0, body }
    }

    /// Committed business rejection: empty body, `code` is the result
    /// discriminant.
    #[must_use]
    pub fn err(code: impl Into<u32>) -> Self {
        Self {
            code: code.into(),
            body: Bytes::new(),
        }
    }

    /// Byte length of the wire reply body produced by [`Self::write_reply_body`]:
    /// `4 + payload` on success, a fixed `12` for the single-entry rejection.
    #[must_use]
    pub const fn reply_body_len(&self) -> usize {
        if self.code == 0 {
            RESULT_COUNT_LEN + self.body.len()
        } else {
            RESULT_COUNT_LEN + RESULT_ENTRY_LEN
        }
    }

    /// Serialize the wire reply body into `dst`: a sparse
    /// result section (`[count: u32]` then `count` x `{index: u32, result: u32}`,
    /// little-endian) followed by the payload. Success (`code == 0`) writes
    /// `count = 0` plus the payload; a rejection writes one `{index: 0, result}`
    /// entry and no payload. Single-event metadata yields at most one entry.
    ///
    /// Writing straight into the caller's buffer (via `build_reply_message_with`)
    /// keeps the commit path to one allocation and one payload copy.
    ///
    /// # Panics
    /// If `dst.len() != self.reply_body_len()`.
    pub fn write_reply_body(&self, dst: &mut [u8]) {
        if self.code == 0 {
            dst[..RESULT_COUNT_LEN].copy_from_slice(&0u32.to_le_bytes());
            dst[RESULT_COUNT_LEN..].copy_from_slice(&self.body);
        } else {
            // One `{index: 0, result: code}` entry: the two u32 halves of the
            // 8-byte entry that follows the count.
            dst[..RESULT_COUNT_LEN].copy_from_slice(&1u32.to_le_bytes());
            let entry = &mut dst[RESULT_COUNT_LEN..RESULT_COUNT_LEN + RESULT_ENTRY_LEN];
            entry[..4].copy_from_slice(&0u32.to_le_bytes());
            entry[4..].copy_from_slice(&self.code.to_le_bytes());
        }
    }

    /// Allocating form of [`Self::write_reply_body`]. The commit path uses the
    /// in-place writer; this stays for tests and ad-hoc callers.
    #[must_use]
    pub fn to_reply_body(&self) -> Bytes {
        let mut buf = BytesMut::zeroed(self.reply_body_len());
        self.write_reply_body(&mut buf);
        buf.freeze()
    }
}

/// Declares a `#[repr(u32)]` result enum with `Ok = 0` injected first, plus
/// `From<Self> for u32` and a `from_u32` round-trip.
///
/// One macro for every enum keeps the discriminants a uniform table and prevents
/// an enum from omitting `Ok = 0` or drifting in its conversions.
macro_rules! result_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident = $code:expr),* $(,)? }) => {
        $(#[$meta])*
        #[repr(u32)]
        #[derive(Copy, Clone, Eq, PartialEq, Debug)]
        pub enum $name {
            Ok = 0,
            $($variant = $code,)*
        }

        impl From<$name> for u32 {
            fn from(value: $name) -> Self {
                value as Self
            }
        }

        impl $name {
            /// Inverse of the `u32` conversion. `None` for an unknown code.
            #[must_use]
            pub const fn from_u32(code: u32) -> Option<Self> {
                match code {
                    0 => Some(Self::Ok),
                    $($code => Some(Self::$variant),)*
                    _ => None,
                }
            }
        }
    };
}

// Streams.
result_enum!(CreateStreamResult { NameAlreadyExists = 1012 });
result_enum!(UpdateStreamResult {
    StreamNotFound = 1009,
    NameAlreadyExists = 1012,
});
result_enum!(DeleteStreamResult { StreamNotFound = 1009 });
result_enum!(PurgeStreamResult { StreamNotFound = 1009 });

// Topics.
result_enum!(CreateTopicResult {
    StreamNotFound = 1009,
    NameAlreadyExists = 2013,
});
result_enum!(UpdateTopicResult {
    StreamNotFound = 1009,
    TopicNotFound = 2010,
    NameAlreadyExists = 2013,
});
result_enum!(DeleteTopicResult {
    StreamNotFound = 1009,
    TopicNotFound = 2010,
});
result_enum!(PurgeTopicResult {
    StreamNotFound = 1009,
    TopicNotFound = 2010,
});

// Partitions. `InvalidPartitionsCount` also covers the partition-id overflow
// guards in the apply handler.
result_enum!(CreatePartitionsResult {
    StreamNotFound = 1009,
    TopicNotFound = 2010,
    InvalidPartitionsCount = 2019,
});
result_enum!(DeletePartitionsResult {
    StreamNotFound = 1009,
    TopicNotFound = 2010,
});

// Users. No dedicated user-not-found code in `IggyError`; `ResourceNotFound = 20`
// stands in until one is added (a separate `IggyError` change).
result_enum!(CreateUserResult {
    InvalidUsername = 43,
    UserAlreadyExists = 46,
});
result_enum!(UpdateUserResult {
    UserNotFound = 20,
    InvalidUsername = 43,
    UsernameAlreadyExists = 46,
});
result_enum!(DeleteUserResult {
    UserNotFound = 20,
    CannotDeleteUser = 48,
});
result_enum!(ChangePasswordResult {
    UserNotFound = 20,
    InvalidCredentials = 42,
});
result_enum!(UpdatePermissionsResult {
    UserNotFound = 20,
    CannotChangePermissions = 49,
});

// Personal access tokens.
result_enum!(CreatePersonalAccessTokenResult {
    AlreadyExists = 51,
    InvalidExpiry = 56,
});
result_enum!(DeletePersonalAccessTokenResult { NotFound = 20 });

// Consumer groups.
result_enum!(CreateConsumerGroupResult {
    StreamNotFound = 1009,
    TopicNotFound = 2010,
    NameAlreadyExists = 5004,
});
result_enum!(DeleteConsumerGroupResult { NotFound = 5000 });

/// `IggyError::Unauthorized`. Any control-plane op can commit as an in-apply
/// authorization no-op, so this code is valid for every op regardless of its
/// own result enum. Pinned to the wire discriminant in
/// [`tests::given_result_discriminants_then_match_iggy_error_codes`].
const UNAUTHORIZED_CODE: u32 = 41;

/// True if `code` is one this op's result enum declares (`Ok = 0` included), or
/// the global `UNAUTHORIZED_CODE`.
///
/// The state machine only commits codes from the op's own enum, plus the
/// in-apply authorization gate's global `Unauthorized` denial, so an
/// unrecognized one is a server bug, not a race (a race still yields a declared
/// code). Enriched internal ops map to their client-op enum; ops with no result
/// section (partition plane) return `true`.
#[must_use]
pub const fn result_code_recognized(operation: Operation, code: u32) -> bool {
    // The in-apply RBAC gate can deny any control-plane op, committing an
    // `Unauthorized` no-op, so accept it globally rather than per-op enum.
    if code == UNAUTHORIZED_CODE {
        return true;
    }
    match operation {
        Operation::CreateStream => CreateStreamResult::from_u32(code).is_some(),
        Operation::UpdateStream => UpdateStreamResult::from_u32(code).is_some(),
        Operation::DeleteStream => DeleteStreamResult::from_u32(code).is_some(),
        Operation::PurgeStream => PurgeStreamResult::from_u32(code).is_some(),
        Operation::CreateTopic | Operation::CreateTopicWithAssignments => {
            CreateTopicResult::from_u32(code).is_some()
        }
        Operation::UpdateTopic => UpdateTopicResult::from_u32(code).is_some(),
        Operation::DeleteTopic => DeleteTopicResult::from_u32(code).is_some(),
        Operation::PurgeTopic => PurgeTopicResult::from_u32(code).is_some(),
        Operation::CreatePartitions | Operation::CreatePartitionsWithAssignments => {
            CreatePartitionsResult::from_u32(code).is_some()
        }
        Operation::DeletePartitions => DeletePartitionsResult::from_u32(code).is_some(),
        Operation::CreateUser => CreateUserResult::from_u32(code).is_some(),
        Operation::UpdateUser => UpdateUserResult::from_u32(code).is_some(),
        Operation::DeleteUser => DeleteUserResult::from_u32(code).is_some(),
        Operation::ChangePassword => ChangePasswordResult::from_u32(code).is_some(),
        Operation::UpdatePermissions => UpdatePermissionsResult::from_u32(code).is_some(),
        Operation::CreatePersonalAccessToken => {
            CreatePersonalAccessTokenResult::from_u32(code).is_some()
        }
        Operation::DeletePersonalAccessToken => {
            DeletePersonalAccessTokenResult::from_u32(code).is_some()
        }
        Operation::CreateConsumerGroup => CreateConsumerGroupResult::from_u32(code).is_some(),
        Operation::DeleteConsumerGroup => DeleteConsumerGroupResult::from_u32(code).is_some(),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{Identifier, IggyError};

    #[test]
    fn from_u32_round_trips_known_codes_and_rejects_unknown() {
        assert_eq!(u32::from(CreateStreamResult::Ok), 0);
        assert_eq!(
            CreateStreamResult::from_u32(u32::from(CreateStreamResult::NameAlreadyExists)),
            Some(CreateStreamResult::NameAlreadyExists),
        );
        assert_eq!(
            UpdateTopicResult::from_u32(u32::from(UpdateTopicResult::TopicNotFound)),
            Some(UpdateTopicResult::TopicNotFound),
        );
        assert_eq!(CreateStreamResult::from_u32(9999), None);
    }

    #[test]
    fn reply_body_serializes_and_round_trips_through_result_code() {
        // Success: `[count = 0]` then payload; decodes back to code 0.
        let ok = ApplyReply::ok(Bytes::from_static(b"payload"));
        let ok_body = ok.to_reply_body();
        assert_eq!(&ok_body[..4], &0u32.to_le_bytes());
        assert_eq!(&ok_body[4..], b"payload");
        assert_eq!(result_code(&ok_body), Some(0));

        // Rejection: one `{index = 0, result}` entry, no payload; decodes to code.
        let err = ApplyReply::err(CreateStreamResult::NameAlreadyExists);
        let err_body = err.to_reply_body();
        assert_eq!(&err_body[0..4], &1u32.to_le_bytes());
        assert_eq!(&err_body[4..8], &0u32.to_le_bytes());
        assert_eq!(&err_body[8..12], &1012u32.to_le_bytes());
        assert_eq!(err_body.len(), 12);
        assert_eq!(result_code(&err_body), Some(1012));

        // The commit hot path writes in place; must equal the allocating form.
        for reply in [ok, err, ApplyReply::default()] {
            let mut dst = vec![0u8; reply.reply_body_len()];
            reply.write_reply_body(&mut dst);
            assert_eq!(Bytes::from(dst), reply.to_reply_body());
        }
    }

    #[test]
    fn result_code_rejects_malformed_body_as_none_never_ok() {
        // A body too short for the `count`, or claiming an entry it cannot hold,
        // is malformed: `None`, never `Some(0)`. A rejection silently flipping to
        // success is the bug this guards.
        assert_eq!(result_code(&[]), None);
        assert_eq!(result_code(&1u32.to_le_bytes()), None); // count=1, no entry
        assert_eq!(result_code(&[1, 0, 0, 0, 0, 0, 0, 0]), None); // count=1, result missing
    }

    #[test]
    fn result_code_recognized_accepts_only_an_ops_declared_codes() {
        // Declared codes pass (incl. `Ok`); an enriched internal op maps to its
        // client-op enum.
        assert!(result_code_recognized(Operation::CreateStream, 0));
        assert!(result_code_recognized(
            Operation::CreateStream,
            u32::from(CreateStreamResult::NameAlreadyExists),
        ));
        assert!(result_code_recognized(
            Operation::CreateTopicWithAssignments,
            u32::from(CreateTopicResult::NameAlreadyExists),
        ));
        // Undeclared codes fail, even a real code from another op. `PurgeStream`
        // has no `TopicNotFound`: the explicit arm rejects it where the prior
        // `_ => true` default would have passed any code.
        assert!(!result_code_recognized(Operation::CreateStream, 9999));
        assert!(!result_code_recognized(
            Operation::PurgeStream,
            u32::from(PurgeTopicResult::TopicNotFound),
        ));
        // Partition-plane ops carry no result section, so any code is accepted.
        assert!(result_code_recognized(Operation::SendMessages, 12345));
        // `Unauthorized` commits for any control-plane op via the in-apply gate,
        // even though no per-op result enum declares it.
        assert!(result_code_recognized(
            Operation::CreateStream,
            UNAUTHORIZED_CODE
        ));
        assert!(result_code_recognized(
            Operation::CreateUser,
            UNAUTHORIZED_CODE
        ));
        assert!(result_code_recognized(
            Operation::CreateConsumerGroup,
            UNAUTHORIZED_CODE
        ));
    }

    // Pins every result-enum discriminant to the `IggyError` variant its doc
    // claims, via `IggyError::as_code()` (the real wire discriminant the SDKs
    // decode). The discriminants are bare literals with no compile-time tie to
    // `IggyError`: renumber a variant and this fails loudly instead of silently
    // corrupting the cross-SDK wire contract. (`IggyError::X as u32` won't work,
    // most variants are fieldful, which `as` cannot cast, hence `as_code`'s
    // pointer read.)
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_result_discriminants_then_match_iggy_error_codes() {
        let id = Identifier::default;

        // StreamIdNotFound (1009).
        let stream_not_found = IggyError::StreamIdNotFound(id()).as_code();
        assert_eq!(
            u32::from(UpdateStreamResult::StreamNotFound),
            stream_not_found
        );
        assert_eq!(
            u32::from(DeleteStreamResult::StreamNotFound),
            stream_not_found
        );
        assert_eq!(
            u32::from(PurgeStreamResult::StreamNotFound),
            stream_not_found
        );
        assert_eq!(
            u32::from(CreateTopicResult::StreamNotFound),
            stream_not_found
        );
        assert_eq!(
            u32::from(UpdateTopicResult::StreamNotFound),
            stream_not_found
        );
        assert_eq!(
            u32::from(DeleteTopicResult::StreamNotFound),
            stream_not_found
        );
        assert_eq!(
            u32::from(PurgeTopicResult::StreamNotFound),
            stream_not_found
        );
        assert_eq!(
            u32::from(CreatePartitionsResult::StreamNotFound),
            stream_not_found
        );
        assert_eq!(
            u32::from(DeletePartitionsResult::StreamNotFound),
            stream_not_found
        );
        assert_eq!(
            u32::from(CreateConsumerGroupResult::StreamNotFound),
            stream_not_found
        );

        // StreamNameAlreadyExists (1012).
        let stream_name_exists = IggyError::StreamNameAlreadyExists(String::new()).as_code();
        assert_eq!(
            u32::from(CreateStreamResult::NameAlreadyExists),
            stream_name_exists
        );
        assert_eq!(
            u32::from(UpdateStreamResult::NameAlreadyExists),
            stream_name_exists
        );

        // TopicIdNotFound (2010).
        let topic_not_found = IggyError::TopicIdNotFound(id(), id()).as_code();
        assert_eq!(u32::from(UpdateTopicResult::TopicNotFound), topic_not_found);
        assert_eq!(u32::from(DeleteTopicResult::TopicNotFound), topic_not_found);
        assert_eq!(u32::from(PurgeTopicResult::TopicNotFound), topic_not_found);
        assert_eq!(
            u32::from(CreatePartitionsResult::TopicNotFound),
            topic_not_found
        );
        assert_eq!(
            u32::from(DeletePartitionsResult::TopicNotFound),
            topic_not_found
        );
        assert_eq!(
            u32::from(CreateConsumerGroupResult::TopicNotFound),
            topic_not_found
        );

        // TopicNameAlreadyExists (2013).
        let topic_name_exists = IggyError::TopicNameAlreadyExists(String::new(), id()).as_code();
        assert_eq!(
            u32::from(CreateTopicResult::NameAlreadyExists),
            topic_name_exists
        );
        assert_eq!(
            u32::from(UpdateTopicResult::NameAlreadyExists),
            topic_name_exists
        );

        // InvalidPartitionsCount (2019).
        assert_eq!(
            u32::from(CreatePartitionsResult::InvalidPartitionsCount),
            IggyError::InvalidPartitionsCount.as_code(),
        );

        // UserAlreadyExists (46) - also stands in for username-already-exists.
        let user_exists = IggyError::UserAlreadyExists.as_code();
        assert_eq!(u32::from(CreateUserResult::UserAlreadyExists), user_exists);
        assert_eq!(
            u32::from(UpdateUserResult::UsernameAlreadyExists),
            user_exists
        );

        // InvalidUsername (43) - username length outside [MIN,MAX] bytes,
        // rejected in-apply for create and rename alike.
        let invalid_username = IggyError::InvalidUsername.as_code();
        assert_eq!(
            u32::from(CreateUserResult::InvalidUsername),
            invalid_username
        );
        assert_eq!(
            u32::from(UpdateUserResult::InvalidUsername),
            invalid_username
        );

        // ResourceNotFound (20) - documented stand-in for user/PAT not-found.
        let resource_not_found = IggyError::ResourceNotFound(String::new()).as_code();
        assert_eq!(
            u32::from(UpdateUserResult::UserNotFound),
            resource_not_found
        );
        assert_eq!(
            u32::from(DeleteUserResult::UserNotFound),
            resource_not_found
        );
        assert_eq!(
            u32::from(ChangePasswordResult::UserNotFound),
            resource_not_found
        );
        assert_eq!(
            u32::from(UpdatePermissionsResult::UserNotFound),
            resource_not_found
        );
        assert_eq!(
            u32::from(DeletePersonalAccessTokenResult::NotFound),
            resource_not_found
        );

        // CannotDeleteUser (48) - root deletion is rejected in-apply.
        assert_eq!(
            u32::from(DeleteUserResult::CannotDeleteUser),
            IggyError::CannotDeleteUser(0).as_code(),
        );

        // CannotChangePermissions (49) - root permissions are immutable in-apply.
        assert_eq!(
            u32::from(UpdatePermissionsResult::CannotChangePermissions),
            IggyError::CannotChangePermissions(0).as_code(),
        );

        // InvalidCredentials (42) - wrong current password on ChangePassword,
        // committed as a rejecting no-op so the client request sequence stays
        // contiguous (a pre-consensus deny would gap it).
        assert_eq!(
            u32::from(ChangePasswordResult::InvalidCredentials),
            IggyError::InvalidCredentials.as_code(),
        );

        // PersonalAccessTokenAlreadyExists (51).
        assert_eq!(
            u32::from(CreatePersonalAccessTokenResult::AlreadyExists),
            IggyError::PersonalAccessTokenAlreadyExists(String::new(), 0).as_code(),
        );

        // InvalidPersonalAccessTokenExpiry (56).
        assert_eq!(
            u32::from(CreatePersonalAccessTokenResult::InvalidExpiry),
            IggyError::InvalidPersonalAccessTokenExpiry.as_code(),
        );

        // ConsumerGroupNameAlreadyExists (5004) / ConsumerGroupIdNotFound (5000).
        assert_eq!(
            u32::from(CreateConsumerGroupResult::NameAlreadyExists),
            IggyError::ConsumerGroupNameAlreadyExists(String::new(), id()).as_code(),
        );
        assert_eq!(
            u32::from(DeleteConsumerGroupResult::NotFound),
            IggyError::ConsumerGroupIdNotFound(id(), id()).as_code(),
        );

        // Unauthorized (41) - the global in-apply RBAC denial code.
        assert_eq!(UNAUTHORIZED_CODE, IggyError::Unauthorized.as_code());
    }
}
