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

use crate::codes::{LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE, LOGOUT_USER_CODE};
use crate::consensus::{Command, HEADER_SIZE, Operation, RequestHeader};
use crate::error::WireError;
use std::ops::Range;
use twox_hash::XxHash3_64;

/// Where `RequestHeader::reserved` carries the legacy command code of a
/// `NonReplicated` request, as little-endian `u32`.
pub const NON_REPLICATED_CODE_RANGE: Range<usize> = 0..4;

/// Maps a command code onto the consensus operation a client sends it as.
///
/// `COMMAND_TABLE` is a protocol registry, not a per-server capability list,
/// so a client build cannot know which codes a given server implements. The
/// server is the authority: an unmapped code is forwarded as non-replicated
/// (the code rides `RequestHeader::reserved`, which that path stamps) and the
/// server answers with a proper error if it does not know it.
#[must_use]
pub fn operation_for_code(code: u32) -> Operation {
    match code {
        LOGIN_REGISTER_CODE | LOGIN_REGISTER_WITH_PAT_CODE => Operation::Register,
        LOGOUT_USER_CODE => Operation::Logout,
        _ => Operation::from_command_code(code).unwrap_or(Operation::NonReplicated),
    }
}

impl RequestHeader {
    /// Builds the header a client sends for `code` with `payload`, given the
    /// identity the session assigned: `client`, `request` and `session`.
    ///
    /// Every rule the server checks lives here so that each client, and the
    /// fixtures other SDKs verify against, agree on the bytes: metadata and
    /// session operations stamp `request_checksum` with the payload hash,
    /// partition and non-replicated operations leave it zero (send batches
    /// carry their own checksum and non-replicated requests bypass dedup),
    /// and a non-replicated request carries its command code in `reserved`.
    ///
    /// # Errors
    ///
    /// Returns `WireError::PayloadTooLarge` when the frame would not fit the
    /// `u32` size field.
    pub fn for_request(
        code: u32,
        client: u128,
        request: u64,
        session: u64,
        payload: &[u8],
    ) -> Result<Self, WireError> {
        let operation = operation_for_code(code);
        let total_size = HEADER_SIZE.saturating_add(payload.len());
        let size = u32::try_from(total_size).map_err(|_| WireError::PayloadTooLarge {
            size: payload.len(),
            max: u32::MAX as usize - HEADER_SIZE,
        })?;
        let request_checksum = if operation.is_partition() || operation == Operation::NonReplicated
        {
            0
        } else {
            u128::from(XxHash3_64::oneshot(payload))
        };
        let mut reserved = [0; 60];
        if operation == Operation::NonReplicated {
            reserved[NON_REPLICATED_CODE_RANGE].copy_from_slice(&code.to_le_bytes());
        }
        Ok(Self {
            command: Command::Request,
            operation,
            size,
            client,
            request,
            session,
            request_checksum,
            // Replicated prepares get a server timestamp; no consumer needs a
            // client clock read here.
            timestamp: 0,
            reserved,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codes::{CREATE_STREAM_CODE, PING_CODE, SEND_MESSAGES_CODE};

    #[test]
    fn login_codes_register_and_logout_logs_out() {
        assert_eq!(operation_for_code(LOGIN_REGISTER_CODE), Operation::Register);
        assert_eq!(
            operation_for_code(LOGIN_REGISTER_WITH_PAT_CODE),
            Operation::Register
        );
        assert_eq!(operation_for_code(LOGOUT_USER_CODE), Operation::Logout);
        assert_eq!(operation_for_code(u32::MAX), Operation::NonReplicated);
    }

    #[test]
    fn metadata_requests_are_stamped_with_the_payload_hash() {
        let payload = [1, 2, 3];
        let header = RequestHeader::for_request(CREATE_STREAM_CODE, 7, 8, 9, &payload).unwrap();
        assert_eq!(header.command, Command::Request);
        assert_eq!(header.size as usize, HEADER_SIZE + payload.len());
        assert_eq!((header.client, header.request, header.session), (7, 8, 9));
        assert_eq!(
            header.request_checksum,
            u128::from(XxHash3_64::oneshot(&payload))
        );
        assert_eq!(header.reserved, [0; 60]);
    }

    #[test]
    fn partition_and_non_replicated_requests_leave_the_stamp_zero() {
        let send = RequestHeader::for_request(SEND_MESSAGES_CODE, 1, 2, 3, &[9; 16]).unwrap();
        assert_eq!(send.request_checksum, 0);
        assert!(send.operation.is_partition());

        let ping = RequestHeader::for_request(PING_CODE, 1, 2, 3, &[]).unwrap();
        assert_eq!(ping.operation, Operation::NonReplicated);
        assert_eq!(ping.request_checksum, 0);
        assert_eq!(
            ping.reserved[NON_REPLICATED_CODE_RANGE],
            PING_CODE.to_le_bytes()
        );
    }
}
