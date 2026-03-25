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

use bytemuck::{CheckedBitPattern, NoUninit};

/// Replicated operation discriminant. Identifies the state machine operation
/// carried in a consensus message body.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, NoUninit, CheckedBitPattern)]
#[repr(u8)]
pub enum Operation {
    #[default]
    Reserved = 0,

    // Metadata operations (shard 0)
    CreateStream = 128,
    UpdateStream = 129,
    DeleteStream = 130,
    PurgeStream = 131,
    CreateTopic = 132,
    UpdateTopic = 133,
    DeleteTopic = 134,
    PurgeTopic = 135,
    CreatePartitions = 136,
    DeletePartitions = 137,
    // TODO: DeleteSegments is a partition operation (is_partition() == true) but its
    // discriminant sits in the metadata range (128-147). Should be moved to 162 once
    // iggy_common's Operation enum is removed and wire compat is no longer a concern.
    DeleteSegments = 138,
    CreateConsumerGroup = 139,
    DeleteConsumerGroup = 140,
    CreateUser = 141,
    UpdateUser = 142,
    DeleteUser = 143,
    ChangePassword = 144,
    UpdatePermissions = 145,
    CreatePersonalAccessToken = 146,
    DeletePersonalAccessToken = 147,

    // Partition operations (routed by namespace)
    SendMessages = 160,
    StoreConsumerOffset = 161,
}

impl Operation {
    /// Metadata / control-plane operations handled by shard 0.
    #[must_use]
    #[inline]
    pub const fn is_metadata(&self) -> bool {
        matches!(
            self,
            Self::CreateStream
                | Self::UpdateStream
                | Self::DeleteStream
                | Self::PurgeStream
                | Self::CreateTopic
                | Self::UpdateTopic
                | Self::DeleteTopic
                | Self::PurgeTopic
                | Self::CreatePartitions
                | Self::DeletePartitions
                | Self::CreateConsumerGroup
                | Self::DeleteConsumerGroup
                | Self::CreateUser
                | Self::UpdateUser
                | Self::DeleteUser
                | Self::ChangePassword
                | Self::UpdatePermissions
                | Self::CreatePersonalAccessToken
                | Self::DeletePersonalAccessToken
        )
    }

    /// Data-plane operations routed to the shard owning the partition.
    #[must_use]
    #[inline]
    pub const fn is_partition(&self) -> bool {
        matches!(
            self,
            Self::SendMessages | Self::StoreConsumerOffset | Self::DeleteSegments
        )
    }

    /// Bidirectional mapping: `Operation` -> client command code.
    ///
    /// Delegates to the dispatch table as the single source of truth.
    #[must_use]
    pub const fn to_command_code(&self) -> Option<u32> {
        match self {
            Self::Reserved => None,
            Self::CreateStream
            | Self::UpdateStream
            | Self::DeleteStream
            | Self::PurgeStream
            | Self::CreateTopic
            | Self::UpdateTopic
            | Self::DeleteTopic
            | Self::PurgeTopic
            | Self::CreatePartitions
            | Self::DeletePartitions
            | Self::DeleteSegments
            | Self::CreateConsumerGroup
            | Self::DeleteConsumerGroup
            | Self::CreateUser
            | Self::UpdateUser
            | Self::DeleteUser
            | Self::ChangePassword
            | Self::UpdatePermissions
            | Self::CreatePersonalAccessToken
            | Self::DeletePersonalAccessToken
            | Self::SendMessages
            | Self::StoreConsumerOffset => match crate::dispatch::lookup_by_operation(*self) {
                Some(meta) => Some(meta.code),
                None => None,
            },
        }
    }

    /// Bidirectional mapping: client command code -> `Operation`.
    ///
    /// Delegates to the dispatch table as the single source of truth.
    #[must_use]
    pub const fn from_command_code(code: u32) -> Option<Self> {
        match crate::dispatch::lookup_command(code) {
            Some(meta) => meta.operation,
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Operation;

    #[test]
    fn command_code_roundtrip() {
        let ops = [
            Operation::CreateStream,
            Operation::UpdateStream,
            Operation::DeleteStream,
            Operation::PurgeStream,
            Operation::CreateTopic,
            Operation::UpdateTopic,
            Operation::DeleteTopic,
            Operation::PurgeTopic,
            Operation::CreatePartitions,
            Operation::DeletePartitions,
            Operation::DeleteSegments,
            Operation::CreateConsumerGroup,
            Operation::DeleteConsumerGroup,
            Operation::CreateUser,
            Operation::UpdateUser,
            Operation::DeleteUser,
            Operation::ChangePassword,
            Operation::UpdatePermissions,
            Operation::CreatePersonalAccessToken,
            Operation::DeletePersonalAccessToken,
            Operation::SendMessages,
            Operation::StoreConsumerOffset,
        ];
        for op in ops {
            let code = op
                .to_command_code()
                .unwrap_or_else(|| panic!("Operation {op:?} has no command code mapping"));
            let back = Operation::from_command_code(code)
                .unwrap_or_else(|| panic!("command code {code} has no Operation mapping"));
            assert_eq!(op, back, "roundtrip failed for {op:?} (code={code})");
        }
    }

    #[test]
    fn reserved_has_no_code() {
        assert_eq!(Operation::Reserved.to_command_code(), None);
    }

    #[test]
    fn read_only_commands_have_no_operation() {
        use crate::codes::{GET_STATS_CODE, GET_STREAM_CODE, PING_CODE, POLL_MESSAGES_CODE};
        assert!(Operation::from_command_code(PING_CODE).is_none());
        assert!(Operation::from_command_code(GET_STATS_CODE).is_none());
        assert!(Operation::from_command_code(GET_STREAM_CODE).is_none());
        assert!(Operation::from_command_code(POLL_MESSAGES_CODE).is_none());
    }

    #[test]
    fn metadata_vs_partition() {
        assert!(Operation::CreateStream.is_metadata());
        assert!(!Operation::CreateStream.is_partition());
        assert!(Operation::SendMessages.is_partition());
        assert!(!Operation::SendMessages.is_metadata());
        assert!(Operation::DeleteSegments.is_partition());
    }
}
