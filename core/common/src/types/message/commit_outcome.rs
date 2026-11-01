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

use crate::IggyError;

/// Where a committing deferred poll (106) or fetch (109) reported an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitErrorScope {
    /// The whole reply is the error.
    Request,
    /// One section of a fetch reply carries the error status.
    Section,
}

impl CommitErrorScope {
    /// Whether `error`, reported in this scope, proves that the server stored no offset. Any
    /// other error, a lost reply and a transport failure after the send leave the outcome
    /// unknown.
    pub fn proves_nothing_stored(self, error: &IggyError) -> bool {
        match self {
            Self::Request => matches!(
                error,
                IggyError::FeatureUnavailable
                    | IggyError::InvalidIdentifier
                    | IggyError::TransientNotAccepted
                    | IggyError::HistoryUnavailable
                    | IggyError::NotPrimary
                    | IggyError::DeferredPollCapacityExceeded
                    // The server refuses a first message larger than `max_bytes` before it
                    // stores the offset.
                    | IggyError::InvalidSizeBytes
                    | IggyError::StreamIdNotFound(..)
                    | IggyError::StreamNameNotFound(..)
                    | IggyError::TopicIdNotFound(..)
                    | IggyError::TopicNameNotFound(..)
                    | IggyError::PartitionNotFound(..)
                    | IggyError::InvalidOffset(..)
                    | IggyError::ConsumerGroupPartitionNotOwned(..)
                    // A read that needs a new offset key above the limit returns no messages.
                    | IggyError::TooManyConsumerOffsets
            ),
            // None of these leaves an offset stored in the history that the request named.
            Self::Section => matches!(
                error,
                IggyError::TransientNotAccepted
                    | IggyError::InvalidSizeBytes
                    | IggyError::HistoryUnavailable
                    | IggyError::NotPrimary
                    | IggyError::ConsumerGroupPartitionNotOwned(..)
                    | IggyError::PartitionNotFound(..)
                    | IggyError::TooManyConsumerOffsets
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_transient_not_accepted_section_when_classifying_then_should_prove_nothing_stored() {
        assert!(CommitErrorScope::Section.proves_nothing_stored(&IggyError::TransientNotAccepted));
    }

    #[test]
    fn given_lost_session_when_classifying_then_should_leave_outcome_unknown_in_both_scopes() {
        for error in [
            IggyError::Unauthenticated,
            IggyError::Unauthorized,
            IggyError::StaleClient,
            IggyError::TransientNotCommitted,
            IggyError::CannotDecryptData,
            IggyError::ShardCommunicationError,
        ] {
            assert!(!CommitErrorScope::Request.proves_nothing_stored(&error));
            assert!(!CommitErrorScope::Section.proves_nothing_stored(&error));
        }
    }

    #[test]
    fn given_caller_condition_in_a_section_when_classifying_then_should_leave_outcome_unknown() {
        for error in [
            IggyError::FeatureUnavailable,
            IggyError::DeferredPollCapacityExceeded,
            IggyError::InvalidOffset(0),
        ] {
            assert!(CommitErrorScope::Request.proves_nothing_stored(&error));
            assert!(!CommitErrorScope::Section.proves_nothing_stored(&error));
        }
    }
}
