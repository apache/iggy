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

use super::{ErrorClass, Op};
use crate::in_flight::InFlightOps;
use crate::shadow::ShadowState;
use iggy_common::IggyError;

impl ErrorClass {
    /// Shadow-free classifier for contexts without shadow state (e.g. replay).
    /// Treats all resource-not-found as concurrent races, connection errors as
    /// transient, everything else as a server bug.
    pub fn coarse(err: &IggyError) -> Self {
        match err {
            IggyError::Disconnected
            | IggyError::NotConnected
            | IggyError::CannotEstablishConnection
            | IggyError::Unauthenticated
            | IggyError::Unauthorized
            | IggyError::TcpError
            | IggyError::QuicError
            | IggyError::StaleClient
            | IggyError::ClientShutdown
            | IggyError::ConnectionClosed
            | IggyError::WebSocketError
            | IggyError::WebSocketConnectionError
            | IggyError::WebSocketCloseError
            | IggyError::WebSocketReceiveError
            | IggyError::WebSocketSendError
            | IggyError::HttpError(_)
            | IggyError::HttpResponseError(_, _)
            | IggyError::FeatureUnavailable => Self::Transient,

            IggyError::StreamNameAlreadyExists(_)
            | IggyError::TopicNameAlreadyExists(_, _)
            | IggyError::ConsumerGroupNameAlreadyExists(_, _)
            | IggyError::StreamNameNotFound(_)
            | IggyError::TopicNameNotFound(_, _)
            | IggyError::StreamIdNotFound(_)
            | IggyError::TopicIdNotFound(_, _)
            | IggyError::PartitionNotFound(_, _, _)
            | IggyError::SegmentNotFound
            | IggyError::ConsumerGroupIdNotFound(_, _)
            | IggyError::ConsumerGroupNameNotFound(_, _)
            | IggyError::ConsumerGroupMemberNotFound(_, _, _)
            | IggyError::ResourceNotFound(_)
            | IggyError::InvalidPartitionsCount
            | IggyError::TooManyPartitions
            | IggyError::NoPartitions(_, _) => Self::ExpectedConcurrent,

            _ => Self::ServerBug,
        }
    }
}

impl Op {
    /// Classify an IggyError using shadow state and in-flight tracking to distinguish
    /// between concurrent races (benign), server bugs, and transient failures.
    pub fn classify_error(
        &self,
        err: &IggyError,
        state: &ShadowState,
        in_flight: &InFlightOps,
    ) -> ErrorClass {
        match err {
            IggyError::Disconnected
            | IggyError::NotConnected
            | IggyError::CannotEstablishConnection
            | IggyError::Unauthenticated
            | IggyError::Unauthorized
            | IggyError::TcpError
            | IggyError::QuicError
            | IggyError::StaleClient
            | IggyError::ClientShutdown
            | IggyError::ConnectionClosed
            | IggyError::WebSocketError
            | IggyError::WebSocketConnectionError
            | IggyError::WebSocketCloseError
            | IggyError::WebSocketReceiveError
            | IggyError::WebSocketSendError
            | IggyError::HttpError(_)
            | IggyError::HttpResponseError(_, _)
            | IggyError::FeatureUnavailable => ErrorClass::Transient,

            IggyError::StreamNameNotFound(name) => {
                if state.was_recently_deleted(name)
                    || !state.stream_exists(name)
                    || in_flight.contains(name)
                {
                    ErrorClass::ExpectedConcurrent
                } else {
                    ErrorClass::ServerBug
                }
            }

            IggyError::StreamIdNotFound(_) => {
                if let Some(stream_name) = self.stream_name()
                    && (state.was_recently_deleted(stream_name)
                        || !state.stream_exists(stream_name)
                        || in_flight.contains(stream_name))
                {
                    return ErrorClass::ExpectedConcurrent;
                }
                ErrorClass::ServerBug
            }

            IggyError::TopicNameNotFound(_, _) | IggyError::TopicIdNotFound(_, _) => {
                if let (Some(s), Some(t)) = (self.stream_name(), self.topic_name())
                    && (state.was_recently_deleted(&format!("{s}/{t}"))
                        || state.was_recently_deleted(s)
                        || !state.topic_exists(s, t)
                        || in_flight.contains(&format!("{s}/{t}"))
                        || in_flight.contains(s))
                {
                    return ErrorClass::ExpectedConcurrent;
                }
                ErrorClass::ServerBug
            }

            IggyError::StreamNameAlreadyExists(name) => {
                if state.stream_exists(name) {
                    ErrorClass::ExpectedConcurrent
                } else {
                    ErrorClass::ServerBug
                }
            }

            IggyError::TopicNameAlreadyExists(_, _) => {
                if let (Some(s), Some(t)) = (self.stream_name(), self.topic_name())
                    && state.topic_exists(s, t)
                {
                    return ErrorClass::ExpectedConcurrent;
                }
                ErrorClass::ServerBug
            }

            IggyError::ConsumerGroupNameAlreadyExists(_, _) => {
                if let (Some(s), Some(t)) = (self.stream_name(), self.topic_name()) {
                    let group_name = match self {
                        Op::CreateConsumerGroup { name, .. } => Some(name.as_str()),
                        _ => None,
                    };
                    if let Some(gn) = group_name
                        && state
                            .get_topic(s, t)
                            .is_some_and(|t| t.consumer_groups.contains(gn))
                    {
                        return ErrorClass::ExpectedConcurrent;
                    }
                }
                ErrorClass::ServerBug
            }

            IggyError::PartitionNotFound(_, _, _)
            | IggyError::SegmentNotFound
            | IggyError::ConsumerGroupIdNotFound(_, _)
            | IggyError::ConsumerGroupNameNotFound(_, _)
            | IggyError::ConsumerGroupMemberNotFound(_, _, _)
            | IggyError::ResourceNotFound(_)
            | IggyError::InvalidPartitionsCount
            | IggyError::TooManyPartitions
            | IggyError::NoPartitions(_, _) => ErrorClass::ExpectedConcurrent,

            _ => ErrorClass::ServerBug,
        }
    }

    pub(crate) fn stream_name(&self) -> Option<&str> {
        match self {
            Op::CreateStream { name } | Op::DeleteStream { name } | Op::PurgeStream { name } => {
                Some(name)
            }
            Op::CreateTopic { stream, .. }
            | Op::DeleteTopic { stream, .. }
            | Op::PurgeTopic { stream, .. }
            | Op::SendMessages { stream, .. }
            | Op::PollMessages { stream, .. }
            | Op::DeleteSegments { stream, .. }
            | Op::CreatePartitions { stream, .. }
            | Op::DeletePartitions { stream, .. }
            | Op::CreateConsumerGroup { stream, .. }
            | Op::DeleteConsumerGroup { stream, .. }
            | Op::JoinConsumerGroup { stream, .. }
            | Op::LeaveConsumerGroup { stream, .. }
            | Op::GetConsumerGroup { stream, .. }
            | Op::PollGroupMessages { stream, .. }
            | Op::StoreConsumerOffset { stream, .. }
            | Op::GetStreamDetails { stream }
            | Op::GetTopicDetails { stream, .. } => Some(stream),
            Op::GetStreams | Op::GetStats => None,
        }
    }

    pub(crate) fn topic_name(&self) -> Option<&str> {
        match self {
            Op::CreateStream { .. }
            | Op::DeleteStream { .. }
            | Op::PurgeStream { .. }
            | Op::GetStreams
            | Op::GetStats
            | Op::GetStreamDetails { .. } => None,
            Op::CreateTopic { name, .. } => Some(name),
            Op::DeleteTopic { topic, .. }
            | Op::PurgeTopic { topic, .. }
            | Op::SendMessages { topic, .. }
            | Op::PollMessages { topic, .. }
            | Op::DeleteSegments { topic, .. }
            | Op::CreatePartitions { topic, .. }
            | Op::DeletePartitions { topic, .. }
            | Op::CreateConsumerGroup { topic, .. }
            | Op::DeleteConsumerGroup { topic, .. }
            | Op::JoinConsumerGroup { topic, .. }
            | Op::LeaveConsumerGroup { topic, .. }
            | Op::GetConsumerGroup { topic, .. }
            | Op::PollGroupMessages { topic, .. }
            | Op::StoreConsumerOffset { topic, .. }
            | Op::GetTopicDetails { topic, .. } => Some(topic),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::Identifier;

    fn id(n: u32) -> Identifier {
        Identifier::numeric(n).unwrap()
    }

    /// Verify that every explicitly-handled IggyError variant in `ErrorClass::coarse`
    /// is classified as intended (not falling through to the `_ => ServerBug` wildcard).
    /// If a new SDK variant is added to a match arm, this test catches it at compile time.
    #[test]
    fn coarse_explicit_variants_not_server_bug() {
        let transient_variants: Vec<IggyError> = vec![
            IggyError::Disconnected,
            IggyError::NotConnected,
            IggyError::CannotEstablishConnection,
            IggyError::Unauthenticated,
            IggyError::Unauthorized,
            IggyError::TcpError,
            IggyError::QuicError,
            IggyError::StaleClient,
            IggyError::ClientShutdown,
            IggyError::ConnectionClosed,
            IggyError::WebSocketError,
            IggyError::WebSocketConnectionError,
            IggyError::WebSocketCloseError,
            IggyError::WebSocketReceiveError,
            IggyError::WebSocketSendError,
            IggyError::HttpError(String::new()),
            IggyError::HttpResponseError(0, String::new()),
            IggyError::FeatureUnavailable,
        ];
        for err in &transient_variants {
            assert_eq!(
                ErrorClass::coarse(err),
                ErrorClass::Transient,
                "Expected Transient for {err}"
            );
        }

        let expected_concurrent_variants: Vec<IggyError> = vec![
            IggyError::StreamNameAlreadyExists(String::new()),
            IggyError::TopicNameAlreadyExists(String::new(), id(1)),
            IggyError::ConsumerGroupNameAlreadyExists(String::new(), id(1)),
            IggyError::StreamNameNotFound(String::new()),
            IggyError::TopicNameNotFound(String::new(), String::new()),
            IggyError::StreamIdNotFound(id(1)),
            IggyError::TopicIdNotFound(id(1), id(1)),
            IggyError::PartitionNotFound(0, id(1), id(1)),
            IggyError::SegmentNotFound,
            IggyError::ConsumerGroupIdNotFound(id(1), id(1)),
            IggyError::ConsumerGroupNameNotFound(String::new(), id(1)),
            IggyError::ConsumerGroupMemberNotFound(0, id(1), id(1)),
            IggyError::ResourceNotFound(String::new()),
            IggyError::InvalidPartitionsCount,
            IggyError::TooManyPartitions,
            IggyError::NoPartitions(id(1), id(1)),
        ];
        for err in &expected_concurrent_variants {
            assert_eq!(
                ErrorClass::coarse(err),
                ErrorClass::ExpectedConcurrent,
                "Expected ExpectedConcurrent for {err}"
            );
        }
    }

    /// Verify that unhandled variants fall through to ServerBug in coarse classification.
    #[test]
    fn coarse_unhandled_is_server_bug() {
        let unhandled = vec![
            IggyError::InvalidCommand,
            IggyError::InvalidFormat,
            IggyError::CannotCreateStream(0),
        ];
        for err in &unhandled {
            assert_eq!(
                ErrorClass::coarse(err),
                ErrorClass::ServerBug,
                "Expected ServerBug for unhandled {err}"
            );
        }
    }
}
