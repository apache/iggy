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

use super::{Op, OpOutcome};
use crate::shadow::{ResourceOrigin, ShadowState};
use iggy_common::IggyError;

impl Op {
    /// Update the shadow state to reflect a completed operation.
    /// Only mutates on success - failed ops leave shadow unchanged.
    pub fn update_shadow(&self, state: &mut ShadowState, outcome: &OpOutcome) {
        if !outcome.is_success() {
            return;
        }

        match self {
            Op::CreateStream { name } => {
                state.apply_create_stream(name.clone(), ResourceOrigin::Dynamic);
            }
            Op::DeleteStream { name } => {
                state.apply_delete_stream(name);
            }
            Op::PurgeStream { name } => {
                state.record_purge(name.clone(), None);
            }
            Op::CreateTopic {
                stream,
                name,
                partitions,
            } => {
                state.apply_create_topic(
                    stream,
                    name.clone(),
                    *partitions,
                    ResourceOrigin::Dynamic,
                );
            }
            Op::DeleteTopic { stream, topic } => {
                state.apply_delete_topic(stream, topic);
            }
            Op::PurgeTopic { stream, topic } => {
                state.record_purge(stream.clone(), Some(topic.clone()));
            }
            Op::SendMessages { .. } => {}
            Op::PollMessages { .. } => {}
            Op::DeleteSegments { .. } => {}
            Op::CreatePartitions {
                stream,
                topic,
                count,
            } => {
                state.apply_create_partitions(stream, topic, *count);
            }
            Op::DeletePartitions {
                stream,
                topic,
                count,
            } => {
                state.apply_delete_partitions(stream, topic, *count);
            }
            Op::CreateConsumerGroup {
                stream,
                topic,
                name,
            } => {
                state.apply_create_consumer_group(stream, topic, name.clone());
            }
            Op::DeleteConsumerGroup {
                stream,
                topic,
                name,
            } => {
                state.apply_delete_consumer_group(stream, topic, name);
            }
            Op::StoreConsumerOffset { .. }
            | Op::JoinConsumerGroup { .. }
            | Op::LeaveConsumerGroup { .. }
            | Op::GetConsumerGroup { .. }
            | Op::PollGroupMessages { .. }
            | Op::GetStreams
            | Op::GetStreamDetails { .. }
            | Op::GetTopicDetails { .. }
            | Op::GetStats => {}
        }

        state.prune_tombstones();
    }

    /// On `ExpectedConcurrent` errors, converge shadow toward the server's
    /// authoritative response. All mutations here are idempotent:
    ///
    /// - Delete ops with "not found": remove from shadow if still present
    /// - Create ops with "already exists": insert into shadow if missing
    pub fn converge_shadow(&self, err: &IggyError, state: &mut ShadowState) {
        match (self, err) {
            (
                Op::DeleteStream { name },
                IggyError::StreamNameNotFound(_) | IggyError::StreamIdNotFound(_),
            ) => {
                state.apply_delete_stream(name);
            }
            (
                Op::DeleteTopic { stream, topic },
                IggyError::TopicNameNotFound(_, _) | IggyError::TopicIdNotFound(_, _),
            ) => {
                state.apply_delete_topic(stream, topic);
            }
            (
                Op::DeleteConsumerGroup {
                    stream,
                    topic,
                    name,
                },
                IggyError::ConsumerGroupNameNotFound(_, _)
                | IggyError::ConsumerGroupIdNotFound(_, _),
            ) => {
                state.apply_delete_consumer_group(stream, topic, name);
            }

            (Op::CreateStream { name }, IggyError::StreamNameAlreadyExists(_)) => {
                if !state.stream_exists(name) {
                    state.apply_create_stream(name.clone(), ResourceOrigin::Dynamic);
                }
            }
            (
                Op::CreateTopic {
                    stream,
                    name,
                    partitions,
                },
                IggyError::TopicNameAlreadyExists(_, _),
            ) => {
                if !state.topic_exists(stream, name) {
                    state.apply_create_topic(
                        stream,
                        name.clone(),
                        *partitions,
                        ResourceOrigin::Dynamic,
                    );
                }
            }
            (
                Op::CreateConsumerGroup {
                    stream,
                    topic,
                    name,
                },
                IggyError::ConsumerGroupNameAlreadyExists(_, _),
            ) => {
                state.apply_create_consumer_group(stream, topic, name.clone());
            }

            _ => {}
        }

        state.prune_tombstones();
    }
}
