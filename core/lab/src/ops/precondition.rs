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

use super::Op;
use crate::shadow::ShadowState;

#[allow(dead_code)]
impl Op {
    /// Check whether this op's preconditions are met according to our shadow state.
    /// A false return means the op would definitely fail, so the worker should skip it.
    pub fn precondition_met(&self, state: &ShadowState) -> bool {
        match self {
            Op::CreateStream { name } => !state.stream_exists(name),
            Op::DeleteStream { name } | Op::PurgeStream { name } => state.stream_exists(name),

            Op::CreateTopic { stream, name, .. } => {
                state.stream_exists(stream) && !state.topic_exists(stream, name)
            }
            Op::DeleteTopic { stream, topic } | Op::PurgeTopic { stream, topic } => {
                state.topic_exists(stream, topic)
            }

            Op::SendMessages {
                stream,
                topic,
                partition,
                ..
            }
            | Op::PollMessages {
                stream,
                topic,
                partition,
                ..
            } => state
                .get_topic(stream, topic)
                .is_some_and(|t| *partition >= 1 && *partition <= t.partitions),

            Op::DeleteSegments {
                stream,
                topic,
                partition,
                ..
            } => state
                .get_topic(stream, topic)
                .is_some_and(|t| *partition >= 1 && *partition <= t.partitions),

            Op::CreatePartitions { stream, topic, .. } => state.topic_exists(stream, topic),

            Op::DeletePartitions {
                stream,
                topic,
                count,
            } => state
                .get_topic(stream, topic)
                .is_some_and(|t| t.partitions > *count),

            Op::CreateConsumerGroup {
                stream,
                topic,
                name,
            } => state
                .get_topic(stream, topic)
                .is_some_and(|t| !t.consumer_groups.contains(name)),

            Op::DeleteConsumerGroup {
                stream,
                topic,
                name,
            } => state
                .get_topic(stream, topic)
                .is_some_and(|t| t.consumer_groups.contains(name)),

            Op::JoinConsumerGroup {
                stream,
                topic,
                group,
            }
            | Op::LeaveConsumerGroup {
                stream,
                topic,
                group,
            }
            | Op::GetConsumerGroup {
                stream,
                topic,
                group,
            }
            | Op::PollGroupMessages {
                stream,
                topic,
                group,
                ..
            } => state
                .get_topic(stream, topic)
                .is_some_and(|t| t.consumer_groups.contains(group)),

            Op::StoreConsumerOffset {
                stream,
                topic,
                partition,
                ..
            } => state
                .get_topic(stream, topic)
                .is_some_and(|t| *partition >= 1 && *partition <= t.partitions),

            Op::GetStreams | Op::GetStats => true,
            Op::GetStreamDetails { stream } => state.stream_exists(stream),
            Op::GetTopicDetails { stream, topic } => state.topic_exists(stream, topic),
        }
    }
}
