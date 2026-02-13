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

use super::{Op, OpKind};
use crate::safe_name::SafeResourceName;
use crate::scenarios::{LaneNamespace, NamespacePrefixes, NamespaceScope};
use crate::shadow::ShadowState;
use rand::{Rng, RngExt};

const DEFAULT_PARTITIONS: u32 = 3;

fn pick_random_group(
    state: &ShadowState,
    rng: &mut impl Rng,
    scope: NamespaceScope,
    namespace: LaneNamespace,
) -> Option<(String, String, String)> {
    let (stream, topic) = state.random_topic(rng, scope, namespace)?;
    let groups = &state.streams[stream].topics[topic].consumer_groups;
    if groups.is_empty() {
        return None;
    }
    let idx = rng.random_range(0..groups.len());
    let group = groups.iter().nth(idx)?.to_owned();
    Some((stream.to_owned(), topic.to_owned(), group))
}

impl Op {
    /// Generate a concrete Op from an OpKind, resolving targets from the shadow state.
    /// Returns None if the precondition for this kind cannot be met.
    ///
    /// Resource-destructive ops (delete/purge stream/topic) are forced to target
    /// churn-namespace resources regardless of the lane's configured namespace.
    #[allow(clippy::too_many_arguments)]
    pub fn generate(
        kind: OpKind,
        state: &ShadowState,
        rng: &mut impl Rng,
        prefixes: &NamespacePrefixes,
        namespace: LaneNamespace,
        messages_per_batch: u32,
        scope: NamespaceScope,
        consumer_id: u32,
    ) -> Option<Self> {
        let effective_namespace = if kind.is_resource_destructive() {
            LaneNamespace::Churn
        } else {
            namespace
        };

        match kind {
            OpKind::CreateStream => {
                let prefix = prefixes.create_prefix(namespace);
                let name = SafeResourceName::random(prefix, rng);
                Some(Op::CreateStream {
                    name: name.into_inner(),
                })
            }

            OpKind::DeleteStream => {
                let name = state.random_stream(rng, scope, effective_namespace)?;
                Some(Op::DeleteStream {
                    name: name.to_owned(),
                })
            }

            OpKind::PurgeStream => {
                let name = state.random_stream(rng, scope, effective_namespace)?;
                Some(Op::PurgeStream {
                    name: name.to_owned(),
                })
            }

            OpKind::CreateTopic => {
                let stream = state.random_stream(rng, scope, effective_namespace)?;
                let name = SafeResourceName::random(&prefixes.base, rng);
                Some(Op::CreateTopic {
                    stream: stream.to_owned(),
                    name: name.into_inner(),
                    partitions: DEFAULT_PARTITIONS,
                })
            }

            OpKind::DeleteTopic => {
                let (stream, topic) = state.random_topic(rng, scope, effective_namespace)?;
                Some(Op::DeleteTopic {
                    stream: stream.to_owned(),
                    topic: topic.to_owned(),
                })
            }

            OpKind::PurgeTopic => {
                let (stream, topic) = state.random_topic(rng, scope, effective_namespace)?;
                Some(Op::PurgeTopic {
                    stream: stream.to_owned(),
                    topic: topic.to_owned(),
                })
            }

            OpKind::SendMessages => {
                let (stream, topic, partitions) =
                    state.random_topic_with_partitions(rng, scope, effective_namespace)?;
                if partitions == 0 {
                    return None;
                }
                let partition = rng.random_range(1..=partitions);
                Some(Op::SendMessages {
                    stream: stream.to_owned(),
                    topic: topic.to_owned(),
                    partition,
                    count: messages_per_batch,
                })
            }

            OpKind::PollMessages => {
                let (stream, topic, partitions) =
                    state.random_topic_with_partitions(rng, scope, effective_namespace)?;
                if partitions == 0 {
                    return None;
                }
                let partition = rng.random_range(1..=partitions);
                Some(Op::PollMessages {
                    stream: stream.to_owned(),
                    topic: topic.to_owned(),
                    partition,
                    count: messages_per_batch,
                    consumer_id,
                })
            }

            OpKind::DeleteSegments => {
                let (stream, topic, partitions) =
                    state.random_topic_with_partitions(rng, scope, effective_namespace)?;
                if partitions == 0 {
                    return None;
                }
                let partition = rng.random_range(1..=partitions);
                Some(Op::DeleteSegments {
                    stream: stream.to_owned(),
                    topic: topic.to_owned(),
                    partition,
                    count: 1,
                })
            }

            OpKind::CreatePartitions => {
                let (stream, topic) = state.random_topic(rng, scope, effective_namespace)?;
                Some(Op::CreatePartitions {
                    stream: stream.to_owned(),
                    topic: topic.to_owned(),
                    count: 1,
                })
            }

            OpKind::DeletePartitions => {
                let (stream, topic, partitions) =
                    state.random_topic_with_partitions(rng, scope, effective_namespace)?;
                if partitions <= 1 {
                    return None;
                }
                Some(Op::DeletePartitions {
                    stream: stream.to_owned(),
                    topic: topic.to_owned(),
                    count: 1,
                })
            }

            OpKind::CreateConsumerGroup => {
                let (stream, topic) = state.random_topic(rng, scope, effective_namespace)?;
                let name = SafeResourceName::random(&prefixes.base, rng);
                Some(Op::CreateConsumerGroup {
                    stream: stream.to_owned(),
                    topic: topic.to_owned(),
                    name: name.into_inner(),
                })
            }

            OpKind::DeleteConsumerGroup => {
                let (stream, topic, group) =
                    pick_random_group(state, rng, scope, effective_namespace)?;
                Some(Op::DeleteConsumerGroup {
                    stream,
                    topic,
                    name: group,
                })
            }

            OpKind::JoinConsumerGroup => {
                let (stream, topic, group) =
                    pick_random_group(state, rng, scope, effective_namespace)?;
                Some(Op::JoinConsumerGroup {
                    stream,
                    topic,
                    group,
                })
            }

            OpKind::LeaveConsumerGroup => {
                let (stream, topic, group) =
                    pick_random_group(state, rng, scope, effective_namespace)?;
                Some(Op::LeaveConsumerGroup {
                    stream,
                    topic,
                    group,
                })
            }

            OpKind::GetConsumerGroup => {
                let (stream, topic, group) =
                    pick_random_group(state, rng, scope, effective_namespace)?;
                Some(Op::GetConsumerGroup {
                    stream,
                    topic,
                    group,
                })
            }

            OpKind::PollGroupMessages => {
                let (stream, topic, group) =
                    pick_random_group(state, rng, scope, effective_namespace)?;
                Some(Op::PollGroupMessages {
                    stream,
                    topic,
                    group,
                    count: messages_per_batch,
                })
            }

            OpKind::StoreConsumerOffset => {
                let (stream, topic, partitions) =
                    state.random_topic_with_partitions(rng, scope, effective_namespace)?;
                if partitions == 0 {
                    return None;
                }
                let partition = rng.random_range(1..=partitions);
                let offset = rng.random_range(0..1000);
                Some(Op::StoreConsumerOffset {
                    stream: stream.to_owned(),
                    topic: topic.to_owned(),
                    partition,
                    offset,
                    consumer_id,
                })
            }

            OpKind::GetStreams => Some(Op::GetStreams),

            OpKind::GetStreamDetails => {
                let stream = state.random_stream(rng, scope, effective_namespace)?;
                Some(Op::GetStreamDetails {
                    stream: stream.to_owned(),
                })
            }

            OpKind::GetTopicDetails => {
                let (stream, topic) = state.random_topic(rng, scope, effective_namespace)?;
                Some(Op::GetTopicDetails {
                    stream: stream.to_owned(),
                    topic: topic.to_owned(),
                })
            }

            OpKind::GetStats => Some(Op::GetStats),
        }
    }
}
