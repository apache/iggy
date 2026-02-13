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

use super::{Op, OpDetail};
use crate::error::LabError;
use crate::payload::{self, PayloadGenerator};
use crate::scenarios::NamespacePrefixes;
use bytes::Bytes;
use iggy::prelude::*;
use iggy_common::IggyError;
use tracing::warn;

fn ident(value: &str) -> Result<Identifier, LabError> {
    Identifier::from_str_value(value).map_err(|_| LabError::InvalidIdentifier(value.to_owned()))
}

fn ident_numeric(value: u32) -> Result<Identifier, LabError> {
    Identifier::numeric(value).map_err(|_| LabError::InvalidIdentifier(value.to_string()))
}

impl Op {
    pub async fn execute(
        &self,
        client: &IggyClient,
        payload_gen: &mut PayloadGenerator,
        prefixes: &NamespacePrefixes,
    ) -> Result<Option<OpDetail>, LabError> {
        match self.stream_name() {
            Some(name) if prefixes.matches(name) => {}
            Some(name) => return Err(LabError::PrefixViolation(name.to_owned())),
            None => match self {
                Op::GetStreams | Op::GetStats => {}
                _ => {
                    return Err(LabError::PrefixViolation(format!(
                        "op {} has no stream_name but is not allowlisted",
                        self.kind_tag()
                    )));
                }
            },
        }

        match self {
            Op::CreateStream { name } => {
                let details = client.create_stream(name).await?;
                Ok(Some(OpDetail::Created { id: details.id }))
            }

            Op::DeleteStream { name } => {
                client.delete_stream(&ident(name)?).await?;
                Ok(None)
            }

            Op::PurgeStream { name } => {
                client.purge_stream(&ident(name)?).await?;
                Ok(None)
            }

            Op::CreateTopic {
                stream,
                name,
                partitions,
            } => {
                let stream_id = ident(stream)?;
                let details = client
                    .create_topic(
                        &stream_id,
                        name,
                        *partitions,
                        CompressionAlgorithm::None,
                        None,
                        IggyExpiry::NeverExpire,
                        MaxTopicSize::Unlimited,
                    )
                    .await?;
                Ok(Some(OpDetail::Created { id: details.id }))
            }

            Op::DeleteTopic { stream, topic } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                client.delete_topic(&stream_id, &topic_id).await?;
                Ok(None)
            }

            Op::PurgeTopic { stream, topic } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                client.purge_topic(&stream_id, &topic_id).await?;
                Ok(None)
            }

            Op::SendMessages {
                stream,
                topic,
                partition,
                count,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                let partitioning = Partitioning::partition_id(*partition);
                let mut messages: Vec<IggyMessage> = Vec::with_capacity(*count as usize);
                for _ in 0..*count {
                    let payload = payload_gen.next();
                    messages.push(
                        IggyMessage::builder()
                            .payload(Bytes::copy_from_slice(payload))
                            .build()
                            .unwrap(),
                    );
                }
                client
                    .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                    .await?;
                Ok(Some(OpDetail::Sent { count: *count }))
            }

            Op::PollMessages {
                stream,
                topic,
                partition,
                count,
                consumer_id,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                let consumer = Consumer::new(ident_numeric(*consumer_id)?);
                let strategy = PollingStrategy::next();
                let polled = client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        Some(*partition),
                        &consumer,
                        &strategy,
                        *count,
                        false,
                    )
                    .await?;
                verify_polled_payloads(&polled);
                Ok(Some(OpDetail::Polled {
                    received: polled.messages.len(),
                    offset: polled.current_offset,
                }))
            }

            Op::DeleteSegments {
                stream,
                topic,
                partition,
                count,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                client
                    .delete_segments(&stream_id, &topic_id, *partition, *count)
                    .await?;
                Ok(None)
            }

            Op::CreatePartitions {
                stream,
                topic,
                count,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                client
                    .create_partitions(&stream_id, &topic_id, *count)
                    .await?;
                Ok(None)
            }

            Op::DeletePartitions {
                stream,
                topic,
                count,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                client
                    .delete_partitions(&stream_id, &topic_id, *count)
                    .await?;
                Ok(None)
            }

            Op::CreateConsumerGroup {
                stream,
                topic,
                name,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                let details = client
                    .create_consumer_group(&stream_id, &topic_id, name)
                    .await?;
                Ok(Some(OpDetail::Created { id: details.id }))
            }

            Op::DeleteConsumerGroup {
                stream,
                topic,
                name,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                let group_id = ident(name)?;
                client
                    .delete_consumer_group(&stream_id, &topic_id, &group_id)
                    .await?;
                Ok(None)
            }

            Op::JoinConsumerGroup {
                stream,
                topic,
                group,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                let group_id = ident(group)?;
                client
                    .join_consumer_group(&stream_id, &topic_id, &group_id)
                    .await?;
                Ok(None)
            }

            Op::LeaveConsumerGroup {
                stream,
                topic,
                group,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                let group_id = ident(group)?;
                client
                    .leave_consumer_group(&stream_id, &topic_id, &group_id)
                    .await?;
                Ok(None)
            }

            Op::GetConsumerGroup {
                stream,
                topic,
                group,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                let group_id = ident(group)?;
                match client
                    .get_consumer_group(&stream_id, &topic_id, &group_id)
                    .await?
                {
                    Some(details) => Ok(Some(OpDetail::GroupInfo {
                        members: details.members_count,
                        partitions: details.partitions_count,
                    })),
                    None => {
                        Err(IggyError::ConsumerGroupNameNotFound(group.clone(), topic_id).into())
                    }
                }
            }

            Op::PollGroupMessages {
                stream,
                topic,
                group,
                count,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                let consumer = Consumer::group(ident(group)?);
                let strategy = PollingStrategy::next();
                let polled = client
                    .poll_messages(
                        &stream_id, &topic_id, None, &consumer, &strategy, *count, true,
                    )
                    .await?;
                verify_polled_payloads(&polled);
                Ok(Some(OpDetail::GroupPolled {
                    received: polled.messages.len(),
                    partition_id: polled.partition_id,
                    offset: polled.current_offset,
                }))
            }

            Op::StoreConsumerOffset {
                stream,
                topic,
                partition,
                offset,
                consumer_id,
            } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                let consumer = Consumer::new(ident_numeric(*consumer_id)?);
                client
                    .store_consumer_offset(
                        &consumer,
                        &stream_id,
                        &topic_id,
                        Some(*partition),
                        *offset,
                    )
                    .await?;
                Ok(None)
            }

            Op::GetStreams => {
                let streams = client.get_streams().await?;
                Ok(Some(OpDetail::StreamList {
                    count: streams.len(),
                }))
            }

            Op::GetStreamDetails { stream } => {
                let stream_id = ident(stream)?;
                match client.get_stream(&stream_id).await? {
                    Some(details) => Ok(Some(OpDetail::StreamInfo {
                        topics: details.topics.len(),
                    })),
                    None => Err(IggyError::StreamNameNotFound(stream.clone()).into()),
                }
            }

            Op::GetTopicDetails { stream, topic } => {
                let stream_id = ident(stream)?;
                let topic_id = ident(topic)?;
                match client.get_topic(&stream_id, &topic_id).await? {
                    Some(details) => Ok(Some(OpDetail::TopicInfo {
                        partitions: details.partitions_count,
                    })),
                    None => Err(IggyError::TopicNameNotFound(stream.clone(), topic.clone()).into()),
                }
            }

            Op::GetStats => {
                let stats = client.get_stats().await?;
                Ok(Some(OpDetail::Stats {
                    streams: stats.streams_count,
                    topics: stats.topics_count,
                    partitions: stats.partitions_count,
                }))
            }
        }
    }
}

/// Best-effort CRC verification of polled messages. Warns on corruption but does not
/// fail the operation - messages produced by scenario setup (sentinels) or external
/// sources won't have the chaos header format and are silently skipped.
fn verify_polled_payloads(polled: &PolledMessages) {
    for msg in &polled.messages {
        if msg.payload.len() < 16 {
            continue;
        }
        if let Err(e) = payload::verify_payload(&msg.payload) {
            warn!(
                partition_id = polled.partition_id,
                offset = msg.header.offset,
                "Payload corruption detected: {e}"
            );
        }
    }
}
