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

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::{Table, presets::ASCII_NO_BORDERS};
use iggy_common::Client;
use iggy_common::{Consumer, Identifier, Partition};
use tracing::{Level, event};

pub struct GetConsumerGroupCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    group_id: Identifier,
}

impl GetConsumerGroupCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, consumer_group_id: Identifier) -> Self {
        Self {
            stream_id,
            topic_id,
            group_id: consumer_group_id,
        }
    }
}

#[async_trait]
impl CliCommand for GetConsumerGroupCmd {
    fn explain(&self) -> String {
        format!(
            "get consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
            self.group_id, self.topic_id, self.stream_id,
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let consumer_group = client
            .get_consumer_group(&self.stream_id, &self.topic_id, &self.group_id)
            .await
            .with_context(|| {
                format!(
                    "Problem getting consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
                    self.group_id, self.topic_id, self.stream_id
                )
            })?;

        if consumer_group.is_none() {
            event!(target: PRINT_TARGET, Level::INFO, "Consumer group with ID: {} was not found", self.group_id);
            return Ok(());
        }

        let consumer_group = consumer_group.unwrap();

        let topic = client
            .get_topic(&self.stream_id, &self.topic_id)
            .await
            .with_context(|| {
                format!(
                    "Problem getting topic with ID: {} in stream with ID: {}",
                    self.topic_id, self.stream_id
                )
            })?
            .with_context(|| {
                format!(
                    "Topic with ID: {} in stream with ID: {} was not found",
                    self.topic_id, self.stream_id
                )
            })?;

        let consumer = Consumer::group(self.group_id.clone());
        if consumer_group.partitions_count > iggy_common::MAX_PARTITIONS_COUNT {
            anyhow::bail!(
                "Consumer group with ID: {} reports partitions count {} above the limit {}",
                self.group_id,
                consumer_group.partitions_count,
                iggy_common::MAX_PARTITIONS_COUNT
            );
        }
        // Size the indexes from the topic partition list. `partitions_count` is a
        // wire field, and a huge value must not allocate before the missing-id check.
        let index_len = topic.partitions.len();
        let mut partitions_by_id: Vec<Option<&Partition>> = vec![None; index_len];
        for partition in &topic.partitions {
            if let Some(slot) = partitions_by_id.get_mut(partition.id as usize) {
                *slot = Some(partition);
            }
        }

        let mut owners: Vec<Option<u32>> = vec![None; index_len];
        let mut members_table = Table::new();
        let render_members = consumer_group.members_count > 0;
        if render_members {
            members_table.load_style(ASCII_NO_BORDERS);
            members_table.set_header(vec!["Member id", "Partitions count", "Partitions"]);
        }
        for member in &consumer_group.members {
            for partition_id in &member.partitions {
                if let Some(slot) = owners.get_mut(*partition_id as usize) {
                    *slot = Some(member.id);
                }
            }
            if render_members {
                members_table.add_row(vec![
                    format!("{}", member.id).as_str(),
                    format!("{}", member.partitions_count).as_str(),
                    member
                        .partitions
                        .iter()
                        .map(|partition_id| format!("{partition_id}"))
                        .collect::<Vec<String>>()
                        .join(", ")
                        .as_str(),
                ]);
            }
        }

        let mut partitions_table = Table::new();
        partitions_table.load_style(ASCII_NO_BORDERS);
        partitions_table.set_header(vec![
            "Partition",
            "Member",
            "Current offset",
            "Stored offset",
            "Lag",
        ]);

        for partition_id in 0..consumer_group.partitions_count {
            let partition = partitions_by_id
                .get(partition_id as usize)
                .copied()
                .flatten()
                .with_context(|| {
                    format!(
                        "Partition with ID: {} in topic with ID: {} was not found",
                        partition_id, self.topic_id
                    )
                })?;

            let consumer_offset = client
                .get_consumer_offset(
                    &consumer,
                    &self.stream_id,
                    &self.topic_id,
                    Some(partition_id),
                )
                .await
                .with_context(|| {
                    format!(
                        "Problem getting consumer offset for consumer group with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {}",
                        self.group_id, self.stream_id, self.topic_id, partition_id
                    )
                })?;

            let member = owners
                .get(partition_id as usize)
                .copied()
                .flatten()
                .map(|member_id| format!("{member_id}"))
                .unwrap_or_else(|| "-".to_string());

            let (current_offset, stored_offset, lag) = match consumer_offset {
                Some(offset_info) => {
                    let lag = offset_info.current_offset - offset_info.stored_offset;
                    (
                        format!("{}", offset_info.current_offset),
                        format!("{}", offset_info.stored_offset),
                        format!("{}", lag),
                    )
                }
                None => (
                    format!("{}", partition.current_offset),
                    "-".to_string(),
                    format!("{}", partition.messages_count),
                ),
            };

            partitions_table.add_row(vec![
                format!("{}", partition_id),
                member,
                current_offset,
                stored_offset,
                lag,
            ]);
        }

        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec![
            "Consumer group id",
            format!("{}", consumer_group.id).as_str(),
        ]);
        table.add_row(vec!["Consumer group name", consumer_group.name.as_str()]);
        table.add_row(vec![
            "Partitions count",
            format!("{}", consumer_group.partitions_count).as_str(),
        ]);
        table.add_row(vec![
            "Members count",
            format!("{}", consumer_group.members_count).as_str(),
        ]);

        if render_members {
            table.add_row(vec!["Members", members_table.to_string().as_str()]);
        }

        table.add_row(vec!["Partitions", partitions_table.to_string().as_str()]);

        event!(target: PRINT_TARGET, Level::INFO,"{table}");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CliCommand, GetConsumerGroupCmd};
    use async_broadcast::Receiver;
    use async_trait::async_trait;
    use iggy_common::{
        Client, ClientInfo, ClientInfoDetails, ClusterClient, ClusterMetadata,
        CompressionAlgorithm, Consumer, ConsumerGroup, ConsumerGroupClient, ConsumerGroupDetails,
        ConsumerOffsetClient, ConsumerOffsetInfo, ConsumerPosition, DiagnosticEvent, Identifier,
        IdentityInfo, IggyByteSize, IggyError, IggyExpiry, IggyMessage, IggyTimestamp,
        MAX_PARTITIONS_COUNT, MaxTopicSize, MessageClient, NonZeroIggyDuration, OptionSpec,
        OptionsScope, Partition, PartitionClient, PartitionContext, Partitioning, Permissions,
        PersonalAccessTokenClient, PersonalAccessTokenExpiry, PersonalAccessTokenInfo,
        PolledMessages, PollingStrategy, RawPersonalAccessToken, SegmentClient,
        SendMessagesResponse, Snapshot, SnapshotCompression, Stats, Stream, StreamClient,
        StreamDetails, StreamUpdateOptions, SystemClient, SystemSnapshotType, Topic, TopicClient,
        TopicCreateOptions, TopicDetails, TopicUpdateOptions, UserClient, UserInfo,
        UserInfoDetails, UserStatus, UserUpdateOptions,
    };

    #[derive(Debug, Clone, Copy)]
    enum TopicReply {
        Error,
        Missing,
        Empty,
        OnePartition,
    }

    #[derive(Debug)]
    struct ScriptedClient {
        partitions_count: u32,
        topic: TopicReply,
        offset_fails: bool,
    }

    fn fail<T>() -> Result<T, IggyError> {
        Err(IggyError::Error)
    }

    fn group(partitions_count: u32) -> ConsumerGroupDetails {
        ConsumerGroupDetails {
            id: 3,
            name: "group".to_owned(),
            partitions_count,
            members_count: 0,
            members: Vec::new(),
        }
    }

    fn topic_details(partitions: Vec<Partition>) -> TopicDetails {
        TopicDetails {
            id: 2,
            created_at: IggyTimestamp::zero(),
            name: "topic".to_owned(),
            size: IggyByteSize::default(),
            message_expiry: IggyExpiry::NeverExpire,
            compression_algorithm: CompressionAlgorithm::None,
            max_topic_size: MaxTopicSize::Unlimited,
            messages_count: 0,
            partitions_count: u32::try_from(partitions.len()).unwrap(),
            partitions,
            options: Default::default(),
        }
    }

    fn partition_zero() -> Partition {
        Partition {
            id: 0,
            created_at: IggyTimestamp::zero(),
            segments_count: 1,
            current_offset: 0,
            size: IggyByteSize::default(),
            messages_count: 0,
            context: PartitionContext::default(),
        }
    }

    fn command() -> GetConsumerGroupCmd {
        GetConsumerGroupCmd::new(
            Identifier::numeric(1).unwrap(),
            Identifier::numeric(2).unwrap(),
            Identifier::numeric(3).unwrap(),
        )
    }

    async fn error_from(client: &ScriptedClient) -> anyhow::Error {
        let mut cmd = command();
        cmd.execute_cmd(client)
            .await
            .expect_err("consumer group get should fail")
    }

    #[async_trait]
    impl ClusterClient for ScriptedClient {
        async fn get_cluster_metadata(&self) -> Result<ClusterMetadata, IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl SystemClient for ScriptedClient {
        async fn get_stats(&self) -> Result<Stats, IggyError> {
            fail()
        }

        async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
            fail()
        }

        async fn get_client(
            &self,
            _client_id: u32,
        ) -> Result<Option<ClientInfoDetails>, IggyError> {
            fail()
        }

        async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
            fail()
        }

        async fn describe_options(
            &self,
            _scope: OptionsScope,
        ) -> Result<Vec<OptionSpec>, IggyError> {
            fail()
        }

        async fn ping(&self) -> Result<(), IggyError> {
            fail()
        }

        async fn heartbeat_interval(&self) -> NonZeroIggyDuration {
            NonZeroIggyDuration::ONE_SECOND
        }

        async fn snapshot(
            &self,
            _compression: SnapshotCompression,
            _snapshot_types: Vec<SystemSnapshotType>,
        ) -> Result<Snapshot, IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl UserClient for ScriptedClient {
        async fn get_user(
            &self,
            _user_id: &Identifier,
        ) -> Result<Option<UserInfoDetails>, IggyError> {
            fail()
        }

        async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
            fail()
        }

        async fn create_user(
            &self,
            _username: &str,
            _password: &str,
            _status: UserStatus,
            _permissions: Option<Permissions>,
        ) -> Result<UserInfoDetails, IggyError> {
            fail()
        }

        async fn delete_user(&self, _user_id: &Identifier) -> Result<(), IggyError> {
            fail()
        }

        async fn update_user(
            &self,
            _user_id: &Identifier,
            _username: Option<&str>,
            _status: Option<UserStatus>,
            _options: &UserUpdateOptions,
        ) -> Result<(), IggyError> {
            fail()
        }

        async fn update_permissions(
            &self,
            _user_id: &Identifier,
            _permissions: Option<Permissions>,
        ) -> Result<(), IggyError> {
            fail()
        }

        async fn change_password(
            &self,
            _user_id: &Identifier,
            _current_password: &str,
            _new_password: &str,
        ) -> Result<(), IggyError> {
            fail()
        }

        async fn login_user(
            &self,
            _username: &str,
            _password: &str,
        ) -> Result<IdentityInfo, IggyError> {
            fail()
        }

        async fn logout_user(&self) -> Result<(), IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl PersonalAccessTokenClient for ScriptedClient {
        async fn get_personal_access_tokens(
            &self,
        ) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
            fail()
        }

        async fn create_personal_access_token(
            &self,
            _name: &str,
            _expiry: PersonalAccessTokenExpiry,
        ) -> Result<RawPersonalAccessToken, IggyError> {
            fail()
        }

        async fn delete_personal_access_token(&self, _name: &str) -> Result<(), IggyError> {
            fail()
        }

        async fn login_with_personal_access_token(
            &self,
            _token: &str,
        ) -> Result<IdentityInfo, IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl StreamClient for ScriptedClient {
        async fn get_stream(
            &self,
            _stream_id: &Identifier,
        ) -> Result<Option<StreamDetails>, IggyError> {
            fail()
        }

        async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
            fail()
        }

        async fn create_stream(&self, _name: &str) -> Result<StreamDetails, IggyError> {
            fail()
        }

        async fn update_stream(
            &self,
            _stream_id: &Identifier,
            _name: &str,
            _options: &StreamUpdateOptions,
        ) -> Result<(), IggyError> {
            fail()
        }

        async fn delete_stream(&self, _stream_id: &Identifier) -> Result<(), IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl TopicClient for ScriptedClient {
        async fn get_topic(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
        ) -> Result<Option<TopicDetails>, IggyError> {
            match self.topic {
                TopicReply::Error => Err(IggyError::Error),
                TopicReply::Missing => Ok(None),
                TopicReply::Empty => Ok(Some(topic_details(Vec::new()))),
                TopicReply::OnePartition => Ok(Some(topic_details(vec![partition_zero()]))),
            }
        }

        async fn get_topics(&self, _stream_id: &Identifier) -> Result<Vec<Topic>, IggyError> {
            fail()
        }

        async fn create_topic(
            &self,
            _stream_id: &Identifier,
            _name: &str,
            _options: &TopicCreateOptions,
        ) -> Result<TopicDetails, IggyError> {
            fail()
        }

        async fn update_topic(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _name: &str,
            _options: &TopicUpdateOptions,
        ) -> Result<(), IggyError> {
            fail()
        }

        async fn delete_topic(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
        ) -> Result<(), IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl PartitionClient for ScriptedClient {
        async fn create_partitions(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _partitions_count: u32,
        ) -> Result<(), IggyError> {
            fail()
        }

        async fn delete_partitions(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _partitions_count: u32,
        ) -> Result<(), IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl SegmentClient for ScriptedClient {
        async fn delete_segments(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _partition_id: u32,
            _segments_count: u32,
        ) -> Result<(), IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl MessageClient for ScriptedClient {
        async fn poll_messages(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _partition_id: Option<u32>,
            _consumer: &Consumer,
            _strategy: &PollingStrategy,
            _count: u32,
            _auto_commit: bool,
        ) -> Result<PolledMessages, IggyError> {
            fail()
        }

        async fn send_messages(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _partitioning: &Partitioning,
            _messages: &mut [IggyMessage],
        ) -> Result<SendMessagesResponse, IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl ConsumerOffsetClient for ScriptedClient {
        async fn store_consumer_position(
            &self,
            _consumer: &Consumer,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _position: ConsumerPosition,
        ) -> Result<(), IggyError> {
            fail()
        }

        async fn store_consumer_offset(
            &self,
            _consumer: &Consumer,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _partition_id: Option<u32>,
            _offset: u64,
        ) -> Result<(), IggyError> {
            fail()
        }

        async fn get_consumer_offset(
            &self,
            _consumer: &Consumer,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _partition_id: Option<u32>,
        ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
            if self.offset_fails {
                Err(IggyError::Error)
            } else {
                Ok(None)
            }
        }

        async fn delete_consumer_offset(
            &self,
            _consumer: &Consumer,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _partition_id: Option<u32>,
        ) -> Result<(), IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl ConsumerGroupClient for ScriptedClient {
        async fn get_consumer_group(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _group_id: &Identifier,
        ) -> Result<Option<ConsumerGroupDetails>, IggyError> {
            Ok(Some(group(self.partitions_count)))
        }

        async fn get_consumer_groups(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
        ) -> Result<Vec<ConsumerGroup>, IggyError> {
            fail()
        }

        async fn create_consumer_group(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _name: &str,
        ) -> Result<ConsumerGroupDetails, IggyError> {
            fail()
        }

        async fn delete_consumer_group(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _group_id: &Identifier,
        ) -> Result<(), IggyError> {
            fail()
        }

        async fn join_consumer_group(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _group_id: &Identifier,
        ) -> Result<(), IggyError> {
            fail()
        }

        async fn leave_consumer_group(
            &self,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _group_id: &Identifier,
        ) -> Result<(), IggyError> {
            fail()
        }
    }

    #[async_trait]
    impl Client for ScriptedClient {
        async fn connect(&self) -> Result<(), IggyError> {
            fail()
        }

        async fn disconnect(&self) -> Result<(), IggyError> {
            fail()
        }

        async fn shutdown(&self) -> Result<(), IggyError> {
            fail()
        }

        async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
            async_broadcast::broadcast(1).1
        }
    }

    #[tokio::test]
    async fn should_report_topic_fetch_failure() {
        let error = error_from(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::Error,
            offset_fails: false,
        })
        .await;

        assert!(error.to_string().contains("Problem getting topic"));
    }

    #[tokio::test]
    async fn should_report_missing_topic() {
        let error = error_from(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::Missing,
            offset_fails: false,
        })
        .await;

        assert!(error.to_string().contains("was not found"));
    }

    #[tokio::test]
    async fn should_report_missing_partition() {
        let error = error_from(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::Empty,
            offset_fails: false,
        })
        .await;

        assert!(error.to_string().contains("Partition with ID: 0"));
    }

    #[tokio::test]
    async fn should_report_offset_fetch_failure() {
        let error = error_from(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::OnePartition,
            offset_fails: true,
        })
        .await;

        assert!(
            error
                .to_string()
                .contains("Problem getting consumer offset")
        );
    }

    #[tokio::test]
    async fn should_reject_partitions_count_above_the_limit() {
        let error = error_from(&ScriptedClient {
            partitions_count: MAX_PARTITIONS_COUNT + 1,
            topic: TopicReply::Empty,
            offset_fails: false,
        })
        .await;

        assert!(error.to_string().contains("above the limit"));
    }
}
