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

use std::collections::HashMap;

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::{Table, presets::ASCII_NO_BORDERS};
use iggy_common::Client;
use iggy_common::{Consumer, ConsumerGroupDetails, Identifier};
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

        // Lookups can be names. Offset reads stay on the group, stream, and
        // topic these replies returned, even if a name is recreated first.
        let consumer = Consumer::group(Identifier::numeric(consumer_group.id)?);
        let mut owners = HashMap::with_capacity(
            consumer_group
                .members
                .iter()
                .map(|member| member.partitions.len())
                .sum(),
        );
        let mut members_table = Table::new();
        let render_members = consumer_group.members_count > 0;
        if render_members {
            members_table.load_style(ASCII_NO_BORDERS);
            members_table.set_header(vec!["Member id", "Partitions count", "Partitions"]);
        }
        for member in &consumer_group.members {
            for partition_id in &member.partitions {
                owners.insert(*partition_id, member.id);
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

        let log_without_partitions = || {
            log_consumer_group(
                &consumer_group,
                render_members.then_some(&members_table),
                None,
            );
        };

        let stream = match client.get_stream(&self.stream_id).await {
            Ok(Some(stream)) => stream,
            Ok(None) => {
                log_without_partitions();
                return Err(anyhow::anyhow!(
                    "Stream with ID: {} was not found",
                    self.stream_id
                ));
            }
            Err(error) => {
                log_without_partitions();
                return Err(error).with_context(|| {
                    format!("Problem getting stream with ID: {}", self.stream_id)
                });
            }
        };
        let stream_id = Identifier::numeric(stream.id)?;

        let topic = match client.get_topic(&stream_id, &self.topic_id).await {
            Ok(Some(topic)) => topic,
            Ok(None) => {
                log_without_partitions();
                return Err(anyhow::anyhow!(
                    "Topic with ID: {} in stream with ID: {} was not found",
                    self.topic_id,
                    self.stream_id
                ));
            }
            Err(error) => {
                log_without_partitions();
                return Err(error).with_context(|| {
                    format!(
                        "Problem getting topic with ID: {} in stream with ID: {}",
                        self.topic_id, self.stream_id
                    )
                });
            }
        };

        let mut partitions_table = Table::new();
        partitions_table.load_style(ASCII_NO_BORDERS);
        partitions_table.set_header(vec![
            "Partition",
            "Member",
            "Current offset",
            "Stored offset",
            "Lag",
        ]);

        let topic_id = Identifier::numeric(topic.id)?;
        let mut partitions = topic.partitions;
        partitions.sort_by_key(|partition| partition.id);
        let mut offset_error = None;
        for partition in &partitions {
            let partition_id = partition.id;
            let member = owners
                .get(&partition_id)
                .map(|member_id| format!("{member_id}"))
                .unwrap_or_else(|| "-".to_string());
            let consumer_offset = client
                .get_consumer_offset(&consumer, &stream_id, &topic_id, Some(partition_id))
                .await
                .with_context(|| {
                    format!(
                        "Problem getting consumer offset for consumer group with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {}",
                        self.group_id, self.stream_id, self.topic_id, partition_id
                    )
                });

            let (current_offset, stored_offset, lag) = match consumer_offset {
                Ok(Some(offset_info)) => {
                    // The topic head is an earlier read. This reply samples the
                    // head and the stored offset together. Recovery can keep a
                    // stored offset above that head when acked messages were
                    // not flushed, and subtracting then underflows.
                    let lag = offset_info
                        .current_offset
                        .saturating_sub(offset_info.stored_offset);
                    (
                        format!("{}", offset_info.current_offset),
                        format!("{}", offset_info.stored_offset),
                        format!("{lag}"),
                    )
                }
                Ok(None) => {
                    // The empty reply drops the primary head. Local topic
                    // counters stay 0 until a report, so they are not the lag.
                    ("-".to_owned(), "-".to_owned(), "-".to_owned())
                }
                Err(error) => {
                    if offset_error.is_none() {
                        offset_error = Some(error);
                    }
                    ("error".to_owned(), "error".to_owned(), "error".to_owned())
                }
            };

            partitions_table.add_row(vec![
                format!("{}", partition_id),
                member,
                current_offset,
                stored_offset,
                lag,
            ]);
        }

        log_consumer_group(
            &consumer_group,
            render_members.then_some(&members_table),
            Some(&partitions_table),
        );

        if let Some(error) = offset_error {
            return Err(error);
        }
        Ok(())
    }
}

fn log_consumer_group(
    consumer_group: &ConsumerGroupDetails,
    members: Option<&Table>,
    partitions: Option<&Table>,
) {
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
    if let Some(members) = members {
        table.add_row(vec!["Members", members.to_string().as_str()]);
    }
    // The members cell lists every assigned partition. A partition table in
    // the same property table would pad each of its rows out to that width.
    event!(target: PRINT_TARGET, Level::INFO, "{table}");
    if let Some(partitions) = partitions {
        event!(target: PRINT_TARGET, Level::INFO, "{partitions}");
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use async_broadcast::Receiver;
    use async_trait::async_trait;
    use iggy_common::{
        Checkpoint, Client, ClientInfo, ClientInfoDetails, ClusterClient, ClusterMetadata,
        Consumer, ConsumerGroup, ConsumerGroupClient, ConsumerGroupDetails, ConsumerGroupMember,
        ConsumerOffsetClient, ConsumerOffsetInfo, DiagnosticEvent, Identifier, IdentityInfo,
        IggyByteSize, IggyError, IggyMessage, IggyTimestamp, MAX_PARTITIONS_COUNT, MessageClient,
        MetadataChanges, MetadataChangesQuery, NonZeroIggyDuration, OptionSpec, OptionsScope,
        Partition, PartitionClient, PartitionContext, Partitioning, Permissions,
        PersonalAccessTokenClient, PersonalAccessTokenExpiry, PersonalAccessTokenInfo, PollOptions,
        PolledMessages, PollingStrategy, RawPersonalAccessToken, SegmentClient,
        SendMessagesResponse, Snapshot, SnapshotCompression, Stats, Stream, StreamClient,
        StreamCreateOptions, StreamDetails, StreamUpdateOptions, SystemClient, SystemSnapshotType,
        Topic, TopicClient, TopicCreateOptions, TopicDetails, TopicUpdateOptions, UserClient,
        UserCreateOptions, UserInfo, UserInfoDetails, UserStatus, UserUpdateOptions,
    };
    use tracing_subscriber::fmt::MakeWriter;

    use super::{CliCommand, GetConsumerGroupCmd};

    #[derive(Debug, Clone)]
    enum TopicReply {
        Error,
        Missing,
        Empty,
        OnePartition,
        Count(u32),
        Partitions(Vec<Partition>),
    }

    #[derive(Debug)]
    struct ScriptedClient {
        partitions_count: u32,
        topic: TopicReply,
        offset_fails_for: Option<u32>,
        /// `(current_offset, stored_offset)` returned for every partition.
        offset: Option<(u64, u64)>,
        members: Vec<ConsumerGroupMember>,
        /// Numeric id returned by `get_consumer_group`.
        group_id: u32,
        stream_missing: bool,
        /// Stream, topic, and group ids passed to `get_consumer_offset`, in call order.
        offset_reads: Mutex<Vec<OffsetRead>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct OffsetRead {
        stream_id: Identifier,
        topic_id: Identifier,
        group_id: Identifier,
    }

    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl Write for LogBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log buffer").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for LogBuffer {
        type Writer = LogBuffer;

        fn make_writer(&'a self) -> Self::Writer {
            LogBuffer(Arc::clone(&self.0))
        }
    }

    fn fail<T>() -> Result<T, IggyError> {
        Err(IggyError::Error)
    }

    fn partition(id: u32) -> Partition {
        let mut partition = partition_zero();
        partition.id = id;
        partition
    }

    fn topic_details(partitions: Vec<Partition>) -> TopicDetails {
        TopicDetails {
            id: 2,
            created_at: IggyTimestamp::zero(),
            name: "topic".to_owned(),
            size: IggyByteSize::default(),
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
            members: Vec::new(),
            in_sync_members: Vec::new(),
        }
    }

    fn command() -> GetConsumerGroupCmd {
        GetConsumerGroupCmd::new(
            Identifier::numeric(1).unwrap(),
            Identifier::numeric(2).unwrap(),
            Identifier::numeric(3).unwrap(),
        )
    }

    async fn with_logs(client: &ScriptedClient) -> (String, anyhow::Result<()>) {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_level(false)
            .with_writer(LogBuffer(Arc::clone(&logs)))
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let mut cmd = command();
        let result = cmd.execute_cmd(client).await;
        drop(_guard);
        let output =
            String::from_utf8(logs.lock().expect("log buffer").clone()).expect("log is utf-8");
        (output, result)
    }

    async fn printed_error(client: &ScriptedClient) -> (String, anyhow::Error) {
        let (output, result) = with_logs(client).await;
        (output, result.expect_err("consumer group get should fail"))
    }

    async fn rendered(client: &ScriptedClient) -> String {
        let (output, result) = with_logs(client).await;
        result.expect("consumer group get should print the group");
        output
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

        async fn get_metadata_changes(
            &self,
            _query: &MetadataChangesQuery,
        ) -> Result<MetadataChanges, IggyError> {
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
            _options: &UserCreateOptions,
        ) -> Result<UserInfoDetails, IggyError> {
            fail()
        }

        async fn delete_user(&self, _user_id: &Identifier) -> Result<(), IggyError> {
            fail()
        }

        async fn update_user(
            &self,
            _user_id: &Identifier,
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
            if self.stream_missing {
                return Ok(None);
            }
            Ok(Some(StreamDetails {
                id: 1,
                created_at: IggyTimestamp::zero(),
                name: "stream".to_owned(),
                size: IggyByteSize::default(),
                messages_count: 0,
                topics_count: 0,
                topics: Vec::new(),
                options: Default::default(),
            }))
        }

        async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
            fail()
        }

        async fn create_stream(
            &self,
            _name: &str,
            _options: &StreamCreateOptions,
        ) -> Result<StreamDetails, IggyError> {
            fail()
        }

        async fn update_stream(
            &self,
            _stream_id: &Identifier,
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
            match &self.topic {
                TopicReply::Error => Err(IggyError::Error),
                TopicReply::Missing => Ok(None),
                TopicReply::Empty => Ok(Some(topic_details(Vec::new()))),
                TopicReply::OnePartition => Ok(Some(topic_details(vec![partition_zero()]))),
                TopicReply::Count(count) => {
                    Ok(Some(topic_details((0..*count).map(partition).collect())))
                }
                TopicReply::Partitions(partitions) => Ok(Some(topic_details(partitions.clone()))),
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
            _partition_id: u32,
            _consumer: &Consumer,
            _strategy: &PollingStrategy,
            _options: PollOptions,
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
            _checkpoint: &Checkpoint,
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
            consumer: &Consumer,
            stream_id: &Identifier,
            topic_id: &Identifier,
            _partition_id: Option<u32>,
        ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
            self.offset_reads
                .lock()
                .expect("offset reads")
                .push(OffsetRead {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    group_id: consumer.id.clone(),
                });
            if self
                .offset_fails_for
                .is_some_and(|partition_id| Some(partition_id) == _partition_id)
            {
                Err(IggyError::Error)
            } else if let Some((current_offset, stored_offset)) = self.offset {
                Ok(Some(ConsumerOffsetInfo {
                    partition_id: 0,
                    current_offset,
                    stored_offset,
                }))
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

        async fn delete_consumer_position(
            &self,
            _consumer: &Consumer,
            _stream_id: &Identifier,
            _topic_id: &Identifier,
            _checkpoint: &Checkpoint,
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
            Ok(Some(ConsumerGroupDetails {
                id: self.group_id,
                name: "group".to_owned(),
                partitions_count: self.partitions_count,
                members_count: u32::try_from(self.members.len()).unwrap(),
                members: self
                    .members
                    .iter()
                    .map(|member| ConsumerGroupMember {
                        id: member.id,
                        partitions_count: member.partitions_count,
                        partitions: member.partitions.clone(),
                    })
                    .collect(),
            }))
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

    #[tokio::test(flavor = "current_thread")]
    async fn should_report_topic_fetch_failure() {
        let (output, error) = printed_error(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::Error,
            offset_fails_for: None,
            offset: None,
            members: Vec::new(),
            group_id: 3,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await;

        assert!(error.to_string().contains("Problem getting topic"));
        assert!(output.contains("group"), "{output}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_report_missing_topic() {
        let (output, error) = printed_error(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::Missing,
            offset_fails_for: None,
            offset: None,
            members: Vec::new(),
            group_id: 3,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await;

        assert!(error.to_string().contains("was not found"));
        assert!(output.contains("group"), "{output}");
        assert!(!output.contains("Current offset"), "{output}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_report_missing_stream() {
        let (output, error) = printed_error(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::OnePartition,
            offset_fails_for: None,
            offset: None,
            members: Vec::new(),
            group_id: 3,
            stream_missing: true,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await;

        assert!(error.to_string().contains("Stream with ID:"));
        assert!(error.to_string().contains("was not found"));
        assert!(output.contains("group"), "{output}");
        assert!(!output.contains("Current offset"), "{output}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_render_group_when_the_topic_has_no_partitions() {
        let output = rendered(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::Empty,
            offset_fails_for: None,
            offset: None,
            members: Vec::new(),
            group_id: 3,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await;

        assert!(output.contains("group"), "{output}");
        assert!(!output.contains("Partition with ID: 0"), "{output}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_render_partition_ids_past_the_partition_count() {
        let output = rendered(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::Partitions(vec![partition(3)]),
            offset_fails_for: None,
            offset: Some((20, 5)),
            members: vec![ConsumerGroupMember {
                id: 17,
                partitions_count: 1,
                partitions: vec![3],
            }],
            group_id: 3,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await;

        assert!(output.contains("17"), "{output}");
        assert!(output.contains("20"), "{output}");
        assert!(!output.contains("Partition with ID: 0"), "{output}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_render_every_remaining_partition_when_ids_are_not_a_prefix() {
        let output = rendered(&ScriptedClient {
            partitions_count: 2,
            topic: TopicReply::Partitions(vec![partition(4), partition(3)]),
            offset_fails_for: None,
            offset: Some((20, 5)),
            members: vec![ConsumerGroupMember {
                id: 17,
                partitions_count: 2,
                partitions: vec![4, 3],
            }],
            group_id: 3,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await;

        let rows = output
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>();
        let row_3 = rows
            .iter()
            .position(|row| row.contains("3 | 17"))
            .unwrap_or_else(|| panic!("partition 3 missing\n{output}"));
        let row_4 = rows
            .iter()
            .position(|row| row.contains("4 | 17"))
            .unwrap_or_else(|| panic!("partition 4 missing\n{output}"));
        assert!(row_3 < row_4, "{output}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_print_the_group_when_one_offset_read_fails() {
        let (output, error) = printed_error(&ScriptedClient {
            partitions_count: 2,
            topic: TopicReply::Partitions(vec![partition(0), partition(1)]),
            offset_fails_for: Some(1),
            offset: Some((20, 5)),
            members: vec![ConsumerGroupMember {
                id: 17,
                partitions_count: 2,
                partitions: vec![0, 1],
            }],
            group_id: 3,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await;

        assert!(
            error
                .to_string()
                .contains("Problem getting consumer offset"),
            "{error}"
        );
        assert!(
            error.to_string().contains("partition with ID: 1"),
            "{error}"
        );
        assert!(output.contains("group"), "{output}");
        assert!(output.contains("17"), "{output}");
        assert!(output.contains("20"), "{output}");
        assert!(output.contains("error"), "{output}");
    }

    #[tokio::test]
    async fn should_render_group_above_the_per_request_partition_limit() {
        let mut cmd = command();
        cmd.execute_cmd(&ScriptedClient {
            partitions_count: MAX_PARTITIONS_COUNT + 1,
            topic: TopicReply::Count(MAX_PARTITIONS_COUNT + 1),
            offset_fails_for: None,
            offset: None,
            members: Vec::new(),
            group_id: 3,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await
        .expect("a topic can grow past one create request");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_print_dashes_when_no_offset_is_stored() {
        let mut partition = partition(0);
        partition.current_offset = 9;
        partition.messages_count = 10;
        let output = rendered(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::Partitions(vec![partition]),
            offset_fails_for: None,
            offset: None,
            members: Vec::new(),
            group_id: 3,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await;

        let rows = output
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>();
        assert!(
            rows.iter().any(|line| line.contains("- | - | -")),
            "local topic counters are not the lag, got {output}"
        );
        assert!(
            !output.contains('9') && !output.contains("10"),
            "local topic counters are not the lag, got {output}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_report_zero_lag_when_stored_offset_leads_current() {
        let mut partition = partition(0);
        partition.current_offset = 99;
        let output = rendered(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::Partitions(vec![partition]),
            offset_fails_for: None,
            offset: Some((4, 10)),
            members: Vec::new(),
            group_id: 3,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await;

        assert!(
            output.contains("4") && output.contains("10") && output.contains("| 0"),
            "lag should clamp to zero, got {output}"
        );
        assert!(
            !output.contains("99"),
            "current offset comes from the offset reply, got {output}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_subtract_the_stored_offset_from_the_offset_reply() {
        let mut partition = partition(0);
        partition.current_offset = 30;
        let output = rendered(&ScriptedClient {
            partitions_count: 1,
            topic: TopicReply::Partitions(vec![partition]),
            offset_fails_for: None,
            offset: Some((20, 5)),
            members: Vec::new(),
            group_id: 3,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        })
        .await;

        assert!(output.contains("20") && output.contains("| 15"), "{output}");
        assert!(
            !output.contains("30") && !output.contains("| 25"),
            "lag uses the offset reply, got {output}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_read_offsets_for_the_returned_group_id() {
        let client = ScriptedClient {
            partitions_count: 2,
            topic: TopicReply::Partitions(vec![partition(0), partition(1)]),
            offset_fails_for: None,
            offset: Some((20, 5)),
            members: Vec::new(),
            group_id: 9,
            stream_missing: false,
            offset_reads: Mutex::new(Vec::new()),
        };
        let mut cmd = GetConsumerGroupCmd::new(
            Identifier::named("stream").unwrap(),
            Identifier::named("topic").unwrap(),
            Identifier::named("group").unwrap(),
        );
        cmd.execute_cmd(&client)
            .await
            .expect("consumer group get should read offsets by numeric id");

        let seen = client.offset_reads.lock().expect("offset reads").clone();
        let expected = OffsetRead {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            group_id: Identifier::numeric(9).unwrap(),
        };
        assert_eq!(seen, vec![expected.clone(), expected]);
    }
}
