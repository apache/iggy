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

//! The methods of the Rust agent: decode the arguments, call the SDK, and
//! return the canonical form of the result.

use super::{RustAgent, sdk_error};
use crate::api::{
    AbandonOperationArgs, BinaryRequestArgs, BinaryRequestToReplicaArgs, BuildConsumerArgs,
    BuildReaderArgs, Commit, ConsumerArg, ConsumerGroupArgs, CreateConsumerGroupArgs,
    CreateStreamArgs, CreateTopicArgs, CreateUserArgs, DescribeOptionsArgs, Expiry, ExpiryKeyword,
    Id, LoginUserArgs, MessageArg, MetadataChangesArgs, MetadataStart, Method, NextArgs, NoArgs,
    OptionsScope, Partitioning, PollMessagesArgs, ResolveOperationArgs, SendMessagesArgs, Strategy,
    StreamArgs, TopicArgs, TopicSize, TopicSizeKeyword, UpdateStreamArgs, UpdateTopicArgs,
    UpdateUserArgs, UserArgs, UserStatus,
};
use crate::canonical::{
    ConnectionInfoResult, ConsumerGroupResult, IdentityResult, MessageResult, OptionSpecResult,
    PolledMessagesResult, RawResult, ReceivedMessageResult, StreamDetailsResult, StreamResult,
    TopicDetailsResult, TopicResult, UserDetailsResult,
};
use crate::protocol::Reply;
use bytes::Bytes;
use futures::StreamExt;
use iggy::prelude::{
    AutoCommit, Client, CommitAfter, CommitPolicy, Consumer, ConsumerGroupClient,
    GlobalPermissions, HeaderKey, Identifier, IggyByteSize, IggyClient, IggyConsumer, IggyDuration,
    IggyError, IggyExpiry, IggyMessage, IggyTimestamp, MaxTopicSize, MessageClient,
    MetadataChangeCursor, MetadataChangeGroup, MetadataChangeStart, MetadataChangesQuery,
    MetadataCurrentPageToken, OptionsScope as SdkOptionsScope, PartitionReader, Permissions,
    PollOptions, PollingStrategy, ReadError, StreamClient, StreamCreateOptions,
    StreamEncryptionKey, StreamUpdateOptions, SystemClient, TopicClient, TopicCreateOptions,
    TopicUpdateOptions, UserClient, UserCreateOptions, UserStatus as SdkUserStatus,
    UserUpdateOptions,
};
use iggy_common::resource_options_json;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// The budget of a resolution that does not name one, the SDK's own budget.
const DEFAULT_RESOLVE_DEADLINE: Duration = Duration::from_secs(30);

pub(super) async fn call(
    agent: &RustAgent,
    entity: &str,
    method_name: &str,
    args: Value,
    save_as: Option<String>,
) -> Reply {
    let Ok(method) = Method::from_str(method_name) else {
        return Reply::Unsupported(format!("the method {method_name}"));
    };
    let outcome = if let Some(client) = agent.client(entity) {
        client_call(agent, &client, method, args, save_as).await
    } else if let Some(reader) = agent.reader(entity) {
        reader_call(&reader, method, args).await
    } else if let Some(consumer) = agent.consumer(entity) {
        consumer_call(&consumer, method, args).await
    } else {
        Err(Reply::Failure(format!("no entity {entity:?}")))
    };
    outcome.unwrap_or_else(|reply| reply)
}

async fn consumer_call(
    consumer: &Mutex<IggyConsumer>,
    method: Method,
    args: Value,
) -> Result<Reply, Reply> {
    if method != Method::Next {
        return Err(Reply::Failure(format!(
            "{} is not a consumer method",
            method.name()
        )));
    }
    decode::<NextArgs>(args)?;
    let mut consumer = consumer.lock().await;
    let received = consumer
        .next()
        .await
        .ok_or_else(|| Reply::Failure("the consumer stream ended".to_owned()))?
        .map_err(|failure| read_error(&failure))?;
    canonical(
        MessageResult::try_from(&received.message).map(|message| ReceivedMessageResult {
            partition_id: received.partition_id,
            message,
        }),
    )
}

async fn reader_call(
    reader: &Mutex<PartitionReader>,
    method: Method,
    args: Value,
) -> Result<Reply, Reply> {
    if method != Method::Next {
        return Err(Reply::Failure(format!(
            "{} is not a reader method",
            method.name()
        )));
    }
    decode::<NextArgs>(args)?;
    let mut reader = reader.lock().await;
    let polled = reader
        .next()
        .await
        .map_err(|failure| read_error(&failure))?;
    canonical(PolledMessagesResult::try_from(&polled))
}

/// The answer for a failed read, with the partition that the read names.
fn read_error(failure: &ReadError) -> Reply {
    let mut reply = sdk_error(&failure.error);
    if let Reply::Error(error) = &mut reply
        && error.partition_id.is_none()
    {
        error.partition_id = failure.partition_id;
    }
    reply
}

async fn client_call(
    agent: &RustAgent,
    client: &IggyClient,
    method: Method,
    args: Value,
    save_as: Option<String>,
) -> Result<Reply, Reply> {
    let value = match method {
        Method::Connect => {
            decode::<NoArgs>(args)?;
            client.connect().await.map_err(sdk)?;
            Value::Null
        }
        Method::Disconnect => {
            decode::<NoArgs>(args)?;
            client.disconnect().await.map_err(sdk)?;
            Value::Null
        }
        Method::Ping => {
            decode::<NoArgs>(args)?;
            client.ping().await.map_err(sdk)?;
            Value::Null
        }
        Method::LoginUser => {
            let args: LoginUserArgs = decode(args)?;
            let identity = client
                .login_user(&args.username, &args.password)
                .await
                .map_err(sdk)?;
            json(&IdentityResult::from(&identity))?
        }
        Method::LogoutUser => {
            decode::<NoArgs>(args)?;
            client.logout_user().await.map_err(sdk)?;
            Value::Null
        }
        Method::GetStream => {
            let args: StreamArgs = decode(args)?;
            let stream = client
                .get_stream(&identifier(&args.stream_id)?)
                .await
                .map_err(sdk)?;
            match stream {
                Some(stream) => json(&StreamDetailsResult::try_from(&stream).map_err(sdk)?)?,
                None => Value::Null,
            }
        }
        Method::GetStreams => {
            decode::<NoArgs>(args)?;
            let streams = client.get_streams().await.map_err(sdk)?;
            let streams = streams
                .iter()
                .map(StreamResult::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(sdk)?;
            json(&streams)?
        }
        Method::CreateStream => {
            let args: CreateStreamArgs = decode(args)?;
            let options = StreamCreateOptions {
                encryption_key: args
                    .encryption_key_hex
                    .as_deref()
                    .map(stream_key)
                    .transpose()?,
                ..StreamCreateOptions::default()
            };
            let stream = client
                .create_stream(&args.name, &options)
                .await
                .map_err(sdk)?;
            json(&StreamDetailsResult::try_from(&stream).map_err(sdk)?)?
        }
        Method::UpdateStream => {
            let args: UpdateStreamArgs = decode(args)?;
            let options = StreamUpdateOptions {
                name: args.name,
                raw_reset: args.reset.into_iter().collect(),
                ..StreamUpdateOptions::default()
            };
            client
                .update_stream(&identifier(&args.stream_id)?, &options)
                .await
                .map_err(sdk)?;
            Value::Null
        }
        Method::DeleteStream => {
            let args: StreamArgs = decode(args)?;
            client
                .delete_stream(&identifier(&args.stream_id)?)
                .await
                .map_err(sdk)?;
            Value::Null
        }
        Method::GetTopic => {
            let args: TopicArgs = decode(args)?;
            let topic = client
                .get_topic(&identifier(&args.stream_id)?, &identifier(&args.topic_id)?)
                .await
                .map_err(sdk)?;
            match topic {
                Some(topic) => json(&TopicDetailsResult::try_from(&topic).map_err(sdk)?)?,
                None => Value::Null,
            }
        }
        Method::GetTopics => {
            let args: StreamArgs = decode(args)?;
            let topics = client
                .get_topics(&identifier(&args.stream_id)?)
                .await
                .map_err(sdk)?;
            let topics = topics
                .iter()
                .map(TopicResult::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(sdk)?;
            json(&topics)?
        }
        Method::CreateTopic => {
            let args: CreateTopicArgs = decode(args)?;
            let options = TopicCreateOptions {
                partitions_count: args.partitions_count,
                message_expiry: args.message_expiry.map(expiry),
                max_topic_size: args.max_topic_size.map(topic_size),
                ..TopicCreateOptions::default()
            };
            let topic = client
                .create_topic(&identifier(&args.stream_id)?, &args.name, &options)
                .await
                .map_err(sdk)?;
            json(&TopicDetailsResult::try_from(&topic).map_err(sdk)?)?
        }
        Method::UpdateTopic => {
            let args: UpdateTopicArgs = decode(args)?;
            let options = TopicUpdateOptions {
                name: args.name,
                message_expiry: args.message_expiry.map(expiry),
                max_topic_size: args.max_topic_size.map(topic_size),
                raw_reset: args.reset.into_iter().collect(),
                ..TopicUpdateOptions::default()
            };
            client
                .update_topic(
                    &identifier(&args.stream_id)?,
                    &identifier(&args.topic_id)?,
                    &options,
                )
                .await
                .map_err(sdk)?;
            Value::Null
        }
        Method::DeleteTopic => {
            let args: TopicArgs = decode(args)?;
            client
                .delete_topic(&identifier(&args.stream_id)?, &identifier(&args.topic_id)?)
                .await
                .map_err(sdk)?;
            Value::Null
        }
        Method::SendMessages => {
            let args: SendMessagesArgs = decode(args)?;
            let mut messages = args
                .messages
                .iter()
                .map(message)
                .collect::<Result<Vec<_>, _>>()?;
            client
                .send_messages(
                    &identifier(&args.stream_id)?,
                    &identifier(&args.topic_id)?,
                    &partitioning(&args.partitioning)?,
                    &mut messages,
                )
                .await
                .map_err(sdk)?;
            Value::Null
        }
        Method::PollMessages => {
            let args: PollMessagesArgs = decode(args)?;
            let options = PollOptions {
                count: args.count,
                commit: commit_policy(args.commit),
                max_wait: IggyDuration::from(Duration::from_millis(args.max_wait_ms.unwrap_or(0))),
                min_count: args.min_count.unwrap_or(1),
                ..PollOptions::default()
            };
            let polled = client
                .poll_messages(
                    &identifier(&args.stream_id)?,
                    &identifier(&args.topic_id)?,
                    args.partition_id,
                    &consumer(args.consumer.as_ref())?,
                    &strategy(args.strategy),
                    options,
                )
                .await
                .map_err(sdk)?;
            json(&PolledMessagesResult::try_from(&polled).map_err(sdk)?)?
        }
        Method::CreateConsumerGroup => {
            let args: CreateConsumerGroupArgs = decode(args)?;
            let group = client
                .create_consumer_group(
                    &identifier(&args.stream_id)?,
                    &identifier(&args.topic_id)?,
                    &args.name,
                )
                .await
                .map_err(sdk)?;
            json(&ConsumerGroupResult::from(&group))?
        }
        Method::GetConsumerGroup => {
            let args: ConsumerGroupArgs = decode(args)?;
            let group = client
                .get_consumer_group(
                    &identifier(&args.stream_id)?,
                    &identifier(&args.topic_id)?,
                    &identifier(&args.group_id)?,
                )
                .await
                .map_err(sdk)?;
            match group {
                Some(group) => json(&ConsumerGroupResult::from(&group))?,
                None => Value::Null,
            }
        }
        Method::DeleteConsumerGroup => {
            let args: ConsumerGroupArgs = decode(args)?;
            client
                .delete_consumer_group(
                    &identifier(&args.stream_id)?,
                    &identifier(&args.topic_id)?,
                    &identifier(&args.group_id)?,
                )
                .await
                .map_err(sdk)?;
            Value::Null
        }
        Method::BuildReader => {
            let args: BuildReaderArgs = decode(args)?;
            let Some(entity) = save_as else {
                return Err(Reply::Failure(
                    "build_reader needs save_result_as".to_owned(),
                ));
            };
            let mut builder = client.reader(
                &identifier(&args.stream_id)?,
                &identifier(&args.topic_id)?,
                &consumer(args.consumer.as_ref())?,
            );
            if let Some(partitions) = args.partitions {
                builder = builder.partitions(partitions);
            }
            if let Some(start) = args.start {
                builder = builder.start(strategy(start));
            }
            let mut options = PollOptions {
                commit: commit_policy(args.commit),
                ..PollOptions::default()
            };
            if let Some(max_wait_ms) = args.max_wait_ms {
                options.max_wait = IggyDuration::from(Duration::from_millis(max_wait_ms));
            }
            let reader = builder.options(options).build().await.map_err(sdk)?;
            agent.keep_reader(entity, reader);
            Value::Null
        }
        Method::ResolveOperation => {
            let args: ResolveOperationArgs = decode(args)?;
            let budget = args
                .deadline_ms
                .map_or(DEFAULT_RESOLVE_DEADLINE, Duration::from_millis);
            client
                .resolve_operation(args.operation_id, Instant::now() + budget)
                .await
                .map_err(sdk)?;
            Value::Null
        }
        Method::AbandonOperation => {
            let args: AbandonOperationArgs = decode(args)?;
            client
                .abandon_operation(args.operation_id)
                .await
                .map_err(sdk)?;
            Value::Null
        }
        Method::BuildConsumer => {
            let args: BuildConsumerArgs = decode(args)?;
            let Some(entity) = save_as else {
                return Err(Reply::Failure(
                    "build_consumer needs save_result_as".to_owned(),
                ));
            };
            let mut options = PollOptions {
                commit: commit_policy(args.commit),
                ..PollOptions::default()
            };
            if let Some(count) = args.count {
                options.count = count;
            }
            if let Some(max_wait_ms) = args.max_wait_ms {
                options.max_wait = IggyDuration::from(Duration::from_millis(max_wait_ms));
            }
            let mut consumer = client
                .consumer_group(
                    &args.group,
                    &identifier_text(&args.stream_id),
                    &identifier_text(&args.topic_id),
                )
                .map_err(sdk)?
                .poll_options(options)
                .build();
            consumer.init().await.map_err(sdk)?;
            agent.keep_consumer(entity, consumer);
            Value::Null
        }
        Method::SendBinaryRequest => {
            let args: BinaryRequestArgs = decode(args)?;
            let payload = hex_payload(&args.payload_hex)?;
            let reply = client
                .send_binary_request(args.code, payload)
                .await
                .map_err(sdk)?;
            json(&RawResult {
                payload_hex: hex::encode(&reply),
            })?
        }
        Method::SendBinaryRequestToReplica => {
            let args: BinaryRequestToReplicaArgs = decode(args)?;
            let payload = hex_payload(&args.payload_hex)?;
            let reply = client
                .send_binary_request_to_replica(args.code, payload, args.replica)
                .await
                .map_err(sdk)?;
            json(&RawResult {
                payload_hex: hex::encode(&reply),
            })?
        }
        Method::DescribeOptions => {
            let args: DescribeOptionsArgs = decode(args)?;
            let scope = match args.scope {
                OptionsScope::Stream => SdkOptionsScope::Stream,
                OptionsScope::Topic => SdkOptionsScope::Topic,
                OptionsScope::User => SdkOptionsScope::User,
            };
            let specs = client.describe_options(scope).await.map_err(sdk)?;
            let specs = specs
                .iter()
                .map(OptionSpecResult::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(sdk)?;
            json(&specs)?
        }
        Method::CreateUser => {
            let args: CreateUserArgs = decode(args)?;
            let permissions = args.permissions.map(|permissions| {
                let global = permissions.global;
                Permissions {
                    global: GlobalPermissions {
                        manage_servers: global.manage_servers,
                        read_servers: global.read_servers,
                        manage_users: global.manage_users,
                        read_users: global.read_users,
                        manage_streams: global.manage_streams,
                        read_streams: global.read_streams,
                        manage_topics: global.manage_topics,
                        read_topics: global.read_topics,
                        poll_messages: global.poll_messages,
                        send_messages: global.send_messages,
                    },
                    streams: None,
                }
            });
            let user = client
                .create_user(
                    &args.username,
                    &args.password,
                    user_status(args.status),
                    permissions,
                    &UserCreateOptions::default(),
                )
                .await
                .map_err(sdk)?;
            json(&UserDetailsResult::try_from(&user).map_err(sdk)?)?
        }
        Method::GetUser => {
            let args: UserArgs = decode(args)?;
            let user = client
                .get_user(&identifier(&args.user_id)?)
                .await
                .map_err(sdk)?;
            match user {
                Some(user) => json(&UserDetailsResult::try_from(&user).map_err(sdk)?)?,
                None => Value::Null,
            }
        }
        Method::UpdateUser => {
            let args: UpdateUserArgs = decode(args)?;
            let options = UserUpdateOptions {
                username: args.username,
                status: args.status.map(user_status),
                ..UserUpdateOptions::default()
            };
            client
                .update_user(&identifier(&args.user_id)?, &options)
                .await
                .map_err(sdk)?;
            Value::Null
        }
        Method::DeleteUser => {
            let args: UserArgs = decode(args)?;
            client
                .delete_user(&identifier(&args.user_id)?)
                .await
                .map_err(sdk)?;
            Value::Null
        }
        Method::GetMetadataChanges => {
            let args: MetadataChangesArgs = decode(args)?;
            let start = match args.start {
                MetadataStart::Earliest => MetadataChangeStart::Earliest,
                MetadataStart::Latest => MetadataChangeStart::Latest,
                MetadataStart::After(cursor) => MetadataChangeStart::After(
                    MetadataChangeCursor::from_str(&cursor).map_err(sdk)?,
                ),
                MetadataStart::Current(token) => MetadataChangeStart::Current(
                    token
                        .as_deref()
                        .map(MetadataCurrentPageToken::from_str)
                        .transpose()
                        .map_err(sdk)?,
                ),
            };
            let groups = args
                .groups
                .iter()
                .map(|group| {
                    MetadataChangeGroup::from_str(group)
                        .map_err(|_| Reply::Failure(format!("unknown group {group:?}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut query = MetadataChangesQuery {
                start,
                groups,
                ..MetadataChangesQuery::default()
            };
            if let Some(limit) = args.limit {
                query.limit = limit;
            }
            let changes = client.get_metadata_changes(&query).await.map_err(sdk)?;
            json(&changes)?
        }
        Method::GetConnectionInfo => {
            decode::<NoArgs>(args)?;
            let info = client.get_connection_info().await;
            json(&ConnectionInfoResult {
                server_address: info.server_address,
            })?
        }
        Method::Next => {
            return Err(Reply::Failure(
                "next is a reader or consumer method".to_owned(),
            ));
        }
    };
    Ok(Reply::Result(value))
}

fn decode<T: DeserializeOwned>(args: Value) -> Result<T, Reply> {
    serde_json::from_value(args).map_err(|error| Reply::Failure(format!("arguments: {error}")))
}

fn json<T: Serialize>(value: &T) -> Result<Value, Reply> {
    serde_json::to_value(value).map_err(|error| Reply::Failure(format!("result: {error}")))
}

fn canonical<T: Serialize>(result: Result<T, IggyError>) -> Result<Reply, Reply> {
    let value = result.map_err(sdk)?;
    json(&value).map(Reply::Result)
}

fn sdk(error: IggyError) -> Reply {
    sdk_error(&error)
}

/// The identifier as the text that consumer builders take.
fn identifier_text(id: &Id) -> String {
    match id {
        Id::Numeric(id) => id.to_string(),
        Id::Named(name) => name.clone(),
    }
}

fn hex_payload(text: &str) -> Result<Bytes, Reply> {
    hex::decode(text)
        .map(Bytes::from)
        .map_err(|error| Reply::Failure(format!("payload_hex: {error}")))
}

fn user_status(status: UserStatus) -> SdkUserStatus {
    match status {
        UserStatus::Active => SdkUserStatus::Active,
        UserStatus::Inactive => SdkUserStatus::Inactive,
    }
}

fn identifier(id: &Id) -> Result<Identifier, Reply> {
    match id {
        Id::Numeric(id) => Identifier::numeric(*id),
        Id::Named(name) => Identifier::named(name),
    }
    .map_err(sdk)
}

fn consumer(consumer: Option<&ConsumerArg>) -> Result<Consumer, Reply> {
    Ok(match consumer {
        None => Consumer::default(),
        Some(ConsumerArg::Consumer(id)) => Consumer::new(identifier(id)?),
        Some(ConsumerArg::Group(id)) => Consumer::group(identifier(id)?),
    })
}

fn strategy(strategy: Strategy) -> PollingStrategy {
    match strategy {
        Strategy::First => PollingStrategy::first(),
        Strategy::Last => PollingStrategy::last(),
        Strategy::Next => PollingStrategy::next(),
        Strategy::Offset(offset) => PollingStrategy::offset(offset),
        Strategy::Timestamp(micros) => PollingStrategy::timestamp(IggyTimestamp::from(micros)),
    }
}

fn commit_policy(commit: Commit) -> CommitPolicy {
    match commit {
        Commit::Manual => CommitPolicy::Manual,
        Commit::OnRead => CommitPolicy::OnRead,
        Commit::AfterProcessing => CommitPolicy::Auto(AutoCommit {
            after: CommitAfter::Processing,
            ..AutoCommit::default()
        }),
    }
}

fn expiry(expiry: Expiry) -> IggyExpiry {
    match expiry {
        Expiry::Keyword(ExpiryKeyword::Never) => IggyExpiry::NeverExpire,
        Expiry::Keyword(ExpiryKeyword::ServerDefault) => IggyExpiry::ServerDefault,
        Expiry::Microseconds(micros) => {
            IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_micros(micros)))
        }
    }
}

fn topic_size(size: TopicSize) -> MaxTopicSize {
    match size {
        TopicSize::Keyword(TopicSizeKeyword::Unlimited) => MaxTopicSize::Unlimited,
        TopicSize::Keyword(TopicSizeKeyword::ServerDefault) => MaxTopicSize::ServerDefault,
        TopicSize::Bytes(bytes) => MaxTopicSize::Custom(IggyByteSize::from(bytes)),
    }
}

fn partitioning(partitioning: &Partitioning) -> Result<iggy::prelude::Partitioning, Reply> {
    Ok(match partitioning {
        Partitioning::Balanced => iggy::prelude::Partitioning::balanced(),
        Partitioning::PartitionId(id) => iggy::prelude::Partitioning::partition_id(*id),
        Partitioning::MessagesKey(key) => {
            iggy::prelude::Partitioning::messages_key_str(key).map_err(sdk)?
        }
    })
}

fn stream_key(hex_text: &str) -> Result<StreamEncryptionKey, Reply> {
    let bytes = hex::decode(hex_text)
        .map_err(|error| Reply::Failure(format!("encryption_key_hex: {error}")))?;
    StreamEncryptionKey::try_from(bytes.as_slice()).map_err(sdk)
}

fn message(message: &MessageArg) -> Result<IggyMessage, Reply> {
    let payload = Bytes::from(message.payload_bytes().map_err(Reply::Failure)?);
    let id = message
        .id
        .as_deref()
        .map(u128::from_str)
        .transpose()
        .map_err(|error| Reply::Failure(format!("message id: {error}")))?;
    let headers = message
        .headers
        .iter()
        .map(|(key, header)| {
            let kind = serde_json::from_value(Value::String(header.kind.clone()))
                .map_err(|error| Reply::Failure(format!("header kind: {error}")))?;
            let value = resource_options_json::decode_value(kind, &header.value).map_err(sdk)?;
            Ok((HeaderKey::from_str(key).map_err(sdk)?, value))
        })
        .collect::<Result<BTreeMap<_, _>, Reply>>()?;
    let built = match (id, headers.is_empty()) {
        (Some(id), true) => IggyMessage::builder().id(id).payload(payload).build(),
        (None, true) => IggyMessage::builder().payload(payload).build(),
        (Some(id), false) => IggyMessage::builder()
            .id(id)
            .payload(payload)
            .user_headers(headers)
            .build(),
        (None, false) => IggyMessage::builder()
            .payload(payload)
            .user_headers(headers)
            .build(),
    };
    built.map_err(sdk)
}
