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
    AbandonOperationArgs, BuildReaderArgs, Commit, ConsumerArg, ConsumerGroupArgs,
    CreateConsumerGroupArgs, CreateStreamArgs, CreateTopicArgs, Expiry, ExpiryKeyword, Id,
    LoginUserArgs, MessageArg, Method, NextArgs, NoArgs, Partitioning, PollMessagesArgs,
    ResolveOperationArgs, SendMessagesArgs, Strategy, StreamArgs, TopicArgs, TopicSize,
    TopicSizeKeyword, UpdateStreamArgs, UpdateTopicArgs,
};
use crate::canonical::{
    ConsumerGroupResult, IdentityResult, PolledMessagesResult, StreamDetailsResult, StreamResult,
    TopicDetailsResult, TopicResult,
};
use crate::protocol::{EntityKind, Reply};
use bytes::Bytes;
use iggy::prelude::PartitionReader;
use iggy::prelude::{
    AutoCommit, Client, CommitAfter, CommitPolicy, Consumer, ConsumerGroupClient, HeaderKey,
    Identifier, IggyByteSize, IggyClient, IggyDuration, IggyError, IggyExpiry, IggyMessage,
    IggyTimestamp, MaxTopicSize, MessageClient, PollOptions, PollingStrategy, StreamClient,
    StreamCreateOptions, StreamEncryptionKey, StreamUpdateOptions, SystemClient, TopicClient,
    TopicCreateOptions, TopicUpdateOptions, UserClient,
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
    let outcome = match method.target() {
        EntityKind::Client => match agent.client(entity) {
            Some(client) => client_call(agent, &client, method, args, save_as).await,
            None => Err(Reply::Failure(format!("no client {entity:?}"))),
        },
        EntityKind::Reader => match agent.reader(entity) {
            Some(reader) => reader_call(&reader, args).await,
            None => Err(Reply::Failure(format!("no reader {entity:?}"))),
        },
        EntityKind::Consumer => Err(Reply::Unsupported("consumer entities".to_owned())),
    };
    outcome.unwrap_or_else(|reply| reply)
}

async fn reader_call(reader: &Mutex<PartitionReader>, args: Value) -> Result<Reply, Reply> {
    decode::<NextArgs>(args)?;
    let mut reader = reader.lock().await;
    let polled = reader.next().await.map_err(|failure| {
        let mut reply = sdk_error(&failure.error);
        if let Reply::Error(error) = &mut reply
            && error.partition_id.is_none()
        {
            error.partition_id = failure.partition_id;
        }
        reply
    })?;
    canonical(PolledMessagesResult::try_from(&polled))
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
        Method::Next => {
            return Err(Reply::Failure("next is a reader method".to_owned()));
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
