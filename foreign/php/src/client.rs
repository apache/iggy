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

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use ext_php_rs::{binary::Binary, exception::PhpResult, php_class, php_impl, types::Zval};
use iggy::prelude::{
    CompressionAlgorithm, Consumer as RustConsumer, IggyClient as RustIggyClient,
    IggyClientBuilder, IggyDuration, IggyExpiry, IggyMessage as RustMessage, MaxTopicSize,
    Partitioning, PollingStrategy as RustPollingStrategy, *,
};
use tokio::sync::Mutex;

use crate::consumer::{AutoCommit, IggyConsumer};
use crate::durability::Durability as PhpDurability;
use crate::error::to_php_exception;
use crate::identifier::PhpIdentifier;
use crate::metadata_changes::{MetadataChangeGroup, MetadataChangeStart, MetadataChanges};
use crate::options::{OptionSpec, OptionsScope, StreamOptionKey, TopicOptionKey, reset_names};
use crate::receive_message::{PollingStrategy, ReceiveMessage};
use crate::runtime::runtime;
use crate::send_message::{SendMessage, SendMessagesResponse};
use crate::stream::StreamDetails;
use crate::topic::TopicDetails;

const DEFAULT_RESOLVE_TIMEOUT: Duration = Duration::from_secs(30);

/// Client-wide limits for active and retained mutation frames, including queued writer bytes.
#[php_class]
#[php(name = "Iggy\\PendingOperationsConfig")]
pub struct PendingOperationsConfig {
    inner: iggy::prelude::PendingOperationsConfig,
}

#[php_impl]
impl PendingOperationsConfig {
    /// Defaults to 256 operations and 64 MiB of headers and bodies.
    #[php(constructor)]
    pub fn __construct(max_operations: Option<&Zval>, max_bytes: Option<&Zval>) -> PhpResult<Self> {
        let defaults = iggy::prelude::PendingOperationsConfig::default();
        Ok(Self {
            inner: iggy::prelude::PendingOperationsConfig {
                max_operations: pending_limit(
                    max_operations,
                    defaults.max_operations,
                    "max_operations",
                )?,
                max_bytes: pending_limit(max_bytes, defaults.max_bytes, "max_bytes")?,
            },
        })
    }

    #[php(getter, change_case = "snake_case")]
    pub fn max_operations(&self) -> usize {
        self.inner.max_operations.get()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn max_bytes(&self) -> usize {
        self.inner.max_bytes.get()
    }
}

/// A PHP class representing the Iggy client.
#[php_class]
#[php(name = "Iggy\\Client")]
pub struct IggyClient {
    inner: Arc<RustIggyClient>,
}

#[php_impl]
impl IggyClient {
    /// Constructs a new IggyClient from a TCP server address.
    #[php(constructor)]
    pub fn __construct(conn: Option<String>, pending_operations: Option<&Zval>) -> PhpResult<Self> {
        let pending_operations = pending_operations
            .filter(|value| !value.is_null())
            .map(|value| {
                value.extract::<&PendingOperationsConfig>().ok_or_else(|| {
                    to_php_exception(
                        "'pending_operations' must be a PendingOperationsConfig or null",
                    )
                })
            })
            .transpose()?;
        let server_address = conn
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "127.0.0.1:8090".to_string());
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(server_address)
            .with_pending_operations(
                pending_operations.map_or_else(Default::default, |limits| limits.inner),
            )
            .build()
            .map_err(to_php_exception)?;

        Ok(Self {
            inner: Arc::new(client),
        })
    }

    /// Constructs a new IggyClient from a connection string.
    ///
    /// The string is marked `#[\SensitiveParameter]`, so exception traces never hold it.
    pub fn from_connection_string(connection_string: String) -> PhpResult<Self> {
        let client =
            RustIggyClient::from_connection_string(&connection_string).map_err(to_php_exception)?;

        Ok(Self {
            inner: Arc::new(client),
        })
    }

    /// Sends a ping request to the server.
    pub fn ping(&self) -> PhpResult {
        let inner = self.inner.clone();
        runtime().block_on(async move { inner.ping().await.map_err(to_php_exception) })
    }

    /// Logs in the user with the given credentials.
    ///
    /// The password is marked `#[\SensitiveParameter]`, so exception traces never hold it.
    pub fn login_user(&self, username: String, password: String) -> PhpResult {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .login_user(&username, &password)
                .await
                .map(|_| ())
                .map_err(to_php_exception)
        })
    }

    /// Connects the IggyClient to its service.
    pub fn connect(&self) -> PhpResult {
        let inner = self.inner.clone();
        runtime().block_on(async move { inner.connect().await.map_err(to_php_exception) })
    }

    /// Replays an exact retained operation. A committed rejection raises its original error
    /// and releases its lane; uncertainty keeps it retained. HTTP is unsupported.
    /// Operation IDs use decimal strings to preserve the full unsigned 64-bit range.
    pub fn resolve_operation(
        &self,
        operation_id: String,
        timeout_micros: Option<&Zval>,
    ) -> PhpResult {
        let operation_id = operation_id_from_decimal(&operation_id)?;
        let timeout = match optional_integer(timeout_micros, "timeout_micros")? {
            Some(value) if value > 0 => Duration::from_micros(value as u64),
            Some(_) => {
                return Err(to_php_exception(
                    "'timeout_micros' must be greater than zero",
                ));
            }
            None => DEFAULT_RESOLVE_TIMEOUT,
        };
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| to_php_exception("'timeout_micros' is too large"))?;
        runtime()
            .block_on(self.inner.resolve_operation(operation_id, deadline))
            .map_err(to_php_exception)
    }

    /// Releases a retained operation without claiming whether it committed.
    pub fn abandon_operation(&self, operation_id: String) -> PhpResult {
        let operation_id = operation_id_from_decimal(&operation_id)?;
        runtime()
            .block_on(self.inner.abandon_operation(operation_id))
            .map_err(to_php_exception)
    }

    /// Creates a new stream.
    ///
    /// `encryption_key` is the stream's own data key: 32 random bytes as canonical padded
    /// standard base64, for example `base64_encode(random_bytes(32))`. Raw bytes or any other
    /// text are refused before anything is sent. The key travels in the request, so protecting
    /// it in transit with TLS is the caller's responsibility. It is marked
    /// `#[\SensitiveParameter]`, so exception traces never hold it. The server stores it wrapped
    /// and never returns it, reads report only its source and fingerprints. Without a key, a
    /// server with encryption enabled generates one. `raw` sends string-valued keys this
    /// build does not know, and the server decides whether to accept them. It is marked
    /// `#[\SensitiveParameter]` too, because an unknown key may hold a secret.
    pub fn create_stream(
        &self,
        name: String,
        encryption_key: Option<Binary<u8>>,
        raw: Option<BTreeMap<String, String>>,
    ) -> PhpResult {
        let options = StreamCreateOptions {
            encryption_key: encryption_key.map(stream_key).transpose()?,
            raw: raw.unwrap_or_default(),
        };
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .create_stream(&name, &options)
                .await
                .map(|_| ())
                .map_err(to_php_exception)
        })
    }

    /// Renames a stream.
    ///
    /// A null name keeps the current one. No stream option is resettable, so a non-empty
    /// `reset` list, or an update that changes nothing, is refused before anything is sent.
    /// `raw` and `raw_reset` reach keys this build does not know, and the server decides
    /// whether each one may be set or reset. `raw` is marked `#[\SensitiveParameter]`,
    /// because an unknown key may hold a secret. Renaming keeps the stream's id, key and
    /// messages.
    #[php(defaults(reset = Vec::<StreamOptionKey>::new()))]
    pub fn update_stream(
        &self,
        stream: PhpIdentifier,
        name: Option<String>,
        reset: Vec<StreamOptionKey>,
        raw: Option<BTreeMap<String, String>>,
        raw_reset: Option<Vec<String>>,
    ) -> PhpResult {
        let stream: Identifier = stream.try_into()?;
        let (reset, raw_reset) = reset_names(reset, raw_reset)?;
        let options = StreamUpdateOptions {
            name,
            reset,
            raw_reset,
            raw: raw.unwrap_or_default(),
        };
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .update_stream(&stream, &options)
                .await
                .map_err(to_php_exception)
        })
    }

    /// Fingerprint of a stream key given as canonical padded standard base64 of 32 bytes, the
    /// value `StreamDetails::$encryption_key_fingerprint` reports for a stream created with it.
    /// The key is marked `#[\SensitiveParameter]`, so exception traces never hold it.
    pub fn encryption_key_fingerprint(key: Binary<u8>) -> PhpResult<String> {
        stream_key(key).map(|key| key.fingerprint())
    }

    /// Gets a stream by id or name.
    pub fn get_stream(&self, stream_id: PhpIdentifier) -> PhpResult<Option<StreamDetails>> {
        let stream_id: Identifier = stream_id.try_into()?;
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .get_stream(&stream_id)
                .await
                .map(|stream| stream.map(StreamDetails::from))
                .map_err(to_php_exception)
        })
    }

    /// Creates a topic.
    ///
    /// Every option left null resolves against the server default at admission. The 64-bit
    /// sizes and durations take an int or a decimal string, so never-expire and unlimited
    /// (18446744073709551615) can be sent. `raw` sends string-valued keys this build does not
    /// know, and the server decides whether to accept them. It is marked
    /// `#[\SensitiveParameter]`, because an unknown key may hold a secret.
    #[allow(clippy::too_many_arguments)]
    pub fn create_topic(
        &self,
        stream: PhpIdentifier,
        name: String,
        partitions_count: u32,
        compression_algorithm: Option<String>,
        message_expiry_micros: Option<&Zval>,
        max_topic_size: Option<&Zval>,
        segment_size: Option<&Zval>,
        durability: Option<PhpDurability>,
        consumer_offset_durability: Option<PhpDurability>,
        messages_required_to_save: Option<&Zval>,
        size_of_messages_required_to_save: Option<&Zval>,
        preallocate_segments: Option<bool>,
        partition_resize_policy: Option<String>,
        raw: Option<BTreeMap<String, String>>,
    ) -> PhpResult {
        let message_expiry_micros = optional_u64(message_expiry_micros, "message_expiry_micros")?;
        let max_topic_size = optional_u64(max_topic_size, "max_topic_size")?;
        let segment_size = optional_u64(segment_size, "segment_size")?;
        let messages_required_to_save =
            optional_unsigned::<u32>(messages_required_to_save, "messages_required_to_save")?;
        let size_of_messages_required_to_save = optional_u64(
            size_of_messages_required_to_save,
            "size_of_messages_required_to_save",
        )?;
        let compression_algorithm = match compression_algorithm {
            Some(value) => CompressionAlgorithm::from_str(&value).map_err(to_php_exception)?,
            None => CompressionAlgorithm::default(),
        };
        let partition_resize_policy = partition_resize_policy
            .as_deref()
            .map(PartitionResizePolicy::from_str)
            .transpose()
            .map_err(|_| {
                to_php_exception("'partition_resize_policy' must be 'mutable' or 'fixed'")
            })?;
        let expiry = message_expiry_micros.map_or(IggyExpiry::ServerDefault, IggyExpiry::from);
        let max_size = max_topic_size.map_or(MaxTopicSize::ServerDefault, MaxTopicSize::from);
        let stream: Identifier = stream.try_into()?;
        let inner = self.inner.clone();

        // `None` is what tells admission to resolve the server default, so the
        // sentinels above must collapse back to it.
        let options = TopicCreateOptions {
            partitions_count: Some(partitions_count),
            compression_algorithm: (compression_algorithm != CompressionAlgorithm::default())
                .then_some(compression_algorithm),
            message_expiry: (expiry != IggyExpiry::ServerDefault).then_some(expiry),
            max_topic_size: (max_size != MaxTopicSize::ServerDefault).then_some(max_size),
            segment_size: segment_size.map(IggyByteSize::from),
            durability: durability.unwrap_or_default().into(),
            consumer_offset_durability: consumer_offset_durability.unwrap_or_default().into(),
            partition_resize_policy,
            messages_required_to_save,
            size_of_messages_required_to_save: size_of_messages_required_to_save
                .map(IggyByteSize::from),
            preallocate_segments,
            raw: raw.unwrap_or_default(),
        };

        runtime().block_on(async move {
            inner
                .create_topic(&stream, &name, &options)
                .await
                .map(|_| ())
                .map_err(to_php_exception)
        })
    }

    /// Renames a topic or changes its message expiry and size cap.
    ///
    /// Null arguments keep the stored values. `message_expiry_micros` and `max_topic_size`
    /// take an int or a decimal string, 18446744073709551615 meaning never expire and
    /// unlimited, and 0 is refused: list the key in `reset` to return it to the server
    /// default. `raw` and `raw_reset` reach keys this build does not know, and the server
    /// decides whether each one may be set or reset. `raw` is marked
    /// `#[\SensitiveParameter]`, because an unknown key may hold a secret.
    #[allow(clippy::too_many_arguments)]
    #[php(defaults(reset = Vec::<TopicOptionKey>::new()))]
    pub fn update_topic(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        name: Option<String>,
        message_expiry_micros: Option<&Zval>,
        max_topic_size: Option<&Zval>,
        reset: Vec<TopicOptionKey>,
        raw: Option<BTreeMap<String, String>>,
        raw_reset: Option<Vec<String>>,
    ) -> PhpResult {
        let message_expiry = optional_u64(message_expiry_micros, "message_expiry_micros")?
            .map(|micros| positive_update(micros, "message_expiry_micros").map(IggyExpiry::from))
            .transpose()?;
        let max_topic_size = optional_u64(max_topic_size, "max_topic_size")?
            .map(|bytes| positive_update(bytes, "max_topic_size").map(MaxTopicSize::from))
            .transpose()?;
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let (reset, raw_reset) = reset_names(reset, raw_reset)?;
        let options = TopicUpdateOptions {
            name,
            message_expiry,
            max_topic_size,
            reset,
            raw_reset,
            raw: raw.unwrap_or_default(),
        };
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .update_topic(&stream, &topic, &options)
                .await
                .map_err(to_php_exception)
        })
    }

    /// The server's option catalog for one resource: each key's kind and whether an update
    /// may set or reset it, whether it is secret, read-only or critical.
    pub fn describe_options(&self, scope: OptionsScope) -> PhpResult<Vec<OptionSpec>> {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .describe_options(scope.into())
                .await
                .map(|specs| specs.into_iter().map(OptionSpec::from).collect())
                .map_err(to_php_exception)
        })
    }

    /// Reads one page of the metadata change log, or of the current state.
    ///
    /// Needs root or the global `manage_servers` permission. `groups` takes
    /// `Iggy\MetadataChangeGroup` cases, empty for every group, and a current state scan
    /// needs the parent groups of each group. `since` and `until` are microseconds as an int
    /// or a decimal string, and only apply to `earliest` and `after`. `limit` is 1 to 1000,
    /// 100 when null. A query the server would refuse throws `Invalid command` (code 3)
    /// before anything is sent.
    #[php(defaults(groups = Vec::<MetadataChangeGroup>::new()))]
    pub fn get_metadata_changes(
        &self,
        start: &MetadataChangeStart,
        groups: Vec<MetadataChangeGroup>,
        since: Option<&Zval>,
        until: Option<&Zval>,
        limit: Option<&Zval>,
    ) -> PhpResult<MetadataChanges> {
        let timestamp = |micros: u64| {
            IggyTimestamp::checked_from_micros(micros)
                .ok_or_else(|| to_php_exception(IggyError::InvalidCommand))
        };
        let query = MetadataChangesQuery {
            start: start.inner.clone(),
            groups: groups.into_iter().map(Into::into).collect(),
            since: optional_u64(since, "since")?.map(timestamp).transpose()?,
            until: optional_u64(until, "until")?.map(timestamp).transpose()?,
            limit: optional_unsigned(limit, "limit")?
                .unwrap_or(MetadataChangesQuery::default().limit),
        };
        query.validate().map_err(to_php_exception)?;
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .get_metadata_changes(&query)
                .await
                .map(MetadataChanges::from)
                .map_err(to_php_exception)
        })
    }

    /// Gets a topic by stream and topic id/name.
    pub fn get_topic(
        &self,
        stream_id: PhpIdentifier,
        topic_id: PhpIdentifier,
    ) -> PhpResult<Option<TopicDetails>> {
        let stream_id: Identifier = stream_id.try_into()?;
        let topic_id: Identifier = topic_id.try_into()?;
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .get_topic(&stream_id, &topic_id)
                .await
                .map(|topic| topic.map(TopicDetails::from))
                .map_err(to_php_exception)
        })
    }

    /// Sends messages to a topic and returns the commit confirmations, one per
    /// partition the batch landed in.
    pub fn send_messages(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        partition_id: u32,
        messages: Vec<&SendMessage>,
    ) -> PhpResult<SendMessagesResponse> {
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let partitioning = Partitioning::partition_id(partition_id);
        let mut messages: Vec<RustMessage> = messages
            .into_iter()
            .map(|message| (*message).clone().inner)
            .collect();
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .send_messages(&stream, &topic, &partitioning, messages.as_mut())
                .await
                .map(SendMessagesResponse::from)
                .map_err(to_php_exception)
        })
    }

    /// Polls messages from the specified topic and partition.
    ///
    /// Each message keeps the partition context of this poll. To continue at an offset under
    /// that context, pass PollingStrategy::offset($offset)->withContext($message->context()).
    /// A stale context fails with HistoryUnavailable (87).
    ///
    /// Without a context, the poll uses the context of its route. It can fail once with
    /// HistoryUnavailable (87) after another client deleted and recreated the partition. The
    /// failed route is dropped, and the next call routes again.
    pub fn poll_messages(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        partition_id: u32,
        polling_strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> PhpResult<Vec<ReceiveMessage>> {
        let consumer = RustConsumer::default();
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let strategy: RustPollingStrategy = polling_strategy.into();
        let inner = self.inner.clone();

        runtime().block_on(async move {
            let polled_messages = inner
                .poll_messages(
                    &stream,
                    &topic,
                    Some(partition_id),
                    &consumer,
                    &strategy,
                    count,
                    auto_commit,
                )
                .await
                .map_err(to_php_exception)?;

            Ok(polled_messages
                .messages
                .into_iter()
                .map(|message| ReceiveMessage {
                    inner: message,
                    partition_id,
                    context: polled_messages.context,
                })
                .collect())
        })
    }

    /// Deletes a stream by id or name.
    pub fn delete_stream(&self, stream_id: PhpIdentifier) -> PhpResult {
        let stream_id: Identifier = stream_id.try_into()?;
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .delete_stream(&stream_id)
                .await
                .map_err(to_php_exception)
        })
    }

    /// Deletes a topic by stream and topic id/name.
    pub fn delete_topic(&self, stream_id: PhpIdentifier, topic_id: PhpIdentifier) -> PhpResult {
        let stream_id: Identifier = stream_id.try_into()?;
        let topic_id: Identifier = topic_id.try_into()?;
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .delete_topic(&stream_id, &topic_id)
                .await
                .map_err(to_php_exception)
        })
    }

    /// Creates and initializes a consumer group consumer.
    ///
    /// `$partition_id` is ignored for a consumer group: the member reads the partitions
    /// the server assigns to it.
    #[allow(clippy::too_many_arguments)]
    #[php(defaults(
        create_consumer_group_if_not_exists = true,
        auto_join_consumer_group = true,
        allow_replay = false
    ))]
    pub fn consumer_group(
        &self,
        name: String,
        stream: String,
        topic: String,
        partition_id: Option<&Zval>,
        polling_strategy: Option<&PollingStrategy>,
        batch_length: Option<&Zval>,
        auto_commit: Option<&AutoCommit>,
        create_consumer_group_if_not_exists: bool,
        auto_join_consumer_group: bool,
        poll_interval_micros: Option<&Zval>,
        polling_retry_interval_micros: Option<&Zval>,
        init_retries: Option<&Zval>,
        init_retry_interval_micros: Option<&Zval>,
        allow_replay: bool,
    ) -> PhpResult<IggyConsumer> {
        let partition_id = optional_unsigned::<u32>(partition_id, "partition_id")?;
        let batch_length = optional_unsigned::<u32>(batch_length, "batch_length")?;
        let poll_interval_micros =
            optional_unsigned::<u64>(poll_interval_micros, "poll_interval_micros")?;
        let polling_retry_interval_micros = optional_unsigned::<u64>(
            polling_retry_interval_micros,
            "polling_retry_interval_micros",
        )?;
        let init_retries = optional_unsigned::<u32>(init_retries, "init_retries")?;
        let init_retry_interval_micros =
            optional_unsigned::<u64>(init_retry_interval_micros, "init_retry_interval_micros")?;
        let mut builder = self
            .inner
            .consumer_group(&name, &stream, &topic)
            .map_err(to_php_exception)?
            .without_encryptor()
            .partition(partition_id);

        builder = if create_consumer_group_if_not_exists {
            builder.create_consumer_group_if_not_exists()
        } else {
            builder.do_not_create_consumer_group_if_not_exists()
        };
        builder = if auto_join_consumer_group {
            builder.auto_join_consumer_group()
        } else {
            builder.do_not_auto_join_consumer_group()
        };
        if let Some(polling_strategy) = polling_strategy {
            builder = builder.polling_strategy(polling_strategy.into());
        }
        if let Some(batch_length) = batch_length {
            builder = builder.batch_length(batch_length);
        }
        if let Some(auto_commit) = auto_commit {
            builder = builder.auto_commit(auto_commit.into());
        }
        builder = match poll_interval_micros {
            Some(micros) => builder.poll_interval(IggyDuration::from(micros)),
            None => builder.without_poll_interval(),
        };
        if let Some(micros) = polling_retry_interval_micros {
            builder = builder.polling_retry_interval(non_zero_duration_micros(
                "polling_retry_interval_micros",
                micros,
            )?);
        }

        match (init_retries, init_retry_interval_micros) {
            (Some(retries), Some(micros)) => {
                builder = builder.init_retries(
                    retries,
                    non_zero_duration_micros("init_retry_interval_micros", micros)?,
                );
            }
            (Some(_), None) => {
                return Err(to_php_exception(
                    "'init_retry_interval_micros' is required if 'init_retries' is set",
                ));
            }
            (None, Some(_)) => {
                return Err(to_php_exception(
                    "'init_retries' is required if 'init_retry_interval_micros' is set",
                ));
            }
            (None, None) => {}
        }
        if allow_replay {
            builder = builder.allow_replay();
        }

        let mut consumer = builder.build();
        runtime().block_on(async move {
            consumer.init().await.map_err(to_php_exception)?;
            let name = consumer.name().to_string();
            let stream = consumer.stream().to_string();
            let topic = consumer.topic().to_string();
            Ok(IggyConsumer {
                inner: Arc::new(Mutex::new(consumer)),
                name,
                stream,
                topic,
            })
        })
    }

    /// Sends a command code with a payload and returns the raw response bytes.
    /// Session-control codes return an invalid-command exception.
    pub fn send_binary_request(&self, code: u32, payload: Binary<u8>) -> PhpResult<Binary<u8>> {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .send_binary_request(code, Bytes::from(Vec::<u8>::from(payload)))
                .await
                .map(|response| Binary::new(Vec::from(response)))
                .map_err(to_php_exception)
        })
    }
}

pub(crate) fn non_zero_duration_micros(field: &str, micros: u64) -> PhpResult<NonZeroIggyDuration> {
    NonZeroIggyDuration::try_from(micros)
        .map_err(|_| to_php_exception(format!("'{field}' must be greater than 0 microseconds")))
}

fn pending_limit(
    value: Option<&Zval>,
    default: NonZeroUsize,
    field: &str,
) -> PhpResult<NonZeroUsize> {
    let Some(value) = optional_integer(value, field)? else {
        return Ok(default);
    };
    usize::try_from(value)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            to_php_exception(format!(
                "'{field}' must be a positive platform-sized integer"
            ))
        })
}

// u64 values above PHP_INT_MAX, such as the never-expire sentinel, only fit a decimal string.
fn optional_u64(value: Option<&Zval>, field: &str) -> PhpResult<Option<u64>> {
    value
        .filter(|value| !value.is_null())
        .map(|value| unsigned_argument(value, field))
        .transpose()
}

pub(crate) fn unsigned_argument(value: &Zval, field: &str) -> PhpResult<u64> {
    let number = match (value.long(), value.str()) {
        (Some(number), _) => u64::try_from(number).ok(),
        (None, Some(text))
            if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            text.parse::<u64>().ok()
        }
        _ => None,
    };
    number.ok_or_else(|| {
        to_php_exception(format!(
            "'{field}' must be a non-negative integer or a decimal string"
        ))
    })
}

fn positive_update(value: u64, field: &str) -> PhpResult<u64> {
    if value == 0 {
        return Err(to_php_exception(format!(
            "'{field}' must be greater than 0, reset it to return to the server default"
        )));
    }
    Ok(value)
}

fn stream_key(key: Binary<u8>) -> PhpResult<StreamEncryptionKey> {
    let bytes: Vec<u8> = key.into();
    str::from_utf8(&bytes)
        .map_err(|_| IggyError::InvalidEncryptionKey)
        .and_then(StreamEncryptionKey::from_str)
        .map_err(to_php_exception)
}

fn optional_unsigned<T: TryFrom<i64>>(value: Option<&Zval>, field: &str) -> PhpResult<Option<T>> {
    optional_integer(value, field)?
        .map(|value| {
            T::try_from(value).map_err(|_| {
                to_php_exception(format!(
                    "'{field}' must be non-negative and fit its integer range"
                ))
            })
        })
        .transpose()
}

// Option<i64> also maps a failed conversion to None in ext-php-rs. Validate the
// supplied value before applying defaults so overflow cannot silently reset a limit.
fn optional_integer(value: Option<&Zval>, field: &str) -> PhpResult<Option<i64>> {
    value
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .long()
                .ok_or_else(|| to_php_exception(format!("'{field}' must be an integer or null")))
        })
        .transpose()
}

fn operation_id_from_decimal(value: &str) -> PhpResult<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(to_php_exception(
            "'operation_id' must contain decimal digits only",
        ));
    }
    value
        .parse::<u64>()
        .map_err(|_| to_php_exception("'operation_id' exceeds the unsigned 64-bit range"))
}
