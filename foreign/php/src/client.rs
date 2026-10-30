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
    IggyClientBuilder, IggyExpiry, IggyMessage as RustMessage, MaxTopicSize, Partitioning,
    PollOptions as RustPollOptions, PollingStrategy as RustPollingStrategy, *,
};
use tokio::sync::Mutex;

use crate::consumer::{ConsumerOffsetInfo, IggyConsumer};
use crate::durability::Durability as PhpDurability;
use crate::error::to_php_exception;
use crate::identifier::PhpIdentifier;
use crate::metadata_changes::{MetadataChangeGroup, MetadataChangeStart, MetadataChanges};
use crate::options::{OptionSpec, OptionsScope, StreamOptionKey, TopicOptionKey, reset_names};
use crate::pending_poll::PendingPoll;
use crate::poll_options::PollOptions;
use crate::reader::PartitionReader;
use crate::receive_message::{Checkpoint, PolledMessages, PollingStrategy};
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
    /// A fetch progress block raises code 14010 without replay. Only explicit
    /// abandonOperation releases it, accepting the unknown server outcome.
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

    /// Polls messages from one partition of a topic, as the standalone consumer `consumer`.
    ///
    /// PollingStrategy::next() starts at the stored offset of `consumer`. `options` defaults to
    /// `new PollOptions()`: up to 1000 messages and no wait. With CommitPolicy::onRead(),
    /// the server stores the offset of the batch for `consumer`.
    ///
    /// To read on, pass `$polled->continuation()` as the next strategy. It keeps the partition
    /// history of the batch, so a read after the partition was recreated fails with
    /// HistoryUnavailable (87), and a read of another partition refuses it with InvalidCommand
    /// (3).
    ///
    /// A fresh strategy uses the context of its route. It can fail once with
    /// HistoryUnavailable (87) after another client deleted and recreated the partition. The
    /// failed route is dropped, and the next call routes again.
    ///
    /// `consumer` polls as a standalone consumer, or as a member of the consumer group of that
    /// id when `group` is true, which reads only the partitions assigned to it.
    ///
    /// Under CommitPolicy::onRead(), a lost reply throws FetchProgressUncertain (14010),
    /// because the server can still store the offset. The partition stays blocked, and the
    /// next poll of it throws PartitionLaneBlocked (14009), until abandonOperation().
    #[allow(clippy::too_many_arguments)]
    pub fn poll_messages(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        partition_id: u32,
        consumer: PhpIdentifier,
        polling_strategy: &PollingStrategy,
        options: Option<&PollOptions>,
        group: Option<bool>,
    ) -> PhpResult<PolledMessages> {
        let consumer = consumer_identity(consumer, group)?;
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let strategy: RustPollingStrategy = polling_strategy.into();
        let options = options.map_or_else(RustPollOptions::default, |options| options.inner);
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .poll_messages(&stream, &topic, partition_id, &consumer, &strategy, options)
                .await
                .map(PolledMessages::from)
                .map_err(to_php_exception)
        })
    }

    /// Starts the poll of pollMessages() in the background and returns at once, so the caller
    /// can wait for its batch in steps with PendingPoll::wait() or cancel it with
    /// PendingPoll::cancel(). Invalid arguments throw here, a failed poll throws from wait().
    #[allow(clippy::too_many_arguments)]
    pub fn start_poll_messages(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        partition_id: u32,
        consumer: PhpIdentifier,
        polling_strategy: &PollingStrategy,
        options: Option<&PollOptions>,
        group: Option<bool>,
    ) -> PhpResult<PendingPoll> {
        let consumer = consumer_identity(consumer, group)?;
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let strategy: RustPollingStrategy = polling_strategy.into();
        let options = options.map_or_else(RustPollOptions::default, |options| options.inner);
        let inner = self.inner.clone();

        let task = runtime().spawn(async move {
            inner
                .poll_messages(&stream, &topic, partition_id, &consumer, &strategy, options)
                .await
        });
        Ok(PendingPoll::from(task))
    }

    /// Deletes the stored offset of the partition of a checkpoint for a standalone consumer, or
    /// for a consumer group when `group` is true, under the partition history that delivered
    /// the checkpoint. The offset of the checkpoint is not used.
    ///
    /// Throws HistoryUnavailable (87) after the partition was deleted and created again,
    /// ConsumerGroupPartitionNotOwned (5009) after its consumer group owner changed, and
    /// ConsumerOffsetNotFound (3021) when no offset is stored. HTTP sends no partition history,
    /// so there the server deletes the offset of the history it holds.
    pub fn delete_consumer_position(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        consumer: PhpIdentifier,
        checkpoint: &Checkpoint,
        group: Option<bool>,
    ) -> PhpResult {
        let consumer = consumer_identity(consumer, group)?;
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let checkpoint = checkpoint.inner;
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .delete_consumer_position(&consumer, &stream, &topic, &checkpoint)
                .await
                .map_err(to_php_exception)
        })
    }

    /// Stores the offset of a checkpoint for a standalone consumer, or for a consumer group
    /// when `group` is true, under the partition history that delivered the checkpoint, which
    /// covers every earlier message of its partition.
    ///
    /// Throws HistoryUnavailable (87) after the partition was deleted and created again, and
    /// ConsumerGroupPartitionNotOwned (5009) after its consumer group owner changed, instead of
    /// storing the offset on the new partition.
    pub fn store_consumer_position(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        consumer: PhpIdentifier,
        checkpoint: &Checkpoint,
        group: Option<bool>,
    ) -> PhpResult {
        let consumer = consumer_identity(consumer, group)?;
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let checkpoint = checkpoint.inner;
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .store_consumer_position(&consumer, &stream, &topic, &checkpoint)
                .await
                .map_err(to_php_exception)
        })
    }

    /// Stores `offset` on `partition_id` for a standalone consumer, or for a consumer group
    /// when `group` is true, under the partition history of its route. After another client
    /// deleted and created the partition again, it can throw HistoryUnavailable (87) once, and
    /// the next call routes again. Prefer storeConsumerPosition() with a checkpoint.
    pub fn store_consumer_offset(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        consumer: PhpIdentifier,
        partition_id: u32,
        offset: u64,
        group: Option<bool>,
    ) -> PhpResult {
        let consumer = consumer_identity(consumer, group)?;
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .store_consumer_offset(&consumer, &stream, &topic, Some(partition_id), offset)
                .await
                .map_err(to_php_exception)
        })
    }

    /// Returns the offset stored on `partition_id` for a standalone consumer, or for a consumer
    /// group when `group` is true, or null when nothing is stored. In a cluster the node of the
    /// connection answers from its own replica, which can lag a store that just returned.
    pub fn get_consumer_offset(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        consumer: PhpIdentifier,
        partition_id: u32,
        group: Option<bool>,
    ) -> PhpResult<Option<ConsumerOffsetInfo>> {
        let consumer = consumer_identity(consumer, group)?;
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let inner = self.inner.clone();

        runtime().block_on(async move {
            let offset = inner
                .get_consumer_offset(&consumer, &stream, &topic, Some(partition_id))
                .await
                .map_err(to_php_exception)?;
            Ok(offset.map(|offset| ConsumerOffsetInfo {
                partition_id: offset.partition_id,
                current_offset: offset.current_offset,
                stored_offset: offset.stored_offset,
            }))
        })
    }

    /// Deletes the offset stored on `partition_id` for a standalone consumer, or for a consumer
    /// group when `group` is true. Throws ConsumerOffsetNotFound (3021) when nothing is stored.
    /// Prefer deleteConsumerPosition() with a checkpoint.
    pub fn delete_consumer_offset(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        consumer: PhpIdentifier,
        partition_id: u32,
        group: Option<bool>,
    ) -> PhpResult {
        let consumer = consumer_identity(consumer, group)?;
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .delete_consumer_offset(&consumer, &stream, &topic, Some(partition_id))
                .await
                .map_err(to_php_exception)
        })
    }

    /// Builds a reader of chosen partitions as a standalone consumer.
    ///
    /// `partitions` must not be empty. `start` is the start of a partition without a stored
    /// offset, PollingStrategy::next() when null, and a continuation is refused. `options`
    /// defaults to a wait of 5 seconds for up to 1000 messages per request: a reader that waits
    /// 0 polls an idle topic in a busy loop. `prefetch_bytes`, 16 MiB when null, and
    /// `prefetch_messages`, the larger of 16000 and `count` when null, bound the requests in
    /// flight and the queued batches.
    #[allow(clippy::too_many_arguments)]
    pub fn reader(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        consumer: PhpIdentifier,
        partitions: Vec<u32>,
        start: Option<&PollingStrategy>,
        options: Option<&PollOptions>,
        prefetch_bytes: Option<&Zval>,
        prefetch_messages: Option<&Zval>,
    ) -> PhpResult<PartitionReader> {
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let consumer = RustConsumer::new(consumer.try_into()?);
        let builder = self
            .inner
            .reader(&stream, &topic, &consumer)
            .partitions(partitions);

        build_reader(builder, start, options, prefetch_bytes, prefetch_messages)
    }

    /// Builds a reader of the partitions that a consumer group assigns to this member.
    ///
    /// The group must exist: the reader joins it, joins again after a session loss, and leaves
    /// it in close(). HTTP cannot join a group, so it throws FeatureUnavailable. The other
    /// arguments are those of reader().
    #[allow(clippy::too_many_arguments)]
    pub fn group_reader(
        &self,
        stream: PhpIdentifier,
        topic: PhpIdentifier,
        group: PhpIdentifier,
        start: Option<&PollingStrategy>,
        options: Option<&PollOptions>,
        prefetch_bytes: Option<&Zval>,
        prefetch_messages: Option<&Zval>,
    ) -> PhpResult<PartitionReader> {
        let stream: Identifier = stream.try_into()?;
        let topic: Identifier = topic.try_into()?;
        let group = RustConsumer::group(group.try_into()?);
        let builder = self.inner.reader(&stream, &topic, &group);

        build_reader(builder, start, options, prefetch_bytes, prefetch_messages)
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
    /// The member reads the partitions that the server assigns to it. `polling_strategy` is
    /// the start of a partition without a stored offset, PollingStrategy::next() when null, and
    /// a continuation is refused. `poll_options` defaults to a wait of 5 seconds for up to 1000
    /// messages per request: a consumer that waits 0 polls an idle topic in a busy loop.
    /// The `commit` of `poll_options` says when offsets are stored. CommitPolicy::manual(), the
    /// default, stores nothing: commit checkpoints after processing, or each restart reads the
    /// same messages again. With CommitPolicy::onRead() the server stores the offset of each
    /// batch that it reads, and CommitPolicy::auto() stores counted positions.
    /// `prefetch_bytes` and `prefetch_messages` are those of reader().
    #[allow(clippy::too_many_arguments)]
    #[php(defaults(create_consumer_group_if_not_exists = true, allow_replay = false))]
    pub fn consumer_group(
        &self,
        name: String,
        stream: String,
        topic: String,
        polling_strategy: Option<&PollingStrategy>,
        poll_options: Option<&PollOptions>,
        prefetch_bytes: Option<&Zval>,
        prefetch_messages: Option<&Zval>,
        create_consumer_group_if_not_exists: bool,
        polling_retry_interval_micros: Option<&Zval>,
        init_retries: Option<&Zval>,
        init_retry_interval_micros: Option<&Zval>,
        allow_replay: bool,
    ) -> PhpResult<IggyConsumer> {
        let settings = ConsumerSettings::new(
            polling_strategy,
            poll_options,
            prefetch_bytes,
            prefetch_messages,
            polling_retry_interval_micros,
            init_retries,
            init_retry_interval_micros,
            allow_replay,
        )?;
        let builder = self
            .inner
            .consumer_group(&name, &stream, &topic)
            .map_err(to_php_exception)?
            .without_encryptor();
        let builder = if create_consumer_group_if_not_exists {
            builder.create_consumer_group_if_not_exists()
        } else {
            builder.do_not_create_consumer_group_if_not_exists()
        };
        initialized_consumer(settings.apply(builder))
    }

    /// Creates and initializes a standalone consumer of `partitions`, identified by `name`: it
    /// stores its offsets under that consumer name and joins no group. `partitions` must not be
    /// empty. The other arguments are those of consumerGroup().
    #[allow(clippy::too_many_arguments)]
    #[php(defaults(allow_replay = false))]
    pub fn consumer(
        &self,
        name: String,
        stream: String,
        topic: String,
        partitions: Vec<u32>,
        polling_strategy: Option<&PollingStrategy>,
        poll_options: Option<&PollOptions>,
        prefetch_bytes: Option<&Zval>,
        prefetch_messages: Option<&Zval>,
        polling_retry_interval_micros: Option<&Zval>,
        init_retries: Option<&Zval>,
        init_retry_interval_micros: Option<&Zval>,
        allow_replay: bool,
    ) -> PhpResult<IggyConsumer> {
        let settings = ConsumerSettings::new(
            polling_strategy,
            poll_options,
            prefetch_bytes,
            prefetch_messages,
            polling_retry_interval_micros,
            init_retries,
            init_retry_interval_micros,
            allow_replay,
        )?;
        let builder = self
            .inner
            .consumer(&name, &stream, &topic)
            .map_err(to_php_exception)?
            .without_encryptor()
            .partitions(partitions);
        initialized_consumer(settings.apply(builder))
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

    /// Sends a command code with a payload to the replica with the given id, as
    /// cluster metadata names it, and returns the raw response bytes.
    /// Replicated commands, polls, consumer-offset reads and session-control
    /// codes return an invalid-command exception, and a replica that cluster
    /// metadata does not name returns an invalid-identifier exception.
    pub fn send_binary_request_to_replica(
        &self,
        code: u32,
        payload: Binary<u8>,
        replica_id: u8,
    ) -> PhpResult<Binary<u8>> {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .send_binary_request_to_replica(
                    code,
                    Bytes::from(Vec::<u8>::from(payload)),
                    replica_id,
                )
                .await
                .map(|response| Binary::new(Vec::from(response)))
                .map_err(to_php_exception)
        })
    }
}

fn build_reader(
    mut builder: ReaderBuilder,
    start: Option<&PollingStrategy>,
    options: Option<&PollOptions>,
    prefetch_bytes: Option<&Zval>,
    prefetch_messages: Option<&Zval>,
) -> PhpResult<PartitionReader> {
    if let Some(start) = start {
        builder = builder.start(start.into());
    }
    if let Some(options) = options {
        builder = builder.options(options.inner);
    }
    if let Some(bytes) = optional_unsigned(prefetch_bytes, "prefetch_bytes")? {
        builder = builder.prefetch_bytes(bytes);
    }
    if let Some(messages) = optional_unsigned(prefetch_messages, "prefetch_messages")? {
        builder = builder.prefetch_messages(messages);
    }

    runtime()
        .block_on(builder.build())
        .map(PartitionReader::from)
        .map_err(to_php_exception)
}

/// The consumer identity of a client call: a consumer group when `group` is true.
fn consumer_identity(consumer: PhpIdentifier, group: Option<bool>) -> PhpResult<RustConsumer> {
    let id = consumer.try_into()?;
    Ok(if group.unwrap_or(false) {
        RustConsumer::group(id)
    } else {
        RustConsumer::new(id)
    })
}

/// The settings that a standalone consumer and a consumer group member share, checked before
/// the consumer is built.
struct ConsumerSettings {
    polling_strategy: Option<RustPollingStrategy>,
    poll_options: Option<RustPollOptions>,
    prefetch_bytes: Option<u32>,
    prefetch_messages: Option<u32>,
    polling_retry_interval: Option<NonZeroIggyDuration>,
    init_retries: Option<(u32, NonZeroIggyDuration)>,
    allow_replay: bool,
}

impl ConsumerSettings {
    #[allow(clippy::too_many_arguments)]
    fn new(
        polling_strategy: Option<&PollingStrategy>,
        poll_options: Option<&PollOptions>,
        prefetch_bytes: Option<&Zval>,
        prefetch_messages: Option<&Zval>,
        polling_retry_interval_micros: Option<&Zval>,
        init_retries: Option<&Zval>,
        init_retry_interval_micros: Option<&Zval>,
        allow_replay: bool,
    ) -> PhpResult<Self> {
        let polling_retry_interval = optional_unsigned::<u64>(
            polling_retry_interval_micros,
            "polling_retry_interval_micros",
        )?
        .map(|micros| non_zero_duration_micros("polling_retry_interval_micros", micros))
        .transpose()?;
        let init_retries = match (
            optional_unsigned::<u32>(init_retries, "init_retries")?,
            optional_unsigned::<u64>(init_retry_interval_micros, "init_retry_interval_micros")?,
        ) {
            (Some(retries), Some(micros)) => Some((
                retries,
                non_zero_duration_micros("init_retry_interval_micros", micros)?,
            )),
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
            (None, None) => None,
        };
        Ok(Self {
            polling_strategy: polling_strategy.map(Into::into),
            poll_options: poll_options.map(|options| options.inner),
            prefetch_bytes: optional_unsigned(prefetch_bytes, "prefetch_bytes")?,
            prefetch_messages: optional_unsigned(prefetch_messages, "prefetch_messages")?,
            polling_retry_interval,
            init_retries,
            allow_replay,
        })
    }

    fn apply(self, mut builder: IggyConsumerBuilder) -> IggyConsumerBuilder {
        if let Some(polling_strategy) = self.polling_strategy {
            builder = builder.polling_strategy(polling_strategy);
        }
        if let Some(poll_options) = self.poll_options {
            builder = builder.poll_options(poll_options);
        }
        if let Some(bytes) = self.prefetch_bytes {
            builder = builder.prefetch_bytes(bytes);
        }
        if let Some(messages) = self.prefetch_messages {
            builder = builder.prefetch_messages(messages);
        }
        if let Some(interval) = self.polling_retry_interval {
            builder = builder.polling_retry_interval(interval);
        }
        if let Some((retries, interval)) = self.init_retries {
            builder = builder.init_retries(retries, interval);
        }
        if self.allow_replay {
            builder = builder.allow_replay();
        }
        builder
    }
}

/// Builds the consumer and initializes it, which joins a group member to its group.
fn initialized_consumer(builder: IggyConsumerBuilder) -> PhpResult<IggyConsumer> {
    let mut consumer = builder.build();
    runtime().block_on(async move {
        consumer.init().await.map_err(to_php_exception)?;
        let name = consumer.name().to_string();
        let stream = consumer.stream().to_string();
        let topic = consumer.topic().to_string();
        let state = consumer.state();
        Ok(IggyConsumer {
            inner: Arc::new(Mutex::new(consumer)),
            state,
            name,
            stream,
            topic,
        })
    })
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

pub(crate) fn optional_unsigned<T: TryFrom<i64>>(
    value: Option<&Zval>,
    field: &str,
) -> PhpResult<Option<T>> {
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
