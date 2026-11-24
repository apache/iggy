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

use iggy::prelude::{
    CompressionAlgorithm, GeoMode as RustGeoMode, GeoTopicDurations, IggyByteSize, IggyError,
    IggyExpiry as RustIggyExpiry, MaxTopicSize as RustMaxTopicSize, OptionKey as RustOptionKey,
    PartitionResizePolicy as RustPartitionResizePolicy, StreamCreateOptions, StreamEncryptionKey,
    TopicCreateOptions, TopicType as RustTopicType, UserCreateOptions,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDelta, PyString};
use pyo3_stub_gen::derive::{
    gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pyfunction, gen_stub_pymethods,
};
use std::collections::BTreeMap;
use std::str::FromStr;

use crate::durability::Durability;
use crate::duration::py_delta_to_iggy_duration;
use crate::topic::{IggyExpiry, MaxTopicSize};

/// Options for `create_stream`.
///
/// The encryption key is write-only: the server stores it wrapped and returns
/// only its source and fingerprints, and this object never shows it.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct StreamOptions {
    pub(crate) inner: StreamCreateOptions,
}

/// Options for `create_topic`.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct TopicOptions {
    pub(crate) inner: TopicCreateOptions,
}

/// Options for `create_user`. No user key exists yet, so only the raw escape
/// hatch is available.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct UserOptions {
    pub(crate) inner: UserCreateOptions,
}

/// A catalog option key, as `update_stream`, `update_topic` and `update_user`
/// take it in `reset`. `str()` gives the key as it travels on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum OptionKey {
    /// Stream or topic name.
    Name,
    /// User name.
    Username,
    /// User status.
    Status,
    /// Topic compression algorithm, fixed at creation.
    CompressionAlgorithm,
    /// Topic message expiry, resettable to never expire.
    MessageExpiry,
    /// Topic size cap, resettable to unlimited.
    MaxTopicSize,
    /// Topic segment size, fixed at creation.
    SegmentSize,
    /// Topic message durability, fixed at creation.
    Durability,
    /// Topic consumer offset durability, fixed at creation.
    ConsumerOffsetDurability,
    /// Topic partition resize policy, fixed at creation.
    PartitionResizePolicy,
    /// Topic message-count flush threshold, fixed at creation.
    MessagesRequiredToSave,
    /// Topic byte flush threshold, fixed at creation.
    SizeOfMessagesRequiredToSave,
    /// Topic segment preallocation, fixed at creation.
    PreallocateSegments,
    /// Topic replication factor, fixed at creation.
    ReplicationFactor,
    /// Stream encryption key, write-only and fixed at creation.
    EncryptionKey,
    /// Who chose the stream's key, computed by the server.
    EncryptionKeySource,
    /// Fingerprint of the stream's key, computed by the server.
    EncryptionKeyFingerprint,
    /// Fingerprints of the server keys wrapping the stream's key, computed by
    /// the server.
    EncryptionKekFingerprint,
    /// Topic segment age roll, fixed at creation.
    SegmentMaxAge,
    /// Topic tiered storage. An update only turns it on.
    TieredStorage,
    /// Time a tiered topic keeps an uploaded segment on local disk,
    /// resettable to one day.
    LocalRetentionAge,
    /// Cap on the uploaded segments a tiered topic keeps on local disk,
    /// resettable to unlimited.
    LocalRetentionSize,
    /// Topic type, normal or geo, fixed at creation.
    TopicType,
    /// How a geo topic writes its messages, fixed at creation.
    GeoMode,
    /// Longest time a send to a geo topic waits for its upload to start.
    /// An update can set it, not reset it.
    FreshUploadInterval,
    /// Shortest time between two uploads of the positions a geo topic's
    /// stored offsets wait for. An update can set it, not reset it.
    ConsumerOffsetUploadInterval,
    /// How often a node uploads a geo topic's changed positions. An update
    /// can set it, not reset it.
    PositionsPublishInterval,
    /// How long a deleted geo topic's tombstone and objects stay. An update
    /// can set it, not reset it.
    TombstoneGrace,
    /// How long a geo topic's group polls wait for the peers' positions after
    /// a node starts. An update can set it, not reset it.
    PositionsResumeTimeout,
    /// Longest wait between two long-term rewrites of a geo topic's
    /// partition. An update can set it, not reset it.
    LongTermRewriteMaxInterval,
    /// The geo origin a shadow stream serves, computed by the server.
    ShadowOrigin,
}

/// Whether partitions can be added to or deleted from a topic after creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum PartitionResizePolicy {
    /// Partitions can be added and deleted.
    Mutable,
    /// The partition set is fixed at topic creation.
    Fixed,
}

/// What a topic is, fixed at creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum TopicType {
    /// Its messages stay in this cluster.
    Normal,
    /// Its messages also become readable in the other datacenters, through
    /// the object store.
    Geo,
}

/// How a geo topic writes its messages, fixed at creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum GeoMode {
    /// A batch goes to the object store before it commits.
    Geo,
    /// A batch is replicated first, and sealed segments are uploaded later.
    GeoTiered,
}

#[derive(FromPyObject)]
pub(crate) enum PyStreamKey<'py> {
    #[pyo3(transparent, annotation = "bytes")]
    Raw(Bound<'py, PyBytes>),
    #[pyo3(transparent, annotation = "str")]
    Base64(Bound<'py, PyString>),
}

#[gen_stub_pymethods]
#[pymethods]
impl StreamOptions {
    /// Constructs stream create options.
    ///
    /// Args:
    ///     encryption_key: The stream's own data key: exactly 32 bytes, or
    ///         their canonical standard padded base64 text. Use 32 random
    ///         bytes, for example `secrets.token_bytes(32)`. The key travels
    ///         inside the create request over whatever transport the client
    ///         uses, so protecting it in transit (TLS) is the caller's
    ///         responsibility. The server never returns it, only its source
    ///         and fingerprints. Without a key, a server with encryption
    ///         enabled generates one for the stream.
    ///     raw: Additional option keys as `dict[str, str]`, sent as string
    ///         values so a key newer than this build can be set. A typed field
    ///         wins on collision.
    ///
    /// Raises:
    ///     ValueError: If `encryption_key` is not exactly 32 bytes or their
    ///         canonical base64 text. The message never contains the key.
    ///     TypeError: If `encryption_key` is neither `bytes` nor `str`.
    #[new]
    #[pyo3(signature = (*, encryption_key=None, raw=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.bytes | builtins.str | None"))]
        encryption_key: Option<PyStreamKey<'_>>,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, builtins.str] | None"))]
        raw: Option<BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: StreamCreateOptions {
                encryption_key: encryption_key
                    .map(StreamEncryptionKey::try_from)
                    .transpose()?,
                raw: raw.unwrap_or_default(),
            },
        })
    }

    /// Whether an encryption key is set, without exposing the key itself.
    #[getter]
    fn has_encryption_key(&self) -> bool {
        self.inner.encryption_key.is_some()
    }

    fn __repr__(&self) -> String {
        let encryption_key = if self.inner.encryption_key.is_some() {
            "..."
        } else {
            "None"
        };
        let secret_key: &str = RustOptionKey::EncryptionKey.into();
        let raw = self
            .inner
            .raw
            .iter()
            .map(|(key, value)| {
                let shown = if key == secret_key {
                    "...".to_owned()
                } else {
                    format!("{value:?}")
                };
                format!("{key:?}: {shown}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("StreamOptions(encryption_key={encryption_key}, raw={{{raw}}})")
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl TopicOptions {
    /// Constructs topic create options.
    ///
    /// Every setting left as `None` resolves against the server default at
    /// admission. Only `message_expiry`, `max_topic_size`, `tiered_storage`,
    /// `local_retention_age`, `local_retention_size`, the six durations of a
    /// geo topic and the name can change after creation, through
    /// `update_topic`.
    ///
    /// The six durations, `fresh_upload_interval` to
    /// `long_term_rewrite_max_interval`, are `datetime.timedelta | None` and
    /// go only on a geo topic, where each overrides the server setting of the
    /// same name on every node. `None` leaves a duration unset, so each node
    /// applies its own setting, and zero is a value. The server refuses them
    /// on a normal topic, and a value out of range, with an `OptionError` of
    /// kind `invalid_option_value`.
    ///
    /// Args:
    ///     partitions_count: Number of partitions as `int`, at most 1000.
    ///         Defaults to 1.
    ///     compression_algorithm: Compression algorithm as `str | None`.
    ///     message_expiry: Message expiry as `IggyExpiry | None`.
    ///         `IggyExpiry.ServerDefault()` is the same as `None`.
    ///     max_topic_size: Maximum topic size as `MaxTopicSize | None`.
    ///         `MaxTopicSize.ServerDefault()` is the same as `None`.
    ///     segment_size: Per-topic segment size in bytes as `int | None`.
    ///     durability: Message completion policy, defaulting to replicated.
    ///     consumer_offset_durability: Independent offset policy, defaulting
    ///         to replicated.
    ///     partition_resize_policy: `PartitionResizePolicy.Fixed` makes the
    ///         server reject later partition creation and deletion on this
    ///         topic.
    ///     messages_required_to_save: Message-count flush threshold as
    ///         `int | None`.
    ///     size_of_messages_required_to_save: Byte flush threshold as
    ///         `int | None`.
    ///     preallocate_segments: Reserve segment bytes on open as `bool | None`.
    ///     replication_factor: Nodes that store each partition as
    ///         `int | None`, 1 to 32 and at most the cluster's node count.
    ///         `None` applies the server's `[cluster] default_replication_factor`,
    ///         every node unless an operator set a number. A write commits on
    ///         a quorum of these nodes, so 2 keeps two copies but stops
    ///         accepting writes while either node is down. The server rejects
    ///         an out-of-range value with an `OptionError` of kind
    ///         `invalid_replication_factor`.
    ///     segment_max_age: Age past a segment's first batch at which a batch
    ///         starts the next segment, as `IggyExpiry | None`, at least 10
    ///         minutes. `IggyExpiry.ServerDefault()` is the same as `None`,
    ///         which rolls segments by size alone. The server refuses a
    ///         duration together with `preallocate_segments=True`.
    ///     tiered_storage: Move sealed segments to the object store as
    ///         `bool | None`. `None` leaves it off. The server refuses `True`
    ///         with `FeatureUnavailable` until it tiers topics.
    ///     local_retention_age: How long a tiered topic keeps an uploaded
    ///         segment on local disk as `IggyExpiry | None`, one day when
    ///         `None`. `IggyExpiry.ServerDefault()` is the same as `None`.
    ///         The server refuses an age above a finite `message_expiry`.
    ///     local_retention_size: Topic-wide cap on the uploaded segments a
    ///         tiered topic keeps on local disk as `MaxTopicSize | None`,
    ///         unlimited when `None`. `MaxTopicSize.ServerDefault()` is the
    ///         same as `None`. The server refuses a size above a finite
    ///         `max_topic_size`.
    ///     topic_type: `TopicType.Geo` also makes the topic's messages
    ///         readable in the other datacenters, through the object store.
    ///         `None` creates a normal topic. A geo topic needs an object
    ///         store on every node of the cluster: while one is not ready,
    ///         the server refuses `TopicType.Geo` with `FeatureUnavailable`.
    ///         A geo topic refuses tiered storage and an encrypted stream,
    ///         and keeps its name, options and partition count.
    ///     geo_mode: How a geo topic writes its messages, as `GeoMode | None`.
    ///         `None` resolves to `GeoMode.Geo` on a geo topic. The server
    ///         refuses `GeoMode.GeoTiered` with `FeatureUnavailable` for now,
    ///         and any mode on a normal topic.
    ///     fresh_upload_interval: Longest time a send waits for the upload of
    ///         its fresh object to start, at most 10 seconds. One object
    ///         carries the sends of every partition a shard leads.
    ///     consumer_offset_upload_interval: Shortest time between two uploads
    ///         of the positions that the offsets consumer groups store
    ///         themselves wait for, at most 1 second. Zero answers such a
    ///         store at its local commit.
    ///     positions_publish_interval: How often a node uploads the changed
    ///         positions of the topic's consumer groups, above zero.
    ///     tombstone_grace: How long the tombstone and the objects of the
    ///         deleted topic stay in the object store, above zero.
    ///     positions_resume_timeout: How long, after a node starts, the group
    ///         polls of the topic's partitions wait for the positions of the
    ///         geo peers. Zero does not wait.
    ///     long_term_rewrite_max_interval: Longest wait between two long-term
    ///         rewrites of a partition, at least 1 second.
    ///     raw: Additional option keys as `dict[str, str]`, sent as string
    ///         values so a key newer than this build can be set. A typed field
    ///         wins on collision.
    ///
    /// Raises:
    ///     ValueError: If `message_expiry`, `max_topic_size`,
    ///         `segment_max_age`, `local_retention_age` or
    ///         `local_retention_size` is out of range, or a duration is
    ///         negative or longer than `2**64 - 1` microseconds.
    ///     RuntimeError: If `compression_algorithm` names no known algorithm.
    ///     TypeError: If a durability is not a `Durability`.
    #[new]
    #[pyo3(
        signature = (*, partitions_count = 1, compression_algorithm = None, message_expiry = None, max_topic_size = None, segment_size = None, durability = None, consumer_offset_durability = None, partition_resize_policy = None, messages_required_to_save = None, size_of_messages_required_to_save = None, preallocate_segments = None, replication_factor = None, segment_max_age = None, tiered_storage = None, local_retention_age = None, local_retention_size = None, topic_type = None, geo_mode = None, fresh_upload_interval = None, consumer_offset_upload_interval = None, positions_publish_interval = None, tombstone_grace = None, positions_resume_timeout = None, long_term_rewrite_max_interval = None, raw = None)
    )]
    #[allow(clippy::too_many_arguments)]
    fn new(
        partitions_count: u32,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] compression_algorithm: Option<
            String,
        >,
        #[gen_stub(override_type(type_repr = "IggyExpiry | None"))] message_expiry: Option<
            &IggyExpiry,
        >,
        #[gen_stub(override_type(type_repr = "MaxTopicSize | None"))] max_topic_size: Option<
            &MaxTopicSize,
        >,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] segment_size: Option<u64>,
        #[gen_stub(override_type(type_repr = "Durability | None"))] durability: Option<
            &Bound<'_, PyAny>,
        >,
        #[gen_stub(override_type(type_repr = "Durability | None"))]
        consumer_offset_durability: Option<&Bound<'_, PyAny>>,
        #[gen_stub(override_type(type_repr = "PartitionResizePolicy | None"))]
        partition_resize_policy: Option<PartitionResizePolicy>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))]
        messages_required_to_save: Option<u32>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))]
        size_of_messages_required_to_save: Option<u64>,
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))] preallocate_segments: Option<
            bool,
        >,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] replication_factor: Option<
            u32,
        >,
        #[gen_stub(override_type(type_repr = "IggyExpiry | None"))] segment_max_age: Option<
            &IggyExpiry,
        >,
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))] tiered_storage: Option<bool>,
        #[gen_stub(override_type(type_repr = "IggyExpiry | None"))] local_retention_age: Option<
            &IggyExpiry,
        >,
        #[gen_stub(override_type(type_repr = "MaxTopicSize | None"))] local_retention_size: Option<
            &MaxTopicSize,
        >,
        #[gen_stub(override_type(type_repr = "TopicType | None"))] topic_type: Option<TopicType>,
        #[gen_stub(override_type(type_repr = "GeoMode | None"))] geo_mode: Option<GeoMode>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        fresh_upload_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        consumer_offset_upload_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        positions_publish_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        tombstone_grace: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        positions_resume_timeout: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        long_term_rewrite_max_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, builtins.str] | None"))]
        raw: Option<BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        let compression_algorithm = compression_algorithm
            .map(|name| CompressionAlgorithm::from_str(&name).map_err(PyRuntimeError::new_err))
            .transpose()?;
        // `None` is what tells admission to resolve the server default, so the
        // explicit sentinels collapse back to it.
        let message_expiry = message_expiry
            .map(RustIggyExpiry::try_from)
            .transpose()?
            .filter(|expiry| *expiry != RustIggyExpiry::ServerDefault);
        let max_topic_size = max_topic_size
            .map(RustMaxTopicSize::try_from)
            .transpose()?
            .filter(|size| *size != RustMaxTopicSize::ServerDefault);
        let segment_max_age = segment_max_age
            .map(RustIggyExpiry::try_from)
            .transpose()?
            .filter(|age| *age != RustIggyExpiry::ServerDefault);
        let local_retention_age = local_retention_age
            .map(RustIggyExpiry::try_from)
            .transpose()?
            .filter(|age| *age != RustIggyExpiry::ServerDefault);
        let local_retention_size = local_retention_size
            .map(RustMaxTopicSize::try_from)
            .transpose()?
            .filter(|size| *size != RustMaxTopicSize::ServerDefault);
        Ok(Self {
            inner: TopicCreateOptions {
                partitions_count: Some(partitions_count),
                compression_algorithm,
                message_expiry,
                max_topic_size,
                segment_size: segment_size.map(IggyByteSize::from),
                durability: Durability::try_from(durability)?.0,
                consumer_offset_durability: Durability::try_from(consumer_offset_durability)?.0,
                partition_resize_policy: partition_resize_policy
                    .map(RustPartitionResizePolicy::from),
                messages_required_to_save,
                size_of_messages_required_to_save: size_of_messages_required_to_save
                    .map(IggyByteSize::from),
                preallocate_segments,
                replication_factor,
                segment_max_age,
                tiered_storage,
                local_retention_age,
                local_retention_size,
                topic_type: topic_type.map(RustTopicType::from),
                geo_mode: geo_mode.map(RustGeoMode::from),
                geo_durations: geo_topic_durations([
                    fresh_upload_interval,
                    consumer_offset_upload_interval,
                    positions_publish_interval,
                    tombstone_grace,
                    positions_resume_timeout,
                    long_term_rewrite_max_interval,
                ])?,
                raw: raw.unwrap_or_default(),
            },
        })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl UserOptions {
    /// Constructs user create options.
    ///
    /// Args:
    ///     raw: Option keys as `dict[str, str]`, sent as string values so a
    ///         key newer than this build can be set. A current server rejects
    ///         every key.
    #[new]
    #[pyo3(signature = (*, raw=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, builtins.str] | None"))]
        raw: Option<BTreeMap<String, String>>,
    ) -> Self {
        Self {
            inner: UserCreateOptions {
                raw: raw.unwrap_or_default(),
            },
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl OptionKey {
    fn __str__(&self) -> &'static str {
        RustOptionKey::from(*self).into()
    }
}

/// Fingerprint of a stream encryption key: the first 8 bytes of SHA-256 over
/// the label `iggy/stream-key-fingerprint/v1` followed by the key, as 16
/// lowercase hex characters. Streams report the same value in
/// `encryption_key_fingerprint`, so a caller can match a key to its stream
/// without the server ever returning the key.
///
/// Args:
///     key: Exactly 32 bytes, or their canonical standard padded base64 text.
///
/// Raises:
///     ValueError: If `key` is not exactly 32 bytes or their canonical base64
///         text. The message never contains the key.
///     TypeError: If `key` is neither `bytes` nor `str`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encryption_key_fingerprint(
    #[gen_stub(override_type(type_repr = "builtins.bytes | builtins.str"))] key: PyStreamKey<'_>,
) -> PyResult<String> {
    StreamEncryptionKey::try_from(key).map(|key| key.fingerprint())
}

/// The durations of a geo topic, in catalog order, as `TopicOptions` and
/// `update_topic` take them. A `None` stays unset and zero is a value.
pub(crate) fn geo_topic_durations(
    durations: [Option<Py<PyDelta>>; 6],
) -> PyResult<GeoTopicDurations> {
    let mut parsed = GeoTopicDurations::default();
    for (key, delta) in GeoTopicDurations::KEYS.into_iter().zip(durations) {
        let Some(delta) = delta else {
            continue;
        };
        let duration = py_delta_to_iggy_duration(&delta)?;
        // The wire carries 64-bit microseconds, and a longer timedelta would
        // wrap into a short duration.
        if duration.get_duration().as_micros() > u128::from(u64::MAX) {
            let name: &str = key.into();
            return Err(PyValueError::new_err(format!(
                "'{name}' must be at most 2**64 - 1 microseconds"
            )));
        }
        parsed.set(key, duration);
    }
    Ok(parsed)
}

impl From<OptionKey> for RustOptionKey {
    fn from(key: OptionKey) -> Self {
        match key {
            OptionKey::Name => Self::Name,
            OptionKey::Username => Self::Username,
            OptionKey::Status => Self::Status,
            OptionKey::CompressionAlgorithm => Self::CompressionAlgorithm,
            OptionKey::MessageExpiry => Self::MessageExpiry,
            OptionKey::MaxTopicSize => Self::MaxTopicSize,
            OptionKey::SegmentSize => Self::SegmentSize,
            OptionKey::Durability => Self::Durability,
            OptionKey::ConsumerOffsetDurability => Self::ConsumerOffsetDurability,
            OptionKey::PartitionResizePolicy => Self::PartitionResizePolicy,
            OptionKey::MessagesRequiredToSave => Self::MessagesRequiredToSave,
            OptionKey::SizeOfMessagesRequiredToSave => Self::SizeOfMessagesRequiredToSave,
            OptionKey::PreallocateSegments => Self::PreallocateSegments,
            OptionKey::ReplicationFactor => Self::ReplicationFactor,
            OptionKey::EncryptionKey => Self::EncryptionKey,
            OptionKey::EncryptionKeySource => Self::EncryptionKeySource,
            OptionKey::EncryptionKeyFingerprint => Self::EncryptionKeyFingerprint,
            OptionKey::EncryptionKekFingerprint => Self::EncryptionKekFingerprint,
            OptionKey::SegmentMaxAge => Self::SegmentMaxAge,
            OptionKey::TieredStorage => Self::TieredStorage,
            OptionKey::LocalRetentionAge => Self::LocalRetentionAge,
            OptionKey::LocalRetentionSize => Self::LocalRetentionSize,
            OptionKey::TopicType => Self::TopicType,
            OptionKey::GeoMode => Self::GeoMode,
            OptionKey::FreshUploadInterval => Self::FreshUploadInterval,
            OptionKey::ConsumerOffsetUploadInterval => Self::ConsumerOffsetUploadInterval,
            OptionKey::PositionsPublishInterval => Self::PositionsPublishInterval,
            OptionKey::TombstoneGrace => Self::TombstoneGrace,
            OptionKey::PositionsResumeTimeout => Self::PositionsResumeTimeout,
            OptionKey::LongTermRewriteMaxInterval => Self::LongTermRewriteMaxInterval,
            OptionKey::ShadowOrigin => Self::ShadowOrigin,
        }
    }
}

impl From<PartitionResizePolicy> for RustPartitionResizePolicy {
    fn from(policy: PartitionResizePolicy) -> Self {
        match policy {
            PartitionResizePolicy::Mutable => Self::Mutable,
            PartitionResizePolicy::Fixed => Self::Fixed,
        }
    }
}

impl From<TopicType> for RustTopicType {
    fn from(topic_type: TopicType) -> Self {
        match topic_type {
            TopicType::Normal => Self::Normal,
            TopicType::Geo => Self::Geo,
        }
    }
}

impl From<GeoMode> for RustGeoMode {
    fn from(mode: GeoMode) -> Self {
        match mode {
            GeoMode::Geo => Self::Geo,
            GeoMode::GeoTiered => Self::GeoTiered,
        }
    }
}

impl From<RustTopicType> for TopicType {
    fn from(topic_type: RustTopicType) -> Self {
        match topic_type {
            RustTopicType::Normal => Self::Normal,
            RustTopicType::Geo => Self::Geo,
        }
    }
}

impl From<RustGeoMode> for GeoMode {
    fn from(mode: RustGeoMode) -> Self {
        match mode {
            RustGeoMode::Geo => Self::Geo,
            RustGeoMode::GeoTiered => Self::GeoTiered,
        }
    }
}

impl TryFrom<PyStreamKey<'_>> for StreamEncryptionKey {
    type Error = PyErr;

    fn try_from(key: PyStreamKey<'_>) -> PyResult<Self> {
        match key {
            PyStreamKey::Raw(bytes) => Self::try_from(bytes.as_bytes()),
            PyStreamKey::Base64(text) => Self::from_str(text.to_str()?),
        }
        .map_err(|error: IggyError| PyValueError::new_err(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::OptionSpec;
    use crate::stream::StreamDetails;
    use crate::topic::TopicDetails;
    use crate::user::UserInfoDetails;
    use iggy::prelude::{
        HeaderKey, Identifier, StreamDetails as RustStreamDetails,
        TopicDetails as RustTopicDetails, TopicUpdateOptions,
        UserInfoDetails as RustUserInfoDetails,
    };
    use iggy_binary_protocol::requests::system::{ResourceTarget, UpdateResourceRequest};
    use iggy_binary_protocol::requests::topics::CreateTopicRequest;
    use iggy_binary_protocol::responses::streams::GetStreamResponse;
    use iggy_binary_protocol::responses::system::DescribeOptionsResponse;
    use iggy_binary_protocol::responses::topics::GetTopicResponse;
    use iggy_binary_protocol::responses::users::UserDetailsResponse;
    use iggy_binary_protocol::{WireDecode, WireEncode, WireIdentifier, WireName};
    use iggy_common::OptionInputMap;
    use iggy_common::create_topic::CreateTopic;
    use iggy_common::wire_conversions::{
        identifier_to_wire, option_specs_from_wire, resource_options_from_wire,
    };
    use pyo3::types::{PyDict, PyModule};
    use serde_json::{Value, json};
    use std::time::Duration;

    const FIXTURE: &str = include_str!("../tests/fixtures/resource_options.json");

    #[test]
    fn given_stream_response_vectors_when_reading_should_match_projections() {
        Python::initialize();
        Python::attach(|py| {
            for vector in vectors("stream_responses") {
                let bytes = decode_hex(&vector["response_hex"]);
                let (response, _) = GetStreamResponse::decode(&bytes).expect("stream decodes");
                let details = RustStreamDetails::try_from(response).expect("stream converts");
                let stream = Bound::new(py, StreamDetails::from(details)).expect("stream wraps");
                let expected = &vector["expected"];

                assert_eq!(
                    attribute::<Option<String>>(stream.as_any(), "encryption_key_source"),
                    expected["encryption_key_source"]
                        .as_str()
                        .map(str::to_owned)
                );
                assert_eq!(
                    attribute::<Option<String>>(stream.as_any(), "encryption_key_fingerprint"),
                    expected["encryption_key_fingerprint"]
                        .as_str()
                        .map(str::to_owned)
                );
                assert_eq!(
                    attribute::<Vec<String>>(stream.as_any(), "encryption_kek_fingerprints"),
                    serde_json::from_value::<Vec<String>>(
                        expected["encryption_kek_fingerprints"].clone()
                    )
                    .expect("fingerprint list")
                );
                // Only a shadow stream's vector carries the key.
                assert_eq!(
                    attribute::<Option<String>>(stream.as_any(), "shadow_origin"),
                    expected["shadow_origin"].as_str().map(str::to_owned)
                );
                assert_options(stream.as_any(), &expected["options"]);
            }
        });
    }

    #[test]
    fn given_topic_and_user_response_vectors_when_reading_should_match_projections() {
        Python::initialize();
        Python::attach(|py| {
            for vector in vectors("topic_responses") {
                let bytes = decode_hex(&vector["response_hex"]);
                let (response, _) = GetTopicResponse::decode(&bytes).expect("topic decodes");
                let details = RustTopicDetails::try_from(response).expect("topic converts");
                let topic = Bound::new(py, TopicDetails::from(details)).expect("topic wraps");
                let expected = &vector["expected"];

                assert_eq!(
                    attribute::<String>(topic.as_any(), "compression_algorithm"),
                    expected["compression_algorithm"]
                        .as_str()
                        .expect("compression")
                );
                let message_expiry = topic.getattr("message_expiry").expect("expiry");
                let expiry = expected["message_expiry"].as_str().expect("expiry");
                if expiry == u64::MAX.to_string() {
                    assert!(
                        message_expiry.getattr("duration").is_err(),
                        "a topic that never expires carries no duration"
                    );
                } else {
                    let duration = message_expiry
                        .getattr("duration")
                        .expect("custom expiry")
                        .extract::<Duration>()
                        .expect("duration");
                    assert_eq!(duration.as_micros().to_string(), expiry);
                }
                let max_topic_size = topic.getattr("max_topic_size").expect("max size");
                assert!(max_topic_size.is_instance_of::<crate::topic::MaxTopicSize>());
                assert_eq!(
                    expected["max_topic_size"].as_str(),
                    Some(u64::MAX.to_string().as_str())
                );
                assert!(
                    max_topic_size.getattr("bytes").is_err(),
                    "the unlimited cap carries no byte count"
                );
                let options = &expected["options"];
                let topic_type: RustTopicType = options["topic_type"]["value"]
                    .as_str()
                    .expect("topic type")
                    .parse()
                    .expect("known topic type");
                assert_eq!(
                    attribute::<TopicType>(topic.as_any(), "topic_type"),
                    TopicType::from(topic_type)
                );
                // Only a geo topic resolves a mode.
                let geo_mode = options["geo_mode"]["value"]
                    .as_str()
                    .map(|mode| mode.parse::<RustGeoMode>().expect("known geo mode"));
                assert_eq!(
                    attribute::<Option<GeoMode>>(topic.as_any(), "geo_mode"),
                    geo_mode.map(GeoMode::from)
                );
                // The projection leaves out a duration the topic does not set.
                for key in GeoTopicDurations::KEYS {
                    let key: &str = key.into();
                    assert_eq!(
                        attribute::<Option<Duration>>(topic.as_any(), key)
                            .map(|duration| duration.as_micros().to_string()),
                        expected[key].as_str().map(str::to_owned),
                        "{key}"
                    );
                }
                assert_options(topic.as_any(), options);
            }

            for vector in vectors("user_responses") {
                let bytes = decode_hex(&vector["response_hex"]);
                let (response, _) = UserDetailsResponse::decode(&bytes).expect("user decodes");
                let details = RustUserInfoDetails::try_from(response).expect("user converts");
                let user = Bound::new(py, UserInfoDetails::from(details)).expect("user wraps");
                let expected = &vector["expected"];

                assert_eq!(
                    attribute::<String>(user.as_any(), "username"),
                    expected["username"].as_str().expect("username")
                );
                assert_options(user.as_any(), &expected["options"]);
            }
        });
    }

    #[test]
    fn given_describe_options_vectors_when_reading_should_expose_every_flag() {
        Python::initialize();
        let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
        let flags = &fixture["constants"]["option_flags"];
        Python::attach(|py| {
            for vector in vectors("describe_options") {
                let bytes = decode_hex(&vector["response_hex"]);
                let (response, _) =
                    DescribeOptionsResponse::decode(&bytes).expect("describe decodes");
                let specs = option_specs_from_wire(response).expect("specs convert");
                let entries = vector["entries"].as_array().expect("entries");
                assert_eq!(specs.len(), entries.len());

                for (spec, entry) in specs.into_iter().zip(entries) {
                    let spec = Bound::new(py, OptionSpec::from(spec)).expect("spec wraps");
                    assert_eq!(
                        attribute::<String>(spec.as_any(), "key"),
                        entry["key"].as_str().expect("key")
                    );
                    let bits = entry["flags"].as_u64().expect("flags");
                    for flag in ["updatable", "resettable", "secret", "read_only", "critical"] {
                        let mask = flags[flag].as_u64().expect("flag bit");
                        assert_eq!(
                            attribute::<bool>(spec.as_any(), flag),
                            bits & mask != 0,
                            "{flag}"
                        );
                    }
                }
            }
        });
    }

    #[test]
    fn given_create_topic_vectors_when_encoding_typed_options_should_match_wire_and_http_bodies() {
        Python::initialize();
        Python::attach(|py| {
            for vector in vectors("create_topic") {
                let case = vector["case"].as_str().expect("case");
                let input = &vector["input"];
                let options = py
                    .get_type::<TopicOptions>()
                    .call((), Some(&topic_option_kwargs(py, input)))
                    .and_then(|options| options.extract::<TopicOptions>().map_err(PyErr::from))
                    .expect("typed options build")
                    .inner;
                let stream_id = identifier(&input["stream_id"]);
                let name = input["name"].as_str().expect("name");
                let partitions_count = options.partitions_count.expect("partitions count");

                // What `create_topic` sends on the binary transports and posts
                // over HTTP.
                let request = CreateTopicRequest {
                    stream_id: identifier_to_wire(&stream_id).expect("wire identifier"),
                    partitions_count,
                    name: WireName::new(name).expect("wire name"),
                    options: options.to_wire().expect("options encode"),
                };
                assert_eq!(
                    request.to_bytes().as_ref(),
                    decode_hex(&vector["body_hex"]).as_slice(),
                    "{case}"
                );
                let http_body = serde_json::to_string(&CreateTopic {
                    stream_id,
                    partitions_count,
                    name: name.to_owned(),
                    options: OptionInputMap(options.to_input_map().expect("input map")),
                })
                .expect("HTTP body encodes");
                assert_eq!(
                    http_body,
                    vector["http_body"].as_str().expect("HTTP body"),
                    "{case}"
                );
            }
        });
    }

    #[test]
    fn given_geo_duration_update_vector_when_encoding_typed_options_should_match_wire_bytes() {
        Python::initialize();
        Python::attach(|py| {
            let vector = case("update_resource", "topic_set_geo_durations");
            let input = &vector["input"];
            assert!(
                input
                    .as_object()
                    .expect("input object")
                    .keys()
                    .all(|key| is_geo_duration(key)),
                "the vector sets only durations"
            );
            let options = TopicUpdateOptions {
                geo_durations: geo_topic_durations(typed_durations(py, input))
                    .expect("durations convert"),
                ..TopicUpdateOptions::default()
            };

            let patch = options.to_patch().expect("patch encodes");
            let target = &vector["target"];
            let request = UpdateResourceRequest {
                target: ResourceTarget::Topic {
                    stream_id: wire_identifier(&target["stream_id"]),
                    topic_id: wire_identifier(&target["topic_id"]),
                },
                reset: patch.reset,
                options: patch.set,
            };
            assert_eq!(
                request.to_bytes().as_ref(),
                decode_hex(&vector["body_hex"]).as_slice()
            );
            // HTTP sends each duration as a JSON number of microseconds.
            let body = serde_json::to_value(options.to_update_body().expect("HTTP body encodes"))
                .expect("HTTP body serializes");
            for key in GeoTopicDurations::KEYS {
                let key: &str = key.into();
                assert_eq!(body[key], json!(decimal(&input[key])), "{key}");
            }
        });
    }

    #[test]
    fn given_zero_and_unset_durations_when_encoding_should_send_zero_and_omit_unset() {
        Python::initialize();
        Python::attach(|py| {
            let zero_keys = [
                RustOptionKey::ConsumerOffsetUploadInterval,
                RustOptionKey::PositionsResumeTimeout,
            ];
            let kwargs = PyDict::new(py);
            for key in zero_keys {
                kwargs
                    .set_item(<&str>::from(key), Duration::ZERO)
                    .expect("argument");
            }
            let created = py
                .get_type::<TopicOptions>()
                .call((), Some(&kwargs))
                .and_then(|options| options.extract::<TopicOptions>().map_err(PyErr::from))
                .expect("typed options build")
                .inner
                .to_wire()
                .expect("create options encode");
            let updated = TopicUpdateOptions {
                geo_durations: geo_topic_durations(GeoTopicDurations::KEYS.map(|key| {
                    zero_keys
                        .contains(&key)
                        .then(|| Duration::ZERO.into_pyobject(py).expect("delta").unbind())
                }))
                .expect("durations convert"),
                ..TopicUpdateOptions::default()
            }
            .to_patch()
            .expect("patch encodes")
            .set;

            for wire in [created, updated] {
                let options = resource_options_from_wire(&wire, true).expect("options decode");
                for key in GeoTopicDurations::KEYS {
                    let sent = options
                        .get(&HeaderKey::from(key))
                        .map(|option| option.value.as_uint64().expect("uint64"));
                    let expected = zero_keys.contains(&key).then_some(0);
                    assert_eq!(sent, expected, "{key}");
                }
            }
        });
    }

    #[test]
    fn given_negative_or_too_long_duration_when_converting_should_refuse_before_encoding() {
        Python::initialize();
        Python::attach(|py| {
            let negative = py
                .eval(
                    c"__import__('datetime').timedelta(microseconds=-1)",
                    None,
                    None,
                )
                .expect("negative timedelta");
            let too_long = py
                .eval(
                    c"__import__('datetime').timedelta(microseconds=2**64)",
                    None,
                    None,
                )
                .expect("long timedelta");
            for (position, key) in GeoTopicDurations::KEYS.into_iter().enumerate() {
                let key: &str = key.into();
                for (delta, message) in [
                    (&negative, "must not be negative"),
                    (&too_long, "must be at most 2**64 - 1 microseconds"),
                ] {
                    let mut durations: [Option<Py<PyDelta>>; 6] = Default::default();
                    durations[position] =
                        Some(delta.cast::<PyDelta>().expect("delta").clone().unbind());
                    let refusal = geo_topic_durations(durations).expect_err("refused");
                    assert!(refusal.is_instance_of::<PyValueError>(py), "{key}");
                    assert!(refusal.to_string().contains(message), "{key}: {refusal}");
                }
            }
        });
    }

    fn vectors(section: &str) -> Vec<Value> {
        let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
        fixture[section].as_array().expect("vector list").clone()
    }

    fn case(section: &str, name: &str) -> Value {
        vectors(section)
            .into_iter()
            .find(|vector| vector["case"] == name)
            .expect("vector exists")
    }

    /// The keyword arguments that build `TopicOptions` for a create vector,
    /// each in the type a Python caller passes.
    fn topic_option_kwargs<'py>(py: Python<'py>, input: &Value) -> Bound<'py, PyDict> {
        let kwargs = PyDict::new(py);
        for (key, value) in input.as_object().expect("input object") {
            let argument = match key.as_str() {
                "stream_id" | "name" => continue,
                "partitions_count" => value
                    .as_u64()
                    .expect("count")
                    .into_pyobject(py)
                    .expect("int")
                    .into_any(),
                "compression_algorithm" => {
                    PyString::new(py, value.as_str().expect("algorithm")).into_any()
                }
                "message_expiry" => IggyExpiry::ExpireDuration {
                    duration: delta(py, value).unbind(),
                }
                .into_pyobject(py)
                .expect("expiry")
                .into_any(),
                "max_topic_size" => MaxTopicSize::Custom {
                    bytes: decimal(value),
                }
                .into_pyobject(py)
                .expect("size")
                .into_any(),
                "segment_size" => decimal(value).into_pyobject(py).expect("int").into_any(),
                "durability" | "consumer_offset_durability" => {
                    durability(py, value.as_str().expect("durability"))
                }
                "topic_type" => TopicType::from(
                    value
                        .as_str()
                        .expect("topic type")
                        .parse::<RustTopicType>()
                        .expect("known topic type"),
                )
                .into_pyobject(py)
                .expect("topic type")
                .into_any(),
                duration if is_geo_duration(duration) => delta(py, value).into_any(),
                other => panic!("no typed argument for {other}"),
            };
            kwargs.set_item(key, argument).expect("argument");
        }
        kwargs
    }

    /// The durations a vector input sets, as `update_topic` takes them.
    fn typed_durations(py: Python<'_>, input: &Value) -> [Option<Py<PyDelta>>; 6] {
        GeoTopicDurations::KEYS.map(|key| {
            let value = &input[<&str>::from(key)];
            (!value.is_null()).then(|| delta(py, value).unbind())
        })
    }

    fn is_geo_duration(key: &str) -> bool {
        GeoTopicDurations::KEYS
            .into_iter()
            .any(|duration| <&str>::from(duration) == key)
    }

    /// `Durability` checks its argument against the class in the
    /// `apache_iggy` module, which a test process never imports, so a module
    /// holding the class is registered first.
    fn durability<'py>(py: Python<'py>, value: &str) -> Bound<'py, PyAny> {
        let module = PyModule::new(py, "apache_iggy").expect("module");
        Durability::register(py, &module).expect("durability registers");
        py.import("sys")
            .and_then(|sys| sys.getattr("modules"))
            .and_then(|modules| modules.call_method1("setdefault", ("apache_iggy", module)))
            .and_then(|module| module.getattr("Durability"))
            .and_then(|class| class.call1((value,)))
            .expect("durability member")
    }

    fn delta<'py>(py: Python<'py>, micros: &Value) -> Bound<'py, PyDelta> {
        Duration::from_micros(decimal(micros))
            .into_pyobject(py)
            .expect("timedelta")
    }

    fn decimal(value: &Value) -> u64 {
        value
            .as_str()
            .expect("decimal text")
            .parse()
            .expect("decimal u64")
    }

    fn identifier(value: &Value) -> Identifier {
        match value {
            Value::Number(id) => {
                Identifier::numeric(u32::try_from(id.as_u64().expect("id")).expect("u32 id"))
            }
            other => Identifier::named(other.as_str().expect("name")),
        }
        .expect("identifier")
    }

    fn wire_identifier(value: &Value) -> WireIdentifier {
        identifier_to_wire(&identifier(value)).expect("wire identifier")
    }

    fn attribute<'py, T: FromPyObjectOwned<'py>>(object: &Bound<'py, PyAny>, name: &str) -> T {
        object
            .getattr(name)
            .expect("attribute exists")
            .extract::<T>()
            .map_err(Into::<PyErr>::into)
            .expect("attribute extracts")
    }

    /// Compare both provenance getters with the fixture's option map.
    fn assert_options(object: &Bound<'_, PyAny>, expected: &Value) {
        let py = object.py();
        let expected = expected.as_object().expect("option map");
        for (getter, explicit) in [("options", true), ("derived_options", false)] {
            let actual = object
                .getattr(getter)
                .and_then(|headers| headers.call_method0("to_scalar_dict"))
                .expect("scalar options");
            let actual = actual.cast::<PyDict>().expect("dict");
            let wanted = PyDict::new(py);
            for (key, option) in expected
                .iter()
                .filter(|(_, option)| option["explicit"].as_bool() == Some(explicit))
            {
                let text = option["value"].as_str().expect("value text");
                let value: Bound<'_, PyAny> = match option["kind"].as_str() {
                    Some("uint64") => text
                        .parse::<u64>()
                        .expect("u64")
                        .into_pyobject(py)
                        .expect("int")
                        .into_any(),
                    Some("uint32") => text
                        .parse::<u32>()
                        .expect("u32")
                        .into_pyobject(py)
                        .expect("int")
                        .into_any(),
                    Some("bool") => text
                        .parse::<bool>()
                        .expect("bool")
                        .into_pyobject(py)
                        .expect("bool")
                        .to_owned()
                        .into_any(),
                    _ => PyString::new(py, text).into_any(),
                };
                wanted.set_item(key, value).expect("set item");
            }
            assert!(
                actual.eq(&wanted).expect("comparable"),
                "{getter}: {actual} != {wanted}"
            );
        }
    }

    fn decode_hex(text: &Value) -> Vec<u8> {
        let text = text.as_str().expect("hex text");
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex byte"))
            .collect()
    }
}
