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

//! The recovery of the geo topics of a cluster that lost every disk, by the
//! cluster rebuilt on empty disks in its place, from what its store holds.
//!
//! An operator starts it on purpose: `[geo] recover_cluster_uid` names the
//! lost cluster, the retired one. Without it a cluster refuses a store that
//! another cluster claimed, as before. With it, replica 0 recovers when its
//! store identity check finds the root claimed by exactly that cluster and
//! it is the caught-up metadata primary, and then:
//!
//! 1. Reads the retired cluster's live geo topic objects, its manifests and
//!    the indexes of its self-describing fresh objects, and plans each
//!    partition, see [`partitions::plan_partition`]. Nothing is written
//!    before every partition is planned, and a topic object under the root
//!    that does not decode stops the recovery.
//! 2. Stores each plan, the report of what it took and dropped, and the
//!    [`RecoveryRecord`] that retires the cluster with a cutover per
//!    partition.
//! 3. Restores the streams and geo topics into metadata, at their ids and
//!    revision, through the `ShadowTopic` operation.
//! 4. Replaces the store identity object with this cluster's, the fence:
//!    the nodes of the retired cluster, should one come back, stop their
//!    uploads at their next check, and every node of this one passes its
//!    check and gets its uploader.
//!
//! Each node then builds the restored partitions, and installs the plan of
//! each one before it serves it, see the partition reconciler.
//!
//! A step that fails stops the recovery, and the next check runs it again
//! from the start. Every step finds what an earlier attempt wrote and goes
//! on: a plan is the same plan of the same store, the record names this
//! cluster, and a restore commits as a no-op.

use std::collections::BTreeMap;

use configs::geo::GeoConfig;
use consensus::MetadataHandle;
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, CreateTopicWithAssignmentsRequest,
};
use iggy_binary_protocol::{WireIdentifier, WireName, WireOptions};
use iggy_common::{
    CompressionAlgorithm, GeoMode, GeoTopicDurations, IggyByteSize, IggyDuration, IggyExpiry,
    IggyTimestamp, MaxTopicSize, TopicCreateOptions, TopicReadCache, TopicType,
};
use iggy_object_store::{
    KeyLayout, ManifestError, ObjectPrefix, ObjectStore, ObjectStoreError, PartitionManifest,
    PutBody, PutMode, PutOptions, RecoveredCutover, RecoveryRead, RecoveryRecord, StoreIdentity,
    TopicCompression, TopicGeoMode, TopicKey, TopicObject, TopicObjectError, TopicRead, TopicState,
    UserMetadata,
};
use metadata::impls::metadata::{MetadataSubmitError, StreamsFrontend};
use metadata::stm::shadow::{ShadowStream, ShadowTopicRequest, ShadowTopicTarget};
use partitions::{
    DropReason, FreshScan, PartitionRecovery, RecoveryPlanError, SkipReason, list_topic_keys,
    plan_partition, scan_fresh_objects,
};
use serde::Serialize;
use tracing::{info, warn};

use crate::shell::ServerShard;

/// What `[geo]` asks of a recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginRecovery {
    /// The `cluster_uid` of the lost cluster.
    retired_cluster_uid: u128,
    /// Whether to take the batches no proof names.
    adopt_unproven: bool,
    /// `[geo] origin`, which the retired cluster's topic objects must name.
    origin: String,
}

#[derive(Debug, thiserror::Error)]
pub enum OriginRecoveryError {
    #[error("the store failed: {0}")]
    Store(#[from] ObjectStoreError),
    #[error(
        "the geo topic object {key:?} names origin {found:?}, and this cluster is {expected:?}"
    )]
    OtherOrigin {
        key: TopicKey,
        expected: String,
        found: String,
    },
    #[error("the geo topic object {0:?} writes in a mode this build does not recover")]
    GeoMode(TopicKey),
    #[error(
        "the geo topic object {key:?}, whose format marker is \"{marker}\", does not decode: \
         {error}. The recovery takes the root only once it reads every topic object under it"
    )]
    UndecodableTopic {
        key: TopicKey,
        /// The first bytes of the object, escaped, empty when unread.
        marker: String,
        error: TopicObjectError,
    },
    #[error("the manifest of {prefix:?} does not decode: {error}")]
    Manifest {
        prefix: ObjectPrefix,
        error: ManifestError,
    },
    #[error("the partition {prefix:?} cannot be planned: {error}")]
    Plan {
        prefix: ObjectPrefix,
        error: RecoveryPlanError,
    },
    #[error(
        "metadata holds stream {0}, which the retired cluster's topics do not name: a recovery \
         runs into a cluster whose disks are empty"
    )]
    NotEmpty(usize),
    #[error("cluster {0:032x} already recovered the retired cluster")]
    RecoveredByAnother(u128),
    #[error("the recovery record in the store does not decode")]
    InvalidRecord,
    #[error("a restore did not commit: {0:?}")]
    Submit(MetadataSubmitError),
    #[error("metadata refused `{proposal}` with code {code}")]
    Refused { proposal: String, code: u32 },
    #[error("the geo topic {0:?} has ids or a name that a stream or topic cannot take")]
    Unrestorable(TopicKey),
}

/// The machine-readable summary of one recovery, stored next to its record.
#[derive(Debug, Clone, Serialize)]
pub struct RecoveryReport {
    pub retired_cluster_uid: String,
    pub successor_cluster_uid: String,
    pub recovered_at: u64,
    pub adopt_unproven: bool,
    /// The fresh objects the recovery did not read, and why.
    pub skipped_objects: Vec<SkippedReport>,
    pub partitions: Vec<PartitionReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SkippedReport {
    pub object_id: String,
    pub reason: String,
}

/// F, P and Q of one partition, each the last offset it covers.
#[derive(Debug, Clone, Serialize)]
pub struct PartitionReport {
    pub stream_id: u64,
    pub topic_id: u64,
    pub partition_id: u64,
    pub created_revision: u64,
    /// F: the last offset of the retired cluster's manifest.
    pub floor: Option<u64>,
    /// P: the last offset that proofs name, without a gap from F.
    pub proven_through: Option<u64>,
    /// Q: the last offset taken, the cutover.
    pub cutover_through: Option<u64>,
    pub proven_batches: usize,
    pub adopted_batches: usize,
    /// The first offset two different proofs bind, when there is one.
    pub conflict_at: Option<u64>,
    pub dropped: Vec<DroppedReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DroppedReport {
    pub object_id: String,
    pub position: u64,
    pub first_offset: u64,
    pub last_offset: u64,
    pub reason: &'static str,
}

impl OriginRecovery {
    /// The recovery `geo` asks for, `None` when it names no cluster.
    #[must_use]
    pub fn from_config(geo: &GeoConfig) -> Option<Self> {
        Some(Self {
            retired_cluster_uid: geo.recovered_cluster_uid()?,
            adopt_unproven: geo.recover_unproven,
            origin: geo.origin.clone(),
        })
    }

    #[must_use]
    pub const fn retired_cluster_uid(&self) -> u128 {
        self.retired_cluster_uid
    }

    /// Recover the retired cluster's geo topics into this cluster,
    /// `successor_cluster_uid`, from `store`: see the module docs. Returns
    /// what it took and dropped.
    ///
    /// # Errors
    /// See [`OriginRecoveryError`]. Nothing is written before every
    /// partition is planned.
    pub async fn run<S: ObjectStore>(
        &self,
        store: &S,
        layout: &KeyLayout,
        shard: &ServerShard,
        successor_cluster_uid: u128,
        now: IggyTimestamp,
    ) -> Result<RecoveryReport, OriginRecoveryError> {
        let retired = self.retired_cluster_uid;
        let topics = self.live_topics(store, layout).await?;
        let streams = streams_of(&topics);
        check_metadata_holds_only(shard, &streams)?;
        let scan = scan_fresh_objects(store, layout, retired).await?;
        let mut recoveries = Vec::new();
        for topic in &topics {
            for partition_id in 0..u64::from(topic.partition_count) {
                let prefix = topic.key.partition_prefix(partition_id);
                let manifest = read_manifest(store, layout, &prefix).await?;
                let recovery =
                    plan_partition(retired, prefix, manifest, &scan, self.adopt_unproven)
                        .map_err(|error| OriginRecoveryError::Plan { prefix, error })?;
                recoveries.push(recovery);
            }
        }
        for recovery in &recoveries {
            if recovery.plan.holds_anything() {
                recovery.plan.store(store, layout).await?;
            }
        }
        let report = self.report(successor_cluster_uid, now, &scan, &recoveries);
        store_report(store, layout, retired, &report).await?;
        self.fence_record(store, layout, successor_cluster_uid, now, &recoveries)
            .await?;
        restore_metadata(shard, retired, &self.origin, &streams, &topics).await?;
        StoreIdentity {
            cluster_uid: successor_cluster_uid,
        }
        .replace(store, layout)
        .await?;
        log_report(&report, &layout.recovery_report_key(retired));
        Ok(report)
    }

    /// The retired cluster's live geo topics, in the order of their
    /// revisions.
    async fn live_topics<S: ObjectStore>(
        &self,
        store: &S,
        layout: &KeyLayout,
    ) -> Result<Vec<RetiredTopic>, OriginRecoveryError> {
        let mut topics = Vec::new();
        for topic in read_retired_topics(store, layout, self.retired_cluster_uid).await? {
            if topic.deleted_at.is_some() {
                continue;
            }
            if topic.origin != self.origin {
                return Err(OriginRecoveryError::OtherOrigin {
                    key: topic.key,
                    expected: self.origin.clone(),
                    found: topic.origin,
                });
            }
            if topic.geo_mode != TopicGeoMode::Geo {
                return Err(OriginRecoveryError::GeoMode(topic.key));
            }
            topics.push(topic);
        }
        topics.sort_by_key(|topic| {
            (
                topic.key.created_revision,
                topic.key.stream_id,
                topic.key.topic_id,
            )
        });
        Ok(topics)
    }

    /// Store the record that retires the cluster, or find the one an
    /// earlier attempt of this cluster stored.
    async fn fence_record<S: ObjectStore>(
        &self,
        store: &S,
        layout: &KeyLayout,
        successor_cluster_uid: u128,
        now: IggyTimestamp,
        recoveries: &[PartitionRecovery],
    ) -> Result<(), OriginRecoveryError> {
        let mut partitions: Vec<RecoveredCutover> = recoveries
            .iter()
            .map(|recovery| RecoveredCutover {
                prefix: recovery.plan.prefix,
                published_end: recovery.plan.published_end(),
                proven_end: recovery.plan.proven_end,
                cutover: recovery.plan.cutover(),
            })
            .collect();
        partitions.sort_by_key(|cutover| cutover.prefix);
        let record = RecoveryRecord {
            retired_cluster_uid: self.retired_cluster_uid,
            successor_cluster_uid,
            recovered_at: now.as_micros(),
            adopted_unproven: self.adopt_unproven,
            partitions,
        };
        if record.create(store, layout).await? {
            return Ok(());
        }
        match RecoveryRecord::read(store, layout, self.retired_cluster_uid).await? {
            RecoveryRead::Found(found) if found.successor_cluster_uid == successor_cluster_uid => {
                Ok(())
            }
            RecoveryRead::Found(found) => Err(OriginRecoveryError::RecoveredByAnother(
                found.successor_cluster_uid,
            )),
            RecoveryRead::Missing | RecoveryRead::Invalid(_) => {
                Err(OriginRecoveryError::InvalidRecord)
            }
        }
    }

    fn report(
        &self,
        successor_cluster_uid: u128,
        now: IggyTimestamp,
        scan: &FreshScan,
        recoveries: &[PartitionRecovery],
    ) -> RecoveryReport {
        RecoveryReport {
            retired_cluster_uid: format!("{:032x}", self.retired_cluster_uid),
            successor_cluster_uid: format!("{successor_cluster_uid:032x}"),
            recovered_at: now.as_micros(),
            adopt_unproven: self.adopt_unproven,
            skipped_objects: scan
                .skipped()
                .iter()
                .map(|skipped| SkippedReport {
                    object_id: format!("{:032x}", skipped.object_id.get()),
                    reason: skip_reason(&skipped.reason),
                })
                .collect(),
            partitions: recoveries.iter().map(PartitionReport::of).collect(),
        }
    }
}

impl PartitionReport {
    fn of(recovery: &PartitionRecovery) -> Self {
        let plan = &recovery.plan;
        let last = |end: u64| end.checked_sub(1);
        Self {
            stream_id: plan.prefix.stream_id,
            topic_id: plan.prefix.topic_id,
            partition_id: plan.prefix.partition_id,
            created_revision: plan.prefix.created_revision,
            floor: last(plan.published_end()),
            proven_through: last(plan.proven_end),
            cutover_through: last(plan.cutover()),
            proven_batches: recovery.proven_batches(),
            adopted_batches: recovery.adopted_batches(),
            conflict_at: recovery.conflict_at,
            dropped: recovery
                .dropped
                .iter()
                .map(|dropped| DroppedReport {
                    object_id: format!("{:032x}", dropped.object_id.get()),
                    position: dropped.position,
                    first_offset: dropped.offsets.start,
                    last_offset: dropped.offsets.end.saturating_sub(1),
                    reason: drop_reason(dropped.reason),
                })
                .collect(),
        }
    }
}

const fn drop_reason(reason: DropReason) -> &'static str {
    match reason {
        DropReason::ProvenPastGap => "proven_past_gap",
        DropReason::InvalidProof => "invalid_proof",
        DropReason::OverlapsProven => "overlaps_proven",
        DropReason::LostOverlap => "lost_overlap",
        DropReason::PastGap => "past_gap",
        DropReason::AdoptionOff => "adoption_off",
        DropReason::ProofsConflict => "proofs_conflict",
        DropReason::InvalidProducer => "invalid_producer",
    }
}

fn skip_reason(reason: &SkipReason) -> String {
    match reason {
        SkipReason::NotSelfDescribing => "not_self_describing".to_owned(),
        SkipReason::Unreadable(error) => format!("unreadable: {error}"),
        SkipReason::OtherCluster(cluster_uid) => format!("other_cluster: {cluster_uid:032x}"),
        SkipReason::Gone => "gone".to_owned(),
    }
}

/// One geo topic of the retired cluster, as the recovery restores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredTopic {
    /// The stream and topic ids and the revision of the topic's partitions.
    pub key: TopicKey,
    pub stream_name: String,
    pub topic_name: String,
    pub partition_count: u32,
    /// The `[geo] origin` of the retired cluster.
    pub origin: String,
    pub geo_mode: TopicGeoMode,
    pub compression: TopicCompression,
    /// How long a message lives, in microseconds, `u64::MAX` for ever.
    pub message_expiry: u64,
    /// The most bytes the topic holds, `u64::MAX` without a limit.
    pub max_topic_size: u64,
    /// When the retired cluster deleted the topic, `None` while it held it.
    pub deleted_at: Option<u64>,
    /// When the retired cluster described the topic, so that the newest
    /// description names its stream.
    pub described_at: u64,
    /// The topic's own `tombstone_grace` in microseconds, zero for none.
    pub tombstone_grace: u64,
    /// The topic's own `read_cache_size` and `read_ahead` in bytes, `None`
    /// for one that it does not set.
    pub read_cache_size: Option<u64>,
    pub read_ahead: Option<u64>,
}

/// Every geo topic that the retired cluster `retired_cluster_uid` described
/// in the store under `layout`, deleted ones too. The one place that knows
/// where the descriptions are: the object of each topic under
/// [`KeyLayout::topics_prefix`]. Objects of another cluster describe nothing.
///
/// # Errors
/// [`OriginRecoveryError::UndecodableTopic`] for a listed object that does
/// not decode, which can be one of the retired cluster's topics, and the
/// store's error for a listing or a read that failed.
pub async fn read_retired_topics<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    retired_cluster_uid: u128,
) -> Result<Vec<RetiredTopic>, OriginRecoveryError> {
    let mut topics = Vec::new();
    for key in list_topic_keys(store, layout).await? {
        let object = match TopicObject::read(store, layout, &key).await? {
            TopicRead::Found(object) => object,
            TopicRead::Missing => continue,
            TopicRead::Invalid(error) => {
                return Err(OriginRecoveryError::UndecodableTopic {
                    key,
                    marker: format_marker(store, layout, &key).await,
                    error,
                });
            }
        };
        if object.cluster_uid == retired_cluster_uid {
            topics.push(RetiredTopic::from(object));
        }
    }
    Ok(topics)
}

impl From<TopicObject> for RetiredTopic {
    fn from(object: TopicObject) -> Self {
        Self {
            key: object.key,
            stream_name: object.stream_name,
            topic_name: object.topic_name,
            partition_count: object.partition_count,
            origin: object.origin,
            geo_mode: object.geo_mode,
            compression: object.compression,
            message_expiry: object.message_expiry,
            max_topic_size: object.max_topic_size,
            deleted_at: match object.state {
                TopicState::Live => None,
                TopicState::Tombstone { deleted_at } => Some(deleted_at),
            },
            described_at: object.published_at,
            tombstone_grace: object.tombstone_grace,
            read_cache_size: object.read_cache_size,
            read_ahead: object.read_ahead,
        }
    }
}

/// The length of the magic that starts a topic object and names its format.
const FORMAT_MARKER_LEN: u64 = 8;

/// The format marker of the topic object under `key`, escaped as text,
/// empty when the read fails.
async fn format_marker<S: ObjectStore>(store: &S, layout: &KeyLayout, key: &TopicKey) -> String {
    store
        .get_range(&layout.topic_key(key), 0..FORMAT_MARKER_LEN)
        .await
        .map(|range| range.bytes.escape_ascii().to_string())
        .unwrap_or_default()
}

/// Whether replica 0 recovers now: only when the operator named the retired
/// cluster, the store names exactly that cluster, and this node is the
/// claimer and the caught-up metadata primary.
#[must_use]
pub const fn may_recover(
    claimer: bool,
    retired_cluster_uid: Option<u128>,
    found: Option<u128>,
    caught_up_primary: bool,
) -> bool {
    match (retired_cluster_uid, found) {
        (Some(retired), Some(found)) => claimer && caught_up_primary && retired == found,
        _ => false,
    }
}

/// The name of each stream the topics name, by its id there, from the
/// newest object of the stream.
fn streams_of(topics: &[RetiredTopic]) -> BTreeMap<u64, String> {
    let mut newest: BTreeMap<u64, &RetiredTopic> = BTreeMap::new();
    for topic in topics {
        let entry = newest.entry(topic.key.stream_id).or_insert(topic);
        if topic.described_at > entry.described_at {
            *entry = topic;
        }
    }
    newest
        .into_iter()
        .map(|(stream_id, topic)| (stream_id, topic.stream_name.clone()))
        .collect()
}

/// Refuse a cluster whose metadata holds a stream that the recovery does not
/// restore: its ids or revisions could take the retired cluster's.
fn check_metadata_holds_only(
    shard: &ServerShard,
    streams: &BTreeMap<u64, String>,
) -> Result<(), OriginRecoveryError> {
    let foreign = shard.plane.metadata().mux_stm.streams().read(|inner| {
        inner.items.iter().find_map(|(stream_id, stream)| {
            let restored = streams
                .get(&(stream_id as u64))
                .is_some_and(|name| **name == *stream.name && stream.shadow.is_none());
            (!restored).then_some(stream_id)
        })
    });
    foreign.map_or(Ok(()), |stream_id| {
        Err(OriginRecoveryError::NotEmpty(stream_id))
    })
}

/// The manifest of `prefix`, `None` when the store holds none.
async fn read_manifest<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
) -> Result<Option<PartitionManifest>, OriginRecoveryError> {
    let key = layout.manifest_key(prefix);
    let size = match store.head(&key).await {
        Ok(head) => head.size,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let read = match store.get_range(&key, 0..size).await {
        Ok(read) => read,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    PartitionManifest::decode(&read.bytes)
        .map(Some)
        .map_err(|error| OriginRecoveryError::Manifest {
            prefix: *prefix,
            error,
        })
}

async fn store_report<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    retired_cluster_uid: u128,
    report: &RecoveryReport,
) -> Result<(), OriginRecoveryError> {
    let bytes = serde_json::to_vec_pretty(report)
        .map_err(|error| ObjectStoreError::InvalidRequest(error.to_string()))?;
    let options = PutOptions {
        mode: PutMode::Overwrite,
        metadata: UserMetadata::default(),
        crc32c: None,
        ..PutOptions::default()
    };
    store
        .put(
            &layout.recovery_report_key(retired_cluster_uid),
            PutBody::from_bytes(bytes),
            &options,
        )
        .await?;
    Ok(())
}

/// Restore every stream, then every topic in the order of its revision.
async fn restore_metadata(
    shard: &ServerShard,
    retired_cluster_uid: u128,
    origin: &str,
    streams: &BTreeMap<u64, String>,
    topics: &[RetiredTopic],
) -> Result<(), OriginRecoveryError> {
    let source = |stream_id: u64| ShadowStream {
        origin: origin.to_owned(),
        cluster_uid: retired_cluster_uid,
        stream_id,
    };
    for (stream_id, name) in streams {
        let name = WireName::new(name).map_err(|_| {
            OriginRecoveryError::Unrestorable(TopicKey {
                stream_id: *stream_id,
                topic_id: 0,
                created_revision: 0,
            })
        })?;
        submit(
            shard,
            ShadowTopicRequest::RestoreStream {
                source: source(*stream_id),
                name,
            },
        )
        .await?;
    }
    for topic in topics {
        let unrestorable = || OriginRecoveryError::Unrestorable(topic.key);
        let stream_id = u32::try_from(topic.key.stream_id).map_err(|_| unrestorable())?;
        let topic_id = u32::try_from(topic.key.topic_id).map_err(|_| unrestorable())?;
        let create = restored_topic(stream_id, topic).ok_or_else(unrestorable)?;
        submit(
            shard,
            ShadowTopicRequest::RestoreTopic {
                target: ShadowTopicTarget {
                    stream_id,
                    source: source(topic.key.stream_id),
                    topic_id,
                    created_revision: topic.key.created_revision,
                },
                create: Box::new(create),
            },
        )
        .await?;
    }
    Ok(())
}

async fn submit(
    shard: &ServerShard,
    request: ShadowTopicRequest,
) -> Result<(), OriginRecoveryError> {
    let proposal = format!("{request:?}");
    match shard
        .plane
        .metadata()
        .submit_shadow_topic_in_process(request)
        .await
    {
        Ok(0) => Ok(()),
        Ok(code) => Err(OriginRecoveryError::Refused { proposal, code }),
        Err(error) => Err(OriginRecoveryError::Submit(error)),
    }
}

/// The create of the geo topic that `object` describes, in the restored
/// stream `stream_id`, without the partition assignments that the metadata
/// primary fills in.
fn restored_topic(
    stream_id: u32,
    object: &RetiredTopic,
) -> Option<CreateTopicWithAssignmentsRequest> {
    let options = TopicCreateOptions {
        compression_algorithm: Some(match object.compression {
            TopicCompression::None => CompressionAlgorithm::None,
            TopicCompression::Gzip => CompressionAlgorithm::Gzip,
        }),
        message_expiry: Some(match object.message_expiry {
            u64::MAX => IggyExpiry::NeverExpire,
            micros => IggyExpiry::ExpireDuration(IggyDuration::from(micros)),
        }),
        max_topic_size: Some(match object.max_topic_size {
            u64::MAX => MaxTopicSize::Unlimited,
            bytes => MaxTopicSize::Custom(IggyByteSize::from(bytes)),
        }),
        topic_type: Some(TopicType::Geo),
        geo_mode: Some(GeoMode::Geo),
        // The only duration of the topic that its object records.
        geo_durations: GeoTopicDurations {
            tombstone_grace: (object.tombstone_grace != 0)
                .then(|| IggyDuration::from(object.tombstone_grace)),
            ..GeoTopicDurations::default()
        },
        read_cache: TopicReadCache {
            size: object.read_cache_size.map(IggyByteSize::from),
            read_ahead: object.read_ahead.map(IggyByteSize::from),
        },
        ..TopicCreateOptions::default()
    };
    Some(CreateTopicWithAssignmentsRequest {
        request: CreateTopicRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            partitions_count: object.partition_count,
            name: WireName::new(&object.topic_name).ok()?,
            options: options.to_wire().ok()?,
        },
        derived_options: WireOptions::empty(),
        partitions: Vec::new(),
    })
}

fn log_report(report: &RecoveryReport, key: &str) {
    for partition in &report.partitions {
        info!(
            stream_id = partition.stream_id,
            topic_id = partition.topic_id,
            partition_id = partition.partition_id,
            floor = ?partition.floor,
            proven_through = ?partition.proven_through,
            cutover_through = ?partition.cutover_through,
            proven_batches = partition.proven_batches,
            adopted_batches = partition.adopted_batches,
            dropped_batches = partition.dropped.len(),
            "recovered a geo partition of the retired cluster"
        );
        if let Some(conflict_at) = partition.conflict_at {
            warn!(
                stream_id = partition.stream_id,
                topic_id = partition.topic_id,
                partition_id = partition.partition_id,
                conflict_at,
                "two proofs of the retired cluster bind one offset to different batches; the \
                 partition is recovered below it and nothing after it is adopted"
            );
        }
    }
    info!(
        retired_cluster_uid = report.retired_cluster_uid,
        successor_cluster_uid = report.successor_cluster_uid,
        partitions = report.partitions.len(),
        skipped_objects = report.skipped_objects.len(),
        report = key,
        "the retired cluster's geo topics are recovered and this cluster owns the store; \
         the report lists what each partition took and dropped"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use iggy_object_store::MemoryStore;

    const RETIRED: u128 = 0x0a0a;

    #[test]
    fn given_each_node_state_when_deciding_to_recover_should_recover_only_what_the_operator_named()
    {
        // (row, claimer, retired cluster named, cluster the store names,
        // caught-up primary, recovers)
        for (row, claimer, retired, found, primary, recovers) in [
            (
                "no recovery asked for",
                true,
                None,
                Some(RETIRED),
                true,
                false,
            ),
            (
                "the store names another cluster",
                true,
                Some(RETIRED),
                Some(7),
                true,
                false,
            ),
            (
                "bytes no cluster wrote",
                true,
                Some(RETIRED),
                None,
                true,
                false,
            ),
            (
                "replica 1",
                false,
                Some(RETIRED),
                Some(RETIRED),
                true,
                false,
            ),
            (
                "replica 0 as backup",
                true,
                Some(RETIRED),
                Some(RETIRED),
                false,
                false,
            ),
            (
                "replica 0 primary, asked for this cluster",
                true,
                Some(RETIRED),
                Some(RETIRED),
                true,
                true,
            ),
        ] {
            assert_eq!(
                may_recover(claimer, retired, found, primary),
                recovers,
                "{row}"
            );
        }
    }

    fn object(stream_id: u64, stream_name: &str, described_at: u64) -> RetiredTopic {
        RetiredTopic {
            key: TopicKey {
                stream_id,
                topic_id: 1,
                created_revision: described_at,
            },
            stream_name: stream_name.to_owned(),
            topic_name: "payments".to_owned(),
            partition_count: 2,
            origin: "dc-a".to_owned(),
            geo_mode: TopicGeoMode::Geo,
            compression: TopicCompression::Gzip,
            message_expiry: 60_000_000,
            max_topic_size: u64::MAX,
            deleted_at: None,
            described_at,
            tombstone_grace: 0,
            read_cache_size: None,
            read_ahead: None,
        }
    }

    /// The object of topic 1 of stream `stream_id`, which `cluster_uid`
    /// describes.
    fn described(stream_id: u64, cluster_uid: u128) -> TopicObject {
        TopicObject {
            key: TopicKey {
                stream_id,
                topic_id: 1,
                created_revision: 9,
            },
            origin: "dc-a".to_owned(),
            cluster_uid,
            stream_name: format!("stream-{stream_id}"),
            topic_name: "payments".to_owned(),
            partition_count: 2,
            compression: TopicCompression::None,
            max_message_size: 1024,
            message_expiry: u64::MAX,
            max_topic_size: 4096,
            geo_mode: TopicGeoMode::Geo,
            created_at: 1,
            published_at: 2,
            state: TopicState::Live,
            tombstone_grace: 0,
            read_cache_size: Some(4 * 1024 * 1024),
            read_ahead: None,
        }
    }

    #[test]
    fn given_topic_objects_of_two_clusters_when_reading_the_retired_topics_should_describe_the_retired_ones_deleted_too()
     {
        let store = MemoryStore::new();
        let layout = KeyLayout::new("iggy").unwrap();
        for object in [
            described(1, RETIRED),
            described(2, RETIRED).into_tombstone(77),
            described(3, 7),
        ] {
            block_on(object.put(&store, &layout)).unwrap();
        }

        let topics = block_on(read_retired_topics(&store, &layout, RETIRED)).unwrap();

        let found: Vec<_> = topics
            .iter()
            .map(|topic| (topic.key.stream_id, topic.deleted_at, topic.max_topic_size))
            .collect();
        assert_eq!(found, [(1, None, 4096), (2, Some(77), 4096)]);
        assert_eq!(topics[0], RetiredTopic::from(described(1, RETIRED)));
    }

    #[test]
    fn given_a_topic_object_that_does_not_decode_when_reading_the_retired_topics_should_refuse_naming_it_and_its_format_marker()
     {
        let layout = KeyLayout::new("iggy").unwrap();
        let broken = described(2, 7);
        let encoded = broken.encode().unwrap();
        let mut older = encoded.clone();
        older[..8].copy_from_slice(b"IGGYTOP2");
        let mut damaged = encoded;
        *damaged.last_mut().unwrap() ^= 1;
        for (row, bytes, marker) in [
            ("an older format", older, "IGGYTOP2"),
            ("damaged bytes", damaged, "IGGYTOP3"),
            ("no topic object", b"garbage".to_vec(), "garbage"),
        ] {
            let store = MemoryStore::new();
            block_on(described(1, RETIRED).put(&store, &layout)).unwrap();
            let expected = TopicObject::decode(&bytes).unwrap_err();
            block_on(store.put(
                &layout.topic_key(&broken.key),
                PutBody::from_bytes(bytes),
                &PutOptions::default(),
            ))
            .unwrap();

            let Err(OriginRecoveryError::UndecodableTopic {
                key,
                marker: found,
                error,
            }) = block_on(read_retired_topics(&store, &layout, RETIRED))
            else {
                panic!("{row}: the read described the topics without the broken one");
            };

            assert_eq!(
                (key, found.as_str(), error),
                (broken.key, marker, expected),
                "{row}"
            );
        }
    }

    #[test]
    fn given_topics_of_a_renamed_stream_when_restoring_should_name_it_after_the_newest_object() {
        let streams = streams_of(&[
            object(3, "orders", 10),
            object(3, "orders-renamed", 20),
            object(5, "payments", 5),
        ]);

        assert_eq!(
            streams,
            BTreeMap::from([(3, "orders-renamed".to_owned()), (5, "payments".to_owned())])
        );
    }

    #[test]
    fn given_a_topic_object_when_restoring_should_create_a_geo_topic_with_its_options() {
        let create = restored_topic(3, &object(3, "orders", 10)).unwrap();

        assert_eq!(create.request.partitions_count, 2);
        assert_eq!(create.request.name.as_str(), "payments");
        let options = TopicCreateOptions::parse_committed(&create.request.options);
        assert_eq!(options.topic_type, Some(TopicType::Geo));
        assert_eq!(options.geo_mode, Some(GeoMode::Geo));
        assert_eq!(
            options.compression_algorithm,
            Some(CompressionAlgorithm::Gzip)
        );
        assert_eq!(
            options.message_expiry,
            Some(IggyExpiry::ExpireDuration(IggyDuration::from(60_000_000)))
        );
        assert_eq!(options.max_topic_size, Some(MaxTopicSize::Unlimited));
        assert_eq!(options.read_cache, TopicReadCache::default());
    }

    #[test]
    fn given_a_topic_object_with_a_read_cache_when_restoring_should_keep_what_the_topic_set() {
        let object = RetiredTopic {
            read_cache_size: Some(16 * 1024 * 1024),
            read_ahead: Some(0),
            ..object(3, "orders", 10)
        };

        let create = restored_topic(3, &object).unwrap();

        assert_eq!(
            TopicCreateOptions::parse_committed(&create.request.options).read_cache,
            TopicReadCache {
                size: Some(IggyByteSize::from(16 * 1024 * 1024)),
                read_ahead: Some(IggyByteSize::from(0)),
            }
        );
    }
}
