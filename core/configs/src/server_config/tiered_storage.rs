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

//! On-disk schema for tiered storage: the object store that takes the sealed
//! segments of tiered topics, and the caps and timeouts of the tier service
//! that each shard runs.
//!
//! The section is optional. Without it the backend is `none`, so existing
//! configuration files keep working, and no topic can turn tiered.
//!
//! The tuning values are provisional. They mirror the defaults of
//! `shard::tier_service::TierServiceConfig`, and measurements of uploads, cold
//! reads and deletes on a loaded shard settle both.

use super::COMPONENT;
use crate::ConfigurationError;
use configs::{ConfigEnv, DisplayFromStrOrNumber};
use iggy_common::{FRESH_UPLOAD_INTERVAL_MAX, IggyByteSize, IggyDuration, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::RangeInclusive;
use std::time::Duration;

pub use iggy_common::{READ_AHEAD_BLOCKS_MIN, READ_AHEAD_CACHE_SHARE, READ_CACHE_BLOCK_SIZE_MIN};

/// Bounds of `request_timeout` and `progress_timeout`. A healthy store can
/// take seconds to answer, and the upper bound keeps a typo from overflowing
/// a deadline.
const TIMEOUT_BOUNDS: RangeInclusive<Duration> = Duration::from_secs(1)..=Duration::from_hours(1);

/// Bounds of `retry_delay` and `max_retry_delay`.
const RETRY_DELAY_BOUNDS: RangeInclusive<Duration> =
    Duration::from_millis(1)..=Duration::from_hours(1);

/// The JSON API of Google Cloud Storage, which an empty `gcs.endpoint`
/// names.
pub const GCS_ENDPOINT: &str = "https://storage.googleapis.com";

/// The smallest `long_term_backlog_limit` other than zero: what one
/// long-term segment holds at most. A rewrite stores one segment at a time,
/// so a lower limit would slow the sends to a partition whose rewrites keep
/// up.
pub const LONG_TERM_BACKLOG_LIMIT_MIN: u64 = 64 * 1024 * 1024;

/// The smallest `fresh_pending_bytes` and `fresh_partition_pending_bytes`:
/// the bytes of messages at which a fresh object closes. Below it, the
/// sends of one shard, or of one partition, could not fill an object, so
/// each object would wait for the upload interval.
pub const FRESH_PENDING_BYTES_MIN: u64 = 4 * 1024 * 1024;

/// The shortest `orphan_grace` other than zero. A store gives the moment of
/// an object to the second.
pub const ORPHAN_GRACE_MIN: Duration = Duration::from_secs(1);

/// The largest `read_cache_block_size`: the largest piece a poll reads from
/// the store, above which most reads would fetch more than they ask for.
/// Mirrors `partitions::DISK_POLL_CHUNK_MAX`.
pub const READ_CACHE_BLOCK_SIZE_MAX: u64 = 1024 * 1024;

/// The object store that tiered storage keeps segments in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TieredStorageBackend {
    /// No store, so no topic can turn tiered.
    #[default]
    None,
    /// A directory on a filesystem.
    Fs,
    /// S3, or a store with the same API, such as MinIO, Ceph RGW or R2.
    S3,
    /// Google Cloud Storage, over its JSON API, set up in `[tiered_storage.gcs]`.
    Gcs,
    /// The memory of the server, with the latency of `mock_s3`. Every
    /// object is lost when the server stops. For benchmarks and tests only.
    MockS3,
}

impl Display for TieredStorageBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::None => "none",
            Self::Fs => "fs",
            Self::S3 => "s3",
            Self::Gcs => "gcs",
            Self::MockS3 => "mock_s3",
        })
    }
}

/// Where the bucket goes in the request URLs of the `s3` backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum S3UrlStyle {
    /// The host names the bucket on AWS, where `endpoint` is empty, and the
    /// path names it on any other endpoint.
    #[default]
    Auto,
    /// `{endpoint}/{bucket}/{key}`, which most stores with the S3 API take.
    Path,
    /// `{bucket}.{endpoint host}/{key}`, which AWS expects.
    VirtualHosted,
}

impl Display for S3UrlStyle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Auto => "auto",
            Self::Path => "path",
            Self::VirtualHosted => "virtual_hosted",
        })
    }
}

/// How the `gcs` backend authorizes its requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GcsAuth {
    /// An access token from the first source that the node has, in the
    /// order of Google's application default credentials: the key file of
    /// `service_account_key`, then the one that
    /// `GOOGLE_APPLICATION_CREDENTIALS` names, then the metadata server of a
    /// Compute Engine instance or a GKE node.
    #[default]
    Auto,
    /// No credentials, for an emulator such as fake-gcs-server.
    None,
}

impl Display for GcsAuth {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Auto => "auto",
            Self::None => "none",
        })
    }
}

/// Where the objects of the `gcs` backend are, and how it gets its access
/// tokens. `[[geo.peers]]` entries take the same table.
#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq, Eq, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct GcsConfig {
    #[serde(default)]
    pub bucket: String,
    /// The URL of the JSON API, empty for Google Cloud Storage. An emulator
    /// takes its own URL, such as `http://localhost:4443`.
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    #[config_env(leaf)]
    pub auth: GcsAuth,
    /// The path of a service account JSON key file, which `auto` takes
    /// before any other source. The file holds a key, so only the server
    /// may read it.
    #[serde(default)]
    pub service_account_key: String,
}

impl GcsConfig {
    /// Why this section cannot reach a bucket, as a key of the section and
    /// what is wrong with it, or `None`.
    #[must_use]
    pub fn fault(&self) -> Option<&'static str> {
        if self.bucket.trim().is_empty() {
            return Some("gcs.bucket must be set for the gcs backend");
        }
        if self.bucket.contains('/') {
            return Some("gcs.bucket must be a bucket name, without '/'");
        }
        None
    }

    /// The endpoint, or Google Cloud Storage when it is empty.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        let endpoint = self.endpoint.trim();
        if endpoint.is_empty() {
            GCS_ENDPOINT
        } else {
            endpoint
        }
    }
}

impl Display for GcsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ bucket: {}, endpoint: {}, auth: {}, service_account_key: {} }}",
            self.bucket, self.endpoint, self.auth, self.service_account_key
        )
    }
}

#[serde_as]
#[derive(Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct TieredStorageConfig {
    #[serde(default)]
    #[config_env(leaf)]
    pub backend: TieredStorageBackend,
    /// The key prefix of this cluster's objects in the store. Two clusters
    /// can share a bucket under different prefixes. The cluster claims its
    /// prefix with the object `{prefix}/meta/identity`: replica 0 writes it
    /// while it is the caught-up metadata primary and no topic keeps data in
    /// the store, and a node uploads and deletes objects only after it reads
    /// that claim back.
    #[serde(default = "default_prefix")]
    pub prefix: String,
    /// The directory that holds the objects of the `fs` backend.
    #[serde(default)]
    pub path: String,
    /// The bucket of the `s3` backend.
    #[serde(default)]
    pub bucket: String,
    /// The URL of the `s3` backend, empty for AWS.
    #[serde(default)]
    pub endpoint: String,
    /// The region of the `s3` backend.
    #[serde(default)]
    pub region: String,
    /// Where the bucket goes in the request URLs of the `s3` backend.
    #[serde(default)]
    #[config_env(leaf)]
    pub url_style: S3UrlStyle,
    /// The access key ID of static `s3` credentials.
    // skip_serializing keeps credentials out of the runtime
    // `current_config.toml`, as for the cluster secrets.
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub access_key_id: String,
    /// The secret access key of static `s3` credentials.
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub secret_access_key: String,
    /// The bucket and the access tokens of the `gcs` backend.
    #[serde(default)]
    pub gcs: GcsConfig,
    #[serde(default)]
    pub jobs: TierJobsConfig,
    /// The latency of the `mock_s3` backend.
    #[serde(default)]
    pub mock_s3: MockS3Config,
    /// The longest wait for the answer to one attempt at a store call other
    /// than a put. From 1 s to 1 h.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_request_timeout")]
    #[config_env(leaf)]
    pub request_timeout: IggyDuration,
    /// The longest a put waits for the store to take the next piece of its
    /// body, or to answer after the last piece. A put has no total deadline.
    /// From 1 s to 1 h.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_progress_timeout")]
    #[config_env(leaf)]
    pub progress_timeout: IggyDuration,
    /// The most attempts at a store call other than a put that keeps failing
    /// with a transient error, the first one included.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// The wait before the first retry, doubled for each retry after it.
    /// From 1 ms to 1 h.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_retry_delay")]
    #[config_env(leaf)]
    pub retry_delay: IggyDuration,
    /// The longest wait between two attempts. From 1 ms to 1 h.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_max_retry_delay")]
    #[config_env(leaf)]
    pub max_retry_delay: IggyDuration,
    /// The bytes of what one shard reads from object stores that it keeps in
    /// memory for the next polls of the same bytes, which also hold the
    /// fresh objects it uploads. Zero keeps nothing that reads fetch and
    /// turns readahead off, and the shard still holds what it uploads.
    /// Otherwise at least [`READ_AHEAD_CACHE_SHARE`] blocks. A topic's option
    /// of the same name limits what its partitions keep of it.
    #[serde(default = "default_read_cache_size")]
    #[config_env(leaf)]
    pub read_cache_size: IggyByteSize,
    /// The bytes of one block of the read cache, the piece it fetches and
    /// keeps. From [`READ_CACHE_BLOCK_SIZE_MIN`] to
    /// [`READ_CACHE_BLOCK_SIZE_MAX`].
    #[serde(default = "default_read_cache_block_size")]
    #[config_env(leaf)]
    pub read_cache_block_size: IggyByteSize,
    /// The most bytes past a poll that the shard fetches before any poll
    /// asks for them, when polls read a segment in order. Zero turns
    /// readahead off. Otherwise at least [`READ_AHEAD_BLOCKS_MIN`] blocks,
    /// and at most one part in [`READ_AHEAD_CACHE_SHARE`] of
    /// `read_cache_size`. A topic's option of the same name overrides it for
    /// the topic.
    #[serde(default = "default_read_ahead")]
    #[config_env(leaf)]
    pub read_ahead: IggyByteSize,
    /// The shortest time between the starts of two uploads of fresh objects
    /// on one shard, unless an object fills first. A send that finds a free
    /// upload slot and the last upload started at least this long ago is
    /// uploaded at once. Zero uploads as soon as a slot is free. Up to 10 s.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_fresh_upload_interval")]
    #[config_env(leaf)]
    pub fresh_upload_interval: IggyDuration,
    /// The bytes of messages one partition that stores them in fresh
    /// objects may have committed that no long-term segment holds yet. Past
    /// it, the primary delays the sends to that partition. Zero turns the
    /// delay off. Otherwise at least [`LONG_TERM_BACKLOG_LIMIT_MIN`].
    #[serde(default = "default_long_term_backlog_limit")]
    #[config_env(leaf)]
    pub long_term_backlog_limit: IggyByteSize,
    /// The most bytes of messages one shard holds for sends to partitions
    /// that store them in fresh objects and that have not reached their
    /// partitions. A send past it is answered as not accepted, unless
    /// nothing waits. At least [`FRESH_PENDING_BYTES_MIN`].
    #[serde(default = "default_fresh_pending_bytes")]
    #[config_env(leaf)]
    pub fresh_pending_bytes: IggyByteSize,
    /// The most of `fresh_pending_bytes` that the waiting sends of one
    /// partition hold. A send past it is answered as not accepted, unless
    /// nothing of its partition waits. At least [`FRESH_PENDING_BYTES_MIN`]
    /// and at most `fresh_pending_bytes`.
    #[serde(default = "default_fresh_partition_pending_bytes")]
    #[config_env(leaf)]
    pub fresh_partition_pending_bytes: IggyByteSize,
    /// How long the sweeps of the root must find the same objects of a
    /// partition incarnation that committed metadata no longer holds before
    /// they delete the incarnation, by the clock of the node that sweeps,
    /// from the second sweep that found them. The sweep lists the root every
    /// hour, or every `orphan_grace` when that is shorter. Zero or
    /// `disabled` turns the sweep off. Otherwise at least
    /// [`ORPHAN_GRACE_MIN`].
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_orphan_grace")]
    #[config_env(leaf)]
    pub orphan_grace: IggyDuration,
}

/// How many jobs of each class the tier service of one shard runs at once,
/// and how many bytes they hold.
///
/// Segment uploads, fresh uploads, object deletes, cold reads that a poll
/// waits for, readahead and housekeeping go through the tier service.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct TierJobsConfig {
    /// Fetches that a waiting poll needs.
    #[serde(default = "default_demand_fetches")]
    pub demand_fetches: u32,
    /// The bytes that demand fetches hold in memory.
    #[serde(default = "default_demand_fetch_bytes")]
    #[config_env(leaf)]
    pub demand_fetch_bytes: IggyByteSize,
    #[serde(default = "default_uploads")]
    pub uploads: u32,
    /// The bytes of the segments that uploads send. A segment larger than
    /// this uploads alone.
    #[serde(default = "default_upload_bytes")]
    #[config_env(leaf)]
    pub upload_bytes: IggyByteSize,
    /// Uploads of fresh objects, which producers wait for. They have slots
    /// of their own, so they never wait behind a segment upload.
    #[serde(default = "default_fresh_uploads")]
    pub fresh_uploads: u32,
    /// Readahead that no poll waits for yet.
    #[serde(default = "default_speculative_fetches")]
    pub speculative_fetches: u32,
    #[serde(default = "default_speculative_fetch_bytes")]
    #[config_env(leaf)]
    pub speculative_fetch_bytes: IggyByteSize,
    #[serde(default = "default_deletes")]
    pub deletes: u32,
    /// Sweeps and verify passes, which start only while no fetch or upload
    /// waits.
    #[serde(default = "default_housekeeping")]
    pub housekeeping: u32,
}

/// What the `mock_s3` backend adds to the time of each store call. Its
/// objects stay in the memory of the server, so the shards share them and a
/// stop loses them. Each latency is shorter than the timeout of its call.
#[serde_as]
#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct MockS3Config {
    /// Each put, from the end of its body to its answer.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default)]
    #[config_env(leaf)]
    pub put_latency: IggyDuration,
    /// Each ranged read.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default)]
    #[config_env(leaf)]
    pub get_latency: IggyDuration,
    /// Each head, delete and listing.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default)]
    #[config_env(leaf)]
    pub other_latency: IggyDuration,
    /// How many bytes of a put's body or of a read move in a second, on top
    /// of the latency. Zero is no limit.
    #[serde(default)]
    #[config_env(leaf)]
    pub bytes_per_second: IggyByteSize,
}

impl Default for TieredStorageConfig {
    fn default() -> Self {
        Self {
            backend: TieredStorageBackend::default(),
            prefix: default_prefix(),
            path: String::new(),
            bucket: String::new(),
            endpoint: String::new(),
            region: String::new(),
            url_style: S3UrlStyle::default(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            gcs: GcsConfig::default(),
            jobs: TierJobsConfig::default(),
            mock_s3: MockS3Config::default(),
            request_timeout: default_request_timeout(),
            progress_timeout: default_progress_timeout(),
            max_attempts: default_max_attempts(),
            retry_delay: default_retry_delay(),
            max_retry_delay: default_max_retry_delay(),
            read_cache_size: default_read_cache_size(),
            read_cache_block_size: default_read_cache_block_size(),
            read_ahead: default_read_ahead(),
            fresh_upload_interval: default_fresh_upload_interval(),
            long_term_backlog_limit: default_long_term_backlog_limit(),
            fresh_pending_bytes: default_fresh_pending_bytes(),
            fresh_partition_pending_bytes: default_fresh_partition_pending_bytes(),
            orphan_grace: default_orphan_grace(),
        }
    }
}

impl Default for TierJobsConfig {
    fn default() -> Self {
        Self {
            demand_fetches: default_demand_fetches(),
            demand_fetch_bytes: default_demand_fetch_bytes(),
            uploads: default_uploads(),
            upload_bytes: default_upload_bytes(),
            fresh_uploads: default_fresh_uploads(),
            speculative_fetches: default_speculative_fetches(),
            speculative_fetch_bytes: default_speculative_fetch_bytes(),
            deletes: default_deletes(),
            housekeeping: default_housekeeping(),
        }
    }
}

/// Prints what [`Display`] prints, so credentials never show.
impl Debug for TieredStorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl TieredStorageConfig {
    /// What the printed configuration shows for the credentials.
    fn credentials(&self) -> &'static str {
        if self.access_key_id.is_empty() && self.secret_access_key.is_empty() {
            "none"
        } else {
            "[redacted]"
        }
    }
}

/// Prints whether credentials are set, never their values.
impl Display for TieredStorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ backend: {}, prefix: {}, path: {}, bucket: {}, endpoint: {}, region: {}, \
             url_style: {}, credentials: {}, gcs: {}, jobs: {}, mock_s3: {}, \
             request_timeout: {}, progress_timeout: {}, max_attempts: {}, retry_delay: {}, \
             max_retry_delay: {}, read_cache_size: {}, read_cache_block_size: {}, \
             read_ahead: {}, fresh_upload_interval: {}, long_term_backlog_limit: {}, \
             fresh_pending_bytes: {}, fresh_partition_pending_bytes: {}, orphan_grace: {} }}",
            self.backend,
            self.prefix,
            self.path,
            self.bucket,
            self.endpoint,
            self.region,
            self.url_style,
            self.credentials(),
            self.gcs,
            self.jobs,
            self.mock_s3,
            self.request_timeout,
            self.progress_timeout,
            self.max_attempts,
            self.retry_delay,
            self.max_retry_delay,
            self.read_cache_size,
            self.read_cache_block_size,
            self.read_ahead,
            self.fresh_upload_interval,
            self.long_term_backlog_limit,
            self.fresh_pending_bytes,
            self.fresh_partition_pending_bytes,
            self.orphan_grace,
        )
    }
}

impl Display for TierJobsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ demand_fetches: {}, demand_fetch_bytes: {}, uploads: {}, upload_bytes: {}, \
             fresh_uploads: {}, speculative_fetches: {}, speculative_fetch_bytes: {}, \
             deletes: {}, housekeeping: {} }}",
            self.demand_fetches,
            self.demand_fetch_bytes,
            self.uploads,
            self.upload_bytes,
            self.fresh_uploads,
            self.speculative_fetches,
            self.speculative_fetch_bytes,
            self.deletes,
            self.housekeeping,
        )
    }
}

impl Display for MockS3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ put_latency: {}, get_latency: {}, other_latency: {}, bytes_per_second: {} }}",
            self.put_latency, self.get_latency, self.other_latency, self.bytes_per_second,
        )
    }
}

impl Validatable<ConfigurationError> for TieredStorageConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        match self.backend {
            // Nothing else is read without a store, so values that a file
            // left from an earlier setup cannot fail the boot. The tier
            // services of geo peers check the tuning on their own, see
            // `Self::validate_tuning`.
            TieredStorageBackend::None => return Ok(()),
            TieredStorageBackend::Fs if self.path.trim().is_empty() => {
                return refuse("path must be set for the fs backend");
            }
            TieredStorageBackend::S3 if self.bucket.trim().is_empty() => {
                return refuse("bucket must be set for the s3 backend");
            }
            TieredStorageBackend::S3 if self.region.trim().is_empty() => {
                return refuse("region must be set for the s3 backend");
            }
            TieredStorageBackend::Gcs => {
                if let Some(fault) = self.gcs.fault() {
                    return refuse(fault);
                }
            }
            TieredStorageBackend::Fs | TieredStorageBackend::S3 | TieredStorageBackend::MockS3 => {}
        }
        if self.access_key_id.is_empty() != self.secret_access_key.is_empty() {
            return refuse("access_key_id and secret_access_key must be set together");
        }
        if self.prefix.is_empty() {
            return refuse("prefix must not be empty");
        }
        self.validate_tuning()?;
        let orphan_grace = self.orphan_grace.get_duration();
        if !orphan_grace.is_zero() && orphan_grace < ORPHAN_GRACE_MIN {
            return refuse(&format!(
                "orphan_grace must be 0 (disabled) or at least {}",
                IggyDuration::new(ORPHAN_GRACE_MIN)
            ));
        }
        if self.backend == TieredStorageBackend::MockS3 {
            self.validate_mock_s3()?;
        }
        Ok(())
    }
}

impl TieredStorageConfig {
    /// Checks the job caps, timeouts, retries and read cache. The tier
    /// services of geo peers take them too, and their reads go through the
    /// read cache, so they must hold even without a backend of this
    /// cluster's own.
    ///
    /// # Errors
    /// [`ConfigurationError::InvalidConfigurationValue`] for a cap of zero, a
    /// duration outside its bounds, no attempt, a backoff cap below its
    /// start, or a read cache that is on and whose block size, size and
    /// readahead do not fit together.
    pub fn validate_tuning(&self) -> Result<(), ConfigurationError> {
        let jobs = &self.jobs;
        let counts = [
            ("demand_fetches", jobs.demand_fetches),
            ("uploads", jobs.uploads),
            ("fresh_uploads", jobs.fresh_uploads),
            ("speculative_fetches", jobs.speculative_fetches),
            ("deletes", jobs.deletes),
            ("housekeeping", jobs.housekeeping),
        ];
        if let Some((name, _)) = counts.iter().find(|(_, count)| *count == 0) {
            return refuse(&format!("jobs.{name} must be greater than zero"));
        }
        let bytes = [
            ("demand_fetch_bytes", jobs.demand_fetch_bytes),
            ("upload_bytes", jobs.upload_bytes),
            ("speculative_fetch_bytes", jobs.speculative_fetch_bytes),
        ];
        if let Some((name, _)) = bytes.iter().find(|(_, size)| size.as_bytes_u64() == 0) {
            return refuse(&format!("jobs.{name} must be greater than zero"));
        }
        let durations = [
            ("request_timeout", self.request_timeout, TIMEOUT_BOUNDS),
            ("progress_timeout", self.progress_timeout, TIMEOUT_BOUNDS),
            ("retry_delay", self.retry_delay, RETRY_DELAY_BOUNDS),
            ("max_retry_delay", self.max_retry_delay, RETRY_DELAY_BOUNDS),
        ];
        if let Some((name, _, bounds)) = durations
            .iter()
            .find(|(_, duration, bounds)| !bounds.contains(&duration.get_duration()))
        {
            return refuse(&format!(
                "{name} must be between {} and {}",
                IggyDuration::new(*bounds.start()),
                IggyDuration::new(*bounds.end())
            ));
        }
        if self.max_attempts == 0 {
            return refuse("max_attempts must be at least 1");
        }
        if self.max_retry_delay.get_duration() < self.retry_delay.get_duration() {
            return refuse("max_retry_delay must not be shorter than retry_delay");
        }
        self.validate_read_cache()?;
        if self.fresh_upload_interval.get_duration() > FRESH_UPLOAD_INTERVAL_MAX {
            return refuse(&format!(
                "fresh_upload_interval must be at most {}",
                IggyDuration::new(FRESH_UPLOAD_INTERVAL_MAX)
            ));
        }
        let backlog_limit = self.long_term_backlog_limit.as_bytes_u64();
        if backlog_limit != 0 && backlog_limit < LONG_TERM_BACKLOG_LIMIT_MIN {
            return refuse(&format!(
                "long_term_backlog_limit must be 0 or at least {}",
                IggyByteSize::from(LONG_TERM_BACKLOG_LIMIT_MIN)
            ));
        }
        self.validate_fresh_pending_bytes()
    }

    /// The block size, size and readahead of a cache that is on. A cache
    /// that is off keeps nothing that reads fetch, and none of them take
    /// effect. The messages give exact bytes: rounded, a value one byte past
    /// a bound prints as the bound.
    fn validate_read_cache(&self) -> Result<(), ConfigurationError> {
        let cache_size = self.read_cache_size.as_bytes_u64();
        if cache_size == 0 {
            return Ok(());
        }
        let block = self.read_cache_block_size.as_bytes_u64();
        if !(READ_CACHE_BLOCK_SIZE_MIN..=READ_CACHE_BLOCK_SIZE_MAX).contains(&block) {
            return refuse(&format!(
                "read_cache_block_size is {block} B and must be between \
                 {READ_CACHE_BLOCK_SIZE_MIN} B and {READ_CACHE_BLOCK_SIZE_MAX} B"
            ));
        }
        if cache_size < READ_AHEAD_CACHE_SHARE * block {
            return refuse(&format!(
                "read_cache_size is {cache_size} B and must be 0 or at least \
                 {READ_AHEAD_CACHE_SHARE} blocks of read_cache_block_size ({block} B), so the \
                 part that holds readahead fits a block"
            ));
        }
        let read_ahead = self.read_ahead.as_bytes_u64();
        if read_ahead == 0 {
            return Ok(());
        }
        if read_ahead < READ_AHEAD_BLOCKS_MIN * block {
            return refuse(&format!(
                "read_ahead is {read_ahead} B and must be 0 or at least {READ_AHEAD_BLOCKS_MIN} \
                 blocks of read_cache_block_size ({block} B), what the first readahead fetches"
            ));
        }
        if read_ahead > cache_size / READ_AHEAD_CACHE_SHARE {
            return refuse(&format!(
                "read_ahead is {read_ahead} B and must be 0 or at most one part in \
                 {READ_AHEAD_CACHE_SHARE} of read_cache_size ({cache_size} B), the part of the \
                 cache that holds what readahead fetched"
            ));
        }
        Ok(())
    }

    fn validate_fresh_pending_bytes(&self) -> Result<(), ConfigurationError> {
        let shard_bytes = self.fresh_pending_bytes.as_bytes_u64();
        let partition_bytes = self.fresh_partition_pending_bytes.as_bytes_u64();
        let floor = IggyByteSize::from(FRESH_PENDING_BYTES_MIN);
        if shard_bytes < FRESH_PENDING_BYTES_MIN {
            return refuse(&format!("fresh_pending_bytes must be at least {floor}"));
        }
        if partition_bytes < FRESH_PENDING_BYTES_MIN {
            return refuse(&format!(
                "fresh_partition_pending_bytes must be at least {floor}"
            ));
        }
        if partition_bytes > shard_bytes {
            return refuse(&format!(
                "fresh_partition_pending_bytes must be at most fresh_pending_bytes ({})",
                self.fresh_pending_bytes
            ));
        }
        Ok(())
    }

    /// A latency at or past the timeout of its call would fail every call.
    fn validate_mock_s3(&self) -> Result<(), ConfigurationError> {
        let mock = &self.mock_s3;
        let latencies = [
            (
                "put_latency",
                mock.put_latency,
                "progress_timeout",
                self.progress_timeout,
            ),
            (
                "get_latency",
                mock.get_latency,
                "request_timeout",
                self.request_timeout,
            ),
            (
                "other_latency",
                mock.other_latency,
                "request_timeout",
                self.request_timeout,
            ),
        ];
        match latencies
            .iter()
            .find(|(_, latency, _, timeout)| latency.get_duration() >= timeout.get_duration())
        {
            Some((name, _, timeout, _)) => {
                refuse(&format!("mock_s3.{name} must be shorter than {timeout}"))
            }
            None => Ok(()),
        }
    }
}

fn refuse(reason: &str) -> Result<(), ConfigurationError> {
    eprintln!("{COMPONENT} tiered_storage.{reason}");
    Err(ConfigurationError::InvalidConfigurationValue)
}

fn default_prefix() -> String {
    "iggy".to_owned()
}

fn default_request_timeout() -> IggyDuration {
    IggyDuration::new_from_secs(30)
}

fn default_progress_timeout() -> IggyDuration {
    IggyDuration::new_from_secs(30)
}

const fn default_max_attempts() -> u32 {
    3
}

fn default_retry_delay() -> IggyDuration {
    IggyDuration::new(std::time::Duration::from_millis(200))
}

fn default_max_retry_delay() -> IggyDuration {
    IggyDuration::new_from_secs(5)
}

fn default_read_cache_size() -> IggyByteSize {
    IggyByteSize::from(64 * 1024 * 1024)
}

fn default_read_cache_block_size() -> IggyByteSize {
    IggyByteSize::from(256 * 1024)
}

fn default_read_ahead() -> IggyByteSize {
    IggyByteSize::from(2 * 1024 * 1024)
}

fn default_fresh_upload_interval() -> IggyDuration {
    IggyDuration::new(Duration::from_millis(10))
}

fn default_long_term_backlog_limit() -> IggyByteSize {
    IggyByteSize::from(1024 * 1024 * 1024)
}

fn default_fresh_pending_bytes() -> IggyByteSize {
    IggyByteSize::from(32 * 1024 * 1024)
}

fn default_fresh_partition_pending_bytes() -> IggyByteSize {
    IggyByteSize::from(8 * 1024 * 1024)
}

fn default_orphan_grace() -> IggyDuration {
    IggyDuration::new(Duration::from_hours(6))
}

const fn default_demand_fetches() -> u32 {
    16
}

fn default_demand_fetch_bytes() -> IggyByteSize {
    IggyByteSize::from(64 * 1024 * 1024)
}

const fn default_uploads() -> u32 {
    2
}

fn default_upload_bytes() -> IggyByteSize {
    IggyByteSize::from(2048 * 1024 * 1024)
}

const fn default_fresh_uploads() -> u32 {
    16
}

const fn default_speculative_fetches() -> u32 {
    4
}

fn default_speculative_fetch_bytes() -> IggyByteSize {
    IggyByteSize::from(32 * 1024 * 1024)
}

const fn default_deletes() -> u32 {
    2
}

const fn default_housekeeping() -> u32 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fs_config() -> TieredStorageConfig {
        TieredStorageConfig {
            backend: TieredStorageBackend::Fs,
            path: "/var/lib/iggy-tiered".to_owned(),
            ..TieredStorageConfig::default()
        }
    }

    fn s3_config() -> TieredStorageConfig {
        TieredStorageConfig {
            backend: TieredStorageBackend::S3,
            bucket: "segments".to_owned(),
            region: "eu-central-1".to_owned(),
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG".to_owned(),
            ..TieredStorageConfig::default()
        }
    }

    #[test]
    fn given_no_backend_when_validating_should_accept_any_other_value() {
        let config = TieredStorageConfig {
            prefix: String::new(),
            max_attempts: 0,
            ..TieredStorageConfig::default()
        };

        assert!(config.validate().is_ok());
    }

    /// Geo peers take the tuning even when this cluster has no store.
    #[test]
    fn given_no_backend_when_validating_the_tuning_should_still_refuse_a_bad_value() {
        let config = TieredStorageConfig {
            max_attempts: 0,
            ..TieredStorageConfig::default()
        };

        assert!(config.validate_tuning().is_err());
        assert!(TieredStorageConfig::default().validate_tuning().is_ok());
    }

    #[test]
    fn given_fs_backend_without_a_path_when_validating_should_refuse() {
        let config = TieredStorageConfig {
            path: " ".to_owned(),
            ..fs_config()
        };

        assert!(config.validate().is_err());
        assert!(fs_config().validate().is_ok());
    }

    #[test]
    fn given_s3_backend_without_a_bucket_or_region_when_validating_should_refuse() {
        let no_bucket = TieredStorageConfig {
            bucket: String::new(),
            ..s3_config()
        };
        let no_region = TieredStorageConfig {
            region: String::new(),
            ..s3_config()
        };

        assert!(no_bucket.validate().is_err());
        assert!(no_region.validate().is_err());
        assert!(s3_config().validate().is_ok());
    }

    fn gcs_config() -> TieredStorageConfig {
        TieredStorageConfig {
            backend: TieredStorageBackend::Gcs,
            gcs: GcsConfig {
                bucket: "segments".to_owned(),
                ..GcsConfig::default()
            },
            ..TieredStorageConfig::default()
        }
    }

    #[test]
    fn given_gcs_backend_without_a_bucket_when_validating_should_refuse() {
        let no_bucket = TieredStorageConfig {
            gcs: GcsConfig::default(),
            ..gcs_config()
        };
        let a_path = TieredStorageConfig {
            gcs: GcsConfig {
                bucket: "segments/iggy".to_owned(),
                ..GcsConfig::default()
            },
            ..gcs_config()
        };
        let s3_keys_unused = TieredStorageConfig {
            bucket: String::new(),
            region: String::new(),
            ..gcs_config()
        };

        assert!(no_bucket.validate().is_err());
        assert!(a_path.validate().is_err());
        assert!(gcs_config().validate().is_ok());
        assert!(s3_keys_unused.validate().is_ok());
    }

    #[test]
    fn given_gcs_section_when_reading_should_take_its_keys_and_default_to_google() {
        let config: TieredStorageConfig = serde_json::from_str(
            r#"{"backend": "gcs", "gcs": {"bucket": "segments", "auth": "none",
                "endpoint": "http://localhost:4443", "service_account_key": "/etc/iggy/key.json"}}"#,
        )
        .unwrap();
        let unknown = serde_json::from_str::<TieredStorageConfig>(
            r#"{"backend": "gcs", "gcs": {"bucket": "segments", "region": "eu"}}"#,
        );

        assert_eq!(config.backend, TieredStorageBackend::Gcs);
        assert_eq!(config.gcs.auth, GcsAuth::None);
        assert_eq!(config.gcs.endpoint(), "http://localhost:4443");
        assert_eq!(config.gcs.service_account_key, "/etc/iggy/key.json");
        assert_eq!(GcsConfig::default().endpoint(), GCS_ENDPOINT);
        assert_eq!(GcsConfig::default().auth, GcsAuth::Auto);
        assert!(config.validate().is_ok());
        assert!(unknown.is_err());
        assert!(config.to_string().contains("gcs: { bucket: segments"));
    }

    #[test]
    fn given_half_of_the_static_credentials_when_validating_should_refuse() {
        let config = TieredStorageConfig {
            secret_access_key: String::new(),
            ..s3_config()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_a_zero_cap_or_timeout_when_validating_should_refuse() {
        let no_uploads = TieredStorageConfig {
            jobs: TierJobsConfig {
                uploads: 0,
                ..TierJobsConfig::default()
            },
            ..fs_config()
        };
        let no_fresh_uploads = TieredStorageConfig {
            jobs: TierJobsConfig {
                fresh_uploads: 0,
                ..TierJobsConfig::default()
            },
            ..fs_config()
        };
        let no_fetch_bytes = TieredStorageConfig {
            jobs: TierJobsConfig {
                demand_fetch_bytes: IggyByteSize::from(0),
                ..TierJobsConfig::default()
            },
            ..fs_config()
        };
        let no_progress_timeout = TieredStorageConfig {
            progress_timeout: IggyDuration::new(std::time::Duration::ZERO),
            ..fs_config()
        };
        let backoff_cap_below_its_start = TieredStorageConfig {
            retry_delay: IggyDuration::new_from_secs(10),
            ..fs_config()
        };

        for config in [
            no_uploads,
            no_fresh_uploads,
            no_fetch_bytes,
            no_progress_timeout,
            backoff_cap_below_its_start,
        ] {
            assert!(config.validate().is_err(), "{config}");
        }
    }

    fn duration(text: &str) -> IggyDuration {
        text.parse().unwrap()
    }

    #[test]
    fn given_a_duration_outside_its_bounds_when_validating_should_refuse() {
        let configs = [
            TieredStorageConfig {
                request_timeout: duration("9223372036854775807s"),
                ..fs_config()
            },
            TieredStorageConfig {
                request_timeout: duration("999ms"),
                ..fs_config()
            },
            TieredStorageConfig {
                progress_timeout: duration("1ns"),
                ..fs_config()
            },
            TieredStorageConfig {
                progress_timeout: duration("61min"),
                ..fs_config()
            },
            TieredStorageConfig {
                retry_delay: duration("999us"),
                ..fs_config()
            },
            TieredStorageConfig {
                max_retry_delay: duration("9223372036854775807s"),
                ..fs_config()
            },
        ];

        for config in configs {
            assert!(config.validate().is_err(), "{config}");
        }
    }

    #[test]
    fn given_durations_at_their_bounds_when_validating_should_accept() {
        let shortest = TieredStorageConfig {
            request_timeout: duration("1s"),
            progress_timeout: duration("1s"),
            retry_delay: duration("1ms"),
            max_retry_delay: duration("1ms"),
            ..fs_config()
        };
        let longest = TieredStorageConfig {
            request_timeout: duration("1h"),
            progress_timeout: duration("1h"),
            retry_delay: duration("1h"),
            max_retry_delay: duration("1h"),
            ..fs_config()
        };

        assert!(shortest.validate().is_ok(), "{shortest}");
        assert!(longest.validate().is_ok(), "{longest}");
    }

    #[test]
    fn given_no_read_cache_keys_when_reading_the_section_should_use_64_mib_256_kib_and_2_mib() {
        let config: TieredStorageConfig =
            serde_json::from_str(r#"{"backend": "fs", "path": "/var/lib/iggy-tiered"}"#).unwrap();

        assert_eq!(config.read_cache_size.as_bytes_u64(), 64 << 20);
        assert_eq!(config.read_cache_block_size.as_bytes_u64(), 256 << 10);
        assert_eq!(config.read_ahead.as_bytes_u64(), 2 << 20);
        let defaults = TieredStorageConfig::default();
        assert_eq!(config.read_cache_size, defaults.read_cache_size);
        assert_eq!(config.read_cache_block_size, defaults.read_cache_block_size);
        assert_eq!(config.read_ahead, defaults.read_ahead);
        assert!(config.validate().is_ok());
        assert!(defaults.validate_tuning().is_ok());
        let printed = format!(
            "read_cache_size: {}, read_cache_block_size: {}, read_ahead: {},",
            config.read_cache_size, config.read_cache_block_size, config.read_ahead
        );
        assert!(config.to_string().contains(&printed), "{config}");
    }

    fn read_cache(size: &str, block_size: &str, read_ahead: &str) -> TieredStorageConfig {
        TieredStorageConfig {
            read_cache_size: size.parse().unwrap(),
            read_cache_block_size: block_size.parse().unwrap(),
            read_ahead: read_ahead.parse().unwrap(),
            ..fs_config()
        }
    }

    #[test]
    fn given_a_read_cache_block_size_outside_64_kib_to_1_mib_when_validating_should_refuse() {
        assert!(read_cache("64 MiB", "64 KiB", "2 MiB").validate().is_ok());
        assert!(read_cache("64 MiB", "1 MiB", "2 MiB").validate().is_ok());
        assert!(read_cache("64 MiB", "65535 B", "0").validate().is_err());
        assert!(read_cache("64 MiB", "1048577 B", "0").validate().is_err());
        assert!(read_cache("64 MiB", "0", "0").validate().is_err());
    }

    /// Geo peers' reads go through the read cache, so a node with peers and
    /// no store of its own checks it too.
    #[test]
    fn given_read_cache_settings_that_do_not_fit_when_validating_the_tuning_of_geo_peers_should_refuse()
     {
        for (size, block_size, read_ahead) in [
            ("64 MiB", "2 MiB", "2 MiB"),
            ("64 MiB", "256 KiB", "9 MiB"),
            ("1 B", "256 KiB", "0"),
        ] {
            let no_backend = TieredStorageConfig {
                read_cache_size: size.parse().unwrap(),
                read_cache_block_size: block_size.parse().unwrap(),
                read_ahead: read_ahead.parse().unwrap(),
                ..TieredStorageConfig::default()
            };
            assert!(no_backend.validate_tuning().is_err(), "{size} {block_size}");
            assert!(no_backend.validate().is_ok(), "{size} {block_size}");
            assert!(
                read_cache(size, block_size, read_ahead).validate().is_err(),
                "{size} {block_size}"
            );
        }
    }

    /// An eighth of the cache holds what readahead fetched, and it has to
    /// hold one block.
    #[test]
    fn given_a_read_cache_smaller_than_eight_blocks_when_validating_should_refuse() {
        assert!(read_cache("2 MiB", "256 KiB", "0").validate().is_ok());
        assert!(read_cache("2097151 B", "256 KiB", "0").validate().is_err());
        assert!(read_cache("8 MiB", "1 MiB", "0").validate().is_ok());
        assert!(read_cache("8388607 B", "1 MiB", "0").validate().is_err());
        assert!(read_cache("1 B", "64 KiB", "0").validate().is_err());
    }

    #[test]
    fn given_read_ahead_below_two_blocks_when_validating_should_refuse() {
        assert!(
            read_cache("64 MiB", "256 KiB", "512 KiB")
                .validate()
                .is_ok()
        );
        assert!(
            read_cache("64 MiB", "256 KiB", "524287 B")
                .validate()
                .is_err()
        );
        assert!(
            read_cache("64 MiB", "256 KiB", "256 KiB")
                .validate()
                .is_err()
        );
        assert!(read_cache("64 MiB", "1 MiB", "1 MiB").validate().is_err());
        assert!(
            read_cache("64 MiB", "256 KiB", "0").validate().is_ok(),
            "no readahead"
        );
    }

    #[test]
    fn given_read_ahead_past_an_eighth_of_the_read_cache_when_validating_should_refuse() {
        assert!(read_cache("64 MiB", "256 KiB", "8 MiB").validate().is_ok());
        assert!(
            read_cache("64 MiB", "256 KiB", "8388609 B")
                .validate()
                .is_err()
        );
        assert!(read_cache("8 MiB", "256 KiB", "1 MiB").validate().is_ok());
        assert!(read_cache("8 MiB", "256 KiB", "2 MiB").validate().is_err());
    }

    #[test]
    fn given_the_read_cache_turned_off_when_validating_should_not_check_its_settings() {
        assert!(read_cache("0", "256 KiB", "1 B").validate().is_ok());
        assert!(read_cache("0", "256 KiB", "1 GiB").validate().is_ok());
        assert!(read_cache("0", "64 KiB", "0").validate().is_ok());
        assert!(read_cache("0", "2 MiB", "0").validate().is_ok());
        assert!(read_cache("0", "0", "0").validate().is_ok());
    }

    #[test]
    fn given_no_fresh_upload_keys_when_reading_the_section_should_use_their_defaults() {
        let config: TieredStorageConfig =
            serde_json::from_str(r#"{"backend": "fs", "path": "/var/lib/iggy-tiered"}"#).unwrap();

        assert_eq!(config.jobs.fresh_uploads, 16);
        assert_eq!(
            config.fresh_upload_interval.get_duration(),
            Duration::from_millis(10)
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn given_a_fresh_upload_interval_when_validating_should_accept_zero_and_refuse_above_the_most()
    {
        let none = TieredStorageConfig {
            fresh_upload_interval: duration("0"),
            ..fs_config()
        };
        let longest = TieredStorageConfig {
            fresh_upload_interval: duration("10s"),
            ..fs_config()
        };
        let too_long = TieredStorageConfig {
            fresh_upload_interval: duration("10001ms"),
            ..fs_config()
        };

        assert!(none.validate().is_ok(), "{none}");
        assert!(longest.validate().is_ok(), "{longest}");
        assert!(too_long.validate().is_err(), "{too_long}");
    }

    #[test]
    fn given_no_long_term_backlog_limit_when_reading_the_section_should_use_its_default() {
        let config: TieredStorageConfig =
            serde_json::from_str(r#"{"backend": "fs", "path": "/var/lib/iggy-tiered"}"#).unwrap();

        assert_eq!(config.long_term_backlog_limit.as_bytes_u64(), 1 << 30);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn given_a_long_term_backlog_limit_when_reading_the_section_should_take_it() {
        let config: TieredStorageConfig = serde_json::from_str(
            r#"{"backend": "fs", "path": "/var/lib/iggy-tiered", "long_term_backlog_limit": "2 GiB"}"#,
        )
        .unwrap();
        let unreadable = serde_json::from_str::<TieredStorageConfig>(
            r#"{"backend": "fs", "path": "/var/lib/iggy-tiered", "long_term_backlog_limit": "lots"}"#,
        );

        assert_eq!(config.long_term_backlog_limit.as_bytes_u64(), 2 << 30);
        assert!(config.validate().is_ok());
        assert!(unreadable.is_err());
    }

    #[test]
    fn given_a_long_term_backlog_limit_when_validating_should_accept_zero_and_refuse_less_than_a_segment()
     {
        let limit = |text: &str| TieredStorageConfig {
            long_term_backlog_limit: text.parse().unwrap(),
            ..fs_config()
        };

        assert_eq!(limit("0").long_term_backlog_limit.as_bytes_u64(), 0);
        assert!(limit("0").validate().is_ok(), "zero turns the delay off");
        assert!(limit("64 MiB").validate().is_ok());
        assert!(limit("1 B").validate().is_err());
        assert!(limit("67108863 B").validate().is_err());
    }

    #[test]
    fn given_no_fresh_pending_bytes_keys_when_reading_the_section_should_use_32_mib_and_8_mib() {
        let config: TieredStorageConfig =
            serde_json::from_str(r#"{"backend": "fs", "path": "/var/lib/iggy-tiered"}"#).unwrap();

        assert_eq!(config.fresh_pending_bytes.as_bytes_u64(), 32 << 20);
        assert_eq!(config.fresh_partition_pending_bytes.as_bytes_u64(), 8 << 20);
        assert_eq!(
            config.fresh_pending_bytes,
            TieredStorageConfig::default().fresh_pending_bytes
        );
        assert_eq!(
            config.fresh_partition_pending_bytes,
            TieredStorageConfig::default().fresh_partition_pending_bytes
        );
        assert!(config.validate().is_ok());
        let printed = format!(
            "fresh_pending_bytes: {}, fresh_partition_pending_bytes: {}, orphan_grace: {} }}",
            config.fresh_pending_bytes, config.fresh_partition_pending_bytes, config.orphan_grace
        );
        assert!(config.to_string().ends_with(&printed), "{config}");
    }

    #[test]
    fn given_fresh_pending_bytes_keys_when_reading_the_section_should_take_them() {
        let config: TieredStorageConfig = serde_json::from_str(
            r#"{"backend": "fs", "path": "/var/lib/iggy-tiered",
                "fresh_pending_bytes": "128 MiB", "fresh_partition_pending_bytes": "48 MiB"}"#,
        )
        .unwrap();
        let unreadable = serde_json::from_str::<TieredStorageConfig>(
            r#"{"backend": "fs", "path": "/var/lib/iggy-tiered", "fresh_pending_bytes": "lots"}"#,
        );

        assert_eq!(config.fresh_pending_bytes.as_bytes_u64(), 128 << 20);
        assert_eq!(
            config.fresh_partition_pending_bytes.as_bytes_u64(),
            48 << 20
        );
        assert!(config.validate().is_ok());
        assert!(unreadable.is_err());
    }

    #[test]
    fn given_fresh_pending_bytes_below_one_object_when_validating_should_refuse() {
        let bounds = |shard: &str, partition: &str| TieredStorageConfig {
            fresh_pending_bytes: shard.parse().unwrap(),
            fresh_partition_pending_bytes: partition.parse().unwrap(),
            ..fs_config()
        };

        assert!(bounds("4 MiB", "4 MiB").validate().is_ok());
        assert!(bounds("4194303 B", "4194303 B").validate().is_err());
        assert!(bounds("0", "0").validate().is_err());
        assert!(bounds("32 MiB", "4194303 B").validate().is_err());
        assert!(bounds("32 MiB", "0").validate().is_err());
    }

    #[test]
    fn given_a_partition_share_above_the_shard_cap_when_validating_should_refuse() {
        let bounds = |shard: &str, partition: &str| TieredStorageConfig {
            fresh_pending_bytes: shard.parse().unwrap(),
            fresh_partition_pending_bytes: partition.parse().unwrap(),
            ..fs_config()
        };

        assert!(bounds("16 MiB", "16 MiB").validate().is_ok());
        assert!(bounds("16 MiB", "16777217 B").validate().is_err());
        assert!(bounds("8 MiB", "32 MiB").validate().is_err());
    }

    #[test]
    fn given_no_orphan_grace_when_reading_the_section_should_wait_six_hours() {
        let config: TieredStorageConfig =
            serde_json::from_str(r#"{"backend": "fs", "path": "/var/lib/iggy-tiered"}"#).unwrap();

        assert_eq!(config.orphan_grace.get_duration(), Duration::from_hours(6));
        assert_eq!(
            config.orphan_grace,
            TieredStorageConfig::default().orphan_grace
        );
        assert!(config.validate().is_ok());
        assert!(config.to_string().contains("orphan_grace: 6h"), "{config}");
    }

    #[test]
    fn given_orphan_grace_disabled_when_reading_the_section_should_turn_the_sweep_off() {
        for text in ["disabled", "0", "none"] {
            let config: TieredStorageConfig = serde_json::from_str(&format!(
                r#"{{"backend": "fs", "path": "/var/lib/iggy-tiered", "orphan_grace": "{text}"}}"#
            ))
            .unwrap();

            assert!(config.orphan_grace.is_zero(), "{text}");
            assert!(config.validate().is_ok(), "{text}");
        }
        let unreadable = serde_json::from_str::<TieredStorageConfig>(
            r#"{"backend": "fs", "path": "/var/lib/iggy-tiered", "orphan_grace": "soon"}"#,
        );
        assert!(unreadable.is_err());
    }

    #[test]
    fn given_orphan_grace_below_a_second_when_validating_should_refuse() {
        let grace = |text: &str| TieredStorageConfig {
            orphan_grace: duration(text),
            ..fs_config()
        };

        assert!(grace("1s").validate().is_ok());
        assert!(grace("30 days").validate().is_ok());
        assert!(grace("999ms").validate().is_err());
        assert!(grace("1us").validate().is_err());
        let ignored_without_a_backend = TieredStorageConfig {
            orphan_grace: duration("1ms"),
            ..TieredStorageConfig::default()
        };
        assert!(ignored_without_a_backend.validate().is_ok());
    }

    #[test]
    fn given_mock_s3_backend_without_its_section_when_reading_should_take_no_latency() {
        let config: TieredStorageConfig =
            serde_json::from_str(r#"{"backend": "mock_s3"}"#).unwrap();

        assert_eq!(config.backend, TieredStorageBackend::MockS3);
        assert_eq!(config.mock_s3, MockS3Config::default());
        assert!(config.mock_s3.put_latency.is_zero());
        assert_eq!(config.mock_s3.bytes_per_second.as_bytes_u64(), 0);
        assert!(config.validate().is_ok(), "{config}");
    }

    #[test]
    fn given_mock_s3_latency_at_the_timeout_of_its_call_when_validating_should_refuse() {
        let mock = |mock_s3: MockS3Config| TieredStorageConfig {
            backend: TieredStorageBackend::MockS3,
            mock_s3,
            ..TieredStorageConfig::default()
        };
        let just_below = mock(MockS3Config {
            put_latency: duration("29999ms"),
            get_latency: duration("29999ms"),
            other_latency: duration("29999ms"),
            bytes_per_second: "100 MiB".parse().unwrap(),
        });
        let refused = [
            mock(MockS3Config {
                put_latency: duration("30s"),
                ..MockS3Config::default()
            }),
            mock(MockS3Config {
                get_latency: duration("30s"),
                ..MockS3Config::default()
            }),
            mock(MockS3Config {
                other_latency: duration("1h"),
                ..MockS3Config::default()
            }),
        ];
        let ignored_by_fs = TieredStorageConfig {
            mock_s3: refused[0].mock_s3.clone(),
            ..fs_config()
        };

        assert!(just_below.validate().is_ok(), "{just_below}");
        for config in refused {
            assert!(config.validate().is_err(), "{config}");
        }
        assert!(ignored_by_fs.validate().is_ok(), "{ignored_by_fs}");
    }

    #[test]
    fn given_credentials_when_printing_or_serializing_should_never_show_them() {
        let config = s3_config();

        let printed = [
            config.to_string(),
            format!("{config:?}"),
            serde_json::to_string(&config).unwrap(),
        ];

        for text in printed {
            assert!(!text.contains("AKIDEXAMPLE"), "{text}");
            assert!(!text.contains("wJalrXUtnFEMI"), "{text}");
        }
        assert!(config.to_string().contains("credentials: [redacted]"));
    }
}
