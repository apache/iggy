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
use configs::ConfigEnv;
use iggy_common::{IggyByteSize, IggyDuration, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::RangeInclusive;
use std::time::Duration;

/// Bounds of `request_timeout` and `progress_timeout`. A healthy store can
/// take seconds to answer, and the upper bound keeps a typo from overflowing
/// a deadline.
const TIMEOUT_BOUNDS: RangeInclusive<Duration> = Duration::from_secs(1)..=Duration::from_hours(1);

/// Bounds of `retry_delay` and `max_retry_delay`.
const RETRY_DELAY_BOUNDS: RangeInclusive<Duration> =
    Duration::from_millis(1)..=Duration::from_hours(1);

/// The longest `fresh_upload_interval`. A producer waits out the interval
/// before its messages are stored, so a longer one would only answer late.
const FRESH_UPLOAD_INTERVAL_MAX: Duration = Duration::from_secs(10);

/// The smallest `long_term_backlog_limit` other than zero: what one
/// long-term segment holds at most. A rewrite stores one segment at a time,
/// so a lower limit would slow the sends to a partition whose rewrites keep
/// up.
pub const LONG_TERM_BACKLOG_LIMIT_MIN: u64 = 64 * 1024 * 1024;

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
}

impl Display for TieredStorageBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::None => "none",
            Self::Fs => "fs",
            Self::S3 => "s3",
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
    /// while it is the caught-up metadata primary and no topic is tiered, and
    /// a node uploads and deletes objects only after it reads that claim back.
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
    #[serde(default)]
    pub jobs: TierJobsConfig,
    /// The longest wait for the answer to one attempt at a store call other
    /// than a put. From 1 s to 1 h.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_request_timeout")]
    #[config_env(leaf)]
    pub request_timeout: IggyDuration,
    /// The longest a put waits for the store to take the next piece of its
    /// body, or to answer after the last piece. A put has no total deadline.
    /// From 1 s to 1 h.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_progress_timeout")]
    #[config_env(leaf)]
    pub progress_timeout: IggyDuration,
    /// The most attempts at a store call other than a put that keeps failing
    /// with a transient error, the first one included.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// The wait before the first retry, doubled for each retry after it.
    /// From 1 ms to 1 h.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_retry_delay")]
    #[config_env(leaf)]
    pub retry_delay: IggyDuration,
    /// The longest wait between two attempts. From 1 ms to 1 h.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_max_retry_delay")]
    #[config_env(leaf)]
    pub max_retry_delay: IggyDuration,
    /// The shortest time between the starts of two uploads of fresh objects
    /// on one shard, unless an object fills first. A send that finds no
    /// upload in flight and the last one started at least this long ago is
    /// uploaded at once. Zero uploads as soon as a slot is free. Up to 10 s.
    #[serde_as(as = "DisplayFromStr")]
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
}

/// How many jobs of each class the tier service of one shard runs at once,
/// and how many bytes they hold.
///
/// Only segment uploads and object deletes go through the tier service so
/// far, so the fetch caps take no effect yet. They take effect once cold
/// reads go through it.
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
            jobs: TierJobsConfig::default(),
            request_timeout: default_request_timeout(),
            progress_timeout: default_progress_timeout(),
            max_attempts: default_max_attempts(),
            retry_delay: default_retry_delay(),
            max_retry_delay: default_max_retry_delay(),
            fresh_upload_interval: default_fresh_upload_interval(),
            long_term_backlog_limit: default_long_term_backlog_limit(),
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
             url_style: {}, credentials: {}, jobs: {}, request_timeout: {}, \
             progress_timeout: {}, max_attempts: {}, retry_delay: {}, max_retry_delay: {}, \
             fresh_upload_interval: {}, long_term_backlog_limit: {} }}",
            self.backend,
            self.prefix,
            self.path,
            self.bucket,
            self.endpoint,
            self.region,
            self.url_style,
            self.credentials(),
            self.jobs,
            self.request_timeout,
            self.progress_timeout,
            self.max_attempts,
            self.retry_delay,
            self.max_retry_delay,
            self.fresh_upload_interval,
            self.long_term_backlog_limit,
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
            TieredStorageBackend::Fs | TieredStorageBackend::S3 => {}
        }
        if self.access_key_id.is_empty() != self.secret_access_key.is_empty() {
            return refuse("access_key_id and secret_access_key must be set together");
        }
        if self.prefix.is_empty() {
            return refuse("prefix must not be empty");
        }
        self.validate_tuning()
    }
}

impl TieredStorageConfig {
    /// Checks the job caps, timeouts and retries. The tier services of geo
    /// peers take them too, so they must hold even without a backend of this
    /// cluster's own.
    ///
    /// # Errors
    /// [`ConfigurationError::InvalidConfigurationValue`] for a cap of zero, a
    /// duration outside its bounds, no attempt, or a backoff cap below its
    /// start.
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
        Ok(())
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

fn default_fresh_upload_interval() -> IggyDuration {
    IggyDuration::new(Duration::from_millis(10))
}

fn default_long_term_backlog_limit() -> IggyByteSize {
    IggyByteSize::from(1024 * 1024 * 1024)
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
    4
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
    fn given_no_fresh_upload_keys_when_reading_the_section_should_use_their_defaults() {
        let config: TieredStorageConfig =
            serde_json::from_str(r#"{"backend": "fs", "path": "/var/lib/iggy-tiered"}"#).unwrap();

        assert_eq!(config.jobs.fresh_uploads, 4);
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
