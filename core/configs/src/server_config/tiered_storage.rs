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
             progress_timeout: {}, max_attempts: {}, retry_delay: {}, max_retry_delay: {} }}",
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
        )
    }
}

impl Display for TierJobsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ demand_fetches: {}, demand_fetch_bytes: {}, uploads: {}, upload_bytes: {}, \
             speculative_fetches: {}, speculative_fetch_bytes: {}, deletes: {}, \
             housekeeping: {} }}",
            self.demand_fetches,
            self.demand_fetch_bytes,
            self.uploads,
            self.upload_bytes,
            self.speculative_fetches,
            self.speculative_fetch_bytes,
            self.deletes,
            self.housekeeping,
        )
    }
}

impl Validatable<ConfigurationError> for TieredStorageConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        let refuse = |reason: &str| {
            eprintln!("{COMPONENT} tiered_storage.{reason}");
            Err(ConfigurationError::InvalidConfigurationValue)
        };
        match self.backend {
            // Nothing else is read without a store, so values that a file
            // left from an earlier setup cannot fail the boot.
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
        let jobs = &self.jobs;
        let counts = [
            ("demand_fetches", jobs.demand_fetches),
            ("uploads", jobs.uploads),
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
        Ok(())
    }
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
