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

//! On-disk schema for geo topics: the name of this cluster among the
//! datacenters, its origin, and the object stores of the other clusters that
//! it reads, its peers.
//!
//! The section is optional. Without it the origin is empty and there are no
//! peers, so existing configuration files keep working.
//!
//! The cluster publishes an object for each of its geo topics in its own
//! `[tiered_storage]` store, and a deleted geo topic leaves a tombstone
//! there. `tombstone_grace` is how long the tombstone and the topic's
//! objects stay before the cluster deletes them.
//!
//! A peer's store is the `[tiered_storage]` store of another cluster, and its
//! keys repeat the keys of that section. The server never writes to a peer's
//! store, so read-only credentials are enough. The tier service of a peer
//! takes its timeouts and job caps from `[tiered_storage]`.

use super::COMPONENT;
use super::tiered_storage::{S3UrlStyle, TieredStorageBackend};
use crate::ConfigurationError;
use configs::{ConfigEnv, DisplayFromStrOrNumber};
use iggy_common::{IggyDuration, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fmt::{self, Debug, Display, Formatter};
use std::time::Duration;

/// The most peers that one cluster reads. It matches the
/// `#[config_env(max_elements = 16)]` cap on `peers`: an environment
/// override exists only for the first 16 entries.
pub const MAX_PEERS: usize = 16;

/// The longest origin name, in bytes. A shadow stream is named
/// `__shadow_{origin}_{stream}`, and a stream name holds at most 255 bytes,
/// so a short origin leaves room for the name of the stream.
pub const MAX_ORIGIN_LEN: usize = 32;

/// The longest `consumer_offset_upload_interval`. A store of a busy log
/// waits up to the interval before its answer, so a longer one only answers
/// late.
pub const CONSUMER_OFFSET_UPLOAD_INTERVAL_MAX: Duration = Duration::from_secs(1);

#[serde_as]
#[derive(Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct GeoConfig {
    /// The name of this cluster among the datacenters. Shadow stream names
    /// and the objects that this cluster publishes for its peers carry it.
    /// Empty when the cluster has no origin, which it needs for peers.
    #[serde(default)]
    pub origin: String,
    /// The clusters whose stores this cluster reads.
    #[serde(default)]
    #[config_env(max_elements = 16)]
    pub peers: Vec<GeoPeerConfig>,
    /// How long a deleted geo topic keeps its tombstone and its objects in
    /// this cluster's store, so that each peer sees the tombstone before the
    /// objects go. A cluster that reads peers also keeps a detached shadow
    /// topic this long after the peer wrote its tombstone.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_tombstone_grace")]
    #[config_env(leaf)]
    pub tombstone_grace: IggyDuration,
    /// The `cluster_uid` of a cluster that lost every disk, as 32 hex
    /// digits, whose geo topics this cluster takes back from its store. Only
    /// replica 0 reads it. Empty, the default, takes nothing back: a cluster
    /// then refuses a store that another cluster claimed.
    #[serde(default)]
    pub recover_cluster_uid: String,
    /// Whether a recovery also takes the batches that no proof names, at the
    /// offsets their writer expected: at least once instead of at most once.
    #[serde(default = "default_recover_unproven")]
    pub recover_unproven: bool,
    /// How often a node lists the manifests of each peer stream that a
    /// shadow stream serves, so its shadow partitions follow the peer.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_manifest_poll_interval")]
    #[config_env(leaf)]
    pub manifest_poll_interval: IggyDuration,
    /// How often a node stores the positions of the consumer groups of its
    /// geo and shadow topics in its store, when they changed. A group that
    /// commits on read re-reads at most this much more in another
    /// datacenter after a failover.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_positions_publish_interval")]
    #[config_env(leaf)]
    pub positions_publish_interval: IggyDuration,
    /// The shortest time between two stores of positions for the offsets
    /// that consumer groups of geo and shadow topics store themselves. Such
    /// a store is answered only once the node's store holds its position,
    /// and the stores that arrive within the interval share one put. Zero
    /// or `disabled` answers them at the local commit and leaves them to
    /// `positions_publish_interval`. Up to 1 s.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_consumer_offset_upload_interval")]
    #[config_env(leaf)]
    pub consumer_offset_upload_interval: IggyDuration,
    /// How often a node lists the consumer group positions that each peer
    /// publishes, and takes those that win over its own.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_positions_poll_interval")]
    #[config_env(leaf)]
    pub positions_poll_interval: IggyDuration,
    /// How long, after a node starts, the group polls of its geo and shadow
    /// partitions wait for the consumer group positions of every peer.
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_positions_resume_timeout")]
    #[config_env(leaf)]
    pub positions_resume_timeout: IggyDuration,
}

impl Default for GeoConfig {
    fn default() -> Self {
        Self {
            origin: String::new(),
            peers: Vec::new(),
            tombstone_grace: default_tombstone_grace(),
            recover_cluster_uid: String::new(),
            recover_unproven: default_recover_unproven(),
            manifest_poll_interval: default_manifest_poll_interval(),
            positions_publish_interval: default_positions_publish_interval(),
            consumer_offset_upload_interval: default_consumer_offset_upload_interval(),
            positions_poll_interval: default_positions_poll_interval(),
            positions_resume_timeout: default_positions_resume_timeout(),
        }
    }
}

impl GeoConfig {
    /// The `cluster_uid` that [`Self::recover_cluster_uid`] names, `None`
    /// when it is empty or not 32 lowercase hex digits.
    #[must_use]
    pub fn recovered_cluster_uid(&self) -> Option<u128> {
        parse_cluster_uid(&self.recover_cluster_uid)
    }
}

/// One peer: its origin name and how to read its store.
#[derive(Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct GeoPeerConfig {
    /// The peer's own `geo.origin`.
    #[serde(default)]
    pub origin: String,
    /// `fs` or `s3`.
    #[serde(default)]
    #[config_env(leaf)]
    pub backend: TieredStorageBackend,
    /// The peer's `tiered_storage.prefix`.
    #[serde(default = "default_prefix")]
    pub prefix: String,
    /// The directory that holds the peer's objects, for the `fs` backend.
    #[serde(default)]
    pub path: String,
    /// The peer's bucket, for the `s3` backend.
    #[serde(default)]
    pub bucket: String,
    /// The URL of the peer's `s3` store, empty for AWS.
    #[serde(default)]
    pub endpoint: String,
    /// The region of the peer's `s3` store.
    #[serde(default)]
    pub region: String,
    /// Where the bucket goes in the request URLs of the `s3` backend.
    #[serde(default)]
    #[config_env(leaf)]
    pub url_style: S3UrlStyle,
    /// The access key ID of the static `s3` credentials, which an `s3` peer
    /// needs.
    // skip_serializing keeps credentials out of the runtime
    // `current_config.toml`, as for the tiered storage credentials.
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub access_key_id: String,
    /// The secret access key of the static `s3` credentials.
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub secret_access_key: String,
}

impl Default for GeoPeerConfig {
    fn default() -> Self {
        Self {
            origin: String::new(),
            backend: TieredStorageBackend::default(),
            prefix: default_prefix(),
            path: String::new(),
            bucket: String::new(),
            endpoint: String::new(),
            region: String::new(),
            url_style: S3UrlStyle::default(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
        }
    }
}

/// Prints what [`Display`] prints, so credentials never show.
impl Debug for GeoConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

/// Prints what [`Display`] prints, so credentials never show.
impl Debug for GeoPeerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for GeoConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{ origin: {}, peers: [", self.origin)?;
        for (index, peer) in self.peers.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            Display::fmt(peer, f)?;
        }
        write!(
            f,
            "], tombstone_grace: {}, manifest_poll_interval: {}, positions_publish_interval: {}, \
             consumer_offset_upload_interval: {}, positions_poll_interval: {}, \
             positions_resume_timeout: {}, recover_cluster_uid: {}, recover_unproven: {} }}",
            self.tombstone_grace,
            self.manifest_poll_interval,
            self.positions_publish_interval,
            self.consumer_offset_upload_interval,
            self.positions_poll_interval,
            self.positions_resume_timeout,
            self.recover_cluster_uid,
            self.recover_unproven
        )
    }
}

/// Prints whether credentials are set, never their values.
impl Display for GeoPeerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ origin: {}, backend: {}, prefix: {}, path: {}, bucket: {}, endpoint: {}, \
             region: {}, url_style: {}, credentials: {} }}",
            self.origin,
            self.backend,
            self.prefix,
            self.path,
            self.bucket,
            self.endpoint,
            self.region,
            self.url_style,
            self.credentials(),
        )
    }
}

impl Validatable<ConfigurationError> for GeoConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        let refuse = |reason: &str| {
            eprintln!("{COMPONENT} geo.{reason}");
            Err(ConfigurationError::InvalidConfigurationValue)
        };
        if !self.origin.is_empty()
            && let Err(reason) = check_origin(&self.origin)
        {
            return refuse(&format!("origin {:?} {reason}", self.origin));
        }
        if self.tombstone_grace.is_zero() {
            return refuse("tombstone_grace must be longer than zero");
        }
        if !self.recover_cluster_uid.is_empty() {
            if self.recovered_cluster_uid().is_none() {
                return refuse(&format!(
                    "recover_cluster_uid {:?} must be 32 lowercase hex digits",
                    self.recover_cluster_uid
                ));
            }
            if self.origin.is_empty() {
                return refuse("origin must be set when recover_cluster_uid is set");
            }
        }
        if self.manifest_poll_interval.is_zero() {
            return refuse("manifest_poll_interval must be longer than zero");
        }
        if self.positions_publish_interval.is_zero() {
            return refuse("positions_publish_interval must be longer than zero");
        }
        if self.positions_poll_interval.is_zero() {
            return refuse("positions_poll_interval must be longer than zero");
        }
        if self.consumer_offset_upload_interval.get_duration() > CONSUMER_OFFSET_UPLOAD_INTERVAL_MAX
        {
            return refuse(&format!(
                "consumer_offset_upload_interval must be at most {}",
                IggyDuration::new(CONSUMER_OFFSET_UPLOAD_INTERVAL_MAX)
            ));
        }
        if self.peers.is_empty() {
            return Ok(());
        }
        if self.origin.is_empty() {
            return refuse("origin must be set when peers are configured");
        }
        if self.peers.len() > MAX_PEERS {
            return refuse(&format!(
                "peers holds {} entries, more than {MAX_PEERS}",
                self.peers.len()
            ));
        }
        let mut origins = vec![self.origin.as_str()];
        let mut roots = Vec::with_capacity(self.peers.len());
        for (index, peer) in self.peers.iter().enumerate() {
            if let Some(fault) = peer.fault() {
                return refuse(&format!("peers[{index}].{fault}"));
            }
            if origins.contains(&peer.origin.as_str()) {
                return refuse(&format!(
                    "peers[{index}].origin {:?} is the origin of this cluster or of another peer",
                    peer.origin
                ));
            }
            origins.push(&peer.origin);
            let root = peer.root();
            if roots.contains(&root) {
                return refuse(&format!(
                    "peers[{index}] reads the same store and prefix as another peer"
                ));
            }
            roots.push(root);
        }
        Ok(())
    }
}

impl GeoPeerConfig {
    /// What the printed configuration shows for the credentials.
    fn credentials(&self) -> &'static str {
        if self.access_key_id.is_empty() && self.secret_access_key.is_empty() {
            "none"
        } else {
            "[redacted]"
        }
    }

    /// Why this peer can never be read, or `None`.
    fn fault(&self) -> Option<String> {
        if let Err(reason) = check_origin(&self.origin) {
            return Some(format!("origin {:?} {reason}", self.origin));
        }
        let missing = match self.backend {
            TieredStorageBackend::None => return Some("backend must be fs or s3".to_owned()),
            TieredStorageBackend::Fs if self.path.trim().is_empty() => "path",
            TieredStorageBackend::S3 if self.bucket.trim().is_empty() => "bucket",
            TieredStorageBackend::S3 if self.region.trim().is_empty() => "region",
            TieredStorageBackend::S3 if self.access_key_id.is_empty() => "access_key_id",
            TieredStorageBackend::S3 if self.secret_access_key.is_empty() => "secret_access_key",
            TieredStorageBackend::Fs | TieredStorageBackend::S3 => {
                if self.prefix.is_empty() {
                    return Some("prefix must not be empty".to_owned());
                }
                return None;
            }
        };
        Some(format!(
            "{missing} must be set for the {} backend",
            self.backend
        ))
    }

    /// What tells two peer roots apart: the backend, where its objects are,
    /// and the prefix. An empty `s3` endpoint is AWS in the region.
    fn root(&self) -> (TieredStorageBackend, String, &str) {
        let location = match self.backend {
            TieredStorageBackend::S3 if self.endpoint.trim().is_empty() => {
                format!("{}/{}", self.region.trim(), self.bucket.trim())
            }
            TieredStorageBackend::S3 => {
                format!("{}/{}", self.endpoint.trim(), self.bucket.trim())
            }
            TieredStorageBackend::Fs | TieredStorageBackend::None => self.path.trim().to_owned(),
        };
        (self.backend, location, &self.prefix)
    }
}

/// Whether `origin` is a valid origin name: 1 to [`MAX_ORIGIN_LEN`] bytes of
/// lowercase ASCII letters, digits and `-`, starting with a letter and not
/// ending with `-`. It holds no `_`, so `__shadow_{origin}_{stream}` splits
/// back at the first `_` after the marker.
/// Why `origin` cannot name a cluster among the datacenters: a lowercase
/// letter, then lowercase letters, digits and '-', at most
/// [`MAX_ORIGIN_LEN`] bytes, not ending with '-'.
///
/// # Errors
/// The reason, as a phrase that follows the name.
pub fn check_origin(origin: &str) -> Result<(), &'static str> {
    // The reasons below spell out the limit.
    const _: () = assert!(MAX_ORIGIN_LEN == 32);
    if origin.is_empty() {
        Err("must not be empty")
    } else if origin.len() > MAX_ORIGIN_LEN {
        Err("is longer than 32 bytes")
    } else if !origin.starts_with(|c: char| c.is_ascii_lowercase()) {
        Err("must start with a lowercase letter")
    } else if !origin
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        Err("holds a character other than a lowercase letter, a digit or '-'")
    } else if origin.ends_with('-') {
        Err("must not end with '-'")
    } else {
        Ok(())
    }
}

/// 32 lowercase hex digits, as the store identity object writes a
/// `cluster_uid`.
fn parse_cluster_uid(text: &str) -> Option<u128> {
    let hex = |byte: u8| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte);
    (text.len() == 32 && text.bytes().all(hex))
        .then(|| u128::from_str_radix(text, 16).ok())
        .flatten()
}

const fn default_recover_unproven() -> bool {
    true
}

fn default_prefix() -> String {
    "iggy".to_owned()
}

/// A day: a peer that is down for less still sees the tombstone.
fn default_tombstone_grace() -> IggyDuration {
    IggyDuration::new_from_secs(24 * 3600)
}

/// Five seconds: a shadow partition stays a few seconds behind its peer, and
/// a listing of each peer stream every few seconds costs little.
fn default_manifest_poll_interval() -> IggyDuration {
    IggyDuration::new_from_secs(5)
}

/// A second: a group that moves to another datacenter re-reads about this
/// much, and an object of a few groups each second costs little.
fn default_positions_publish_interval() -> IggyDuration {
    IggyDuration::new_from_secs(1)
}

/// 50 ms: a commit that finds no store running is stored at once, so the
/// interval spaces the puts of a busy log, each of which rewrites the
/// node's whole object of that log. Five times the fresh upload interval,
/// as an object of positions carries no messages: at most 20 puts a second
/// for each log a node leads, while it takes commits.
fn default_consumer_offset_upload_interval() -> IggyDuration {
    IggyDuration::new(Duration::from_millis(50))
}

/// A second, as the publishing: a listing of each peer's positions a second.
fn default_positions_poll_interval() -> IggyDuration {
    IggyDuration::new_from_secs(1)
}

/// Ten seconds: a few listings of each peer, and a short wait when a peer
/// does not answer.
fn default_positions_resume_timeout() -> IggyDuration {
    IggyDuration::new_from_secs(10)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3_peer(origin: &str) -> GeoPeerConfig {
        GeoPeerConfig {
            origin: origin.to_owned(),
            backend: TieredStorageBackend::S3,
            bucket: format!("iggy-{origin}"),
            region: "us-east-1".to_owned(),
            access_key_id: "AKIDPEEREXAMPLE".to_owned(),
            secret_access_key: "peerSecretKeyExample".to_owned(),
            ..GeoPeerConfig::default()
        }
    }

    fn fs_peer(origin: &str) -> GeoPeerConfig {
        GeoPeerConfig {
            origin: origin.to_owned(),
            backend: TieredStorageBackend::Fs,
            path: format!("/mnt/{origin}"),
            ..GeoPeerConfig::default()
        }
    }

    fn with_peers(peers: Vec<GeoPeerConfig>) -> GeoConfig {
        GeoConfig {
            origin: "dc-a".to_owned(),
            peers,
            ..GeoConfig::default()
        }
    }

    #[test]
    fn given_no_section_when_validating_should_accept() {
        assert!(GeoConfig::default().validate().is_ok());
    }

    #[test]
    fn given_no_tombstone_grace_when_loading_should_keep_tombstones_for_a_day() {
        let config: GeoConfig = serde_json::from_str(r#"{"origin":"dc-a"}"#).unwrap();
        let hour: GeoConfig =
            serde_json::from_str(r#"{"origin":"dc-a","tombstone_grace":"1 h"}"#).unwrap();

        assert_eq!(hour.tombstone_grace.get_duration().as_secs(), 3600);
        assert_eq!(config.tombstone_grace.get_duration().as_secs(), 24 * 3600);
        assert_eq!(GeoConfig::default().tombstone_grace, config.tombstone_grace);
        assert!(
            config
                .to_string()
                .contains("tombstone_grace: 1day, manifest_poll_interval: 5s,"),
            "{config}"
        );
    }

    #[test]
    fn given_no_positions_keys_when_loading_should_publish_and_poll_each_second() {
        let config: GeoConfig = serde_json::from_str(r#"{"origin":"dc-a"}"#).unwrap();
        let tuned: GeoConfig = serde_json::from_str(
            r#"{"origin":"dc-a","positions_publish_interval":"250 ms","positions_poll_interval":"2 s","positions_resume_timeout":"1 min"}"#,
        )
        .unwrap();

        assert_eq!(
            config.positions_publish_interval.get_duration().as_secs(),
            1
        );
        assert_eq!(config.positions_poll_interval.get_duration().as_secs(), 1);
        assert_eq!(config.positions_resume_timeout.get_duration().as_secs(), 10);
        assert_eq!(
            tuned.positions_publish_interval.get_duration().as_millis(),
            250
        );
        assert_eq!(tuned.positions_poll_interval.get_duration().as_secs(), 2);
        assert_eq!(tuned.positions_resume_timeout.get_duration().as_secs(), 60);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn given_a_zero_positions_interval_when_validating_should_refuse() {
        for key in ["positions_publish_interval", "positions_poll_interval"] {
            let config: GeoConfig =
                serde_json::from_str(&format!(r#"{{"origin":"dc-a","{key}":"0"}}"#)).unwrap();
            assert!(config.validate().is_err(), "{key}");
        }
    }

    #[test]
    fn given_no_offset_upload_interval_when_loading_should_store_the_positions_of_manual_commits_every_50_ms()
     {
        let config: GeoConfig = serde_json::from_str(r#"{"origin":"dc-a"}"#).unwrap();
        let tuned: GeoConfig =
            serde_json::from_str(r#"{"origin":"dc-a","consumer_offset_upload_interval":"5 ms"}"#)
                .unwrap();

        assert_eq!(
            config
                .consumer_offset_upload_interval
                .get_duration()
                .as_millis(),
            50
        );
        assert_eq!(
            tuned
                .consumer_offset_upload_interval
                .get_duration()
                .as_millis(),
            5
        );
        assert!(config.validate().is_ok());
        assert!(tuned.validate().is_ok());
    }

    #[test]
    fn given_a_disabled_offset_upload_interval_when_validating_should_accept_it_as_zero() {
        for value in ["0", "disabled"] {
            let config: GeoConfig = serde_json::from_str(&format!(
                r#"{{"origin":"dc-a","consumer_offset_upload_interval":"{value}"}}"#
            ))
            .unwrap();

            assert!(config.consumer_offset_upload_interval.is_zero(), "{value}");
            assert!(config.validate().is_ok(), "{value}");
        }
    }

    #[test]
    fn given_an_offset_upload_interval_past_a_second_when_validating_should_refuse() {
        let config: GeoConfig = serde_json::from_str(
            r#"{"origin":"dc-a","consumer_offset_upload_interval":"1001 ms"}"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_no_recovery_keys_when_loading_should_take_nothing_back() {
        let config: GeoConfig = serde_json::from_str(r#"{"origin":"dc-a"}"#).unwrap();

        assert_eq!(config.recovered_cluster_uid(), None);
        assert!(config.recover_unproven);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn given_a_recover_cluster_uid_when_validating_should_take_only_32_lowercase_hex_digits() {
        let uid = "0123456789abcdef0011223344556677";
        let config = |recover_cluster_uid: &str| GeoConfig {
            origin: "dc-a".to_owned(),
            recover_cluster_uid: recover_cluster_uid.to_owned(),
            ..GeoConfig::default()
        };

        assert_eq!(
            config(uid).recovered_cluster_uid(),
            Some(0x0123_4567_89ab_cdef_0011_2233_4455_6677)
        );
        assert!(config(uid).validate().is_ok());
        for refused in [
            "0123456789ABCDEF0011223344556677",
            "123456789abcdef0011223344556677",
            "+123456789abcdef0011223344556677",
            "0123456789abcdef00112233445566770",
        ] {
            assert!(config(refused).validate().is_err(), "{refused}");
        }
        let without_origin = GeoConfig {
            origin: String::new(),
            ..config(uid)
        };
        assert!(without_origin.validate().is_err());
    }

    #[test]
    fn given_a_zero_tombstone_grace_when_validating_should_refuse() {
        let config = GeoConfig {
            tombstone_grace: IggyDuration::new_from_secs(0),
            ..GeoConfig::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_an_origin_without_peers_when_validating_should_accept() {
        let config = GeoConfig {
            origin: "dc-a".to_owned(),
            ..GeoConfig::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn given_two_peers_when_validating_should_accept() {
        assert!(
            with_peers(vec![s3_peer("dc-b"), fs_peer("dc-c")])
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn given_origin_names_when_checking_should_accept_only_the_shadow_safe_ones() {
        for valid in [
            "a",
            "dc-a",
            "eu-central-1",
            "x1",
            &"a".repeat(MAX_ORIGIN_LEN),
        ] {
            assert_eq!(check_origin(valid), Ok(()), "{valid:?}");
        }
        for invalid in [
            "",
            "dc_a",
            "DC-A",
            "1dc",
            "-dc",
            "dc-",
            "dc a",
            "dc.a",
            "dć",
            &"a".repeat(MAX_ORIGIN_LEN + 1),
        ] {
            assert!(check_origin(invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn given_an_invalid_origin_when_validating_should_refuse() {
        let config = GeoConfig {
            origin: "dc_a".to_owned(),
            ..GeoConfig::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_peers_without_an_origin_when_validating_should_refuse() {
        let config = GeoConfig {
            peers: vec![s3_peer("dc-b")],
            ..GeoConfig::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_a_peer_that_cannot_be_read_when_validating_should_refuse() {
        let peers = [
            GeoPeerConfig {
                origin: "dc_b".to_owned(),
                ..s3_peer("dc-b")
            },
            GeoPeerConfig {
                backend: TieredStorageBackend::None,
                ..s3_peer("dc-b")
            },
            GeoPeerConfig {
                path: " ".to_owned(),
                ..fs_peer("dc-b")
            },
            GeoPeerConfig {
                bucket: String::new(),
                ..s3_peer("dc-b")
            },
            GeoPeerConfig {
                region: String::new(),
                ..s3_peer("dc-b")
            },
            GeoPeerConfig {
                access_key_id: String::new(),
                ..s3_peer("dc-b")
            },
            GeoPeerConfig {
                secret_access_key: String::new(),
                ..s3_peer("dc-b")
            },
            GeoPeerConfig {
                prefix: String::new(),
                ..fs_peer("dc-b")
            },
        ];

        for peer in peers {
            let config = with_peers(vec![peer]);
            assert!(config.validate().is_err(), "{config}");
        }
    }

    #[test]
    fn given_a_repeated_origin_when_validating_should_refuse() {
        let own = with_peers(vec![s3_peer("dc-a")]);
        let twice = with_peers(vec![s3_peer("dc-b"), fs_peer("dc-b")]);

        assert!(own.validate().is_err());
        assert!(twice.validate().is_err());
    }

    #[test]
    fn given_two_peers_on_one_root_when_validating_should_refuse() {
        let same_bucket = with_peers(vec![
            s3_peer("dc-b"),
            GeoPeerConfig {
                bucket: "iggy-dc-b".to_owned(),
                ..s3_peer("dc-c")
            },
        ]);
        let other_prefix = with_peers(vec![
            s3_peer("dc-b"),
            GeoPeerConfig {
                bucket: "iggy-dc-b".to_owned(),
                prefix: "iggy/c".to_owned(),
                ..s3_peer("dc-c")
            },
        ]);

        assert!(same_bucket.validate().is_err());
        assert!(other_prefix.validate().is_ok());
    }

    #[test]
    fn given_more_peers_than_the_cap_when_validating_should_refuse() {
        let peers = (0..=MAX_PEERS)
            .map(|index| fs_peer(&format!("dc-{index}")))
            .collect::<Vec<_>>();
        let at_cap = with_peers(peers[..MAX_PEERS].to_vec());

        assert!(at_cap.validate().is_ok());
        assert!(with_peers(peers).validate().is_err());
    }

    #[test]
    fn given_credentials_when_printing_or_serializing_should_never_show_them() {
        let config = with_peers(vec![s3_peer("dc-b"), fs_peer("dc-c")]);

        let printed = [
            config.to_string(),
            format!("{config:?}"),
            format!("{:?}", config.peers[0]),
            serde_json::to_string(&config).unwrap(),
        ];

        for text in printed {
            assert!(!text.contains("AKIDPEEREXAMPLE"), "{text}");
            assert!(!text.contains("peerSecretKeyExample"), "{text}");
        }
        let text = config.to_string();
        assert!(text.contains("origin: dc-b"), "{text}");
        assert!(text.contains("credentials: [redacted]"), "{text}");
        assert!(text.contains("credentials: none"), "{text}");
    }
}
