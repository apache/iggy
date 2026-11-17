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

use crate::error::ObjectStoreError;
use crate::fresh::FreshObjectId;
use crate::store::{MAX_KEY_LEN, MAX_KEY_SEGMENTS, validate_key};

const META: &str = "meta";
const MANIFEST: &str = "manifest.bin";
const IDENTITY: &str = "identity";
const FRESH: &str = "fresh";
/// Hexadecimal digits of a fresh object's id.
const FRESH_ID_DIGITS: usize = 32;
/// Hexadecimal digits of the replica in a fresh object's key.
const FRESH_REPLICA_DIGITS: usize = 2;
const TOPICS: &str = "topics";
/// Decimal digits of `u64::MAX`: the most that an id takes, and the width of
/// a zero-padded offset.
const U64_DIGITS: usize = 20;
/// The longest part of a key after the root: the `.index` key of a segment
/// whose four ids and start offset are all `u64::MAX`.
const LONGEST_SUFFIX_LEN: usize = 5 * (1 + U64_DIGITS) + 1 + SegmentObject::Index.extension().len();
/// The longest root that leaves room for every key under it.
const MAX_ROOT_LEN: usize = MAX_KEY_LEN - LONGEST_SUFFIX_LEN;
/// The most segments that a key adds after the root: `meta`, four ids and
/// `manifest.bin`.
const MOST_SUFFIX_SEGMENTS: usize = 6;
/// The most segments of a root that leaves room for every key under it.
const MAX_ROOT_SEGMENTS: usize = MAX_KEY_SEGMENTS - MOST_SUFFIX_SEGMENTS;

/// Where the objects of one partition incarnation live.
///
/// A partition records its prefix once, in its chain file, and builds every
/// key from that record. It never builds a key from its local ids at read
/// time, because a follower or a restored partition reads objects that other
/// ids wrote. Stream, topic and partition ids are reused after a delete, so
/// `created_revision`, which is unique across the cluster, tells the
/// incarnations apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectPrefix {
    pub stream_id: u64,
    pub topic_id: u64,
    pub partition_id: u64,
    pub created_revision: u64,
}

/// One of the two objects of a tiered segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SegmentObject {
    /// The segment's `.log`, byte for byte.
    Log,
    /// The uploader's sparse `.index`.
    Index,
}

impl SegmentObject {
    pub const ALL: [Self; 2] = [Self::Log, Self::Index];

    #[must_use]
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Log => "log",
            Self::Index => "index",
        }
    }
}

/// The parts of a segment object's key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SegmentKey {
    pub prefix: ObjectPrefix,
    pub start_offset: u64,
    pub object: SegmentObject,
}

/// The parts of a geo topic object's key: the topic incarnation it describes.
///
/// Every partition of a geo topic comes with the topic, so they all share
/// its `created_revision`, and their [`ObjectPrefix`]es follow from this key
/// and the partition count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicKey {
    pub stream_id: u64,
    pub topic_id: u64,
    pub created_revision: u64,
}

impl TopicKey {
    /// Where the objects of the topic's partition `partition_id` live.
    #[must_use]
    pub const fn partition_prefix(&self, partition_id: u64) -> ObjectPrefix {
        ObjectPrefix {
            stream_id: self.stream_id,
            topic_id: self.topic_id,
            partition_id,
            created_revision: self.created_revision,
        }
    }
}

/// Builds and parses the keys of tiered storage under the bucket prefix from
/// configuration, its root:
///
/// ```text
/// {root}/{stream_id}/{topic_id}/{partition_id}/{created_revision}/{start_offset:020}.log
/// {root}/{stream_id}/{topic_id}/{partition_id}/{created_revision}/{start_offset:020}.index
/// {root}/meta/{stream_id}/{topic_id}/{partition_id}/{created_revision}/manifest.bin
/// {root}/meta/identity
/// {root}/meta/fresh/{replica:02x}/{object_id:032x}
/// {root}/meta/topics/{stream_id}/{topic_id}/{created_revision}
/// ```
///
/// The zero-padded offset matches the segment's file name on disk, so a
/// listing returns the segments of an incarnation in offset order. Ids are
/// decimal and never `meta`, so no segment key falls under `{root}/meta/`.
/// Under `{root}/meta/`, a manifest key goes on with an id, and `identity`,
/// `fresh` and `topics` are not ids, so no two kinds share a key. The
/// cluster's identity is not part of any key: the store identity object
/// under [`Self::identity_key`] claims the root for one cluster.
///
/// One listing of [`Self::topics_prefix`] finds the object of every geo
/// topic under the root, and one listing of [`Self::stream_topics_prefix`]
/// finds those of one stream.
///
/// Roots never nest in the keys of another root, so two clusters can share a
/// bucket under roots such as `iggy` and `iggy/b`. A listing under a root
/// still returns the keys of every longer root, so retire and the orphan
/// sweep delete only the keys that [`Self::parse_segment_key`] accepts and
/// the keys that [`Self::manifest_key`] builds, and the sweep of geo topic
/// objects acts only on the keys that [`Self::parse_topic_key`] accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyLayout {
    root: String,
}

impl KeyLayout {
    /// The layout under `root`.
    ///
    /// # Errors
    /// [`ObjectStoreError::InvalidKey`] when `root` is not a valid key, for
    /// example when it is empty or ends with `/`. The same error comes for a
    /// root longer than 913 bytes or with more than 248 segments, because
    /// the longest key under it would then break [`MAX_KEY_LEN`] or
    /// [`MAX_KEY_SEGMENTS`]. It also comes for a root with a segment after
    /// its first that is all digits or `meta`. The keys under the shorter
    /// root continue with such a segment, so the two roots would share keys.
    pub fn new(root: impl Into<String>) -> Result<Self, ObjectStoreError> {
        // The docs and the reasons below spell out these limits.
        const _: () = assert!(MAX_ROOT_LEN == 913 && MAX_ROOT_SEGMENTS == 248);
        let root = root.into();
        validate_key(&root)?;
        let reason = if root.len() > MAX_ROOT_LEN {
            "is longer than 913 bytes, the most that a root can take"
        } else if root.split('/').count() > MAX_ROOT_SEGMENTS {
            "has more than 248 segments, the most that a root can take"
        } else if root.split('/').skip(1).any(continues_another_root) {
            "has a segment after the first that is all digits or \"meta\", as in the keys of a shorter root"
        } else {
            return Ok(Self { root });
        };
        Err(ObjectStoreError::InvalidKey { key: root, reason })
    }

    #[must_use]
    pub fn root(&self) -> &str {
        &self.root
    }

    /// The listing prefix of every segment object of the incarnation, with a
    /// trailing `/`.
    #[must_use]
    pub fn incarnation_prefix(&self, prefix: &ObjectPrefix) -> String {
        format!(
            "{}/{}/{}/{}/{}/",
            self.root,
            prefix.stream_id,
            prefix.topic_id,
            prefix.partition_id,
            prefix.created_revision
        )
    }

    #[must_use]
    pub fn segment_key(&self, key: &SegmentKey) -> String {
        format!(
            "{}{:0width$}.{}",
            self.incarnation_prefix(&key.prefix),
            key.start_offset,
            key.object.extension(),
            width = U64_DIGITS
        )
    }

    #[must_use]
    pub fn manifest_key(&self, prefix: &ObjectPrefix) -> String {
        format!(
            "{}/{META}/{}/{}/{}/{}/{MANIFEST}",
            self.root,
            prefix.stream_id,
            prefix.topic_id,
            prefix.partition_id,
            prefix.created_revision
        )
    }

    /// The key of the store identity object, which names the cluster that
    /// owns the root. No other root can continue with `meta`, and no id is
    /// `identity`, so no other key of any root is this key.
    #[must_use]
    pub fn identity_key(&self) -> String {
        format!("{}/{META}/{IDENTITY}", self.root)
    }

    /// The key of the fresh object `object_id`: an object that holds the
    /// messages of batches whose partitions commit where they sit in it.
    ///
    /// Fresh objects live under `meta`, which no other root can continue
    /// with. No id of a stream is `fresh`, so no manifest key is under
    /// [`Self::fresh_prefix`].
    ///
    /// The key names the replica that wrote the object before the id, which
    /// names it again. So one listing returns the objects of one replica,
    /// which is what the replica that deletes them asks for.
    #[must_use]
    pub fn fresh_key(&self, object_id: u128) -> String {
        format!(
            "{}{object_id:0width$x}",
            self.fresh_replica_prefix(FreshObjectId::from_raw(object_id).replica()),
            width = FRESH_ID_DIGITS
        )
    }

    /// The listing prefix of the fresh objects that `replica` wrote, with a
    /// trailing `/`.
    #[must_use]
    pub fn fresh_replica_prefix(&self, replica: u8) -> String {
        format!("{}{replica:02x}/", self.fresh_prefix())
    }

    /// The listing prefix of every fresh object of the root, with a trailing
    /// `/`.
    #[must_use]
    pub fn fresh_prefix(&self) -> String {
        format!("{}/{META}/{FRESH}/", self.root)
    }

    /// The id of the fresh object that a listed key names, or `None` when
    /// this layout did not build the key for one. Parsing is strict, so only
    /// a key that [`Self::fresh_key`] builds again byte for byte parses.
    #[must_use]
    pub fn parse_fresh_key(&self, key: &str) -> Option<u128> {
        let name = key.strip_prefix(self.fresh_prefix().as_str())?;
        let id = name.get(FRESH_REPLICA_DIGITS + 1..)?;
        let lowercase_hex = |byte: u8| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte);
        let id = (id.len() == FRESH_ID_DIGITS && id.bytes().all(lowercase_hex))
            .then(|| u128::from_str_radix(id, 16).ok())
            .flatten()?;
        (self.fresh_key(id) == key).then_some(id)
    }

    /// The listing prefix of every geo topic object under the root, with a
    /// trailing `/`.
    #[must_use]
    pub fn topics_prefix(&self) -> String {
        format!("{}/{META}/{TOPICS}/", self.root)
    }

    /// The listing prefix of the geo topic objects of one stream, with a
    /// trailing `/`.
    #[must_use]
    pub fn stream_topics_prefix(&self, stream_id: u64) -> String {
        format!("{}{stream_id}/", self.topics_prefix())
    }

    /// The key of the object that describes a geo topic incarnation to the
    /// clusters that read this store.
    #[must_use]
    pub fn topic_key(&self, key: &TopicKey) -> String {
        format!(
            "{}{}/{}",
            self.stream_topics_prefix(key.stream_id),
            key.topic_id,
            key.created_revision
        )
    }

    /// The geo topic incarnation that a listed key names, or `None` when
    /// this layout did not build the key for a geo topic object. Parsing is
    /// strict, as for [`Self::parse_segment_key`].
    #[must_use]
    pub fn parse_topic_key(&self, key: &str) -> Option<TopicKey> {
        let mut parts = key
            .strip_prefix(self.root.as_str())?
            .strip_prefix('/')?
            .strip_prefix(META)?
            .strip_prefix('/')?
            .strip_prefix(TOPICS)?
            .strip_prefix('/')?
            .split('/');
        let topic = TopicKey {
            stream_id: parse_id(parts.next()?)?,
            topic_id: parse_id(parts.next()?)?,
            created_revision: parse_id(parts.next()?)?,
        };
        parts.next().is_none().then_some(topic)
    }

    /// The segment object that a listed key names, or `None` when this layout
    /// did not build the key for a segment, as for a manifest or a key under
    /// another root. Parsing is strict, so only a key that
    /// [`Self::segment_key`] builds again byte for byte parses.
    #[must_use]
    pub fn parse_segment_key(&self, key: &str) -> Option<SegmentKey> {
        let mut parts = key
            .strip_prefix(self.root.as_str())?
            .strip_prefix('/')?
            .split('/');
        let prefix = ObjectPrefix {
            stream_id: parse_id(parts.next()?)?,
            topic_id: parse_id(parts.next()?)?,
            partition_id: parse_id(parts.next()?)?,
            created_revision: parse_id(parts.next()?)?,
        };
        let (stem, extension) = parts.next()?.split_once('.')?;
        if parts.next().is_some() {
            return None;
        }
        let object = SegmentObject::ALL
            .into_iter()
            .find(|object| object.extension() == extension)?;
        Some(SegmentKey {
            prefix,
            start_offset: parse_offset(stem)?,
            object,
        })
    }
}

/// Whether a root segment can follow a shorter root in its own keys, where
/// only ids and `meta` come.
fn continues_another_root(segment: &str) -> bool {
    segment == META || segment.bytes().all(|byte| byte.is_ascii_digit())
}

/// An id as [`KeyLayout`] writes it: decimal, with no sign and no leading
/// zero.
fn parse_id(part: &str) -> Option<u64> {
    let canonical = part == "0" || part.bytes().next().is_some_and(|first| first != b'0');
    if canonical && part.bytes().all(|byte| byte.is_ascii_digit()) {
        part.parse().ok()
    } else {
        None
    }
}

/// A start offset as [`KeyLayout`] writes it: exactly 20 decimal digits.
fn parse_offset(stem: &str) -> Option<u64> {
    if stem.len() == U64_DIGITS && stem.bytes().all(|byte| byte.is_ascii_digit()) {
        stem.parse().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{MAX_KEY_SEGMENT_LEN, validate_prefix};

    const PREFIX: ObjectPrefix = ObjectPrefix {
        stream_id: 1,
        topic_id: 22,
        partition_id: 0,
        created_revision: 4096,
    };

    fn layout() -> KeyLayout {
        KeyLayout::new("tenant/iggy").unwrap()
    }

    #[test]
    fn given_segment_when_building_keys_should_follow_rfc_layout() {
        let log = SegmentKey {
            prefix: PREFIX,
            start_offset: 1_000,
            object: SegmentObject::Log,
        };
        let index = SegmentKey {
            object: SegmentObject::Index,
            ..log
        };

        assert_eq!(
            layout().segment_key(&log),
            "tenant/iggy/1/22/0/4096/00000000000000001000.log"
        );
        assert_eq!(
            layout().segment_key(&index),
            "tenant/iggy/1/22/0/4096/00000000000000001000.index"
        );
        assert_eq!(
            layout().incarnation_prefix(&PREFIX),
            "tenant/iggy/1/22/0/4096/"
        );
        assert_eq!(
            layout().manifest_key(&PREFIX),
            "tenant/iggy/meta/1/22/0/4096/manifest.bin"
        );
        assert_eq!(layout().identity_key(), "tenant/iggy/meta/identity");
    }

    const TOPIC: TopicKey = TopicKey {
        stream_id: 1,
        topic_id: 22,
        created_revision: 4096,
    };

    #[test]
    fn given_topic_when_building_keys_should_group_them_by_stream() {
        let layout = layout();

        assert_eq!(layout.topics_prefix(), "tenant/iggy/meta/topics/");
        assert_eq!(layout.stream_topics_prefix(1), "tenant/iggy/meta/topics/1/");
        assert_eq!(
            layout.topic_key(&TOPIC),
            "tenant/iggy/meta/topics/1/22/4096"
        );
        assert!(validate_prefix(&layout.topics_prefix()).is_ok());
        assert!(validate_prefix(&layout.stream_topics_prefix(u64::MAX)).is_ok());
        assert!(
            layout
                .topic_key(&TOPIC)
                .starts_with(&layout.stream_topics_prefix(TOPIC.stream_id))
        );
        assert_eq!(
            TOPIC.partition_prefix(3),
            ObjectPrefix {
                partition_id: 3,
                ..PREFIX
            }
        );
    }

    #[test]
    fn given_built_topic_keys_when_parsing_should_round_trip() {
        let layout = layout();
        for created_revision in [0, 1, 4096, u64::MAX] {
            let topic = TopicKey {
                created_revision,
                ..TOPIC
            };
            let key = layout.topic_key(&topic);

            assert!(validate_key(&key).is_ok(), "{key}");
            assert_eq!(layout.parse_topic_key(&key), Some(topic), "{key}");
            assert_eq!(layout.parse_segment_key(&key), None, "{key}");
        }
    }

    #[test]
    fn given_keys_layout_did_not_build_for_a_topic_when_parsing_should_return_none() {
        let layout = layout();
        let segment = layout.segment_key(&SegmentKey {
            prefix: PREFIX,
            start_offset: 1_000,
            object: SegmentObject::Log,
        });
        for key in [
            layout.identity_key().as_str(),
            layout.manifest_key(&PREFIX).as_str(),
            segment.as_str(),
            "tenant/iggy/meta/topics/1/22",
            "tenant/iggy/meta/topics/1/22/4096/",
            "tenant/iggy/meta/topics/1/22/4096/x",
            "tenant/iggy/meta/topics/01/22/4096",
            "tenant/iggy/meta/topics/1/22/+4096",
            "tenant/iggy/meta/topics/1/22/18446744073709551616",
            "tenant/iggy/meta/topic/1/22/4096",
            "tenant/iggy/b/meta/topics/1/22/4096",
            "tenant/iggy2/meta/topics/1/22/4096",
        ] {
            assert_eq!(layout.parse_topic_key(key), None, "{key}");
        }
    }

    #[test]
    fn given_a_fresh_object_id_when_building_its_key_should_pad_it_and_parse_it_back() {
        let layout = layout();
        let key = layout.fresh_key(0x1f);

        assert_eq!(
            key,
            "tenant/iggy/meta/fresh/00/0000000000000000000000000000001f"
        );
        assert!(key.starts_with(&layout.fresh_prefix()));
        let written_by_ten = FreshObjectId::new(10, 2, 3, 4).unwrap().get();
        let key_of_ten = layout.fresh_key(written_by_ten);
        assert!(key_of_ten.starts_with("tenant/iggy/meta/fresh/0a/"));
        assert!(key_of_ten.starts_with(&layout.fresh_replica_prefix(10)));
        assert!(!key_of_ten.starts_with(&layout.fresh_replica_prefix(1)));
        assert_eq!(layout.parse_fresh_key(&key_of_ten), Some(written_by_ten));
        assert!(validate_key(&key).is_ok());
        assert_eq!(layout.parse_fresh_key(&key), Some(0x1f));
        assert_eq!(
            layout.parse_fresh_key(&layout.fresh_key(u128::MAX)),
            Some(u128::MAX)
        );
        assert_eq!(layout.parse_segment_key(&key), None);
    }

    #[test]
    fn given_keys_no_fresh_object_has_when_parsing_as_fresh_should_return_none() {
        let layout = layout();
        let other_root = KeyLayout::new("tenant/other").unwrap();

        for key in [
            layout.identity_key(),
            layout.manifest_key(&PREFIX),
            other_root.fresh_key(1),
            "tenant/iggy/meta/fresh/00/1f".to_owned(),
            "tenant/iggy/meta/fresh/0000000000000000000000000000001f".to_owned(),
            "tenant/iggy/meta/fresh/00/0000000000000000000000000000001F".to_owned(),
            "tenant/iggy/meta/fresh/00/0000000000000000000000000000001f/x".to_owned(),
            "tenant/iggy/meta/fresh/00/+000000000000000000000000000001f".to_owned(),
            // The id says replica 0 wrote it.
            "tenant/iggy/meta/fresh/01/0000000000000000000000000000001f".to_owned(),
            "tenant/iggy/meta/fresh/0A/0000000000000000000000000000001f".to_owned(),
        ] {
            assert_eq!(layout.parse_fresh_key(&key), None, "{key}");
        }
    }

    #[test]
    fn given_identity_key_when_parsing_as_segment_should_return_none() {
        let layout = layout();
        let key = layout.identity_key();

        assert!(validate_key(&key).is_ok(), "{key}");
        assert_eq!(layout.parse_segment_key(&key), None);
        assert_ne!(
            KeyLayout::new("tenant/iggy/b").unwrap().identity_key(),
            key,
            "a longer root has an identity key of its own"
        );
    }

    #[test]
    fn given_built_keys_when_parsing_should_round_trip() {
        for start_offset in [0, 1, 1_000, u64::MAX] {
            for object in SegmentObject::ALL {
                let segment = SegmentKey {
                    prefix: PREFIX,
                    start_offset,
                    object,
                };
                let key = layout().segment_key(&segment);

                assert!(validate_key(&key).is_ok(), "{key}");
                assert_eq!(layout().parse_segment_key(&key), Some(segment), "{key}");
            }
        }
    }

    #[test]
    fn given_keys_layout_did_not_build_when_parsing_should_return_none() {
        let layout = layout();
        for key in [
            "tenant/iggy/meta/1/22/0/4096/manifest.bin",
            "tenant/iggy2/1/22/0/4096/00000000000000001000.log",
            "other/1/22/0/4096/00000000000000001000.log",
            "tenant/iggy/01/22/0/4096/00000000000000001000.log",
            "tenant/iggy/+1/22/0/4096/00000000000000001000.log",
            "tenant/iggy/1/22/00/4096/00000000000000001000.log",
            "tenant/iggy/1/22/0/4096/1000.log",
            "tenant/iggy/1/22/0/4096/000000000000000001000.log",
            "tenant/iggy/1/22/0/4096/99999999999999999999.log",
            "tenant/iggy/1/22/0/4096/00000000000000001000.timeindex",
            "tenant/iggy/1/22/0/4096/00000000000000001000.log.tmp",
            "tenant/iggy/1/22/0/4096/00000000000000001000",
            "tenant/iggy/1/22/0/4096/00000000000000001000.log/x",
            "tenant/iggy/1/22/0/00000000000000001000.log",
        ] {
            assert_eq!(layout.parse_segment_key(key), None, "{key}");
        }
    }

    #[test]
    fn given_offsets_when_building_keys_should_sort_in_offset_order() {
        let offsets = [0, 9, 10, 999, 1_000, u64::MAX];
        let keys: Vec<String> = offsets
            .iter()
            .map(|&start_offset| {
                layout().segment_key(&SegmentKey {
                    prefix: PREFIX,
                    start_offset,
                    object: SegmentObject::Log,
                })
            })
            .collect();

        assert!(keys.is_sorted(), "{keys:?}");
    }

    const WIDEST: ObjectPrefix = ObjectPrefix {
        stream_id: u64::MAX,
        topic_id: u64::MAX,
        partition_id: u64::MAX,
        created_revision: u64::MAX,
    };

    /// A root of `len` bytes in segments of the longest length.
    fn root_of_len(len: usize) -> String {
        let segment = "r".repeat(MAX_KEY_SEGMENT_LEN);
        let mut root = String::new();
        while root.len() < len {
            if !root.is_empty() {
                root.push('/');
            }
            let segment_len = (len - root.len()).min(MAX_KEY_SEGMENT_LEN);
            root.push_str(&segment[..segment_len]);
        }
        root
    }

    fn widest_keys(layout: &KeyLayout) -> [String; 3] {
        let index = layout.segment_key(&SegmentKey {
            prefix: WIDEST,
            start_offset: u64::MAX,
            object: SegmentObject::Index,
        });
        let topic = layout.topic_key(&TopicKey {
            stream_id: u64::MAX,
            topic_id: u64::MAX,
            created_revision: u64::MAX,
        });
        [index, layout.manifest_key(&WIDEST), topic]
    }

    #[test]
    fn given_invalid_root_when_building_layout_should_refuse() {
        for root in ["", "iggy/", "/iggy", "a//b", ".iggy", "iggy."] {
            assert!(KeyLayout::new(root).is_err(), "{root:?}");
        }
        assert_eq!(KeyLayout::new("iggy").unwrap().root(), "iggy");
    }

    #[test]
    fn given_root_that_nests_in_another_when_building_layout_should_refuse() {
        for root in [
            "iggy/1",
            "iggy/meta",
            "iggy/1/x",
            "iggy/x/0",
            "tenant/42/iggy",
        ] {
            assert!(
                matches!(
                    KeyLayout::new(root),
                    Err(ObjectStoreError::InvalidKey { reason, .. }) if reason.contains("all digits")
                ),
                "{root:?}"
            );
        }
        for root in [
            "1",
            "meta",
            "iggy/b",
            "iggy/1a",
            "iggy/meta2",
            "tenant-42/iggy",
        ] {
            assert!(KeyLayout::new(root).is_ok(), "{root:?}");
        }
    }

    #[test]
    fn given_longest_root_when_building_widest_keys_should_keep_them_valid() {
        let root = root_of_len(MAX_ROOT_LEN);
        assert_eq!(root.len(), MAX_ROOT_LEN);
        let layout = KeyLayout::new(root).unwrap();

        let [index, manifest, topic] = widest_keys(&layout);

        assert_eq!(index.len(), MAX_KEY_LEN);
        assert!(validate_key(&index).is_ok());
        assert!(validate_key(&manifest).is_ok());
        assert!(validate_key(&topic).is_ok());
    }

    #[test]
    fn given_root_past_longest_when_building_layout_should_refuse() {
        let root = root_of_len(MAX_ROOT_LEN + 1);

        assert!(matches!(
            KeyLayout::new(root),
            Err(ObjectStoreError::InvalidKey { reason, .. }) if reason.contains("913 bytes")
        ));
    }

    #[test]
    fn given_root_with_most_segments_when_building_widest_keys_should_keep_them_valid() {
        let root = format!("{}r", "r/".repeat(MAX_ROOT_SEGMENTS - 1));
        let layout = KeyLayout::new(root).unwrap();

        let [index, manifest, topic] = widest_keys(&layout);

        assert_eq!(manifest.split('/').count(), MAX_KEY_SEGMENTS);
        assert!(validate_key(&index).is_ok());
        assert!(validate_key(&manifest).is_ok());
        assert!(validate_key(&topic).is_ok());
    }

    #[test]
    fn given_root_past_most_segments_when_building_layout_should_refuse() {
        let root = format!("{}r", "r/".repeat(MAX_ROOT_SEGMENTS));

        assert!(matches!(
            KeyLayout::new(root),
            Err(ObjectStoreError::InvalidKey { reason, .. }) if reason.contains("248 segments")
        ));
    }
}
