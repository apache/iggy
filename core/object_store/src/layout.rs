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
use crate::store::{MAX_KEY_LEN, validate_key};

const META: &str = "meta";
const MANIFEST: &str = "manifest.bin";
/// Decimal digits of `u64::MAX`: the most that an id takes, and the width of
/// a zero-padded offset.
const U64_DIGITS: usize = 20;
/// The longest part of a key after the root: the `.index` key of a segment
/// whose four ids and start offset are all `u64::MAX`.
const LONGEST_SUFFIX_LEN: usize = 5 * (1 + U64_DIGITS) + 1 + SegmentObject::Index.extension().len();
/// The longest root that leaves room for every key under it.
const MAX_ROOT_LEN: usize = MAX_KEY_LEN - LONGEST_SUFFIX_LEN;

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

/// Builds and parses the keys of tiered storage under the bucket prefix from
/// configuration, its root:
///
/// ```text
/// {root}/{stream_id}/{topic_id}/{partition_id}/{created_revision}/{start_offset:020}.log
/// {root}/{stream_id}/{topic_id}/{partition_id}/{created_revision}/{start_offset:020}.index
/// {root}/meta/{stream_id}/{topic_id}/{partition_id}/{created_revision}/manifest.bin
/// ```
///
/// The zero-padded offset matches the segment's file name on disk, so a
/// listing returns the segments of an incarnation in offset order. Ids are
/// decimal and never `meta`, so no segment key falls under `{root}/meta/`.
/// The cluster's identity is not part of any key: the store identity object
/// claims the root for one cluster.
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
    /// root longer than 913 bytes, because the longest key under it would
    /// then break [`MAX_KEY_LEN`].
    pub fn new(root: impl Into<String>) -> Result<Self, ObjectStoreError> {
        // The docs and the reason below spell out this limit.
        const _: () = assert!(MAX_ROOT_LEN == 913);
        let root = root.into();
        validate_key(&root)?;
        if root.len() > MAX_ROOT_LEN {
            return Err(ObjectStoreError::InvalidKey {
                key: root,
                reason: "is longer than 913 bytes, the most that a root can take",
            });
        }
        Ok(Self { root })
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

    #[test]
    fn given_invalid_root_when_building_layout_should_refuse() {
        for root in ["", "iggy/", "/iggy", "a//b", ".iggy"] {
            assert!(KeyLayout::new(root).is_err(), "{root:?}");
        }
        assert_eq!(KeyLayout::new("iggy").unwrap().root(), "iggy");
    }

    #[test]
    fn given_longest_root_when_building_widest_keys_should_keep_them_valid() {
        let root = format!("{}r", "r/".repeat(MAX_ROOT_LEN / 2));
        assert_eq!(root.len(), MAX_ROOT_LEN);
        let layout = KeyLayout::new(root).unwrap();
        let widest = ObjectPrefix {
            stream_id: u64::MAX,
            topic_id: u64::MAX,
            partition_id: u64::MAX,
            created_revision: u64::MAX,
        };

        let index = layout.segment_key(&SegmentKey {
            prefix: widest,
            start_offset: u64::MAX,
            object: SegmentObject::Index,
        });
        let manifest = layout.manifest_key(&widest);

        assert_eq!(index.len(), MAX_KEY_LEN);
        assert!(validate_key(&index).is_ok());
        assert!(validate_key(&manifest).is_ok());
    }

    #[test]
    fn given_root_past_longest_when_building_layout_should_refuse() {
        let root = format!("{}rr", "r/".repeat(MAX_ROOT_LEN / 2));
        assert_eq!(root.len(), MAX_ROOT_LEN + 1);

        assert!(matches!(
            KeyLayout::new(root),
            Err(ObjectStoreError::InvalidKey { .. })
        ));
    }
}
