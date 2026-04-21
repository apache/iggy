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

use std::cmp::Ordering;

/// Recency key derived from a semver-like gitref such as `0.7.0` or
/// `0.7.0-edge.1`. Larger = more recent.
///
/// Ordering rules (differ from strict semver because suffixes in this
/// project denote post-release nightlies, not pre-release candidates):
/// - Compare `(major, minor, patch)` first.
/// - On equal base, a suffixed build (`-edge.N`, `-dev.N`, ...) ranks
///   ABOVE the plain release, because nightlies ship AFTER the tag.
/// - Two suffixed builds compare by suffix build number, then tag name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SemverRecency {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub suffix: Option<SemverSuffix>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SemverSuffix {
    pub tag: String,
    pub number: u32,
}

impl Ord for SemverRecency {
    fn cmp(&self, other: &Self) -> Ordering {
        self.major
            .cmp(&other.major)
            .then(self.minor.cmp(&other.minor))
            .then(self.patch.cmp(&other.patch))
            .then(cmp_suffix(self.suffix.as_ref(), other.suffix.as_ref()))
    }
}

impl PartialOrd for SemverRecency {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Parse a gitref into a recency key. Returns `None` for plain commit
/// hashes or anything that doesn't match `X.Y.Z` / `X.Y.Z-tag[.N]`.
pub fn parse_semver_recency(gitref: &str) -> Option<SemverRecency> {
    let raw = gitref.trim().trim_start_matches('v');
    let (base, suffix_str) = match raw.split_once('-') {
        Some((base, rest)) => (base, Some(rest)),
        None => (raw, None),
    };
    let mut parts = base.split('.');
    let major = parts.next()?.parse::<u32>().ok()?;
    let minor = parts.next()?.parse::<u32>().ok()?;
    let patch = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let suffix = suffix_str.map(parse_suffix);
    Some(SemverRecency {
        major,
        minor,
        patch,
        suffix,
    })
}

fn parse_suffix(raw: &str) -> SemverSuffix {
    match raw.rsplit_once('.') {
        Some((tag, number)) if !tag.is_empty() => SemverSuffix {
            tag: tag.to_string(),
            number: number.parse().unwrap_or(0),
        },
        _ => SemverSuffix {
            tag: raw.to_string(),
            number: 0,
        },
    }
}

fn cmp_suffix(left: Option<&SemverSuffix>, right: Option<&SemverSuffix>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (Some(l), Some(r)) => l.number.cmp(&r.number).then_with(|| l.tag.cmp(&r.tag)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_plain_commit_hash_when_parsing_should_return_none() {
        assert!(parse_semver_recency("abc123").is_none());
        assert!(parse_semver_recency("main").is_none());
        assert!(parse_semver_recency("0.7").is_none());
        assert!(parse_semver_recency("0.7.0.1").is_none());
    }

    #[test]
    fn given_plain_semver_when_parsing_should_yield_base_only() {
        let key = parse_semver_recency("0.7.0").expect("parse");
        assert_eq!(key.major, 0);
        assert_eq!(key.minor, 7);
        assert_eq!(key.patch, 0);
        assert!(key.suffix.is_none());
    }

    #[test]
    fn given_suffixed_version_when_parsing_should_capture_tag_and_number() {
        let key = parse_semver_recency("0.7.0-edge.1").expect("parse");
        let suffix = key.suffix.as_ref().expect("suffix");
        assert_eq!(suffix.tag, "edge");
        assert_eq!(suffix.number, 1);
    }

    #[test]
    fn given_leading_v_when_parsing_should_still_match() {
        let key = parse_semver_recency("v0.7.0-dev.4").expect("parse");
        assert_eq!(key.major, 0);
        assert_eq!(key.suffix.as_ref().expect("suffix").tag.as_str(), "dev");
    }

    #[test]
    fn given_suffixed_and_plain_same_base_when_comparing_should_rank_suffixed_higher() {
        let edge = parse_semver_recency("0.7.0-edge.1").expect("parse");
        let plain = parse_semver_recency("0.7.0").expect("parse");
        assert!(edge > plain);
    }

    #[test]
    fn given_bigger_base_when_comparing_should_win_regardless_of_suffix() {
        let plain_newer = parse_semver_recency("0.7.0").expect("parse");
        let edge_older = parse_semver_recency("0.6.9-dev.4").expect("parse");
        assert!(plain_newer > edge_older);
    }

    #[test]
    fn given_same_suffix_tag_different_numbers_when_comparing_should_prefer_higher_number() {
        let first = parse_semver_recency("0.7.0-edge.1").expect("parse");
        let fifth = parse_semver_recency("0.7.0-edge.5").expect("parse");
        assert!(fifth > first);
    }

    #[test]
    fn given_user_example_sequence_when_sorting_should_order_newest_first() {
        let mut entries = [
            parse_semver_recency("0.6.9-dev.4").expect("parse"),
            parse_semver_recency("0.7.0-edge.1").expect("parse"),
            parse_semver_recency("0.7.0").expect("parse"),
        ];
        entries.sort_by(|left, right| right.cmp(left));
        assert_eq!(
            entries[0],
            parse_semver_recency("0.7.0-edge.1").expect("parse")
        );
        assert_eq!(entries[1], parse_semver_recency("0.7.0").expect("parse"));
        assert_eq!(
            entries[2],
            parse_semver_recency("0.6.9-dev.4").expect("parse")
        );
    }
}
