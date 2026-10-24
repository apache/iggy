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

use super::{
    MetadataChangeCursor, MetadataChangeGroup, MetadataChangeGroups, MetadataCurrentPageToken,
};
use crate::{IggyError, IggyTimestamp};
use std::collections::BTreeSet;
use std::str::FromStr;

/// Records or items per page when the caller does not say.
pub const DEFAULT_METADATA_CHANGES_LIMIT: u32 = 100;

/// Most records or items one page returns.
pub const MAX_METADATA_CHANGES_LIMIT: u32 = 1000;

const FROM_KEY: &str = "from";
const AFTER_KEY: &str = "after";
const CURRENT_AFTER_KEY: &str = "current_after";
const GROUPS_KEY: &str = "groups";
const SINCE_KEY: &str = "since";
const UNTIL_KEY: &str = "until";
const LIMIT_KEY: &str = "limit";
const FROM_EARLIEST: &str = "earliest";
const FROM_LATEST: &str = "latest";
const FROM_CURRENT: &str = "current";

/// Where a change log query starts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum MetadataChangeStart {
    /// The oldest operation the requested classes still hold completely.
    #[default]
    Earliest,
    /// No records, only a cursor at the newest retained record.
    Latest,
    /// Records after a cursor.
    After(MetadataChangeCursor),
    /// Current state pages: `None` starts a scan, a token continues it.
    Current(Option<MetadataCurrentPageToken>),
}

/// A change log query.
///
/// `groups` empty means every group. `since` and `until` keep records whose
/// prepare timestamp is in the inclusive range, and only apply to `Earliest`
/// and `After`.
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataChangesQuery {
    pub start: MetadataChangeStart,
    pub groups: Vec<MetadataChangeGroup>,
    pub since: Option<IggyTimestamp>,
    pub until: Option<IggyTimestamp>,
    pub limit: u32,
}

impl Default for MetadataChangesQuery {
    fn default() -> Self {
        Self {
            start: MetadataChangeStart::Earliest,
            groups: Vec::new(),
            since: None,
            until: None,
            limit: DEFAULT_METADATA_CHANGES_LIMIT,
        }
    }
}

impl MetadataChangesQuery {
    /// Check the request rules every transport shares and return the
    /// normalized group set.
    ///
    /// # Errors
    ///
    /// `InvalidCommand` when the limit is outside 1 to 1000, `since` is above
    /// `until`, a time bound accompanies `Latest` or `Current`, a cursor is not
    /// canonical, a current scan misses a parent group, or a page token was
    /// issued for other groups.
    pub fn validate(&self) -> Result<MetadataChangeGroups, IggyError> {
        if !(1..=MAX_METADATA_CHANGES_LIMIT).contains(&self.limit) {
            return Err(IggyError::InvalidCommand);
        }
        if let (Some(since), Some(until)) = (self.since, self.until)
            && since.as_micros() > until.as_micros()
        {
            return Err(IggyError::InvalidCommand);
        }
        let groups = MetadataChangeGroups::requested(&self.groups);
        let has_time_bound = self.since.is_some() || self.until.is_some();
        match &self.start {
            MetadataChangeStart::Earliest => {}
            MetadataChangeStart::After(cursor) => {
                if !cursor.is_canonical() {
                    return Err(IggyError::InvalidCommand);
                }
            }
            MetadataChangeStart::Latest => {
                if has_time_bound {
                    return Err(IggyError::InvalidCommand);
                }
            }
            MetadataChangeStart::Current(token) => {
                if has_time_bound || groups.missing_parent().is_some() {
                    return Err(IggyError::InvalidCommand);
                }
                if token.as_ref().is_some_and(|token| token.groups() != groups) {
                    return Err(IggyError::InvalidCommand);
                }
            }
        }
        Ok(groups)
    }

    /// The HTTP query parameters for this query, in a stable order.
    #[must_use]
    pub fn http_query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::with_capacity(5);
        match &self.start {
            MetadataChangeStart::Earliest => pairs.push((FROM_KEY, FROM_EARLIEST.to_owned())),
            MetadataChangeStart::Latest => pairs.push((FROM_KEY, FROM_LATEST.to_owned())),
            MetadataChangeStart::After(cursor) => pairs.push((AFTER_KEY, cursor.to_string())),
            MetadataChangeStart::Current(token) => {
                pairs.push((FROM_KEY, FROM_CURRENT.to_owned()));
                if let Some(token) = token {
                    pairs.push((CURRENT_AFTER_KEY, token.to_string()));
                }
            }
        }
        if !self.groups.is_empty() {
            let names: Vec<&'static str> = MetadataChangeGroups::requested(&self.groups)
                .iter()
                .map(Into::into)
                .collect();
            pairs.push((GROUPS_KEY, names.join(",")));
        }
        if let Some(since) = self.since {
            pairs.push((SINCE_KEY, since.as_micros().to_string()));
        }
        if let Some(until) = self.until {
            pairs.push((UNTIL_KEY, until.as_micros().to_string()));
        }
        pairs.push((LIMIT_KEY, self.limit.to_string()));
        pairs
    }

    /// Parse the HTTP query parameters of `GET /metadata/changes`.
    ///
    /// Exactly one of `from` and `after` is required. Unknown or repeated
    /// keys, an empty group element and malformed values are refused. The
    /// result is not yet validated, call [`Self::validate`].
    ///
    /// # Errors
    ///
    /// `InvalidCommand` for any refused parameter.
    pub fn from_http_query<'a>(
        pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, IggyError> {
        let mut seen = BTreeSet::new();
        let mut from = None;
        let mut after = None;
        let mut current_after = None;
        let mut query = Self::default();
        for (key, value) in pairs {
            if !seen.insert(key) {
                return Err(IggyError::InvalidCommand);
            }
            match key {
                FROM_KEY => from = Some(value),
                AFTER_KEY => after = Some(MetadataChangeCursor::from_str(value)?),
                CURRENT_AFTER_KEY => {
                    current_after = Some(MetadataCurrentPageToken::from_str(value)?)
                }
                GROUPS_KEY => query.groups = parse_groups(value)?,
                SINCE_KEY => query.since = Some(parse_micros(value)?),
                UNTIL_KEY => query.until = Some(parse_micros(value)?),
                LIMIT_KEY => query.limit = parse_canonical(value)?,
                _ => return Err(IggyError::InvalidCommand),
            }
        }
        query.start = match (from, after, current_after) {
            (Some(FROM_EARLIEST), None, None) => MetadataChangeStart::Earliest,
            (Some(FROM_LATEST), None, None) => MetadataChangeStart::Latest,
            (Some(FROM_CURRENT), None, token) => MetadataChangeStart::Current(token),
            (None, Some(cursor), None) => MetadataChangeStart::After(cursor),
            _ => return Err(IggyError::InvalidCommand),
        };
        Ok(query)
    }
}

fn parse_groups(value: &str) -> Result<Vec<MetadataChangeGroup>, IggyError> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|name| MetadataChangeGroup::from_str(name).map_err(|_| IggyError::InvalidCommand))
        .collect()
}

fn parse_micros(value: &str) -> Result<IggyTimestamp, IggyError> {
    IggyTimestamp::checked_from_micros(parse_canonical(value)?).ok_or(IggyError::InvalidCommand)
}

/// A decimal integer without a sign, padding or leading zeros.
fn parse_canonical<T: FromStr>(value: &str) -> Result<T, IggyError> {
    let canonical = !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value == "0" || !value.starts_with('0'));
    if !canonical {
        return Err(IggyError::InvalidCommand);
    }
    value.parse().map_err(|_| IggyError::InvalidCommand)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::primitives::metadata_changes::{
        WireMetadataChangeGroup, WireMetadataCurrentPageToken,
    };

    fn cursor() -> MetadataChangeCursor {
        MetadataChangeCursor {
            log_id: 9,
            op: 12,
            checksum: 34,
            ordinal: 0,
        }
    }

    fn token_for(groups: MetadataChangeGroups) -> MetadataCurrentPageToken {
        WireMetadataCurrentPageToken {
            anchor: cursor().into(),
            required_head: 20,
            groups_mask: groups.mask(),
            group: WireMetadataChangeGroup::Streams,
            last_key: bytes::Bytes::from_static(&[1, 0, 0, 0]),
        }
        .into()
    }

    fn query_from(start: MetadataChangeStart) -> MetadataChangesQuery {
        MetadataChangesQuery {
            start,
            ..MetadataChangesQuery::default()
        }
    }

    #[test]
    fn given_default_query_when_validating_should_select_every_group_from_earliest() {
        let query = MetadataChangesQuery::default();
        assert_eq!(query.limit, DEFAULT_METADATA_CHANGES_LIMIT);
        assert_eq!(query.start, MetadataChangeStart::Earliest);
        assert_eq!(query.validate().unwrap(), MetadataChangeGroups::all());
    }

    #[test]
    fn given_limit_outside_one_to_thousand_when_validating_should_refuse() {
        for limit in [0, 1001, u32::MAX] {
            let query = MetadataChangesQuery {
                limit,
                ..MetadataChangesQuery::default()
            };
            assert_eq!(query.validate(), Err(IggyError::InvalidCommand));
        }
        for limit in [1, 1000] {
            let query = MetadataChangesQuery {
                limit,
                ..MetadataChangesQuery::default()
            };
            assert!(query.validate().is_ok());
        }
    }

    #[test]
    fn given_since_above_until_when_validating_should_refuse() {
        let query = MetadataChangesQuery {
            since: Some(IggyTimestamp::from(5)),
            until: Some(IggyTimestamp::from(4)),
            ..MetadataChangesQuery::default()
        };
        assert_eq!(query.validate(), Err(IggyError::InvalidCommand));
    }

    #[test]
    fn given_time_bound_with_latest_or_current_when_validating_should_refuse() {
        for start in [
            MetadataChangeStart::Latest,
            MetadataChangeStart::Current(None),
        ] {
            let query = MetadataChangesQuery {
                since: Some(IggyTimestamp::from(1)),
                ..query_from(start)
            };
            assert_eq!(query.validate(), Err(IggyError::InvalidCommand));
        }
        let after = MetadataChangesQuery {
            until: Some(IggyTimestamp::from(1)),
            ..query_from(MetadataChangeStart::After(cursor()))
        };
        assert!(after.validate().is_ok());
    }

    #[test]
    fn given_current_scan_without_parent_group_when_validating_should_refuse() {
        let query = MetadataChangesQuery {
            groups: vec![MetadataChangeGroup::Topics],
            ..query_from(MetadataChangeStart::Current(None))
        };
        assert_eq!(query.validate(), Err(IggyError::InvalidCommand));
        let changes = MetadataChangesQuery {
            groups: vec![MetadataChangeGroup::Topics],
            ..query_from(MetadataChangeStart::Earliest)
        };
        assert!(changes.validate().is_ok());
    }

    #[test]
    fn given_page_token_for_other_groups_when_validating_should_refuse() {
        let groups = vec![MetadataChangeGroup::Streams, MetadataChangeGroup::Topics];
        let matching = MetadataChangesQuery {
            groups: groups.clone(),
            ..query_from(MetadataChangeStart::Current(Some(token_for(
                MetadataChangeGroups::requested(&groups),
            ))))
        };
        assert!(matching.validate().is_ok());
        let other = MetadataChangesQuery {
            groups,
            ..query_from(MetadataChangeStart::Current(Some(token_for(
                MetadataChangeGroups::all(),
            ))))
        };
        assert_eq!(other.validate(), Err(IggyError::InvalidCommand));
    }

    #[test]
    fn given_non_canonical_cursor_when_validating_should_refuse() {
        let query = query_from(MetadataChangeStart::After(MetadataChangeCursor {
            log_id: 1,
            op: 0,
            checksum: 0,
            ordinal: 3,
        }));
        assert_eq!(query.validate(), Err(IggyError::InvalidCommand));
    }

    #[test]
    fn given_every_start_mode_when_round_tripping_http_query_should_preserve_it() {
        let groups = vec![MetadataChangeGroup::Topics, MetadataChangeGroup::Streams];
        for start in [
            MetadataChangeStart::Earliest,
            MetadataChangeStart::Latest,
            MetadataChangeStart::After(cursor()),
            MetadataChangeStart::Current(None),
            MetadataChangeStart::Current(Some(token_for(MetadataChangeGroups::requested(&groups)))),
        ] {
            let original = MetadataChangesQuery {
                start,
                groups: groups.clone(),
                since: None,
                until: None,
                limit: 500,
            };
            let pairs = original.http_query_pairs();
            let parsed = MetadataChangesQuery::from_http_query(
                pairs.iter().map(|(key, value)| (*key, value.as_str())),
            )
            .unwrap();
            assert_eq!(parsed.start, original.start);
            assert_eq!(parsed.limit, 500);
            assert_eq!(
                parsed.groups,
                vec![MetadataChangeGroup::Streams, MetadataChangeGroup::Topics]
            );
        }
    }

    #[test]
    fn given_refused_http_parameters_when_parsing_should_refuse_each() {
        let after = cursor().to_string();
        let cases: Vec<Vec<(&str, &str)>> = vec![
            vec![],
            vec![("from", "earliest"), ("after", &after)],
            vec![("from", "newest")],
            vec![("from", "earliest"), ("from", "latest")],
            vec![("from", "earliest"), ("unknown", "1")],
            vec![("from", "earliest"), ("groups", "streams,,topics")],
            vec![("from", "earliest"), ("groups", "streams,")],
            vec![("from", "earliest"), ("groups", "everything")],
            vec![("from", "earliest"), ("since", "-1")],
            vec![("from", "earliest"), ("since", "01")],
            vec![("from", "earliest"), ("limit", "ten")],
            vec![("from", "earliest"), ("limit", "01")],
            vec![("from", "earliest"), ("limit", "+5")],
            vec![("from", "latest"), ("current_after", "cs1_AAAA")],
            vec![("after", "mc1_short")],
        ];
        for case in cases {
            assert_eq!(
                MetadataChangesQuery::from_http_query(case.clone()),
                Err(IggyError::InvalidCommand),
                "{case:?}"
            );
        }
    }

    #[test]
    fn given_empty_groups_parameter_when_parsing_should_mean_every_group() {
        let parsed =
            MetadataChangesQuery::from_http_query([("from", "latest"), ("groups", "")]).unwrap();
        assert!(parsed.groups.is_empty());
        assert_eq!(parsed.limit, DEFAULT_METADATA_CHANGES_LIMIT);
    }
}
