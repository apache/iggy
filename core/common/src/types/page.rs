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

//! Pages of the list commands: the query of one page and the page itself.
//!
//! A client reads a list page by page. Each page carries the cursor of the
//! next one until the list ends. A walk is not a snapshot: an entity that
//! someone creates or deletes during the walk can appear or not appear.

use crate::IggyError;
use crate::types::metadata_changes::position::decode_canonical;
use crate::types::metadata_changes::query::parse_canonical;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use iggy_binary_protocol::primitives::page::{WirePageCursor, WirePageOrder, WirePageQuery};
use iggy_binary_protocol::{MAX_WIRE_NAME_LENGTH, WireEncode, WireName};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use strum::{Display as StrumDisplay, EnumString};

pub use iggy_binary_protocol::primitives::page::{DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT};

/// Text prefix of a page cursor.
pub const PAGE_CURSOR_PREFIX: &str = "pg1_";

const ORDER_KEY: &str = "order";
const DESCENDING_KEY: &str = "descending";
const LIMIT_KEY: &str = "limit";
const CURSOR_KEY: &str = "cursor";
const NAME_KEY: &str = "name";

/// The order of a list page.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, StrumDisplay, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum PageOrder {
    /// The numeric id.
    #[default]
    Id,
    /// The creation order. Entities with one creation time keep the order of
    /// their ids. Partitions and consumer groups walk by id in this order too,
    /// because their ids follow creation.
    Created,
}

/// The position after which the next page starts.
///
/// Clients treat it as opaque and return it as they got it. Its text form is
/// `pg1_` followed by unpadded Base64url. It holds no direction, filter or
/// parent, so it is valid only with the query that returned it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCursor(WirePageCursor);

/// One page request of a list command.
///
/// The lists of named entities take a [`NamedPageQuery`], which adds a name
/// filter. A partition has no name, so the partitions of a topic take this
/// type, which cannot carry one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageQuery {
    pub order: PageOrder,
    pub descending: bool,
    /// The most items of the page, 1 to [`MAX_PAGE_LIMIT`].
    pub limit: u32,
    /// The `next` of the previous page, `None` for the first page.
    pub cursor: Option<PageCursor>,
}

/// One page request of a list of named entities: streams, topics, consumer
/// groups and users.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NamedPageQuery {
    pub page: PageQuery,
    /// Only the entries whose name holds this text, case-sensitive.
    pub name: Option<String>,
}

/// One page of a list. A page can be short or empty and still have a `next`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    /// The cursor of the next page, `None` on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<PageCursor>,
}

impl Default for PageQuery {
    fn default() -> Self {
        Self {
            order: PageOrder::Id,
            descending: false,
            limit: DEFAULT_PAGE_LIMIT,
            cursor: None,
        }
    }
}

impl From<PageQuery> for NamedPageQuery {
    fn from(page: PageQuery) -> Self {
        Self { page, name: None }
    }
}

impl PageQuery {
    /// # Errors
    /// `InvalidCommand` for a limit outside 1 to [`MAX_PAGE_LIMIT`].
    pub fn validate(&self) -> Result<(), IggyError> {
        if !(1..=MAX_PAGE_LIMIT).contains(&self.limit) {
            return Err(IggyError::InvalidCommand);
        }
        Ok(())
    }

    /// The query of the page after `page`, or `None` after the last page.
    ///
    /// # Errors
    /// `InvalidFormat` for a page whose `next` is the cursor of this query. A
    /// server that answers the cursor it was sent would keep a walk going
    /// forever.
    pub fn next<T>(&self, page: &Page<T>) -> Result<Option<Self>, IggyError> {
        let Some(cursor) = page.next else {
            return Ok(None);
        };
        if self.cursor == Some(cursor) {
            tracing::warn!("the server answered the page cursor {cursor} it was sent");
            return Err(IggyError::InvalidFormat);
        }
        Ok(Some(Self {
            cursor: Some(cursor),
            ..self.clone()
        }))
    }

    /// The first query of a walk over a whole list: ascending id order, in
    /// pages of [`MAX_PAGE_LIMIT`].
    #[must_use]
    pub fn whole_list() -> Self {
        Self {
            limit: MAX_PAGE_LIMIT,
            ..Self::default()
        }
    }

    /// Every item from the page of this query to the end of the list, read
    /// one page at a time with `read_page`.
    ///
    /// # Errors
    /// The first error of `read_page` or of [`Self::next`].
    pub async fn read_to_end<T, F, Fut>(self, mut read_page: F) -> Result<Vec<T>, IggyError>
    where
        F: FnMut(Self) -> Fut,
        Fut: Future<Output = Result<Page<T>, IggyError>>,
    {
        let mut query = self;
        let mut items = Vec::new();
        loop {
            let page = read_page(query.clone()).await?;
            let next = query.next(&page)?;
            items.extend(page.items);
            match next {
                Some(next) => query = next,
                None => return Ok(items),
            }
        }
    }

    /// The HTTP query parameters of this query, in a stable order. They always
    /// hold the limit, so a server reads them as a page request.
    #[must_use]
    pub fn http_query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = vec![
            (ORDER_KEY, self.order.to_string()),
            (DESCENDING_KEY, self.descending.to_string()),
            (LIMIT_KEY, self.limit.to_string()),
        ];
        if let Some(cursor) = self.cursor {
            pairs.push((CURSOR_KEY, cursor.to_string()));
        }
        pairs
    }

    /// [`NamedPageQuery::from_http_query`] for a list without names. A `name`
    /// key is refused.
    ///
    /// # Errors
    /// `InvalidCommand` for any refused parameter.
    pub fn from_http_query<'a>(
        pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, IggyError> {
        let query = NamedPageQuery::from_http_query(pairs)?;
        if query.name.is_some() {
            return Err(IggyError::InvalidCommand);
        }
        Ok(query.page)
    }
}

impl NamedPageQuery {
    /// # Errors
    /// `InvalidCommand` as for [`PageQuery::validate`], and for a name that is
    /// empty or longer than 255 bytes.
    pub fn validate(&self) -> Result<(), IggyError> {
        self.page.validate()?;
        if self
            .name
            .as_ref()
            .is_some_and(|name| name.is_empty() || name.len() > MAX_WIRE_NAME_LENGTH)
        {
            return Err(IggyError::InvalidCommand);
        }
        Ok(())
    }

    /// The query of the page after `page`, or `None` after the last page.
    ///
    /// # Errors
    /// As for [`PageQuery::next`].
    pub fn next<T>(&self, page: &Page<T>) -> Result<Option<Self>, IggyError> {
        Ok(self.page.next(page)?.map(|next| Self {
            page: next,
            name: self.name.clone(),
        }))
    }

    /// [`PageQuery::read_to_end`] with the name filter of this query.
    ///
    /// # Errors
    /// As for [`PageQuery::read_to_end`].
    pub async fn read_to_end<T, F, Fut>(self, mut read_page: F) -> Result<Vec<T>, IggyError>
    where
        F: FnMut(Self) -> Fut,
        Fut: Future<Output = Result<Page<T>, IggyError>>,
    {
        let name = self.name;
        self.page
            .read_to_end(|page| {
                read_page(Self {
                    page,
                    name: name.clone(),
                })
            })
            .await
    }

    /// [`PageQuery::http_query_pairs`] and the name filter.
    #[must_use]
    pub fn http_query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = self.page.http_query_pairs();
        if let Some(name) = &self.name {
            pairs.push((NAME_KEY, name.clone()));
        }
        pairs
    }

    /// Whether the HTTP query parameters of a list request ask for one page:
    /// they hold a `limit` or a `cursor`. Without both, a list route answers
    /// the whole list, in the order and with the filter of the other keys.
    #[must_use]
    pub fn selects_page(pairs: &[(&str, &str)]) -> bool {
        pairs
            .iter()
            .any(|(key, _)| matches!(*key, LIMIT_KEY | CURSOR_KEY))
    }

    /// Parse the page parameters of an HTTP list request. A missing parameter
    /// keeps its default.
    ///
    /// Unknown or repeated keys and malformed values are refused. The result
    /// is not yet validated, see [`Self::validate`].
    ///
    /// # Errors
    /// `InvalidCommand` for any refused parameter.
    pub fn from_http_query<'a>(
        pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, IggyError> {
        let mut seen = BTreeSet::new();
        let mut query = Self::default();
        for (key, value) in pairs {
            if !seen.insert(key) {
                return Err(IggyError::InvalidCommand);
            }
            match key {
                ORDER_KEY => {
                    query.page.order =
                        PageOrder::from_str(value).map_err(|_| IggyError::InvalidCommand)?;
                }
                DESCENDING_KEY => {
                    query.page.descending =
                        bool::from_str(value).map_err(|_| IggyError::InvalidCommand)?;
                }
                LIMIT_KEY => query.page.limit = parse_canonical(value)?,
                CURSOR_KEY => query.page.cursor = Some(PageCursor::from_str(value)?),
                NAME_KEY => query.name = Some(value.to_owned()),
                _ => return Err(IggyError::InvalidCommand),
            }
        }
        Ok(query)
    }
}

impl From<PageOrder> for WirePageOrder {
    fn from(order: PageOrder) -> Self {
        match order {
            PageOrder::Id => Self::Id,
            PageOrder::Created => Self::Created,
        }
    }
}

impl From<WirePageOrder> for PageOrder {
    fn from(order: WirePageOrder) -> Self {
        match order {
            WirePageOrder::Id => Self::Id,
            WirePageOrder::Created => Self::Created,
        }
    }
}

impl From<WirePageCursor> for PageCursor {
    fn from(cursor: WirePageCursor) -> Self {
        Self(cursor)
    }
}

impl From<PageCursor> for WirePageCursor {
    fn from(cursor: PageCursor) -> Self {
        cursor.0
    }
}

impl TryFrom<&PageQuery> for WirePageQuery {
    type Error = IggyError;

    fn try_from(query: &PageQuery) -> Result<Self, Self::Error> {
        query.validate()?;
        Ok(Self {
            order: query.order.into(),
            descending: query.descending,
            limit: query.limit,
            cursor: query.cursor.map(Into::into),
            name: None,
        })
    }
}

impl TryFrom<&NamedPageQuery> for WirePageQuery {
    type Error = IggyError;

    fn try_from(query: &NamedPageQuery) -> Result<Self, Self::Error> {
        query.validate()?;
        let name = query
            .name
            .as_deref()
            .map(WireName::new)
            .transpose()
            .map_err(|_| IggyError::InvalidCommand)?;
        Ok(Self {
            name,
            ..Self::try_from(&query.page)?
        })
    }
}

/// The page block of a list without names, such as the partitions of a
/// topic. A block with a name filter is refused.
impl TryFrom<WirePageQuery> for PageQuery {
    type Error = IggyError;

    fn try_from(page: WirePageQuery) -> Result<Self, Self::Error> {
        if page.name.is_some() {
            return Err(IggyError::InvalidCommand);
        }
        let query = Self {
            order: page.order.into(),
            descending: page.descending,
            limit: page.limit,
            cursor: page.cursor.map(PageCursor::from),
        };
        query.validate()?;
        Ok(query)
    }
}

impl TryFrom<WirePageQuery> for NamedPageQuery {
    type Error = IggyError;

    /// A [`WireName`] already holds 1 to 255 bytes, so only the page needs a
    /// check.
    fn try_from(mut page: WirePageQuery) -> Result<Self, Self::Error> {
        let name = page.name.take().map(|name| name.as_str().to_owned());
        Ok(Self {
            page: PageQuery::try_from(page)?,
            name,
        })
    }
}

impl Display for PageCursor {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{PAGE_CURSOR_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(self.0.to_bytes())
        )
    }
}

impl FromStr for PageCursor {
    type Err = IggyError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let bytes = decode_canonical(text, PAGE_CURSOR_PREFIX)?;
        WirePageCursor::decode_exact(&bytes)
            .map(Self)
            .map_err(|_| IggyError::InvalidCommand)
    }
}

impl Serialize for PageCursor {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PageCursor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::from_str(&text).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::ready;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    const ID_SEVEN: &str = "pg1_AQcAAAAAAAAA";

    fn created(created_at: u64, id: u64) -> PageCursor {
        PageCursor(WirePageCursor::Created { created_at, id })
    }

    #[test]
    fn given_cursors_when_printing_should_round_trip_through_the_pg1_text() {
        assert_eq!(PageCursor(WirePageCursor::Id(7)).to_string(), ID_SEVEN);
        for cursor in [
            PageCursor(WirePageCursor::Id(0)),
            PageCursor(WirePageCursor::Id(u64::MAX)),
            created(1_700_000_000_000_000, 42),
            created(u64::MAX, u64::MAX),
        ] {
            assert_eq!(PageCursor::from_str(&cursor.to_string()).unwrap(), cursor);
        }
    }

    #[test]
    fn given_malformed_cursor_text_when_parsing_should_refuse() {
        let refused = [
            "",
            "AQcAAAAAAAAA",
            "mc1_AQcAAAAAAAAA",
            "pg2_AQcAAAAAAAAA",
            "pg1_AQcAAAAAAAAA=",
            "pg1_AQcAAAAAAAA+",
            "pg1_AAcAAAAAAAAA",
            "pg1_AwcAAAAAAAAA",
            "pg1_AQcAAAAAAAAAAA",
            "pg1_AgcAAAAAAAAA",
        ];
        for text in refused {
            assert_eq!(
                PageCursor::from_str(text),
                Err(IggyError::InvalidCommand),
                "{text}"
            );
        }
        // 17 bytes leave two spare bits in the last character, which a
        // canonical text keeps at zero.
        let text = created(9, 3).to_string();
        let spare_bit_set = format!("{}B", text.strip_suffix('A').unwrap());
        assert_eq!(
            PageCursor::from_str(&spare_bit_set),
            Err(IggyError::InvalidCommand)
        );
    }

    fn text_pairs<'a>(pairs: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
        pairs
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect()
    }

    #[test]
    fn given_a_query_when_round_tripping_http_pairs_should_keep_it() {
        let query = NamedPageQuery {
            page: PageQuery {
                order: PageOrder::Created,
                descending: true,
                limit: 20,
                cursor: Some(created(9, 3)),
            },
            name: Some("ord ers".to_owned()),
        };
        let pairs = query.http_query_pairs();
        assert_eq!(
            &pairs[..3],
            &[
                ("order", "created".to_owned()),
                ("descending", "true".to_owned()),
                ("limit", "20".to_owned()),
            ]
        );
        assert_eq!(
            NamedPageQuery::from_http_query(text_pairs(&pairs)).unwrap(),
            query
        );
        let unnamed = query.page.http_query_pairs();
        assert_eq!(
            PageQuery::from_http_query(text_pairs(&unnamed)).unwrap(),
            query.page
        );
        assert_eq!(
            NamedPageQuery::from_http_query(Vec::<(&str, &str)>::new()).unwrap(),
            NamedPageQuery::default()
        );
    }

    #[test]
    fn given_refused_http_parameters_when_parsing_should_refuse_each() {
        let refused: &[&[(&str, &str)]] = &[
            &[("offset", "10")],
            &[("consistency", "linearizable")],
            &[("limit", "10"), ("limit", "10")],
            &[("limit", "010")],
            &[("limit", "+10")],
            &[("limit", "-1")],
            &[("limit", "")],
            &[("order", "name")],
            &[("order", "ID")],
            &[("descending", "1")],
            &[("descending", "yes")],
            &[("cursor", "pg1_")],
        ];
        for pairs in refused {
            assert_eq!(
                NamedPageQuery::from_http_query(pairs.iter().copied()),
                Err(IggyError::InvalidCommand),
                "{pairs:?}"
            );
            assert_eq!(
                PageQuery::from_http_query(pairs.iter().copied()),
                Err(IggyError::InvalidCommand),
                "{pairs:?}"
            );
        }
        assert_eq!(
            PageQuery::from_http_query([("name", "orders")]),
            Err(IggyError::InvalidCommand),
            "a list without names refuses a name key"
        );
    }

    #[test]
    fn given_http_parameters_when_asking_for_one_page_should_need_a_limit_or_a_cursor() {
        let whole_list: &[&[(&str, &str)]] = &[
            &[],
            &[
                ("name", "ord"),
                ("order", "created"),
                ("descending", "true"),
            ],
        ];
        for pairs in whole_list {
            assert!(!NamedPageQuery::selects_page(pairs), "{pairs:?}");
        }
        let one_page: &[&[(&str, &str)]] =
            &[&[("limit", "10")], &[("name", "ord"), ("cursor", ID_SEVEN)]];
        for pairs in one_page {
            assert!(NamedPageQuery::selects_page(pairs), "{pairs:?}");
        }
    }

    #[test]
    fn given_limits_and_names_when_validating_should_keep_the_ranges() {
        let with = |limit, name: Option<&str>| NamedPageQuery {
            page: PageQuery {
                limit,
                ..PageQuery::default()
            },
            name: name.map(str::to_owned),
        };
        assert!(with(1, None).validate().is_ok());
        assert!(
            with(MAX_PAGE_LIMIT, Some("n".repeat(255).as_str()))
                .validate()
                .is_ok()
        );
        for refused in [
            with(0, None),
            with(MAX_PAGE_LIMIT + 1, None),
            with(10, Some("")),
            with(10, Some("n".repeat(256).as_str())),
        ] {
            assert_eq!(refused.validate(), Err(IggyError::InvalidCommand));
            assert_eq!(
                WirePageQuery::try_from(&refused),
                Err(IggyError::InvalidCommand)
            );
        }
        for limit in [0, MAX_PAGE_LIMIT + 1] {
            let page = PageQuery {
                limit,
                ..PageQuery::default()
            };
            assert_eq!(page.validate(), Err(IggyError::InvalidCommand));
            assert_eq!(
                WirePageQuery::try_from(&page),
                Err(IggyError::InvalidCommand)
            );
        }
    }

    #[test]
    fn given_a_wire_block_with_a_name_when_reading_a_list_without_names_should_refuse_it() {
        let named = NamedPageQuery {
            name: Some("n".to_owned()),
            ..NamedPageQuery::default()
        };
        let wire = WirePageQuery::try_from(&named).unwrap();
        assert_eq!(
            PageQuery::try_from(wire.clone()),
            Err(IggyError::InvalidCommand)
        );
        assert_eq!(NamedPageQuery::try_from(wire).unwrap(), named);
        let unnamed = WirePageQuery::try_from(&PageQuery::default()).unwrap();
        assert_eq!(unnamed.name, None);
        assert_eq!(PageQuery::try_from(unnamed).unwrap(), PageQuery::default());
    }

    #[test]
    fn given_a_query_when_round_tripping_the_wire_block_should_keep_it() {
        let query = NamedPageQuery {
            page: PageQuery {
                order: PageOrder::Created,
                descending: true,
                limit: 1000,
                cursor: Some(created(5, 6)),
            },
            name: Some("orders".to_owned()),
        };
        let wire = WirePageQuery::try_from(&query).unwrap();
        assert_eq!(NamedPageQuery::try_from(wire.clone()).unwrap(), query);
        let over_limit = WirePageQuery {
            limit: 1001,
            ..wire
        };
        assert_eq!(
            NamedPageQuery::try_from(over_limit),
            Err(IggyError::InvalidCommand)
        );
    }

    #[test]
    fn given_a_page_when_asking_for_the_next_query_should_carry_its_cursor() {
        let query = NamedPageQuery {
            page: PageQuery {
                limit: 2,
                ..PageQuery::default()
            },
            name: Some("ord".to_owned()),
        };
        let more = Page {
            items: vec![1, 2],
            next: Some(PageCursor(WirePageCursor::Id(2))),
        };
        let next = query.next(&more).unwrap().unwrap();
        assert_eq!(next.page.cursor, more.next);
        assert_eq!(next.name, query.name);
        let last = Page::<u32> {
            items: vec![3],
            next: None,
        };
        assert_eq!(next.next(&last), Ok(None));
        assert_eq!(
            next.next(&more),
            Err(IggyError::InvalidFormat),
            "a page that answers the cursor it was read with"
        );
    }

    /// Drives a future whose every await is ready.
    fn run_ready<T>(future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("a ready reader never suspends"),
        }
    }

    #[test]
    fn given_a_list_of_several_pages_when_reading_to_the_end_should_follow_each_cursor() {
        const ITEMS: u64 = 7;
        let first = NamedPageQuery {
            page: PageQuery {
                limit: 3,
                ..PageQuery::default()
            },
            name: Some("ord".to_owned()),
        };
        let mut queries = Vec::new();
        let read = run_ready(first.clone().read_to_end(|query| {
            let start = match query.page.cursor.map(WirePageCursor::from) {
                None => 0,
                Some(WirePageCursor::Id(id)) => id + 1,
                Some(cursor) => panic!("an id walk got {cursor:?}"),
            };
            let end = (start + u64::from(query.page.limit)).min(ITEMS);
            let next = (end < ITEMS).then(|| PageCursor(WirePageCursor::Id(end - 1)));
            queries.push(query);
            ready(Ok(Page {
                items: (start..end).collect(),
                next,
            }))
        }));
        assert_eq!(read, Ok((0..ITEMS).collect::<Vec<_>>()));
        let cursors: Vec<_> = queries.iter().map(|query| query.page.cursor).collect();
        assert_eq!(
            cursors,
            [
                None,
                Some(PageCursor(WirePageCursor::Id(2))),
                Some(PageCursor(WirePageCursor::Id(5)))
            ]
        );
        assert!(
            queries.iter().all(|query| NamedPageQuery {
                page: PageQuery {
                    cursor: None,
                    ..query.page.clone()
                },
                name: query.name.clone(),
            } == first),
            "every page keeps the order, the limit and the filter"
        );
    }

    #[test]
    fn given_a_failing_page_when_reading_to_the_end_should_return_its_error() {
        let mut pages = 0;
        let read = run_ready(PageQuery::whole_list().read_to_end(|query| {
            pages += 1;
            ready(if query.cursor.is_none() {
                Ok(Page {
                    items: vec![1_u32],
                    next: Some(PageCursor(WirePageCursor::Id(1))),
                })
            } else {
                Err(IggyError::Unauthorized)
            })
        }));
        assert_eq!(read, Err(IggyError::Unauthorized));
        assert_eq!(pages, 2);
    }

    #[test]
    fn given_a_page_that_answers_its_own_cursor_when_reading_to_the_end_should_fail() {
        const MAX_PAGES: u32 = 4;
        let repeated = PageCursor(WirePageCursor::Id(1));
        let mut pages = 0;
        let read = run_ready(PageQuery::whole_list().read_to_end(|_| {
            pages += 1;
            assert!(
                pages <= MAX_PAGES,
                "the walk must stop at the repeated cursor"
            );
            ready(Ok(Page {
                items: vec![pages],
                next: Some(repeated),
            }))
        }));
        assert_eq!(read, Err(IggyError::InvalidFormat));
        assert_eq!(
            pages, 2,
            "the second page answered the cursor it was read with"
        );
    }

    #[test]
    fn given_a_page_when_serializing_json_should_leave_out_next_on_the_last_page() {
        let more = Page {
            items: vec![1u32],
            next: Some(PageCursor(WirePageCursor::Id(7))),
        };
        let json = serde_json::to_string(&more).unwrap();
        assert_eq!(json, format!(r#"{{"items":[1],"next":"{ID_SEVEN}"}}"#));
        assert_eq!(serde_json::from_str::<Page<u32>>(&json).unwrap(), more);
        let last = Page::<u32> {
            items: Vec::new(),
            next: None,
        };
        assert_eq!(serde_json::to_string(&last).unwrap(), r#"{"items":[]}"#);
        assert_eq!(
            serde_json::from_str::<Page<u32>>(r#"{"items":[]}"#).unwrap(),
            last
        );
        assert!(serde_json::from_str::<Page<u32>>(r#"{"items":[],"next":"pg1_"}"#).is_err());
    }
}
