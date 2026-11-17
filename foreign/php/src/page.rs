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

use std::str::FromStr;

use ext_php_rs::{exception::PhpResult, php_class, php_enum, php_impl, types::Zval};
use iggy::prelude::{
    ConsumerGroup as RustConsumerGroup, DEFAULT_PAGE_LIMIT, NamedPageQuery, Page, PageCursor,
    PageOrder as RustPageOrder, PageQuery, Partition as RustPartition, Stream as RustStream,
    Topic as RustTopic, UserInfo as RustUserInfo,
};

use crate::client::optional_unsigned;
use crate::consumer_group::ConsumerGroup;
use crate::error::to_php_exception;
use crate::stream::Stream;
use crate::topic::{Partition, Topic};
use crate::user::UserInfo;

/// The order of a list page. Values are the HTTP API names.
#[php_enum]
#[php(name = "Iggy\\PageOrder")]
#[derive(Clone, Copy)]
pub enum PageOrder {
    #[php(value = "id")]
    Id,
    #[php(value = "created")]
    Created,
}

impl From<PageOrder> for RustPageOrder {
    fn from(order: PageOrder) -> Self {
        match order {
            PageOrder::Id => Self::Id,
            PageOrder::Created => Self::Created,
        }
    }
}

/// The query that a page method sends. The Rust SDK checks a query only after its
/// authentication check, so the query is checked here, and a refused one throws
/// `Invalid command` (code 3) before anything is sent, also on a client that never connected.
pub(crate) fn page_query(
    order: Option<PageOrder>,
    descending: bool,
    limit: Option<&Zval>,
    cursor: Option<String>,
) -> PhpResult<PageQuery> {
    let query = PageQuery {
        order: order.map(Into::into).unwrap_or_default(),
        descending,
        limit: optional_unsigned(limit, "limit")?.unwrap_or(DEFAULT_PAGE_LIMIT),
        cursor: cursor
            .as_deref()
            .map(PageCursor::from_str)
            .transpose()
            .map_err(to_php_exception)?,
    };
    query.validate().map_err(to_php_exception)?;
    Ok(query)
}

/// [`page_query`] with the name filter of the lists of named entities.
pub(crate) fn named_page_query(
    order: Option<PageOrder>,
    descending: bool,
    limit: Option<&Zval>,
    cursor: Option<String>,
    name: Option<String>,
) -> PhpResult<NamedPageQuery> {
    let query = NamedPageQuery {
        page: page_query(order, descending, limit, cursor)?,
        name,
    };
    query.validate().map_err(to_php_exception)?;
    Ok(query)
}

// A PHP class cannot be generic, so each item type gets its own page class. The doc texts
// start with a space, as a `///` line does, because the stub prints them as they are.
macro_rules! list_page {
    (
        $page:ident,
        $php_name:literal,
        $item:ident,
        $rust_item:ty,
        $page_doc:literal,
        $items_doc:literal $(,)?
    ) => {
        #[doc = $page_doc]
        #[php_class]
        #[php(name = $php_name)]
        pub struct $page {
            items: Vec<$item>,
            next: Option<String>,
        }

        #[php_impl]
        impl $page {
            #[doc = $items_doc]
            ///
            /// The items are rebuilt on each getter call, so cache the array in PHP if you read it
            /// more than once.
            #[php(getter)]
            pub fn items(&self) -> Vec<$item> {
                self.items.clone()
            }

            /// The `cursor` of the next page of the same query, null on the last page. A page can be
            /// empty and still have one.
            #[php(getter)]
            pub fn next(&self) -> Option<String> {
                self.next.clone()
            }
        }

        impl From<Page<$rust_item>> for $page {
            fn from(page: Page<$rust_item>) -> Self {
                Self {
                    items: page.items.into_iter().map($item::from).collect(),
                    next: page.next.map(|cursor| cursor.to_string()),
                }
            }
        }
    };
}

list_page!(
    StreamsPage,
    "Iggy\\StreamsPage",
    Stream,
    RustStream,
    " One page of the streams, as `Client::getStreamsPage()` returns it.",
    " The streams of the page, each an `Iggy\\Stream`.",
);
list_page!(
    TopicsPage,
    "Iggy\\TopicsPage",
    Topic,
    RustTopic,
    " One page of the topics of a stream, as `Client::getTopicsPage()` returns it.",
    " The topics of the page, each an `Iggy\\Topic`.",
);
list_page!(
    PartitionsPage,
    "Iggy\\PartitionsPage",
    Partition,
    RustPartition,
    " One page of the partitions of a topic, as `Client::getPartitionsPage()` returns it.",
    " The partitions of the page, each an `Iggy\\Partition`.",
);
list_page!(
    ConsumerGroupsPage,
    "Iggy\\ConsumerGroupsPage",
    ConsumerGroup,
    RustConsumerGroup,
    " One page of the consumer groups of a topic, as `Client::getConsumerGroupsPage()` returns it.",
    " The consumer groups of the page, each an `Iggy\\ConsumerGroup`.",
);
list_page!(
    UsersPage,
    "Iggy\\UsersPage",
    UserInfo,
    RustUserInfo,
    " One page of the users, as `Client::getUsersPage()` returns it.",
    " The users of the page, each an `Iggy\\UserInfo`.",
);
