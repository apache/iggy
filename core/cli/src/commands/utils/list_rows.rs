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

//! The rows of a list command, read page by page.

use crate::commands::cli_command::{PRINT_TARGET, print_json};
use iggy_common::{IggyError, MAX_PAGE_LIMIT, NamedPageQuery, Page, PageCursor};
use serde::Serialize;
use tracing::{Level, event};

/// The rows that a list command reads.
///
/// `query` gives the order, the direction, the start and the name filter.
/// Each read sets the limit of its own page, so the limit of `query` has no
/// effect.
#[derive(Debug, Clone, Default)]
pub struct ListRows {
    pub query: NamedPageQuery,
    /// The most rows to print. Without it, the command reads every row.
    pub row_limit: Option<usize>,
}

impl ListRows {
    /// Reads the rows page by page. With a row limit, it also returns the
    /// cursor of the rows after the last one it read, if more rows follow.
    ///
    /// A page can hold fewer rows than it asks for, so the read goes on
    /// until it holds the row limit or the list ends.
    ///
    /// # Errors
    ///
    /// The error of the first page read that fails, or of
    /// [`NamedPageQuery::next`].
    pub async fn read<T, Fut>(
        &self,
        mut read_page: impl FnMut(NamedPageQuery) -> Fut,
    ) -> Result<(Vec<T>, Option<PageCursor>), IggyError>
    where
        Fut: Future<Output = Result<Page<T>, IggyError>>,
    {
        let Some(row_limit) = self.row_limit else {
            let mut query = self.query.clone();
            query.page.limit = MAX_PAGE_LIMIT;
            return query.read_to_end(read_page).await.map(|rows| (rows, None));
        };
        let mut query = self.query.clone();
        let mut rows = Vec::new();
        loop {
            // A page asks only for the missing rows: its cursor marks the
            // position after its last row, so rows that it read and the
            // command did not print would be lost.
            query.page.limit = u32::try_from(row_limit - rows.len())
                .map_or(MAX_PAGE_LIMIT, |missing| missing.min(MAX_PAGE_LIMIT));
            let page = read_page(query.clone()).await?;
            let next = query.next(&page)?;
            rows.extend(page.items);
            match next {
                Some(next) if rows.len() < row_limit => query = next,
                next => return Ok((rows, next.and_then(|next| next.page.cursor))),
            }
        }
    }

    /// Prints the rows that [`Self::read`] returned as JSON. With a row limit,
    /// it prints the `Page` that the HTTP list routes answer, so the cursor
    /// stays inside the document. Without one, no cursor can follow, so it
    /// prints the bare array.
    ///
    /// # Errors
    ///
    /// The error of the JSON serialization or of the stdout write.
    pub fn print_json<T: Serialize>(
        &self,
        items: Vec<T>,
        next: Option<PageCursor>,
    ) -> anyhow::Result<()> {
        match self.row_limit {
            Some(_) => print_json(&Page { items, next }),
            None => print_json(&items),
        }
    }
}

/// Prints the `next:` line after the rows, when more rows follow them.
pub fn print_next(next: Option<PageCursor>) {
    if let Some(next) = next {
        event!(target: PRINT_TARGET, Level::INFO, "next: {next}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::primitives::page::WirePageCursor;
    use std::cell::RefCell;
    use std::future::ready;

    /// A list of `len` items read from a fake server, which answers each page
    /// with at most `page_items` of the items it asks for. It records the
    /// limit of each page read.
    struct FakeList {
        len: u64,
        page_items: u64,
        limits: RefCell<Vec<u32>>,
    }

    impl FakeList {
        fn new(len: u64, page_items: u64) -> Self {
            Self {
                len,
                page_items,
                limits: RefCell::new(Vec::new()),
            }
        }

        fn page(&self, query: &NamedPageQuery) -> Result<Page<u64>, IggyError> {
            self.limits.borrow_mut().push(query.page.limit);
            let start = query.page.cursor.map_or(0, |cursor| match cursor.into() {
                WirePageCursor::Id(id) => id + 1,
                other => panic!("an id walk got {other:?}"),
            });
            let end = (start + u64::from(query.page.limit).min(self.page_items)).min(self.len);
            let next = (end < self.len && end > 0).then(|| WirePageCursor::Id(end - 1).into());
            Ok(Page {
                items: (start..end).collect(),
                next,
            })
        }

        async fn read(&self, row_limit: Option<usize>) -> (Vec<u64>, Option<PageCursor>) {
            let rows = ListRows {
                query: NamedPageQuery::default(),
                row_limit,
            };
            rows.read(|query| ready(self.page(&query)))
                .await
                .expect("every page reads")
        }
    }

    #[tokio::test]
    async fn given_a_row_limit_above_one_page_when_reading_should_ask_only_for_the_missing_rows() {
        let list = FakeList::new(3_000, 1_000);
        let (rows, next) = list.read(Some(2_500)).await;
        assert_eq!(rows, (0..2_500).collect::<Vec<_>>());
        assert_eq!(next, Some(WirePageCursor::Id(2_499).into()));
        assert_eq!(*list.limits.borrow(), [1_000, 1_000, 500]);
    }

    #[tokio::test]
    async fn given_short_pages_when_reading_with_a_row_limit_should_go_on_until_it_holds_them() {
        let list = FakeList::new(10, 2);
        let (rows, next) = list.read(Some(5)).await;
        assert_eq!(rows, [0, 1, 2, 3, 4]);
        assert_eq!(next, Some(WirePageCursor::Id(4).into()));
        assert_eq!(*list.limits.borrow(), [5, 3, 1]);
    }

    #[tokio::test]
    async fn given_an_empty_page_with_next_when_reading_should_go_on_after_it() {
        let empty_then_rows = RefCell::new(0);
        let rows = ListRows {
            query: NamedPageQuery::default(),
            row_limit: Some(2),
        };
        let (rows, next) = rows
            .read(|query| {
                *empty_then_rows.borrow_mut() += 1;
                let page = match query.page.cursor {
                    None => Page {
                        items: Vec::new(),
                        next: Some(WirePageCursor::Id(7).into()),
                    },
                    Some(_) => Page {
                        items: vec![8_u64],
                        next: None,
                    },
                };
                ready(Ok(page))
            })
            .await
            .expect("every page reads");
        assert_eq!((rows, next), (vec![8], None));
        assert_eq!(*empty_then_rows.borrow(), 2);
    }

    #[tokio::test]
    async fn given_an_empty_page_that_answers_its_own_cursor_when_reading_should_fail() {
        const MAX_PAGES: u32 = 4;
        let pages = RefCell::new(0);
        let rows = ListRows {
            query: NamedPageQuery::default(),
            row_limit: Some(2),
        };
        let result = rows
            .read(|_| {
                *pages.borrow_mut() += 1;
                assert!(
                    *pages.borrow() <= MAX_PAGES,
                    "the read must stop at the repeated cursor"
                );
                ready(Ok(Page::<u64> {
                    items: Vec::new(),
                    next: Some(WirePageCursor::Id(7).into()),
                }))
            })
            .await;
        assert_eq!(result, Err(IggyError::InvalidFormat));
        assert_eq!(*pages.borrow(), 2);
    }

    #[tokio::test]
    async fn given_a_list_shorter_than_the_row_limit_when_reading_should_return_no_cursor() {
        let list = FakeList::new(3, 1_000);
        assert_eq!(list.read(Some(5)).await, (vec![0, 1, 2], None));
    }

    #[tokio::test]
    async fn given_no_row_limit_when_reading_should_read_every_page_of_the_most_items() {
        let list = FakeList::new(2_100, 1_000);
        let (rows, next) = list.read(None).await;
        assert_eq!((rows.len(), next), (2_100, None));
        assert_eq!(*list.limits.borrow(), [1_000, 1_000, 1_000]);
    }

    #[tokio::test]
    async fn given_a_failing_page_when_reading_should_return_its_error() {
        let rows = ListRows {
            query: NamedPageQuery::default(),
            row_limit: Some(3),
        };
        let result = rows
            .read(|_| ready(Err::<Page<u64>, _>(IggyError::Unauthorized)))
            .await;
        assert_eq!(result, Err(IggyError::Unauthorized));
    }
}
