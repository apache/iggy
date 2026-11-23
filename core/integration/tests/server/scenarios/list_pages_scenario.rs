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

//! Pages of the list commands, read through the SDK on every transport.

use crate::server::scenarios::create_client;
use crate::server::scenarios::permissions_scenario::assert_error_code;
use iggy::prelude::*;
use integration::harness::{TestHarness, login_root};
use std::cell::Cell;

const STREAM: &str = "pages-stream";
const TOPIC: &str = "pages-topic";
const PARTITIONS: u32 = 10;
const PAGE_LIMIT: u32 = 3;
const READER: &str = "pages-reader";
const PASSWORD: &str = "password123";

pub async fn run(harness: &TestHarness) {
    let root = create_client(harness).await;
    login_root(&root).await.expect("login root");
    let stream_id = Identifier::named(STREAM).unwrap();
    let topic_id = Identifier::named(TOPIC).unwrap();
    root.create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_topic(&root, &stream_id, TOPIC, PARTITIONS).await;

    assert_partition_pages(&root, &stream_id, &topic_id).await;
    assert_stream_pages(&root).await;
    assert_topic_pages(&root, &stream_id).await;
    assert_consumer_group_pages(&root, &stream_id, &topic_id).await;
    assert_user_pages(&root).await;
    assert_walks_across_deletes(&root).await;
    assert_page_refusals(harness, &root, &stream_id, &topic_id).await;

    root.delete_stream(&stream_id).await.expect("delete stream");
}

async fn assert_partition_pages(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
) {
    let all: Vec<u32> = (0..PARTITIONS).collect();
    let ascending = PageQuery {
        limit: PAGE_LIMIT,
        ..PageQuery::default()
    };
    assert_eq!(
        walk_partitions(client, stream_id, topic_id, ascending.clone()).await,
        (all.clone(), 4),
        "pages of 3, 3, 3 and 1, and only the last one without next"
    );
    let descending = PageQuery {
        descending: true,
        ..ascending.clone()
    };
    assert_eq!(
        walk_partitions(client, stream_id, topic_id, descending).await,
        (all.iter().rev().copied().collect(), 4)
    );
    let created = PageQuery {
        order: PageOrder::Created,
        ..ascending
    };
    assert_eq!(
        walk_partitions(client, stream_id, topic_id, created)
            .await
            .0,
        all,
        "partition ids follow creation"
    );

    let details = client
        .get_topic(stream_id, topic_id)
        .await
        .expect("get topic")
        .expect("the topic exists");
    let whole = PageQuery {
        limit: PARTITIONS,
        ..PageQuery::default()
    };
    let page = client
        .get_partitions_page(stream_id, topic_id, &whole)
        .await
        .expect("one page of every partition");
    assert_eq!(page.next, None);
    let shape = |partitions: &[Partition]| {
        partitions
            .iter()
            .map(|partition| {
                (
                    partition.id,
                    partition.created_at,
                    partition.members.clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        shape(&page.items),
        shape(&details.partitions),
        "a page item is the partition of the topic details"
    );
}

/// Stream `pages-s5` takes the id of the deleted `pages-s2`, so the id order
/// and the creation order differ.
async fn assert_stream_pages(client: &IggyClient) {
    for name in ["pages-s1", "pages-s2", "pages-s3", "pages-s4"] {
        client
            .create_stream(name, &StreamCreateOptions::default())
            .await
            .expect("create stream");
    }
    client
        .delete_stream(&Identifier::named("pages-s2").unwrap())
        .await
        .expect("delete stream");
    client
        .create_stream("pages-s5", &StreamCreateOptions::default())
        .await
        .expect("create a stream in the freed id");
    let by_id = [STREAM, "pages-s1", "pages-s5", "pages-s3", "pages-s4"];
    let by_created = [STREAM, "pages-s1", "pages-s3", "pages-s4", "pages-s5"];
    assert_walks(
        &by_id,
        &by_created,
        |stream: &Stream| stream.name.as_str(),
        |query| async move { client.get_streams_page(&query.into()).await },
    )
    .await;
    let listed = client.get_streams().await.expect("get streams");
    let names: Vec<&str> = listed.iter().map(|stream| stream.name.as_str()).collect();
    assert_eq!(names, by_id, "the list is in id order");

    let filtered = NamedPageQuery {
        name: Some("s3".to_owned()),
        ..NamedPageQuery::default()
    };
    let page = client
        .get_streams_page(&filtered)
        .await
        .expect("a page of the streams with s3 in the name");
    let names: Vec<&str> = page
        .items
        .iter()
        .map(|stream| stream.name.as_str())
        .collect();
    assert_eq!(names, ["pages-s3"]);
}

async fn assert_topic_pages(client: &IggyClient, stream_id: &Identifier) {
    for name in ["pages-t1", "pages-t2", "pages-t3"] {
        create_topic(client, stream_id, name, 1).await;
    }
    let by_id = [TOPIC, "pages-t1", "pages-t2", "pages-t3"];
    assert_walks(
        &by_id,
        &by_id,
        |topic: &Topic| topic.name.as_str(),
        |query| async move { client.get_topics_page(stream_id, &query.into()).await },
    )
    .await;
    let listed = client.get_topics(stream_id).await.expect("get topics");
    let names: Vec<&str> = listed.iter().map(|topic| topic.name.as_str()).collect();
    assert_eq!(names, by_id, "the list is in id order");

    let missing_stream = Identifier::named("pages-missing-stream").unwrap();
    let page = client
        .get_topics_page(&missing_stream, &NamedPageQuery::default())
        .await
        .expect("a page of the topics of a missing stream");
    assert_eq!(
        (page.items.len(), page.next),
        (0, None),
        "a missing stream lists as an empty last page"
    );
}

async fn assert_consumer_group_pages(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
) {
    let groups = ["pages-g1", "pages-g2", "pages-g3", "pages-g4"];
    for name in groups {
        client
            .create_consumer_group(stream_id, topic_id, name)
            .await
            .expect("create consumer group");
    }
    assert_walks(
        &groups,
        &groups,
        |group: &ConsumerGroup| group.name.as_str(),
        |query| async move {
            client
                .get_consumer_groups_page(stream_id, topic_id, &query.into())
                .await
        },
    )
    .await;
    let listed = client
        .get_consumer_groups(stream_id, topic_id)
        .await
        .expect("get consumer groups");
    let names: Vec<&str> = listed.iter().map(|group| group.name.as_str()).collect();
    assert_eq!(names, groups, "the list is in id order");
}

/// User `pages-u5` takes the id of the deleted `pages-u2`. The root user is
/// the first one created.
async fn assert_user_pages(client: &IggyClient) {
    for username in ["pages-u1", "pages-u2", "pages-u3", "pages-u4"] {
        create_user(client, username).await;
    }
    client
        .delete_user(&Identifier::named("pages-u2").unwrap())
        .await
        .expect("delete user");
    create_user(client, "pages-u5").await;
    let by_id = [
        DEFAULT_ROOT_USERNAME,
        "pages-u1",
        "pages-u5",
        "pages-u3",
        "pages-u4",
    ];
    let by_created = [
        DEFAULT_ROOT_USERNAME,
        "pages-u1",
        "pages-u3",
        "pages-u4",
        "pages-u5",
    ];
    assert_walks(
        &by_id,
        &by_created,
        |user: &UserInfo| user.username.as_str(),
        |query| async move { client.get_users_page(&query.into()).await },
    )
    .await;
    let listed = client.get_users().await.expect("get users");
    let names: Vec<&str> = listed.iter().map(|user| user.username.as_str()).collect();
    assert_eq!(names, by_id, "the list is in id order");
}

/// Deletes the stream that a cursor names between two pages of a walk, then
/// the stream of a topics walk. No entity is created between the delete and
/// the next page, so no new stream can take the freed id.
async fn assert_walks_across_deletes(client: &IggyClient) {
    for order in [PageOrder::Id, PageOrder::Created] {
        let prefix = format!("pages-{order}-");
        let names: Vec<String> = (1..=4).map(|index| format!("{prefix}{index}")).collect();
        for name in &names {
            client
                .create_stream(name, &StreamCreateOptions::default())
                .await
                .expect("create stream");
        }
        let first = NamedPageQuery {
            page: PageQuery {
                order,
                limit: 2,
                ..PageQuery::default()
            },
            name: Some(prefix),
        };
        let page = client
            .get_streams_page(&first)
            .await
            .expect("the first page");
        let mut read: Vec<String> = page
            .items
            .iter()
            .map(|stream| stream.name.clone())
            .collect();
        assert_eq!(read, names[..2], "{order} order: the first page");
        client
            .delete_stream(&Identifier::named(&names[1]).unwrap())
            .await
            .expect("delete the stream that the cursor names");
        let rest = first
            .next(&page)
            .expect("a new cursor")
            .expect("more pages")
            .read_to_end(|query| async move { client.get_streams_page(&query).await })
            .await
            .expect("the pages after the deleted stream");
        read.extend(rest.into_iter().map(|stream| stream.name));
        assert_eq!(
            read, names,
            "{order} order: no stream twice and none left out"
        );
        for name in [&names[0], &names[2], &names[3]] {
            client
                .delete_stream(&Identifier::named(name).unwrap())
                .await
                .expect("delete stream");
        }
    }

    let parent = Identifier::named("pages-parent").unwrap();
    client
        .create_stream("pages-parent", &StreamCreateOptions::default())
        .await
        .expect("create stream");
    for name in ["pages-parent-t1", "pages-parent-t2", "pages-parent-t3"] {
        create_topic(client, &parent, name, 1).await;
    }
    let first = NamedPageQuery::from(PageQuery {
        limit: 2,
        ..PageQuery::default()
    });
    let page = client
        .get_topics_page(&parent, &first)
        .await
        .expect("the first page of topics");
    let next = first
        .next(&page)
        .expect("a new cursor")
        .expect("more topics");
    client
        .delete_stream(&parent)
        .await
        .expect("delete the stream during the walk");
    assert_error_code(
        client.get_topics_page(&parent, &next).await,
        IggyError::StreamIdNotFound(Identifier::default()),
        "a topics page after its stream was deleted",
    );
}

async fn assert_page_refusals(
    harness: &TestHarness,
    root: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
) {
    let empty = PageQuery {
        limit: 0,
        ..PageQuery::default()
    };
    assert_error_code(
        root.get_partitions_page(stream_id, topic_id, &empty).await,
        IggyError::InvalidCommand,
        "a limit of 0",
    );
    assert_error_code(
        root.get_streams_page(&empty.into()).await,
        IggyError::InvalidCommand,
        "a limit of 0",
    );
    let first = NamedPageQuery::from(PageQuery {
        limit: 1,
        ..PageQuery::default()
    });
    let id_page = root
        .get_streams_page(&first)
        .await
        .expect("a first page in id order");
    let other_order = NamedPageQuery::from(PageQuery {
        order: PageOrder::Created,
        cursor: Some(id_page.next.expect("more than one stream")),
        ..PageQuery::default()
    });
    assert_error_code(
        root.get_streams_page(&other_order).await,
        IggyError::InvalidCommand,
        "an id cursor in a creation walk",
    );
    let missing_topic = Identifier::named("pages-missing-topic").unwrap();
    let missing_topic_error =
        IggyError::TopicIdNotFound(Identifier::default(), Identifier::default());
    assert_error_code(
        root.get_partitions_page(stream_id, &missing_topic, &PageQuery::default())
            .await,
        missing_topic_error.clone(),
        "the partitions of a missing topic",
    );
    assert_error_code(
        root.get_consumer_groups_page(stream_id, &missing_topic, &NamedPageQuery::default())
            .await,
        missing_topic_error,
        "the consumer groups of a missing topic",
    );

    create_user(root, READER).await;
    let reader = create_client(harness).await;
    reader
        .login_user(READER, PASSWORD)
        .await
        .expect("login the user without permissions");
    let query = PageQuery::default();
    let named = NamedPageQuery::default();
    // A list holds only what the caller may read, so without a grant it is an
    // empty last page rather than a refusal.
    for (page, what) in [
        (
            reader
                .get_streams_page(&named)
                .await
                .map(|page| (page.items.len(), page.next.is_some())),
            "a streams page without a grant",
        ),
        (
            reader
                .get_topics_page(stream_id, &named)
                .await
                .map(|page| (page.items.len(), page.next.is_some())),
            "a topics page without a grant",
        ),
    ] {
        assert_eq!(page.expect(what), (0, false), "{what}");
    }
    assert_error_code(
        reader
            .get_partitions_page(stream_id, topic_id, &query)
            .await,
        IggyError::Unauthorized,
        "a partitions page without the permission to read the topic",
    );
    assert_error_code(
        reader
            .get_consumer_groups_page(stream_id, topic_id, &named)
            .await,
        IggyError::Unauthorized,
        "a consumer groups page without the permission to read the topic",
    );
    assert_error_code(
        reader.get_users_page(&named).await,
        IggyError::Unauthorized,
        "a users page without the permission to read users",
    );
    root.delete_user(&Identifier::named(READER).unwrap())
        .await
        .expect("delete the user without permissions");
}

async fn create_topic(client: &IggyClient, stream_id: &Identifier, name: &str, partitions: u32) {
    client
        .create_topic(
            stream_id,
            name,
            &TopicCreateOptions {
                partitions_count: Some(partitions),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
}

async fn create_user(client: &IggyClient, username: &str) {
    client
        .create_user(
            username,
            PASSWORD,
            UserStatus::Active,
            None,
            &UserCreateOptions::default(),
        )
        .await
        .expect("create user");
}

/// Walks each order both ways in pages of [`PAGE_LIMIT`] and compares the
/// names with `by_id` and `by_created`, both in ascending order.
async fn assert_walks<T, Fut>(
    by_id: &[&str],
    by_created: &[&str],
    name: impl Fn(&T) -> &str,
    mut read_page: impl FnMut(PageQuery) -> Fut,
) where
    Fut: Future<Output = Result<Page<T>, IggyError>>,
{
    for (order, ascending) in [(PageOrder::Id, by_id), (PageOrder::Created, by_created)] {
        for descending in [false, true] {
            let first = PageQuery {
                order,
                descending,
                limit: PAGE_LIMIT,
                ..PageQuery::default()
            };
            let (items, pages) = walk(first, &mut read_page).await;
            let mut expected = ascending.to_vec();
            if descending {
                expected.reverse();
            }
            let names: Vec<&str> = items.iter().map(&name).collect();
            let case = format!("{order} order, descending {descending}");
            assert_eq!(names, expected, "{case}");
            assert_eq!(
                pages,
                expected.len().div_ceil(PAGE_LIMIT as usize),
                "{case}: only the last page has no next"
            );
        }
    }
}

/// Every page of partitions from `first` on, as the partition ids and the
/// pages read.
async fn walk_partitions(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    first: PageQuery,
) -> (Vec<u32>, usize) {
    let (partitions, pages) = walk(first, |query| async move {
        client
            .get_partitions_page(stream_id, topic_id, &query)
            .await
    })
    .await;
    (
        partitions.iter().map(|partition| partition.id).collect(),
        pages,
    )
}

/// Every page from `first` on, as the items and the pages read.
async fn walk<T, Fut>(
    first: PageQuery,
    mut read_page: impl FnMut(PageQuery) -> Fut,
) -> (Vec<T>, usize)
where
    Fut: Future<Output = Result<Page<T>, IggyError>>,
{
    let pages = Cell::new(0);
    let items = first
        .read_to_end(|query| {
            pages.set(pages.get() + 1);
            read_page(query)
        })
        .await
        .expect("every page reads");
    (items, pages.get())
}
