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

use crate::common::global_context::GlobalContext;
use cucumber::{given, then, when};
use iggy::prelude::{
    GlobalPermissions, HeaderKey, Identifier, IggyError, MAX_METADATA_CHANGES_LIMIT,
    MetadataChange, MetadataChangeGroup, MetadataChangeKind, MetadataChangeRecord,
    MetadataChangeStart, MetadataChangeStatus, MetadataChanges, MetadataChangesQuery,
    MetadataCurrentItem, MetadataOptions, OptionKey, Permissions, ResourceOptions, StreamClient,
    StreamCreateOptions, StreamEncryptionSource, SystemClient, UserClient, UserCreateOptions,
    UserStatus,
};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;

const ROOT_USER_ID: u32 = 0;
const FOLLOW_ATTEMPTS: usize = 100;
const FOLLOW_INTERVAL: Duration = Duration::from_millis(100);
const HIDDEN_OPTIONS: [OptionKey; 3] = [
    OptionKey::EncryptionKey,
    OptionKey::EncryptionKeyWrapped,
    OptionKey::CriticalKeys,
];

#[given(regex = r#"^I hold the latest metadata change cursor for the groups "([^"]+)"$"#)]
pub async fn given_latest_cursor(world: &mut GlobalContext, groups: String) {
    world.last_change_groups = groups
        .split(',')
        .map(|name| MetadataChangeGroup::from_str(name.trim()).expect("known group"))
        .collect();
    let page = read_changes(world, MetadataChangeStart::Latest)
        .await
        .expect("latest");
    assert_eq!(page.status, MetadataChangeStatus::Ok);
    world.last_change_cursor = Some(page.next.expect("latest cursor"));
}

#[when("I delete the stream")]
pub async fn when_delete_stream(world: &mut GlobalContext) {
    let client = world.client.as_ref().expect("client");
    let stream_id = world.last_stream_id.expect("stream created");
    client
        .delete_stream(&Identifier::numeric(stream_id).expect("stream id"))
        .await
        .expect("delete stream");
}

#[when(regex = r#"^I create and delete (\d+) streams named "([^"]+)"$"#)]
pub async fn when_churn_streams(world: &mut GlobalContext, count: u32, prefix: String) {
    let client = world.client.as_ref().expect("client");
    for index in 0..count {
        let stream = client
            .create_stream(
                &format!("{prefix}-{index}"),
                &StreamCreateOptions::default(),
            )
            .await
            .expect("create stream");
        client
            .delete_stream(&Identifier::numeric(stream.id).expect("stream id"))
            .await
            .expect("delete stream");
    }
}

#[then(
    regex = r#"^following the metadata changes should return the kinds "([^"]+)" for the stream$"#
)]
pub async fn then_followed_kinds(world: &mut GlobalContext, kinds: String) {
    let expected: Vec<MetadataChangeKind> = kinds
        .split(',')
        .map(|name| MetadataChangeKind::from_str(name.trim()).expect("known kind"))
        .collect();
    let stream_id = world.last_stream_id.expect("stream created");
    let mut records = Vec::new();
    let mut cursor = world.last_change_cursor.expect("held cursor");
    // A two phase delete commits its record after the call returns.
    for _ in 0..FOLLOW_ATTEMPTS {
        let page = read_changes(world, MetadataChangeStart::After(cursor))
            .await
            .expect("change page");
        assert_eq!(page.status, MetadataChangeStatus::Ok);
        cursor = page.next.expect("continuation");
        records.extend(page.records);
        if page.has_more {
            continue;
        }
        let names_stream =
            |record: &&MetadataChangeRecord| change_stream_id(record) == Some(stream_id);
        if records.iter().filter(names_stream).count() >= expected.len() {
            break;
        }
        sleep(FOLLOW_INTERVAL).await;
    }
    for record in &records {
        let group = record.change.group().expect("known group");
        assert!(world.last_change_groups.contains(&group), "{record:?}");
    }
    let mine: Vec<MetadataChangeRecord> = records
        .into_iter()
        .filter(|record| change_stream_id(record) == Some(stream_id))
        .collect();
    let kinds: Vec<MetadataChangeKind> = mine
        .iter()
        .filter_map(|record| record.change.kind())
        .collect();
    assert_eq!(kinds, expected);
    world.last_changes = mine;
}

#[then("each followed change should name the root user as its actor")]
pub async fn then_root_actor(world: &mut GlobalContext) {
    for record in &world.last_changes {
        assert_eq!(record.actor_user_id, Some(ROOT_USER_ID), "{record:?}");
    }
}

#[then(regex = r#"^the followed "([^"]+)" change should carry a requested op before its own op$"#)]
pub async fn then_requested_op(world: &mut GlobalContext, kind: String) {
    let record = followed(world, &kind);
    let MetadataChange::StreamDeleted(deleted) = &record.change else {
        panic!("expected a stream deletion, got {record:?}");
    };
    let requested = deleted.requested_op.expect("requested op");
    assert!(requested < record.op, "{record:?}");
}

#[then(regex = r#"^the followed "([^"]+)" change should report encryption key source "([^"]+)"$"#)]
pub async fn then_change_key_source(world: &mut GlobalContext, kind: String, source: String) {
    let options = followed_stream_options(world, &kind);
    let expected: &'static str = StreamEncryptionSource::from_str(&source)
        .expect("known source")
        .into();
    let reported = options
        .get(&HeaderKey::from(OptionKey::EncryptionKeySource))
        .and_then(|option| option.value.as_str().ok().map(str::to_owned));
    assert_eq!(reported.as_deref(), Some(expected));
}

#[then(regex = r#"^the followed "([^"]+)" change should carry the stream key fingerprint$"#)]
pub async fn then_change_fingerprint(world: &mut GlobalContext, kind: String) {
    let options = followed_stream_options(world, &kind);
    let key = world.last_stream_key.as_ref().expect("stream key");
    let reported = options
        .get(&HeaderKey::from(OptionKey::EncryptionKeyFingerprint))
        .and_then(|option| option.value.as_str().ok().map(str::to_owned));
    assert_eq!(reported, Some(key.fingerprint()));
}

#[then("the followed changes should expose no encryption key material")]
pub async fn then_changes_hide_keys(world: &mut GlobalContext) {
    for record in &world.last_changes {
        let (MetadataChange::StreamCreated(stream) | MetadataChange::StreamUpdated(stream)) =
            &record.change
        else {
            continue;
        };
        let MetadataOptions::Readable(options) = &stream.options else {
            panic!("stream options must stay readable: {record:?}");
        };
        for hidden in HIDDEN_OPTIONS {
            assert!(!options.contains_key(&HeaderKey::from(hidden)), "{hidden}");
        }
    }
}

#[then(regex = r#"^following the metadata changes should answer "([^"]+)"$"#)]
pub async fn then_follow_status(world: &mut GlobalContext, status: String) {
    let expected = MetadataChangeStatus::from_str(&status).expect("known status");
    let cursor = world.last_change_cursor.expect("held cursor");
    let page = read_changes(world, MetadataChangeStart::After(cursor))
        .await
        .expect("change page");
    assert_eq!(page.status, expected);
    if expected != MetadataChangeStatus::Ok {
        assert!(page.records.is_empty());
        assert_eq!(page.next, None);
    }
}

#[when(regex = r#"^I read the current state for the groups "([^"]+)"$"#)]
pub async fn when_read_current(world: &mut GlobalContext, groups: String) {
    world.last_change_groups = groups
        .split(',')
        .map(|name| MetadataChangeGroup::from_str(name.trim()).expect("known group"))
        .collect();
    let mut start = MetadataChangeStart::Current(None);
    let mut items = Vec::new();
    loop {
        let page = read_changes(world, start).await.expect("current page");
        assert_eq!(page.status, MetadataChangeStatus::Ok);
        items.extend(page.items);
        match page.current_page_token {
            Some(token) => start = MetadataChangeStart::Current(Some(token)),
            None => {
                world.last_change_cursor = Some(page.next.expect("anchor"));
                break;
            }
        }
    }
    world.last_current_items = items;
}

#[then("the current state should list the same streams as the stream list")]
pub async fn then_current_matches_list(world: &mut GlobalContext) {
    let client = world.client.as_ref().expect("client");
    let listed: BTreeMap<u32, String> = client
        .get_streams()
        .await
        .expect("streams")
        .into_iter()
        .map(|stream| (stream.id, stream.name))
        .collect();
    let projected: BTreeMap<u32, String> = world
        .last_current_items
        .iter()
        .filter_map(|item| match item {
            MetadataCurrentItem::Stream(stream) => Some((stream.stream_id, stream.name.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(projected, listed);
}

#[given(
    regex = r#"^a user named "([^"]+)" with password "([^"]+)" and no server permissions exists$"#
)]
pub async fn given_user_without_server_permissions(
    world: &mut GlobalContext,
    username: String,
    password: String,
) {
    let client = world.client.as_ref().expect("client");
    let user = client
        .create_user(
            &username,
            &password,
            UserStatus::Active,
            Some(Permissions {
                global: GlobalPermissions {
                    read_servers: true,
                    ..GlobalPermissions::default()
                },
                streams: None,
            }),
            &UserCreateOptions::default(),
        )
        .await
        .expect("create user");
    world.last_user_id = Some(user.id);
}

#[when(regex = r#"^I authenticate as "([^"]+)" with password "([^"]+)"$"#)]
pub async fn when_authenticate_as(world: &mut GlobalContext, username: String, password: String) {
    let client = world.client.as_ref().expect("client");
    client.logout_user().await.expect("logout");
    client
        .login_user(&username, &password)
        .await
        .expect("login");
}

#[then("reading the metadata changes should be refused as unauthorized")]
pub async fn then_changes_refused(world: &mut GlobalContext) {
    world.last_change_groups = Vec::new();
    let error = read_changes(world, MetadataChangeStart::Earliest)
        .await
        .expect_err("manage_servers is required");
    assert_eq!(error.as_code(), IggyError::Unauthorized.as_code());
}

async fn read_changes(
    world: &GlobalContext,
    start: MetadataChangeStart,
) -> Result<MetadataChanges, IggyError> {
    let client = world.client.as_ref().expect("client");
    client
        .get_metadata_changes(&MetadataChangesQuery {
            start,
            groups: world.last_change_groups.clone(),
            limit: MAX_METADATA_CHANGES_LIMIT,
            ..MetadataChangesQuery::default()
        })
        .await
}

fn followed<'a>(world: &'a GlobalContext, kind: &str) -> &'a MetadataChangeRecord {
    let kind = MetadataChangeKind::from_str(kind).expect("known kind");
    world
        .last_changes
        .iter()
        .find(|record| record.change.kind() == Some(kind))
        .expect("followed change of that kind")
}

fn followed_stream_options(world: &GlobalContext, kind: &str) -> ResourceOptions {
    let record = followed(world, kind);
    let (MetadataChange::StreamCreated(stream) | MetadataChange::StreamUpdated(stream)) =
        &record.change
    else {
        panic!("expected a stream change, got {record:?}");
    };
    let MetadataOptions::Readable(options) = &stream.options else {
        panic!("stream options must stay readable: {record:?}");
    };
    options.clone()
}

const fn change_stream_id(record: &MetadataChangeRecord) -> Option<u32> {
    match &record.change {
        MetadataChange::StreamCreated(stream) | MetadataChange::StreamUpdated(stream) => {
            Some(stream.stream_id)
        }
        MetadataChange::StreamDeleted(stream) => Some(stream.stream_id),
        MetadataChange::TopicCreated(topic) | MetadataChange::TopicUpdated(topic) => {
            Some(topic.stream_id)
        }
        MetadataChange::TopicDeleted(topic) => Some(topic.stream_id),
        MetadataChange::PartitionsChanged(partitions) => Some(partitions.stream_id),
        _ => None,
    }
}
