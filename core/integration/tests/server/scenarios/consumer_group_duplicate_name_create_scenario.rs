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

//! Re-creating a consumer group under an existing name must NOT destroy the
//! live group: the existing group, its members, and their partition
//! assignments have to survive (an ejected member would never recover). This
//! is the non-destructive duplicate-create path that the (HTTP-bound, hence
//! non-vsr) `concurrent_addition` cold matrix does not cover under vsr.

use crate::server::scenarios::{
    CONSUMER_GROUP_NAME, PARTITIONS_COUNT, STREAM_NAME, TOPIC_NAME, USERNAME_1, cleanup,
    create_client, join_consumer_group,
};
use iggy::prelude::CompressionAlgorithm;
use iggy::prelude::Identifier;
use iggy::prelude::IggyExpiry;
use iggy::prelude::MaxTopicSize;
use iggy::prelude::{ConsumerGroupClient, StreamClient, TopicClient};
use integration::harness::{
    TestHarness, assert_clean_system, create_user, delete_user, login_user,
};

pub async fn run(harness: &TestHarness) {
    let system_client = harness
        .root_client()
        .await
        .expect("Failed to get root client");
    let client1 = create_client(harness).await;

    system_client.create_stream(STREAM_NAME).await.unwrap();
    system_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    let original_id = system_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap()
        .id;

    // A member joins and gets all partitions.
    create_user(&system_client, USERNAME_1).await;
    login_user(&client1, USERNAME_1).await;
    join_consumer_group(&client1).await;

    let before = get_group(&system_client).await;
    assert_eq!(
        before.members_count, 1,
        "the joined member must be present before the duplicate create"
    );

    // Re-create the SAME name. The reply may be ok or an already-exists-style
    // error; what matters is that the live group is left intact -- it must not
    // be removed and replaced by a fresh empty group with a new id.
    let _ = system_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await;

    let after = get_group(&system_client).await;
    assert_eq!(
        after.id, original_id,
        "duplicate create must not replace the group with a new id"
    );
    assert_eq!(
        after.members_count, 1,
        "duplicate create must not eject the existing member"
    );
    assert_eq!(after.members.len(), 1);
    assert_eq!(
        after.members[0].partitions_count, PARTITIONS_COUNT,
        "the surviving member must keep its partition assignment"
    );

    // This scenario creates only USERNAME_1, so delete just that user (the
    // shared `cleanup(_, true)` deletes USERNAME_1/2/3 and would fail with
    // UserNotFound on the two it never created); `cleanup(_, false)` drops the
    // stream.
    delete_user(&system_client, USERNAME_1).await;
    cleanup(&system_client, false).await;
    assert_clean_system(&system_client).await;
}

async fn get_group(
    client: &iggy::clients::client::IggyClient,
) -> iggy::prelude::ConsumerGroupDetails {
    client
        .get_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap()
        .expect("consumer group must still exist")
}
