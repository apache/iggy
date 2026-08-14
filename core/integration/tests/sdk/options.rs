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

use iggy::prelude::*;
use iggy_common::{OptionsScope, TOPIC_OPTION_KEYS, topic_option_keys};
use integration::iggy_harness;
use std::collections::BTreeMap;
use std::str::FromStr;

#[iggy_harness]
async fn given_topic_scope_when_describing_options_should_list_the_catalog(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    let specs = client.describe_options(OptionsScope::Topic).await.unwrap();
    let mut served: Vec<&str> = specs.iter().map(|spec| spec.key.as_str()).collect();
    served.sort_unstable();
    let mut accepted: Vec<&str> = TOPIC_OPTION_KEYS.to_vec();
    accepted.sort_unstable();
    // Exact equality, not per-key contains: the catalog is the only way a
    // client discovers a key, and create rejects anything outside it, so a key
    // accepted but unlisted is undiscoverable and one listed but unaccepted is
    // a lie.
    assert_eq!(served, accepted, "catalog must match the accepted key set");
    // `partitions_count` is deliberately absent: it is a fixed field of the
    // CreateTopic command, not a topic setting.
    assert!(!served.contains(&"partitions_count"));
}

#[iggy_harness]
async fn given_runtime_knobs_when_creating_topic_should_persist_them_per_topic(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("knob-stream").await.unwrap();
    let stream = Identifier::named("knob-stream").unwrap();

    // Zero is rejected: a flush threshold of zero never trips.
    let zero_threshold = client
        .create_topic(
            &stream,
            "knob-zero",
            &TopicCreateOptions {
                partitions_count: Some(1),
                messages_required_to_save: Some(0),
                ..TopicCreateOptions::default()
            },
        )
        .await;
    assert!(zero_threshold.is_err(), "zero flush threshold must reject");

    client
        .create_topic(
            &stream,
            "knob-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                enforce_fsync: Some(true),
                messages_required_to_save: Some(7),
                size_of_messages_required_to_save: Some(IggyByteSize::from(4096u64)),
                segment_size: Some(IggyByteSize::from(1024 * 1024u64)),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();

    let details = client
        .get_topic(&stream, &Identifier::named("knob-topic").unwrap())
        .await
        .unwrap()
        .expect("topic exists");
    for (key, expected_explicit) in [
        (topic_option_keys::ENFORCE_FSYNC, true),
        (topic_option_keys::MESSAGES_REQUIRED_TO_SAVE, true),
        (topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE, true),
        // Never sent, so admission derived it from the built-in default.
        (topic_option_keys::PREALLOCATE_SEGMENTS, false),
    ] {
        let option = details
            .options
            .get(&HeaderKey::from_str(key).unwrap())
            .unwrap_or_else(|| panic!("{key} echoes back"));
        assert_eq!(option.explicit, expected_explicit, "{key} provenance");
    }

    // Messages still flow with the per-topic thresholds installed.
    let mut messages = vec![
        IggyMessage::builder()
            .payload("knob".into())
            .build()
            .unwrap(),
    ];
    let topic = Identifier::named("knob-topic").unwrap();
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
    let polled = client
        .poll_messages(
            &stream,
            &topic,
            Some(0),
            &Consumer::new(Identifier::numeric(1).unwrap()),
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .unwrap();
    assert_eq!(polled.messages.len(), 1);
}

#[iggy_harness]
async fn given_stream_scope_when_describing_options_should_be_empty(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    let specs = client.describe_options(OptionsScope::Stream).await.unwrap();
    assert!(specs.is_empty(), "streams have no catalog keys yet");
}

#[iggy_harness]
async fn given_explicit_segment_size_when_creating_topic_should_roll_at_it(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("seg-stream").await.unwrap();
    let stream = Identifier::named("seg-stream").unwrap();

    // Below the floor: denied with the key name before consensus.
    let too_small = client
        .create_topic(
            &stream,
            "seg-too-small",
            &TopicCreateOptions {
                partitions_count: Some(1),
                segment_size: Some(IggyByteSize::from(1024u64)),
                ..TopicCreateOptions::default()
            },
        )
        .await;
    assert!(too_small.is_err(), "1 KiB segment must be rejected");

    client
        .create_topic(
            &stream,
            "seg-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                segment_size: Some(IggyByteSize::from(1024 * 1024u64)),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();

    let topic = Identifier::named("seg-topic").unwrap();
    let details = client
        .get_topic(&stream, &topic)
        .await
        .unwrap()
        .expect("topic exists");
    let segment_key = HeaderKey::from_str(topic_option_keys::SEGMENT_SIZE).unwrap();
    let segment = details
        .options
        .get(&segment_key)
        .expect("explicit segment size echoes back");
    assert!(segment.explicit);
    assert_eq!(
        u64::from_le_bytes(segment.value.as_bytes().try_into().unwrap()),
        1024 * 1024
    );

    // Push ~3 MiB through a 1 MiB segment: the partition must roll.
    let payload = vec![b'x'; 512 * 1024];
    for _ in 0..6 {
        let mut messages = vec![
            IggyMessage::builder()
                .payload(payload.clone().into())
                .build()
                .unwrap(),
        ];
        client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .unwrap();
    }
    let details = client
        .get_topic(&stream, &topic)
        .await
        .unwrap()
        .expect("topic exists");
    assert!(
        details.partitions[0].segments_count > 1,
        "3 MiB through a 1 MiB segment must roll at least once, got {} segments",
        details.partitions[0].segments_count
    );
}

#[iggy_harness]
async fn given_explicit_expiry_when_creating_topic_should_echo_provenance(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("options-stream").await.unwrap();
    client
        .create_topic(
            &Identifier::named("options-stream").unwrap(),
            "options-topic",
            &TopicCreateOptions {
                partitions_count: Some(2),
                message_expiry: Some(IggyExpiry::ExpireDuration("5m".parse().unwrap())),
                segment_size: Some(IggyByteSize::from(1024 * 1024u64)),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();

    let details = client
        .get_topic(
            &Identifier::named("options-stream").unwrap(),
            &Identifier::named("options-topic").unwrap(),
        )
        .await
        .unwrap()
        .expect("topic exists");
    assert_eq!(details.partitions_count, 2);

    let expiry_key = HeaderKey::from_str(topic_option_keys::MESSAGE_EXPIRY).unwrap();
    let expiry = details
        .options
        .get(&expiry_key)
        .expect("explicit expiry echoes back");
    assert!(expiry.explicit, "client-sent key keeps its provenance");

    let size_key = HeaderKey::from_str(topic_option_keys::MAX_TOPIC_SIZE).unwrap();
    let size = details
        .options
        .get(&size_key)
        .expect("derived size echoes back");
    assert!(!size.explicit, "default-filled key is marked derived");

    // partitions_count never reaches the options map: it is a command field.
    let partitions_key = HeaderKey::from_str("partitions_count").unwrap();
    assert!(!details.options.contains_key(&partitions_key));
}

#[iggy_harness]
async fn given_update_options_when_updating_topic_should_patch_not_replace(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("update-stream").await.unwrap();
    let stream = Identifier::named("update-stream").unwrap();
    let topic = Identifier::named("update-topic").unwrap();

    client
        .create_topic(
            &stream,
            "update-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                segment_size: Some(IggyByteSize::from(1024 * 1024u64)),
                enforce_fsync: Some(true),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();

    // A create-time knob is refused by name: nothing re-pushes it to the
    // partitions, so storing it would read as applied and not be.
    let create_only = client
        .update_topic(
            &stream,
            &topic,
            "update-topic",
            &TopicUpdateOptions {
                raw: BTreeMap::from([(
                    topic_option_keys::SEGMENT_SIZE.to_string(),
                    "2MiB".to_string(),
                )]),
                ..TopicUpdateOptions::default()
            },
        )
        .await;
    assert!(create_only.is_err(), "segment_size must be create-only");

    client
        .update_topic(
            &stream,
            &topic,
            "update-topic",
            &TopicUpdateOptions::default(),
        )
        .await
        .unwrap();

    let details = client
        .get_topic(&stream, &topic)
        .await
        .unwrap()
        .expect("topic exists");
    // The keys the update did not mention keep the values create resolved.
    for (key, expected_explicit) in [
        (topic_option_keys::SEGMENT_SIZE, true),
        (topic_option_keys::ENFORCE_FSYNC, true),
        (topic_option_keys::PREALLOCATE_SEGMENTS, false),
    ] {
        let option = details
            .options
            .get(&HeaderKey::from_str(key).unwrap())
            .unwrap_or_else(|| panic!("{key} survives the update"));
        assert_eq!(option.explicit, expected_explicit, "{key} provenance");
    }
    let segment_key = HeaderKey::from_str(topic_option_keys::SEGMENT_SIZE).unwrap();
    let segment = details.options.get(&segment_key).unwrap();
    assert_eq!(
        u64::from_le_bytes(segment.value.as_bytes().try_into().unwrap()),
        1024 * 1024,
        "an update must not reset a key it never sent"
    );
}

#[iggy_harness]
async fn given_unknown_key_when_updating_stream_or_user_should_reject(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("opt-stream").await.unwrap();

    // Streams and users have no catalog keys yet, so the update blocks exist
    // as the extension point and reject every key by name rather than storing
    // one nothing reads.
    let stream_rejected = client
        .update_stream(
            &Identifier::named("opt-stream").unwrap(),
            "opt-stream",
            &StreamUpdateOptions {
                raw: BTreeMap::from([("future_key".to_string(), "1".to_string())]),
            },
        )
        .await;
    assert!(stream_rejected.is_err(), "unknown stream key must reject");

    // An empty block still succeeds: that is the plain rename path.
    client
        .update_stream(
            &Identifier::named("opt-stream").unwrap(),
            "opt-stream-renamed",
            &StreamUpdateOptions::default(),
        )
        .await
        .unwrap();

    client
        .create_user("opt-user", "secret123", UserStatus::Active, None)
        .await
        .unwrap();
    let user = Identifier::named("opt-user").unwrap();
    let user_rejected = client
        .update_user(
            &user,
            Some("opt-user"),
            None,
            &UserUpdateOptions {
                raw: BTreeMap::from([("future_key".to_string(), "1".to_string())]),
            },
        )
        .await;
    assert!(user_rejected.is_err(), "unknown user key must reject");

    client
        .update_user(
            &user,
            Some("opt-user-renamed"),
            None,
            &UserUpdateOptions::default(),
        )
        .await
        .unwrap();
}

#[iggy_harness]
async fn given_sentinel_zeros_when_creating_topic_should_report_resolved_defaults(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("sentinel-stream").await.unwrap();
    let stream = Identifier::named("sentinel-stream").unwrap();

    // 0 means "resolve the server default", not "expire immediately" / "no
    // space". Admission normalizes it away, so the stored map must report the
    // resolved value as derived -- never a literal 0 marked explicit, which is
    // what a client would then read back as the effective configuration.
    client
        .create_topic(
            &stream,
            "sentinel-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::from(0u64)),
                max_topic_size: Some(MaxTopicSize::from(0u64)),
                segment_size: Some(IggyByteSize::from(0u64)),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();

    let details = client
        .get_topic(&stream, &Identifier::named("sentinel-topic").unwrap())
        .await
        .unwrap()
        .expect("topic exists");

    for key in [
        topic_option_keys::MESSAGE_EXPIRY,
        topic_option_keys::MAX_TOPIC_SIZE,
        topic_option_keys::SEGMENT_SIZE,
    ] {
        let option = details
            .options
            .get(&HeaderKey::from_str(key).unwrap())
            .unwrap_or_else(|| panic!("{key} resolves to a default rather than vanishing"));
        assert!(
            !option.explicit,
            "{key} sentinel must resolve to a derived default, not persist as explicit"
        );
        assert_ne!(
            option.value.as_bytes(),
            0u64.to_le_bytes(),
            "{key} must report the resolved value, not the sentinel"
        );
    }
}

#[iggy_harness]
async fn given_rename_only_when_updating_topic_should_leave_settings_alone(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("patch-stream").await.unwrap();
    let stream = Identifier::named("patch-stream").unwrap();

    client
        .create_topic(
            &stream,
            "patch-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                compression_algorithm: Some(CompressionAlgorithm::Gzip),
                message_expiry: Some(IggyExpiry::from(5_000_000u64)),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::named("patch-topic").unwrap();

    // Settings live only in the options block, so an update that carries none
    // of them is a pure rename. While they were fixed fields of the command
    // this was impossible: every update rewrote all three whether or not the
    // caller meant to.
    client
        .update_topic(
            &stream,
            &topic,
            "patch-renamed",
            &TopicUpdateOptions::default(),
        )
        .await
        .unwrap();

    let details = client
        .get_topic(&stream, &Identifier::named("patch-renamed").unwrap())
        .await
        .unwrap()
        .expect("topic exists");
    assert_eq!(details.name, "patch-renamed");
    assert_eq!(details.compression_algorithm, CompressionAlgorithm::Gzip);
    assert_eq!(details.message_expiry, IggyExpiry::from(5_000_000u64));

    // Sending one key changes that key and leaves the other alone.
    client
        .update_topic(
            &stream,
            &Identifier::named("patch-renamed").unwrap(),
            "patch-renamed",
            &TopicUpdateOptions {
                message_expiry: Some(IggyExpiry::from(9_000_000u64)),
                ..TopicUpdateOptions::default()
            },
        )
        .await
        .unwrap();

    let details = client
        .get_topic(&stream, &Identifier::named("patch-renamed").unwrap())
        .await
        .unwrap()
        .expect("topic exists");
    assert_eq!(details.message_expiry, IggyExpiry::from(9_000_000u64));
    assert_eq!(
        details.compression_algorithm,
        CompressionAlgorithm::Gzip,
        "a key the update did not carry keeps its value"
    );
}
