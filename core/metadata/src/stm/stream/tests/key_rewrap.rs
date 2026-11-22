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

use super::*;

#[test]
fn given_stale_rewrap_entry_for_recreated_stream_when_applying_should_skip_it() {
    let old = key_context(OLD_CONFIG_KEY, &[]);
    let rotated = key_context(NEW_CONFIG_KEY, &[OLD_CONFIG_KEY]);
    let mut inner = StreamsInner::new();
    let first = WrapSet::from(envelope(&old, 1, StreamKeyOrigin::Server));
    create_encrypted_stream(&mut inner, "orders", &first, 5);
    let delete = DeleteStreamRequest {
        stream_id: WireIdentifier::numeric(0),
    };
    assert_eq!(
        crate::stm::lifecycle::apply_with_lifecycle_completion(
            &delete,
            &mut inner,
            IggyTimestamp::now()
        )
        .code,
        0
    );
    let second = WrapSet::from(envelope(&old, 2, StreamKeyOrigin::Server));
    create_encrypted_stream(&mut inner, "orders", &second, 9);
    let stale = rotated.target_set(&first).unwrap().unwrap();
    let fresh = rotated.target_set(&second).unwrap().unwrap();

    for skipped in [
        rewrap_entry(0, 5, &first, stale.as_ref()),
        rewrap_entry(0, 9, &first, fresh.as_ref()),
        rewrap_entry(7, 9, &second, fresh.as_ref()),
    ] {
        let reply = StateHandler::apply(
            &RewrapStreamKeysRequest {
                entries: vec![skipped],
            },
            &mut inner,
            IggyTimestamp::now(),
        );
        assert_eq!(replaced_count(&reply), 0);
        assert_eq!(
            stream_wrap_set(&inner.items[0].options).unwrap(),
            Some(second.clone())
        );
    }

    let reply = StateHandler::apply(
        &RewrapStreamKeysRequest {
            entries: vec![rewrap_entry(0, 9, &second, fresh.as_ref())],
        },
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(replaced_count(&reply), 1);
    assert_eq!(
        stream_wrap_set(&inner.items[0].options).unwrap(),
        Some(fresh)
    );
}

#[test]
fn given_rewrap_entry_of_earlier_incarnation_with_same_key_when_applying_should_skip_it() {
    let old = key_context(OLD_CONFIG_KEY, &[]);
    let rotated = key_context(NEW_CONFIG_KEY, &[OLD_CONFIG_KEY]);
    let mut inner = StreamsInner::new();
    let first = WrapSet::from(envelope(&old, 1, StreamKeyOrigin::Client));
    create_encrypted_stream(&mut inner, "orders", &first, 5);
    let delete = DeleteStreamRequest {
        stream_id: WireIdentifier::numeric(0),
    };
    assert_eq!(
        crate::stm::lifecycle::apply_with_lifecycle_completion(
            &delete,
            &mut inner,
            IggyTimestamp::now()
        )
        .code,
        0
    );
    let second = WrapSet::from(envelope(&old, 1, StreamKeyOrigin::Client));
    create_encrypted_stream(&mut inner, "orders", &second, 9);
    let stale = rotated
        .target_set(&first)
        .expect("rewrap target builds")
        .expect("first set lacks the new KEK");

    let reply = StateHandler::apply(
        &RewrapStreamKeysRequest {
            entries: vec![rewrap_entry(0, 5, &first, stale.as_ref())],
        },
        &mut inner,
        IggyTimestamp::now(),
    );

    assert_eq!(replaced_count(&reply), 0);
    assert_eq!(
        stream_wrap_set(&inner.items[0].options).expect("stored set decodes"),
        Some(second)
    );
}

#[test]
fn given_rewrap_entry_of_another_key_when_applying_should_skip_it() {
    let old = key_context(OLD_CONFIG_KEY, &[]);
    let rotated = key_context(NEW_CONFIG_KEY, &[OLD_CONFIG_KEY]);
    let mut inner = StreamsInner::new();
    let stored = WrapSet::from(envelope(&old, 3, StreamKeyOrigin::Server));
    create_encrypted_stream(&mut inner, "payments", &stored, 4);
    let before = inner.items[0].options.clone();
    let other = WrapSet::from(envelope(&old, 9, StreamKeyOrigin::Server));
    let other_rewrapped = rotated
        .target_set(&other)
        .expect("rewrap target builds")
        .expect("other set lacks the new KEK");

    let reply = StateHandler::apply(
        &RewrapStreamKeysRequest {
            entries: vec![rewrap_entry(0, 4, &other, other_rewrapped.as_ref())],
        },
        &mut inner,
        IggyTimestamp::now(),
    );

    assert_eq!(replaced_count(&reply), 0);
    assert_eq!(inner.items[0].options, before);
}

#[test]
fn given_malformed_rewrap_set_when_applying_should_skip_without_mutation() {
    let old = key_context(OLD_CONFIG_KEY, &[]);
    let new = key_context(NEW_CONFIG_KEY, &[]);
    let third = key_context(THIRD_CONFIG_KEY, &[]);
    let mut inner = StreamsInner::new();
    let stored = WrapSet::from(envelope(&old, 3, StreamKeyOrigin::Client));
    create_encrypted_stream(&mut inner, "payments", &stored, 4);
    let before = inner.items[0].options.clone();
    let same_key = |context: &ServerKeyContext| envelope(context, 3, StreamKeyOrigin::Client);
    let mut sorted = vec![same_key(&old), same_key(&new), same_key(&third)];
    sorted.sort_by_key(WrappedStreamKey::kek_fingerprint);
    let concat = |envelopes: &[WrappedStreamKey]| -> Vec<u8> {
        envelopes
            .iter()
            .flat_map(|envelope| envelope.as_ref().to_vec())
            .collect()
    };
    let mut reversed = sorted[..2].to_vec();
    reversed.reverse();
    let malformed = [
        Vec::new(),
        concat(&reversed),
        concat(&[same_key(&old), same_key(&old)]),
        concat(&[same_key(&old), envelope(&new, 9, StreamKeyOrigin::Client)]),
        concat(&[same_key(&old), envelope(&new, 3, StreamKeyOrigin::Server)]),
        concat(
            &[
                sorted.clone(),
                vec![envelope(&new, 3, StreamKeyOrigin::Client)],
            ]
            .concat(),
        ),
    ];
    for set in malformed {
        let reply = StateHandler::apply(
            &RewrapStreamKeysRequest {
                entries: vec![rewrap_entry(0, 4, &stored, &set)],
            },
            &mut inner,
            IggyTimestamp::now(),
        );
        assert_eq!(replaced_count(&reply), 0, "{} bytes", set.len());
        assert_eq!(inner.items[0].options, before);
    }
}

#[test]
fn given_well_formed_set_of_other_key_or_origin_when_rewrapping_should_skip_it() {
    let old = key_context(OLD_CONFIG_KEY, &[]);
    let new = key_context(NEW_CONFIG_KEY, &[]);
    let mut inner = StreamsInner::new();
    let stored = WrapSet::from(envelope(&old, 3, StreamKeyOrigin::Client));
    create_encrypted_stream(&mut inner, "payments", &stored, 4);
    let before = inner.items[0].options.clone();
    let other_key = WrapSet::from(envelope(&new, 9, StreamKeyOrigin::Client));
    let relabeled = WrapSet::from(envelope(&new, 3, StreamKeyOrigin::Server));
    let rewrapped = WrapSet::from(envelope(&new, 3, StreamKeyOrigin::Client));

    let mut replaced = Vec::new();
    for set in [&other_key, &relabeled] {
        let reply = StateHandler::apply(
            &RewrapStreamKeysRequest {
                entries: vec![rewrap_entry(0, 4, &stored, set.as_ref())],
            },
            &mut inner,
            IggyTimestamp::now(),
        );
        replaced.push(replaced_count(&reply));
        assert_eq!(inner.items[0].options, before);
    }
    let reply = StateHandler::apply(
        &RewrapStreamKeysRequest {
            entries: vec![rewrap_entry(0, 4, &stored, rewrapped.as_ref())],
        },
        &mut inner,
        IggyTimestamp::now(),
    );
    replaced.push(replaced_count(&reply));
    assert_eq!(replaced, [0, 0, 1]);
    assert_eq!(
        stream_wrap_set(&inner.items[0].options).unwrap(),
        Some(rewrapped)
    );
}

#[test]
fn given_plaintext_stream_key_when_applying_create_should_refuse_without_storing() {
    let mut options = ResourceOptions::new();
    options.insert(
        HeaderKey::from(OptionKey::EncryptionKey),
        OptionValue::explicit(HeaderValue::try_from([7u8; 32].as_slice()).unwrap()),
    );
    let request = CreateStreamRequest {
        name: WireName::new("secrets").unwrap(),
        options: resource_options_to_wire(&options, OptionsProvenance::All).unwrap(),
    };
    let mut inner = StreamsInner::new();

    let reply = StateHandler::apply(&request, &mut inner, IggyTimestamp::now());

    assert_eq!(
        reply.code,
        u32::from(CreateStreamResult::InvalidOptionValue)
    );
    assert!(inner.items.is_empty());
    assert!(inner.index.is_empty());
}
