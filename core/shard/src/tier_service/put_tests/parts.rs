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

//! Uploads in parts against the scripted provider, on the virtual clock.

use super::*;

/// Parts of 1 MiB, at most three in flight, no deadline.
fn parts_policy() -> StoreCallPolicy {
    StoreCallPolicy {
        parts: PartsPolicy {
            part_len: MIB as u64,
            in_flight: 3,
            deadline: None,
        },
        ..policy()
    }
}

fn layout() -> KeyLayout {
    KeyLayout::new("iggy").unwrap()
}

/// A store over a provider that takes uploads in parts, with `policy`.
fn parts_store_with(policy: StoreCallPolicy) -> (VirtualBus, Store) {
    let bus = VirtualBus::default();
    let provider = ScriptedStore::new(&bus);
    provider.limits.set(Some(PartsLimits {
        min_part_len: 1,
        max_parts: 32,
    }));
    let store = TierStore::new(
        provider,
        bus.clone(),
        policy,
        StoreWriter {
            replica: 1,
            shard: 2,
            layout: Some(layout()),
            ..StoreWriter::default()
        },
    );
    (bus, store)
}

fn parts_store() -> (VirtualBus, Store) {
    parts_store_with(parts_policy())
}

/// `mib` MiB whose every MiB differs, so parts out of order would show.
fn large(mib: usize) -> Bytes {
    (0..mib * MIB)
        .map(|index| u8::try_from(index / MIB).unwrap_or(u8::MAX) ^ 0x5a)
        .collect::<Vec<u8>>()
        .into()
}

/// What the parts of `store` did: uploads begun, completed, aborted, fallen
/// back to a whole put, and parts sent.
fn parts_counted(store: &Store) -> (u64, u64, u64, u64, u64) {
    store.counters().parts_values()
}

/// The options of a create-only put of `bytes` that may go up in parts, as
/// those of the long-term rewrite.
fn in_parts(bytes: &Bytes) -> PutOptions {
    PutOptions {
        parts: PutParts::Allowed,
        ..create_only(bytes)
    }
}

fn commit_after(delay: Duration) -> Script {
    Script::commit_after(delay)
}

#[test]
fn given_a_large_body_when_putting_should_send_its_parts_side_by_side_within_the_budget() {
    let (bus, store) = parts_store();
    let bytes = large(8);
    store
        .provider()
        .part_scripts
        .borrow_mut()
        .extend([commit_after(Duration::from_millis(100)); 8]);

    bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)))
        .unwrap();

    let provider = store.provider();
    assert_eq!(provider.most_parts_running.get(), 3);
    // Three waves of at most three parts.
    assert_eq!(bus.now(), Duration::from_millis(300));
    assert_eq!(stored(&store, "a/b"), bytes);
    assert_eq!(provider.puts.borrow().len(), 0, "no whole put");
    assert_eq!(parts_counted(&store), (1, 1, 0, 0, 8));
    assert_eq!(store.parts_in_flight(), 0);
    let held = futures::executor::block_on(provider.objects.head("a/b")).unwrap();
    assert!(held.metadata.contains_key(PUT_ID_METADATA));
}

#[test]
fn given_a_part_that_fails_once_should_send_it_again_and_complete() {
    let (bus, store) = parts_store();
    let bytes = large(4);
    store
        .provider()
        .part_scripts
        .borrow_mut()
        .push_back(Script::of(Outcome::Refuse(|| {
            ObjectStoreError::Unavailable("503".to_owned())
        })));

    bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)))
        .unwrap();

    assert_eq!(stored(&store, "a/b"), bytes);
    assert_eq!(store.provider().part_attempts.get(), 5);
    assert_eq!(counted(&store).0, 1, "one retry");
    assert_eq!(parts_counted(&store), (1, 1, 0, 0, 5));
}

#[test]
fn given_a_part_past_the_threshold_of_a_second_attempt_should_wait_for_its_first() {
    let (bus, store) = parts_store();
    let bytes = large(2);
    let slow = Duration::from_secs(4);
    store
        .provider()
        .part_scripts
        .borrow_mut()
        .extend([commit_after(slow), commit_after(FAST)]);

    bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)))
        .unwrap();

    // A second attempt that loses could land after the object exists, so the
    // slow part is waited for.
    assert!(slow > THRESHOLD);
    assert_eq!(bus.now(), slow);
    assert_eq!(store.provider().part_attempts.get(), 2);
    assert_eq!(counted(&store), (0, 0, 0, 0));
    assert_eq!(stored(&store, "a/b"), bytes);
    assert_eq!(store.provider().commits.get(), 1);
}

#[test]
fn given_a_part_that_fails_for_good_should_abort_the_upload_and_return_its_error() {
    let (bus, store) = parts_store();
    let bytes = large(4);
    store
        .provider()
        .part_scripts
        .borrow_mut()
        .push_back(Script::of(Outcome::Refuse(|| {
            ObjectStoreError::Auth("denied".to_owned())
        })));

    let failed = bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)));

    assert!(
        matches!(failed, Err(ObjectStoreError::Auth(_))),
        "{failed:?}"
    );
    let provider = store.provider();
    assert_eq!(provider.aborts.get(), 1);
    assert!(provider.uploads.borrow().is_empty(), "the upload is gone");
    assert!(futures::executor::block_on(provider.objects.head("a/b")).is_err());
    assert_eq!(parts_counted(&store).2, 1, "one upload aborted");
    assert_eq!(store.parts_in_flight(), 0);
}

#[test]
fn given_a_lost_answer_on_completion_when_the_retry_finds_its_object_should_count_as_stored() {
    let (bus, store) = parts_store();
    let bytes = large(2);
    store
        .provider()
        .complete_scripts
        .borrow_mut()
        .push_back(Script::of(Outcome::LoseAnswer(lost)));

    bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)))
        .unwrap();

    let provider = store.provider();
    assert_eq!(provider.completions.get(), 1, "the object decided");
    assert_eq!(provider.heads.get(), 1);
    assert_eq!(provider.commits.get(), 1);
    assert_eq!(counted(&store).3, 1, "own object found");
    assert_eq!(stored(&store, "a/b"), bytes);
    assert_eq!(provider.aborts.get(), 1, "what the upload kept is removed");
    assert_eq!(parts_counted(&store), (1, 1, 0, 0, 2));
}

/// The error the S3 provider returns for a 200 that holds neither the result
/// of the completion nor an error.
#[test]
fn given_a_completion_whose_200_holds_no_result_when_it_created_the_object_should_count_as_stored()
{
    let (bus, store) = parts_store();
    let bytes = large(2);
    store
        .provider()
        .complete_scripts
        .borrow_mut()
        .push_back(Script::of(Outcome::LoseAnswer(|| {
            ObjectStoreError::Unavailable("a 200 that holds no result".to_owned())
        })));

    bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)))
        .unwrap();

    let provider = store.provider();
    assert_eq!(provider.completions.get(), 1, "the object decided");
    assert_eq!(provider.heads.get(), 1);
    assert_eq!(counted(&store).3, 1, "own object found");
    assert_eq!(stored(&store, "a/b"), bytes);
}

#[test]
fn given_a_lost_answer_on_completion_when_another_writer_holds_the_key_should_refuse_and_abort() {
    let (bus, store) = parts_store();
    let bytes = large(2);
    put_by_another_writer(&store, "a/b", &bytes, UserMetadata::new());
    store
        .provider()
        .complete_scripts
        .borrow_mut()
        .push_back(Script::of(Outcome::Refuse(lost)));

    let refused = bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)));

    assert!(
        matches!(refused, Err(ObjectStoreError::PreconditionFailed { .. })),
        "{refused:?}"
    );
    let provider = store.provider();
    assert_eq!(provider.heads.get(), 1);
    assert_eq!(provider.aborts.get(), 1);
    assert!(provider.uploads.borrow().is_empty());
    assert_eq!(parts_counted(&store).2, 1);
}

/// A refusal that no completion of this upload can explain is a conflict,
/// which the caller's own check settles.
#[test]
fn given_another_writers_object_when_the_first_completion_is_refused_should_return_it_unchecked() {
    let (bus, store) = parts_store();
    let bytes = large(2);
    put_by_another_writer(&store, "a/b", &large(3), UserMetadata::new());

    let refused = bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)));

    assert!(
        matches!(refused, Err(ObjectStoreError::PreconditionFailed { .. })),
        "{refused:?}"
    );
    let provider = store.provider();
    assert_eq!(provider.heads.get(), 0);
    assert_eq!(provider.completions.get(), 1);
    assert_eq!(provider.aborts.get(), 1);
}

#[test]
fn given_a_completion_that_finds_the_upload_gone_should_upload_again() {
    let (bus, store) = parts_store();
    let bytes = large(2);
    store
        .provider()
        .complete_scripts
        .borrow_mut()
        .push_back(Script::of(Outcome::Refuse(|| ObjectStoreError::NotFound {
            key: "a/b".to_owned(),
        })));

    bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)))
        .unwrap();

    let provider = store.provider();
    assert_eq!(provider.begun.get(), 2);
    assert_eq!(stored(&store, "a/b"), bytes);
    assert_eq!(parts_counted(&store), (2, 1, 1, 0, 4));
}

/// A put of a fresh object, which producers wait for, never opts in.
#[test]
fn given_a_large_create_only_body_without_the_opt_in_when_putting_should_put_it_whole_and_count_no_fallback()
 {
    let (bus, store) = parts_store();
    let bytes = large(4);

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();

    let provider = store.provider();
    assert_eq!(provider.begun.get(), 0);
    assert_eq!(provider.part_attempts.get(), 0);
    assert_eq!(provider.puts.borrow().len(), 1);
    assert_eq!(stored(&store, "a/b"), bytes);
    assert_eq!(parts_counted(&store), (0, 0, 0, 0, 0));
}

#[test]
fn given_a_large_create_only_body_with_the_opt_in_when_putting_should_put_it_in_parts() {
    let (bus, store) = parts_store();
    let bytes = large(4);

    bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)))
        .unwrap();

    let provider = store.provider();
    assert_eq!(provider.begun.get(), 1);
    assert_eq!(provider.part_attempts.get(), 4);
    assert!(provider.puts.borrow().is_empty());
    assert_eq!(stored(&store, "a/b"), bytes);
    assert_eq!(parts_counted(&store), (1, 1, 0, 0, 4));
}

#[test]
fn given_a_body_under_twice_the_part_or_a_provider_without_parts_should_put_it_whole() {
    let (bus, store) = parts_store();
    let small: Bytes = vec![7; MIB + MIB / 2].into();

    bus.run(store.put("a/small", body(&small), &in_parts(&small)))
        .unwrap();

    assert_eq!(store.provider().begun.get(), 0);
    assert_eq!(store.provider().puts.borrow().len(), 1);
    let (bus, store) = parts_store();
    store.provider().limits.set(None);
    let bytes = large(4);

    bus.run(store.put("a/large", body(&bytes), &in_parts(&bytes)))
        .unwrap();

    assert_eq!(store.provider().begun.get(), 0);
    assert_eq!(store.provider().puts.borrow().len(), 1);
    assert_eq!(parts_counted(&store), (0, 0, 0, 0, 0));
}

#[test]
fn given_an_overwrite_or_a_streamed_body_should_put_it_whole() {
    let (bus, store) = parts_store();
    let bytes = large(4);

    let overwrite = PutOptions {
        parts: PutParts::Allowed,
        ..PutOptions::default()
    };
    bus.run(store.put("a/b", body(&bytes), &overwrite)).unwrap();
    let streamed = PutBody::from_stream(
        bytes.len() as u64,
        futures::stream::iter([Ok(bytes.clone())]),
    );
    bus.run(store.put("a/c", streamed, &in_parts(&bytes)))
        .unwrap();

    assert_eq!(store.provider().begun.get(), 0);
    assert_eq!(store.provider().puts.borrow().len(), 2);
}

#[test]
fn given_a_store_that_refuses_uploads_in_parts_should_put_whole_and_remember_it() {
    let (bus, store) = parts_store();
    store
        .provider()
        .begin_refusals
        .borrow_mut()
        .push_back(|| ObjectStoreError::Unsupported("multipart".to_owned()));
    let (first, second) = (large(4), large(5));

    bus.run(store.put("a/1", body(&first), &in_parts(&first)))
        .unwrap();
    bus.run(store.put("a/2", body(&second), &in_parts(&second)))
        .unwrap();

    let provider = store.provider();
    assert_eq!(provider.begun.get(), 1, "asked once");
    assert_eq!(provider.puts.borrow().len(), 2);
    assert_eq!(stored(&store, "a/1"), first);
    assert_eq!(stored(&store, "a/2"), second);
    assert_eq!(parts_counted(&store), (0, 0, 0, 2, 0));
}

#[test]
fn given_two_puts_in_parts_should_share_the_budget_and_each_send_one_part_at_least() {
    let (bus, store) = parts_store();
    let (first, second) = (large(4), large(4));
    store
        .provider()
        .part_scripts
        .borrow_mut()
        .extend([commit_after(Duration::from_millis(100)); 8]);
    let (first_options, second_options) = (in_parts(&first), in_parts(&second));
    let both = future::join(
        store.put("a/1", body(&first), &first_options),
        store.put("a/2", body(&second), &second_options),
    );

    let (one, two) = bus.run(both);

    one.unwrap();
    two.unwrap();
    // Three slots for the first, and one part of the second outside them.
    assert_eq!(store.provider().most_parts_running.get(), 4);
    assert_eq!(store.parts_in_flight(), 0);
    assert_eq!(stored(&store, "a/1"), first);
    assert_eq!(stored(&store, "a/2"), second);
}

#[test]
fn given_a_put_in_parts_dropped_midway_should_free_its_slots_and_leave_what_the_sweep_removes() {
    let (bus, store) = parts_store();
    let bytes = large(4);
    store
        .provider()
        .part_scripts
        .borrow_mut()
        .extend([commit_after(Duration::from_secs(60)); 4]);
    let options = in_parts(&bytes);
    let put = store.put("a/b", body(&bytes), &options).boxed_local();

    let raced = bus.run(future::select(put, bus.sleep(Duration::from_secs(1))));
    let future::Either::Right(((), put)) = raced else {
        panic!("the put ended before the timer");
    };
    drop(put);

    assert_eq!(store.parts_in_flight(), 0, "the slots are free");
    assert_eq!(
        store.provider().uploads.borrow().len(),
        1,
        "the upload stays"
    );
    let grace = Duration::from_secs(3600);
    let early = bus
        .run(store.sweep_uploads(&layout(), grace, None))
        .unwrap();
    assert_eq!((early.removed, early.kept), (0, 1));
    bus.run_for(grace);
    let swept = bus
        .run(store.sweep_uploads(&layout(), grace, None))
        .unwrap();
    assert_eq!((swept.removed, swept.kept), (1, 0));
    assert!(store.provider().uploads.borrow().is_empty());
    // A new put has the whole budget.
    store.provider().most_parts_running.set(0);
    store
        .provider()
        .part_scripts
        .borrow_mut()
        .extend([commit_after(Duration::from_millis(100)); 4]);
    bus.run(store.put("a/c", body(&bytes), &in_parts(&bytes)))
        .unwrap();
    assert_eq!(store.provider().most_parts_running.get(), 3);
}

#[test]
fn given_an_upload_in_parts_past_its_deadline_should_abort_it_and_fail_as_transient() {
    let (bus, store) = parts_store_with(StoreCallPolicy {
        parts: PartsPolicy {
            deadline: Some(Duration::from_secs(2)),
            ..parts_policy().parts
        },
        ..parts_policy()
    });
    let bytes = large(2);
    store
        .provider()
        .part_scripts
        .borrow_mut()
        .extend([Script::of(Outcome::Hang); 8]);

    let failed = bus.run(store.put("a/b", body(&bytes), &in_parts(&bytes)));

    assert!(
        matches!(failed, Err(ObjectStoreError::Transport(_))),
        "{failed:?}"
    );
    assert_eq!(bus.now(), Duration::from_secs(2));
    assert_eq!(store.provider().aborts.get(), 1);
    assert_eq!(store.provider().parts_running.get(), 0, "no part runs on");
    assert_eq!(parts_counted(&store).2, 1);
}
