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

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::path::Path;
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::channel::oneshot;
use iggy_object_store::{
    ErrorClass, FsStore, ListRequest, ObjectStoreError, PutBody, PutMode, PutOptions, TierScope,
};
use message_bus::IggyMessageBus;

use super::test_store::TestStore;
use super::*;

/// How often a test looks again at what the service's tasks did.
const TASK_POLL_INTERVAL: Duration = Duration::from_millis(1);

type Bus = Rc<IggyMessageBus>;
type Service = TierService<TestStore, Bus>;

fn bus() -> Bus {
    Rc::new(IggyMessageBus::new(0))
}

fn service(config: &TierServiceConfig) -> Service {
    TierService::new(TestStore::default(), bus(), config)
}

/// Every class with one job slot and no byte cap.
fn single_slots() -> TierServiceConfig {
    let one = ClassLimits {
        jobs: 1,
        bytes: None,
    };
    TierServiceConfig {
        demand_fetch: one,
        upload: one,
        speculative_fetch: one,
        delete: one,
        housekeeping: one,
        calls: StoreCallPolicy::default(),
    }
}

fn request(class: TierJobClass, scope: &TierScope) -> TierJobRequest {
    TierJobRequest::new(class, scope.token())
}

/// A job that notes in `started` when it starts and runs until `gate`
/// opens.
fn gated(
    service: &Service,
    request: TierJobRequest,
    name: &'static str,
    started: &Rc<RefCell<Vec<&'static str>>>,
) -> (oneshot::Sender<()>, TierJobHandle<&'static str>) {
    let (open, gate) = oneshot::channel::<()>();
    let started = Rc::clone(started);
    let handle = service.submit(request, move |_store| async move {
        started.borrow_mut().push(name);
        let _ = gate.await;
        name
    });
    (open, handle)
}

/// Wait for the service's tasks to bring the test to `done`.
async fn until(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(
            Instant::now() < deadline,
            "the tier service did not get there"
        );
        compio::time::sleep(TASK_POLL_INTERVAL).await;
    }
}

/// Give the service's tasks a few turns, for a test that checks that
/// something does not happen.
async fn settle() {
    for _ in 0..10 {
        compio::time::sleep(TASK_POLL_INTERVAL).await;
    }
}

struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn given_every_class_when_indexing_should_follow_the_order_of_all() {
    for (index, class) in TierJobClass::ALL.into_iter().enumerate() {
        assert_eq!(slot(class), index, "{class:?}");
    }
}

#[compio::test]
async fn given_job_when_submitting_should_run_it_on_its_own_task() {
    let service = service(&TierServiceConfig::default());
    let scope = TierScope::new();
    let ran = Rc::new(Cell::new(false));

    let handle = service.submit(request(TierJobClass::Upload, &scope), {
        let ran = Rc::clone(&ran);
        move |_store| async move {
            ran.set(true);
            7
        }
    });

    assert!(!ran.get(), "the job ran inside submit");
    assert_eq!(handle.await, Ok(7));
    assert!(ran.get());
}

#[compio::test]
async fn given_cancelled_scope_when_submitting_should_answer_cancelled_without_running() {
    let service = service(&TierServiceConfig::default());
    let scope = TierScope::new();
    scope.cancel();
    let ran = Rc::new(Cell::new(false));

    let handle = service.submit(request(TierJobClass::Delete, &scope), {
        let ran = Rc::clone(&ran);
        move |_store| async move { ran.set(true) }
    });

    assert_eq!(handle.await, Err(TierJobCancelled));
    settle().await;
    assert!(!ran.get());
    assert_eq!(service.load(TierJobClass::Delete), ClassLoad::default());
}

#[compio::test]
async fn given_dropped_handle_when_the_job_waits_should_finish_the_job_anyway() {
    let service = service(&TierServiceConfig::default());
    let scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let (open, handle) = gated(
        &service,
        request(TierJobClass::SpeculativeFetch, &scope),
        "readahead",
        &started,
    );
    until(|| !started.borrow().is_empty()).await;

    drop(handle);
    open.send(()).unwrap();

    until(|| service.load(TierJobClass::SpeculativeFetch).running == 0).await;
}

#[compio::test]
async fn given_class_at_its_job_cap_when_submitting_more_should_start_them_in_order() {
    let mut config = single_slots();
    config.upload.jobs = 2;
    let service = service(&config);
    let scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let mut jobs: VecDeque<_> = ["first", "second", "third"]
        .into_iter()
        .map(|name| {
            gated(
                &service,
                request(TierJobClass::Upload, &scope),
                name,
                &started,
            )
        })
        .collect();

    until(|| started.borrow().len() == 2).await;
    settle().await;
    assert_eq!(*started.borrow(), ["first", "second"]);
    assert_eq!(
        service.load(TierJobClass::Upload),
        ClassLoad {
            running: 2,
            running_bytes: 0,
            queued: 1,
        }
    );

    let (open, handle) = jobs.pop_front().unwrap();
    open.send(()).unwrap();
    assert_eq!(handle.await, Ok("first"));
    until(|| started.borrow().len() == 3).await;
    assert_eq!(*started.borrow(), ["first", "second", "third"]);
    for (open, handle) in jobs {
        open.send(()).unwrap();
        assert!(handle.await.is_ok());
    }
    assert_eq!(service.load(TierJobClass::Upload), ClassLoad::default());
}

#[compio::test]
async fn given_byte_cap_when_the_next_job_does_not_fit_should_hold_it_and_the_jobs_behind_it() {
    let mut config = single_slots();
    config.demand_fetch = ClassLimits {
        jobs: 8,
        bytes: Some(100),
    };
    let service = service(&config);
    let scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let fetch = |bytes| request(TierJobClass::DemandFetch, &scope).with_bytes(bytes);

    let (open_a, a) = gated(&service, fetch(60), "a", &started);
    let (open_b, b) = gated(&service, fetch(60), "b", &started);
    let (open_c, c) = gated(&service, fetch(10), "c", &started);
    until(|| !started.borrow().is_empty()).await;
    settle().await;
    // `c` would fit beside `a`, but it does not pass `b`.
    assert_eq!(*started.borrow(), ["a"]);

    open_a.send(()).unwrap();
    assert_eq!(a.await, Ok("a"));
    until(|| started.borrow().len() == 3).await;
    assert_eq!(*started.borrow(), ["a", "b", "c"]);
    assert_eq!(service.load(TierJobClass::DemandFetch).running_bytes, 70);
    open_b.send(()).unwrap();
    open_c.send(()).unwrap();
    assert_eq!((b.await, c.await), (Ok("b"), Ok("c")));
}

#[compio::test]
async fn given_job_above_the_byte_cap_when_its_class_is_idle_should_run_it_alone() {
    let mut config = single_slots();
    config.upload = ClassLimits {
        jobs: 4,
        bytes: Some(100),
    };
    let service = service(&config);
    let scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let upload = |bytes| request(TierJobClass::Upload, &scope).with_bytes(bytes);

    let (open_large, large) = gated(&service, upload(500), "large", &started);
    let (open_small, small) = gated(&service, upload(1), "small", &started);
    until(|| !started.borrow().is_empty()).await;
    settle().await;
    assert_eq!(*started.borrow(), ["large"]);

    open_large.send(()).unwrap();
    assert_eq!(large.await, Ok("large"));
    until(|| started.borrow().len() == 2).await;
    open_small.send(()).unwrap();
    assert_eq!(small.await, Ok("small"));
}

#[compio::test]
async fn given_uploads_at_their_cap_when_a_demand_fetch_comes_should_start_it_at_once() {
    let service = service(&single_slots());
    let scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let (open_running, running) = gated(
        &service,
        request(TierJobClass::Upload, &scope),
        "upload",
        &started,
    );
    let (open_queued, queued) = gated(
        &service,
        request(TierJobClass::Upload, &scope),
        "queued upload",
        &started,
    );
    until(|| !started.borrow().is_empty()).await;

    let (open_fetch, fetch) = gated(
        &service,
        request(TierJobClass::DemandFetch, &scope),
        "fetch",
        &started,
    );

    until(|| started.borrow().len() == 2).await;
    assert_eq!(*started.borrow(), ["upload", "fetch"]);
    for open in [open_fetch, open_running, open_queued] {
        open.send(()).unwrap();
    }
    assert_eq!(
        (fetch.await, running.await, queued.await),
        (Ok("fetch"), Ok("upload"), Ok("queued upload"))
    );
}

#[compio::test]
async fn given_a_waiting_demand_fetch_when_a_readahead_slot_frees_should_hold_the_next_readahead() {
    let service = service(&single_slots());
    let scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let job = |class, name| gated(&service, request(class, &scope), name, &started);
    let (open_readahead, readahead) = job(TierJobClass::SpeculativeFetch, "readahead");
    let (open_fetch, fetch) = job(TierJobClass::DemandFetch, "fetch");
    let (open_waiting_fetch, waiting_fetch) = job(TierJobClass::DemandFetch, "waiting fetch");
    let (open_next_readahead, next_readahead) =
        job(TierJobClass::SpeculativeFetch, "next readahead");
    until(|| started.borrow().len() == 2).await;

    open_readahead.send(()).unwrap();
    assert_eq!(readahead.await, Ok("readahead"));
    settle().await;
    assert_eq!(*started.borrow(), ["readahead", "fetch"]);

    open_fetch.send(()).unwrap();
    assert_eq!(fetch.await, Ok("fetch"));
    until(|| started.borrow().len() == 4).await;
    assert_eq!(
        *started.borrow(),
        ["readahead", "fetch", "waiting fetch", "next readahead"]
    );
    open_waiting_fetch.send(()).unwrap();
    open_next_readahead.send(()).unwrap();
    assert!(waiting_fetch.await.is_ok() && next_readahead.await.is_ok());
}

/// A held upload keeps a segment on a disk that is filling, so a backlog of
/// cold reads must not hold uploads.
#[compio::test]
async fn given_a_waiting_demand_fetch_when_an_upload_slot_frees_should_start_the_next_upload() {
    let service = service(&single_slots());
    let scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let job = |class, name| gated(&service, request(class, &scope), name, &started);
    let (open_upload, upload) = job(TierJobClass::Upload, "upload");
    let (open_fetch, fetch) = job(TierJobClass::DemandFetch, "fetch");
    let (open_waiting_fetch, waiting_fetch) = job(TierJobClass::DemandFetch, "waiting fetch");
    let (open_next_upload, next_upload) = job(TierJobClass::Upload, "next upload");
    until(|| started.borrow().len() == 2).await;

    open_upload.send(()).unwrap();
    assert_eq!(upload.await, Ok("upload"));

    until(|| started.borrow().len() == 3).await;
    assert_eq!(*started.borrow(), ["upload", "fetch", "next upload"]);
    for open in [open_fetch, open_waiting_fetch, open_next_upload] {
        open.send(()).unwrap();
    }
    assert!(fetch.await.is_ok() && waiting_fetch.await.is_ok() && next_upload.await.is_ok());
}

#[compio::test]
async fn given_a_waiting_upload_when_housekeeping_comes_should_hold_it_until_nothing_waits() {
    let service = service(&single_slots());
    let scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let job = |class, name| gated(&service, request(class, &scope), name, &started);
    let (open_upload, upload) = job(TierJobClass::Upload, "upload");
    let (open_waiting_upload, waiting_upload) = job(TierJobClass::Upload, "waiting upload");
    let (open_sweep, sweep) = job(TierJobClass::Housekeeping, "sweep");
    let (open_delete, delete) = job(TierJobClass::Delete, "delete");
    until(|| started.borrow().len() == 2).await;
    settle().await;
    // Deletes never wait for another class, and housekeeping waits for the
    // upload in the queue.
    assert_eq!(*started.borrow(), ["upload", "delete"]);

    open_upload.send(()).unwrap();
    assert_eq!(upload.await, Ok("upload"));
    until(|| started.borrow().len() == 4).await;
    assert_eq!(
        *started.borrow(),
        ["upload", "delete", "waiting upload", "sweep"]
    );
    for open in [open_waiting_upload, open_sweep, open_delete] {
        open.send(()).unwrap();
    }
    assert!(waiting_upload.await.is_ok() && sweep.await.is_ok() && delete.await.is_ok());
}

#[compio::test]
async fn given_queued_job_when_its_scope_is_cancelled_should_answer_cancelled_and_never_start() {
    let service = service(&single_slots());
    let running_scope = TierScope::new();
    let queued_scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let (open, running) = gated(
        &service,
        request(TierJobClass::Upload, &running_scope),
        "running",
        &started,
    );
    let (_open_queued, queued) = gated(
        &service,
        request(TierJobClass::Upload, &queued_scope),
        "queued",
        &started,
    );
    until(|| !started.borrow().is_empty()).await;

    queued_scope.cancel();

    assert_eq!(queued.await, Err(TierJobCancelled));
    open.send(()).unwrap();
    assert_eq!(running.await, Ok("running"));
    settle().await;
    assert_eq!(*started.borrow(), ["running"]);
    assert_eq!(service.load(TierJobClass::Upload), ClassLoad::default());
}

#[compio::test]
async fn given_running_job_when_its_scope_is_cancelled_should_drop_it_and_free_its_slot() {
    let service = service(&single_slots());
    let scope = TierScope::new();
    let dropped = Rc::new(Cell::new(false));
    let running = service.submit(request(TierJobClass::Upload, &scope), {
        let flag = DropFlag(Rc::clone(&dropped));
        move |_store| async move {
            let _flag = flag;
            std::future::pending::<()>().await;
        }
    });
    let next_scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let (open, next) = gated(
        &service,
        request(TierJobClass::Upload, &next_scope),
        "next",
        &started,
    );
    settle().await;
    assert!(started.borrow().is_empty());

    drop(scope);

    assert_eq!(running.await, Err(TierJobCancelled));
    until(|| dropped.get()).await;
    until(|| !started.borrow().is_empty()).await;
    open.send(()).unwrap();
    assert_eq!(next.await, Ok("next"));
}

#[compio::test]
async fn given_admitted_job_when_its_scope_is_cancelled_before_its_first_poll_should_never_call_it()
{
    let service = service(&TierServiceConfig::default());
    let scope = TierScope::new();
    let called = Rc::new(Cell::new(false));
    let handle = service.submit(request(TierJobClass::Upload, &scope), {
        let called = Rc::clone(&called);
        move |_store| {
            called.set(true);
            async {}
        }
    });
    assert_eq!(service.load(TierJobClass::Upload).running, 1);

    scope.cancel();

    assert_eq!(handle.await, Err(TierJobCancelled));
    until(|| service.load(TierJobClass::Upload).running == 0).await;
    assert!(!called.get());
}

#[test]
fn given_running_job_when_its_runtime_drops_its_task_should_give_back_its_slot_and_bytes() {
    let runtime = compio::runtime::Runtime::new().unwrap();
    let scope = TierScope::new();
    let started = Rc::new(RefCell::new(Vec::new()));
    let (service, _open, handle) = runtime.block_on(async {
        let service = service(&TierServiceConfig::default());
        let upload = request(TierJobClass::Upload, &scope).with_bytes(100);
        let (open, handle) = gated(&service, upload, "upload", &started);
        until(|| !started.borrow().is_empty()).await;
        (service, open, handle)
    });
    assert_eq!(service.load(TierJobClass::Upload).running_bytes, 100);

    drop(runtime);

    assert_eq!(service.load(TierJobClass::Upload), ClassLoad::default());
    assert_eq!(handle.now_or_never(), Some(Err(TierJobCancelled)));
}

#[test]
fn given_admitted_job_when_its_runtime_drops_its_task_before_its_first_poll_should_give_back_its_slot_and_bytes()
 {
    let runtime = compio::runtime::Runtime::new().unwrap();
    let scope = TierScope::new();
    let called = Rc::new(Cell::new(false));
    let service = runtime.enter(|| service(&TierServiceConfig::default()));
    let _handle = runtime.enter(|| {
        let called = Rc::clone(&called);
        let upload = request(TierJobClass::Upload, &scope).with_bytes(100);
        service.submit(upload, move |_store| async move { called.set(true) })
    });
    assert_eq!(service.load(TierJobClass::Upload).running_bytes, 100);

    drop(runtime);

    assert!(!called.get());
    assert_eq!(service.load(TierJobClass::Upload), ClassLoad::default());
}

/// A policy whose timeouts and backoff fit a test.
fn quick_calls() -> StoreCallPolicy {
    StoreCallPolicy {
        request_timeout: Duration::from_millis(50),
        progress_timeout: Duration::from_millis(50),
        max_attempts: 3,
        retry_delay: Duration::from_millis(1),
        max_retry_delay: Duration::from_millis(2),
    }
}

fn tier_store(store: TestStore) -> TierStore<TestStore, Bus> {
    TierStore::new(store, bus(), quick_calls())
}

/// The names of the files in `directory`, sorted.
fn names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort_unstable();
    names
}

/// A body of `pieces` pieces of `piece_len` bytes each.
fn body(pieces: usize, piece_len: usize) -> PutBody {
    let len = u64::try_from(pieces * piece_len).unwrap();
    let stream = futures::stream::iter((0..pieces).map(move |index| {
        let fill = u8::try_from(index).unwrap_or(u8::MAX);
        Ok(bytes::Bytes::from(vec![fill; piece_len]))
    }));
    PutBody::from_stream(len, stream)
}

#[compio::test]
async fn given_transient_failures_when_heading_should_retry_until_the_call_succeeds() {
    let provider = TestStore::default();
    provider
        .inner
        .put("a/b", PutBody::from_bytes(vec![1]), &PutOptions::default())
        .await
        .unwrap();
    provider.failures.borrow_mut().extend([
        ObjectStoreError::Unavailable("503".to_owned()),
        ObjectStoreError::Throttled("slow down".to_owned()),
    ]);
    let store = tier_store(provider);

    let meta = store.head("a/b").await.unwrap();

    assert_eq!(meta.size, 1);
    assert_eq!(store.provider().calls.get(), 3);
}

#[compio::test]
async fn given_more_transient_failures_than_attempts_when_listing_should_return_the_last_error() {
    let provider = TestStore::default();
    provider
        .failures
        .borrow_mut()
        .extend((0..5).map(|attempt| ObjectStoreError::Transport(format!("reset {attempt}"))));
    let store = tier_store(provider);

    let error = store.list(&ListRequest::new("a/")).await.unwrap_err();

    assert!(matches!(error, ObjectStoreError::Transport(ref reason) if reason == "reset 2"));
    assert_eq!(store.provider().calls.get(), 3);
}

#[compio::test]
async fn given_permanent_error_when_deleting_should_not_retry() {
    let provider = TestStore::default();
    provider
        .failures
        .borrow_mut()
        .push_back(ObjectStoreError::Auth("denied".to_owned()));
    let store = tier_store(provider);

    let error = store.delete(&["a/b".to_owned()]).await.unwrap_err();

    assert!(matches!(error, ObjectStoreError::Auth(_)));
    assert_eq!(store.provider().calls.get(), 1);
}

#[compio::test]
async fn given_store_that_never_answers_when_getting_should_fail_as_transport_on_every_attempt() {
    let provider = TestStore::default();
    provider.hangs.set(true);
    let store = tier_store(provider);
    let started = Instant::now();

    let error = store.get_range("a/b", 0..10).await.unwrap_err();

    assert!(matches!(error, ObjectStoreError::Transport(_)), "{error}");
    assert_eq!(store.provider().calls.get(), 3);
    assert!(started.elapsed() >= quick_calls().request_timeout * 3);
}

#[compio::test]
async fn given_slow_put_that_keeps_taking_pieces_when_putting_should_finish_past_the_progress_timeout()
 {
    let provider = TestStore::default();
    provider.piece_delay.set(Duration::from_millis(10));
    let store = tier_store(provider);
    let started = Instant::now();

    store
        .put("a/b", body(12, 4), &PutOptions::default())
        .await
        .unwrap();

    assert!(started.elapsed() > quick_calls().progress_timeout);
    assert_eq!(store.provider().inner.head("a/b").await.unwrap().size, 48);
    assert_eq!(store.provider().calls.get(), 1);
}

#[compio::test]
async fn given_put_that_stops_taking_pieces_when_putting_should_fail_as_transport_and_store_nothing()
 {
    let provider = TestStore::default();
    provider.stall_after_pieces.set(Some(2));
    let store = tier_store(provider);

    let error = store
        .put("a/b", body(4, 4), &PutOptions::default())
        .await
        .unwrap_err();

    assert!(matches!(error, ObjectStoreError::Transport(_)), "{error}");
    assert_eq!(error.class(), ErrorClass::Transient);
    assert!(store.provider().inner.head("a/b").await.is_err());
}

#[compio::test]
async fn given_put_without_an_answer_after_its_last_piece_when_putting_should_fail_as_transport() {
    let provider = TestStore::default();
    provider.hang_after_body.set(true);
    let store = tier_store(provider);

    let error = store
        .put("a/b", body(2, 4), &PutOptions::default())
        .await
        .unwrap_err();

    assert!(matches!(error, ObjectStoreError::Transport(_)), "{error}");
}

/// Uploads put with `IfAbsent`, so what a cut put leaves on disk must not
/// refuse the retry.
#[compio::test]
async fn given_fs_put_cut_by_the_progress_timeout_when_putting_it_again_should_store_the_whole_body()
 {
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("a");
    let store = TierStore::new(FsStore::new(directory.path()), bus(), quick_calls());
    let if_absent = PutOptions {
        mode: PutMode::IfAbsent,
        ..PutOptions::default()
    };
    // Two of the body's four pieces, then nothing.
    let stalled = PutBody::from_stream(
        16,
        body(2, 4).into_pieces().chain(futures::stream::pending()),
    );
    let cut = store.put("a/b", stalled, &if_absent).await.unwrap_err();
    assert!(matches!(cut, ObjectStoreError::Transport(_)), "{cut}");
    let left = names(&parent);
    assert!(
        matches!(left.as_slice(), [scratch] if scratch.starts_with(".b.")),
        "{left:?}"
    );

    store.put("a/b", body(4, 4), &if_absent).await.unwrap();

    let stored = store.get_range("a/b", 0..16).await.unwrap();
    assert_eq!(stored.bytes, [[0_u8; 4], [1; 4], [2; 4], [3; 4]].concat());
    // A task of the retry removes the scratch file of the cut put.
    until(|| names(&parent) == ["b"]).await;
}

#[compio::test]
async fn given_put_through_the_service_when_the_provider_stalls_should_free_the_upload_slot() {
    let provider = TestStore::default();
    provider.stall_after_pieces.set(Some(1));
    let config = TierServiceConfig {
        calls: quick_calls(),
        ..single_slots()
    };
    let service = TierService::new(provider, bus(), &config);
    let scope = TierScope::new();

    let outcome = service
        .submit(request(TierJobClass::Upload, &scope), |store| async move {
            store.put("a/b", body(3, 4), &PutOptions::default()).await
        })
        .await
        .unwrap();

    assert!(matches!(outcome, Err(ObjectStoreError::Transport(_))));
    until(|| service.load(TierJobClass::Upload).running == 0).await;
}
