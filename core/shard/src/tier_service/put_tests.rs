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

//! The puts of a tier store against a provider that a script drives, on a
//! clock that moves only when nothing else can run, so no test sleeps.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use bytes::Bytes;
use futures::channel::oneshot;
use futures::future::{self, LocalBoxFuture};
use futures::{FutureExt, StreamExt};
use iggy_object_store::{
    DeleteOutcome, ListPage, ListRequest, MemoryStore, ObjectMeta, ObjectRange, ObjectStore,
    ObjectStoreError, PutBody, PutMode, PutOptions, UserMetadata,
};
use message_bus::{
    BusMessage, ConnectionLostFn, JoinHandle, MessageBus, ReplicaForwardFn, SendError,
};
use server_common::MESSAGE_ALIGN;
use server_common::iobuf::Frozen;

use super::{PUT_ID_METADATA, SecondAttempts, StoreCallPolicy, StoreWriter, TierStore};

const MIB: usize = 1024 * 1024;
const PIECE: usize = 256 * 1024;
/// The threshold of a second attempt at a 1 MiB body under [`policy`].
const THRESHOLD: Duration = Duration::from_millis(360);
const FAST: Duration = Duration::from_millis(60);

/// A bus whose clock moves only when [`VirtualBus::run`] finds nothing else
/// that can run, and that runs the tasks it spawns beside the test.
#[derive(Clone, Default)]
struct VirtualBus(Rc<Clock>);

#[derive(Default)]
struct Clock {
    now_micros: Cell<u64>,
    timers: RefCell<Vec<(u64, Rc<Timer>)>>,
    tasks: RefCell<Vec<LocalBoxFuture<'static, ()>>>,
}

#[derive(Default)]
struct Timer {
    waker: RefCell<Option<Waker>>,
    dropped: Cell<bool>,
}

struct Sleep {
    clock: Rc<Clock>,
    deadline: u64,
    timer: Option<Rc<Timer>>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.clock.now_micros.get() >= self.deadline {
            return Poll::Ready(());
        }
        if self.timer.is_none() {
            let timer = Rc::new(Timer::default());
            self.clock
                .timers
                .borrow_mut()
                .push((self.deadline, Rc::clone(&timer)));
            self.timer = Some(timer);
        }
        if let Some(timer) = &self.timer {
            *timer.waker.borrow_mut() = Some(context.waker().clone());
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(timer) = &self.timer {
            timer.dropped.set(true);
        }
    }
}

struct Woken(AtomicBool);

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl VirtualBus {
    fn now(&self) -> Duration {
        Duration::from_micros(self.0.now_micros.get())
    }

    /// Runs `future` to its end with the tasks spawned meanwhile, and moves
    /// the clock to the next timer whenever none of them can run.
    fn run<T>(&self, future: impl Future<Output = T>) -> T {
        let woken = Arc::new(Woken(AtomicBool::new(true)));
        let waker = Waker::from(Arc::clone(&woken));
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            while woken.0.swap(false, Ordering::SeqCst) {
                if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                    return output;
                }
                self.run_tasks(&mut context);
            }
            assert!(self.advance(), "nothing can run and no timer is set");
        }
    }

    /// Lets the spawned tasks run for `duration` of the clock.
    fn run_for(&self, duration: Duration) {
        self.run(self.sleep(duration));
    }

    fn run_tasks(&self, context: &mut Context<'_>) {
        let mut tasks = std::mem::take(&mut *self.0.tasks.borrow_mut());
        tasks.retain_mut(|task| task.as_mut().poll(context).is_pending());
        let mut spawned = self.0.tasks.borrow_mut();
        if !spawned.is_empty() {
            context.waker().wake_by_ref();
        }
        tasks.append(&mut spawned);
        *spawned = tasks;
    }

    /// Moves the clock to the earliest timer and wakes what waits for it,
    /// false without a timer.
    fn advance(&self) -> bool {
        let due = {
            let mut timers = self.0.timers.borrow_mut();
            timers.retain(|(_, timer)| !timer.dropped.get());
            let Some(earliest) = timers.iter().map(|(deadline, _)| *deadline).min() else {
                return false;
            };
            self.0.now_micros.set(self.0.now_micros.get().max(earliest));
            let now = self.0.now_micros.get();
            let (due, later): (Vec<_>, Vec<_>) =
                timers.drain(..).partition(|(deadline, _)| *deadline <= now);
            *timers = later;
            due
        };
        for (_, timer) in due {
            if let Some(waker) = timer.waker.borrow_mut().take() {
                waker.wake();
            }
        }
        true
    }
}

impl MessageBus for VirtualBus {
    fn monotonic_micros(&self) -> u64 {
        self.0.now_micros.get()
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> {
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        Sleep {
            clock: Rc::clone(&self.0),
            deadline: self.0.now_micros.get().saturating_add(micros),
            timer: None,
        }
    }

    fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.0.tasks.borrow_mut().push(future.boxed_local());
    }

    fn realtime_micros(&self) -> u64 {
        1_800_000_000_000_000 + self.0.now_micros.get()
    }

    async fn send_to_client(
        &self,
        _client_id: u128,
        _data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        unreachable!("a tier store sends no client traffic");
    }

    async fn send_to_replica(
        &self,
        _replica: u8,
        _data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        unreachable!("a tier store sends no replica traffic");
    }

    fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}

    fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}

    fn track_background(&self, _handle: JoinHandle<()>) {}
}

/// What the provider does with one attempt at a put.
#[derive(Clone, Copy)]
struct Script {
    /// The wait before the provider takes each piece of the body.
    piece_delay: Duration,
    /// How fast the provider sends what it took: after each piece, it waits
    /// for the piece to leave before it takes the next. Zero is no limit.
    bytes_per_second: u64,
    /// The provider takes this many pieces and then no more.
    stall_after: Option<usize>,
    /// From the end of the body to the commit at the store.
    commit_after: Duration,
    /// From the commit to the answer.
    answer_after: Duration,
    outcome: Outcome,
}

#[derive(Clone, Copy)]
enum Outcome {
    /// The store commits the put, or refuses a create-only put that finds
    /// an object, and answers so. It commits even when the caller dropped
    /// the call, as a store that took the whole request does.
    Commit,
    /// The store commits, and the caller gets this error: the answer was
    /// lost.
    LoseAnswer(fn() -> ObjectStoreError),
    /// The store takes the body, refuses the put with this error and stores
    /// nothing.
    Refuse(fn() -> ObjectStoreError),
    /// The request never leaves.
    NotSent,
    /// The store takes the body and neither commits nor answers.
    Hang,
}

impl Script {
    const fn of(outcome: Outcome) -> Self {
        Self {
            piece_delay: Duration::ZERO,
            bytes_per_second: 0,
            stall_after: None,
            commit_after: Duration::ZERO,
            answer_after: Duration::ZERO,
            outcome,
        }
    }

    /// Commits `commit_after` after the body and answers at once.
    const fn commit_after(commit_after: Duration) -> Self {
        Self {
            commit_after,
            ..Self::of(Outcome::Commit)
        }
    }
}

/// What a head reports instead of what the store holds.
type Disguise = fn(ObjectMeta) -> ObjectMeta;

/// A provider over a memory store that follows a script for each attempt
/// at a put, and that keeps what the test checks.
struct ScriptedStore {
    bus: VirtualBus,
    objects: Rc<MemoryStore>,
    scripts: RefCell<VecDeque<Script>>,
    /// What an attempt does once the scripts ran out.
    otherwise: Cell<Script>,
    /// The options of each attempt at a put, in order.
    puts: RefCell<Vec<PutOptions>>,
    heads: Cell<usize>,
    /// Puts that the store committed.
    commits: Rc<Cell<usize>>,
    /// Create-only puts that the store refused at their commit.
    refusals: Rc<Cell<usize>>,
    /// Changes what a head reports.
    disguise: Cell<Option<Disguise>>,
}

impl ScriptedStore {
    fn new(bus: &VirtualBus) -> Self {
        Self {
            bus: bus.clone(),
            objects: Rc::new(MemoryStore::new()),
            scripts: RefCell::default(),
            otherwise: Cell::new(Script::of(Outcome::Commit)),
            puts: RefCell::default(),
            heads: Cell::new(0),
            commits: Rc::default(),
            refusals: Rc::default(),
            disguise: Cell::new(None),
        }
    }

    fn script(&self, scripts: impl IntoIterator<Item = Script>) {
        self.scripts.borrow_mut().extend(scripts);
    }

    fn put_ids(&self) -> Vec<String> {
        self.puts
            .borrow()
            .iter()
            .map(|options| {
                options
                    .metadata
                    .get(PUT_ID_METADATA)
                    .cloned()
                    .unwrap_or_default()
            })
            .collect()
    }

    /// The store side of a put whose body arrived: the commit, which runs on
    /// whether the caller waits or not, and the answer it hands over.
    fn commit(
        &self,
        key: &str,
        bytes: Vec<u8>,
        options: &PutOptions,
        script: Script,
    ) -> oneshot::Receiver<Result<(), ObjectStoreError>> {
        let (answer, answered) = oneshot::channel();
        let objects = Rc::clone(&self.objects);
        let commits = Rc::clone(&self.commits);
        let refusals = Rc::clone(&self.refusals);
        let bus = self.bus.clone();
        let key = key.to_owned();
        let options = options.clone();
        self.bus.spawn(async move {
            bus.sleep(script.commit_after).await;
            let committed = objects
                .put(&key, PutBody::from_bytes(bytes), &options)
                .await;
            match &committed {
                Ok(()) => commits.set(commits.get() + 1),
                Err(ObjectStoreError::PreconditionFailed { .. }) => {
                    refusals.set(refusals.get() + 1);
                }
                Err(_) => {}
            }
            let _ = answer.send(committed);
        });
        answered
    }
}

impl ObjectStore for ScriptedStore {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        self.puts.borrow_mut().push(options.clone());
        let script = self
            .scripts
            .borrow_mut()
            .pop_front()
            .unwrap_or_else(|| self.otherwise.get());
        if matches!(script.outcome, Outcome::NotSent) {
            return Err(ObjectStoreError::NotSent("connection refused".to_owned()));
        }
        let mut pieces = body.into_pieces();
        let mut bytes = Vec::new();
        let mut taken = 0;
        loop {
            if script.stall_after == Some(taken) {
                future::pending::<()>().await;
            }
            self.bus.sleep(script.piece_delay).await;
            let Some(piece) = pieces.next().await else {
                break;
            };
            let piece = piece.map_err(ObjectStoreError::Body)?;
            if let Some(micros) =
                (piece.len() as u64 * 1_000_000).checked_div(script.bytes_per_second)
            {
                self.bus.sleep(Duration::from_micros(micros)).await;
            }
            bytes.extend_from_slice(&piece);
            taken += 1;
        }
        let lost = match script.outcome {
            Outcome::Hang => return future::pending().await,
            Outcome::Refuse(error) => {
                self.bus
                    .sleep(script.commit_after + script.answer_after)
                    .await;
                return Err(error());
            }
            Outcome::LoseAnswer(error) => Some(error),
            Outcome::Commit | Outcome::NotSent => None,
        };
        let committed = self
            .commit(key, bytes, options, script)
            .await
            .unwrap_or_else(|_| Err(ObjectStoreError::Transport("no commit".to_owned())));
        self.bus.sleep(script.answer_after).await;
        lost.map_or(committed, |error| Err(error()))
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        self.objects.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        self.heads.set(self.heads.get() + 1);
        let held = self.objects.head(key).await?;
        Ok(match self.disguise.get() {
            Some(change) => change(held),
            None => held,
        })
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        self.objects.delete(keys).await
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        self.objects.list(request).await
    }
}

type Store = TierStore<ScriptedStore, VirtualBus>;

fn policy() -> StoreCallPolicy {
    StoreCallPolicy {
        request_timeout: Duration::from_secs(30),
        progress_timeout: Duration::from_secs(30),
        max_attempts: 4,
        retry_delay: Duration::from_millis(100),
        max_retry_delay: Duration::from_secs(1),
        attempt_stall_timeout: Duration::from_secs(1),
        attempt_answer_timeout: Duration::from_secs(5),
        second_attempts: SecondAttempts {
            after: Duration::from_millis(300),
            after_per_mib: Duration::from_millis(60),
            slots: 2,
        },
    }
}

fn store_with(policy: StoreCallPolicy) -> (VirtualBus, Store) {
    let bus = VirtualBus::default();
    let store = TierStore::new(
        ScriptedStore::new(&bus),
        bus.clone(),
        policy,
        StoreWriter {
            replica: 1,
            shard: 2,
            ..StoreWriter::default()
        },
    );
    (bus, store)
}

fn store() -> (VirtualBus, Store) {
    store_with(policy())
}

/// 1 MiB of `fill`.
fn object(fill: u8) -> Bytes {
    Bytes::from(vec![fill; MIB])
}

fn body(bytes: &Bytes) -> PutBody {
    PutBody::from_bytes_in_pieces(bytes.clone(), PIECE)
}

/// The CRC32C of `bytes`, as a memory store computes it.
fn crc32c_of(bytes: &Bytes) -> u32 {
    let store = MemoryStore::new();
    futures::executor::block_on(async {
        store
            .put(
                "crc",
                PutBody::from_bytes(bytes.clone()),
                &PutOptions::default(),
            )
            .await
            .unwrap();
        store.head("crc").await.unwrap().crc32c.unwrap()
    })
}

fn create_only(bytes: &Bytes) -> PutOptions {
    PutOptions {
        mode: PutMode::IfAbsent,
        crc32c: Some(crc32c_of(bytes)),
        ..PutOptions::default()
    }
}

fn lost() -> ObjectStoreError {
    ObjectStoreError::Transport("connection reset".to_owned())
}

/// The object under `key`, the way `store` holds it, put by another writer
/// with `metadata`.
fn put_by_another_writer(store: &Store, key: &str, bytes: &Bytes, metadata: UserMetadata) {
    let options = PutOptions {
        metadata,
        ..PutOptions::default()
    };
    futures::executor::block_on(store.provider().objects.put(
        key,
        PutBody::from_bytes(bytes.clone()),
        &options,
    ))
    .unwrap();
}

fn stored(store: &Store, key: &str) -> Bytes {
    futures::executor::block_on(store.provider().objects.get_range(key, 0..u64::MAX))
        .unwrap()
        .bytes
}

/// The counters of `store`: put retries, second attempts, second attempt
/// wins and own objects found.
fn counted(store: &Store) -> (u64, u64, u64, u64) {
    store.counters().values()
}

#[test]
fn given_attempts_that_never_left_when_putting_should_send_again_without_a_check() {
    let (bus, store) = store();
    let bytes = object(1);
    store
        .provider()
        .script([Script::of(Outcome::NotSent), Script::of(Outcome::NotSent)]);

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();

    let provider = store.provider();
    assert_eq!(provider.puts.borrow().len(), 3);
    assert_eq!(provider.heads.get(), 0);
    assert_eq!(provider.commits.get(), 1);
    assert_eq!(stored(&store, "a/b"), bytes);
    assert_eq!(counted(&store), (2, 0, 0, 0));
}

#[test]
fn given_a_lost_answer_when_the_retry_finds_the_object_it_stored_should_count_as_stored() {
    let (bus, store) = store();
    let bytes = object(1);
    store
        .provider()
        .script([Script::of(Outcome::LoseAnswer(lost))]);

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();

    let provider = store.provider();
    assert_eq!(provider.puts.borrow().len(), 2);
    assert_eq!(provider.heads.get(), 1);
    assert_eq!((provider.commits.get(), provider.refusals.get()), (1, 1));
    assert_eq!(stored(&store, "a/b"), bytes);
    assert_eq!(counted(&store), (1, 0, 0, 1));
}

/// Each case changes one thing that a head of the put's own object reports.
fn refused_when_the_object_differs(disguise: Disguise) {
    let (bus, store) = store();
    let bytes = object(1);
    store
        .provider()
        .script([Script::of(Outcome::LoseAnswer(lost))]);
    store.provider().disguise.set(Some(disguise));

    let refused = bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)));

    assert!(
        matches!(refused, Err(ObjectStoreError::PreconditionFailed { .. })),
        "{refused:?}"
    );
    assert_eq!(store.provider().heads.get(), 1);
    assert_eq!(counted(&store).3, 0);
}

#[test]
fn given_a_lost_answer_when_the_object_found_has_another_size_should_refuse() {
    refused_when_the_object_differs(|held| ObjectMeta {
        size: held.size + 1,
        ..held
    });
}

#[test]
fn given_a_lost_answer_when_the_object_found_has_another_crc32c_should_refuse() {
    refused_when_the_object_differs(|held| ObjectMeta {
        crc32c: held.crc32c.map(|crc32c| crc32c ^ 1),
        ..held
    });
}

#[test]
fn given_a_lost_answer_when_the_object_found_has_another_put_id_should_refuse() {
    refused_when_the_object_differs(|mut held| {
        held.metadata
            .insert(PUT_ID_METADATA.to_owned(), format!("{:032x}", 7));
        held
    });
}

#[test]
fn given_a_lost_answer_when_the_store_keeps_no_crc32c_should_take_the_id_and_size() {
    let (bus, store) = store();
    let bytes = object(1);
    store
        .provider()
        .script([Script::of(Outcome::LoseAnswer(lost))]);
    store.provider().disguise.set(Some(|held| ObjectMeta {
        crc32c: None,
        ..held
    }));

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();

    assert_eq!(counted(&store).3, 1);
}

/// Another writer stored the same bytes under the key, between an attempt
/// that may have reached the store and the next one.
#[test]
fn given_an_unknown_attempt_when_another_writer_stored_the_same_bytes_should_refuse() {
    let (bus, store) = store();
    let bytes = object(1);
    put_by_another_writer(&store, "a/b", &bytes, UserMetadata::new());
    store.provider().script([Script::of(Outcome::Refuse(lost))]);

    let refused = bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)));

    assert!(
        matches!(refused, Err(ObjectStoreError::PreconditionFailed { .. })),
        "{refused:?}"
    );
    assert_eq!(store.provider().heads.get(), 1);
    assert_eq!(store.provider().puts.borrow().len(), 2);
}

/// A refusal that no earlier attempt can explain is a conflict, which the
/// caller's own check settles.
#[test]
fn given_another_writers_object_when_the_first_attempt_is_refused_should_return_it_unchecked() {
    let (bus, store) = store();
    let bytes = object(1);
    put_by_another_writer(&store, "a/b", &object(2), UserMetadata::new());

    let refused = bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)));

    assert!(
        matches!(refused, Err(ObjectStoreError::PreconditionFailed { .. })),
        "{refused:?}"
    );
    assert_eq!(store.provider().puts.borrow().len(), 1);
    assert_eq!(store.provider().heads.get(), 0);
}

#[test]
fn given_an_attempt_that_never_left_when_the_next_is_refused_should_return_it_unchecked() {
    let (bus, store) = store();
    let bytes = object(1);
    put_by_another_writer(&store, "a/b", &bytes, UserMetadata::new());
    store.provider().script([Script::of(Outcome::NotSent)]);

    let refused = bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)));

    assert!(
        matches!(refused, Err(ObjectStoreError::PreconditionFailed { .. })),
        "{refused:?}"
    );
    assert_eq!(store.provider().puts.borrow().len(), 2);
    assert_eq!(store.provider().heads.get(), 0);
}

#[test]
fn given_a_body_that_misses_the_callers_crc32c_when_putting_should_not_send_again() {
    let (bus, store) = store();
    let bytes = object(1);
    let options = PutOptions {
        crc32c: Some(crc32c_of(&bytes) ^ 1),
        ..create_only(&bytes)
    };

    let refused = bus.run(store.put("a/b", body(&bytes), &options));

    assert!(
        matches!(refused, Err(ObjectStoreError::ChecksumMismatch { .. })),
        "{refused:?}"
    );
    assert_eq!(store.provider().puts.borrow().len(), 1);
}

#[test]
fn given_a_permanent_failure_when_putting_should_not_send_again() {
    let permanent: [fn() -> ObjectStoreError; 3] = [
        || ObjectStoreError::Auth("denied".to_owned()),
        || ObjectStoreError::InvalidRequest("bad header".to_owned()),
        || ObjectStoreError::Body(io::Error::other("read failed")),
    ];
    for error in permanent {
        let (bus, store) = store();
        let bytes = object(1);
        store
            .provider()
            .script([Script::of(Outcome::Refuse(error))]);

        let failed = bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)));

        assert_eq!(
            failed.unwrap_err().to_string(),
            error().to_string(),
            "the error is returned as it came"
        );
        assert_eq!(store.provider().puts.borrow().len(), 1);
    }
}

#[test]
fn given_a_streamed_body_when_the_put_fails_should_attempt_it_once_without_an_id() {
    let (bus, store) = store();
    store.provider().script([Script::of(Outcome::Refuse(lost))]);
    let streamed =
        PutBody::from_stream(4, futures::stream::iter([Ok(Bytes::from_static(b"segm"))]));

    let failed = bus.run(store.put("a/b", streamed, &PutOptions::default()));

    assert!(
        matches!(failed, Err(ObjectStoreError::Transport(_))),
        "{failed:?}"
    );
    assert_eq!(store.provider().puts.borrow().len(), 1);
    assert_eq!(store.provider().put_ids(), [String::new()]);
}

#[test]
fn given_failures_past_the_attempts_when_putting_should_return_the_last_one() {
    let (bus, store) = store();
    let bytes = object(1);
    store
        .provider()
        .otherwise
        .set(Script::of(Outcome::Refuse(|| {
            ObjectStoreError::Unavailable("503".to_owned())
        })));

    let failed = bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)));

    assert!(
        matches!(failed, Err(ObjectStoreError::Unavailable(_))),
        "{failed:?}"
    );
    assert_eq!(store.provider().puts.borrow().len(), 4);
    assert_eq!(counted(&store).0, 3);
    // 100 ms, 200 ms and 400 ms between the four attempts.
    assert_eq!(bus.now(), Duration::from_millis(700));
}

#[test]
fn given_an_overwrite_with_a_lost_answer_when_putting_should_send_again_without_a_check() {
    let (bus, store) = store();
    let bytes = object(1);
    store
        .provider()
        .script([Script::of(Outcome::LoseAnswer(lost))]);

    bus.run(store.put("a/b", body(&bytes), &PutOptions::default()))
        .unwrap();

    assert_eq!(store.provider().puts.borrow().len(), 2);
    assert_eq!(store.provider().heads.get(), 0);
    assert_eq!(stored(&store, "a/b"), bytes);
}

#[test]
fn given_two_puts_with_retries_should_carry_one_id_per_put() {
    let (bus, store) = store();
    let bytes = object(1);
    store.provider().script([
        Script::of(Outcome::NotSent),
        Script::of(Outcome::Commit),
        Script::of(Outcome::NotSent),
    ]);

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();
    bus.run(store.put("a/c", body(&bytes), &create_only(&bytes)))
        .unwrap();

    let ids = store.provider().put_ids();
    assert_eq!(ids.len(), 4);
    assert_eq!(ids[0], ids[1]);
    assert_eq!(ids[2], ids[3]);
    assert_ne!(ids[0], ids[2]);
    for id in &ids {
        assert_eq!(id.len(), 32, "{id}");
        assert!(id.bytes().all(|byte| byte.is_ascii_hexdigit()), "{id}");
    }
    let held = futures::executor::block_on(store.provider().objects.head("a/c")).unwrap();
    assert_eq!(held.metadata.get(PUT_ID_METADATA), Some(&ids[2]));
}

/// A store whose create-only puts get no second attempt, so a test sees the
/// deadlines of the first one alone.
fn store_without_second_attempts() -> (VirtualBus, Store) {
    store_with(StoreCallPolicy {
        second_attempts: SecondAttempts {
            after: Duration::ZERO,
            ..policy().second_attempts
        },
        ..policy()
    })
}

#[test]
fn given_a_create_only_attempt_whose_body_stops_moving_should_give_it_up_after_the_stall_timeout() {
    let (bus, store) = store_without_second_attempts();
    let bytes = object(1);
    store.provider().script([Script {
        stall_after: Some(1),
        ..Script::of(Outcome::Commit)
    }]);

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();

    assert_eq!(store.provider().puts.borrow().len(), 2);
    assert_eq!(bus.now(), Duration::from_millis(1_100));
}

#[test]
fn given_a_create_only_attempt_without_an_answer_should_give_it_up_after_the_answer_timeout() {
    let (bus, store) = store_without_second_attempts();
    let bytes = object(1);
    store.provider().script([Script::of(Outcome::Hang)]);

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();

    assert_eq!(store.provider().puts.borrow().len(), 2);
    assert_eq!(bus.now(), Duration::from_millis(5_100));
}

const SLOW_LINK: u64 = 100 * 1024;

/// At about 100 KiB/s a 64 KiB piece leaves in 640 ms, well within the stall
/// deadline, although the whole body takes ten seconds.
#[test]
fn given_a_create_only_body_in_memory_that_moves_at_100_kib_a_second_should_not_cut_it() {
    let (bus, store) = store_without_second_attempts();
    let bytes = object(1);
    store.provider().script([Script {
        bytes_per_second: SLOW_LINK,
        ..Script::of(Outcome::Commit)
    }]);

    bus.run(store.put("a/b", partitions::memory_body(&bytes), &create_only(&bytes)))
        .unwrap();

    assert_eq!(store.provider().puts.borrow().len(), 1);
    assert_eq!(bus.now(), Duration::from_millis(10_240));
    assert_eq!(stored(&store, "a/b"), bytes);
}

/// The same link with 1 MiB pieces takes ten seconds for one piece, so the
/// stall deadline cuts every attempt of a body that moves.
#[test]
fn given_a_create_only_body_in_1_mib_pieces_on_a_100_kib_a_second_link_should_cut_every_attempt() {
    let (bus, store) = store_without_second_attempts();
    let bytes = object(1);
    store.provider().otherwise.set(Script {
        bytes_per_second: SLOW_LINK,
        ..Script::of(Outcome::Commit)
    });

    let failed = bus.run(store.put(
        "a/b",
        PutBody::from_bytes_in_pieces(bytes.clone(), MIB),
        &create_only(&bytes),
    ));

    assert!(
        matches!(failed, Err(ObjectStoreError::Transport(_))),
        "{failed:?}"
    );
    assert_eq!(store.provider().puts.borrow().len(), 4);
    const { assert!(partitions::MEMORY_PIECE_LEN < MIB) };
}

#[test]
fn given_a_create_only_body_in_memory_that_moves_no_piece_within_the_stall_timeout_should_cut_every_attempt()
 {
    let (bus, store) = store_without_second_attempts();
    let bytes = object(1);
    store.provider().otherwise.set(Script {
        piece_delay: Duration::from_millis(1_100),
        ..Script::of(Outcome::Commit)
    });

    let failed = bus.run(store.put("a/b", partitions::memory_body(&bytes), &create_only(&bytes)));

    assert!(
        matches!(failed, Err(ObjectStoreError::Transport(_))),
        "{failed:?}"
    );
    assert_eq!(store.provider().puts.borrow().len(), 4);
    // Four attempts cut at 1 s, with 100 ms, 200 ms and 400 ms between them.
    assert_eq!(bus.now(), Duration::from_millis(4 * 1_000 + 700));
    assert_eq!(store.provider().commits.get(), 0);
}

/// An overwrite cut early could land after a later put of other bytes, so
/// its attempts wait as long as a put that streams.
#[test]
fn given_an_overwrite_slower_than_the_attempt_deadlines_should_let_it_finish() {
    let (bus, store) = store();
    let bytes = object(1);
    store.provider().script([Script {
        piece_delay: Duration::from_secs(2),
        answer_after: Duration::from_secs(10),
        ..Script::of(Outcome::Commit)
    }]);

    bus.run(store.put("a/b", body(&bytes), &PutOptions::default()))
        .unwrap();

    assert_eq!(store.provider().puts.borrow().len(), 1);
    // Four pieces and the end of the body, each after 2 s, then the answer.
    assert_eq!(bus.now(), Duration::from_secs(5 * 2 + 10));
}

#[test]
fn given_an_overwrite_whose_body_stops_moving_should_give_it_up_after_the_progress_timeout() {
    let (bus, store) = store();
    let bytes = object(1);
    store.provider().script([Script {
        stall_after: Some(1),
        ..Script::of(Outcome::Commit)
    }]);

    bus.run(store.put("a/b", body(&bytes), &PutOptions::default()))
        .unwrap();

    assert_eq!(store.provider().puts.borrow().len(), 2);
    assert_eq!(
        bus.now(),
        policy().progress_timeout + Duration::from_millis(100)
    );
    assert_eq!(stored(&store, "a/b"), bytes);
}

#[test]
fn given_an_overwrite_without_an_answer_should_give_it_up_after_the_progress_timeout() {
    let (bus, store) = store();
    let bytes = object(1);
    store.provider().script([Script::of(Outcome::Hang)]);

    bus.run(store.put("a/b", body(&bytes), &PutOptions::default()))
        .unwrap();

    assert_eq!(store.provider().puts.borrow().len(), 2);
    assert_eq!(
        bus.now(),
        policy().progress_timeout + Duration::from_millis(100)
    );
    assert_eq!(stored(&store, "a/b"), bytes);
}

#[test]
fn given_a_streamed_put_without_an_answer_should_wait_for_the_progress_timeout() {
    let (bus, store) = store();
    store.provider().script([Script::of(Outcome::Hang)]);
    let streamed =
        PutBody::from_stream(4, futures::stream::iter([Ok(Bytes::from_static(b"segm"))]));

    let failed = bus.run(store.put("a/b", streamed, &PutOptions::default()));

    assert!(
        matches!(failed, Err(ObjectStoreError::Transport(_))),
        "{failed:?}"
    );
    assert_eq!(bus.now(), policy().progress_timeout);
}

#[test]
fn given_a_size_when_computing_the_threshold_should_add_its_share_and_turn_off_at_zero() {
    let second = policy().second_attempts;

    assert_eq!(second.threshold(0), Some(Duration::from_millis(300)));
    assert_eq!(second.threshold(MIB as u64), Some(THRESHOLD));
    assert_eq!(
        second.threshold(MIB as u64 / 2),
        Some(Duration::from_millis(330))
    );
    let off = SecondAttempts {
        after: Duration::ZERO,
        ..second
    };
    assert_eq!(off.threshold(MIB as u64), None);
    let no_slots = SecondAttempts { slots: 0, ..second };
    assert_eq!(no_slots.threshold(MIB as u64), None);
}

#[test]
fn given_a_slow_first_attempt_when_the_second_answers_fast_should_return_near_the_threshold() {
    let (bus, store) = store();
    let bytes = object(1);
    store.provider().script([
        Script::commit_after(Duration::from_secs(4)),
        Script::commit_after(FAST),
    ]);

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();

    assert_eq!(bus.now(), THRESHOLD + FAST);
    assert_eq!(counted(&store), (0, 1, 1, 0));
    // The first attempt reached the store, which commits it late and finds
    // the object of the second.
    bus.run_for(Duration::from_secs(10));
    let provider = store.provider();
    assert_eq!((provider.commits.get(), provider.refusals.get()), (1, 1));
    assert_eq!(provider.heads.get(), 0);
    assert_eq!(stored(&store, "a/b"), bytes);
}

#[test]
fn given_a_first_attempt_that_lands_first_when_the_second_is_refused_should_count_as_stored() {
    let (bus, store) = store();
    let bytes = object(1);
    store.provider().script([
        Script {
            commit_after: Duration::from_millis(400),
            answer_after: Duration::from_secs(3),
            ..Script::of(Outcome::Commit)
        },
        Script::commit_after(Duration::from_millis(100)),
    ]);

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();

    assert_eq!(bus.now(), THRESHOLD + Duration::from_millis(100));
    assert_eq!(counted(&store), (0, 1, 0, 1));
    let provider = store.provider();
    assert_eq!((provider.commits.get(), provider.refusals.get()), (1, 1));
    assert_eq!(provider.heads.get(), 1);
}

#[test]
fn given_a_second_attempt_that_fails_when_the_first_answers_should_take_the_first() {
    let (bus, store) = store();
    let bytes = object(1);
    store.provider().script([
        Script::commit_after(Duration::from_secs(1)),
        Script::of(Outcome::Refuse(|| {
            ObjectStoreError::Throttled("slow down".to_owned())
        })),
    ]);

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();

    assert_eq!(bus.now(), Duration::from_secs(1));
    assert_eq!(counted(&store), (0, 1, 0, 0));
    assert_eq!(store.provider().commits.get(), 1);
}

#[test]
fn given_every_slot_taken_when_another_put_is_slow_should_send_it_no_second_attempt() {
    let (bus, store) = store_with(StoreCallPolicy {
        second_attempts: SecondAttempts {
            slots: 1,
            ..policy().second_attempts
        },
        ..policy()
    });
    let (first, second) = (object(1), object(2));
    store.provider().script([
        Script::commit_after(Duration::from_secs(2)),
        Script::commit_after(Duration::from_secs(2)),
        Script::commit_after(FAST),
    ]);
    let finished = |name: &'static str, bytes: Bytes| {
        let (bus, store) = (bus.clone(), &store);
        async move {
            let options = create_only(&bytes);
            store.put(name, body(&bytes), &options).await.unwrap();
            bus.now()
        }
    };

    let (first_done, second_done) = bus.run(future::join(
        finished("a/1", first),
        finished("a/2", second),
    ));

    assert_eq!(first_done, THRESHOLD + FAST);
    assert_eq!(second_done, Duration::from_secs(2));
    assert_eq!(counted(&store).1, 1);
    assert_eq!(store.provider().puts.borrow().len(), 3);
}

#[test]
fn given_second_attempts_turned_off_when_a_put_is_slow_should_wait_for_its_one_attempt() {
    let (bus, store) = store_with(StoreCallPolicy {
        second_attempts: SecondAttempts {
            after: Duration::ZERO,
            ..policy().second_attempts
        },
        ..policy()
    });
    let bytes = object(1);
    store
        .provider()
        .script([Script::commit_after(Duration::from_secs(2))]);

    bus.run(store.put("a/b", body(&bytes), &create_only(&bytes)))
        .unwrap();

    assert_eq!(bus.now(), Duration::from_secs(2));
    assert_eq!(counted(&store).1, 0);
    assert_eq!(store.provider().puts.borrow().len(), 1);
}

/// A dropped first attempt of an overwrite can land after a later put of
/// other bytes, so only a create-only put gets a second one.
#[test]
fn given_a_slow_overwrite_should_send_no_second_attempt() {
    let (bus, store) = store();
    let bytes = object(1);
    store
        .provider()
        .script([Script::commit_after(Duration::from_secs(2))]);

    bus.run(store.put("a/b", body(&bytes), &PutOptions::default()))
        .unwrap();

    assert_eq!(bus.now(), Duration::from_secs(2));
    assert_eq!(counted(&store).1, 0);
}

/// The measured shape: most puts take tens of milliseconds and a few take
/// seconds. Each put then takes at most the threshold and one normal put.
#[test]
fn given_a_store_that_answers_one_put_in_ten_late_should_bound_every_put_and_store_each_once() {
    const PUTS: usize = 100;
    let (bus, store) = store();
    for index in 0..PUTS {
        if index % 10 == 0 {
            store.provider().script([
                Script::commit_after(Duration::from_secs(4)),
                Script::commit_after(FAST),
            ]);
        } else {
            store.provider().script([Script::commit_after(FAST)]);
        }
    }
    let objects: Vec<Bytes> = (0..PUTS)
        .map(|index| object(u8::try_from(index).unwrap()))
        .collect();

    let mut slowest = Duration::ZERO;
    for (index, bytes) in objects.iter().enumerate() {
        let started = bus.now();
        bus.run(store.put(&format!("a/{index}"), body(bytes), &create_only(bytes)))
            .unwrap();
        slowest = slowest.max(bus.now().saturating_sub(started));
    }
    bus.run_for(Duration::from_secs(10));

    assert_eq!(slowest, THRESHOLD + FAST);
    let provider = store.provider();
    assert_eq!(provider.commits.get(), PUTS);
    assert_eq!(provider.refusals.get(), PUTS / 10);
    for (index, bytes) in objects.iter().enumerate() {
        assert_eq!(&stored(&store, &format!("a/{index}")), bytes, "{index}");
    }
    assert_eq!(counted(&store), (0, 10, 10, 0));
}
