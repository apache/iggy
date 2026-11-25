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

//! An object store bucket that the simulated clusters share.
//!
//! The bucket keeps its objects in memory, as [`iggy_object_store::MemoryStore`]
//! does, and answers on the virtual clock of the cluster that calls, so a run
//! with store faults replays from its seed.
//! Each cluster reaches the bucket through a [`BucketHandle`] of its own. The
//! owner writes through its handle. A peer wraps its handle in
//! [`iggy_object_store::ReadOnlyStore`], as a server does with the store of a
//! peer.
//!
//! The faults of each call come from the bucket's own PRNG stream: a latency,
//! a transient error before the call reaches the objects, and a put or delete
//! that takes effect but answers as failed. The harness can also stall the
//! bucket until it heals, take it down, and cut one cluster off from it. The
//! access log keeps every call with the cluster that made it, and the change
//! log keeps the key of every put and delete that committed.

// The clusters of a run share one thread, as the shards of a node do.
#![allow(clippy::future_not_send)]

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::future::poll_fn;
use std::mem;
use std::ops::Range;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use clock::Clock;
use iggy_object_store::{
    DeleteOutcome, ListPage, ListRequest, ObjectMeta, ObjectRange, ObjectStore, ObjectStoreError,
    Objects, PutBody, PutOptions, StoreCall, checked_crc32c, read_body, validate_delete,
    validate_key, validate_metadata, validate_prefix, validate_range,
};
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::SeedableRng;

use crate::deps::SimClock;
use crate::executor::time::TimerHandle;

/// One simulated cluster: the owner of a bucket, or a peer that reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterId(pub u8);

/// The faults that a bucket draws for each call. The default draws none, and
/// then a call draws nothing from the bucket's stream.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BucketFaults {
    /// The shortest time a call takes.
    pub latency_min: Duration,
    /// The longest time a call takes. Each call draws its time between the
    /// two.
    pub latency_max: Duration,
    /// The chance that a call fails before it reaches the objects.
    pub error_probability: f64,
    /// The chance that a put or a delete takes effect and then answers as
    /// failed. A real store can lose the answer to a call that it applied.
    pub lost_answer_probability: f64,
}

/// One call that reached a bucket, in the order the calls arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Access {
    pub cluster: ClusterId,
    pub call: StoreCall,
}

/// A bucket that the clusters of a run share. A clone is another handle to
/// the same bucket.
#[derive(Debug, Clone)]
pub struct SimBucket {
    shared: Rc<Shared>,
}

/// How one cluster reaches a [`SimBucket`]. Its calls wait on the timer of
/// that cluster.
#[derive(Debug, Clone)]
pub struct BucketHandle {
    bucket: SimBucket,
    cluster: ClusterId,
    timer: TimerHandle,
}

#[derive(Debug)]
struct Shared {
    owner: ClusterId,
    objects: RefCell<Objects>,
    /// The key of each put and delete that committed, in commit order.
    changes: RefCell<Vec<String>>,
    prng: RefCell<Xoshiro256PlusPlus>,
    faults: Cell<BucketFaults>,
    down: Cell<bool>,
    cut: RefCell<BTreeSet<ClusterId>>,
    stall: RefCell<Stall>,
    accesses: RefCell<Vec<Access>>,
}

/// The calls that wait for the bucket to heal.
#[derive(Debug, Default)]
struct Stall {
    on: bool,
    waiters: Vec<Waker>,
}

/// What a call drew from the bucket's stream when it arrived.
#[derive(Debug, Clone, Copy)]
struct Draws {
    latency: Duration,
    fails: bool,
    loses_answer: bool,
}

impl SimBucket {
    /// An empty bucket of `owner` whose faults draw from a stream seeded with
    /// `seed`.
    #[must_use]
    pub fn new(owner: ClusterId, seed: u64, faults: BucketFaults) -> Self {
        Self {
            shared: Rc::new(Shared {
                owner,
                objects: RefCell::default(),
                changes: RefCell::default(),
                prng: RefCell::new(Xoshiro256PlusPlus::seed_from_u64(seed)),
                faults: Cell::new(faults),
                down: Cell::new(false),
                cut: RefCell::default(),
                stall: RefCell::default(),
                accesses: RefCell::default(),
            }),
        }
    }

    /// How `cluster` reaches the bucket, with calls that wait on `timer`.
    #[must_use]
    pub fn handle(&self, cluster: ClusterId, timer: TimerHandle) -> BucketHandle {
        BucketHandle {
            bucket: self.clone(),
            cluster,
            timer,
        }
    }

    #[must_use]
    pub fn owner(&self) -> ClusterId {
        self.shared.owner
    }

    /// The faults that calls draw from now on.
    pub fn set_faults(&self, faults: BucketFaults) {
        self.shared.faults.set(faults);
    }

    /// Take the bucket down, so every call fails once its latency passed, or
    /// bring it back.
    pub fn set_down(&self, down: bool) {
        self.shared.down.set(down);
    }

    /// Cut `cluster` off, so its calls fail as a call over a broken link does,
    /// or connect it again.
    pub fn set_cut(&self, cluster: ClusterId, cut: bool) {
        let mut clusters = self.shared.cut.borrow_mut();
        if cut {
            clusters.insert(cluster);
        } else {
            clusters.remove(&cluster);
        }
    }

    /// Make every call wait, the ones in flight included, until [`Self::heal`].
    pub fn stall(&self) {
        self.shared.stall.borrow_mut().on = true;
    }

    /// End a [`Self::stall`] and let the calls that wait go on.
    pub fn heal(&self) {
        let waiters = {
            let mut stall = self.shared.stall.borrow_mut();
            stall.on = false;
            mem::take(&mut stall.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    /// Every key, in byte order.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.shared.objects.borrow().keys()
    }

    /// The bytes under `key`, read past every fault and outside the access
    /// log.
    #[must_use]
    pub fn object(&self, key: &str) -> Option<Bytes> {
        self.shared.objects.borrow().bytes(key)
    }

    /// The keys of the puts and deletes that committed, from change `from`
    /// on, in commit order. A key that changed twice is there twice.
    #[must_use]
    pub fn changes_since(&self, from: usize) -> Vec<String> {
        self.shared
            .changes
            .borrow()
            .get(from..)
            .unwrap_or_default()
            .to_vec()
    }

    /// Every call so far, in the order the calls arrived.
    #[must_use]
    pub fn accesses(&self) -> Vec<Access> {
        self.shared.accesses.borrow().clone()
    }

    /// The calls of a cluster other than the owner that could change the
    /// bucket. A peer never makes one.
    #[must_use]
    pub fn foreign_writes(&self) -> Vec<Access> {
        self.shared
            .accesses
            .borrow()
            .iter()
            .filter(|access| access.cluster != self.shared.owner && access.call.mutates())
            .cloned()
            .collect()
    }
}

impl Shared {
    fn draw(&self, mutates: bool) -> Draws {
        let faults = self.faults.get();
        let mut prng = self.prng.borrow_mut();
        let latency = if faults.latency_max > faults.latency_min {
            prng.random_range(faults.latency_min..=faults.latency_max)
        } else {
            faults.latency_min
        };
        let mut chance = |probability: f64| probability > 0.0 && prng.random_bool(probability);
        Draws {
            latency,
            fails: chance(faults.error_probability),
            loses_answer: mutates && chance(faults.lost_answer_probability),
        }
    }

    /// Ready once the bucket does not stall.
    async fn healed(&self) {
        poll_fn(|context| {
            let mut stall = self.stall.borrow_mut();
            if !stall.on {
                return Poll::Ready(());
            }
            if !stall
                .waiters
                .iter()
                .any(|waiter| waiter.will_wake(context.waker()))
            {
                stall.waiters.push(context.waker().clone());
            }
            Poll::Pending
        })
        .await;
    }

    fn unreachable_from(&self, cluster: ClusterId) -> bool {
        self.down.get() || self.cut.borrow().contains(&cluster)
    }
}

impl BucketHandle {
    #[must_use]
    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    /// Log `call`, draw its faults, and wait out its latency and any stall.
    /// The error is the one the call fails with before it reaches the
    /// objects.
    async fn arrive(&self, call: StoreCall) -> Result<Draws, ObjectStoreError> {
        let shared = &self.bucket.shared;
        let draws = shared.draw(call.mutates());
        shared.accesses.borrow_mut().push(Access {
            cluster: self.cluster,
            call,
        });
        if !draws.latency.is_zero() {
            self.timer.sleep(draws.latency).await;
        }
        shared.healed().await;
        if shared.unreachable_from(self.cluster) {
            return Err(ObjectStoreError::Transport(
                "simulated: the bucket is unreachable".to_owned(),
            ));
        }
        if draws.fails {
            return Err(unavailable());
        }
        Ok(draws)
    }

    /// The time a put commits at, on the clock of this handle's cluster.
    fn commit_time(&self) -> SystemTime {
        let micros = SimClock::new(self.timer.clone()).realtime().as_micros();
        UNIX_EPOCH + Duration::from_micros(micros)
    }
}

impl ObjectStore for BucketHandle {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        validate_key(key)?;
        validate_metadata(&options.metadata)?;
        let bytes = read_body(body).await?;
        let draws = self
            .arrive(StoreCall::Put {
                key: key.to_owned(),
                mode: options.mode.clone(),
            })
            .await?;
        let crc32c = checked_crc32c(key, &bytes, options)?;
        let shared = &self.bucket.shared;
        let committed =
            shared
                .objects
                .borrow_mut()
                .insert(key, bytes, crc32c, options, self.commit_time());
        if committed.is_ok() {
            shared.changes.borrow_mut().push(key.to_owned());
        }
        if draws.loses_answer {
            return Err(unavailable());
        }
        committed
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        validate_key(key)?;
        validate_range(&range)?;
        self.arrive(StoreCall::GetRange {
            key: key.to_owned(),
            range: range.clone(),
        })
        .await?;
        self.bucket.shared.objects.borrow().get_range(key, &range)
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        validate_key(key)?;
        self.arrive(StoreCall::Head {
            key: key.to_owned(),
        })
        .await?;
        self.bucket.shared.objects.borrow().head(key)
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        validate_delete(keys)?;
        let draws = self
            .arrive(StoreCall::Delete {
                keys: keys.to_vec(),
            })
            .await?;
        let shared = &self.bucket.shared;
        let deleted = shared.objects.borrow_mut().delete(keys);
        shared.changes.borrow_mut().extend_from_slice(keys);
        if draws.loses_answer {
            return Err(unavailable());
        }
        Ok(deleted)
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        validate_prefix(request.prefix)?;
        self.arrive(StoreCall::List {
            prefix: request.prefix.to_owned(),
        })
        .await?;
        Ok(self.bucket.shared.objects.borrow().list(request))
    }
}

fn unavailable() -> ObjectStoreError {
    ObjectStoreError::Unavailable("simulated: the bucket failed the call".to_owned())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::executor::block_on;
    use iggy_object_store::{
        ErrorClass, ObjectStore, PutBody, PutMode, PutOptions, ReadOnlyStore, StoreCall,
    };

    use super::{Access, BucketFaults, BucketHandle, ClusterId, SimBucket};
    use crate::executor::{DetExecutor, RunOutcome};

    const OWNER: ClusterId = ClusterId(0);
    const PEER: ClusterId = ClusterId(1);
    const POLL_BUDGET: u32 = 10_000;

    /// The owner's handle of a bucket without faults, on a timer that never
    /// moves.
    fn owner_handle(bucket: &SimBucket) -> BucketHandle {
        bucket.handle(OWNER, DetExecutor::new(0).timer())
    }

    fn body(bytes: &'static [u8]) -> PutBody {
        PutBody::from_bytes(Bytes::from_static(bytes))
    }

    /// Run `call` as a task of `executor`, and return a cell that holds its
    /// output once it ends.
    fn spawned<T: 'static>(
        executor: &mut DetExecutor,
        call: impl Future<Output = T> + 'static,
    ) -> Rc<RefCell<Option<T>>> {
        let output = Rc::new(RefCell::new(None));
        let ended = Rc::clone(&output);
        executor.spawn(async move {
            *ended.borrow_mut() = Some(call.await);
        });
        output
    }

    fn settle(executor: &mut DetExecutor) {
        assert!(matches!(
            executor.run_until_stalled(POLL_BUDGET),
            RunOutcome::Quiescent { .. }
        ));
    }

    macro_rules! sim_bucket {
        ($($case:ident),* $(,)?) => {$(
            #[test]
            fn $case() {
                // The bucket stamps puts with the virtual clock on purpose,
                // and this case compares them with the wall clock.
                if stringify!($case) == "given_object_when_reading_times_should_report_commit_time" {
                    return;
                }
                let bucket = SimBucket::new(OWNER, 7, BucketFaults::default());
                block_on(iggy_object_store::contract::$case(
                    &owner_handle(&bucket),
                    stringify!($case),
                ));
            }
        )*};
    }

    iggy_object_store::contract_cases!(sim_bucket);
    iggy_object_store::if_absent_contract_cases!(sim_bucket);

    #[test]
    fn given_a_put_when_heading_should_stamp_the_virtual_clock_of_the_writer() {
        let mut executor = DetExecutor::new(1);
        let bucket = SimBucket::new(OWNER, 1, BucketFaults::default());
        let handle = bucket.handle(OWNER, executor.timer());
        executor.advance_time(Duration::from_secs(90));

        block_on(handle.put("a/b", body(b"x"), &PutOptions::default())).unwrap();
        let head = block_on(handle.head("a/b")).unwrap();

        let stamped = head
            .last_modified
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        assert_eq!(
            stamped,
            Duration::from_micros(crate::deps::SIM_EPOCH_MICROS) + Duration::from_secs(90)
        );
    }

    #[test]
    fn given_a_latency_when_calling_should_answer_once_virtual_time_passes_it() {
        let mut executor = DetExecutor::new(2);
        let bucket = SimBucket::new(
            OWNER,
            2,
            BucketFaults {
                latency_min: Duration::from_millis(30),
                latency_max: Duration::from_millis(30),
                ..BucketFaults::default()
            },
        );
        let handle = bucket.handle(OWNER, executor.timer());

        let put = spawned(&mut executor, async move {
            handle.put("a/b", body(b"x"), &PutOptions::default()).await
        });
        settle(&mut executor);
        assert!(put.borrow().is_none(), "the put waits out its latency");
        assert_eq!(
            bucket.keys(),
            Vec::<String>::new(),
            "nothing commits before it answers"
        );

        executor.advance_time(Duration::from_millis(30));
        settle(&mut executor);

        assert!(matches!(*put.borrow(), Some(Ok(()))));
        assert_eq!(bucket.keys(), ["a/b"]);
    }

    #[test]
    fn given_lost_answers_when_putting_should_store_the_object_and_answer_transient() {
        let bucket = SimBucket::new(
            OWNER,
            3,
            BucketFaults {
                lost_answer_probability: 1.0,
                ..BucketFaults::default()
            },
        );
        let handle = owner_handle(&bucket);
        let if_absent = PutOptions {
            mode: PutMode::IfAbsent,
            ..PutOptions::default()
        };

        let first = block_on(handle.put("a/b", body(b"mine"), &if_absent));
        bucket.set_faults(BucketFaults::default());
        let retry = block_on(handle.put("a/b", body(b"mine"), &if_absent));

        assert_eq!(
            first.unwrap_err().class(),
            ErrorClass::Transient,
            "the answer is lost"
        );
        assert_eq!(bucket.object("a/b").unwrap(), "mine", "the put committed");
        assert_eq!(
            retry.unwrap_err().class(),
            ErrorClass::PreconditionFailed,
            "a retry meets the caller's own object"
        );
    }

    #[test]
    fn given_errors_when_putting_should_store_nothing() {
        let bucket = SimBucket::new(
            OWNER,
            4,
            BucketFaults {
                error_probability: 1.0,
                ..BucketFaults::default()
            },
        );
        let handle = owner_handle(&bucket);

        let put = block_on(handle.put("a/b", body(b"x"), &PutOptions::default()));
        let read = block_on(handle.get_range("a/b", 0..1));

        assert_eq!(put.unwrap_err().class(), ErrorClass::Transient);
        assert_eq!(read.unwrap_err().class(), ErrorClass::Transient);
        assert_eq!(bucket.keys(), Vec::<String>::new());
    }

    #[test]
    fn given_a_stall_when_it_heals_should_answer_the_calls_that_waited() {
        let mut executor = DetExecutor::new(5);
        let bucket = SimBucket::new(OWNER, 5, BucketFaults::default());
        let handle = bucket.handle(OWNER, executor.timer());
        bucket.stall();

        let put = spawned(&mut executor, async move {
            handle.put("a/b", body(b"x"), &PutOptions::default()).await
        });
        settle(&mut executor);
        executor.advance_time(Duration::from_secs(60));
        settle(&mut executor);
        assert!(
            put.borrow().is_none(),
            "the put waits while the bucket stalls"
        );

        bucket.heal();
        settle(&mut executor);

        assert!(matches!(*put.borrow(), Some(Ok(()))));
        assert_eq!(bucket.keys(), ["a/b"]);
    }

    #[test]
    fn given_an_outage_or_a_cut_link_when_calling_should_fail_transient_until_it_ends() {
        let bucket = SimBucket::new(OWNER, 6, BucketFaults::default());
        let owner = owner_handle(&bucket);
        let peer = bucket.handle(PEER, DetExecutor::new(6).timer());
        block_on(owner.put("a/b", body(b"x"), &PutOptions::default())).unwrap();

        bucket.set_cut(PEER, true);
        let cut_read = block_on(peer.head("a/b"));
        let owner_read = block_on(owner.head("a/b"));
        bucket.set_cut(PEER, false);
        bucket.set_down(true);
        let down_read = block_on(owner.head("a/b"));
        bucket.set_down(false);
        let back = block_on(peer.head("a/b"));

        assert_eq!(cut_read.unwrap_err().class(), ErrorClass::Transient);
        assert!(
            owner_read.is_ok(),
            "a cut link leaves the owner's calls alone"
        );
        assert_eq!(down_read.unwrap_err().class(), ErrorClass::Transient);
        assert!(back.is_ok());
    }

    #[test]
    fn given_a_peer_behind_a_read_only_store_when_it_calls_should_log_no_foreign_write() {
        let bucket = SimBucket::new(OWNER, 8, BucketFaults::default());
        let owner = owner_handle(&bucket);
        let peer = ReadOnlyStore::new(bucket.handle(PEER, DetExecutor::new(8).timer()));
        let writable_peer = bucket.handle(PEER, DetExecutor::new(8).timer());
        block_on(owner.put("a/b", body(b"x"), &PutOptions::default())).unwrap();

        assert!(block_on(peer.put("a/c", body(b"y"), &PutOptions::default())).is_err());
        assert!(block_on(peer.head("a/b")).is_ok());
        assert_eq!(bucket.foreign_writes(), Vec::new());

        let deleted = block_on(writable_peer.delete(&["a/b".to_owned()])).unwrap();

        assert!(deleted.is_complete());
        assert_eq!(
            bucket.foreign_writes(),
            vec![Access {
                cluster: PEER,
                call: StoreCall::Delete {
                    keys: vec!["a/b".to_owned()],
                },
            }]
        );
        assert_eq!(bucket.accesses().len(), 3, "the refused put never arrived");
    }

    #[test]
    fn given_one_seed_when_drawing_faults_twice_should_draw_the_same_calls() {
        let outcomes = |seed: u64| {
            let bucket = SimBucket::new(
                OWNER,
                seed,
                BucketFaults {
                    error_probability: 0.5,
                    ..BucketFaults::default()
                },
            );
            let handle = owner_handle(&bucket);
            (0..64)
                .map(|_| block_on(handle.head("a/missing")).unwrap_err().class())
                .collect::<Vec<_>>()
        };

        assert_eq!(outcomes(9), outcomes(9));
        assert_ne!(outcomes(9), outcomes(10));
    }
}
