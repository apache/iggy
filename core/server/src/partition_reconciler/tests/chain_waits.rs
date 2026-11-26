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

//! The partitions whose segment chain is gone and that wait for what the
//! object store holds of them.

use super::*;
use crate::partition_reconciler::{
    CHAIN_STORE_READS, ChainWait, RetirementTarget, copy_lost_here, start_chain_read, yield_now,
};
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::LocalBoxFuture;
use iggy_common::PartitionStats;
use iggy_object_store::{TierJobCancelled, TierScopeToken};
use partitions::{StoreViewOutcome, StoreViewReader, StoreViewRequest};
use std::cell::RefCell;

const INCARNATION: u64 = 7;

/// A reader whose reads end only when the test answers them.
#[derive(Default)]
struct HeldReader {
    reads: RefCell<Vec<HeldRead>>,
}

struct HeldRead {
    request: StoreViewRequest,
    scope: TierScopeToken,
    answer: oneshot::Sender<StoreViewOutcome>,
}

impl StoreViewReader for HeldReader {
    fn read(
        &self,
        request: StoreViewRequest,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, StoreViewOutcome> {
        let (answer, answered) = oneshot::channel();
        self.reads.borrow_mut().push(HeldRead {
            request,
            scope,
            answer,
        });
        async move { answered.await.unwrap_or(Err(TierJobCancelled)) }.boxed_local()
    }
}

fn request(ns: IggyNamespace, created_revision: u64, from: u64) -> StoreViewRequest {
    StoreViewRequest {
        prefix: object_prefix(ns, created_revision),
        from,
        front: from + 100,
        local_sealed: Vec::new(),
    }
}

/// A shard with a held reader, whose reconciler wakes count in the cell.
fn ctx_with_reader(tmp: &TempDir) -> (Rc<ReconcilerCtx>, Rc<HeldReader>, Rc<Cell<usize>>) {
    let config = test_config(tmp);
    let shard = build_test_shard(0, &config, TestMux::default());
    let reader = Rc::new(HeldReader::default());
    shard.set_store_view_reader(Some(Rc::clone(&reader) as Rc<dyn StoreViewReader>));
    let wakes = Rc::new(Cell::new(0));
    shard.set_metadata_tick_handler(Some(Rc::new({
        let wakes = Rc::clone(&wakes);
        move || wakes.set(wakes.get() + 1)
    })));
    (make_ctx(shard, Rc::new(config)), reader, wakes)
}

fn wait_of(ctx: &ReconcilerCtx, ns: IggyNamespace) -> Option<&'static str> {
    ctx.chain_rebuilds.borrow().get(&ns).map(|wait| match wait {
        ChainWait::Queued(_) => "queued",
        ChainWait::Reading { .. } => "reading",
        ChainWait::Read { .. } => "read",
    })
}

async fn settle() {
    for _ in 0..16 {
        yield_now().await;
    }
}

#[compio::test]
async fn given_a_partition_that_waits_for_the_store_when_its_read_runs_should_leave_the_pass_and_wake_the_reconciler_when_it_ends()
 {
    let tmp = TempDir::new().expect("tempdir for system path");
    let (ctx, reader, wakes) = ctx_with_reader(&tmp);
    let ns = IggyNamespace::new(0, 0, 0);

    start_chain_read(&ctx, ns, request(ns, INCARNATION, 0));

    assert_eq!(wait_of(&ctx, ns), Some("reading"));
    assert!(ctx.chain_read_due(ns), "a read under way backs nothing off");
    assert_eq!(wakes.get(), 0);

    let read = reader.reads.borrow_mut().remove(0);
    assert_eq!(read.request, request(ns, INCARNATION, 0));
    read.answer
        .send(Err(TierJobCancelled))
        .unwrap_or_else(|_| panic!("the read is waited for"));
    settle().await;

    assert_eq!(wait_of(&ctx, ns), Some("read"));
    assert_eq!(wakes.get(), 1, "the end of the read wakes the reconciler");
}

#[compio::test]
async fn given_more_waiting_partitions_than_reads_when_reading_should_queue_the_rest_without_backing_them_off()
 {
    let tmp = TempDir::new().expect("tempdir for system path");
    let (ctx, reader, _) = ctx_with_reader(&tmp);
    let namespaces: Vec<IggyNamespace> = (0..=CHAIN_STORE_READS)
        .map(|partition| IggyNamespace::new(0, 0, partition))
        .collect();

    for ns in &namespaces {
        start_chain_read(&ctx, *ns, request(*ns, INCARNATION, 0));
    }

    assert_eq!(reader.reads.borrow().len(), CHAIN_STORE_READS);
    let last = namespaces[CHAIN_STORE_READS];
    assert_eq!(wait_of(&ctx, last), Some("queued"));
    assert!(
        ctx.chain_read_due(last),
        "a read waiting for a slot gets it when another ends"
    );
}

#[compio::test]
async fn given_a_read_under_way_when_its_wait_is_dropped_should_cancel_it_and_not_bring_it_back() {
    let tmp = TempDir::new().expect("tempdir for system path");
    let (ctx, reader, wakes) = ctx_with_reader(&tmp);
    let ns = IggyNamespace::new(0, 0, 0);
    start_chain_read(&ctx, ns, request(ns, INCARNATION, 0));

    ctx.stop_waiting_for_chain(ns);

    let read = reader.reads.borrow_mut().remove(0);
    assert!(read.scope.is_cancelled());
    let _ = read.answer.send(Err(TierJobCancelled));
    settle().await;
    assert_eq!(wait_of(&ctx, ns), None);
    assert_eq!(wakes.get(), 0);
}

/// A tiered partition with no copy on any replica cannot install a lifecycle
/// fence, so a delete of its topic would wait for the store forever.
#[compio::test]
async fn given_a_copy_that_waits_for_the_store_when_its_topic_is_deleted_should_count_as_lost() {
    let tmp = TempDir::new().expect("tempdir for system path");
    let (ctx, _, _) = ctx_with_reader(&tmp);
    let ns = IggyNamespace::new(0, 0, 0);
    ctx.await_chain_rebuilds([(ns, request(ns, INCARNATION, 0))]);
    let target = |incarnation, deletes| RetirementTarget {
        topic_id: 0,
        partition_id: 0,
        namespace: ns,
        incarnation,
        deletes,
        retired_members: 0,
    };

    assert!(copy_lost_here(&ctx, &target(INCARNATION, true)));
    assert!(
        !copy_lost_here(&ctx, &target(INCARNATION, false)),
        "only a delete loses nothing by retiring a copy the store may rebuild"
    );
    assert!(!copy_lost_here(&ctx, &target(INCARNATION + 1, true)));
}

/// A shard whose committed topic has one partition with a directory that a
/// prior life left, and the committed incarnation and watermark of it.
async fn prior_life(tmp: &TempDir) -> (Rc<TestShard>, Rc<ReconcilerCtx>, IggyNamespace, u64) {
    let config = test_config(tmp);
    let mux = TestMux::default();
    seed_stream(&mux, 1, "stream-a");
    seed_topic(&mux, 2, 0, "topic-a", vec![assignment(0, 1)]);
    let shard = build_test_shard(0, &config, mux);
    let ctx = make_ctx(Rc::clone(&shard), Rc::new(config));
    let ns = IggyNamespace::new(0, 0, 0);
    let inputs = fetch_partition_build_inputs(&ctx, ns).expect("the partition is committed");
    let created_revision = inputs.metadata.created_revision;
    drop(
        build_partition_fresh(
            ctx.config.as_ref(),
            shard.plane.partitions().config(),
            ns,
            Arc::new(PartitionStats::default()),
            created_revision,
            0,
            inputs.runtime.expect("the topic options are readable"),
            CLUSTER_ID,
            0,
            consensus::ReplicaSet::all(1),
            inputs.metadata.created_view,
            Rc::clone(&shard.bus),
        )
        .await
        .expect("the prior life builds"),
    );
    (shard, ctx, ns, created_revision)
}

/// A wait that boot or an earlier check left for another incarnation or
/// another trim asks the store for what the partition no longer needs, so
/// the check loads it again from local state.
#[compio::test]
async fn given_a_wait_for_another_incarnation_or_trim_when_reconciling_should_drop_it_and_load_the_partition()
 {
    let stales: [fn(IggyNamespace, u64) -> StoreViewRequest; 2] = [
        |ns, created_revision| request(ns, created_revision + 1, 0),
        |ns, created_revision| request(ns, created_revision, 42),
    ];
    for stale in stales {
        let tmp = TempDir::new().expect("tempdir for system path");
        let (shard, ctx, ns, created_revision) = prior_life(&tmp).await;
        ctx.await_chain_rebuilds([(ns, stale(ns, created_revision))]);

        reconcile_pass(&ctx).await;

        assert!(shard.plane.partitions().contains(&ns));
        assert_eq!(wait_of(&ctx, ns), None);
    }
}

#[compio::test]
async fn given_a_wait_that_boot_handed_over_when_reconciling_should_read_the_store_with_its_request()
 {
    let tmp = TempDir::new().expect("tempdir for system path");
    let (shard, ctx, ns, created_revision) = prior_life(&tmp).await;
    let reader = Rc::new(HeldReader::default());
    shard.set_store_view_reader(Some(Rc::clone(&reader) as Rc<dyn StoreViewReader>));
    let handed_over = request(ns, created_revision, 0);
    ctx.await_chain_rebuilds([(ns, handed_over.clone())]);

    reconcile_pass(&ctx).await;

    let reads = reader.reads.borrow();
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].request, handed_over);
    assert_eq!(wait_of(&ctx, ns), Some("reading"));
    assert!(
        !shard.plane.partitions().contains(&ns),
        "the partition waits for the store"
    );
}
