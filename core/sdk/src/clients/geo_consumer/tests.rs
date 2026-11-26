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

use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures::future::{BoxFuture, ready};
use futures::task::{ArcWake, noop_waker_ref, waker};
use futures::{FutureExt, Stream, StreamExt};
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    Checkpoint, Consumer, Identifier, IggyError, IggyMessage, PartitionContext, PollingStrategy,
    ReaderStamp,
};
use tokio::sync::watch;
use tokio::time::{sleep, timeout};

use super::{
    GeoOrigin, GeoReadError, GeoReceivedMessage, GeoSources, IggyGeoConsumer, LogCatalog,
    LogConsumer, ShadowStream, TopicNames,
};
use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::consumer::ReceivedMessage;
use crate::clients::consumer_builder::IggyConsumerBuilder;
use crate::clients::reader::ReadError;
use crate::tcp::tcp_client::TcpClient;

const STREAM: &str = "orders";
const TOPIC: &str = "payments";
const GROUP: &str = "billing";
const DC_B: &str = "dc-b";
const DC_C: &str = "dc-c";
const INTERVAL: Duration = Duration::from_secs(5);
const TEST_BUDGET: Duration = Duration::from_secs(120);

/// Far above the ids of real readers, which start at 1.
static NEXT_READER: AtomicU64 = AtomicU64::new(1 << 40);

#[tokio::test]
async fn given_messages_in_the_local_topic_and_a_shadow_when_consuming_should_take_turns_and_name_each_origin()
 {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    catalog.log(STREAM).send(0, 0..3);
    catalog.log(&shadow).send(0, 0..3);

    let mut received = Vec::new();
    for _ in 0..6 {
        let message = next_message(&mut consumer).await;
        received.push((
            message.origin.name().map(str::to_owned),
            message.origin.is_shadow(),
            message.checkpoint().offset(),
        ));
    }

    let dc_b = Some(DC_B.to_owned());
    assert_eq!(
        received,
        [
            (None, false, 0),
            (dc_b.clone(), true, 0),
            (None, false, 1),
            (dc_b.clone(), true, 1),
            (None, false, 2),
            (dc_b, true, 2),
        ]
    );
}

#[tokio::test]
async fn given_one_silent_log_when_the_other_has_messages_should_hand_them_over_without_waiting() {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::Both).await;

    catalog.log(STREAM).send(0, 0..2);
    for offset in 0..2 {
        let message = ready_message(&mut consumer);
        assert!(message.origin.is_local());
        assert_eq!(message.checkpoint().offset(), offset);
    }
    catalog.log(&shadow).send(1, 7..9);
    for offset in 7..9 {
        let message = ready_message(&mut consumer);
        assert_eq!(message.origin.name(), Some(DC_B));
        assert_eq!(message.checkpoint().offset(), offset);
    }
    assert!(consumer.next().now_or_never().is_none());
}

#[tokio::test(start_paused = true)]
async fn given_idle_logs_when_polled_should_stay_pending_until_a_log_has_a_message() {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    let wakes = Arc::new(WakeCounter::default());
    let waker = waker(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);

    assert!(Pin::new(&mut consumer).poll_next(&mut cx).is_pending());
    sleep(INTERVAL / 2).await;

    assert_eq!(wakes.count(), 0, "an idle consumer woke itself");
    assert_eq!(catalog.log(STREAM).polls(), 1);
    assert_eq!(catalog.log(&shadow).polls(), 1);
    catalog.log(&shadow).send(0, 0..1);
    assert_eq!(wakes.count(), 1, "a message wakes the consumer");
    assert!(matches!(
        Pin::new(&mut consumer).poll_next(&mut cx),
        Poll::Ready(Some(Ok(_)))
    ));
}

#[tokio::test(start_paused = true)]
async fn given_a_shadow_that_appears_after_init_when_the_next_search_runs_should_read_it_without_a_restart()
 {
    let catalog = FakeCatalog::default();
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    let started = tokio::time::Instant::now();
    let shadow = catalog.add_shadow(DC_B);
    catalog.queue(&shadow, 0, 0..2);

    let first = next_message(&mut consumer).await;
    let second = next_message(&mut consumer).await;

    assert!(started.elapsed() >= INTERVAL, "read before the next search");
    for (message, offset) in [(first, 0), (second, 1)] {
        assert_eq!(message.origin.name(), Some(DC_B));
        assert_eq!(message.checkpoint().offset(), offset);
    }
    assert_eq!(catalog.opens_of(&shadow), 1);
}

#[tokio::test(start_paused = true)]
async fn given_a_shadow_stream_without_the_topic_when_the_topic_arrives_should_read_it_from_the_next_search()
 {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    catalog.world().holding.remove(&shadow);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    assert_eq!(catalog.opens_of(&shadow), 0);

    catalog.world().holding.insert(shadow.clone());
    catalog.queue(&shadow, 0, 0..1);

    let message = next_message(&mut consumer).await;
    assert_eq!(message.origin.name(), Some(DC_B));
    assert_eq!(catalog.opens_of(&shadow), 1);
}

#[tokio::test(start_paused = true)]
async fn given_a_shadow_that_is_gone_when_its_consumer_reports_it_missing_should_drop_it_quietly_and_read_the_others()
 {
    let catalog = FakeCatalog::default();
    let gone = catalog.add_shadow(DC_B);
    let staying = catalog.add_shadow(DC_C);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    let gone_log = catalog.log(&gone);
    gone_log.send(0, 0..1);
    let gone_message = next_message(&mut consumer).await;
    catalog.remove_shadow(&gone);
    gone_log.fail(ReadError::new(
        None,
        IggyError::StreamNameNotFound(gone.clone()),
    ));
    catalog.log(&staying).send(0, 0..1);
    let later = catalog.log(&staying);
    tokio::spawn(async move {
        sleep(INTERVAL * 2).await;
        later.send(0, 1..2);
    });

    let first = next_message(&mut consumer).await;
    let second = next_message(&mut consumer).await;

    for (message, offset) in [(first, 0), (second, 1)] {
        assert_eq!(message.origin.name(), Some(DC_C));
        assert_eq!(message.checkpoint().offset(), offset);
    }
    assert!(
        gone_log.is_shut_down(),
        "the consumer of the gone shadow runs"
    );
    assert_eq!(
        consumer.commit(gone_message.checkpoint()).await,
        Err(IggyError::InvalidCommand)
    );
    assert!(!catalog.log(&staying).is_shut_down());
}

#[tokio::test(start_paused = true)]
async fn given_a_shadow_that_reports_itself_missing_without_waiting_when_polled_should_give_way() {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    let log = catalog.log(&shadow);
    let polls_before = log.polls();
    for _ in 0..8 {
        log.fail(ReadError::new(
            None,
            IggyError::StreamNameNotFound(shadow.clone()),
        ));
    }
    let wakes = Arc::new(WakeCounter::default());
    let waker = waker(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    let wakes_before = wakes.count();

    assert!(Pin::new(&mut consumer).poll_next(&mut cx).is_pending());

    assert_eq!(log.polls() - polls_before, 2, "one poll, and one more");
    assert!(wakes.count() > wakes_before, "it asks to be polled again");
}

#[tokio::test(start_paused = true)]
async fn given_a_shadow_that_still_holds_the_topic_when_its_consumer_reports_it_missing_should_report_the_error_at_the_next_search()
 {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    let started = tokio::time::Instant::now();
    let missing = IggyError::TopicNameNotFound(TOPIC.to_owned(), shadow.clone());
    catalog
        .log(&shadow)
        .fail(ReadError::new(None, missing.clone()));
    catalog.log(&shadow).send(0, 0..1);

    let message = ready_message(&mut consumer);
    let error = next_error(&mut consumer).await;

    assert_eq!(message.origin.name(), Some(DC_B));
    assert!(started.elapsed() >= INTERVAL, "reported before the search");
    assert_eq!(error.origin.as_ref().and_then(GeoOrigin::name), Some(DC_B));
    assert_eq!(error.stream.as_deref(), Some(shadow.as_str()));
    assert_eq!(error.error.error, missing);
    assert!(!catalog.log(&shadow).is_shut_down());
}

#[tokio::test(start_paused = true)]
async fn given_local_only_when_shadows_exist_should_read_the_local_topic_and_never_list_the_streams()
 {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::LocalOnly).await;
    let local = catalog.log(STREAM);
    tokio::spawn(async move {
        sleep(INTERVAL * 3).await;
        local.send(0, 0..1);
    });

    let message = next_message(&mut consumer).await;

    assert!(message.origin.is_local());
    assert_eq!(catalog.opens_of(&shadow), 0);
    assert_eq!(catalog.world().listings, 0);
}

#[tokio::test]
async fn given_shadows_only_when_the_local_topic_exists_should_read_only_the_shadows() {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::ShadowsOnly).await;
    catalog.log(&shadow).send(0, 0..1);

    let message = next_message(&mut consumer).await;

    assert_eq!(message.origin.name(), Some(DC_B));
    assert_eq!(catalog.opens_of(STREAM), 0);
}

#[tokio::test]
async fn given_messages_of_two_logs_when_committing_and_marking_them_should_reach_the_log_that_yielded_each()
 {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    catalog.log(STREAM).send(0, 0..1);
    catalog.log(&shadow).send(0, 4..5);
    let local = next_message(&mut consumer).await;
    let shadowed = next_message(&mut consumer).await;

    consumer.commit(shadowed.checkpoint()).await.unwrap();
    consumer.commit(local.checkpoint()).await.unwrap();
    consumer.mark(shadowed.checkpoint()).unwrap();

    assert_eq!(catalog.log(STREAM).commits(), [local.checkpoint()]);
    assert_eq!(catalog.log(&shadow).commits(), [shadowed.checkpoint()]);
    assert_eq!(catalog.log(STREAM).marks(), []);
    assert_eq!(catalog.log(&shadow).marks(), [shadowed.checkpoint()]);
    let foreign = Checkpoint::new(0, 4, PartitionContext::default());
    assert_eq!(
        consumer.commit(foreign).await,
        Err(IggyError::InvalidCommand)
    );
    assert_eq!(consumer.mark(foreign), Err(IggyError::InvalidCommand));
}

#[tokio::test]
async fn given_an_error_of_a_shadow_when_seeking_its_partition_should_name_its_origin_and_seek_its_log()
 {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    catalog
        .log(&shadow)
        .fail(ReadError::new(Some(1), IggyError::HistoryUnavailable));

    let error = next_error(&mut consumer).await;
    let origin = error.origin.clone().expect("the error names its log");
    consumer
        .seek(&origin, 1, PollingStrategy::next())
        .await
        .unwrap();

    assert_eq!(origin.name(), Some(DC_B));
    assert_eq!(error.stream.as_deref(), Some(shadow.as_str()));
    assert_eq!(error.error.partition_id, Some(1));
    assert_eq!(
        error.to_string(),
        format!(
            "stream {shadow} of origin {DC_B}: partition ID: 1: {}",
            IggyError::HistoryUnavailable
        )
    );
    assert_eq!(catalog.log(&shadow).seeks(), [(1, PollingStrategy::next())]);
    assert_eq!(catalog.log(STREAM).seeks(), []);
}

#[tokio::test(start_paused = true)]
async fn given_a_shadow_that_refuses_the_user_when_searching_should_report_it_once_and_read_the_others()
 {
    let catalog = FakeCatalog::default();
    let refusing = catalog.add_shadow(DC_B);
    catalog
        .world()
        .refusals
        .insert(refusing.clone(), IggyError::Unauthorized);
    let readable = catalog.add_shadow(DC_C);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    catalog.log(&readable).send(0, 0..1);
    let later = catalog.log(&readable);
    tokio::spawn(async move {
        sleep(INTERVAL * 6).await;
        later.send(0, 1..2);
    });

    let error = next_error(&mut consumer).await;
    let first = next_message(&mut consumer).await;
    let second = next_message(&mut consumer).await;

    assert_eq!(error.origin.as_ref().and_then(GeoOrigin::name), Some(DC_B));
    assert_eq!(error.stream.as_deref(), Some(refusing.as_str()));
    assert_eq!(error.error.error, IggyError::Unauthorized);
    assert_eq!(first.origin.name(), Some(DC_C));
    assert_eq!(second.checkpoint().offset(), 1);
    let world = catalog.world();
    let attempts = world.attempts.get(&refusing).copied().unwrap_or_default();
    assert!(
        attempts > 1 && attempts < world.listings,
        "{attempts} attempts in {} searches",
        world.listings
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_listing_that_keeps_failing_when_searching_should_report_it_once_and_read_the_local_topic()
 {
    let catalog = FakeCatalog::default();
    catalog.world().listing_error = Some(IggyError::Disconnected);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    let local = catalog.log(STREAM);
    tokio::spawn(async move {
        sleep(INTERVAL * 3).await;
        local.send(0, 0..1);
    });

    let error = next_error(&mut consumer).await;
    let message = next_message(&mut consumer).await;

    assert_eq!(error.origin, None);
    assert_eq!(error.error.error, IggyError::Disconnected);
    assert!(message.origin.is_local());
    assert!(catalog.world().listings > 2);
}

#[tokio::test]
async fn given_a_consumer_of_two_logs_when_shutting_down_should_shut_down_each_and_end() {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::Both).await;

    consumer.shutdown().await.unwrap();

    assert!(catalog.log(STREAM).is_shut_down());
    assert!(catalog.log(&shadow).is_shut_down());
    assert!(consumer.next().await.is_none());
    assert_eq!(
        consumer
            .seek(&GeoOrigin::local(), 0, PollingStrategy::next())
            .await,
        Err(IggyError::InvalidConfiguration)
    );
}

#[tokio::test]
async fn given_a_shut_down_consumer_when_init_runs_again_should_refuse_it() {
    let catalog = FakeCatalog::default();
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    consumer.shutdown().await.unwrap();

    assert_eq!(consumer.init().await, Err(IggyError::InvalidConfiguration));
    assert!(consumer.next().await.is_none());
    assert_eq!(catalog.attempts_of(STREAM), 1);
}

#[tokio::test]
async fn given_an_init_cancelled_while_the_local_consumer_joins_when_init_runs_again_should_open_it_once()
 {
    let catalog = FakeCatalog::default();
    let gate = catalog.hold_opens(STREAM);
    let mut consumer = catalog.consumer(GeoSources::Both);

    cancel_after_polls(consumer.init());
    gate.open();
    consumer.init().await.unwrap();
    catalog.log(STREAM).send(0, 0..1);

    assert!(ready_message(&mut consumer).origin.is_local());
    assert_eq!(catalog.attempts_of(STREAM), 1);
    assert_eq!(catalog.opens_of(STREAM), 1);
}

#[tokio::test]
async fn given_an_init_cancelled_during_the_first_search_when_init_runs_again_should_keep_one_local_consumer()
 {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let gate = catalog.hold_listings();
    let mut consumer = catalog.consumer(GeoSources::Both);

    cancel_after_polls(consumer.init());
    gate.open();
    consumer.init().await.unwrap();
    catalog.log(STREAM).send(0, 0..2);
    catalog.log(&shadow).send(0, 0..1);
    let mut origins = Vec::new();
    for _ in 0..3 {
        origins.push(ready_message(&mut consumer).origin);
    }

    assert_eq!(catalog.attempts_of(STREAM), 1);
    assert_eq!(catalog.world().listings, 1, "init resumed its search");
    assert_eq!(origins.iter().filter(|origin| origin.is_local()).count(), 2);
    assert!(consumer.next().now_or_never().is_none());
}

#[tokio::test]
async fn given_a_shutdown_cancelled_while_a_shadow_joins_when_shutdown_runs_again_should_shut_down_every_consumer()
 {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let gate = catalog.hold_opens(&shadow);
    let mut consumer = catalog.initialized(GeoSources::Both).await;

    cancel_after_polls(consumer.shutdown());
    assert!(!catalog.log(STREAM).is_shut_down());
    assert_eq!(consumer.init().await, Err(IggyError::InvalidConfiguration));
    assert!(consumer.next().await.is_none());
    gate.open();
    consumer.shutdown().await.unwrap();

    assert!(catalog.log(STREAM).is_shut_down());
    assert!(catalog.log(&shadow).is_shut_down());
    assert_eq!(consumer.shutdown().await, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn given_a_shadow_whose_join_is_slow_when_initializing_should_read_the_local_topic_and_the_other_shadows_without_waiting()
 {
    let catalog = FakeCatalog::default();
    let slow = catalog.add_shadow(DC_B);
    let healthy = catalog.add_shadow(DC_C);
    let gate = catalog.hold_opens(&slow);
    let mut consumer = catalog.initialized(GeoSources::Both).await;

    catalog.log(STREAM).send(0, 0..1);
    catalog.log(&healthy).send(0, 0..1);
    let first = ready_message(&mut consumer);
    let second = ready_message(&mut consumer);
    assert_eq!(catalog.opens_of(&slow), 0);
    catalog.queue(&slow, 0, 0..1);
    gate.open();
    let third = next_message(&mut consumer).await;

    assert!(first.origin.is_local());
    assert_eq!(second.origin.name(), Some(DC_C));
    assert_eq!(third.origin.name(), Some(DC_B));
    assert_eq!(catalog.attempts_of(&slow), 1);
}

#[tokio::test]
async fn given_only_the_last_log_has_messages_when_consuming_should_count_each_message_in_the_log_that_yielded_it()
 {
    let catalog = FakeCatalog::default();
    let shadow = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    catalog.log(&shadow).send(0, 0..3);

    for _ in 0..3 {
        ready_message(&mut consumer);
    }
    assert!(consumer.next().now_or_never().is_none());

    assert_eq!(catalog.log(&shadow).counted(), 3);
    assert_eq!(catalog.log(STREAM).counted(), 0);
}

#[tokio::test(start_paused = true)]
async fn given_a_message_of_a_later_log_when_a_search_drops_an_earlier_log_before_the_next_poll_should_count_it_in_its_own_log()
 {
    let catalog = FakeCatalog::default();
    let earlier = catalog.add_shadow(DC_B);
    let mut consumer = catalog.initialized(GeoSources::Both).await;
    let later = catalog.add_shadow(DC_C);
    catalog.queue(&later, 0, 0..1);
    let message = next_message(&mut consumer).await;
    assert_eq!(message.origin.name(), Some(DC_C));

    consumer.drop_shadow(&earlier);
    assert!(consumer.next().now_or_never().is_none());

    assert_eq!(catalog.log(&later).counted(), 1);
    assert_eq!(catalog.log(STREAM).counted(), 0);
    assert!(catalog.log(&earlier).is_shut_down());
}

#[test]
fn given_origins_with_and_without_a_name_when_asked_where_they_come_from_should_be_shadows_exactly_when_named()
 {
    let local = GeoOrigin::local();
    let shadow = GeoOrigin::shadow(DC_B);

    assert!(local.is_local() && !local.is_shadow());
    assert_eq!(local.name(), None);
    assert_eq!(local.to_string(), "local");
    assert!(shadow.is_shadow() && !shadow.is_local());
    assert_eq!(shadow.name(), Some(DC_B));
    assert_eq!(shadow.to_string(), DC_B);
}

#[tokio::test]
async fn given_a_consumer_polled_before_init_should_fail_each_poll() {
    let catalog = FakeCatalog::default();
    let mut consumer = catalog.consumer(GeoSources::Both);

    let error = next_error(&mut consumer).await;

    assert_eq!(error.error.error, IggyError::InvalidConfiguration);
    assert_eq!(catalog.opens_of(STREAM), 0);
}

#[tokio::test]
async fn given_a_standalone_consumer_when_reading_its_shadows_should_refuse_it_before_sending_anything()
 {
    let builder = IggyConsumerBuilder::new(
        IggyRwLock::new(ClientWrapper::Tcp(TcpClient::default())),
        GROUP.to_owned(),
        Consumer::new(Identifier::named(GROUP).unwrap()),
        Identifier::named(STREAM).unwrap(),
        Identifier::named(TOPIC).unwrap(),
        None,
    );
    let mut consumer = builder.with_shadows().build();

    assert_eq!(consumer.init().await, Err(IggyError::InvalidConfiguration));
}

#[tokio::test]
async fn given_shadows_only_with_partitions_for_a_group_when_initializing_should_refuse_it_before_sending_anything()
 {
    let builder = IggyConsumerBuilder::new(
        IggyRwLock::new(ClientWrapper::Tcp(TcpClient::default())),
        GROUP.to_owned(),
        Consumer::group(Identifier::named(GROUP).unwrap()),
        Identifier::named(STREAM).unwrap(),
        Identifier::named(TOPIC).unwrap(),
        None,
    )
    .partitions([1]);
    let mut consumer = builder
        .with_shadows()
        .sources(GeoSources::ShadowsOnly)
        .build();

    let initialized = timeout(TEST_BUDGET, consumer.init())
        .await
        .expect("init in time");

    assert_eq!(initialized, Err(IggyError::InvalidCommand));
}

#[test]
fn given_a_geo_consumer_should_move_between_tasks() {
    fn assert_send<T: Send>() {}
    assert_send::<IggyGeoConsumer>();
}

async fn next_message(consumer: &mut IggyGeoConsumer) -> GeoReceivedMessage {
    timeout(TEST_BUDGET, consumer.next())
        .await
        .expect("an item in time")
        .expect("the consumer goes on")
        .expect("a message")
}

async fn next_error(consumer: &mut IggyGeoConsumer) -> GeoReadError {
    let item = timeout(TEST_BUDGET, consumer.next())
        .await
        .expect("an item in time")
        .expect("the consumer goes on");
    match item {
        Ok(message) => panic!("a message in place of an error: {message:?}"),
        Err(error) => error,
    }
}

/// Polls `call` a few times, none of which ends it, then drops it, as a caller that gives up
/// does.
fn cancel_after_polls(call: impl Future) {
    let mut call = std::pin::pin!(call);
    let mut cx = Context::from_waker(noop_waker_ref());
    for _ in 0..3 {
        assert!(
            call.as_mut().poll(&mut cx).is_pending(),
            "the call ended before the cancel"
        );
    }
}

/// The next item, which must be a message that is there already.
fn ready_message(consumer: &mut IggyGeoConsumer) -> GeoReceivedMessage {
    consumer
        .next()
        .now_or_never()
        .expect("a message at once")
        .expect("the consumer goes on")
        .expect("a message")
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl WakeCounter {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl ArcWake for WakeCounter {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// The streams that a fake catalog serves. The stream of the consumer always holds the topic.
#[derive(Default)]
struct World {
    shadows: Vec<ShadowStream>,
    /// The shadow streams that hold the topic.
    holding: BTreeSet<String>,
    /// The error that opening each stream returns.
    refusals: HashMap<String, IggyError>,
    listing_error: Option<IggyError>,
    listings: usize,
    /// The opens tried of each stream, refused ones too.
    attempts: HashMap<String, usize>,
    /// Each consumer opened, with its stream, oldest first.
    opened: Vec<(String, Arc<LogHandle>)>,
    /// Messages for a stream that its consumer takes once it opens.
    queued: HashMap<String, Vec<(u32, Range<u64>)>>,
    /// The streams whose opens wait for a gate.
    open_gates: HashMap<String, Arc<Gate>>,
    listing_gate: Option<Arc<Gate>>,
}

/// Holds back each call that waits on it until it opens.
struct Gate(watch::Sender<bool>);

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self(watch::channel(false).0))
    }

    fn open(&self) {
        self.0.send_replace(true);
    }

    async fn pass(self: Arc<Self>) {
        let mut opened = self.0.subscribe();
        let passed = opened.wait_for(|open| *open).await.is_ok();
        assert!(passed, "the gate outlives each call that waits on it");
    }
}

#[derive(Clone, Default)]
struct FakeCatalog {
    world: Arc<Mutex<World>>,
}

impl FakeCatalog {
    fn world(&self) -> std::sync::MutexGuard<'_, World> {
        self.world.lock().unwrap()
    }

    fn consumer(&self, sources: GeoSources) -> IggyGeoConsumer {
        IggyGeoConsumer::new(
            GROUP.to_owned(),
            Identifier::named(STREAM).unwrap(),
            Identifier::named(TOPIC).unwrap(),
            sources,
            INTERVAL,
            Arc::new(self.clone()),
        )
    }

    async fn initialized(&self, sources: GeoSources) -> IggyGeoConsumer {
        let mut consumer = self.consumer(sources);
        timeout(TEST_BUDGET, consumer.init())
            .await
            .expect("init in time")
            .unwrap();
        consumer
    }

    /// Holds each open of `stream` until the gate opens.
    fn hold_opens(&self, stream: &str) -> Arc<Gate> {
        let gate = Gate::new();
        self.world()
            .open_gates
            .insert(stream.to_owned(), Arc::clone(&gate));
        gate
    }

    /// Holds each listing of the streams until the gate opens.
    fn hold_listings(&self) -> Arc<Gate> {
        let gate = Gate::new();
        self.world().listing_gate = Some(Arc::clone(&gate));
        gate
    }

    /// Lists the shadow stream of `origin`, which holds the topic, and returns its name.
    fn add_shadow(&self, origin: &str) -> String {
        let name = format!("__shadow_{origin}_{STREAM}");
        let mut world = self.world();
        world.shadows.push(ShadowStream {
            name: name.clone(),
            origin: origin.to_owned(),
        });
        world.holding.insert(name.clone());
        name
    }

    fn remove_shadow(&self, name: &str) {
        let mut world = self.world();
        world.shadows.retain(|shadow| shadow.name != name);
        world.holding.remove(name);
    }

    fn queue(&self, stream: &str, partition: u32, offsets: Range<u64>) {
        self.world()
            .queued
            .entry(stream.to_owned())
            .or_default()
            .push((partition, offsets));
    }

    /// The consumer of `stream` opened last.
    fn log(&self, stream: &str) -> Arc<LogHandle> {
        self.world()
            .opened
            .iter()
            .rev()
            .find(|(opened, _)| opened == stream)
            .map(|(_, handle)| Arc::clone(handle))
            .unwrap_or_else(|| panic!("no consumer of {stream} opened"))
    }

    fn opens_of(&self, stream: &str) -> usize {
        self.world()
            .opened
            .iter()
            .filter(|(opened, _)| opened == stream)
            .count()
    }

    fn attempts_of(&self, stream: &str) -> usize {
        self.world()
            .attempts
            .get(stream)
            .copied()
            .unwrap_or_default()
    }

    fn open(&self, stream: &str) -> BoxFuture<'static, Result<Box<dyn LogConsumer>, IggyError>> {
        let gate = {
            let mut world = self.world();
            *world.attempts.entry(stream.to_owned()).or_default() += 1;
            world.open_gates.get(stream).cloned()
        };
        match gate {
            None => ready(self.opened(stream)).boxed(),
            Some(gate) => {
                let catalog = self.clone();
                let stream = stream.to_owned();
                async move {
                    gate.pass().await;
                    catalog.opened(&stream)
                }
                .boxed()
            }
        }
    }

    fn opened(&self, stream: &str) -> Result<Box<dyn LogConsumer>, IggyError> {
        let mut world = self.world();
        if let Some(error) = world.refusals.get(stream) {
            return Err(error.clone());
        }
        let (sender, receiver) = unbounded();
        let handle = Arc::new(LogHandle {
            reader: NEXT_READER.fetch_add(1, Ordering::Relaxed),
            sender,
            commits: Mutex::default(),
            marks: Mutex::default(),
            seeks: Mutex::default(),
            polls: AtomicUsize::new(0),
            counted: AtomicUsize::new(0),
            shut_down: AtomicBool::new(false),
        });
        for (partition, offsets) in world.queued.remove(stream).unwrap_or_default() {
            handle.send(partition, offsets);
        }
        world.opened.push((stream.to_owned(), Arc::clone(&handle)));
        Ok(Box::new(FakeLog { handle, receiver }))
    }

    fn listing(&self) -> Result<Vec<ShadowStream>, IggyError> {
        let world = self.world();
        match &world.listing_error {
            Some(error) => Err(error.clone()),
            None => Ok(world.shadows.clone()),
        }
    }
}

impl LogCatalog for FakeCatalog {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }

    fn open_local(&self) -> BoxFuture<'static, Result<Box<dyn LogConsumer>, IggyError>> {
        self.open(STREAM)
    }

    fn names(&self) -> BoxFuture<'static, Result<TopicNames, IggyError>> {
        ready(Ok(TopicNames {
            stream: STREAM.to_owned(),
            topic: TOPIC.to_owned(),
        }))
        .boxed()
    }

    fn shadow_streams(&self) -> BoxFuture<'static, Result<Vec<ShadowStream>, IggyError>> {
        let gate = {
            let mut world = self.world();
            world.listings += 1;
            world.listing_gate.clone()
        };
        match gate {
            None => ready(self.listing()).boxed(),
            Some(gate) => {
                let catalog = self.clone();
                async move {
                    gate.pass().await;
                    catalog.listing()
                }
                .boxed()
            }
        }
    }

    fn holds_topic(
        &self,
        stream: &str,
        topic: &str,
    ) -> BoxFuture<'static, Result<bool, IggyError>> {
        let holds = topic == TOPIC && self.world().holding.contains(stream);
        ready(Ok(holds)).boxed()
    }

    fn open_shadow(
        &self,
        stream: &str,
        _topic: &str,
    ) -> BoxFuture<'static, Result<Box<dyn LogConsumer>, IggyError>> {
        self.open(stream)
    }
}

/// What the geo consumer did with one fake consumer, and the sender of its messages.
struct LogHandle {
    reader: u64,
    sender: UnboundedSender<Result<ReceivedMessage, ReadError>>,
    commits: Mutex<Vec<Checkpoint>>,
    marks: Mutex<Vec<Checkpoint>>,
    seeks: Mutex<Vec<(u32, PollingStrategy)>>,
    polls: AtomicUsize,
    /// The calls of `count_uncounted`.
    counted: AtomicUsize,
    shut_down: AtomicBool,
}

impl LogHandle {
    /// Hands over one message at each of `offsets` of `partition`.
    fn send(&self, partition: u32, offsets: Range<u64>) {
        for offset in offsets {
            let mut message = IggyMessage::builder()
                .payload(Bytes::from(format!("{partition}:{offset}")))
                .build()
                .unwrap();
            message.header.offset = offset;
            let stamp = ReaderStamp {
                reader: self.reader,
                generation: 1,
            };
            let checkpoint =
                Checkpoint::stamped(partition, offset, PartitionContext::default(), Some(stamp));
            self.sender
                .unbounded_send(Ok(ReceivedMessage::yielded(message, offset, checkpoint)))
                .unwrap();
        }
    }

    fn fail(&self, error: ReadError) {
        self.sender.unbounded_send(Err(error)).unwrap();
    }

    fn commits(&self) -> Vec<Checkpoint> {
        self.commits.lock().unwrap().clone()
    }

    fn marks(&self) -> Vec<Checkpoint> {
        self.marks.lock().unwrap().clone()
    }

    fn seeks(&self) -> Vec<(u32, PollingStrategy)> {
        self.seeks.lock().unwrap().clone()
    }

    fn polls(&self) -> usize {
        self.polls.load(Ordering::SeqCst)
    }

    fn counted(&self) -> usize {
        self.counted.load(Ordering::SeqCst)
    }

    fn is_shut_down(&self) -> bool {
        self.shut_down.load(Ordering::SeqCst)
    }
}

struct FakeLog {
    handle: Arc<LogHandle>,
    receiver: UnboundedReceiver<Result<ReceivedMessage, ReadError>>,
}

impl Stream for FakeLog {
    type Item = Result<ReceivedMessage, ReadError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.handle.polls.fetch_add(1, Ordering::SeqCst);
        self.receiver.poll_next_unpin(cx)
    }
}

impl LogConsumer for FakeLog {
    fn reader_id(&self) -> u64 {
        self.handle.reader
    }

    fn commit(&self, checkpoint: Checkpoint) -> BoxFuture<'static, Result<(), IggyError>> {
        self.handle.commits.lock().unwrap().push(checkpoint);
        ready(Ok(())).boxed()
    }

    fn mark(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        self.handle.marks.lock().unwrap().push(checkpoint);
        Ok(())
    }

    fn seek(
        &mut self,
        partition: u32,
        start: PollingStrategy,
    ) -> BoxFuture<'_, Result<(), IggyError>> {
        self.handle.seeks.lock().unwrap().push((partition, start));
        ready(Ok(())).boxed()
    }

    fn count_uncounted(&mut self) {
        self.handle.counted.fetch_add(1, Ordering::SeqCst);
    }

    fn shutdown(&mut self) -> BoxFuture<'_, Result<(), IggyError>> {
        self.handle.shut_down.store(true, Ordering::SeqCst);
        self.receiver.close();
        ready(Ok(())).boxed()
    }
}
