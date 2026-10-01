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

//! The newest probe of each Kafka topic, shared by every Fetch.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::time::{Instant, timeout_at};

use crate::bridge::iggy_bridge::TopicProbe;

/// How long a probe counts as new, and how often a waiting Fetch asks for one.
pub const PROBE_INTERVAL: Duration = Duration::from_millis(100);

/// A topic's probe, or the Kafka code the probe failed with.
pub type TopicProbed = Result<Arc<TopicProbe>, i16>;

/// The board sweeps no map smaller than this.
const MIN_SWEEP: usize = 1024;

/// The oldest start a shared probe can have and still count as new at `now`.
pub fn recent(now: Instant) -> Instant {
    now.checked_sub(PROBE_INTERVAL).unwrap_or(now)
}

/// A probe, and when its `get_topic` started.
#[derive(Debug)]
pub struct Snapshot {
    pub started: Instant,
    pub probe: TopicProbed,
    /// When `probe` failed, the newest probe of the topic that did not.
    pub last_good: Option<Good>,
}

/// A probe that succeeded, and when its `get_topic` started.
#[derive(Debug, Clone)]
pub struct Good {
    pub started: Instant,
    pub probe: Arc<TopicProbe>,
}

impl Snapshot {
    /// The newest probe of the topic that succeeded: this one, or the one before it failed.
    pub fn good(&self) -> Option<Good> {
        self.probe.as_ref().map_or_else(
            |_| self.last_good.clone(),
            |probe| {
                Some(Good {
                    started: self.started,
                    probe: Arc::clone(probe),
                })
            },
        )
    }
}

/// The newest probe of each Kafka topic, shared by every Fetch.
///
/// One refresh runs at a time per topic, in its own task, so a Fetch that gives up leaves no call
/// behind in the SDK queue.
#[derive(Default)]
pub struct ProbeBoard {
    topics: Mutex<Topics>,
}

/// Every topic on the board, and the size at which the next insert sweeps first.
#[derive(Default)]
struct Topics {
    by_name: HashMap<String, Arc<Topic>>,
    sweep_at: usize,
}

impl Topics {
    /// Forgets each topic that no Fetch and no refresh holds, that no waiting Fetch watches for
    /// writes, and whose probe is too old to share. Any client can name topics, so the board must
    /// not keep them all.
    ///
    /// A waiting Fetch holds only its write receiver. Dropping its topic would drop the sender under
    /// it, and the next write would wake nothing.
    fn sweep(&mut self, now: Instant) {
        let since = recent(now);
        self.by_name.retain(|_, topic| {
            Arc::strong_count(topic) > 1
                || topic.written.receiver_count() > 0
                || topic.newer_than(since)
        });
        // Twice what is left, so sweeps cost O(1) per insert.
        self.sweep_at = self.by_name.len().saturating_mul(2).max(MIN_SWEEP);
    }
}

#[derive(Default)]
struct Topic {
    newest: watch::Sender<Option<Arc<Snapshot>>>,
    /// Held by the refresh that runs.
    refresh: Arc<AsyncMutex<()>>,
    /// When the last gateway write ended, so a Fetch that waits can probe at once.
    written: watch::Sender<Option<Instant>>,
}

impl Topic {
    /// Whether its newest probe started at `since` or later.
    fn newer_than(&self, since: Instant) -> bool {
        self.newest
            .borrow()
            .as_ref()
            .is_some_and(|snapshot| snapshot.started >= since)
    }
}

impl ProbeBoard {
    /// A probe of `topic` that started at `since` or later. `None` if none comes by `deadline`.
    ///
    /// Runs `load` in a task when no refresh of `topic` runs.
    pub async fn probe<F, Fut>(
        &self,
        topic: &str,
        since: Instant,
        deadline: Instant,
        load: F,
    ) -> Option<Arc<Snapshot>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = TopicProbed> + Send + 'static,
    {
        let topic = self.topic(topic);
        let mut newest = topic.newest.subscribe();
        let mut load = Some(load);
        loop {
            if let Some(snapshot) = newest
                .borrow_and_update()
                .as_ref()
                .filter(|snapshot| snapshot.started >= since)
            {
                return Some(Arc::clone(snapshot));
            }
            if Instant::now() >= deadline {
                return None;
            }
            if load.is_some()
                && let Ok(running) = Arc::clone(&topic.refresh).try_lock_owned()
                && let Some(load) = load.take()
            {
                let topic = Arc::clone(&topic);
                let pending = load();
                tokio::spawn(async move {
                    let started = Instant::now();
                    let probe = pending.await;
                    // Unlock first. A waiter that this wakes and that wants a newer probe must
                    // find the lock free, or it waits out its deadline.
                    drop(running);
                    let last_good = match &probe {
                        Ok(_) => None,
                        Err(_) => topic
                            .newest
                            .borrow()
                            .as_ref()
                            .and_then(|newest| newest.good()),
                    };
                    topic.newest.send_replace(Some(Arc::new(Snapshot {
                        started,
                        probe,
                        last_good,
                    })));
                });
            }
            timeout_at(deadline, newest.changed()).await.ok()?.ok()?;
        }
    }

    /// The newest probe of `topic`, however old. Loads nothing.
    pub fn newest(&self, topic: &str) -> Option<Arc<Snapshot>> {
        let topics = self.topics.lock().unwrap_or_else(PoisonError::into_inner);
        topics.by_name.get(topic)?.newest.borrow().clone()
    }

    /// Marks a gateway write to `topic` that ended just now. Wakes the Fetches that wait on it.
    pub fn wrote(&self, topic: &str) {
        let topics = self.topics.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(topic) = topics.by_name.get(topic) {
            topic.written.send_replace(Some(Instant::now()));
        }
    }

    /// Changes on each [`Self::wrote`] for `topic` after this call, to the time of that write.
    pub fn writes(&self, topic: &str) -> watch::Receiver<Option<Instant>> {
        self.topic(topic).written.subscribe()
    }

    fn topic(&self, name: &str) -> Arc<Topic> {
        let mut topics = self.topics.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(topic) = topics.by_name.get(name) {
            return Arc::clone(topic);
        }
        if topics.by_name.len() >= topics.sweep_at {
            topics.sweep(Instant::now());
        }
        Arc::clone(topics.by_name.entry(name.to_owned()).or_default())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::Notify;
    use tokio::time::sleep;

    use super::*;

    /// A load that counts itself and ends when `release` says so.
    fn gated(
        loads: &Arc<AtomicUsize>,
        release: &Arc<Notify>,
    ) -> impl FnOnce() -> std::pin::Pin<Box<dyn Future<Output = TopicProbed> + Send>> {
        let (loads, release) = (Arc::clone(loads), Arc::clone(release));
        move || {
            loads.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                release.notified().await;
                Ok(Arc::new(TopicProbe::default()))
            })
        }
    }

    fn later(duration: Duration) -> Instant {
        Instant::now() + duration
    }

    /// A load that ends at once.
    fn done() -> std::future::Ready<TopicProbed> {
        std::future::ready(Ok(Arc::new(TopicProbe::default())))
    }

    fn names(board: &ProbeBoard) -> Vec<String> {
        let mut names: Vec<String> = board
            .topics
            .lock()
            .unwrap()
            .by_name
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    #[tokio::test]
    async fn given_two_fetches_when_both_want_a_new_probe_should_share_one_load() {
        let board = ProbeBoard::default();
        let (loads, release) = (Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new()));
        let (since, deadline) = (Instant::now(), later(Duration::from_secs(5)));

        let (first, second, ()) = tokio::join!(
            board.probe("a", since, deadline, gated(&loads, &release)),
            board.probe("a", since, deadline, gated(&loads, &release)),
            async {
                sleep(Duration::from_millis(20)).await;
                release.notify_one();
            },
        );
        assert!(Arc::ptr_eq(&first.unwrap(), &second.unwrap()));
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn given_a_probe_when_a_newer_one_is_wanted_should_load_again() {
        let board = ProbeBoard::default();
        let (loads, release) = (Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new()));
        let deadline = later(Duration::from_secs(5));
        let probe = |since| {
            release.notify_one();
            board.probe("a", since, deadline, gated(&loads, &release))
        };

        let first = probe(Instant::now()).await.unwrap();
        let again = probe(first.started).await.unwrap();
        assert!(Arc::ptr_eq(&first, &again), "new enough");
        assert_eq!(loads.load(Ordering::SeqCst), 1);

        sleep(Duration::from_millis(1)).await;
        let newer = probe(Instant::now()).await.unwrap();
        assert!(newer.started > first.started);
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn given_a_load_that_hangs_when_the_deadline_passes_should_give_up() {
        let board = ProbeBoard::default();
        let deadline = later(Duration::from_millis(20));
        let hangs = || std::future::pending::<TopicProbed>();
        assert!(
            board
                .probe("a", Instant::now(), deadline, hangs)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn given_old_probes_when_the_board_sweeps_should_keep_only_topics_in_use() {
        let board = ProbeBoard::default();
        let deadline = later(Duration::from_secs(5));
        for name in ["a", "b"] {
            board
                .probe(name, Instant::now(), deadline, done)
                .await
                .unwrap();
        }
        let in_use = board.topic("b");
        board
            .topics
            .lock()
            .unwrap()
            .sweep(later(PROBE_INTERVAL * 2));
        assert_eq!(names(&board), vec!["b"]);
        drop(in_use);
    }

    #[tokio::test]
    async fn given_a_waiting_fetch_when_the_board_sweeps_should_keep_its_topic_so_a_write_wakes_it()
    {
        let board = ProbeBoard::default();
        let deadline = later(Duration::from_secs(5));
        board
            .probe("a", Instant::now(), deadline, done)
            .await
            .unwrap();
        let mut writes = board.writes("a");
        board
            .topics
            .lock()
            .unwrap()
            .sweep(later(PROBE_INTERVAL * 2));
        assert_eq!(names(&board), vec!["a"], "a watched topic stays");

        board.wrote("a");
        tokio::time::timeout(Duration::from_secs(1), writes.changed())
            .await
            .expect("the write wakes the wait")
            .expect("the sender is still on the board");
    }

    #[tokio::test]
    async fn given_a_refresh_that_fails_when_a_good_probe_came_before_should_keep_it() {
        let board = ProbeBoard::default();
        let deadline = later(Duration::from_secs(5));
        let fails = || std::future::ready::<TopicProbed>(Err(6));
        let first = board
            .probe("a", Instant::now(), deadline, done)
            .await
            .unwrap();
        let good = first.good().expect("the first probe succeeded");
        assert_eq!(good.started, first.started);

        for _ in 0..2 {
            sleep(Duration::from_millis(1)).await;
            let failed = board
                .probe("a", Instant::now(), deadline, fails)
                .await
                .unwrap();
            assert_eq!(failed.probe, Err(6));
            let kept = failed.good().expect("the last good probe");
            assert!(
                Arc::ptr_eq(&kept.probe, &good.probe),
                "kept through each failure"
            );
            assert_eq!(kept.started, first.started, "with the time it started");
        }

        sleep(Duration::from_millis(1)).await;
        let recovered = board
            .probe("a", Instant::now(), deadline, done)
            .await
            .unwrap();
        assert!(recovered.last_good.is_none(), "a success needs no fallback");
    }

    #[tokio::test]
    async fn given_many_new_names_when_their_probes_age_should_forget_them() {
        let board = ProbeBoard::default();
        let deadline = later(Duration::from_secs(5));
        for index in 0..MIN_SWEEP {
            let name = format!("t{index}");
            board
                .probe(&name, Instant::now(), deadline, done)
                .await
                .unwrap();
        }
        sleep(PROBE_INTERVAL * 2).await;
        board
            .probe("new", Instant::now(), deadline, done)
            .await
            .unwrap();
        assert_eq!(names(&board), vec!["new"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn given_waiters_for_a_newer_probe_when_a_refresh_ends_should_start_the_next_one() {
        let board = Arc::new(ProbeBoard::default());
        let (loads, release) = (Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new()));
        let running = tokio::spawn({
            let (board, loads, release) =
                (Arc::clone(&board), Arc::clone(&loads), Arc::clone(&release));
            let deadline = later(Duration::from_secs(5));
            async move {
                let load = gated(&loads, &release);
                board.probe("a", Instant::now(), deadline, load).await
            }
        });
        while loads.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(1)).await;
        }
        // The running refresh started before `since`, so its probe is too old for the waiters.
        sleep(Duration::from_millis(20)).await;
        let since = Instant::now();
        let waiters: Vec<_> = (0..4)
            .map(|_| {
                let board = Arc::clone(&board);
                let deadline = later(Duration::from_secs(1));
                tokio::spawn(async move { board.probe("a", since, deadline, done).await })
            })
            .collect();
        sleep(Duration::from_millis(20)).await;
        release.notify_one();
        assert!(running.await.unwrap().is_some());
        for waiter in waiters {
            let snapshot = waiter
                .await
                .unwrap()
                .expect("a newer probe before the deadline");
            assert!(snapshot.started >= since);
        }
    }

    #[tokio::test]
    async fn given_a_probe_when_asked_for_the_newest_should_return_it_without_a_load() {
        let board = ProbeBoard::default();
        assert!(board.newest("a").is_none(), "nothing loads here");
        assert!(names(&board).is_empty(), "and no topic joins the board");
        let since = Instant::now();
        board
            .probe("a", since, later(Duration::from_secs(1)), done)
            .await
            .expect("the load ends at once");
        let newest = board.newest("a").expect("the probe is on the board");
        assert!(newest.started >= since);
    }
}
