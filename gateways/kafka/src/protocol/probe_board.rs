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

use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::time::{Instant, timeout_at};

use crate::bridge::TopicProbe;

/// A topic's probe, or the Kafka code the probe failed with.
pub type TopicProbed = Result<Arc<TopicProbe>, i16>;

/// A probe, and when its `get_topic` started.
#[derive(Debug)]
pub struct Snapshot {
    pub started: Instant,
    pub probe: TopicProbed,
}

/// The newest probe of each Kafka topic, shared by every Fetch.
///
/// One refresh runs at a time per topic, in its own task, so a Fetch that gives up leaves no call
/// behind in the SDK queue.
#[derive(Default)]
pub struct ProbeBoard {
    topics: Mutex<HashMap<String, Arc<Topic>>>,
}

#[derive(Default)]
struct Topic {
    newest: watch::Sender<Option<Arc<Snapshot>>>,
    /// Held by the refresh that runs.
    refresh: Arc<AsyncMutex<()>>,
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
                    let _running = running;
                    let started = Instant::now();
                    let probe = pending.await;
                    topic
                        .newest
                        .send_replace(Some(Arc::new(Snapshot { started, probe })));
                });
            }
            timeout_at(deadline, newest.changed()).await.ok()?.ok()?;
        }
    }

    fn topic(&self, name: &str) -> Arc<Topic> {
        let mut topics = self.topics.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(topic) = topics.get(name) {
            return Arc::clone(topic);
        }
        Arc::clone(topics.entry(name.to_owned()).or_default())
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
}
