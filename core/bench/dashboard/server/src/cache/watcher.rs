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

use crate::error::IggyBenchDashboardServerError;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver};
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tracing::{error, info};

/// Lets a writer finish its burst of writes before a reload reads the files.
const RELOAD_DELAY: Duration = Duration::from_secs(5);

pub struct CacheWatcher {
    _watcher: RecommendedWatcher,
}

impl CacheWatcher {
    /// Watches `results_dir` and calls `reload` on a blocking thread after its files change. The
    /// server starts one watcher per results directory, so a change reloads only the runs of
    /// that directory.
    pub fn new<F>(reload: F, results_dir: PathBuf) -> Result<Self, IggyBenchDashboardServerError>
    where
        F: Fn() -> Result<(), IggyBenchDashboardServerError> + Send + Sync + 'static,
    {
        // The one slot keeps a change that comes during a reload, so one more reload follows.
        // The changes after it find the slot full, and that reload covers them too.
        let (changes, pending_changes) = mpsc::channel(1);

        let mut watcher = notify::recommended_watcher(move |res: Result<Event, _>| match res {
            Ok(event) => {
                if matches!(
                    event.kind,
                    EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                ) && let Err(TrySendError::Closed(())) = changes.try_send(())
                {
                    error!("The cache reload task has stopped, so the cache stays as it is");
                }
            }
            Err(e) => error!("Watch error: {:?}", e),
        })
        .map_err(|e| IggyBenchDashboardServerError::InvalidPath(e.to_string()))?;

        watcher
            .watch(&results_dir, RecursiveMode::Recursive)
            .map_err(|e| IggyBenchDashboardServerError::InvalidPath(e.to_string()))?;

        tokio::spawn(reload_on_change(
            Arc::new(reload),
            pending_changes,
            RELOAD_DELAY,
        ));

        Ok(Self { _watcher: watcher })
    }
}

/// Runs one reload at a time, so two loads never race to swap in their runs.
async fn reload_on_change<F>(reload: Arc<F>, mut pending_changes: Receiver<()>, delay: Duration)
where
    F: Fn() -> Result<(), IggyBenchDashboardServerError> + Send + Sync + 'static,
{
    while pending_changes.recv().await.is_some() {
        sleep(delay).await;
        // The load below reads after every change so far. A change from here on refills the slot.
        let _ = pending_changes.try_recv();
        info!("Reloading cache...");

        let reload = Arc::clone(&reload);
        match spawn_blocking(move || reload()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("Failed to reload cache: {}", e),
            Err(e) => error!("Cache reload task failed: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::BenchmarkCache;
    use crate::config;
    use crate::omb::OmbCache;
    use crate::omb::test_fixtures::{NO_CONSUMERS, RC1_DEFAULT};
    use std::path::Path;
    use std::time::Instant;
    use uuid::Uuid;

    const FIXTURE_RESULTS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../fixtures/results");
    const SHIPPED_CONFIG: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../dashboard.toml");
    const REPORT_FILE: &str = "report.json";
    const FIRST_RUN_DIR: &str = "old-0.4.202-pinned-producer-and-consumer";
    const FIRST_RUN: &str = "6b1e288a-9155-4da3-8d3f-ec4d929de3d3";
    const SECOND_RUN_DIR: &str = "phase1a-pinned-producer-2p-20MB";
    const SECOND_RUN: &str = "b5c560ad-1ad2-49f0-88dd-c29db75126aa";
    const RELOAD_TIMEOUT: Duration = Duration::from_secs(10);
    const POLL_INTERVAL: Duration = Duration::from_millis(10);

    fn copy_fixture_run(run_dir: &str, results_dir: &Path) {
        let target = results_dir.join(run_dir);
        std::fs::create_dir(&target).expect("the run directory is created");
        std::fs::copy(
            Path::new(FIXTURE_RESULTS_DIR)
                .join(run_dir)
                .join(REPORT_FILE),
            target.join(REPORT_FILE),
        )
        .expect("the fixture report is copied");
    }

    fn listed_runs(cache: &BenchmarkCache) -> Vec<Uuid> {
        let mut runs: Vec<_> = cache
            .get_summaries()
            .iter()
            .map(|summary| summary.uuid)
            .collect();
        runs.sort_unstable();
        runs
    }

    fn run(uuid: &str) -> Uuid {
        uuid.parse().expect("the fixture uuid is valid")
    }

    #[tokio::test]
    async fn given_run_written_right_after_reload_when_changes_stop_should_load_it() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let config = config::load(Path::new(SHIPPED_CONFIG)).expect("the shipped config is valid");
        let cache = Arc::new(BenchmarkCache::new(
            results_dir.path().to_path_buf(),
            config.thresholds,
        ));
        let (changes, pending_changes) = mpsc::channel(1);
        let reload = {
            let cache = Arc::clone(&cache);
            Arc::new(move || cache.load())
        };
        let reloads = tokio::spawn(reload_on_change(reload, pending_changes, Duration::ZERO));

        copy_fixture_run(FIRST_RUN_DIR, results_dir.path());
        changes.send(()).await.expect("the reload task runs");
        let deadline = Instant::now() + RELOAD_TIMEOUT;
        while listed_runs(&cache).is_empty() {
            assert!(Instant::now() < deadline, "the first run never loaded");
            sleep(POLL_INTERVAL).await;
        }
        copy_fixture_run(SECOND_RUN_DIR, results_dir.path());
        changes.send(()).await.expect("the reload task runs");
        drop(changes);
        reloads
            .await
            .expect("the reload task ends after the last change");

        let mut expected = vec![run(FIRST_RUN), run(SECOND_RUN)];
        expected.sort_unstable();
        assert_eq!(listed_runs(&cache), expected);
    }

    #[tokio::test]
    async fn given_omb_run_written_after_load_when_changes_stop_should_reload_omb_runs() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        RC1_DEFAULT.copy_into(results_dir.path());
        let cache = Arc::new(OmbCache::new(results_dir.path().to_path_buf()));
        cache.load().expect("the OMB results directory is readable");
        let (changes, pending_changes) = mpsc::channel(1);
        let reload = {
            let cache = Arc::clone(&cache);
            Arc::new(move || cache.load())
        };
        let reloads = tokio::spawn(reload_on_change(reload, pending_changes, Duration::ZERO));

        NO_CONSUMERS.copy_into(results_dir.path());
        changes.send(()).await.expect("the reload task runs");
        drop(changes);
        reloads
            .await
            .expect("the reload task ends after the last change");

        let runs: Vec<_> = cache
            .run_list()
            .runs
            .iter()
            .map(|summary| summary.id)
            .collect();
        assert_eq!(runs, [RC1_DEFAULT.id(), NO_CONSUMERS.id()]);
    }
}
