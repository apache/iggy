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

use super::{BenchmarkCache, Result};
use crate::dedicated_thread::{DedicatedThread, ThreadStopped};
use crate::error::IggyBenchDashboardServerError;
use actix_web::web::Bytes;
use bench_dashboard_shared::series::{BenchmarkSeries, SeriesMetric, SeriesScope};
use bench_report::report::BenchmarkReport;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use uuid::Uuid;

/// A run's group series take about 100 KB of JSON per group, and the actor series of a 64-actor run
/// about 6 MB, so this holds the group series of a few hundred runs.
pub const SERIES_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
const SERIES_BUILD_THREAD: &str = "series-build";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SeriesKey {
    pub uuid: Uuid,
    pub metric: SeriesMetric,
    pub scope: SeriesScope,
}

/// Serialized series responses. An insert past the byte bound evicts the least recently used
/// entries. A reload of the results directory clears the cache and starts a new generation,
/// so a response read from the files before the reload never enters the cache after it.
#[derive(Debug)]
pub struct SeriesCache {
    max_bytes: usize,
    state: Mutex<SeriesCacheState>,
}

#[derive(Debug, Default)]
struct SeriesCacheState {
    entries: HashMap<SeriesKey, CachedSeries>,
    bytes: usize,
    generation: u64,
    /// Ticks on every use, to find the least recently used entry.
    clock: u64,
}

#[derive(Debug)]
struct CachedSeries {
    json: Bytes,
    last_used: u64,
}

impl SeriesCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            state: Mutex::default(),
        }
    }

    pub fn get(&self, key: &SeriesKey) -> Option<Bytes> {
        let mut state = self.lock();
        state.clock += 1;
        let now = state.clock;
        let entry = state.entries.get_mut(key)?;
        entry.last_used = now;
        Some(entry.json.clone())
    }

    /// Take it before the report is read, and pass it to `insert`.
    pub fn generation(&self) -> u64 {
        self.lock().generation
    }

    /// Skips the response when a reload came after `generation`, or when the response alone is over
    /// the bound.
    pub fn insert(&self, key: SeriesKey, json: Bytes, generation: u64) {
        let mut state = self.lock();
        if generation != state.generation || json.len() > self.max_bytes {
            return;
        }
        state.remove(&key);
        while state.bytes + json.len() > self.max_bytes
            && let Some(oldest) = state.least_recently_used()
        {
            state.remove(&oldest);
        }
        state.clock += 1;
        let last_used = state.clock;
        state.bytes += json.len();
        state.entries.insert(key, CachedSeries { json, last_used });
    }

    pub fn clear(&self) {
        let mut state = self.lock();
        state.entries.clear();
        state.bytes = 0;
        state.generation += 1;
    }

    fn lock(&self) -> MutexGuard<'_, SeriesCacheState> {
        // No critical section can panic halfway, so a poisoned lock still holds a whole cache.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl SeriesCacheState {
    fn remove(&mut self, key: &SeriesKey) {
        if let Some(removed) = self.entries.remove(key) {
            self.bytes -= removed.json.len();
        }
    }

    fn least_recently_used(&self) -> Option<SeriesKey> {
        self.entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| *key)
    }
}

/// Builds series responses one at a time on a dedicated thread. A build holds a whole report, up to
/// 90 MB of JSON and the parsed report, and every thread that frees one keeps that memory in its
/// own malloc arena. So a single thread bounds the memory that builds use and leave behind.
pub struct SeriesBuilder {
    thread: DedicatedThread<BuildJob, Result<Bytes>>,
}

struct BuildJob {
    report: PathBuf,
    key: SeriesKey,
    generation: u64,
}

impl SeriesBuilder {
    /// Every build puts its response into the series cache of `cache`.
    pub fn start(cache: Arc<BenchmarkCache>) -> std::io::Result<Self> {
        let thread = DedicatedThread::start(SERIES_BUILD_THREAD, move |job: BuildJob| {
            job.run(cache.series())
        })?;
        Ok(Self { thread })
    }

    /// Take `generation` from the series cache before the report path.
    pub async fn build(&self, report: PathBuf, key: SeriesKey, generation: u64) -> Result<Bytes> {
        self.thread
            .run(BuildJob {
                report,
                key,
                generation,
            })
            .await
            .map_err(|ThreadStopped| {
                IggyBenchDashboardServerError::InternalError(
                    "The series build thread has stopped".to_owned(),
                )
            })?
    }
}

impl BuildJob {
    fn run(self, cache: &SeriesCache) -> Result<Bytes> {
        // Concurrent misses queue one job each. The first one builds, the rest find its response.
        if let Some(json) = cache.get(&self.key) {
            return Ok(json);
        }
        // The pages ask for both metrics of a scope together, and the parse costs far more than a
        // series, so one parse builds both. The sibling goes in first, so it never evicts the
        // requested series.
        let sibling = SeriesKey {
            metric: sibling_metric(self.key.metric),
            ..self.key
        };
        let [sibling_json, json] = read_series(
            &self.report,
            self.key.scope,
            [sibling.metric, self.key.metric],
        )?;
        cache.insert(sibling, sibling_json, self.generation);
        cache.insert(self.key, json.clone(), self.generation);
        Ok(json)
    }
}

fn sibling_metric(metric: SeriesMetric) -> SeriesMetric {
    match metric {
        SeriesMetric::Latency => SeriesMetric::Throughput,
        SeriesMetric::Throughput => SeriesMetric::Latency,
    }
}

/// Parses the report once, and returns the series of each metric in the order of `metrics`.
fn read_series(path: &Path, scope: SeriesScope, metrics: [SeriesMetric; 2]) -> Result<[Bytes; 2]> {
    let report = read_report(path)?;
    let [first, second] = metrics.map(|metric| BenchmarkSeries::new(&report, metric, scope));
    drop(report);
    Ok([serialize(&first, path)?, serialize(&second, path)?])
}

fn serialize(series: &BenchmarkSeries, path: &Path) -> Result<Bytes> {
    serde_json::to_vec(series)
        // Without spare capacity, so the cache bound counts every byte that it holds.
        .map(|json| Bytes::from(json.into_boxed_slice()))
        .map_err(|e| {
            IggyBenchDashboardServerError::InternalError(format!(
                "Failed to serialize the series of '{}': {e}",
                path.display()
            ))
        })
}

pub fn read_report(path: &Path) -> Result<BenchmarkReport> {
    let raw = std::fs::read(path).map_err(|e| {
        IggyBenchDashboardServerError::InternalError(format!(
            "Failed to read report file '{}': {e}",
            path.display()
        ))
    })?;
    serde_json::from_slice(&raw).map_err(|e| {
        IggyBenchDashboardServerError::InvalidJson(format!(
            "Failed to parse report file '{}': {e}",
            path.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENTRY_BYTES: usize = 100;
    const FULL_CACHE_RUNS: [u128; 3] = [1, 2, 3];
    const MAX_BYTES: usize = FULL_CACHE_RUNS.len() * ENTRY_BYTES;
    const NEW_RUN: u128 = 4;
    /// Never read, because the job finds the cached series first.
    const MISSING_REPORT: &str = "missing/report.json";
    /// 8 producers, 8 consumers, and the groups of producers, consumers, and both.
    const FIXTURE_REPORT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../fixtures/results/old-0.4.202-pinned-producer-and-consumer/report.json"
    );

    fn key(run: u128) -> SeriesKey {
        SeriesKey {
            uuid: Uuid::from_u128(run),
            metric: SeriesMetric::Latency,
            scope: SeriesScope::Group,
        }
    }

    fn fixture_key(metric: SeriesMetric, scope: SeriesScope) -> SeriesKey {
        SeriesKey {
            uuid: Uuid::from_u128(NEW_RUN),
            metric,
            scope,
        }
    }

    fn fixture_job(key: SeriesKey, generation: u64) -> BuildJob {
        BuildJob {
            report: PathBuf::from(FIXTURE_REPORT),
            key,
            generation,
        }
    }

    fn metric_of(json: &Bytes) -> SeriesMetric {
        serde_json::from_slice::<BenchmarkSeries>(json)
            .expect("a cached response is a series")
            .metric
    }

    fn json(len: usize) -> Bytes {
        Bytes::from(vec![b'0'; len])
    }

    fn full_cache() -> SeriesCache {
        let cache = SeriesCache::new(MAX_BYTES);
        for run in FULL_CACHE_RUNS {
            cache.insert(key(run), json(ENTRY_BYTES), cache.generation());
        }
        cache
    }

    fn cached_runs(cache: &SeriesCache) -> Vec<u128> {
        let mut runs: Vec<_> = cache
            .lock()
            .entries
            .keys()
            .map(|key| key.uuid.as_u128())
            .collect();
        runs.sort_unstable();
        runs
    }

    #[test]
    fn given_full_cache_when_inserting_should_evict_least_recently_used_entry() {
        let cache = full_cache();
        assert_eq!(cache.get(&key(1)), Some(json(ENTRY_BYTES)));

        cache.insert(key(NEW_RUN), json(ENTRY_BYTES), cache.generation());

        assert_eq!(cached_runs(&cache), [1, 3, NEW_RUN]);
        assert_eq!(cache.lock().bytes, MAX_BYTES);
    }

    #[test]
    fn given_full_cache_when_inserting_larger_entry_should_evict_until_it_fits() {
        let cache = full_cache();

        cache.insert(key(NEW_RUN), json(2 * ENTRY_BYTES), cache.generation());

        assert_eq!(cached_runs(&cache), [3, NEW_RUN]);
        assert_eq!(cache.lock().bytes, MAX_BYTES);
    }

    #[test]
    fn given_entry_above_bound_when_inserting_should_skip_it_and_keep_the_rest() {
        let cache = full_cache();

        cache.insert(key(NEW_RUN), json(MAX_BYTES + 1), cache.generation());

        assert_eq!(cached_runs(&cache), FULL_CACHE_RUNS);
    }

    #[test]
    fn given_cached_key_when_inserting_again_should_replace_entry_and_its_bytes() {
        let cache = full_cache();

        cache.insert(key(2), json(ENTRY_BYTES / 2), cache.generation());

        assert_eq!(cached_runs(&cache), FULL_CACHE_RUNS);
        assert_eq!(cache.get(&key(2)), Some(json(ENTRY_BYTES / 2)));
        assert_eq!(cache.lock().bytes, MAX_BYTES - ENTRY_BYTES / 2);
    }

    #[test]
    fn given_reload_when_clearing_should_drop_entries_and_refuse_older_builds() {
        let cache = full_cache();
        let before_reload = cache.generation();

        cache.clear();
        cache.insert(key(NEW_RUN), json(ENTRY_BYTES), before_reload);

        assert!(cached_runs(&cache).is_empty());
        assert_eq!(cache.lock().bytes, 0);
        cache.insert(key(NEW_RUN), json(ENTRY_BYTES), cache.generation());
        assert_eq!(cached_runs(&cache), [NEW_RUN]);
    }

    #[test]
    fn given_series_cached_after_job_was_queued_when_job_runs_should_return_it_without_build() {
        let cache = SeriesCache::new(MAX_BYTES);
        cache.insert(key(NEW_RUN), json(ENTRY_BYTES), cache.generation());

        let built = BuildJob {
            report: PathBuf::from(MISSING_REPORT),
            key: key(NEW_RUN),
            generation: cache.generation(),
        }
        .run(&cache);

        assert_eq!(
            built.expect("the job returns the cached series"),
            json(ENTRY_BYTES)
        );
    }

    #[test]
    fn given_group_miss_when_job_runs_should_cache_both_group_metrics_only() {
        let cache = SeriesCache::new(SERIES_CACHE_MAX_BYTES);
        let latency = fixture_key(SeriesMetric::Latency, SeriesScope::Group);
        let throughput = fixture_key(SeriesMetric::Throughput, SeriesScope::Group);

        let built = fixture_job(latency, cache.generation())
            .run(&cache)
            .expect("the job builds the fixture series");

        assert_eq!(metric_of(&built), SeriesMetric::Latency);
        assert_eq!(cache.get(&latency), Some(built));
        let sibling = cache
            .get(&throughput)
            .expect("the job caches the other metric of the scope");
        assert_eq!(metric_of(&sibling), SeriesMetric::Throughput);
        for metric in [SeriesMetric::Latency, SeriesMetric::Throughput] {
            assert!(
                cache
                    .get(&fixture_key(metric, SeriesScope::Actors))
                    .is_none(),
                "{metric:?}"
            );
        }
    }

    #[test]
    fn given_sibling_built_when_sibling_job_runs_should_answer_from_cache() {
        let cache = SeriesCache::new(SERIES_CACHE_MAX_BYTES);
        let latency = fixture_key(SeriesMetric::Latency, SeriesScope::Actors);
        let throughput = fixture_key(SeriesMetric::Throughput, SeriesScope::Actors);
        fixture_job(latency, cache.generation())
            .run(&cache)
            .expect("the job builds the fixture series");

        let answered = BuildJob {
            report: PathBuf::from(MISSING_REPORT),
            key: throughput,
            generation: cache.generation(),
        }
        .run(&cache)
        .expect("the job answers from the cache without reading the report");

        assert_eq!(metric_of(&answered), SeriesMetric::Throughput);
        assert_eq!(cache.get(&throughput), Some(answered));
    }

    #[test]
    fn given_reload_during_build_when_job_runs_should_cache_neither_metric() {
        let cache = SeriesCache::new(SERIES_CACHE_MAX_BYTES);
        let latency = fixture_key(SeriesMetric::Latency, SeriesScope::Group);
        let throughput = fixture_key(SeriesMetric::Throughput, SeriesScope::Group);
        let before_reload = cache.generation();

        cache.clear();
        let built = fixture_job(latency, before_reload)
            .run(&cache)
            .expect("the job still answers its caller");

        assert_eq!(metric_of(&built), SeriesMetric::Latency);
        assert!(cache.get(&latency).is_none());
        assert!(cache.get(&throughput).is_none());
        assert_eq!(cache.lock().bytes, 0);
    }
}
