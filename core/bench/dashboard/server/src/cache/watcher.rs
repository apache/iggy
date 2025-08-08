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

use super::BenchmarkCache;
use crate::error::IggyBenchDashboardServerError;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::time::sleep;
use tracing::{error, info};

pub struct CacheWatcher {
    _watcher: RecommendedWatcher,
}

impl CacheWatcher {
    pub fn new(
        cache: Arc<BenchmarkCache>,
        results_dir: PathBuf,
    ) -> Result<Self, IggyBenchDashboardServerError> {
        let cache_clone = Arc::clone(&cache);
        let runtime_handle = Handle::current();

        let mut watcher = notify::recommended_watcher(move |res: Result<Event, _>| match res {
            Ok(event) => {
                if matches!(
                    event.kind,
                    EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                ) {
                    let cache = Arc::clone(&cache_clone);
                    runtime_handle.spawn(async move {
                        cache.schedule_reload().await;
                    });
                }
            }
            Err(e) => error!("Watch error: {:?}", e),
        })
        .map_err(|e| IggyBenchDashboardServerError::InvalidPath(e.to_string()))?;

        watcher
            .watch(&results_dir, RecursiveMode::Recursive)
            .map_err(|e| IggyBenchDashboardServerError::InvalidPath(e.to_string()))?;

        Ok(Self { _watcher: watcher })
    }
}

impl BenchmarkCache {
    pub async fn schedule_reload(self: Arc<Self>) {
        let mut last_reload = self.last_reload_request.lock().await;

        // Skip if reloaded recently
        if let Some(instant) = *last_reload
            && instant.elapsed() < Duration::from_secs(10)
        {
            return;
        }

        *last_reload = Some(Instant::now());
        drop(last_reload);

        sleep(Duration::from_secs(5)).await;
        info!("Reloading cache...");

        self.clear();
        if let Err(e) = self.load().await {
            error!("Failed to reload cache: {}", e);
        }
    }
}
