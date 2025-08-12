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
use bench_dashboard_shared::BenchmarkReportLight;
use dashmap::{DashMap, DashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{error, info};
use uuid::Uuid;

mod loader;
mod query;
mod storage;
mod watcher;

pub use watcher::CacheWatcher;

pub type Result<T> = std::result::Result<T, IggyBenchDashboardServerError>;
pub type HardwareIdentifier = String;
pub type Gitref = String;

const WORKFLOW_FILE: &str = "gh_workflows.txt";

#[derive(Debug, Clone)]
pub struct BenchmarkCache {
    /// Map benchmark identifier to benchmark light report and path
    benchmarks: DashMap<Uuid, (BenchmarkReportLight, PathBuf)>,

    /// Map hardware identifier to git ref
    hardware_to_gitref: DashMap<HardwareIdentifier, DashSet<Gitref>>,

    /// Map git ref to benchmark directory names
    gitref_to_benchmarks: DashMap<Gitref, DashSet<Uuid>>,

    /// Path to the results directory
    results_dir: PathBuf,

    /// Last reload request time
    last_reload_request: Arc<Mutex<Option<Instant>>>,

    /// Workflows downloaded from GitHub
    gh_workflows: DashSet<u64>,

    /// File containing list of downloaded workflows
    gh_workflows_file: Arc<Mutex<File>>,
}

impl BenchmarkCache {
    pub async fn new(results_dir: PathBuf) -> Self {
        let gh_workflows_path = results_dir
            .join(WORKFLOW_FILE)
            .to_str()
            .unwrap()
            .to_string();

        info!("GH workflows path: {}", gh_workflows_path);

        let gh_workflows_file = OpenOptions::new()
            .write(true)
            .read(true)
            .append(true)
            .create(true)
            .open(&gh_workflows_path)
            .await
            .unwrap_or_else(|_| panic!("Failed to open GH workflows file: {gh_workflows_path}"));

        Self {
            benchmarks: DashMap::new(),
            hardware_to_gitref: DashMap::new(),
            gitref_to_benchmarks: DashMap::new(),
            results_dir,
            last_reload_request: Arc::new(Mutex::new(None)),
            gh_workflows: DashSet::new(),
            gh_workflows_file: Arc::new(Mutex::new(gh_workflows_file)),
        }
    }

    pub fn is_gh_workflow_present(&self, workflow_id: u64) -> bool {
        self.gh_workflows.contains(&workflow_id)
    }

    pub async fn insert_gh_workflow(&self, workflow_id: u64) {
        let mut file = self.gh_workflows_file.lock().await;

        if self.gh_workflows.insert(workflow_id)
            && let Err(e) = file.write_all(format!("{workflow_id}\n").as_bytes()).await
        {
            error!("Failed to write GH workflow ID to file: {}", e);
        }
    }
}
