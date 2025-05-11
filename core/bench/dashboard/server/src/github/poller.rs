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

use crate::cache::BenchmarkCache;
use crate::{error::IggyBenchDashboardServerError, github::client::IggyBenchDashboardGithubClient};
use file_operation::async_copy_dir_files;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{fs, sync::watch, task::JoinHandle, time::sleep};
use tracing::{info, trace};

/// A long-running background task that polls GitHub for workflow runs.
pub struct IggyBenchDashboardGithubPoller {
    shutdown_tx: watch::Sender<bool>,
    join_handle: JoinHandle<()>,
}

impl IggyBenchDashboardGithubPoller {
    pub fn start(
        output_dir: PathBuf,
        branch: String,
        interval_seconds: u64,
        cache: Arc<BenchmarkCache>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let join_handle = tokio::spawn(async move {
            if let Err(e) =
                poll_github_with_shutdown(output_dir, branch, interval_seconds, shutdown_rx, cache)
                    .await
            {
                tracing::error!("Error in poll_github: {}", e);
            }
        });
        Self {
            shutdown_tx,
            join_handle,
        }
    }

    pub async fn shutdown(self) {
        if let Err(err) = self.shutdown_tx.send(true) {
            tracing::error!("Error sending shutdown signal: {:?}", err);
        }
        if let Err(e) = self.join_handle.await {
            tracing::error!("GithubPoller task join error: {:?}", e);
        }
    }
}

/// Poll GitHub for successful workflow runs, download and copy artifacts.
/// This function periodically checks for a shutdown signal.
async fn poll_github_with_shutdown(
    performance_results_dir: PathBuf,
    branch: String,
    interval_seconds: u64,
    mut shutdown_rx: watch::Receiver<bool>,
    cache: Arc<BenchmarkCache>,
) -> Result<(), IggyBenchDashboardServerError> {
    let gh = IggyBenchDashboardGithubClient::new()?;

    info!(
        "Polling GitHub for successful workflow runs on branch {} every {} seconds, copying artifacts to {:?}...",
        branch, interval_seconds, performance_results_dir
    );

    loop {
        if *shutdown_rx.borrow() {
            info!("Shutdown signal received, exiting poll loop");
            break;
        }

        trace!("Woken up...");

        let workflows = gh.get_successful_workflow_runs(&branch).await?;
        if workflows.is_empty() {
            info!("No workflow runs found, sleeping...");
            sleep(Duration::from_secs(interval_seconds)).await;
            continue;
        }

        let tags = gh.get_server_tags().await?;

        for workflow in workflows {
            if cache.is_gh_workflow_present(*workflow.id) {
                trace!(
                    "Workflow ID {} is already in the cache, skipping",
                    workflow.id
                );
                continue;
            }

            let sha1 = &workflow.head_sha;
            let gitref = IggyBenchDashboardGithubClient::get_tag_for_commit(&tags, sha1)
                .map(|tag| tag.name)
                .unwrap_or_else(|| sha1.chars().take(8).collect());

            let workflow_id = workflow.id;
            let artifacts_dir = gh.download_artifact(*workflow_id).await?;
            let temp_dir = artifacts_dir.path().join("performance_results");
            info!(
                "Artifacts for git ref {} (sha1 {}) are in {:?}",
                gitref, sha1, temp_dir
            );

            let mut dir_entries = fs::read_dir(&temp_dir).await?;
            while let Some(entry) = dir_entries.next_entry().await? {
                if *shutdown_rx.borrow() {
                    info!("Shutdown signal received during artifact processing, exiting poll loop");
                    return Ok(());
                }

                let path = entry.path();
                if path.is_dir() {
                    let dir_name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
                        IggyBenchDashboardServerError::InvalidPath("Invalid directory name".into())
                    })?;
                    let bench_destination_dir = performance_results_dir.join(dir_name);
                    info!("Copying {} to {:?}", path.display(), bench_destination_dir);

                    let source = path.to_str().ok_or_else(|| {
                        IggyBenchDashboardServerError::InvalidPath("Invalid source path".into())
                    })?;
                    let destination = bench_destination_dir.to_str().ok_or_else(|| {
                        IggyBenchDashboardServerError::InvalidPath(
                            "Invalid destination path".into(),
                        )
                    })?;

                    async_copy_dir_files(source, destination).await?;
                }
            }

            cache.insert_gh_workflow(*workflow_id).await;
        }

        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("Shutdown signal received during sleep, exiting poll loop");
                    break;
                }
            }
            _ = sleep(Duration::from_secs(interval_seconds)) => {},
        }
    }

    Ok(())
}
