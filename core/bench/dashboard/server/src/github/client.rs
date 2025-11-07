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
use octocrab::{
    Octocrab,
    models::{
        RunId,
        repos::Tag,
        workflows::{Run, WorkflowListArtifact},
    },
    params::actions::ArchiveFormat,
};
use std::io::Cursor;
use tempfile::TempDir;
use tokio::fs::read_dir;
use tracing::{info, trace};
use zip::ZipArchive;

const OWNER: &str = "apache";
const REPO: &str = "iggy";
const WORKFLOW_FILE: &str = "performance.yml";

pub struct IggyBenchDashboardGithubClient {
    octocrab: Octocrab,
}

impl IggyBenchDashboardGithubClient {
    pub fn new() -> Result<Self, IggyBenchDashboardServerError> {
        let token = std::env::var("GITHUB_TOKEN").map_err(|_| {
            IggyBenchDashboardServerError::InternalError("GITHUB_TOKEN not set".into())
        })?;
        let octocrab = Octocrab::builder().personal_token(token).build()?;
        Ok(Self { octocrab })
    }
    pub async fn download_artifact(
        &self,
        workflow_id: u64,
    ) -> Result<TempDir, IggyBenchDashboardServerError> {
        let runs = self.get_all_workflow_runs().await?;
        if runs.is_empty() {
            return Err(IggyBenchDashboardServerError::NotFound(
                "No successful workflow runs found".into(),
            ));
        }

        let run_id = RunId(workflow_id);

        runs.iter().find(|run| run.id == run_id).ok_or_else(|| {
            IggyBenchDashboardServerError::NotFound(format!(
                "Workflow run {workflow_id} not found in {WORKFLOW_FILE}"
            ))
        })?;

        let artifacts = self.get_artifacts_for_workflow_run(run_id).await?;

        let artifact = artifacts.first().ok_or_else(|| {
            IggyBenchDashboardServerError::NotFound("No artifacts found in the workflow run".into())
        })?;
        let artifact_id = artifact.id;

        info!("Downloading new artifact ID: {}", artifact_id);

        let bytes = self
            .octocrab
            .actions()
            .download_artifact(OWNER, REPO, artifact_id, ArchiveFormat::Zip)
            .await?;
        info!(
            "Downloaded artifact ID: {}, bytes length: {}",
            artifact_id,
            bytes.len()
        );

        let temp_dir = TempDir::new()?;
        let output_dir = temp_dir.path();

        info!("Unzipping to directory: {:?}", output_dir);

        let cursor = Cursor::new(bytes);
        let mut zip = ZipArchive::new(cursor)?;

        for i in 0..zip.len() {
            let mut file = zip.by_index(i)?;
            let outpath = output_dir.join(file.mangled_name());

            if file.is_dir() {
                std::fs::create_dir_all(&outpath)?;
            } else {
                if let Some(parent) = outpath.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut outfile = std::fs::File::create(&outpath)?;
                std::io::copy(&mut file, &mut outfile)?;
            }

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;

                if let Some(mode) = file.unix_mode() {
                    std::fs::set_permissions(&outpath, std::fs::Permissions::from_mode(mode))?;
                }
            }
        }

        info!("Artifact unzipped to: {:?}", output_dir);

        let mut entries = Vec::new();
        let mut dir = read_dir(output_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                entries.push(path);
            }
        }

        if entries.len() != 1 {
            return Err(IggyBenchDashboardServerError::InternalError(format!(
                "Expected exactly one directory in the unzipped artifact directory {}, found {}",
                temp_dir.path().display(),
                entries.len()
            )));
        }

        Ok(temp_dir)
    }

    /// Retrieves workflow runs for the specified workflow file (`performance.yml`)
    /// that have a successful status.
    async fn get_all_workflow_runs(&self) -> Result<Vec<Run>, IggyBenchDashboardServerError> {
        let runs: Vec<Run> = self
            .octocrab
            .workflows(OWNER, REPO)
            .list_runs(WORKFLOW_FILE)
            .status("success")
            .per_page(100)
            .send()
            .await?
            .into_iter()
            .collect();
        Ok(runs)
    }

    /// Retrieves workflow runs for the specified workflow file (`performance.yml`)
    /// that were triggered on a given branch and have a successful status.
    pub async fn get_successful_workflow_runs(
        &self,
        branch: &str,
    ) -> Result<Vec<Run>, IggyBenchDashboardServerError> {
        let runs: Vec<Run> = self
            .octocrab
            .workflows(OWNER, REPO)
            .list_runs(WORKFLOW_FILE)
            .status("success")
            .branch(branch)
            .send()
            .await?
            .into_iter()
            .collect();

        trace!(
            "Found {} successful workflow runs on {} branch",
            runs.len(),
            branch
        );
        Ok(runs)
    }

    async fn get_artifacts_for_workflow_run(
        &self,
        run_id: RunId,
    ) -> Result<Vec<WorkflowListArtifact>, IggyBenchDashboardServerError> {
        let artifact_response = self
            .octocrab
            .actions()
            .list_workflow_run_artifacts(OWNER, REPO, run_id)
            .send()
            .await?;

        let artifacts = artifact_response
            .value
            .ok_or_else(|| {
                IggyBenchDashboardServerError::NotFound(format!(
                    "No artifacts found for workflow run {run_id}"
                ))
            })?
            .into_iter()
            .collect();
        Ok(artifacts)
    }

    /// Given a list of tags and a commit SHA, returns the tag that starts with that commit SHA.
    pub fn get_tag_for_commit(tags: &Vec<Tag>, commit_sha: &str) -> Option<Tag> {
        for tag in tags {
            if tag.commit.sha.starts_with(commit_sha) {
                return Some(tag.clone());
            }
        }
        None
    }

    pub async fn get_server_tags(&self) -> Result<Vec<Tag>, IggyBenchDashboardServerError> {
        let mut tags = Vec::new();
        let mut page = self.octocrab.repos(OWNER, REPO).list_tags().send().await?;

        loop {
            for tag in &page {
                if tag.name.starts_with("server") {
                    tags.push(tag.clone());
                }
            }
            page = match self.octocrab.get_page(&page.next).await? {
                Some(next_page) => next_page,
                None => break,
            };
        }

        Ok(tags)
    }
}
