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

use std::path::PathBuf;

use anyhow::{Context, Result};
use dircpy::*;
use git2::{
    Branch, BranchType, Cred, FetchOptions, RemoteCallbacks, Repository, build::CheckoutBuilder,
};
use tempfile::TempDir;
use tokio::{fs, process::Command};
use tracing::{error, info};

pub struct LocalBenchmarkRunner {
    iggy_repository_path: PathBuf,
    repo: Repository,
    temp_dir: TempDir,
}

impl LocalBenchmarkRunner {
    pub fn new(iggy_repository_path: &str) -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let repo = Repository::open(iggy_repository_path)
            .with_context(|| format!("Failed to open repository at '{}'", iggy_repository_path))?;

        let iggy_repository_path = PathBuf::from(iggy_repository_path).canonicalize()?;
        info!("Opened repository at '{}'", iggy_repository_path.display());
        Ok(Self {
            iggy_repository_path,
            repo,
            temp_dir,
        })
    }

    pub fn checkout_origin_master(&self) -> Result<()> {
        let ref_name = "refs/remotes/origin/master";
        self.repo
            .set_head(ref_name)
            .with_context(|| format!("Failed to set HEAD to '{}'", ref_name))?;
        self.repo
            .checkout_head(Some(CheckoutBuilder::new().force()))
            .with_context(|| "Failed to checkout HEAD".to_string())?;

        info!("Checked out to '{}'", ref_name);

        Ok(())
    }

    pub fn fetch_from_remote(&self) -> Result<()> {
        let mut remote = self.repo.find_remote("origin")?;

        let mut cb = RemoteCallbacks::new();
        cb.credentials(|_, username_from_url, _| {
            Cred::ssh_key_from_agent(username_from_url.unwrap())
        });

        let mut fetch_options = FetchOptions::new();
        fetch_options.remote_callbacks(cb);

        let refspecs: Vec<&str> = Vec::new();

        info!("Fetching from origin...");

        remote
            .fetch(&refspecs, Some(&mut fetch_options), None)
            .with_context(|| "Failed to fetch from origin")?;

        Ok(())
    }

    pub async fn build_benchmark_bin(&self) -> Result<()> {
        info!("Building benchmark binary...");
        let build_output = Command::new("cargo")
            .arg("build")
            .arg("--release")
            .arg("--bin")
            .arg("iggy-bench")
            .current_dir(&self.iggy_repository_path)
            .output()
            .await
            .with_context(|| "Failed to execute cargo build command")?;

        if !build_output.status.success() {
            let stderr = String::from_utf8_lossy(&build_output.stderr);
            error!("{}", stderr);
            anyhow::bail!("Failed to build benchmark binary");
        }
        Ok(())
    }

    pub async fn copy_scripts_and_bench_to_temp_dir(&self) -> Result<()> {
        let scripts_dir = self.repo.workdir().unwrap().join("scripts");
        let temp_dir = self.temp_dir.path().join("scripts");

        info!(
            "Copying {} to {}...",
            scripts_dir.display(),
            temp_dir.display()
        );

        copy_dir(&scripts_dir, &temp_dir).with_context(|| "Failed to copy scripts")?;

        let bench_bin = self
            .repo
            .workdir()
            .unwrap()
            .join("target/release/iggy-bench");

        let temp_bench_bin = self.temp_dir.path().join("iggy-bench");

        info!(
            "Copying {} to {}...",
            bench_bin.display(),
            temp_bench_bin.display()
        );

        fs::copy(bench_bin, temp_bench_bin)
            .await
            .with_context(|| "Failed to copy benchmark binary")?;

        Ok(())
    }

    fn checkout_branch(&self, branch: &Branch) -> Result<()> {
        let branch_name = branch.name()?.context("Branch name is not valid UTF-8")?;
        self.repo
            .set_head(&format!("refs/heads/{}", branch_name))
            .with_context(|| format!("Failed to set HEAD to branch '{}'", branch_name))?;
        self.repo
            .checkout_head(Some(CheckoutBuilder::new().force()))
            .with_context(|| format!("Failed to checkout branch '{}'", branch_name))?;

        info!("Checked out to branch '{}'", branch_name);

        Ok(())
    }

    fn checkout_commit(&self, commit_ref: &str) -> Result<()> {
        let obj = self
            .repo
            .revparse_single(commit_ref)
            .with_context(|| format!("Failed to resolve git ref '{}'", commit_ref))?;
        let commit = obj
            .peel_to_commit()
            .with_context(|| format!("Failed to peel object to commit for ref '{}'", commit_ref))?;
        self.repo
            .set_head_detached(commit.id())
            .with_context(|| format!("Failed to set HEAD to commit '{}'", commit_ref))?;
        self.repo
            .checkout_head(Some(CheckoutBuilder::new().force()))
            .with_context(|| format!("Failed to checkout commit '{}'", commit_ref))?;

        info!("Checked out to commit '{}'", commit_ref);

        Ok(())
    }

    pub fn checkout_to_gitref(&self, gitref: &str) -> Result<()> {
        match self.repo.find_branch(gitref, BranchType::Local) {
            Ok(branch) => {
                self.checkout_branch(&branch)?;
            }
            Err(_) => {
                let remote_branch_ref = format!("refs/remotes/origin/{}", gitref);
                if let Ok(reference) = self.repo.find_reference(&remote_branch_ref) {
                    let commit = reference.peel_to_commit().with_context(|| {
                        format!("Failed to peel remote branch '{}'", remote_branch_ref)
                    })?;

                    let branch = self
                        .repo
                        .branch(gitref, &commit, false)
                        .with_context(|| format!("Failed to create local branch '{}'", gitref))?;

                    self.checkout_branch(&branch)?;

                    info!(
                        "Created and checked out to local branch '{}' tracking '{}'",
                        gitref, remote_branch_ref
                    );
                } else {
                    self.checkout_commit(gitref)?;
                }
            }
        }

        Ok(())
    }

    /// Retrieves the last `n` commits up to `gitref`, inclusive.
    pub fn get_last_n_commits(&self, gitref: &str, n: u64) -> Result<Vec<String>> {
        info!("Getting last {} commits starting from '{}'", n, gitref);

        // Resolve the gitref to a Git object (could be a commit, tag, or branch)
        let obj = self
            .repo
            .revparse_single(gitref)
            .with_context(|| format!("Failed to resolve git ref '{}'", gitref))?;

        // Ensure the object is a commit
        let commit = obj
            .peel_to_commit()
            .with_context(|| format!("Reference '{}' does not point to a commit", gitref))?;

        let oid = commit.id();

        // Initialize the revwalk starting from the resolved commit OID
        let mut revwalk = self.repo.revwalk()?;
        revwalk.push(oid)?;
        revwalk.set_sorting(git2::Sort::TOPOLOGICAL | git2::Sort::TIME)?;

        let mut commits = Vec::new();
        for commit_result in revwalk.take(n as usize) {
            let oid =
                commit_result.with_context(|| "Failed to iterate over commits during revwalk")?;
            commits.push(oid.to_string());
        }

        if commits.len() < n as usize {
            anyhow::bail!(
                "Requested {} commits, but only found {} commits starting from '{}'",
                n,
                commits.len(),
                gitref
            );
        }

        // Reverse to start from the oldest commit to the newest
        commits.reverse();

        info!("Successfully retrieved the following commits for benchmarking:");
        for commit in &commits {
            info!(" - {}", commit);
        }

        Ok(commits)
    }

    pub async fn run_benchmark(&self) -> Result<()> {
        let bench_bin_path = self.temp_dir.path().join("iggy-bench");
        let script_path = self
            .temp_dir
            .path()
            .join("scripts/performance/run-standard-performance-suite.sh");

        if !script_path.exists() {
            anyhow::bail!("Benchmark script not found at '{}'", script_path.display());
        }

        info!(
            "Running benchmark script: {} {} in {}",
            script_path.display(),
            bench_bin_path.display(),
            self.iggy_repository_path.display()
        );

        let status = Command::new(&script_path)
            .current_dir(&self.iggy_repository_path)
            .arg(&bench_bin_path)
            .status()
            .await
            .context(format!(
                "Failed to execute benchmark script {} {}",
                script_path.display(),
                bench_bin_path.display()
            ))?;

        if !status.success() {
            anyhow::bail!("Benchmark script exited with status {}", status);
        }

        info!("Benchmark completed successfully.");

        Ok(())
    }
}
