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

mod local_benchmark_runner;
mod utils;
use anyhow::{Context, Result};
use dircpy::copy_dir;
use local_benchmark_runner::LocalBenchmarkRunner;
use tracing::info;

use crate::args::IggyBenchRunnerArgs;

pub struct IggyBenchRunnerApp {
    args: IggyBenchRunnerArgs,
}

impl IggyBenchRunnerApp {
    pub fn new(args: IggyBenchRunnerArgs) -> Result<Self> {
        Ok(Self { args })
    }

    pub async fn run(&self) -> Result<()> {
        self.local_benchmark(&self.args).await
    }

    async fn local_benchmark(&self, args: &IggyBenchRunnerArgs) -> Result<()> {
        let repo_path = args.directory.clone();
        let local_benchmark = LocalBenchmarkRunner::new(&repo_path)?;
        local_benchmark.fetch_from_remote()?;
        if !args.skip_master_checkout {
            local_benchmark.checkout_origin_master()?;
        }
        local_benchmark.build_benchmark_bin().await?;
        local_benchmark.copy_scripts_and_bench_to_temp_dir().await?;
        local_benchmark.checkout_to_gitref(&args.gitref)?;

        let commits = local_benchmark.get_last_n_commits(&args.gitref, args.count)?;

        for commit in commits {
            info!("Processing commit: {}", commit);
            local_benchmark.checkout_to_gitref(&commit)?;
            local_benchmark
                .run_benchmark()
                .await
                .context("Failed to run benchmark")?;
        }

        let source_dir = repo_path + "/performance_results";

        // Copy results to the output directory
        tokio::fs::create_dir_all(&self.args.output_dir).await?;
        let target_dir = format!("{}/{}", self.args.output_dir, args.gitref);

        // Remove target directory if it exists to ensure clean copy
        if std::path::Path::new(&target_dir).exists() {
            std::fs::remove_dir_all(&target_dir)?;
        }
        info!("Copying {} to {}", source_dir, target_dir);

        // Recursively copy the entire directory
        copy_dir(&source_dir, &target_dir)?;

        Ok(())
    }
}
