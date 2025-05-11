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

use clap::{CommandFactory, Parser, Subcommand, error::ErrorKind};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Subcommand, Deserialize)]
pub enum PollGithub {
    PollGithub(PollGithubArgs),
}

#[derive(Debug, Parser, Deserialize)]
pub struct PollGithubArgs {
    /// How often to poll GitHub for new artifacts, in seconds
    #[arg(short, long, default_value = "60")]
    pub interval_seconds: u64,

    /// Branch to filter artifacts by
    #[arg(short, long, default_value = "master")]
    pub branch: String,
}

#[derive(Debug, Deserialize, Parser)]
#[command(author, version, about, long_about = None)]
pub struct IggyBenchDashboardServerArgs {
    /// Server host address
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Server port
    #[arg(long, default_value_t = 8061)]
    pub port: u16,

    /// Directory containing performance results
    #[arg(long, default_value = "./performance_results")]
    pub results_dir: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Allowed CORS origins (comma-separated)
    #[arg(long, default_value = "*")]
    pub cors_origins: String,

    /// Poll GitHub for new artifacts
    #[command(subcommand)]
    pub github: Option<PollGithub>,
}

impl IggyBenchDashboardServerArgs {
    pub fn parse() -> Self {
        Self::parse_from(std::env::args())
    }

    pub fn validate(&self) {
        let mut cmd = IggyBenchDashboardServerArgs::command();
        if !self.results_dir.exists() {
            cmd.error(
                ErrorKind::InvalidValue,
                format!(
                    "Results directory does not exist: {}",
                    self.results_dir.display()
                ),
            )
            .exit();
        }
        if !self.results_dir.is_dir() {
            cmd.error(
                ErrorKind::InvalidValue,
                format!(
                    "Results path is not a directory: {}",
                    self.results_dir.display()
                ),
            )
            .exit();
        }

        if self.github.is_some() && std::env::var("GITHUB_TOKEN").is_err() {
            cmd.error(
                ErrorKind::InvalidValue,
                "GITHUB_TOKEN env variable not set, but GitHub polling enabled",
            )
            .exit();
        }
    }

    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn cors_origins_list(&self) -> Vec<String> {
        self.cors_origins
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    }
}
