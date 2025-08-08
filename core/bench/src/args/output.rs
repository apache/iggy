/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use clap::{Parser, Subcommand};

#[derive(Subcommand, Debug, Clone)]
pub enum BenchmarkOutputCommand {
    /// Output results to a directory subcommand
    Output(BenchmarkOutputArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct BenchmarkOutputArgs {
    /// Output directory path for storing benchmark results
    #[arg(long, short = 'o', default_value = "performance_results")]
    pub output_dir: String,

    /// Identifier for the benchmark run (defaults to hostname if not provided)
    #[arg(long, default_value_t = hostname::get().unwrap().to_string_lossy().to_string())]
    pub identifier: String,

    /// Additional remark for the benchmark (e.g., no-cache)
    #[arg(long)]
    pub remark: Option<String>,

    /// Extra information
    #[arg(long)]
    pub extra_info: Option<String>,

    /// Git reference (commit hash, branch or tag) used for note in the benchmark results
    #[arg(long)]
    pub gitref: Option<String>,

    /// Git reference date used for note in the benchmark results, preferably merge date of the commit
    #[arg(long)]
    pub gitref_date: Option<String>,

    /// Open generated charts in browser after benchmark is finished
    #[arg(long, short = 'c', default_value_t = false)]
    pub open_charts: bool,

    /// Enable OpenTelemetry metrics export
    #[arg(long, default_value_t = false)]
    pub otel_enabled: bool,

    /// OpenTelemetry collector endpoint (gRPC)
    #[arg(long, default_value = "http://localhost:4317")]
    pub otel_endpoint: String,

    /// OpenTelemetry metrics export interval in seconds
    #[arg(long, default_value_t = 1)]
    pub otel_export_interval_secs: u64,

    /// OpenTelemetry metrics export timeout in seconds
    #[arg(long, default_value_t = 10)]
    pub otel_export_timeout_secs: u64,

    /// Metrics buffer size (number of events)
    #[arg(long, default_value_t = 10000)]
    pub otel_buffer_size: usize,

    /// Metrics buffer flush interval in milliseconds
    #[arg(long, default_value_t = 100)]
    pub otel_buffer_flush_ms: u64,
}
