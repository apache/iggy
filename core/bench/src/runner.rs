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

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bench_report::{hardware::BenchmarkHardware, individual_metrics::BenchmarkIndividualMetrics};
use iggy::prelude::IggyError;
use tokio::time::sleep;
use tracing::{error, info};

use crate::analytics::report_builder::{BenchmarkReportBuilder, cluster_suffix};
use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::Benchmarkable;
use crate::plot::{ChartType, plot_chart};
use crate::poll_artifacts::RunStatus;
use crate::utils::cpu_name::append_cpu_name_lowercase;
use crate::utils::{
    ClientFactory, collect_server_logs_and_save_to_file, params_from_args_and_metrics,
};

pub struct BenchmarkRunner {
    args: Option<IggyBenchArgs>,
}

impl BenchmarkRunner {
    pub const fn new(args: IggyBenchArgs) -> Self {
        Self { args: Some(args) }
    }

    #[allow(clippy::cognitive_complexity)]
    pub async fn run(mut self) -> Result<RunStatus, IggyError> {
        let args = self.args.take().unwrap();
        let pretty = args.pretty;
        let should_open_charts = args.open_charts();

        let transport = args.transport();
        let server_addr = args.server_address();
        info!("Starting to benchmark: {transport} with server: {server_addr}",);

        let mut benchmark: Box<dyn Benchmarkable> = args.into();
        benchmark.print_info();
        let interrupt = tokio::signal::ctrl_c();
        tokio::pin!(interrupt);
        let mut join_handles = tokio::select! {
            result = benchmark.run() => result?,
            _ = &mut interrupt => return Ok(RunStatus::Interrupted),
        };

        let mut individual_metrics = Vec::new();

        loop {
            let joined = tokio::select! {
                result = join_handles.join_next() => result,
                _ = &mut interrupt => {
                    info!("Received Ctrl-C, stopping actors and preserving artifacts...");
                    join_handles.shutdown().await;
                    return Ok(RunStatus::Interrupted);
                }
            };
            let Some(individual_metric) = joined else {
                break;
            };
            match individual_metric {
                Ok(Ok(individual_metric)) => individual_metrics.push(individual_metric),
                Ok(Err(error)) => {
                    join_handles.shutdown().await;
                    return Err(error);
                }
                Err(error) => {
                    error!("Benchmark actor failed: {error}");
                    join_handles.shutdown().await;
                    return Err(IggyError::Error);
                }
            }
        }

        info!("All actors joined!");

        if let (Some(artifacts), Some(output_dir)) = (
            &benchmark.args().poll_artifacts,
            benchmark.args().output_dir(),
        ) {
            let mut dir_name = benchmark.args().generate_dir_name();
            append_cpu_name_lowercase(&mut dir_name);
            artifacts
                .write(&Path::new(&output_dir).join(dir_name), RunStatus::Running)
                .map_err(|error| {
                    error!("Failed to write poll artifacts: {error}");
                    IggyError::CannotWriteToFile
                })?;
        }
        tokio::select! {
            result = Self::report(benchmark.args(), benchmark.client_factory(), individual_metrics, pretty, should_open_charts) => result?,
            _ = &mut interrupt => return Ok(RunStatus::Interrupted),
        }
        Ok(RunStatus::Completed)
    }

    async fn report(
        args: &IggyBenchArgs,
        client_factory: &Arc<dyn ClientFactory>,
        individual_metrics: Vec<BenchmarkIndividualMetrics>,
        pretty: bool,
        should_open_charts: bool,
    ) -> Result<(), IggyError> {
        let admin_client = client_factory.create_authenticated_client().await?;

        let hardware = BenchmarkHardware::get_system_info_with_identifier(args.identifier());
        let params = params_from_args_and_metrics(args, &individual_metrics);

        let report = BenchmarkReportBuilder::build(
            hardware,
            params,
            individual_metrics,
            args.moving_average_window(),
            &admin_client,
        )
        .await;

        // Sleep just to see result prints after all tasks are joined (they print per-actor results)
        sleep(Duration::from_millis(10)).await;

        report.print_summary(pretty);

        if let Some(output_dir) = args.output_dir() {
            // Generate the full output path using the directory name generator
            let mut dir_name = args.generate_dir_name();
            append_cpu_name_lowercase(&mut dir_name);
            // Cluster runs share params (and thus the dir name) with single-node
            // runs; suffix keeps them from overwriting each other's results.
            dir_name.push_str(&cluster_suffix(report.cluster.as_ref()));
            let full_output_path = Path::new(&output_dir)
                .join(dir_name.clone())
                .to_string_lossy()
                .to_string();

            // Dump the report to JSON
            report.dump_to_json(&full_output_path);

            if let Err(e) =
                collect_server_logs_and_save_to_file(&admin_client, Path::new(&full_output_path))
                    .await
            {
                error!("Failed to collect server logs: {e}");
            }

            // Generate the plots
            plot_chart(
                &report,
                &full_output_path,
                &ChartType::Throughput,
                should_open_charts,
            )
            .map_err(|e| {
                error!("Failed to generate plots: {e}");
                IggyError::CannotWriteToFile
            })?;
            plot_chart(
                &report,
                &full_output_path,
                &ChartType::Latency,
                should_open_charts,
            )
            .map_err(|e| {
                error!("Failed to generate plots: {e}");
                IggyError::CannotWriteToFile
            })?;
            plot_chart(
                &report,
                &full_output_path,
                &ChartType::LatencyDistribution,
                should_open_charts,
            )
            .map_err(|e| {
                error!("Failed to generate plots: {e}");
                IggyError::CannotWriteToFile
            })?;
        }

        Ok(())
    }
}
