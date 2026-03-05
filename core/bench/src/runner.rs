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

use crate::analytics::report_builder::BenchmarkReportBuilder;
use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::Benchmarkable;
use crate::plot::{ChartType, plot_chart};
use crate::utils::cpu_name::append_cpu_name_lowercase;
use crate::utils::{collect_server_logs_and_save_to_file, params_from_args_and_metrics};
use bench_report::hardware::BenchmarkHardware;
use iggy::prelude::{Client, IggyClient, IggyError, UserClient};
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

pub struct BenchmarkRunner {
    args: Option<IggyBenchArgs>,
}

impl BenchmarkRunner {
    pub const fn new(args: IggyBenchArgs) -> Self {
        Self { args: Some(args) }
    }

    #[allow(clippy::cognitive_complexity)]
    pub async fn run(mut self) -> Result<(), IggyError> {
        let args = self.args.take().unwrap();
        let should_open_charts = args.open_charts();

        let transport = args.transport();
        let server_addr = args.server_address();
        info!("Starting to benchmark: {transport} with server: {server_addr}",);

        let mut benchmark: Box<dyn Benchmarkable> = args.into();
        benchmark.print_info();
        let mut join_handles = benchmark.run().await?;

        let mut individual_metrics = Vec::new();

        while let Some(individual_metric) = join_handles.join_next().await {
            let individual_metric = individual_metric.expect("Failed to join actor!");
            match individual_metric {
                Ok(individual_metric) => individual_metrics.push(individual_metric),
                Err(e) => return Err(e),
            }
        }

        info!("All actors joined!");

        let client_factory = benchmark.client_factory();
        let admin_client_wrapper = client_factory.create_client().await;
        let admin_client = IggyClient::create(admin_client_wrapper, None, None);
        admin_client.connect().await?;
        admin_client
            .login_user(client_factory.username(), client_factory.password())
            .await?;

        let hardware =
            BenchmarkHardware::get_system_info_with_identifier(benchmark.args().identifier());
        let params = params_from_args_and_metrics(benchmark.args(), &individual_metrics);

        let report = BenchmarkReportBuilder::build(
            hardware,
            params,
            individual_metrics,
            benchmark.args().moving_average_window(),
            &admin_client,
        )
        .await;

        // Sleep just to see result prints after all tasks are joined (they print per-actor results)
        sleep(Duration::from_millis(10)).await;

        report.print_summary();

        if let Some(output_dir) = benchmark.args().output_dir() {
            // Generate the full output path using the directory name generator
            let mut dir_name = benchmark.args().generate_dir_name();
            append_cpu_name_lowercase(&mut dir_name);
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
