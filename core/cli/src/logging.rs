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

use iggy::cli_command::PRINT_TARGET;
use std::path::PathBuf;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    filter::{self, LevelFilter},
    fmt::{self},
    layer::{Layer, SubscriberExt},
};

pub(crate) struct Logging {
    file_guard: Option<WorkerGuard>,
    stdout_guard: Option<WorkerGuard>,
}

impl Logging {
    pub(crate) fn new() -> Self {
        Logging {
            file_guard: None,
            stdout_guard: None,
        }
    }

    pub(crate) fn init(&mut self, quiet: bool, debug: &Option<PathBuf>) -> &mut Self {
        let mut layers = vec![];

        let stdout_filter = filter::filter_fn(|metadata| metadata.target().contains(PRINT_TARGET));
        let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());

        let stdout_layer = fmt::Layer::default()
            .without_time()
            .with_level(false)
            .with_target(false)
            .with_writer(stdout_writer)
            .with_filter(if quiet {
                LevelFilter::OFF
            } else {
                LevelFilter::INFO
            })
            .boxed();

        self.stdout_guard = Some(stdout_guard);

        layers.push(stdout_layer.with_filter(stdout_filter).boxed());

        if let Some(file_path) = debug {
            let _ = std::fs::remove_file(file_path); // Remove file if it exists
            let file_appender = tracing_appender::rolling::never("", file_path);
            let (non_blocking_file, file_guard) = tracing_appender::non_blocking(file_appender);

            let file_layer = fmt::layer()
                .with_target(true)
                .with_writer(non_blocking_file)
                .with_filter(LevelFilter::TRACE)
                .boxed();
            self.file_guard = Some(file_guard);

            layers.push(file_layer);
        }

        let subscriber = tracing_subscriber::registry().with(layers);

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        self
    }
}
