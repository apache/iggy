/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use super::common;
use crate::harness::config::ConnectorsRuntimeConfig;
use crate::harness::context::TestContext;
use crate::harness::error::TestBinaryError;
use crate::harness::port_reserver::SinglePortReserver;
use crate::harness::traits::{IggyServerDependent, TestBinary};
use assert_cmd::prelude::CommandCargoExt;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const TEST_VERBOSITY_ENV_VAR: &str = "IGGY_TEST_VERBOSE";

pub struct ConnectorsRuntimeHandle {
    config: ConnectorsRuntimeConfig,
    context: Arc<TestContext>,
    envs: HashMap<String, String>,
    child_handle: Option<Child>,
    server_address: SocketAddr,
    iggy_address: Option<SocketAddr>,
    stdout_path: Option<PathBuf>,
    stderr_path: Option<PathBuf>,
    port_reserver: Option<SinglePortReserver>,
}

impl std::fmt::Debug for ConnectorsRuntimeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorsRuntimeHandle")
            .field("server_address", &self.server_address)
            .field("iggy_address", &self.iggy_address)
            .field("is_running", &self.child_handle.is_some())
            .finish_non_exhaustive()
    }
}

impl ConnectorsRuntimeHandle {
    pub fn http_address(&self) -> SocketAddr {
        self.server_address
    }

    pub fn http_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.server_address.ip(),
            self.server_address.port()
        )
    }

    pub fn state_path(&self) -> PathBuf {
        self.context.connectors_runtime_state_path()
    }

    pub fn collect_logs(&self) -> (String, String) {
        common::collect_logs(&self.stdout_path, &self.stderr_path)
    }

    fn build_envs(&mut self) {
        let state_path = self.context.connectors_runtime_state_path();
        self.envs.insert(
            "IGGY_CONNECTORS_STATE_PATH".to_string(),
            state_path.display().to_string(),
        );
        self.envs.insert(
            "IGGY_CONNECTORS_HTTP_ADDRESS".to_string(),
            self.server_address.to_string(),
        );

        if let Some(addr) = self.iggy_address {
            self.envs
                .insert("IGGY_CONNECTORS_IGGY_ADDRESS".to_string(), addr.to_string());
        }

        if let Some(ref config_path) = self.config.config_path {
            self.envs.insert(
                "IGGY_CONNECTORS_CONFIG_PATH".to_string(),
                config_path.display().to_string(),
            );
        }

        for (k, v) in &self.config.extra_envs {
            self.envs.insert(k.clone(), v.clone());
        }
    }
}

impl TestBinary for ConnectorsRuntimeHandle {
    type Config = ConnectorsRuntimeConfig;

    fn with_config(config: Self::Config, context: Arc<TestContext>) -> Self {
        let reserver =
            SinglePortReserver::new().expect("Failed to reserve port for connectors runtime");
        let server_address = reserver.address();

        Self {
            config,
            context,
            envs: HashMap::new(),
            child_handle: None,
            server_address,
            iggy_address: None,
            stdout_path: None,
            stderr_path: None,
            port_reserver: Some(reserver),
        }
    }

    fn start(&mut self) -> Result<(), TestBinaryError> {
        self.build_envs();

        #[allow(deprecated)]
        let mut command = if let Some(ref path) = self.config.executable_path {
            Command::new(path)
        } else {
            Command::cargo_bin("iggy-connectors").map_err(|e| TestBinaryError::ProcessSpawn {
                binary: "iggy-connectors".to_string(),
                source: std::io::Error::other(e.to_string()),
            })?
        };

        command.envs(&self.envs);

        let verbose = std::env::var(TEST_VERBOSITY_ENV_VAR).is_ok()
            || self.envs.contains_key(TEST_VERBOSITY_ENV_VAR);

        if verbose {
            command.stdout(Stdio::inherit());
            command.stderr(Stdio::inherit());
        } else {
            let stdout_path = self.context.connectors_runtime_stdout_path();
            let stderr_path = self.context.connectors_runtime_stderr_path();

            let stdout_file =
                File::create(&stdout_path).map_err(|e| TestBinaryError::FileSystemError {
                    path: stdout_path.clone(),
                    source: e,
                })?;
            let stderr_file =
                File::create(&stderr_path).map_err(|e| TestBinaryError::FileSystemError {
                    path: stderr_path.clone(),
                    source: e,
                })?;

            command.stdout(stdout_file);
            command.stderr(stderr_file);

            self.stdout_path = Some(fs::canonicalize(&stdout_path)?);
            self.stderr_path = Some(fs::canonicalize(&stderr_path)?);
        }

        let child = command.spawn().map_err(|e| TestBinaryError::ProcessSpawn {
            binary: "iggy-connectors".to_string(),
            source: e,
        })?;
        self.child_handle = Some(child);

        // Release port reservation immediately after spawn to avoid SO_REUSEPORT
        // load-balancing conflicts during health checks.
        if let Some(reserver) = self.port_reserver.take() {
            reserver.release();
        }

        Ok(())
    }

    fn stop(&mut self) -> Result<(), TestBinaryError> {
        if let Some(child) = self.child_handle.take() {
            let child = common::graceful_kill(child);

            if let Ok(output) = child.wait_with_output() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);

                if let Some(path) = self.stderr_path.as_ref()
                    && let Ok(mut f) = OpenOptions::new().append(true).create(true).open(path)
                {
                    let _ = f.write_all(stderr.as_bytes());
                }

                if let Some(path) = self.stdout_path.as_ref()
                    && let Ok(mut f) = OpenOptions::new().append(true).create(true).open(path)
                {
                    let _ = f.write_all(stdout.as_bytes());
                }
            }
        }

        Ok(())
    }

    fn is_running(&self) -> bool {
        self.pid().is_some_and(common::is_process_alive)
    }

    fn assert_running(&self) {
        if let Some(pid) = self.pid().filter(|&p| !common::is_process_alive(p)) {
            let (stdout, stderr) = self.collect_logs();
            panic!(
                "Connector runtime (pid {}) has crashed\n\n\
                 === STDOUT ===\n{}\n\n\
                 === STDERR ===\n{}",
                pid, stdout, stderr
            );
        }
    }

    fn pid(&self) -> Option<u32> {
        self.child_handle.as_ref().map(|c| c.id())
    }
}

impl IggyServerDependent for ConnectorsRuntimeHandle {
    fn set_iggy_address(&mut self, addr: SocketAddr) {
        self.iggy_address = Some(addr);
    }

    async fn wait_ready(&mut self) -> Result<(), TestBinaryError> {
        let http_address = self.http_url();
        let client = reqwest::Client::new();

        for retry in 0..common::DEFAULT_HEALTH_CHECK_RETRIES {
            match client.get(&http_address).send().await {
                Ok(_) => {
                    return Ok(());
                }
                Err(_) => {
                    if retry == common::DEFAULT_HEALTH_CHECK_RETRIES - 1 {
                        return Err(TestBinaryError::HealthCheckFailed {
                            binary: "iggy-connectors".to_string(),
                            address: http_address,
                            retries: common::DEFAULT_HEALTH_CHECK_RETRIES,
                        });
                    }
                    sleep(Duration::from_millis(
                        common::DEFAULT_HEALTH_CHECK_INTERVAL_MS,
                    ))
                    .await;
                }
            }
        }

        unreachable!()
    }
}

impl Drop for ConnectorsRuntimeHandle {
    fn drop(&mut self) {
        let _ = self.stop();
        common::dump_logs_on_panic("Iggy connectors", &self.stdout_path, &self.stderr_path);
    }
}
