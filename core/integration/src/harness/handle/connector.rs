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

use crate::harness::config::ConnectorConfig;
use crate::harness::context::TestContext;
use crate::harness::error::TestBinaryError;
use crate::harness::traits::{IggyDependent, TestBinary};
use assert_cmd::prelude::CommandCargoExt;
use rand::Rng as _;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread::panicking;
use std::time::Duration;
use tokio::time::sleep;

const TEST_VERBOSITY_ENV_VAR: &str = "IGGY_TEST_VERBOSE";
const MAX_HEALTH_CHECK_RETRIES: u32 = 1000;

pub struct ConnectorHandle {
    config: ConnectorConfig,
    context: Arc<TestContext>,
    envs: HashMap<String, String>,
    child_handle: Option<Child>,
    server_address: SocketAddr,
    iggy_address: Option<SocketAddr>,
    stdout_path: Option<PathBuf>,
    stderr_path: Option<PathBuf>,
}

impl std::fmt::Debug for ConnectorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorHandle")
            .field("server_address", &self.server_address)
            .field("iggy_address", &self.iggy_address)
            .field("is_running", &self.child_handle.is_some())
            .finish_non_exhaustive()
    }
}

impl ConnectorHandle {
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
        self.context.connector_state_path()
    }

    pub fn collect_logs(&self) -> (String, String) {
        let stdout = self
            .stdout_path
            .as_ref()
            .and_then(|p| fs::read_to_string(p).ok())
            .unwrap_or_else(|| "[No stdout log]".to_string());

        let stderr = self
            .stderr_path
            .as_ref()
            .and_then(|p| fs::read_to_string(p).ok())
            .unwrap_or_else(|| "[No stderr log]".to_string());

        (stdout, stderr)
    }

    fn find_available_port() -> SocketAddr {
        let mut rng = rand::rng();
        let max_retries = 100;

        for _ in 0..max_retries {
            let port = rng.random_range(20000..=29999);
            let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
            if TcpListener::bind(addr).is_ok() {
                return addr;
            }
        }

        panic!("Failed to find available port after {max_retries} retries");
    }

    fn build_envs(&mut self) {
        let state_path = self.context.connector_state_path();
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

impl TestBinary for ConnectorHandle {
    type Config = ConnectorConfig;

    fn with_config(config: Self::Config, context: Arc<TestContext>) -> Self {
        let server_address = Self::find_available_port();

        Self {
            config,
            context,
            envs: HashMap::new(),
            child_handle: None,
            server_address,
            iggy_address: None,
            stdout_path: None,
            stderr_path: None,
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

        command.envs(self.envs.clone());

        let verbose = std::env::var(TEST_VERBOSITY_ENV_VAR).is_ok()
            || self.envs.contains_key(TEST_VERBOSITY_ENV_VAR);

        if verbose {
            command.stdout(Stdio::inherit());
            command.stderr(Stdio::inherit());
        } else {
            let stdout_path = self.context.connector_stdout_path();
            let stderr_path = self.context.connector_stderr_path();

            let stdout_file =
                File::create(&stdout_path).map_err(|e| TestBinaryError::DirectoryCreation {
                    path: stdout_path.clone(),
                    source: e,
                })?;
            let stderr_file =
                File::create(&stderr_path).map_err(|e| TestBinaryError::DirectoryCreation {
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

        Ok(())
    }

    fn stop(&mut self) -> Result<(), TestBinaryError> {
        #[allow(unused_mut)]
        if let Some(mut child) = self.child_handle.take() {
            #[cfg(unix)]
            unsafe {
                libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
            }

            #[cfg(not(unix))]
            child.kill().ok();

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
        self.child_handle.is_some()
    }

    fn assert_running(&self) {
        if let Some(ref child) = self.child_handle {
            let mut child_clone = unsafe { std::ptr::read(child as *const Child) };
            if let Ok(Some(status)) = child_clone.try_wait() {
                let (stdout, stderr) = self.collect_logs();
                panic!(
                    "Connector runtime has crashed with exit status: {}\n\n\
                     === STDOUT ===\n{}\n\n\
                     === STDERR ===\n{}",
                    status, stdout, stderr
                );
            }
            std::mem::forget(child_clone);
        }
    }

    fn pid(&self) -> Option<u32> {
        self.child_handle.as_ref().map(|c| c.id())
    }
}

impl IggyDependent for ConnectorHandle {
    fn set_iggy_address(&mut self, addr: SocketAddr) {
        self.iggy_address = Some(addr);
    }

    async fn wait_ready(&self) -> Result<(), TestBinaryError> {
        let http_address = self.http_url();
        let client = reqwest::Client::new();

        for retry in 0..MAX_HEALTH_CHECK_RETRIES {
            match client.get(&http_address).send().await {
                Ok(_) => return Ok(()),
                Err(_) => {
                    if retry == MAX_HEALTH_CHECK_RETRIES - 1 {
                        return Err(TestBinaryError::HealthCheckFailed {
                            binary: "iggy-connectors".to_string(),
                            address: http_address,
                            retries: MAX_HEALTH_CHECK_RETRIES,
                        });
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            }
        }

        Ok(())
    }
}

impl Drop for ConnectorHandle {
    fn drop(&mut self) {
        let _ = self.stop();

        if panicking() {
            let (stdout, stderr) = self.collect_logs();
            eprintln!("Iggy connectors stdout:\n{}", stdout);
            eprintln!("Iggy connectors stderr:\n{}", stderr);
        }
    }
}
