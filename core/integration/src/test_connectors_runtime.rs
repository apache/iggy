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

use assert_cmd::prelude::CommandCargoExt;
use rand::Rng;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;
use std::{collections::HashMap, net::TcpListener};
use tokio::time::sleep;
use uuid::Uuid;

pub const STATE_PATH_ENV_VAR: &str = "IGGY_CONNECTORS_STATE_PATH";
pub const CONSUMER_NAME: &str = "connectors";
const LOCAL_STATE_PREFIX: &str = "local_state_";

#[derive(Debug)]
pub struct TestConnectorsRuntime {
    envs: HashMap<String, String>,
    child_handle: Option<Child>,
    server_address: SocketAddr,
    stdout_file_path: Option<PathBuf>,
    stderr_file_path: Option<PathBuf>,
    server_executable_path: Option<String>,
    local_state_path: String,
    cleanup: bool,
}

impl TestConnectorsRuntime {
    pub fn with_iggy_address(
        iggy_tcp_server_address: &str,
        envs: Option<HashMap<String, String>>,
    ) -> Self {
        Self::new(iggy_tcp_server_address, envs, None)
    }

    pub fn new(
        iggy_tcp_server_address: &str,
        extra_envs: Option<HashMap<String, String>>,
        server_executable_path: Option<String>,
    ) -> Self {
        let mut envs = HashMap::new();
        if let Some(extra) = extra_envs {
            for (key, value) in extra {
                envs.insert(key, value);
            }
        }

        envs.insert(
            "IGGY_CONNECTORS_IGGY_ADDRESS".to_string(),
            iggy_tcp_server_address.to_string(),
        );
        envs.insert(
            "IGGY_CONNECTORS_IGGY_CONSUMER".to_string(),
            CONSUMER_NAME.to_string(),
        );
        Self::create(envs, server_executable_path)
    }

    pub fn create(
        mut envs: HashMap<String, String>,
        server_executable_path: Option<String>,
    ) -> Self {
        let server_address = Self::get_random_server_address();

        // If IGGY_CONNECTORS_STATE_PATH is not set, use a random path starting with "local_state_"
        let local_state_path = if let Some(state_path) = envs.get(STATE_PATH_ENV_VAR) {
            state_path.to_string()
        } else {
            Self::get_random_path()
        };

        envs.insert(
            "IGGY_CONNECTORS_STATE_PATH".to_string(),
            local_state_path.clone(),
        );

        Self {
            envs,
            child_handle: None,
            server_address,
            stdout_file_path: None,
            stderr_file_path: None,
            server_executable_path,
            local_state_path,
            cleanup: true,
        }
    }

    pub fn start(&mut self) {
        self.cleanup();
        self.envs
            .entry("IGGY_CONNECTORS_HTTP_ADDRESS".to_string())
            .or_insert(self.server_address.to_string());
        let mut command = if let Some(server_executable_path) = &self.server_executable_path {
            Command::new(server_executable_path)
        } else {
            Command::cargo_bin("iggy-connectors").unwrap()
        };
        command.envs(self.envs.clone());
        let child = command
            .spawn()
            .expect("Failed to start Connectors Runtime process");
        self.child_handle = Some(child);
    }

    pub fn stop(&mut self) {
        #[allow(unused_mut)]
        if let Some(mut child_handle) = self.child_handle.take() {
            #[cfg(unix)]
            unsafe {
                use libc::SIGTERM;
                use libc::kill;
                kill(child_handle.id() as libc::pid_t, SIGTERM);
            }

            #[cfg(not(unix))]
            child_handle.kill().unwrap();

            if let Ok(output) = child_handle.wait_with_output() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                if let Some(stderr_file_path) = &self.stderr_file_path {
                    OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(stderr_file_path)
                        .unwrap()
                        .write_all(stderr.as_bytes())
                        .unwrap();
                }

                if let Some(stdout_file_path) = &self.stdout_file_path {
                    OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(stdout_file_path)
                        .unwrap()
                        .write_all(stdout.as_bytes())
                        .unwrap();
                }
            }
        }
        self.cleanup();
    }

    pub fn is_started(&self) -> bool {
        self.child_handle.is_some()
    }

    pub fn pid(&self) -> u32 {
        self.child_handle.as_ref().unwrap().id()
    }

    pub fn get_random_path() -> String {
        format!("{}{}", LOCAL_STATE_PREFIX, Uuid::now_v7().to_u128_le())
    }

    fn get_http_api_address(&self) -> String {
        format!(
            "http://{}:{}",
            self.server_address.ip(),
            self.server_address.port()
        )
    }

    pub async fn ensure_started(&self) {
        let http_api_address = self.get_http_api_address();
        let client = reqwest::Client::new();
        let max_retries = 1000;
        let mut retries = 0;
        while let Err(error) = client.get(&http_api_address).send().await {
            sleep(Duration::from_millis(20)).await;
            retries += 1;
            if retries >= max_retries {
                panic!(
                    "Failed to ping Connectors runtime: {http_api_address} after {max_retries} retries. {error}"
                );
            }
        }
        println!("Connectors runtime address started at: {http_api_address}");
    }

    fn get_random_server_address() -> SocketAddr {
        let mut rng = rand::thread_rng();
        let max_retries = 100;

        for _ in 0..max_retries {
            #[cfg(target_os = "linux")]
            let port = rng.gen_range(20000..=29999);

            #[cfg(target_os = "macos")]
            let port = rng.gen_range(20000..=49151);

            #[cfg(target_os = "windows")]
            let port = rng.gen_range(20000..=49151);

            let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
            if TcpListener::bind(addr).is_ok() {
                return addr;
            }
        }

        panic!("Failed to find a free port after {max_retries} retries");
    }

    fn cleanup(&self) {
        if !self.cleanup {
            return;
        }

        if fs::metadata(&self.local_state_path).is_ok() {
            fs::remove_dir_all(&self.local_state_path).unwrap();
        }
    }
}

impl Drop for TestConnectorsRuntime {
    fn drop(&mut self) {
        self.stop();
    }
}
