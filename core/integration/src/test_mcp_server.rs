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
use rmcp::{
    RoleClient, ServiceExt,
    model::{ClientCapabilities, ClientInfo, Implementation, InitializeRequestParam},
    service::RunningService,
    transport::StreamableHttpClientTransport,
};
use std::fs::{self, File};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread::panicking;
use std::time::Duration;
use std::{collections::HashMap, net::TcpListener};
use tokio::time::sleep;

pub const CONSUMER_NAME: &str = "mcp";
const MCP_PATH: &str = "/mcp";
const TEST_VERBOSITY_ENV_VAR: &str = "IGGY_TEST_VERBOSE";

pub type McpClient = RunningService<RoleClient, InitializeRequestParam>;

#[derive(Debug)]
pub struct TestMcpServer {
    envs: HashMap<String, String>,
    child_handle: Option<Child>,
    server_address: SocketAddr,
    stdout_file_path: Option<PathBuf>,
    stderr_file_path: Option<PathBuf>,
    server_executable_path: Option<String>,
}

impl TestMcpServer {
    pub fn with_iggy_address(iggy_tcp_server_address: &str) -> Self {
        Self::new(iggy_tcp_server_address, None, None)
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

        envs.insert("IGGY_MCP_HTTP_PATH".to_string(), MCP_PATH.to_string());
        envs.insert(
            "IGGY_MCP_IGGY_ADDRESS".to_string(),
            iggy_tcp_server_address.to_string(),
        );
        envs.insert(
            "IGGY_MCP_IGGY_CONSUMER".to_string(),
            CONSUMER_NAME.to_string(),
        );
        envs.insert("IGGY_MCP_TRANSPORT".to_string(), "http".to_string());
        Self::create(envs, server_executable_path)
    }

    pub fn create(envs: HashMap<String, String>, server_executable_path: Option<String>) -> Self {
        let server_address = Self::get_random_server_address();

        Self {
            envs,
            child_handle: None,
            server_address,
            stdout_file_path: None,
            stderr_file_path: None,
            server_executable_path,
        }
    }

    pub fn start(&mut self) {
        self.envs
            .entry("IGGY_MCP_HTTP_ADDRESS".to_string())
            .or_insert(self.server_address.to_string());
        let mut command = if let Some(server_executable_path) = &self.server_executable_path {
            Command::new(server_executable_path)
        } else {
            Command::cargo_bin("iggy-mcp").unwrap()
        };
        command.envs(self.envs.clone());

        // By default, MCP server logs are redirected to files,
        // and dumped to stderr when test fails. With IGGY_TEST_VERBOSE=1
        // logs are dumped to stdout during test execution.
        if std::env::var(TEST_VERBOSITY_ENV_VAR).is_ok()
            || self.envs.contains_key(TEST_VERBOSITY_ENV_VAR)
        {
            command.stdout(Stdio::inherit());
            command.stderr(Stdio::inherit());
        } else {
            command.stdout(self.get_stdout_file());
            self.stdout_file_path = Some(fs::canonicalize(self.get_stdout_file_path()).unwrap());
            command.stderr(self.get_stderr_file());
            self.stderr_file_path = Some(fs::canonicalize(self.get_stderr_file_path()).unwrap());
        }

        let child = command.spawn().unwrap();
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

            let _ = child_handle.wait();
        }
    }

    pub fn is_started(&self) -> bool {
        self.child_handle.is_some()
    }

    pub fn pid(&self) -> u32 {
        self.child_handle.as_ref().unwrap().id()
    }

    fn get_http_mcp_api_address(&self) -> String {
        format!("{}{MCP_PATH}", self.get_http_api_address())
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
        let max_retries = 3000;
        let mut retries = 0;
        while let Err(error) = client.get(&http_api_address).send().await {
            sleep(Duration::from_millis(20)).await;
            retries += 1;
            if retries >= max_retries {
                panic!(
                    "Failed to ping MCP server: {http_api_address} after {max_retries} retries. {error}"
                );
            }
        }
        println!("MCP server address started at: {http_api_address}");
    }

    pub async fn get_client(&self) -> McpClient {
        let mcp_http_api_address = self.get_http_mcp_api_address();
        let transport = StreamableHttpClientTransport::from_uri(mcp_http_api_address);
        let client_info = ClientInfo {
            protocol_version: Default::default(),
            capabilities: ClientCapabilities::default(),
            client_info: Implementation {
                name: "test-mcp-client".to_string(),
                version: "1.0.0".to_string(),
                ..Default::default()
            },
        };
        client_info
            .serve(transport)
            .await
            .inspect_err(|error| {
                eprintln!("MCP client error: {error}");
            })
            .expect("Failed to create MCP client")
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

    fn get_stdout_file_path(&self) -> String {
        format!("/tmp/iggy-mcp-{}.stdout", self.server_address.port())
    }

    fn get_stderr_file_path(&self) -> String {
        format!("/tmp/iggy-mcp-{}.stderr", self.server_address.port())
    }

    fn get_stdout_file(&self) -> Stdio {
        Stdio::from(File::create(self.get_stdout_file_path()).unwrap())
    }

    fn get_stderr_file(&self) -> Stdio {
        Stdio::from(File::create(self.get_stderr_file_path()).unwrap())
    }

    fn read_file_to_string(path: &str) -> String {
        fs::read_to_string(path).unwrap_or_else(|_| format!("Failed to read file: {}", path))
    }
}

impl Drop for TestMcpServer {
    fn drop(&mut self) {
        self.stop();

        if panicking() {
            if let Some(stdout_file_path) = &self.stdout_file_path {
                eprintln!(
                    "Iggy MCP server stdout:\n{}",
                    Self::read_file_to_string(stdout_file_path.to_str().unwrap())
                );
            }

            if let Some(stderr_file_path) = &self.stderr_file_path {
                eprintln!(
                    "Iggy MCP server stderr:\n{}",
                    Self::read_file_to_string(stderr_file_path.to_str().unwrap())
                );
            }
        }

        // Clean up log files
        if let Some(stdout_file_path) = &self.stdout_file_path {
            fs::remove_file(stdout_file_path).unwrap();
        }

        if let Some(stderr_file_path) = &self.stderr_file_path {
            fs::remove_file(stderr_file_path).unwrap();
        }
    }
}
