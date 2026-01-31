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

use crate::harness::config::{IpAddrKind, TestServerConfig};
use crate::harness::context::TestContext;
use crate::harness::error::TestBinaryError;
use crate::harness::port_reserver::PortReserver;
use crate::harness::traits::{Restartable, TestBinary};
use assert_cmd::prelude::CommandCargoExt;
use iggy::prelude::DEFAULT_ROOT_PASSWORD;
use iggy::prelude::DEFAULT_ROOT_USERNAME;
use rand::Rng as _;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle, available_parallelism, sleep};
use std::time::{Duration, Instant};

const SLEEP_INTERVAL_MS: u64 = 20;
const MAX_PORT_WAIT_DURATION_S: u64 = 60;
const TEST_VERBOSITY_ENV_VAR: &str = "IGGY_TEST_VERBOSE";

#[derive(Debug, Clone)]
struct ServerProtocolAddr {
    tcp: Option<SocketAddr>,
    http: Option<SocketAddr>,
    quic: Option<SocketAddr>,
    websocket: Option<SocketAddr>,
}

impl ServerProtocolAddr {
    fn empty() -> Self {
        Self {
            tcp: None,
            http: None,
            quic: None,
            websocket: None,
        }
    }
}

pub struct ServerHandle {
    server_id: u32,
    config: TestServerConfig,
    context: Arc<TestContext>,
    envs: HashMap<String, String>,
    child_handle: Option<Child>,
    addrs: ServerProtocolAddr,
    stdout_path: Option<PathBuf>,
    stderr_path: Option<PathBuf>,
    watchdog_handle: Option<JoinHandle<()>>,
    watchdog_stop: Arc<AtomicBool>,
    generated_cert_dir: Option<PathBuf>,
    port_reserver: Option<PortReserver>,
}

impl std::fmt::Debug for ServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerHandle")
            .field("addrs", &self.addrs)
            .field("cleanup", &self.config.cleanup)
            .field("is_running", &self.child_handle.is_some())
            .finish_non_exhaustive()
    }
}

impl ServerHandle {
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.addrs.tcp
    }

    pub fn raw_tcp_addr(&self) -> Option<String> {
        self.addrs.tcp.map(|addr| addr.to_string())
    }

    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.addrs.http
    }

    pub fn quic_addr(&self) -> Option<SocketAddr> {
        self.addrs.quic
    }

    pub fn websocket_addr(&self) -> Option<SocketAddr> {
        self.addrs.websocket
    }

    pub fn data_path(&self) -> PathBuf {
        self.context
            .base_dir()
            .join(format!("server_{}_local_data", self.server_id))
    }

    fn stdout_log_path(&self) -> PathBuf {
        self.context
            .base_dir()
            .join(format!("server_{}_stdout.log", self.server_id))
    }

    fn stderr_log_path(&self) -> PathBuf {
        self.context
            .base_dir()
            .join(format!("server_{}_stderr.log", self.server_id))
    }

    /// Get the TLS CA certificate path for client use.
    /// Returns Some only when TLS is configured with generated certs.
    pub fn tls_ca_cert_path(&self) -> Option<PathBuf> {
        self.generated_cert_dir
            .as_ref()
            .map(|dir| dir.join("test_cert.pem"))
    }

    /// Check if server has TLS enabled.
    pub fn has_tls(&self) -> bool {
        self.config.tls.is_some()
    }

    /// Check if server TLS uses self-signed certificates (no CA available).
    pub fn has_self_signed_tls(&self) -> bool {
        self.config.tls.as_ref().is_some_and(|tls| tls.self_signed)
    }

    pub fn collect_logs(&self) -> (String, String) {
        super::common::collect_logs(&self.stdout_path, &self.stderr_path)
    }

    fn build_envs(&mut self) -> Result<(), TestBinaryError> {
        // Pass through IGGY_* env vars from parent process, except those critical for test isolation.
        const PROTECTED_PREFIXES: &[&str] = &[
            "IGGY_SYSTEM_PATH",
            "IGGY_TCP_ADDRESS",
            "IGGY_HTTP_ADDRESS",
            "IGGY_QUIC_ADDRESS",
            "IGGY_WEBSOCKET_ADDRESS",
        ];

        for (key, value) in std::env::vars() {
            if key.starts_with("IGGY_") && !PROTECTED_PREFIXES.iter().any(|p| key.starts_with(p)) {
                self.envs.insert(key, value);
            }
        }

        let cpu_allocation = match available_parallelism() {
            Ok(parallelism) => {
                let available_cpus = parallelism.get();
                if available_cpus >= 4 {
                    let mut rng = rand::rng();
                    let max_start = available_cpus - 4;
                    let start = rng.random_range(0..=max_start);
                    format!("{}..{}", start, start + 4)
                } else {
                    "all".to_string()
                }
            }
            Err(_) => "0..4".to_string(),
        };
        self.envs
            .entry("IGGY_SYSTEM_SHARDING_CPU_ALLOCATION".to_string())
            .or_insert(cpu_allocation);

        if self.config.ip_kind == IpAddrKind::V6 {
            self.envs
                .entry("IGGY_TCP_IPV6".to_string())
                .or_insert_with(|| "true".to_string());
        }

        self.envs
            .entry("IGGY_ROOT_USERNAME".to_string())
            .or_insert_with(|| DEFAULT_ROOT_USERNAME.to_string());
        self.envs
            .entry("IGGY_ROOT_PASSWORD".to_string())
            .or_insert_with(|| DEFAULT_ROOT_PASSWORD.to_string());

        let data_path = self.data_path();
        self.envs.insert(
            "IGGY_SYSTEM_PATH".to_string(),
            data_path.display().to_string(),
        );

        // Protocol enablement (special handling for defaults)
        if !self.config.quic_enabled {
            self.envs
                .entry("IGGY_QUIC_ENABLED".to_string())
                .or_insert_with(|| "false".to_string());
        }
        if !self.config.websocket_enabled {
            self.envs
                .entry("IGGY_WEBSOCKET_ENABLED".to_string())
                .or_insert_with(|| "false".to_string());
        }
        if !self.config.http_enabled {
            self.envs
                .entry("IGGY_HTTP_ENABLED".to_string())
                .or_insert_with(|| "false".to_string());
        }

        // Encryption (special handling for key injection)
        if let Some(ref enc) = self.config.encryption {
            self.envs
                .entry("IGGY_SYSTEM_ENCRYPTION_ENABLED".to_string())
                .or_insert_with(|| "true".to_string());
            self.envs
                .entry("IGGY_SYSTEM_ENCRYPTION_KEY".to_string())
                .or_insert_with(|| enc.key.clone());
        }

        if let Some(tls) = self.config.tls.clone() {
            self.set_tls_envs("TCP", &tls);
        }
        if let Some(tls) = self.config.websocket_tls.clone() {
            self.set_tls_envs("WEBSOCKET", &tls);
        }

        // Extra envs from config (includes resolved config paths from macro)
        for (k, v) in &self.config.extra_envs {
            self.envs.insert(k.clone(), v.clone());
        }

        self.set_protocol_addresses()
    }

    fn set_tls_envs(&mut self, protocol: &str, tls: &crate::harness::config::TlsConfig) {
        self.envs
            .entry(format!("IGGY_{protocol}_TLS_ENABLED"))
            .or_insert_with(|| "true".to_string());

        let self_signed_str = if tls.self_signed { "true" } else { "false" };
        self.envs
            .entry(format!("IGGY_{protocol}_TLS_SELF_SIGNED"))
            .or_insert_with(|| self_signed_str.to_string());

        if !tls.self_signed {
            let cert_dir = self.generated_cert_dir.as_ref().unwrap_or(&tls.cert_dir);
            self.envs
                .entry(format!("IGGY_{protocol}_TLS_CERT_FILE"))
                .or_insert_with(|| cert_dir.join("test_cert.pem").display().to_string());
            self.envs
                .entry(format!("IGGY_{protocol}_TLS_KEY_FILE"))
                .or_insert_with(|| cert_dir.join("test_key.pem").display().to_string());
        }
    }

    fn set_protocol_addresses(&mut self) -> Result<(), TestBinaryError> {
        let reserver = PortReserver::reserve(self.config.ip_kind, &self.config)?;
        let addrs = reserver.addresses();

        if let Some(tcp) = addrs.tcp {
            if !self.envs.contains_key("IGGY_TCP_ADDRESS") {
                self.envs
                    .insert("IGGY_TCP_ADDRESS".to_string(), tcp.to_string());
            }
            self.addrs.tcp = Some(tcp);
        }

        if let Some(http) = addrs.http {
            if !self.envs.contains_key("IGGY_HTTP_ADDRESS") {
                self.envs
                    .insert("IGGY_HTTP_ADDRESS".to_string(), http.to_string());
            }
            self.addrs.http = Some(http);
        }

        if let Some(quic) = addrs.quic {
            if !self.envs.contains_key("IGGY_QUIC_ADDRESS") {
                self.envs
                    .insert("IGGY_QUIC_ADDRESS".to_string(), quic.to_string());
            }
            self.addrs.quic = Some(quic);
        }

        if let Some(websocket) = addrs.websocket {
            if !self.envs.contains_key("IGGY_WEBSOCKET_ADDRESS") {
                self.envs
                    .insert("IGGY_WEBSOCKET_ADDRESS".to_string(), websocket.to_string());
            }
            self.addrs.websocket = Some(websocket);
        }

        self.port_reserver = Some(reserver);
        Ok(())
    }

    fn wait_for_server_ready(&mut self) -> Result<(), TestBinaryError> {
        let data_path = self.data_path();
        let config_path = data_path.join("runtime/current_config.toml");

        let max_attempts = (MAX_PORT_WAIT_DURATION_S * 1000) / SLEEP_INTERVAL_MS;
        let deadline = Instant::now() + Duration::from_secs(MAX_PORT_WAIT_DURATION_S);

        for _ in 0..max_attempts {
            if let Some(child) = self.child_handle.as_mut()
                && let Ok(Some(status)) = child.try_wait()
            {
                let (stdout, stderr) = self.collect_logs();
                return Err(TestBinaryError::ProcessCrashed {
                    binary: "iggy-server".to_string(),
                    exit_code: status.code(),
                    stdout,
                    stderr,
                });
            }

            if config_path.exists() {
                // Server has written config file - it has bound to ports successfully.
                // Release pre-reserved ports so server has exclusive access.
                if let Some(reserver) = self.port_reserver.take() {
                    reserver.release();
                }
                return Ok(());
            }

            if Instant::now() >= deadline {
                break;
            }

            sleep(Duration::from_millis(SLEEP_INTERVAL_MS));
        }

        Err(TestBinaryError::StartupTimeout {
            binary: "iggy-server".to_string(),
            timeout_secs: MAX_PORT_WAIT_DURATION_S,
        })
    }

    fn start_watchdog(&mut self) {
        let Some(child) = &self.child_handle else {
            return;
        };
        let pid = child.id();
        let stop_signal = self.watchdog_stop.clone();
        let stdout_path = self.stdout_path.clone();
        let stderr_path = self.stderr_path.clone();

        let handle = thread::Builder::new()
            .name("test-server-watchdog".to_string())
            .spawn(move || {
                Self::watchdog_loop(pid, stop_signal, stdout_path, stderr_path);
            })
            .expect("Failed to spawn watchdog thread");

        self.watchdog_handle = Some(handle);
    }

    fn watchdog_loop(
        pid: u32,
        stop_signal: Arc<AtomicBool>,
        stdout_path: Option<PathBuf>,
        stderr_path: Option<PathBuf>,
    ) {
        const CHECK_INTERVAL: Duration = Duration::from_millis(100);

        loop {
            if stop_signal.load(Ordering::SeqCst) {
                return;
            }

            if !super::common::is_process_alive(pid) {
                let stdout_content = stdout_path
                    .as_ref()
                    .and_then(|p| fs::read_to_string(p).ok())
                    .unwrap_or_else(|| "[No stdout log]".to_string());

                let stderr_content = stderr_path
                    .as_ref()
                    .and_then(|p| fs::read_to_string(p).ok())
                    .unwrap_or_else(|| "[No stderr log]".to_string());

                panic!(
                    "Server process (PID {}) has died unexpectedly!\n\
                     === STDOUT ===\n{}\n\n\
                     === STDERR ===\n{}",
                    pid, stdout_content, stderr_content
                );
            }

            thread::sleep(CHECK_INTERVAL);
        }
    }

    fn stop_watchdog(&mut self) {
        self.watchdog_stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.watchdog_handle.take() {
            let _ = handle.join();
        }
    }
}

impl TestBinary for ServerHandle {
    type Config = TestServerConfig;

    fn with_config(config: Self::Config, context: Arc<TestContext>) -> Self {
        Self {
            server_id: 0,
            config,
            context,
            envs: HashMap::new(),
            child_handle: None,
            addrs: ServerProtocolAddr::empty(),
            stdout_path: None,
            stderr_path: None,
            watchdog_handle: None,
            watchdog_stop: Arc::new(AtomicBool::new(false)),
            generated_cert_dir: None,
            port_reserver: None,
        }
    }

    fn start(&mut self) -> Result<(), TestBinaryError> {
        // Generate TLS certificates if configured for TCP or WebSocket
        let needs_cert_generation = self
            .config
            .tls
            .as_ref()
            .is_some_and(|tls| tls.generate_certs)
            || self
                .config
                .websocket_tls
                .as_ref()
                .is_some_and(|tls| tls.generate_certs);

        if needs_cert_generation {
            let cert_dir = self.context.base_dir().join("certs");
            fs::create_dir_all(&cert_dir).map_err(|e| TestBinaryError::FileSystemError {
                path: cert_dir.clone(),
                source: e,
            })?;
            crate::test_tls_utils::generate_test_certificates(cert_dir.to_str().unwrap()).map_err(
                |e| TestBinaryError::InvalidState {
                    message: format!("Failed to generate TLS certificates: {e}"),
                },
            )?;
            self.generated_cert_dir = Some(cert_dir);
        }

        self.build_envs()?;

        let data_path = self.data_path();
        let config_path = data_path.join("runtime/current_config.toml");
        if config_path.exists() {
            let _ = fs::remove_file(&config_path);
        }

        #[allow(deprecated)]
        let mut command = if let Some(ref path) = self.config.executable_path {
            Command::new(path)
        } else {
            Command::cargo_bin("iggy-server").map_err(|e| TestBinaryError::ProcessSpawn {
                binary: "iggy-server".to_string(),
                source: std::io::Error::other(e.to_string()),
            })?
        };

        command.env("IGGY_SYSTEM_PATH", data_path.display().to_string());
        command.envs(&self.envs);

        let verbose = std::env::var(TEST_VERBOSITY_ENV_VAR).is_ok()
            || self.envs.contains_key(TEST_VERBOSITY_ENV_VAR);

        if verbose {
            command.stdout(Stdio::inherit());
            command.stderr(Stdio::inherit());
        } else {
            let stdout_path = self.stdout_log_path();
            let stderr_path = self.stderr_log_path();

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
            binary: "iggy-server".to_string(),
            source: e,
        })?;
        self.child_handle = Some(child);
        self.watchdog_stop = Arc::new(AtomicBool::new(false));

        self.wait_for_server_ready()?;
        self.start_watchdog();

        Ok(())
    }

    fn stop(&mut self) -> Result<(), TestBinaryError> {
        self.stop_watchdog();

        if let Some(child) = self.child_handle.take() {
            let child = super::common::graceful_kill(child);

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
        self.pid().is_some_and(super::common::is_process_alive)
    }

    fn assert_running(&self) {
        if let Some(pid) = self.pid().filter(|&p| !super::common::is_process_alive(p)) {
            let (stdout, stderr) = self.collect_logs();
            panic!(
                "Server process (pid {}) has crashed\n\n\
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

impl Restartable for ServerHandle {
    fn restart(&mut self) -> Result<(), TestBinaryError> {
        let cleanup = self.config.cleanup;
        self.config.cleanup = false;
        self.stop()?;
        self.config.cleanup = cleanup;
        self.start()
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let _ = self.stop();
        super::common::dump_logs_on_panic("Iggy server", &self.stdout_path, &self.stderr_path);
    }
}
