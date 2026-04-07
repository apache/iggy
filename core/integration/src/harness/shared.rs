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

use crate::harness::config::TestServerConfig;
use crate::harness::context::TestContext;
use crate::harness::error::TestBinaryError;
use crate::harness::handle::ServerHandle;
use crate::harness::traits::TestBinary;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::sync::OnceCell;

/// Shared server state: addresses, handles, and active user count.
///
/// Rust statics are not dropped at process exit, so we use an atomic counter
/// to track active tests. The last test to finish stops the server and
/// cleans up the test directory.
pub struct SharedServerInfo {
    tcp_addr: Option<SocketAddr>,
    http_addr: Option<SocketAddr>,
    quic_addr: Option<SocketAddr>,
    websocket_addr: Option<SocketAddr>,
    tls_ca_cert_path: Option<PathBuf>,
    handle: std::sync::Mutex<Option<ServerHandle>>,
    context: Arc<TestContext>,
    active_count: AtomicUsize,
    any_test_failed: AtomicBool,
}

impl SharedServerInfo {
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.tcp_addr
    }

    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.http_addr
    }

    pub fn quic_addr(&self) -> Option<SocketAddr> {
        self.quic_addr
    }

    pub fn websocket_addr(&self) -> Option<SocketAddr> {
        self.websocket_addr
    }

    pub fn tls_ca_cert_path(&self) -> Option<&PathBuf> {
        self.tls_ca_cert_path.as_ref()
    }

    /// Increment the active test count. Called when a test acquires this server.
    pub fn acquire(&self) {
        self.active_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Mark that a test using this server has failed.
    /// Preserves the test directory for debugging.
    pub fn mark_failed(&self) {
        self.any_test_failed.store(true, Ordering::Release);
    }

    /// Decrement the active test count. When it reaches zero, stops the server
    /// and cleans up the test directory (unless any test failed).
    pub fn release(&self) {
        let prev = self.active_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            if let Ok(mut guard) = self.handle.lock() {
                if let Some(ref mut server) = *guard {
                    let _ = server.stop();
                }
                *guard = None;
            }
            if !self.any_test_failed.load(Ordering::Acquire) {
                self.context.cleanup();
            }
        }
    }
}

static REGISTRY: LazyLock<DashMap<String, Arc<OnceCell<Arc<SharedServerInfo>>>>> =
    LazyLock::new(DashMap::new);

/// Global registry for shared server instances.
///
/// Tests with the same `shared_server` key share a single iggy-server process.
/// The first test to request a key starts the server; all subsequent tests
/// get a reference to the already-running server. The last test to finish
/// stops the server and cleans up the test directory.
pub struct SharedServerRegistry;

impl SharedServerRegistry {
    /// Get or start a shared server for the given key.
    ///
    /// Thread-safe: concurrent callers for the same key will block until the
    /// first caller finishes initialization, then all receive the same `Arc`.
    ///
    /// Callers must call `SharedServerInfo::acquire()` after obtaining the
    /// reference and `SharedServerInfo::release()` when done (typically in
    /// `TestHarness::stop()`).
    pub async fn get_or_start(
        key: &str,
        config: TestServerConfig,
    ) -> Result<Arc<SharedServerInfo>, TestBinaryError> {
        let cell = REGISTRY
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .value()
            .clone();

        cell.get_or_try_init(|| async move {
            let mut context = TestContext::new(Some(format!("shared_server_{key}")), true)?;
            context.ensure_created()?;
            let context = Arc::new(context);

            let mut server = ServerHandle::with_config(config, context.clone());
            server.start()?;

            Ok(Arc::new(SharedServerInfo {
                tcp_addr: server.tcp_addr(),
                http_addr: server.http_addr(),
                quic_addr: server.quic_addr(),
                websocket_addr: server.websocket_addr(),
                tls_ca_cert_path: server.tls_ca_cert_path(),
                handle: std::sync::Mutex::new(Some(server)),
                context,
                active_count: AtomicUsize::new(0),
                any_test_failed: AtomicBool::new(false),
            }))
        })
        .await
        .cloned()
    }
}
