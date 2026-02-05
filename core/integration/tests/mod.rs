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

use ctor::{ctor, dtor};
use integration::harness::get_test_directory;
use lazy_static::lazy_static;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once, RwLock};
use std::{panic, thread};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

mod cli;
mod cluster;
mod config_provider;
mod connectors;
mod data_integrity;
mod mcp;
mod sdk;
mod server;
mod state;

lazy_static! {
    static ref TESTS_FAILED: AtomicBool = AtomicBool::new(false);
    static ref LOGS_BUFFER: Arc<RwLock<HashMap<String, Vec<u8>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    static ref FAILED_TEST_CASES: Arc<RwLock<HashSet<String>>> =
        Arc::new(RwLock::new(HashSet::new()));
    /// File handles for real-time log writing, keyed by test name.
    static ref LOG_FILES: Arc<Mutex<HashMap<String, File>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

static INIT: Once = Once::new();
static UNKNOWN_TEST_NAME: &str = "unknown";

fn setup() {
    let log_buffer = LOGS_BUFFER.clone();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_thread_names(true)
                .with_writer(ThreadBufferWriter(log_buffer)),
        )
        .init();

    let panic_buffer = LOGS_BUFFER.clone();
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let thread = thread::current();
        let thread_name = thread.name().unwrap_or(UNKNOWN_TEST_NAME);

        if let Ok(mut map) = panic_buffer.write() {
            let buffer = map.entry(thread_name.to_string()).or_default();
            let panic_msg = format!("\nPANIC: {info}\n");
            buffer.extend_from_slice(panic_msg.as_bytes());
        }

        let failed_tests = FAILED_TEST_CASES.clone();
        if let Ok(mut failed) = failed_tests.write() {
            failed.insert(thread_name.to_string());
        }

        TESTS_FAILED.store(true, Ordering::SeqCst);

        default_hook(info);
    }));
}

struct ThreadBufferWriter(Arc<RwLock<HashMap<String, Vec<u8>>>>);

impl<'a> MakeWriter<'a> for ThreadBufferWriter {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(self.0.clone())
    }
}

struct LogWriter(Arc<RwLock<HashMap<String, Vec<u8>>>>);

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let thread = thread::current();
        let thread_name = thread.name().unwrap_or(UNKNOWN_TEST_NAME);

        // Write to memory buffer (for failed test output at end)
        if let Ok(mut map) = self.0.write() {
            let buffer = map.entry(thread_name.to_string()).or_default();
            buffer.extend_from_slice(buf);
        }

        // Write to file in real-time
        write_to_log_file(thread_name, buf);

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn write_to_log_file(test_name: &str, buf: &[u8]) {
    let Ok(mut files) = LOG_FILES.lock() else {
        return;
    };

    // Try to get existing file handle or create one
    if let std::collections::hash_map::Entry::Vacant(entry) = files.entry(test_name.to_string())
        && let Some(dir) = get_test_directory(test_name)
    {
        let log_path = dir.join("test_stdout.log");
        if let Ok(file) = OpenOptions::new().create(true).append(true).open(&log_path) {
            entry.insert(file);
        }
    }

    // Write to file if we have a handle
    if let Some(file) = files.get_mut(test_name) {
        let _ = file.write_all(buf);
    }
}

fn teardown() {
    // Flush and close all log file handles
    if let Ok(mut files) = LOG_FILES.lock() {
        for (_, file) in files.iter_mut() {
            let _ = file.flush();
        }
        files.clear();
    }

    // Print logs for failed tests to stderr
    if TESTS_FAILED.load(Ordering::SeqCst)
        && let (Ok(buffer), Ok(failed)) = (LOGS_BUFFER.read(), FAILED_TEST_CASES.read())
    {
        for test in failed.iter() {
            if let Some(logs) = buffer.get(test) {
                eprintln!("Logs for failed test '{test}':");
                eprintln!("{}", String::from_utf8_lossy(logs));
            }
        }
    }
}

#[ctor]
fn before_all_tests() {
    INIT.call_once(|| {
        setup();
    });
}

#[dtor]
fn after_all_tests() {
    teardown();
}
