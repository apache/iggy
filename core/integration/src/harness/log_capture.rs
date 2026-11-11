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

//! Per-test log capture for a test binary that uses the harness.
//!
//! A test binary installs it once, at its crate root:
//!
//! ```ignore
//! integration::install_log_capture!();
//! ```
//!
//! The macro expands to a `ctor` constructor and a `dtor` destructor, so the
//! calling crate depends on `ctor` and `dtor`. Each test thread writes its
//! tracing output to `test_stdout.log` in its test directory. At exit, the
//! logs of each failed test go to stderr.

use super::context::{TEST_STDOUT_LOG_FILE, get_test_directory};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, Once, RwLock};
use std::{io, panic, thread};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

#[macro_export]
macro_rules! install_log_capture {
    () => {
        #[::ctor::ctor(unsafe)]
        fn before_all_tests() {
            $crate::harness::log_capture::install();
        }

        #[::dtor::dtor(unsafe)]
        fn after_all_tests() {
            $crate::harness::log_capture::finish();
        }
    };
}

static TESTS_FAILED: AtomicBool = AtomicBool::new(false);
static LOGS_BUFFER: LazyLock<RwLock<HashMap<String, Vec<u8>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static FAILED_TEST_CASES: LazyLock<RwLock<HashSet<String>>> =
    LazyLock::new(|| RwLock::new(HashSet::new()));
static SAVED_LOG_TESTS: LazyLock<RwLock<HashSet<String>>> =
    LazyLock::new(|| RwLock::new(HashSet::new()));
/// File handles for real-time log writing, keyed by test name.
static LOG_FILES: LazyLock<Mutex<HashMap<String, File>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static INIT: Once = Once::new();
static UNKNOWN_TEST_NAME: &str = "unknown";

/// Installs the tracing subscriber and the panic hook, once per process.
pub fn install() {
    INIT.call_once(setup);
}

/// Makes the failure logs of the calling test land in its `test_stdout.log`
/// instead of on stderr. Call it on the thread of the test.
pub fn use_saved_logs_on_failure() {
    let thread = thread::current();
    if let Some(test_name) = thread.name()
        && let Ok(mut tests) = SAVED_LOG_TESTS.write()
    {
        tests.insert(test_name.to_owned());
    }
}

/// Flushes the log files and reports the logs of each failed test.
pub fn finish() {
    if let Ok(mut files) = LOG_FILES.lock() {
        for file in files.values_mut() {
            let _ = file.flush();
        }
        files.clear();
    }

    if TESTS_FAILED.load(Ordering::SeqCst)
        && let (Ok(buffer), Ok(failed)) = (LOGS_BUFFER.read(), FAILED_TEST_CASES.read())
    {
        for test in failed.iter() {
            if let Some(logs) = buffer.get(test) {
                if SAVED_LOG_TESTS
                    .read()
                    .is_ok_and(|tests| tests.contains(test))
                    && let Some(directory) = get_test_directory(test)
                {
                    let path = directory.join(TEST_STDOUT_LOG_FILE);
                    // The panic hook adds evidence to the buffer after the last tracing write.
                    if path.is_file() && fs::write(&path, logs).is_ok() {
                        eprintln!("Logs for failed test '{test}': {}", path.display());
                        continue;
                    }
                }
                eprintln!("Logs for failed test '{test}':");
                eprintln!("{}", String::from_utf8_lossy(logs));
            }
        }
    }
}

fn setup() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_thread_names(true)
                .with_writer(|| LogWriter),
        )
        .init();

    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let thread = thread::current();
        let thread_name = thread.name().unwrap_or(UNKNOWN_TEST_NAME);

        if let Ok(mut map) = LOGS_BUFFER.write() {
            let buffer = map.entry(thread_name.to_string()).or_default();
            let panic_msg = format!("\nPANIC: {info}\n");
            buffer.extend_from_slice(panic_msg.as_bytes());
        }

        if let Ok(mut failed) = FAILED_TEST_CASES.write() {
            failed.insert(thread_name.to_string());
        }

        TESTS_FAILED.store(true, Ordering::SeqCst);

        default_hook(info);
    }));
}

struct LogWriter;

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let thread = thread::current();
        let thread_name = thread.name().unwrap_or(UNKNOWN_TEST_NAME);

        // The buffer feeds the failure report in `finish`.
        if let Ok(mut map) = LOGS_BUFFER.write() {
            let buffer = map.entry(thread_name.to_string()).or_default();
            buffer.extend_from_slice(buf);
        }

        write_to_log_file(thread_name, buf);

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn write_to_log_file(test_name: &str, buf: &[u8]) {
    let Ok(mut files) = LOG_FILES.lock() else {
        return;
    };

    if let std::collections::hash_map::Entry::Vacant(entry) = files.entry(test_name.to_string())
        && let Some(dir) = get_test_directory(test_name)
    {
        let log_path = dir.join(TEST_STDOUT_LOG_FILE);
        if let Ok(file) = OpenOptions::new().create(true).append(true).open(&log_path) {
            entry.insert(file);
        }
    }

    if let Some(file) = files.get_mut(test_name) {
        let _ = file.write_all(buf);
    }
}
