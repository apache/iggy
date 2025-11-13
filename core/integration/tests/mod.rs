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
use lazy_static::lazy_static;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::{panic, thread};

mod cli;
mod config_provider;
mod connectors;
mod data_integrity;
mod mcp;
mod sdk;
mod server;
mod state;
mod streaming;

lazy_static! {
    static ref TESTS_FAILED: AtomicBool = AtomicBool::new(false);
    static ref LOGS_BUFFER: Arc<RwLock<HashMap<String, Vec<u8>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    static ref FAILED_TEST_CASES: Arc<RwLock<HashSet<String>>> =
        Arc::new(RwLock::new(HashSet::new()));
}

static INIT: Once = Once::new();
static UNKNOWN_TEST_NAME: &str = "unknown";

fn setup() {
    let log_buffer = LOGS_BUFFER.clone();

    let mut logger = env_logger::builder();
    logger.is_test(true);
    logger.filter(None, log::LevelFilter::Info);
    logger.target(env_logger::Target::Pipe(Box::new(LogWriter(log_buffer))));
    logger.format(move |buf, record| {
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.6f");
        let level = record.level();

        writeln!(buf, "{timestamp} {level:>5} - {}", record.args())
    });
    logger.init();

    // Set up a custom panic hook to catch test failures
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        // store failed test name
        let thread = thread::current();
        let thread_name = thread.name().unwrap_or(UNKNOWN_TEST_NAME);
        let failed_tests = FAILED_TEST_CASES.clone();
        failed_tests
            .write()
            .unwrap()
            .insert(thread_name.to_string());

        // If a test panics, set the failure flag to true
        TESTS_FAILED.store(true, Ordering::SeqCst);

        // Call the default panic hook to continue normal behavior
        default_hook(info);
    }));
}

struct LogWriter(Arc<RwLock<HashMap<String, Vec<u8>>>>);

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let thread = thread::current();
        let thread_name = thread.name().unwrap_or(UNKNOWN_TEST_NAME);
        let mut map = self.0.write().unwrap();

        let buffer = map.entry(thread_name.to_string()).or_default();
        buffer.extend_from_slice(buf);

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn teardown() {
    if TESTS_FAILED.load(Ordering::SeqCst)
        && let Ok(buffer) = LOGS_BUFFER.read()
        && let Ok(failed) = FAILED_TEST_CASES.read()
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
