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

//! `[system.segment] write_io = "uncached"` end to end: the boot probe must
//! refuse a data directory whose filesystem rejects `RWF_DONTCACHE`, and
//! segments flushed through the uncached ops must leave the page cache behind
//! them and read back through the regular path, live and after a restart
//! reopens them from disk.

use std::collections::HashMap;
use std::fs;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::{TestBinaryError, TestHarness, TestServerConfig, disk};
use serial_test::parallel;
use server_common::uncached_io::{
    UncachedIoError, probe_uncached_write, require_uncached_io_tests,
};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

const SHM_DIR: &str = "/dev/shm";
const REQUIRE_UNCACHED_IO_ENV: &str = "IGGY_REQUIRE_UNCACHED_IO";
const STREAM_NAME: &str = "uncached-stream";
const TOPIC_NAME: &str = "uncached-topic";
const PARTITION_ID: u32 = 0;
const BATCHES: u32 = 4;
const MESSAGES_PER_BATCH: u32 = 128;
/// Below the batch size, so every batch crosses the flush threshold.
const MESSAGES_REQUIRED_TO_SAVE: u32 = 64;
const PAYLOAD_LEN: usize = 2048;
/// Budget for the live flushes to land in the segment files.
const FLUSH_INSTALL_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Bound on one poll round trip: if the (restarted) server dies, the SDK's
/// reconnect loop would otherwise hang the test forever.
const POLL_TIMEOUT: Duration = Duration::from_secs(60);
/// Budget for the pages to leave the cache once writeback has been forced.
const PAGE_DROP_TIMEOUT: Duration = Duration::from_secs(10);
/// Share of the segment's pages allowed to remain resident, in percent.
/// Measured on ext4: 1 of 263 pages with `uncached` against 263 of 263 with
/// `buffered`. The slack covers the only residency `uncached` can leave
/// behind, a block no single flush wrote whole.
const MAX_RESIDENT_PERCENT: usize = 25;

/// The probe runs once at boot, so a system path on a filesystem that
/// rejects `RWF_DONTCACHE` (tmpfs here; every filesystem on kernels below
/// 6.14) must stop the server before it serves anything, with the reason and
/// the remedy in its output.
#[tokio::test]
#[parallel]
async fn given_uncached_write_io_when_the_system_path_is_on_tmpfs_should_refuse_to_boot() {
    let Some((system_path, expected_reason)) = tmpfs_system_path() else {
        return;
    };
    let mut envs = uncached_envs();
    envs.insert(
        "IGGY_SYSTEM_PATH".to_string(),
        system_path.path().display().to_string(),
    );
    let mut harness = TestHarness::builder()
        .cluster_nodes(1)
        .server(TestServerConfig::builder().extra_envs(envs).build())
        .build()
        .unwrap();

    let error = harness
        .start()
        .await
        .expect_err("boot with write_io = \"uncached\" over tmpfs must refuse");
    let (exit_code, stdout, stderr) = match error {
        TestBinaryError::ProcessCrashed {
            exit_code,
            stdout,
            stderr,
            ..
        } => (exit_code, stdout, stderr),
        other => panic!("the server must exit on its own rather than hang or serve: {other}"),
    };
    assert!(
        matches!(exit_code, Some(code) if code != 0),
        "the refusal must be a non-zero exit, got {exit_code:?}"
    );
    if logs_are_captured() {
        let diagnostics = format!("{stdout}\n{stderr}");
        assert!(
            diagnostics.contains(&expected_reason),
            "the refusal must carry the probe's reason {expected_reason:?}, got:\n{diagnostics}"
        );
        assert!(
            diagnostics.contains("UncachedWriteUnsupported"),
            "the refusal must surface as ServerError::UncachedWriteUnsupported, got:\n{diagnostics}"
        );
    }
}

/// Segments flushed through the `RWF_DONTCACHE` ops must leave the page cache
/// behind them and must be what the regular read path serves, while the writer
/// is live and after a restart reopens them from disk. Skipped where the data
/// directory's filesystem or the kernel cannot do uncached writes at all.
#[tokio::test]
#[parallel]
async fn given_uncached_write_io_when_the_server_restarts_should_serve_the_flushed_messages() {
    let mut harness = TestHarness::builder()
        .cluster_nodes(1)
        .server(
            TestServerConfig::builder()
                .extra_envs(uncached_envs())
                .build(),
        )
        .build()
        .unwrap();
    let data_path = harness.server().data_path();
    fs::create_dir_all(&data_path).expect("create the data directory for the probe");
    match probe_blocking(data_path.clone()) {
        Ok(()) => {}
        Err(error @ UncachedIoError::Unsupported { .. }) => {
            skip_or_fail(&error.to_string());
            return;
        }
        Err(error) => panic!(
            "probe under {} hit an I/O error: {error}",
            data_path.display()
        ),
    }

    harness.start().await.unwrap();
    assert_eq!(
        effective_write_io(&data_path),
        "uncached",
        "the env override must reach the server"
    );
    let client = harness
        .tcp_root_client()
        .await
        .expect("create TCP client for sending messages");
    create_stream_and_topic(&client).await;
    let payloads = send_batches(&client).await;
    let payload_bytes = payloads.iter().map(|payload| payload.len() as u64).sum();
    wait_until_segments_hold(&data_path, payload_bytes).await;
    // Before any poll: reading the segment back would fault its pages in
    // again and hide the drop this assertion is about.
    assert_segment_pages_left_the_cache(&data_path).await;
    assert_polled_payloads(&client, &payloads).await;
    drop(client);

    harness.restart_server().await.unwrap();
    let client = harness
        .tcp_root_client()
        .await
        .expect("create TCP client after restart");
    assert_polled_payloads(&client, &payloads).await;
}

fn uncached_envs() -> HashMap<String, String> {
    HashMap::from([(
        "IGGY_SYSTEM_SEGMENT_WRITE_IO".to_string(),
        "uncached".to_string(),
    )])
}

/// A fresh directory on tmpfs plus the probe's own reason for refusing it,
/// `None` (reason printed) when `/dev/shm` is not tmpfs or this kernel takes
/// `RWF_DONTCACHE` there.
fn tmpfs_system_path() -> Option<(TempDir, String)> {
    let dir = match tempfile::tempdir_in(SHM_DIR) {
        Ok(dir) => dir,
        Err(error) => {
            skip_or_fail(&format!(
                "cannot create a directory under {SHM_DIR}: {error}"
            ));
            return None;
        }
    };
    match probe_blocking(dir.path().to_path_buf()) {
        Err(UncachedIoError::Unsupported { filesystem, .. }) if filesystem != "tmpfs" => {
            skip_or_fail(&format!("{SHM_DIR} is {filesystem}, not tmpfs"));
            None
        }
        Err(error @ UncachedIoError::Unsupported { .. }) => Some((dir, error.to_string())),
        Ok(()) => {
            skip_or_fail("tmpfs takes RWF_DONTCACHE on this kernel");
            None
        }
        Err(error) => panic!("probe under {SHM_DIR} hit an I/O error: {error}"),
    }
}

/// libtest swallows the output of a passing test, so a test that skips itself
/// for good is invisible. Loud on a box that cannot do uncached writes, fatal
/// on one where they are required to have run.
fn skip_or_fail(reason: &str) {
    assert!(
        !require_uncached_io_tests(),
        "### {REQUIRE_UNCACHED_IO_ENV}=1 forbids skipping: {reason} ###"
    );
    eprintln!("######## SKIPPING UNCACHED TEST: {reason} ########");
}

/// The probe submits the segment writers' own io_uring op, so it needs a
/// compio runtime; these tests run on tokio.
fn probe_blocking(dir: PathBuf) -> Result<(), UncachedIoError> {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new()
            .expect("compio runtime")
            .block_on(probe_uncached_write(&dir))
    })
    .join()
    .expect("probe thread")
}

/// `IGGY_TEST_VERBOSE` inherits the server's stdio, leaving nothing captured
/// to assert on.
fn logs_are_captured() -> bool {
    std::env::var("IGGY_TEST_VERBOSE").is_err()
}

/// `system.segment.write_io` as the server re-serialized it into
/// `current_config.toml`. That file is output only, so this proves the env
/// override reached the configuration and nothing about the write path.
fn effective_write_io(data_path: &Path) -> String {
    let path = data_path.join("runtime/current_config.toml");
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let config: toml::Value = toml::from_str(&content)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    config["system"]["segment"]["write_io"]
        .as_str()
        .expect("system.segment.write_io is a string")
        .to_owned()
}

async fn create_stream_and_topic(client: &IggyClient) {
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                messages_required_to_save: Some(MESSAGES_REQUIRED_TO_SAVE),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
}

/// Sends `BATCHES` batches to the single partition, each past the flush
/// threshold, returning every payload in send order (offset order).
async fn send_batches(client: &IggyClient) -> Vec<String> {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let partitioning = Partitioning::partition_id(PARTITION_ID);
    let mut payloads = Vec::with_capacity((BATCHES * MESSAGES_PER_BATCH) as usize);
    for batch in 0..BATCHES {
        let mut messages = Vec::with_capacity(MESSAGES_PER_BATCH as usize);
        for index in 0..MESSAGES_PER_BATCH {
            let payload = payload_for(batch * MESSAGES_PER_BATCH + index);
            messages.push(
                IggyMessage::builder()
                    .payload(payload.clone().into())
                    .build()
                    .expect("build message"),
            );
            payloads.push(payload);
        }
        client
            .send_messages(&stream, &topic, &partitioning, &mut messages)
            .await
            .unwrap_or_else(|error| panic!("send batch {batch}: {error}"));
    }
    payloads
}

/// `PAYLOAD_LEN` bytes unique to `offset`: a tag, then filler that shifts
/// with the offset so a misplaced or repeated write cannot pass the
/// comparison.
fn payload_for(offset: u32) -> String {
    let tag = format!("uncached-{offset:05}-");
    let filler = (tag.len()..PAYLOAD_LEN)
        .map(|position| char::from(b'a' + ((position + offset as usize) % 26) as u8));
    tag.chars().chain(filler).collect()
}

/// Total bytes of partition segment `.log` files under `data_path`;
/// preallocation keeps the logical length at the written bytes.
fn segment_log_bytes(data_path: &Path) -> u64 {
    let mut total = 0;
    let _ = disk::walk(data_path, &mut |path| {
        if disk::is_segment_log(path) {
            total += fs::metadata(path).map_or(0, |meta| meta.len());
        }
        false
    });
    total
}

/// Waits until the segment files hold at least `min_bytes`. Callers pass the
/// payload bytes alone: headers come on top, so the bound is only met once
/// nearly every batch was flushed while the server was live, by the uncached
/// writer rather than the shutdown flush.
async fn wait_until_segments_hold(data_path: &Path, min_bytes: u64) {
    let deadline = tokio::time::Instant::now() + FLUSH_INSTALL_TIMEOUT;
    loop {
        let installed = segment_log_bytes(data_path);
        if installed >= min_bytes {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {installed} of at least {min_bytes} segment bytes reached disk within \
             {FLUSH_INSTALL_TIMEOUT:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The one behavioural check that `buffered` cannot pass: a segment written
/// with `RWF_DONTCACHE` loses its pages as writeback completes, while a
/// buffered one keeps every page it just wrote even after an fsync.
///
/// The drop is gated on writeback completing, not on the write returning, so
/// left alone the pages linger until the bdi flusher reaches them, which
/// `dirty_writeback_centisecs` alone can stretch past any sane budget on a
/// loaded box. Forcing the writeback removes that scheduling from the
/// assertion without weakening it: fsync evicts nothing by itself.
async fn assert_segment_pages_left_the_cache(data_path: &Path) {
    let log_path = disk::walk(data_path, &mut |path| disk::is_segment_log(path))
        .expect("the flushed batches must have created a segment .log");
    force_writeback(&log_path);
    let deadline = tokio::time::Instant::now() + PAGE_DROP_TIMEOUT;
    loop {
        let (resident, pages) = page_residency(&log_path);
        assert!(
            pages > 1,
            "{} holds {pages} page(s), too few to tell a dropped page from a partial tail",
            log_path.display()
        );
        if resident * 100 <= pages * MAX_RESIDENT_PERCENT {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{resident} of {pages} pages of {} are still in the page cache after \
             {PAGE_DROP_TIMEOUT:?}; uncached segment writes did not drop them",
            log_path.display()
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Pushes `path`'s dirty pages to the device and returns once they are clean.
/// Read-only so nothing here can dirty a page; `fsync` needs no write access.
fn force_writeback(path: &Path) {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .unwrap_or_else(|error| panic!("fsync {}: {error}", path.display()));
}

/// Pages of `path` the page cache still holds, and the file's page count.
/// `mincore` over a read-only mapping reports residency without faulting
/// anything in, so measuring does not change what it measures.
fn page_residency(path: &Path) -> (usize, usize) {
    let file =
        fs::File::open(path).unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
    let len = usize::try_from(
        file.metadata()
            .unwrap_or_else(|error| panic!("stat {}: {error}", path.display()))
            .len(),
    )
    .expect("segment length fits in usize");
    if len == 0 {
        return (0, 0);
    }
    // SAFETY: a fresh read-only mapping of a file held open for the whole
    // call; the pointer is only handed back to `mincore` and `munmap`.
    let (result, error, residency) = unsafe {
        let page_size = usize::try_from(libc::sysconf(libc::_SC_PAGESIZE)).expect("page size");
        let mut residency = vec![0u8; len.div_ceil(page_size)];
        let address = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        );
        assert!(
            address != libc::MAP_FAILED,
            "mmap {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
        let result = libc::mincore(address, len, residency.as_mut_ptr());
        let error = std::io::Error::last_os_error();
        libc::munmap(address, len);
        (result, error, residency)
    };
    assert_eq!(result, 0, "mincore {}: {error}", path.display());
    (
        residency.iter().filter(|page| *page & 1 == 1).count(),
        residency.len(),
    )
}

/// Polls the partition from offset 0 in pages and checks every payload sits
/// at its send-order offset.
async fn assert_polled_payloads(client: &IggyClient, expected: &[String]) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let mut polled = Vec::with_capacity(expected.len());
    while polled.len() < expected.len() {
        let page = timeout(
            POLL_TIMEOUT,
            client.poll_messages(
                &stream,
                &topic,
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::offset(polled.len() as u64),
                MESSAGES_PER_BATCH,
                false,
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "poll from offset {} did not return within {POLL_TIMEOUT:?}",
                polled.len()
            )
        })
        .expect("poll messages");
        assert!(
            !page.messages.is_empty(),
            "poll from offset {} returned nothing, {} of {} messages read back",
            polled.len(),
            polled.len(),
            expected.len()
        );
        polled.extend(page.messages);
    }
    assert_eq!(polled.len(), expected.len(), "more messages than were sent");
    for (offset, (message, payload)) in polled.iter().zip(expected).enumerate() {
        assert_eq!(message.header.offset, offset as u64);
        assert!(
            message.payload.as_ref() == payload.as_bytes(),
            "payload at offset {offset} differs from what was sent"
        );
    }
}
