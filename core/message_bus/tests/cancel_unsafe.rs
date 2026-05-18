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

//! Regression test pinning the compio cancel-unsafe property in CI.
//!
//! ## Why this test must never be deleted
//!
//! It empirically demonstrates that `compio`'s `io_uring` `read*`
//! futures are NOT cancel-safe: dropping a mid-flight `read_exact`
//! future loses bytes that the kernel has already DMA'd into the
//! caller-owned buffer.
//!
//! Sources:
//!   - `compio-runtime-0.11.0/src/runtime/future.rs:163-169`
//!     (`Submit::drop` posts fire-and-forget `IORING_OP_ASYNC_CANCEL`).
//!   - `compio-driver-0.11.4/src/lib.rs:135-148`
//!     (`AsyncifyPool::dispatch` doc: "the cancellation is not reliable.
//!     The underlying operation may continue.").
//!   - `compio-driver-0.11.4/src/sys/iour/mod.rs:282-301`
//!     (cancel SQE submitted with no completion wait).
//!
//! The bus's transports structurally avoid the trap by never
//! `select!`-ing over a TCP read on a still-alive connection. If a
//! future refactor of the IO model removes that guard, this test stays
//! green only because the underlying compio behaviour stays unchanged;
//! the protective code path is the bus's responsibility, not compio's.
//!
//! If THIS test ever flakes or starts passing under the cancel-safe
//! verdict, the IO model assumptions across the bus must be re-audited
//! before any `select!` over a read can be reintroduced.
//!
//! Mechanics:
//!   1. Loopback TCP pair.
//!   2. Sender writes 200 'A' bytes, sleeps 150 ms, writes 56 'B' bytes.
//!   3. Receiver wraps `read_exact(buf, 256)` in a `select!` with a
//!      50 ms timer. Timer wins; the `read_exact` future is dropped.
//!   4. Receiver re-issues `read_exact(buf, 256)` on the same stream.
//!   5. Assert: phase 2 returns `Err(UnexpectedEof)` (bytes lost) and
//!      the bytes that DO arrive in phase 2 are 'B' bytes only - the
//!      200 'A' bytes that were DMA'd-into-the-dropped-buffer are gone.

use std::io::ErrorKind;
use std::time::Duration;

use compio::io::{AsyncReadExt, AsyncWriteExt};
use compio::net::{TcpListener, TcpStream};
use futures::FutureExt;

const N1: usize = 200;
const N2: usize = 56;
const TOTAL: usize = N1 + N2;

#[compio::test]
async fn read_future_drop_loses_already_dma_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connect = TcpStream::connect(addr);
    let accept = listener.accept();
    let (client_res, accept_res) = futures::join!(connect, accept);
    let mut client = client_res.unwrap();
    let (mut server, _) = accept_res.unwrap();

    let sender = compio::runtime::spawn(async move {
        let buf1 = vec![b'A'; N1];
        client.write_all(buf1).await.0.unwrap();
        compio::time::sleep(Duration::from_millis(150)).await;
        let buf2 = vec![b'B'; N2];
        client.write_all(buf2).await.0.unwrap();
        // Drop closes the TCP write half so phase 2's read_exact sees EOF
        // once the cancelled bytes are accounted for.
    });

    let phase1_buf = vec![0u8; TOTAL];
    let phase1_timed_out = {
        let read_fut = server.read_exact(phase1_buf);
        let timer = compio::time::sleep(Duration::from_millis(50));
        futures::select! {
            res = read_fut.fuse() => {
                let (_, b) = (res.0, res.1);
                let _ = b;
                false
            }
            () = timer.fuse() => true,
        }
    };
    assert!(
        phase1_timed_out,
        "test premise broken: read_exact finished within the 50 ms window; \
         expected the 50 ms timer to fire while only the first 200 B had arrived",
    );

    let phase2_buf = vec![0u8; TOTAL];
    let phase2 = compio::time::timeout(Duration::from_secs(2), server.read_exact(phase2_buf)).await;

    let phase2_inner = phase2.expect(
        "phase 2 timed out: read_exact hung waiting for bytes. \
         Either compio became cancel-safe (verdict needs review) or the sender \
         did not close cleanly (test infrastructure regression).",
    );
    let (read_res, buf) = (phase2_inner.0, phase2_inner.1);
    let err = read_res.expect_err(
        "phase 2 returned Ok(()): all 256 bytes preserved across the dropped read_exact. \
         compio cancel semantics changed; re-audit the no-`select!`-over-reads guard \
         across the message_bus transports before relaxing it.",
    );
    assert_eq!(
        err.kind(),
        ErrorKind::UnexpectedEof,
        "phase 2 reported {err:?}; expected UnexpectedEof from the truncated stream",
    );

    let slice = buf.as_slice();
    let phase2_b_count = slice.iter().take_while(|&&b| b == b'B').count();
    assert!(
        phase2_b_count > 0 && phase2_b_count <= N2,
        "phase 2 read {phase2_b_count} 'B' bytes (expected 1..={N2}); \
         the 200 'A' bytes from the dropped read_exact should NOT reappear",
    );
    let head_has_a = slice[..phase2_b_count].contains(&b'A');
    assert!(
        !head_has_a,
        "phase 2 saw 'A' bytes that should have been lost with the dropped read_exact",
    );

    let _ = sender.await;
}
