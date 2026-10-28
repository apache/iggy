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

use bytes::Bytes;
use compio::BufResult;
use compio::fs::File;
use compio::io::AsyncReadAtExt;
use std::cell::Cell;
use std::future::poll_fn;
use std::io;
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};

/// A file body is read and sent one piece at a time, so an upload holds one piece in memory
/// whatever the object's size.
pub(crate) const PIECE_LEN: u64 = 1024 * 1024;

pub enum PutBody {
    Bytes(Bytes),
    /// `len` bytes of `file` starting at `offset`, such as a sealed segment. The file must hold
    /// them all, a short file fails the put with [`crate::ObjectStoreError::Source`]. With a
    /// `pacer`, each piece waits for its turn under the pacer's byte rate before it is read.
    File {
        file: File,
        offset: u64,
        len: u64,
        pacer: Option<Rc<UploadPacer>>,
    },
}

impl PutBody {
    pub fn len(&self) -> u64 {
        match self {
            Self::Bytes(bytes) => bytes.len() as u64,
            Self::File { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Caps the byte rate of the file bodies that share it, one piece at a time.
pub struct UploadPacer {
    bytes_per_second: u64,
    next_piece_at: Cell<Instant>,
}

impl UploadPacer {
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second: bytes_per_second.max(1),
            next_piece_at: Cell::new(Instant::now()),
        }
    }

    /// Waits until `len` more bytes fit under the rate. Idle time earns no burst.
    async fn admit(&self, len: u64) {
        let now = Instant::now();
        let start = self.next_piece_at.get().max(now);
        let nanos = u128::from(len) * 1_000_000_000 / u128::from(self.bytes_per_second);
        let spacing = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
        self.next_piece_at.set(start + spacing);
        if start > now {
            compio::time::sleep(start - now).await;
        }
    }
}

/// CRC32C of `len` bytes of `file` starting at `offset`, read one piece at a time with a yield
/// between pieces, so hashing a large segment never holds the shard for long.
///
/// # Errors
/// Returns the read error, or `UnexpectedEof` when the file holds fewer bytes.
pub async fn file_crc32c(file: &File, offset: u64, len: u64) -> io::Result<u32> {
    let mut checksum = 0;
    let mut read = 0;
    while read < len {
        let piece = read_piece(file, offset, len, read, None).await?;
        checksum = crc32c::crc32c_append(checksum, &piece);
        read += piece.len() as u64;
        yield_now().await;
    }
    Ok(checksum)
}

/// Reads the piece of `len` bytes at `sent` bytes into a body that starts at `offset`.
pub(crate) async fn read_piece(
    file: &File,
    offset: u64,
    len: u64,
    sent: u64,
    pacer: Option<&UploadPacer>,
) -> io::Result<Bytes> {
    let piece = (len - sent).min(PIECE_LEN);
    if let Some(pacer) = pacer {
        pacer.admit(piece).await;
    }
    // `read_exact_at` fills the spare capacity, so the buffer starts empty.
    let BufResult(result, buffer) = file
        .read_exact_at(Vec::with_capacity(piece as usize), offset + sent)
        .await;
    result.map(|()| Bytes::from(buffer))
}

async fn yield_now() {
    let mut yielded = false;
    poll_fn(|context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[compio::test]
    async fn given_file_range_when_hashing_should_match_one_shot_crc32c() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("segment");
        let bytes: Vec<u8> = (0..PIECE_LEN * 2 + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        std::fs::write(&path, &bytes).unwrap();
        let file = File::open(&path).await.unwrap();

        let whole = file_crc32c(&file, 0, bytes.len() as u64).await.unwrap();
        let tail = file_crc32c(&file, 5, bytes.len() as u64 - 5).await.unwrap();
        let past_end = file_crc32c(&file, 0, bytes.len() as u64 + 1).await;

        assert_eq!(whole, crc32c::crc32c(&bytes));
        assert_eq!(tail, crc32c::crc32c(&bytes[5..]));
        assert_eq!(past_end.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

    #[compio::test]
    async fn given_pacer_when_admitting_pieces_should_space_them_by_the_rate() {
        const BYTES_PER_SECOND: u64 = 10 * 1024 * 1024;
        let pacer = UploadPacer::new(BYTES_PER_SECOND);
        let started = Instant::now();

        for _ in 0..3 {
            pacer.admit(1024 * 1024).await;
        }

        // The first piece goes at once, the next two wait 100 ms each.
        assert!(started.elapsed() >= Duration::from_millis(190));
    }
}
