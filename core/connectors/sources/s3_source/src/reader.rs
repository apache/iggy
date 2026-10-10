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

use std::{io, ops::ControlFlow};

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    batch::RecordBatch,
    config::ResolvedConfig,
    framing::{FramedRecord, FramingError, RecordFramer},
    state::ActiveObjectState,
};

const INPUT_SLICE_BYTES: usize = 64 * 1024;
const SCAN_BYTES_PER_POLL: u64 = 16 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct ReadBatch {
    pub(crate) records: Vec<FramedRecord>,
    pub(crate) next_offset: u64,
    pub(crate) complete: bool,
}

#[derive(Debug)]
pub(crate) enum ReadError {
    Io(io::Error),
    Framing(FramingError),
    InvalidLength,
}

pub(crate) async fn read_batch(
    body: impl AsyncRead + Unpin,
    object: &ActiveObjectState,
    config: &ResolvedConfig,
) -> Result<ReadBatch, ReadError> {
    read_with_budget(body, object, config, SCAN_BYTES_PER_POLL).await
}

async fn read_with_budget(
    mut body: impl AsyncRead + Unpin,
    object: &ActiveObjectState,
    config: &ResolvedConfig,
    scan_budget: u64,
) -> Result<ReadBatch, ReadError> {
    let start = object.next_byte_offset;
    // A restored EOF offset is not proof of completion. Validate a full GET,
    // without emitting already acknowledged bytes or issuing an invalid range.
    let verify_only = start == object.size;
    let expected_length = if verify_only {
        object.size
    } else {
        object.size - start
    };
    let mut framer = RecordFramer::new(start, config.delimiter.clone(), config.max_record_bytes);
    let mut batch = RecordBatch::new(config.max_batch_bytes, config.max_batch_messages);
    let mut buffer = Vec::with_capacity(INPUT_SLICE_BYTES);
    let mut received = 0_u64;

    loop {
        buffer.clear();
        let count = (&mut body)
            .take(INPUT_SLICE_BYTES as u64)
            .read_buf(&mut buffer)
            .await
            .map_err(ReadError::Io)?;
        if count == 0 {
            if received != expected_length {
                return Err(ReadError::InvalidLength);
            }
            if !verify_only
                && let Some(record) = framer.finish().map_err(ReadError::Framing)?
                && let Err(record) = batch.try_push(record)
            {
                return Ok(ReadBatch {
                    records: batch.records,
                    next_offset: record.start_offset,
                    complete: false,
                });
            }
            return Ok(ReadBatch {
                records: batch.records,
                next_offset: object.size,
                complete: true,
            });
        }

        received = received
            .checked_add(count as u64)
            .ok_or(ReadError::InvalidLength)?;
        if received > expected_length {
            return Err(ReadError::InvalidLength);
        }
        if verify_only {
            continue;
        }

        if let Some(next_offset) = framer
            .push_with(&buffer[..count], |record| match batch.try_push(record) {
                Ok(()) => ControlFlow::Continue(()),
                Err(_) => ControlFlow::Break(()),
            })
            .map_err(ReadError::Framing)?
        {
            return Ok(ReadBatch {
                records: batch.records,
                next_offset,
                complete: false,
            });
        }
        let next_offset = framer.consumed_offset();
        if received < expected_length
            && (batch.is_full() || received >= scan_budget)
            && next_offset > start
        {
            return Ok(ReadBatch {
                records: batch.records,
                next_offset,
                complete: false,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::ReadBuf;

    use super::*;
    use crate::config::S3SourceConfig;

    fn config() -> ResolvedConfig {
        let raw: S3SourceConfig = serde_json::from_str(
            r#"{"bucket":"events","max_record_bytes":16,"max_batch_bytes":32,"max_batch_messages":2}"#
        ).expect("config should deserialize");
        ResolvedConfig::try_from(&raw).expect("config should resolve")
    }

    fn object(size: u64, next_byte_offset: u64) -> ActiveObjectState {
        ActiveObjectState {
            key: "logs/a".into(),
            etag: "etag".into(),
            size,
            next_byte_offset,
        }
    }

    #[test]
    fn given_rejected_record_when_read_should_resume_without_skipping_empty_records() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let data = b"a\n\nb\nc\n";
            let first = read_batch(data.as_slice(), &object(7, 0), &config())
                .await
                .expect("batch");
            assert_eq!(first.records.len(), 2);
            assert_eq!(first.next_offset, 5);
            assert!(!first.complete);
            let second = read_batch(&data[5..], &object(7, 5), &config())
                .await
                .expect("batch");
            assert_eq!(second.records[0].payload, b"c");
            assert!(second.complete);
        });
    }

    #[test]
    fn given_small_batch_limits_when_read_repeatedly_should_preserve_every_record_and_boundary() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let input = b"a\n\nbb\nc\nlast";
            for (max_batch_messages, max_batch_bytes) in [(1, 32), (2, 4)] {
                let mut config = config();
                config.max_batch_messages = max_batch_messages;
                config.max_batch_bytes = max_batch_bytes;
                let mut offset = 0;
                let mut records = Vec::new();
                loop {
                    let batch = read_batch(
                        &input[offset as usize..],
                        &object(input.len() as u64, offset),
                        &config,
                    )
                    .await
                    .expect("batch");
                    assert!(batch.next_offset > offset);
                    assert!(batch.records.len() <= max_batch_messages);
                    assert!(
                        batch
                            .records
                            .iter()
                            .map(|record| record.payload.len())
                            .sum::<usize>()
                            <= max_batch_bytes
                    );
                    offset = batch.next_offset;
                    records.extend(batch.records);
                    if batch.complete {
                        break;
                    }
                }
                assert_eq!(offset, input.len() as u64);
                assert_eq!(
                    records
                        .iter()
                        .map(|record| (record.payload.as_slice(), record.start_offset))
                        .collect::<Vec<_>>(),
                    vec![
                        (b"a".as_slice(), 0),
                        (b"bb".as_slice(), 3),
                        (b"c".as_slice(), 6),
                        (b"last".as_slice(), 8)
                    ]
                );
            }
        });
    }

    #[test]
    fn given_exact_full_batch_when_eof_confirmed_should_complete() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            for data in [b"a\nb\n".as_slice(), b"a\nb", b"\n\n", b""] {
                let batch = read_batch(data, &object(data.len() as u64, 0), &config())
                    .await
                    .expect("batch");
                assert!(batch.complete);
                assert_eq!(batch.next_offset, data.len() as u64);
            }
        });
    }

    #[test]
    fn given_final_record_rejected_when_read_should_resume_at_its_start() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let batch = read_batch(b"a\nb\nlast".as_slice(), &object(8, 0), &config())
                .await
                .expect("batch");
            assert_eq!(batch.next_offset, 4);
            assert_eq!(batch.records.len(), 2);
            assert!(!batch.complete);
        });
    }

    #[test]
    fn given_inconsistent_length_when_read_should_fail_without_completion() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let mut config = config();
            config.max_batch_messages = 3;
            for size in [2, 8] {
                assert!(matches!(
                    read_batch(b"a\nb\n".as_slice(), &object(size, 0), &config).await,
                    Err(ReadError::InvalidLength)
                ));
            }
        });
    }

    struct BrokenBody {
        sent: bool,
    }
    impl AsyncRead for BrokenBody {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.sent {
                return Poll::Ready(Err(io::Error::other("broken body")));
            }
            self.sent = true;
            output.put_slice(b"a\npartial");
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn given_body_error_when_read_should_not_flush_partial_record() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            assert!(matches!(
                read_batch(BrokenBody { sent: false }, &object(100, 0), &config()).await,
                Err(ReadError::Io(_))
            ));
        });
    }

    #[test]
    fn given_oversized_late_record_when_read_should_reject_entire_batch() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let data = b"a\n01234567890123456\n";
            assert!(matches!(
                read_batch(data.as_slice(), &object(data.len() as u64, 0), &config()).await,
                Err(ReadError::Framing(FramingError::RecordTooLarge {
                    start_offset: 2,
                    max_record_bytes: 16
                }))
            ));
        });
    }

    #[test]
    fn given_saved_eof_when_read_should_validate_body_without_replaying_records() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let batch = read_batch(b"a\nb\n".as_slice(), &object(4, 4), &config())
                .await
                .expect("batch");
            assert!(batch.complete);
            assert!(batch.records.is_empty());
            assert!(matches!(
                read_batch(b"a\n".as_slice(), &object(4, 4), &config()).await,
                Err(ReadError::InvalidLength)
            ));
        });
    }

    #[test]
    fn given_empty_stream_exceeding_scan_budget_when_read_should_checkpoint_progress() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let mut body = tokio::io::repeat(b'\n').take(4 * INPUT_SLICE_BYTES as u64);
            let batch = read_with_budget(
                &mut body,
                &object(4 * INPUT_SLICE_BYTES as u64, 0),
                &config(),
                1,
            )
            .await
            .expect("batch");
            assert!(!batch.complete);
            assert!(batch.records.is_empty());
            assert_eq!(batch.next_offset, INPUT_SLICE_BYTES as u64);
        });
    }

    #[test]
    fn given_first_record_beyond_scan_budget_when_read_should_finish_without_splitting() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let length = INPUT_SLICE_BYTES + 1;
            let mut config = config();
            config.max_record_bytes = length;
            config.max_batch_bytes = length;
            let body = tokio::io::repeat(b'x')
                .take(length as u64)
                .chain(b"\n".as_slice());
            let batch = read_with_budget(body, &object(length as u64 + 1, 0), &config, 1)
                .await
                .expect("record should finish");
            assert!(batch.complete);
            assert_eq!(batch.records.len(), 1);
            assert_eq!(batch.records[0].payload.len(), length);
        });
    }
}
