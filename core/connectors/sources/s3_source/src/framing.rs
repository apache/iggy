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

use std::ops::ControlFlow;

use iggy_connector_sdk::ProducedMessage;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FramedRecord {
    pub(crate) payload: Vec<u8>,
    pub(crate) start_offset: u64,
}

impl FramedRecord {
    pub(crate) fn into_produced_message(self, message_id: u128) -> ProducedMessage {
        ProducedMessage {
            id: Some(message_id),
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: None,
            payload: self.payload,
        }
    }
}

pub(crate) struct RecordFramer {
    delimiter: Vec<u8>,
    pending: Vec<u8>,
    pending_start_offset: u64,
    max_record_bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FramingError {
    RecordTooLarge {
        start_offset: u64,
        max_record_bytes: usize,
    },
}

impl RecordFramer {
    pub(crate) fn new(start_offset: u64, delimiter: Vec<u8>, max_record_bytes: usize) -> Self {
        Self {
            delimiter,
            pending: Vec::new(),
            pending_start_offset: start_offset,
            max_record_bytes,
        }
    }

    /// A rejected record stops payload emission, but the whole slice is still
    /// validated so oversized read-ahead cannot bypass the record limit.
    /// Returns the first rejected record's offset, not the scanned boundary.
    pub(crate) fn push_with(
        &mut self,
        chunk: &[u8],
        mut emit: impl FnMut(FramedRecord) -> ControlFlow<()>,
    ) -> Result<Option<u64>, FramingError> {
        // Only the delimiter overlap can contain a match starting in old bytes.
        let mut search_start = self.pending.len().saturating_sub(self.delimiter.len() - 1);
        self.pending.extend_from_slice(chunk);

        let mut rejected_offset = None;
        let mut record_start = 0;

        while let Some(relative_delimiter_start) = self.pending[search_start..]
            .windows(self.delimiter.len())
            .position(|window| window == self.delimiter.as_slice())
        {
            let delimiter_start = search_start + relative_delimiter_start;
            let payload_len = delimiter_start - record_start;

            if payload_len > self.max_record_bytes {
                return Err(FramingError::RecordTooLarge {
                    start_offset: self.pending_start_offset + record_start as u64,
                    max_record_bytes: self.max_record_bytes,
                });
            }
            let next_record_start = delimiter_start + self.delimiter.len();

            if delimiter_start > record_start && rejected_offset.is_none() {
                let start_offset = self.pending_start_offset + record_start as u64;
                if emit(FramedRecord {
                    payload: self.pending[record_start..delimiter_start].to_vec(),
                    start_offset,
                })
                .is_break()
                {
                    rejected_offset = Some(start_offset);
                }
            }

            record_start = next_record_start;
            search_start = next_record_start;
        }

        if record_start > 0 {
            self.pending.drain(..record_start);
            self.pending_start_offset += record_start as u64;
        }

        let minimum_payload_bytes = self.pending.len().saturating_sub(self.delimiter.len() - 1);
        if minimum_payload_bytes > self.max_record_bytes {
            return Err(FramingError::RecordTooLarge {
                start_offset: self.pending_start_offset,
                max_record_bytes: self.max_record_bytes,
            });
        }

        Ok(rejected_offset)
    }

    pub(crate) fn finish(self) -> Result<Option<FramedRecord>, FramingError> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        if self.pending.len() > self.max_record_bytes {
            return Err(FramingError::RecordTooLarge {
                start_offset: self.pending_start_offset,
                max_record_bytes: self.max_record_bytes,
            });
        }

        Ok(Some(FramedRecord {
            payload: self.pending,
            start_offset: self.pending_start_offset,
        }))
    }

    pub(crate) fn consumed_offset(&self) -> u64 {
        self.pending_start_offset
    }

    #[cfg(test)]
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<Vec<FramedRecord>, FramingError> {
        let mut records = Vec::new();
        self.push_with(chunk, |record| {
            records.push(record);
            ControlFlow::Continue(())
        })?;
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use std::{hint::black_box, time::Instant};

    use super::*;

    const TEST_MAX_RECORD_BYTES: usize = 1024;

    fn test_framer(start_offset: u64) -> RecordFramer {
        RecordFramer::new(start_offset, b"||".to_vec(), TEST_MAX_RECORD_BYTES)
    }

    #[test]
    fn given_many_small_records_when_emission_stops_should_only_construct_records_through_rejection()
     {
        let input = b"a\n".repeat(32_768);
        let mut framer = RecordFramer::new(20, b"\n".to_vec(), 1);
        let mut emitted = 0;
        let rejected = framer
            .push_with(&input, |record| {
                emitted += 1;
                assert_eq!(record.payload, b"a");
                if emitted == 1 {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            })
            .expect("records should fit");

        assert_eq!(emitted, 2);
        assert_eq!(rejected, Some(22));
        assert_eq!(framer.consumed_offset(), 20 + input.len() as u64);
        assert!(framer.pending.is_empty());
    }

    #[test]
    fn given_rejected_record_when_later_record_is_oversized_should_still_return_error() {
        for input in [b"a||b||long||".as_slice(), b"a||b||longer"] {
            let mut framer = RecordFramer::new(0, b"||".to_vec(), 3);
            let mut emitted = 0;
            let result = framer.push_with(input, |_| {
                emitted += 1;
                ControlFlow::Break(())
            });
            assert_eq!(emitted, 1);
            assert_eq!(
                result,
                Err(FramingError::RecordTooLarge {
                    start_offset: 6,
                    max_record_bytes: 3,
                })
            );
        }
    }

    #[test]
    #[ignore = "manual microbenchmark: run with --ignored --nocapture"]
    fn given_small_records_when_benchmarked_should_compare_eager_and_bounded_emission() {
        let input = b"a\n".repeat(32_768);
        let iterations = 200;
        let started = Instant::now();
        for _ in 0..iterations {
            let mut framer = RecordFramer::new(0, b"\n".to_vec(), 1);
            let records = framer.push(black_box(&input)).expect("valid records");
            black_box(records.into_iter().next());
        }
        let eager = started.elapsed();
        let started = Instant::now();
        for _ in 0..iterations {
            let mut framer = RecordFramer::new(0, b"\n".to_vec(), 1);
            let mut accepted = false;
            let rejected = framer
                .push_with(black_box(&input), |record| {
                    black_box(record);
                    if std::mem::replace(&mut accepted, true) {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                })
                .expect("valid records");
            assert_eq!(rejected, Some(2));
        }
        eprintln!(
            "{iterations} x 64 KiB, one-record batches: eager {eager:?}, bounded {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn given_two_custom_delimited_records_when_pushed_should_emit_payloads_and_boundaries() {
        let mut framer = test_framer(0);

        let records = framer.push(b"a||b||");

        assert_eq!(
            records,
            Ok(vec![
                FramedRecord {
                    payload: b"a".to_vec(),
                    start_offset: 0,
                },
                FramedRecord {
                    payload: b"b".to_vec(),
                    start_offset: 3,
                },
            ])
        );
    }

    #[test]
    fn given_leading_empty_record_when_pushed_should_omit_it_and_preserve_next_record_boundaries() {
        let mut framer = test_framer(0);

        let records = framer.push(b"||a||");

        assert_eq!(
            records,
            Ok(vec![FramedRecord {
                payload: b"a".to_vec(),
                start_offset: 2,
            }])
        );
    }

    #[test]
    fn given_only_empty_records_when_pushed_should_advance_consumed_offset() {
        let mut framer = test_framer(0);

        assert_eq!(framer.push(b"||||"), Ok(Vec::new()));
        assert_eq!(framer.consumed_offset(), 4);
    }

    #[test]
    fn given_whitespace_only_record_when_pushed_should_preserve_payload() {
        let mut framer = test_framer(0);

        let records = framer.push(b" ||");

        assert_eq!(
            records,
            Ok(vec![FramedRecord {
                payload: b" ".to_vec(),
                start_offset: 0,
            }])
        );
    }

    #[test]
    fn given_invalid_utf8_record_when_pushed_should_preserve_original_bytes() {
        let mut framer = test_framer(0);

        let records = framer.push(b"\xFF\xFE||");

        assert_eq!(
            records,
            Ok(vec![FramedRecord {
                payload: b"\xFF\xFE".to_vec(),
                start_offset: 0,
            }])
        );
    }

    #[test]
    fn given_nonempty_unterminated_record_when_finished_should_emit_final_record() {
        let mut framer = test_framer(10);

        assert_eq!(framer.push(b"final"), Ok(Vec::new()));
        assert_eq!(
            framer.finish(),
            Ok(Some(FramedRecord {
                payload: b"final".to_vec(),
                start_offset: 10,
            }))
        );
    }

    #[test]
    fn given_trailing_delimiter_when_finished_should_not_emit_extra_record() {
        let mut framer = test_framer(0);

        let records = framer
            .push(b"a||")
            .expect("record should fit within the configured limit");

        assert_eq!(records.len(), 1);
        assert_eq!(framer.finish(), Ok(None));
    }

    #[test]
    fn given_empty_input_when_finished_should_not_emit_record() {
        let framer = test_framer(0);

        assert_eq!(framer.finish(), Ok(None));
    }

    #[test]
    fn given_record_at_size_limit_when_pushed_should_succeed() {
        let mut framer = RecordFramer::new(0, b"||".to_vec(), 3);

        assert_eq!(
            framer.push(b"abc||"),
            Ok(vec![FramedRecord {
                payload: b"abc".to_vec(),
                start_offset: 0,
            }])
        );
    }

    #[test]
    fn given_delimited_record_over_size_limit_when_pushed_should_return_error() {
        let mut framer = RecordFramer::new(0, b"||".to_vec(), 3);

        assert_eq!(
            framer.push(b"abcd||"),
            Err(FramingError::RecordTooLarge {
                start_offset: 0,
                max_record_bytes: 3,
            })
        );
    }

    #[test]
    fn given_unterminated_record_over_size_limit_when_finished_should_return_error() {
        let mut framer = RecordFramer::new(0, b"||".to_vec(), 3);

        assert_eq!(framer.push(b"abcd"), Ok(Vec::new()));
        assert_eq!(
            framer.finish(),
            Err(FramingError::RecordTooLarge {
                start_offset: 0,
                max_record_bytes: 3,
            })
        );
    }

    #[test]
    fn given_split_delimiters_when_framed_should_preserve_records_and_offsets() {
        for delimiter in [b"\n".as_slice(), b"\r\n", "💥".as_bytes(), b"aba"] {
            let input = [b"first", delimiter, delimiter, b"last", delimiter, b"tail"].concat();
            let first_end = 5 + delimiter.len() as u64;
            let last_start = first_end + delimiter.len() as u64;
            let last_end = last_start + 4 + delimiter.len() as u64;
            let expected = vec![
                FramedRecord {
                    payload: b"first".to_vec(),
                    start_offset: 0,
                },
                FramedRecord {
                    payload: b"last".to_vec(),
                    start_offset: last_start,
                },
                FramedRecord {
                    payload: b"tail".to_vec(),
                    start_offset: last_end,
                },
            ];

            for split in 0..=input.len() {
                let mut framer = RecordFramer::new(0, delimiter.to_vec(), 10);
                let mut records = framer
                    .push(&input[..split])
                    .expect("first chunk should fit");
                records.extend(framer.push(&[]).expect("empty chunk should be harmless"));
                records.extend(
                    framer
                        .push(&input[split..])
                        .expect("second chunk should fit"),
                );
                records.extend(framer.finish().expect("tail should fit"));
                assert_eq!(
                    records, expected,
                    "delimiter: {delimiter:?}, split: {split}"
                );
            }
        }
    }

    #[test]
    fn given_overlapping_delimiter_when_pushed_bytewise_should_preserve_partial_delimiter_at_eof() {
        let mut framer = RecordFramer::new(0, b"aba".to_vec(), 3);
        for byte in b"ababa" {
            assert_eq!(framer.push(&[*byte]), Ok(Vec::new()));
        }
        assert_eq!(framer.consumed_offset(), 3);
        assert_eq!(
            framer.finish(),
            Ok(Some(FramedRecord {
                payload: vec![b'b', b'a'],
                start_offset: 3,
            }))
        );
    }

    #[test]
    fn given_large_record_when_pushed_bytewise_should_stay_bounded_and_emit_at_limit() {
        let max_record_bytes = 1024 * 1024;
        let mut framer = RecordFramer::new(0, b"||".to_vec(), max_record_bytes);
        for _ in 0..max_record_bytes {
            assert_eq!(framer.push(b"x"), Ok(Vec::new()));
            assert!(framer.pending.len() <= max_record_bytes);
        }
        assert_eq!(framer.push(b"|"), Ok(Vec::new()));
        assert_eq!(framer.pending.len(), max_record_bytes + 1);
        let records = framer.push(b"|").expect("record at limit should fit");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload.len(), max_record_bytes);
        assert!(records[0].payload.iter().all(|byte| *byte == b'x'));
        assert_eq!(framer.consumed_offset(), (max_record_bytes + 2) as u64);
        assert!(framer.pending.is_empty());
    }

    #[test]
    fn given_incomplete_record_beyond_lookahead_when_pushed_should_reject_it() {
        let mut framer = RecordFramer::new(10, b"||".to_vec(), 3);
        assert_eq!(framer.push(b"abcd"), Ok(Vec::new()));
        assert_eq!(
            framer.push(b"e"),
            Err(FramingError::RecordTooLarge {
                start_offset: 10,
                max_record_bytes: 3,
            })
        );
    }
}
