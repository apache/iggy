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

use crate::framing::FramedRecord;

pub(crate) struct RecordBatch {
    pub(crate) records: Vec<FramedRecord>,
    payload_bytes: usize,
    max_batch_bytes: usize,
    max_batch_messages: usize,
}

impl RecordBatch {
    pub(crate) fn new(max_batch_bytes: usize, max_batch_messages: usize) -> Self {
        Self {
            records: Vec::new(),
            payload_bytes: 0,
            max_batch_bytes,
            max_batch_messages,
        }
    }

    pub(crate) fn try_push(&mut self, record: FramedRecord) -> Result<(), FramedRecord> {
        if self.records.len() >= self.max_batch_messages
            || record.payload.len() > self.max_batch_bytes - self.payload_bytes
        {
            return Err(record);
        }

        self.payload_bytes += record.payload.len();
        self.records.push(record);
        Ok(())
    }

    pub(crate) fn is_full(&self) -> bool {
        self.records.len() == self.max_batch_messages || self.payload_bytes == self.max_batch_bytes
    }
}

#[cfg(test)]
mod tests {
    use crate::framing::RecordFramer;

    use super::*;

    #[test]
    fn given_records_exactly_filling_byte_limit_when_batched_should_preserve_records() {
        let mut framer = RecordFramer::new(0, b"\n".to_vec(), 5);
        let mut batch = RecordBatch::new(5, 2);

        for record in framer.push(b"ab\ncde\n").expect("records should fit") {
            assert_eq!(batch.try_push(record), Ok(()));
        }

        assert_eq!(batch.payload_bytes, 5);
        assert_eq!(
            batch.records,
            vec![
                FramedRecord {
                    payload: b"ab".to_vec(),
                    start_offset: 0,
                },
                FramedRecord {
                    payload: b"cde".to_vec(),
                    start_offset: 3,
                },
            ]
        );
    }

    #[test]
    fn given_batch_limit_when_record_rejected_should_preserve_batch_and_return_record() {
        for (max_batch_bytes, max_batch_messages) in [(5, 3), (10, 1)] {
            let mut batch = RecordBatch::new(max_batch_bytes, max_batch_messages);
            assert_eq!(
                batch.try_push(FramedRecord {
                    payload: b"ab".to_vec(),
                    start_offset: 0,
                }),
                Ok(())
            );

            let rejected = FramedRecord {
                payload: b"cdef".to_vec(),
                start_offset: 3,
            };
            assert_eq!(
                batch.try_push(rejected),
                Err(FramedRecord {
                    payload: b"cdef".to_vec(),
                    start_offset: 3,
                })
            );
            assert_eq!(batch.payload_bytes, 2);
            assert_eq!(
                batch.records,
                vec![FramedRecord {
                    payload: b"ab".to_vec(),
                    start_offset: 0,
                }]
            );
        }
    }

    #[test]
    fn given_empty_records_and_read_ahead_when_batch_fills_should_resume_at_rejected_record() {
        let input = b"a\n\nb\nc\n";
        for (max_batch_bytes, max_batch_messages) in [(1, 10), (10, 1)] {
            let mut framer = RecordFramer::new(20, b"\n".to_vec(), 10);
            let mut batch = RecordBatch::new(max_batch_bytes, max_batch_messages);
            let rejected = framer
                .push(input)
                .expect("records should fit")
                .into_iter()
                .find_map(|record| batch.try_push(record).err())
                .expect("second record should exceed batch limit");

            assert_eq!(batch.records.len(), 1);
            assert_eq!(batch.records[0].payload, b"a");
            assert_eq!(rejected.start_offset, 23);
            assert_eq!(framer.consumed_offset(), 27);

            let mut resumed = RecordFramer::new(rejected.start_offset, b"\n".to_vec(), 10);
            let remaining = resumed
                .push(&input[(rejected.start_offset - 20) as usize..])
                .expect("remaining records should fit");
            assert_eq!(
                remaining,
                vec![
                    FramedRecord {
                        payload: b"b".to_vec(),
                        start_offset: 23,
                    },
                    FramedRecord {
                        payload: b"c".to_vec(),
                        start_offset: 25,
                    },
                ]
            );
        }
    }

    #[test]
    fn given_trailing_empty_records_when_all_records_fit_should_allow_consumed_boundary() {
        let mut framer = RecordFramer::new(20, b"\n".to_vec(), 10);
        let mut batch = RecordBatch::new(10, 2);

        for record in framer.push(b"a\n\npartial").expect("record should fit") {
            assert_eq!(batch.try_push(record), Ok(()));
        }

        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].start_offset, 20);
        assert_eq!(framer.consumed_offset(), 23);
    }
}
