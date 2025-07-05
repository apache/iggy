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

use bench_report::numeric_parameter::BenchmarkNumericParameter;
use bytes::Bytes;
use iggy::prelude::*;
use rand::{Rng, distr::Alphanumeric};

pub struct BenchmarkMessagesBatch {
    pub messages: Vec<IggyMessage>,
    pub user_data_bytes: u64,
    pub total_bytes: u64,
}

pub struct BenchmarkBatchGenerator {
    message_size: BenchmarkNumericParameter,
    messages_per_batch: BenchmarkNumericParameter,
    full_payload: Bytes,
    batch: BenchmarkMessagesBatch,
    is_fixed: bool,
}

impl BenchmarkBatchGenerator {
    pub fn new(
        message_size: BenchmarkNumericParameter,
        messages_per_batch: BenchmarkNumericParameter,
    ) -> Self {
        let distr = Alphanumeric; // generate random payload consisting of alphanumeric characters
        let max_len = message_size.max() as usize;
        let is_fixed = message_size.is_fixed() && messages_per_batch.is_fixed();

        let random: Vec<u8> = rand::rng().sample_iter(distr).take(max_len).collect();

        let mut batch_generator = Self {
            message_size,
            messages_per_batch,
            full_payload: Bytes::from(random),
            batch: BenchmarkMessagesBatch {
                messages: Vec::with_capacity(messages_per_batch.max() as usize),
                user_data_bytes: 0,
                total_bytes: 0,
            },
            is_fixed,
        };

        if is_fixed {
            batch_generator.batch = batch_generator.build_single_fixed();
        }

        batch_generator
    }

    pub fn generate_batch(&mut self) -> &mut BenchmarkMessagesBatch {
        if self.is_fixed {
            // Set origin timestamp for fixed batch.
            // Technically it's not correct, because we're setting one
            // timestamp for all messages, but it's close enough.
            let now = IggyTimestamp::now().as_micros();
            for msg in &mut self.batch.messages {
                msg.header.origin_timestamp = now;
            }
            assert!(
                !self.batch.messages.is_empty(),
                "Benchmark batch should contain at least one message"
            );
            return &mut self.batch;
        }

        let messages_per_batch = self.messages_per_batch.get() as usize;
        let message_size = &self.message_size;
        let full = &self.full_payload;

        let mut user_data_bytes = 0u64;
        let mut total_bytes = 0u64;

        // When using builder, origin timestamp is set by default
        let messages = (0..messages_per_batch)
            .map(|_| {
                let msg = IggyMessage::builder()
                    .payload(full.slice(0..message_size.get() as usize))
                    .build()
                    .unwrap();
                user_data_bytes += msg.payload.len() as u64;
                total_bytes += msg.get_size_bytes().as_bytes_u64();
                msg
            })
            .collect();

        self.batch = BenchmarkMessagesBatch {
            messages,
            user_data_bytes,
            total_bytes,
        };

        assert!(
            !self.batch.messages.is_empty(),
            "Benchmark batch should contain at least one message"
        );
        &mut self.batch
    }

    pub fn generate_owned_batch(&self) -> BenchmarkMessagesBatch {
        self.build_single_fixed()
    }

    #[inline]
    fn build_single_fixed(&self) -> BenchmarkMessagesBatch {
        let payload_length = self.message_size.max();
        let messages_per_batch = self.messages_per_batch.max();
        let payload = self.full_payload.slice(0..payload_length as usize);

        let mut user_data_bytes = 0u64;
        let mut total_bytes = 0u64;
        let messages = (0..messages_per_batch)
            .map(|_| {
                let msg = IggyMessage::builder()
                    .payload(payload.clone())
                    .build()
                    .unwrap();
                user_data_bytes += msg.payload.len() as u64;
                total_bytes += msg.get_size_bytes().as_bytes_u64();
                msg
            })
            .collect();

        BenchmarkMessagesBatch {
            messages,
            user_data_bytes,
            total_bytes,
        }
    }
}
