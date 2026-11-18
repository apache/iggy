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

//! OpenMessaging Benchmark (OMB) results. OMB measures differently from iggy-bench, so its runs
//! have their own model, and no iggy-bench type takes an OMB run.

pub mod chart;
pub mod compare;
pub mod detail;
pub mod id;
pub mod quantile;
pub mod result;
pub mod setup;
pub mod summary;
#[cfg(test)]
mod test_fixtures;
pub mod workload;

/// OMB samples its statistics once per window of this length. The value is fixed in the OMB code,
/// and the result file doesn't record it.
pub const OMB_WINDOW_SECONDS: u64 = 10;
/// OMB divides bytes by 1024 twice for its MB/s, so its megabyte is a mebibyte. iggy-bench divides
/// by [`crate::metrics::BYTES_PER_MEGABYTE`], so the two throughputs differ for the same bytes.
pub const OMB_BYTES_PER_MEBIBYTE: f64 = 1_048_576.0;

/// The throughput of `messages_per_second` messages of `message_size` bytes, in MiB/s.
pub fn mebibytes_per_second(messages_per_second: f64, message_size: u64) -> f64 {
    messages_per_second * message_size as f64 / OMB_BYTES_PER_MEBIBYTE
}
