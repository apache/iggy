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

use crate::args::{common::IggyBenchArgs, kind::BenchmarkKindCommand};
use human_repr::HumanCount;
use iggy::prelude::IggyDuration;
use std::{
    fmt::Display,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicI64, Ordering},
    },
    time::Instant,
};

const MINIMUM_MSG_PAYLOAD_SIZE: usize = 20;

/// Determines how to calculate the finish condition's workload division
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkFinishConditionMode {
    /// Global condition shares work across all actors
    Shared,

    /// Global condition shares work across all actors (half of the total workload)
    SharedHalf,

    /// Per-actor condition for producers
    PerProducer,

    /// Per-actor condition for consumers
    PerConsumer,

    /// Per-actor condition for producing consumers (both send/receive)
    PerProducingConsumer,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BenchmarkFinishConditionType {
    TotalData,
    MessageBatchesCount,
    Duration,
}

pub struct BenchmarkFinishCondition {
    kind: BenchmarkFinishConditionType,
    total: u64,
    left_total: Arc<AtomicI64>,
    mode: BenchmarkFinishConditionMode,
    /// Lazily initialized on first `is_elapsed()` call so the timer starts
    /// when the actor begins polling, not when the condition is constructed.
    start_time: OnceLock<Instant>,
}

impl BenchmarkFinishCondition {
    /// Creates a new benchmark finish condition based on benchmark arguments.
    ///
    /// # Parameters
    /// * `args` - The benchmark arguments
    /// * `mode` - The finish condition mode that determines how workload is divided
    ///
    /// The mode parameter automatically determines the appropriate workload division factor:
    /// - Global: factor = 1 (total workload is shared across all actors)
    /// - `PerProducer`: factor = number of producers
    /// - `PerConsumer`: factor = number of consumers
    /// - `PerProducingConsumer`: factor = number of producing consumers * 2
    pub fn new(args: &IggyBenchArgs, mode: BenchmarkFinishConditionMode) -> Arc<Self> {
        let total_data = args.total_data();
        let batches_count = args.message_batches();

        let total_data_factor = match mode {
            BenchmarkFinishConditionMode::Shared => 1,
            BenchmarkFinishConditionMode::SharedHalf => 2,
            BenchmarkFinishConditionMode::PerProducer => args.producers(),
            BenchmarkFinishConditionMode::PerConsumer => args.consumers(),
            BenchmarkFinishConditionMode::PerProducingConsumer => args.producers() * 2,
        };

        let total_data_multiplier = match args.benchmark_kind {
            BenchmarkKindCommand::PinnedProducer(_)
            | BenchmarkKindCommand::BalancedProducer(_)
            | BenchmarkKindCommand::BalancedProducerAndConsumerGroup(_) => args.producers(),
            BenchmarkKindCommand::PinnedConsumer(_)
            | BenchmarkKindCommand::BalancedConsumerGroup(_) => args.consumers(),
            BenchmarkKindCommand::PinnedProducerAndConsumer(_) => {
                args.producers() + args.consumers()
            }
            BenchmarkKindCommand::EndToEndProducingConsumer(_)
            | BenchmarkKindCommand::EndToEndProducingConsumerGroup(_) => args.producers() * 2,
            BenchmarkKindCommand::Stress(_) => args.producers() + args.consumers(),
            BenchmarkKindCommand::Examples => unreachable!(),
        };

        match (total_data, batches_count) {
            (None, Some(count)) => {
                let count_per_actor = (count.get() * total_data_multiplier) / total_data_factor;

                Arc::new(Self {
                    kind: BenchmarkFinishConditionType::MessageBatchesCount,
                    total: u64::from(count_per_actor),
                    left_total: Arc::new(AtomicI64::new(i64::from(count_per_actor))),
                    mode,
                    start_time: OnceLock::new(),
                })
            }
            (Some(size), None) => {
                let bytes_per_actor = size.as_bytes_u64() / u64::from(total_data_factor);

                Arc::new(Self {
                    kind: BenchmarkFinishConditionType::TotalData,
                    total: bytes_per_actor,
                    left_total: Arc::new(AtomicI64::new(
                        i64::try_from(bytes_per_actor).unwrap_or(i64::MAX),
                    )),
                    mode,
                    start_time: OnceLock::new(),
                })
            }
            (None, None) => {
                // Stress benchmark uses --duration; extract it from args
                if let BenchmarkKindCommand::Stress(stress_args) = &args.benchmark_kind {
                    Self::new_duration(stress_args.duration())
                } else {
                    panic!("Either --total-messages-size or --message-batches must be provided")
                }
            }
            (Some(_), Some(_)) => {
                panic!("Cannot specify both --total-messages-size and --message-batches")
            }
        }
    }

    /// Creates an "empty" benchmark finish condition that is already satisfied.
    /// This is useful for consumer-only actors that don't need to produce any messages.
    pub fn new_empty() -> Arc<Self> {
        Arc::new(Self {
            kind: BenchmarkFinishConditionType::MessageBatchesCount,
            total: 0,
            left_total: Arc::new(AtomicI64::new(0)),
            mode: BenchmarkFinishConditionMode::Shared,
            start_time: OnceLock::new(),
        })
    }

    /// Creates a duration-based finish condition that completes after the given time elapses.
    pub fn new_duration(duration: IggyDuration) -> Arc<Self> {
        Arc::new(Self {
            kind: BenchmarkFinishConditionType::Duration,
            total: duration.as_micros(),
            left_total: Arc::new(AtomicI64::new(i64::MAX)),
            mode: BenchmarkFinishConditionMode::Shared,
            start_time: OnceLock::new(),
        })
    }

    pub fn account_and_check(&self, size_to_subtract: u64) -> bool {
        match self.kind {
            BenchmarkFinishConditionType::TotalData => {
                self.left_total.fetch_sub(
                    i64::try_from(size_to_subtract).unwrap_or(i64::MAX),
                    Ordering::AcqRel,
                );
                self.left_total.load(Ordering::Acquire) <= 0
            }
            BenchmarkFinishConditionType::MessageBatchesCount => {
                self.left_total.fetch_sub(1, Ordering::AcqRel);
                self.left_total.load(Ordering::Acquire) <= 0
            }
            BenchmarkFinishConditionType::Duration => self.is_elapsed(),
        }
    }

    pub fn is_done(&self) -> bool {
        match self.kind {
            BenchmarkFinishConditionType::Duration => self.is_elapsed(),
            _ => self.left() <= 0,
        }
    }

    fn is_elapsed(&self) -> bool {
        let start = self.start_time.get_or_init(Instant::now);
        start.elapsed() >= std::time::Duration::from_micros(self.total)
    }

    pub const fn total(&self) -> u64 {
        self.total
    }

    pub fn total_str(&self) -> String {
        match self.kind {
            BenchmarkFinishConditionType::TotalData => {
                format!(
                    "messages of size: {} ({})",
                    self.total.human_count_bytes(),
                    self.mode
                )
            }
            BenchmarkFinishConditionType::MessageBatchesCount => {
                format!("{} batches ({})", self.total.human_count_bare(), self.mode)
            }
            BenchmarkFinishConditionType::Duration => {
                let secs = self.total / 1_000_000;
                format!("duration: {secs}s ({mode})", mode = self.mode)
            }
        }
    }

    pub fn left(&self) -> i64 {
        self.left_total.load(Ordering::Relaxed)
    }

    pub fn status(&self) -> String {
        let done = i64::try_from(self.total()).unwrap_or(i64::MAX) - self.left();
        let total = i64::try_from(self.total()).unwrap_or(i64::MAX);
        match self.kind {
            BenchmarkFinishConditionType::TotalData => {
                format!(
                    "{}/{} ({})",
                    done.human_count_bytes(),
                    total.human_count_bytes(),
                    self.mode
                )
            }
            BenchmarkFinishConditionType::MessageBatchesCount => {
                format!(
                    "{}/{} ({})",
                    done.human_count_bare(),
                    total.human_count_bare(),
                    self.mode
                )
            }
            BenchmarkFinishConditionType::Duration => {
                let elapsed_secs = self.start_time.get().map_or(0, |s| s.elapsed().as_secs());
                let total_secs = self.total / 1_000_000;
                format!("{elapsed_secs}s/{total_secs}s ({mode})", mode = self.mode)
            }
        }
    }

    pub fn max_capacity(&self) -> usize {
        match self.kind {
            BenchmarkFinishConditionType::TotalData => {
                let value = self.left_total.load(Ordering::Relaxed);
                usize::try_from(value).unwrap_or(0) / MINIMUM_MSG_PAYLOAD_SIZE
            }
            BenchmarkFinishConditionType::MessageBatchesCount => {
                let value = self.left_total.load(Ordering::Relaxed);
                usize::try_from(value).unwrap_or(0)
            }
            // Duration-based conditions use a reasonable default buffer size
            BenchmarkFinishConditionType::Duration => 10_000,
        }
    }
}

impl Display for BenchmarkFinishConditionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shared => write!(f, "shared"),
            Self::SharedHalf => write!(f, "shared-half"),
            Self::PerProducer => write!(f, "per-producer"),
            Self::PerConsumer => write!(f, "per-consumer"),
            Self::PerProducingConsumer => {
                write!(f, "per-producing-consumer")
            }
        }
    }
}
