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
use std::{
    fmt::Display,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
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
    ByTotalData,
    ByMessageBatchesCount,
}

pub struct BenchmarkFinishCondition {
    kind: BenchmarkFinishConditionType,
    total: usize,
    left_total: Arc<AtomicI64>,
    mode: BenchmarkFinishConditionMode,
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
            BenchmarkKindCommand::Examples => unreachable!(),
        };

        Arc::new(match (total_data, batches_count) {
            (None, Some(count)) => {
                let count_per_actor = (count.get() * total_data_multiplier) / total_data_factor;
                let count_per_actor_i64 =
                    i64::try_from(count_per_actor).expect("count_per_actor overflow");

                Self {
                    kind: BenchmarkFinishConditionType::ByMessageBatchesCount,
                    total: usize::from(count_per_actor),
                    left_total: Arc::new(AtomicI64::new(count_per_actor_i64)),
                    mode,
                }
            }
            (Some(size), None) => {
                let bytes_per_actor = size.as_bytes_usize() / usize::from(total_data_factor);

                Self {
                    kind: BenchmarkFinishConditionType::ByTotalData,
                    total: bytes_per_actor,
                    left_total: Arc::new(AtomicI64::new(
                        i64::try_from(bytes_per_actor).unwrap_or(i64::MAX),
                    )),
                    mode,
                }
            }
            _ => unreachable!(),
        })
    }

    /// Creates an "empty" benchmark finish condition that is already satisfied.
    /// This is useful for consumer-only actors that don't need to produce any messages.
    pub fn new_empty() -> Arc<Self> {
        Arc::new(Self {
            kind: BenchmarkFinishConditionType::ByMessageBatchesCount,
            total: 0,
            left_total: Arc::new(AtomicI64::new(0)),
            mode: BenchmarkFinishConditionMode::Shared,
        })
    }

    pub fn account_and_check(&self, size_to_subtract: u64) -> bool {
        match self.kind {
            BenchmarkFinishConditionType::ByTotalData => {
                self.left_total.fetch_sub(
                    i64::try_from(size_to_subtract).unwrap_or(i64::MAX),
                    Ordering::AcqRel,
                );
            }
            BenchmarkFinishConditionType::ByMessageBatchesCount => {
                self.left_total.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.left_total.load(Ordering::Acquire) <= 0
    }

    pub fn is_done(&self) -> bool {
        self.left() <= 0
    }

    pub const fn total(&self) -> usize {
        self.total
    }

    pub fn total_str(&self) -> String {
        match self.kind {
            BenchmarkFinishConditionType::ByTotalData => {
                format!(
                    "messages of size: {} ({})",
                    self.total.human_count_bytes(),
                    self.mode
                )
            }

            BenchmarkFinishConditionType::ByMessageBatchesCount => {
                format!("{} batches ({})", self.total.human_count_bare(), self.mode)
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
            BenchmarkFinishConditionType::ByTotalData => {
                format!(
                    "{}/{} ({})",
                    done.human_count_bytes(),
                    total.human_count_bytes(),
                    self.mode
                )
            }
            BenchmarkFinishConditionType::ByMessageBatchesCount => {
                format!(
                    "{}/{} ({})",
                    done.human_count_bare(),
                    total.human_count_bare(),
                    self.mode
                )
            }
        }
    }

    pub fn max_capacity(&self) -> usize {
        let value = self.left_total.load(Ordering::Relaxed);
        if self.kind == BenchmarkFinishConditionType::ByTotalData {
            usize::try_from(value).unwrap_or(0) / MINIMUM_MSG_PAYLOAD_SIZE
        } else {
            usize::try_from(value).unwrap_or(0)
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
