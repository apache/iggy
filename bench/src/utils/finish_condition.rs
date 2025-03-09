use human_repr::HumanCount;
use iggy::utils::byte_size::IggyByteSize;
use std::{
    num::NonZeroU32,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

const MINIMUM_MSG_PAYLOAD_SIZE: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq)]
enum BenchmarkFinishConditionType {
    ByTotalData,
    ByMessageBatchesCount,
}

pub struct BenchmarkFinishCondition {
    kind: BenchmarkFinishConditionType,
    total: u64,
    left_total: Arc<AtomicI64>,
}

impl BenchmarkFinishCondition {
    pub fn new(
        total_data: Option<IggyByteSize>,
        total_data_factor: u32,
        batches_count: Option<NonZeroU32>,
    ) -> Arc<Self> {
        Arc::new(match (total_data, batches_count) {
            (None, Some(count)) => Self {
                kind: BenchmarkFinishConditionType::ByMessageBatchesCount,
                total: count.get() as u64,
                left_total: Arc::new(AtomicI64::new(count.get() as i64)),
            },
            (Some(size), None) => Self {
                kind: BenchmarkFinishConditionType::ByTotalData,
                total: size.as_bytes_u64() / total_data_factor as u64,
                left_total: Arc::new(AtomicI64::new(
                    (size.as_bytes_u64() / total_data_factor as u64) as i64,
                )),
            },
            _ => unreachable!(),
        })
    }

    pub fn account_and_check(&self, size_to_subtract: u64) -> bool {
        match self.kind {
            BenchmarkFinishConditionType::ByTotalData => {
                self.left_total
                    .fetch_sub(size_to_subtract as i64, Ordering::AcqRel);
            }
            BenchmarkFinishConditionType::ByMessageBatchesCount => {
                self.left_total.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.left_total.load(Ordering::Acquire) <= 0
    }

    pub fn check(&self) -> bool {
        self.left() <= 0
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn total_str(&self) -> String {
        match self.kind {
            BenchmarkFinishConditionType::ByTotalData => {
                format!("messages of size: {}", self.total.human_count_bytes())
            }

            BenchmarkFinishConditionType::ByMessageBatchesCount => {
                format!("{} messages", self.total.human_count_bare())
            }
        }
    }

    pub fn left(&self) -> i64 {
        self.left_total.load(Ordering::Relaxed)
    }

    pub fn status(&self) -> String {
        let done = self.total() as i64 - self.left();
        let total = self.total() as i64;
        match self.kind {
            BenchmarkFinishConditionType::ByTotalData => {
                format!("{}/{}", done.human_count_bytes(), total.human_count_bytes())
            }
            BenchmarkFinishConditionType::ByMessageBatchesCount => {
                format!("{}/{}", done.human_count_bare(), total.human_count_bare())
            }
        }
    }

    pub fn max_capacity(&self) -> usize {
        let value = self.left_total.load(Ordering::Relaxed);
        if self.kind == BenchmarkFinishConditionType::ByTotalData {
            value as usize / MINIMUM_MSG_PAYLOAD_SIZE
        } else {
            value as usize
        }
    }
}
