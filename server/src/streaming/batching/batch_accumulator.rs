use iggy::models::batch::{IggyBatch, IggyHeader, IggyMutableBatch, IGGY_BATCH_OVERHEAD};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use std::sync::Arc;

use crate::streaming::segments::IggyBatchFetchResult;

#[derive(Debug, Default)]
pub struct BatchAccumulator {
    base_offset: u64,
    base_timestamp: u64,
    current_size: IggyByteSize,
    current_offset: u64,
    current_timestamp: u64,
    leading_header: Option<IggyHeader>,
    batches: Vec<IggyMutableBatch>,
}

impl BatchAccumulator {
    pub fn coalesce_batch(&mut self, current_offset: u64, mut batch: IggyMutableBatch) -> u32 {
        let header = self.leading_header.get_or_insert_with(|| {
            let mut header = batch.header;
            let base_timestamp = IggyTimestamp::now().as_micros();
            self.base_offset = current_offset;
            self.base_timestamp = base_timestamp;
            self.current_offset = current_offset;
            self.current_timestamp = base_timestamp;

            header.base_offset = current_offset;
            header.base_timestamp = base_timestamp;
            header
        });

        let batch_size = batch.get_size();
        let messages_count =
            batch.update_header_and_messages_offsets_and_timestamps(current_offset, header);
        self.current_size += batch_size;
        self.current_offset = header.base_offset + header.last_offset_delta as u64;
        self.current_timestamp = header.base_timestamp + header.last_timestamp_delta as u64;
        self.batches.push(batch);

        messages_count
    }

    pub fn get_messages_by_offset(&self, start_offset: u64, end_offset: u64) -> Vec<IggyBatch> {
        let start_idx = self.batches.partition_point(|batch| {
            let last_offset = batch.header.base_offset + batch.header.last_offset_delta as u64;
            last_offset < start_offset
        });
        let end_idx = self.batches.partition_point(|batch| {
            let last_offset = batch.header.base_offset + batch.header.last_offset_delta as u64;
            last_offset <= end_offset
        });
        let batches = self.batches[start_idx..end_idx]
            .iter()
            .map(|batch| IggyBatch::new(batch.header, batch.batch.clone().freeze()))
            .collect();
        batches
    }

    pub fn get_messages_by_timestamp(&self, start_timestamp: u64, count: usize) -> Vec<Arc<()>> {
        //TODO: Fix me
        /*
        let start_idx = self
            .messages
            .partition_point(|msg| msg.timestamp < start_timestamp);
        let end_idx = std::cmp::min(start_idx + count, self.messages.len());
        self.messages[start_idx..end_idx].to_vec()
        */
        todo!()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn unsaved_messages_count(&self) -> usize {
        self.batches
            .iter()
            .map(|b| b.header)
            .map(|h| (h.last_offset_delta + 1) as usize)
            .sum()
    }

    pub fn batch_max_offset(&self) -> u64 {
        self.current_offset
    }

    pub fn batch_max_timestamp(&self) -> u64 {
        self.current_timestamp
    }

    pub fn batch_base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn batch_base_timestamp(&self) -> u64 {
        self.base_timestamp
    }

    pub fn materialize(self) -> (IggyHeader, Vec<IggyMutableBatch>) {
        (self.leading_header.unwrap(), self.batches)
    }
}
