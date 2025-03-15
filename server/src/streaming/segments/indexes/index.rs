/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::streaming::segments::segment::Segment;
use iggy::error::IggyError;
use iggy::error::IggyError::InvalidOffset;

#[derive(Debug, Eq, Clone, Copy, Default)]
pub struct Index {
    pub offset: u32,
    pub position: u32,
    pub timestamp: u64,
}

impl PartialEq<Self> for Index {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
            && self.position == other.position
            && self.timestamp == other.timestamp
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IndexRange {
    pub start: Index,
    pub end: Index,
}

impl Segment {
    pub fn load_highest_lower_bound_index(
        &self,
        indices: &[Index],
        start_offset: u32,
        end_offset: u32,
    ) -> Result<IndexRange, IggyError> {
        let starting_offset_idx = binary_search_index(indices, start_offset);
        let ending_offset_idx = binary_search_index(indices, end_offset);

        match (starting_offset_idx, ending_offset_idx) {
            (Some(starting_offset_idx), Some(ending_offset_idx)) => Ok(IndexRange {
                start: indices[starting_offset_idx],
                end: indices[ending_offset_idx],
            }),
            (Some(starting_offset_idx), None) => Ok(IndexRange {
                start: indices[starting_offset_idx],
                end: *indices.last().unwrap(),
            }),
            (None, _) => Err(InvalidOffset(start_offset as u64 + self.start_offset)),
        }
    }
}

fn binary_search_index(indices: &[Index], offset: u32) -> Option<usize> {
    match indices.binary_search_by(|index| index.offset.cmp(&offset)) {
        Ok(index) => Some(index),
        Err(index) => {
            if index < indices.len() {
                Some(index)
            } else {
                None
            }
        }
    }
}

impl IndexRange {
    pub fn max_range() -> Self {
        Self {
            start: Index {
                offset: 0,
                position: 0,
                timestamp: 0,
            },
            end: Index {
                offset: u32::MAX - 1,
                position: u32::MAX,
                timestamp: u64::MAX,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::{SegmentConfig, SystemConfig};
    use iggy::utils::expiry::IggyExpiry;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    async fn create_segment() -> Segment {
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig {
            segment: SegmentConfig {
                cache_indexes: true,
                ..Default::default()
            },
            ..Default::default()
        });

        Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            config,
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn create_test_indices(segment: &mut Segment) {
        let indexes = vec![
            Index {
                offset: 5,
                position: 0,
                timestamp: 1000,
            },
            Index {
                offset: 20,
                position: 100,
                timestamp: 2000,
            },
            Index {
                offset: 35,
                position: 200,
                timestamp: 3000,
            },
            Index {
                offset: 50,
                position: 300,
                timestamp: 4000,
            },
            Index {
                offset: 65,
                position: 400,
                timestamp: 5000,
            },
        ];
        segment.indexes.as_mut().unwrap().extend(indexes);
    }

    #[tokio::test]
    async fn should_find_both_indices() {
        let mut segment = create_segment().await;
        create_test_indices(&mut segment);
        let result = segment
            .load_highest_lower_bound_index(segment.indexes.as_ref().unwrap(), 15, 45)
            .unwrap();

        assert_eq!(result.start.offset, 20);
        assert_eq!(result.end.offset, 50);
    }

    #[tokio::test]
    async fn start_and_end_index_should_be_equal() {
        let mut segment = create_segment().await;
        create_test_indices(&mut segment);
        let result_end_range = segment
            .load_highest_lower_bound_index(segment.indexes.as_ref().unwrap(), 65, 100)
            .unwrap();

        assert_eq!(result_end_range.start.offset, 65);
        assert_eq!(result_end_range.end.offset, 65);

        let result_start_range = segment
            .load_highest_lower_bound_index(segment.indexes.as_ref().unwrap(), 0, 5)
            .unwrap();
        assert_eq!(result_start_range.start.offset, 5);
        assert_eq!(result_start_range.end.offset, 5);
    }

    #[tokio::test]
    async fn should_clamp_last_index_when_out_of_range() {
        let mut segment = create_segment().await;
        create_test_indices(&mut segment);
        let result = segment
            .load_highest_lower_bound_index(segment.indexes.as_ref().unwrap(), 5, 100)
            .unwrap();

        assert_eq!(result.start.offset, 5);
        assert_eq!(result.end.offset, 65);
    }

    #[tokio::test]
    async fn should_return_err_when_both_indices_out_of_range() {
        let mut segment = create_segment().await;
        create_test_indices(&mut segment);

        let result =
            segment.load_highest_lower_bound_index(segment.indexes.as_ref().unwrap(), 100, 200);
        assert!(result.is_err());
    }
}
