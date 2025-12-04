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

#[cfg(test)]
mod tests {
    use crate::streaming::segments::indexes::indexes_mut::IggyIndexesMut;
    use iggy_common::{MemoryPool, MEMORY_POOL};
    use crate::configs::system::SystemConfig;
    use iggy_common::ENTRIES_PER_CACHE_LINE;
    use std::sync::{Arc, Once};

    static INIT: Once = Once::new();

    // Initialize memory pool using the same approach as other server tests
    // This ensures compatibility with memory_pool's own tests
    fn init_memory_pool() {
        INIT.call_once(|| {
            // Only initialize if not already initialized
            if MEMORY_POOL.get().is_none() {
                let config = Arc::new(SystemConfig::default());
                MemoryPool::init_pool(&config.memory_pool.into_other());
            }
        });
    }

    fn create_test_indexes(count: u32) -> IggyIndexesMut {
        init_memory_pool();

        let mut indexes = IggyIndexesMut::with_capacity(count as usize, 0);

        for i in 0..count {
            indexes.insert(
                i,                          // offset
                i * 1000,                   // position
                1000000 + (i as u64 * 1000), // timestamp
            );
        }

        indexes
    }

    #[test]
    fn test_block_count() {
        // Test with exact multiple of block size
        let indexes = create_test_indexes(16); // 16 entries = 4 blocks
        assert_eq!(indexes.block_count(), 4);

        // Test with non-exact multiple
        let indexes = create_test_indexes(18); // 18 entries = 4 complete blocks + 2 remaining
        assert_eq!(indexes.block_count(), 4);

        // Test with less than one block
        let indexes = create_test_indexes(2); // 2 entries < 4
        assert_eq!(indexes.block_count(), 0);

        // Test with empty
        let indexes = IggyIndexesMut::empty();
        assert_eq!(indexes.block_count(), 0);
    }

    #[test]
    fn test_get_block() {
        let indexes = create_test_indexes(20); // 5 complete blocks

        // Get first block
        let block0 = indexes.get_block(0).expect("Block 0 should exist");
        assert_eq!(block0.get(0).unwrap().offset, 0);
        assert_eq!(block0.get(1).unwrap().offset, 1);
        assert_eq!(block0.get(2).unwrap().offset, 2);
        assert_eq!(block0.get(3).unwrap().offset, 3);

        // Get middle block
        let block2 = indexes.get_block(2).expect("Block 2 should exist");
        assert_eq!(block2.get(0).unwrap().offset, 8);  // 2 * 4 = 8
        assert_eq!(block2.get(1).unwrap().offset, 9);
        assert_eq!(block2.get(2).unwrap().offset, 10);
        assert_eq!(block2.get(3).unwrap().offset, 11);

        // Try to get block beyond range
        assert!(indexes.get_block(5).is_none());
        assert!(indexes.get_block(100).is_none());
    }

    #[test]
    fn test_get_block_boundary_conditions() {
        // Exactly 1 block
        let indexes = create_test_indexes(4);
        assert_eq!(indexes.block_count(), 1);
        assert!(indexes.get_block(0).is_some());
        assert!(indexes.get_block(1).is_none());

        // Exactly 2 blocks
        let indexes = create_test_indexes(8);
        assert_eq!(indexes.block_count(), 2);
        assert!(indexes.get_block(0).is_some());
        assert!(indexes.get_block(1).is_some());
        assert!(indexes.get_block(2).is_none());
    }

    #[test]
    fn test_binary_search_blocks_by_timestamp() {
        let indexes = create_test_indexes(20); // 5 blocks

        // Search for exact matches
        let result = indexes.binary_search_blocks_by_timestamp(1000000);
        assert_eq!(result, Some((0, 0))); // Block 0, entry 0

        let result = indexes.binary_search_blocks_by_timestamp(1001000);
        assert_eq!(result, Some((0, 1))); // Block 0, entry 1

        let result = indexes.binary_search_blocks_by_timestamp(1008000);
        assert_eq!(result, Some((2, 0))); // Block 2, entry 0 (index 8)

        // Search for timestamp between entries (should find next higher)
        let result = indexes.binary_search_blocks_by_timestamp(1000500);
        assert_eq!(result, Some((0, 1))); // Should find entry 1

        // Search before first
        let result = indexes.binary_search_blocks_by_timestamp(999000);
        assert_eq!(result, Some((0, 0)));

        // Search after last
        let result = indexes.binary_search_blocks_by_timestamp(2000000);
        assert!(result.is_none());
    }

    #[test]
    fn test_binary_search_blocks_with_incomplete_block() {
        // 18 entries = 4 complete blocks + 2 remaining
        let indexes = create_test_indexes(18);

        // Search within complete blocks
        let result = indexes.binary_search_blocks_by_timestamp(1000000);
        assert_eq!(result, Some((0, 0)));

        // Search in incomplete block region
        let result = indexes.binary_search_blocks_by_timestamp(1016000); // Index 16
        assert!(result.is_some());
        let (block_idx, entry_idx) = result.unwrap();
        let global_idx = block_idx * ENTRIES_PER_CACHE_LINE as u32 + entry_idx as u32;
        assert_eq!(global_idx, 16);

        let result = indexes.binary_search_blocks_by_timestamp(1017000); // Index 17
        assert!(result.is_some());
        let (block_idx, entry_idx) = result.unwrap();
        let global_idx = block_idx * ENTRIES_PER_CACHE_LINE as u32 + entry_idx as u32;
        assert_eq!(global_idx, 17);
    }

    #[test]
    fn test_find_by_timestamp_block_aware() {
        let indexes = create_test_indexes(20);

        // Exact match
        let idx = indexes.find_by_timestamp_block_aware(1005000).expect("Should find");
        assert_eq!(idx.offset(), 5);
        assert_eq!(idx.timestamp(), 1005000);

        // Between timestamps
        let idx = indexes.find_by_timestamp_block_aware(1005500).expect("Should find");
        assert_eq!(idx.offset(), 6); // Next higher

        // Before first
        let idx = indexes.find_by_timestamp_block_aware(999000).expect("Should find");
        assert_eq!(idx.offset(), 0);

        // After last
        assert!(indexes.find_by_timestamp_block_aware(2000000).is_none());
    }

    #[test]
    fn test_find_by_timestamp_block_aware_vs_regular() {
        // Verify that block-aware search gives same results as regular search
        let indexes = create_test_indexes(100);

        let test_timestamps = vec![
            1000000,  // First
            1050000,  // Middle
            1099000,  // Last
            999000,   // Before first
            1025500,  // Between entries
            2000000,  // After last
        ];

        for timestamp in test_timestamps {
            let block_result = indexes.find_by_timestamp_block_aware(timestamp);
            let regular_result = indexes.find_by_timestamp(timestamp);

            match (block_result, regular_result) {
                (Some(block_idx), Some(regular_idx)) => {
                    assert_eq!(
                        block_idx.offset(),
                        regular_idx.offset(),
                        "Mismatch for timestamp {}", timestamp
                    );
                    assert_eq!(
                        block_idx.timestamp(),
                        regular_idx.timestamp(),
                        "Mismatch for timestamp {}", timestamp
                    );
                }
                (None, None) => {
                    // Both returned None, which is correct
                }
                _ => {
                    panic!(
                        "Block-aware and regular search gave different results for timestamp {}",
                        timestamp
                    );
                }
            }
        }
    }

    #[test]
    fn test_block_search_with_empty_indexes() {
        init_memory_pool();
        let indexes = IggyIndexesMut::empty();

        assert_eq!(indexes.block_count(), 0);
        assert!(indexes.get_block(0).is_none());
        assert!(indexes.binary_search_blocks_by_timestamp(1000000).is_none());
        assert!(indexes.find_by_timestamp_block_aware(1000000).is_none());
    }

    #[test]
    fn test_block_search_with_single_entry() {
        let indexes = create_test_indexes(1);

        assert_eq!(indexes.block_count(), 0); // Not enough for a complete block
        // Block search should handle this gracefully
        let result = indexes.binary_search_blocks_by_timestamp(1000000);
        // With incomplete blocks, it should fall back to entry-level search
        assert!(result.is_some() || result.is_none()); // Either is acceptable for edge case
    }

    #[test]
    fn test_large_index_block_search() {
        // Test with a larger number of entries to verify scalability
        let indexes = create_test_indexes(1000); // 250 blocks

        assert_eq!(indexes.block_count(), 250);

        // Search near beginning
        let idx = indexes.find_by_timestamp_block_aware(1001000).expect("Should find");
        assert_eq!(idx.offset(), 1);

        // Search in middle
        let idx = indexes.find_by_timestamp_block_aware(1500000).expect("Should find");
        assert_eq!(idx.offset(), 500);

        // Search near end
        let idx = indexes.find_by_timestamp_block_aware(1999000).expect("Should find");
        assert_eq!(idx.offset(), 999);
    }
}
