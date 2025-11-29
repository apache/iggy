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

use super::IggyIndex;

/// Number of index entries per cache line.
/// Each IggyIndex is 16 bytes, so 4 entries = 64 bytes = 1 cache line.
pub const ENTRIES_PER_CACHE_LINE: usize = 4;

/// Cache line size in bytes (typically 64 bytes on modern CPUs).
pub const CACHE_LINE_SIZE: usize = 64;

/// A cache-line-aligned block containing exactly 4 index entries.
///
/// This structure ensures that a contiguous group of 4 index entries
/// occupies exactly one CPU cache line (64 bytes), improving cache
/// efficiency during binary search operations.
///
/// # Memory Layout
/// ```text
/// Cache Line (64 bytes):
/// [Entry 0: 16B][Entry 1: 16B][Entry 2: 16B][Entry 3: 16B]
/// ```
///
/// # Benefits
/// - Zero padding waste (100% utilization)
/// - Guaranteed cache alignment
/// - Predictable cache behavior
/// - SIMD-friendly (can process 4 timestamps in parallel)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct IndexCacheLineBlock {
    entries: [IggyIndex; ENTRIES_PER_CACHE_LINE],
}

impl IndexCacheLineBlock {
    /// Creates a new cache-line block from an array of 4 entries.
    pub fn new(entries: [IggyIndex; ENTRIES_PER_CACHE_LINE]) -> Self {
        Self { entries }
    }

    /// Creates a new cache-line block with default (zero) entries.
    pub fn default() -> Self {
        Self {
            entries: [IggyIndex::default(); ENTRIES_PER_CACHE_LINE],
        }
    }

    /// Gets a reference to a specific entry in the block.
    ///
    /// # Arguments
    /// * `index` - Index within the block (0-3)
    ///
    /// # Returns
    /// Some(&IggyIndex) if index is valid, None otherwise
    pub fn get(&self, index: usize) -> Option<&IggyIndex> {
        self.entries.get(index)
    }

    /// Gets a mutable reference to a specific entry in the block.
    ///
    /// # Arguments
    /// * `index` - Index within the block (0-3)
    ///
    /// # Returns
    /// Some(&mut IggyIndex) if index is valid, None otherwise
    pub fn get_mut(&mut self, index: usize) -> Option<&mut IggyIndex> {
        self.entries.get_mut(index)
    }

    /// Returns a reference to all entries in the block.
    pub fn entries(&self) -> &[IggyIndex; ENTRIES_PER_CACHE_LINE] {
        &self.entries
    }

    /// Returns a mutable reference to all entries in the block.
    pub fn entries_mut(&mut self) -> &mut [IggyIndex; ENTRIES_PER_CACHE_LINE] {
        &mut self.entries
    }

    /// Finds an entry by timestamp using linear search within the block.
    ///
    /// Since there are only 4 entries, linear search is often faster than
    /// more complex algorithms, especially with CPU prefetching.
    ///
    /// # Arguments
    /// * `target_timestamp` - The timestamp to search for
    ///
    /// # Returns
    /// The first entry with timestamp >= target_timestamp, or None if not found
    pub fn find_by_timestamp(&self, target_timestamp: u64) -> Option<&IggyIndex> {
        // Linear search through 4 entries (very fast with prefetching)
        for entry in &self.entries {
            if entry.timestamp >= target_timestamp {
                return Some(entry);
            }
        }
        None
    }

    /// Finds the index position within the block for a given timestamp.
    ///
    /// # Arguments
    /// * `target_timestamp` - The timestamp to search for
    ///
    /// # Returns
    /// The index (0-3) of the first entry with timestamp >= target_timestamp,
    /// or None if not found
    pub fn find_position_by_timestamp(&self, target_timestamp: u64) -> Option<usize> {
        for (i, entry) in self.entries.iter().enumerate() {
            if entry.timestamp >= target_timestamp {
                return Some(i);
            }
        }
        None
    }

    /// Gets the minimum timestamp in this block.
    pub fn min_timestamp(&self) -> u64 {
        self.entries[0].timestamp
    }

    /// Gets the maximum timestamp in this block.
    pub fn max_timestamp(&self) -> u64 {
        self.entries[ENTRIES_PER_CACHE_LINE - 1].timestamp
    }

    /// Checks if the target timestamp falls within this block's range.
    ///
    /// # Arguments
    /// * `target_timestamp` - The timestamp to check
    ///
    /// # Returns
    /// true if target_timestamp is within [min, max] range of this block
    pub fn contains_timestamp(&self, target_timestamp: u64) -> bool {
        target_timestamp >= self.min_timestamp() && target_timestamp <= self.max_timestamp()
    }
}

impl Default for IndexCacheLineBlock {
    fn default() -> Self {
        Self::default()
    }
}

impl std::ops::Index<usize> for IndexCacheLineBlock {
    type Output = IggyIndex;

    fn index(&self, index: usize) -> &Self::Output {
        &self.entries[index]
    }
}

impl std::ops::IndexMut<usize> for IndexCacheLineBlock {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.entries[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_line_block_size() {
        use std::mem::{align_of, size_of};

        // Verify the block is exactly 64 bytes
        assert_eq!(size_of::<IndexCacheLineBlock>(), CACHE_LINE_SIZE);

        // Verify it's 64-byte aligned
        assert_eq!(align_of::<IndexCacheLineBlock>(), CACHE_LINE_SIZE);

        // Verify each entry is 16 bytes
        assert_eq!(size_of::<IggyIndex>(), 16);
    }

    #[test]
    fn test_cache_line_block_alignment() {
        // Allocate a block and verify its address is 64-byte aligned
        let block = IndexCacheLineBlock::default();
        let addr = &block as *const _ as usize;
        assert_eq!(addr % 64, 0, "Block should be 64-byte aligned");
    }

    #[test]
    fn test_find_by_timestamp() {
        let entries = [
            IggyIndex {
                offset: 0,
                position: 0,
                timestamp: 1000,
            },
            IggyIndex {
                offset: 1,
                position: 100,
                timestamp: 2000,
            },
            IggyIndex {
                offset: 2,
                position: 200,
                timestamp: 3000,
            },
            IggyIndex {
                offset: 3,
                position: 300,
                timestamp: 4000,
            },
        ];

        let block = IndexCacheLineBlock::new(entries);

        // Exact match
        assert_eq!(block.find_by_timestamp(2000).unwrap().offset, 1);

        // Between timestamps (should return next higher)
        assert_eq!(block.find_by_timestamp(2500).unwrap().offset, 2);

        // Before first
        assert_eq!(block.find_by_timestamp(500).unwrap().offset, 0);

        // After last
        assert!(block.find_by_timestamp(5000).is_none());
    }

    #[test]
    fn test_contains_timestamp() {
        let entries = [
            IggyIndex {
                offset: 0,
                position: 0,
                timestamp: 1000,
            },
            IggyIndex {
                offset: 1,
                position: 100,
                timestamp: 2000,
            },
            IggyIndex {
                offset: 2,
                position: 200,
                timestamp: 3000,
            },
            IggyIndex {
                offset: 3,
                position: 300,
                timestamp: 4000,
            },
        ];

        let block = IndexCacheLineBlock::new(entries);

        assert!(block.contains_timestamp(1000));
        assert!(block.contains_timestamp(2500));
        assert!(block.contains_timestamp(4000));
        assert!(!block.contains_timestamp(500));
        assert!(!block.contains_timestamp(5000));
    }

    #[test]
    fn test_min_max_timestamp() {
        let entries = [
            IggyIndex {
                offset: 0,
                position: 0,
                timestamp: 1000,
            },
            IggyIndex {
                offset: 1,
                position: 100,
                timestamp: 2000,
            },
            IggyIndex {
                offset: 2,
                position: 200,
                timestamp: 3000,
            },
            IggyIndex {
                offset: 3,
                position: 300,
                timestamp: 4000,
            },
        ];

        let block = IndexCacheLineBlock::new(entries);

        assert_eq!(block.min_timestamp(), 1000);
        assert_eq!(block.max_timestamp(), 4000);
    }
}
