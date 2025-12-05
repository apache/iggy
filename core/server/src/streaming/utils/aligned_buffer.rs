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

use iggy_common::CACHE_LINE_SIZE;

/// Checks if a buffer pointer is cache-line aligned.
pub fn is_cache_line_aligned(ptr: *const u8) -> bool {
    (ptr as usize).is_multiple_of(CACHE_LINE_SIZE)
}

/// Rounds up a size to the next multiple of cache line size.
pub fn round_up_to_cache_line(size: usize) -> usize {
    (size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_cache_line_aligned() {
        // Allocate an aligned vec
        #[repr(align(64))]
        struct Aligned {
            _data: [u8; 64],
        }

        let aligned = Aligned { _data: [0; 64] };
        let ptr = aligned._data.as_ptr();

        assert!(is_cache_line_aligned(ptr));

        // Test misaligned pointer
        let misaligned_ptr = unsafe { ptr.add(1) };
        assert!(!is_cache_line_aligned(misaligned_ptr));
    }

    #[test]
    fn test_round_up_to_cache_line() {
        assert_eq!(round_up_to_cache_line(0), 0);
        assert_eq!(round_up_to_cache_line(1), 64);
        assert_eq!(round_up_to_cache_line(63), 64);
        assert_eq!(round_up_to_cache_line(64), 64);
        assert_eq!(round_up_to_cache_line(65), 128);
        assert_eq!(round_up_to_cache_line(127), 128);
        assert_eq!(round_up_to_cache_line(128), 128);
        assert_eq!(round_up_to_cache_line(1000), 1024);
    }
}
