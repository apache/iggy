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
mod benchmarks {
    use crate::configs::system::SystemConfig;
    use crate::streaming::segments::indexes::indexes_mut::IggyIndexesMut;
    use iggy_common::{MemoryPool, MEMORY_POOL};
    use std::sync::{Arc, Once};
    use std::time::Instant;

    static INIT: Once = Once::new();

    fn init_memory_pool() {
        INIT.call_once(|| {
            if MEMORY_POOL.get().is_none() {
                let config = Arc::new(SystemConfig::default());
                MemoryPool::init_pool(&config.memory_pool.into_other());
            }
        });
    }

    fn create_indexes(size: usize) -> IggyIndexesMut {
        init_memory_pool();
        let mut indexes = IggyIndexesMut::with_capacity(size, 0);

        for i in 0..size as u32 {
            indexes.insert(
                i,
                i * 1000,
                1000000 + (i as u64 * 1000),
            );
        }

        indexes
    }

    #[test]
    fn bench_write_performance() {
        init_memory_pool();

        for size in [1_000, 10_000, 100_000].iter() {
            let iterations = 10;
            let mut total_time = std::time::Duration::ZERO;

            for _ in 0..iterations {
                let start = Instant::now();
                let mut indexes = IggyIndexesMut::with_capacity(*size, 0);
                for i in 0..*size as u32 {
                    indexes.insert(i, i * 1000, 1000000 + (i as u64 * 1000));
                }
                total_time += start.elapsed();
                drop(indexes); // Ensure cleanup
            }

            let avg_time = total_time / iterations;
            let throughput = (*size as f64) / avg_time.as_secs_f64();

            println!(
                "Write {} entries: avg {:?} ({:.0} writes/sec)",
                size, avg_time, throughput
            );
        }
    }

    #[test]
    fn bench_search_comparison() {
        for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
            let indexes = create_indexes(*size);
            let target_timestamp = 1000000 + ((*size / 2) as u64 * 1000);
            let iterations = 10_000;

            // Benchmark regular search
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = indexes.find_by_timestamp(target_timestamp);
            }
            let regular_time = start.elapsed();

            // Benchmark block-aware search
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = indexes.find_by_timestamp_block_aware(target_timestamp);
            }
            let block_time = start.elapsed();

            let regular_ns = regular_time.as_nanos() / iterations;
            let block_ns = block_time.as_nanos() / iterations;
            let improvement = ((regular_ns as f64 - block_ns as f64) / regular_ns as f64) * 100.0;

            println!(
                "Search {} entries: regular {:>5}ns, block-aware {:>5}ns, improvement: {:.1}%",
                size, regular_ns, block_ns, improvement
            );
        }
    }

    #[test]
    fn bench_sequential_read() {
        for size in [1_000, 10_000, 100_000].iter() {
            let indexes = create_indexes(*size);
            let iterations = 100;

            // Benchmark entry-by-entry read
            let start = Instant::now();
            for _ in 0..iterations {
                for i in 0..*size as u32 {
                    let _ = indexes.get(i);
                }
            }
            let entry_time = start.elapsed();

            // Benchmark block-by-block read
            let block_count = indexes.block_count();
            let start = Instant::now();
            for _ in 0..iterations {
                for i in 0..block_count {
                    let _ = indexes.get_block(i);
                }
            }
            let block_time = start.elapsed();

            let entry_ns = entry_time.as_nanos() / (iterations * *size as u128);
            let block_ns = block_time.as_nanos() / (iterations * block_count as u128);

            println!(
                "Sequential read {} entries: per-entry {:>5}ns, per-block {:>5}ns",
                size, entry_ns, block_ns
            );
        }
    }

    #[test]
    fn bench_memory_footprint() {
        for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
            let indexes = create_indexes(*size);
            let memory_bytes = indexes.count() as usize * std::mem::size_of::<iggy_common::IggyIndex>();
            let memory_mb = memory_bytes as f64 / 1024.0 / 1024.0;

            println!(
                "Memory for {} entries: {:.2} MB ({} bytes per entry)",
                size,
                memory_mb,
                std::mem::size_of::<iggy_common::IggyIndex>()
            );
        }
    }

    #[test]
    fn bench_search_patterns() {
        let size = 100_000;
        let indexes = create_indexes(size);
        let iterations = 10_000;

        let patterns = [
            ("first", 1000000),
            ("middle", 1000000 + ((size / 2) as u64 * 1000)),
            ("last", 1000000 + ((size - 1) as u64 * 1000)),
            ("not_found", 1000000 + (size as u64 * 1000) + 5000),
        ];

        println!("\nSearch patterns for {} entries:", size);

        for (pattern_name, target_ts) in patterns.iter() {
            // Regular search
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = indexes.find_by_timestamp(*target_ts);
            }
            let regular_ns = start.elapsed().as_nanos() / iterations;

            // Block-aware search
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = indexes.find_by_timestamp_block_aware(*target_ts);
            }
            let block_ns = start.elapsed().as_nanos() / iterations;

            let improvement = ((regular_ns as f64 - block_ns as f64) / regular_ns as f64) * 100.0;

            println!(
                "  {:<12}: regular {:>5}ns, block-aware {:>5}ns, improvement: {:>5.1}%",
                pattern_name, regular_ns, block_ns, improvement
            );
        }
    }

    #[test]
    fn bench_cache_efficiency() {
        let size = 100_000;
        let indexes = create_indexes(size);

        println!("\nCache efficiency metrics for {} entries:", size);
        println!("  Index entry size: {} bytes", std::mem::size_of::<iggy_common::IggyIndex>());
        println!("  Entries per cache line: 4");
        println!("  Total blocks: {}", indexes.block_count());
        println!("  Buffer aligned: {}", indexes.is_buffer_aligned());

        // Calculate expected cache misses for binary search
        let entry_count = size as f64;
        let block_count = (size / 4) as f64;

        let regular_probes = entry_count.log2().ceil();
        let block_probes = block_count.log2().ceil();

        // Assuming worst case: each entry access spans 2 cache lines in regular search
        let regular_cache_lines = regular_probes * 2.0;
        let block_cache_lines = block_probes; // Each block = 1 cache line

        let cache_reduction = ((regular_cache_lines - block_cache_lines) / regular_cache_lines) * 100.0;

        println!("\nExpected cache behavior:");
        println!("  Regular search probes: {:.0}", regular_probes);
        println!("  Regular cache lines loaded: {:.0}", regular_cache_lines);
        println!("  Block search probes: {:.0}", block_probes);
        println!("  Block cache lines loaded: {:.0}", block_cache_lines);
        println!("  Expected cache miss reduction: {:.1}%", cache_reduction);
    }
}
