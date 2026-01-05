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

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use iggy_common::collections::SegmentedSlab;
use imbl::HashMap as ImHashMap;

const ENTRIES: usize = 10_000;
const SEGMENT_SIZE: usize = 1024;

type BenchSlab<T> = SegmentedSlab<T, SEGMENT_SIZE>;

fn setup_imhashmap() -> ImHashMap<usize, usize> {
    let mut map = ImHashMap::new();
    for i in 0..ENTRIES {
        map = map.update(i, i * 2);
    }
    map
}

fn setup_segmented_slab() -> BenchSlab<usize> {
    let mut slab: BenchSlab<usize> = SegmentedSlab::new();
    for i in 0..ENTRIES {
        let (new_slab, _) = slab.insert(i * 2);
        slab = new_slab;
    }
    slab
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(100));

    let im_map = setup_imhashmap();
    let slab = setup_segmented_slab();

    group.bench_function(BenchmarkId::new("ImHashMap", ENTRIES), |b| {
        b.iter(|| {
            for i in 0..100 {
                std::hint::black_box(im_map.get(&(i * 100)));
            }
        });
    });

    group.bench_function(BenchmarkId::new("SegmentedSlab", ENTRIES), |b| {
        b.iter(|| {
            for i in 0..100 {
                std::hint::black_box(slab.get(i * 100));
            }
        });
    });

    group.finish();
}

fn bench_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("update");
    group.throughput(Throughput::Elements(1));

    group.bench_function(BenchmarkId::new("ImHashMap", ENTRIES), |b| {
        b.iter_batched(
            setup_imhashmap,
            |mut map| {
                for i in 0..100 {
                    map = map.update(i % ENTRIES, i);
                }
                map
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::new("SegmentedSlab", ENTRIES), |b| {
        b.iter_batched(
            setup_segmented_slab,
            |mut slab| {
                for i in 0..100 {
                    let (new_slab, _) = slab.update(i % ENTRIES, i);
                    slab = new_slab;
                }
                slab
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("clone");
    group.throughput(Throughput::Elements(1));

    let im_map = setup_imhashmap();
    let slab = setup_segmented_slab();

    group.bench_function(BenchmarkId::new("ImHashMap", ENTRIES), |b| {
        b.iter(|| std::hint::black_box(im_map.clone()));
    });

    group.bench_function(BenchmarkId::new("SegmentedSlab", ENTRIES), |b| {
        b.iter(|| std::hint::black_box(slab.clone()));
    });

    group.finish();
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    group.throughput(Throughput::Elements(1));

    group.bench_function("ImHashMap", |b| {
        b.iter_batched(
            ImHashMap::<usize, usize>::new,
            |mut map| {
                for i in 0..100 {
                    map = map.update(i, i);
                }
                map
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("SegmentedSlab", |b| {
        b.iter_batched(
            BenchSlab::<usize>::new,
            |mut slab| {
                for _ in 0..100 {
                    let (new_slab, _) = slab.insert(0usize);
                    slab = new_slab;
                }
                slab
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("remove");
    group.throughput(Throughput::Elements(1));

    group.bench_function(BenchmarkId::new("ImHashMap", ENTRIES), |b| {
        b.iter_batched(
            setup_imhashmap,
            |mut map| {
                for i in 0..100 {
                    map = map.without(&(i % ENTRIES));
                }
                map
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::new("SegmentedSlab", ENTRIES), |b| {
        b.iter_batched(
            setup_segmented_slab,
            |mut slab| {
                for i in 0..100 {
                    let (new_slab, _) = slab.remove(i % ENTRIES);
                    slab = new_slab;
                }
                slab
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_get,
    bench_update,
    bench_clone,
    bench_insert,
    bench_remove
);
criterion_main!(benches);
