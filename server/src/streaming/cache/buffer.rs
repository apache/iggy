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

use crate::streaming::local_sizeable::RealSize;

use super::memory_tracker::CacheMemoryTracker;
use atone::Vc;
use iggy::utils::byte_size::IggyByteSize;
use std::fmt::Debug;
use std::ops::Index;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Debug)]
pub struct SmartCache<T: RealSize + Debug> {
    buffer: Vc<T>,
    memory_tracker: Arc<CacheMemoryTracker>,
    current_size: IggyByteSize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<T> SmartCache<T>
where
    T: RealSize + Clone + Debug,
{
    pub fn new() -> Self {
        let current_size = IggyByteSize::default();
        let buffer = Vc::new();
        let memory_tracker = CacheMemoryTracker::get_instance().unwrap();

        Self {
            buffer,
            memory_tracker,
            current_size,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    // Used only for cache validation tests
    #[cfg(test)]
    pub fn to_vec(&self) -> Vec<T> {
        let mut vec = Vec::with_capacity(self.buffer.len());
        vec.extend(self.buffer.iter().cloned());
        vec
    }

    /// Pushes an element to the buffer, and if adding the element would exceed the memory limit,
    /// removes the oldest elements until there's enough space for the new element.
    /// It's preferred to use `extend` instead of this method.
    pub fn push_safe(&mut self, element: T) {
        let element_size = element.real_size();

        while !self.memory_tracker.will_fit_into_cache(element_size) {
            if let Some(oldest_element) = self.buffer.pop_front() {
                let oldest_size = oldest_element.real_size();
                self.memory_tracker
                    .decrement_used_memory(oldest_size.as_bytes_u64());
                self.current_size -= oldest_size;
            }
        }

        self.memory_tracker
            .increment_used_memory(element_size.as_bytes_u64());
        self.current_size += element_size;
        self.buffer.push_back(element);
    }

    /// Removes the oldest elements until there's enough space for the new element.
    pub fn evict_by_size(&mut self, size_to_remove: u64) {
        let mut removed_size = IggyByteSize::default();

        while let Some(element) = self.buffer.pop_front() {
            if removed_size >= size_to_remove {
                break;
            }
            let elem_size = element.real_size();
            self.memory_tracker
                .decrement_used_memory(elem_size.as_bytes_u64());
            self.current_size -= elem_size;
            removed_size += elem_size;
        }
    }

    pub fn purge(&mut self) {
        self.buffer.clear();
        self.memory_tracker
            .decrement_used_memory(self.current_size.as_bytes_u64());
        self.current_size = IggyByteSize::default();
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn current_size(&self) -> IggyByteSize {
        self.current_size
    }

    /// Extends the buffer with the given elements, and always adding the elements,
    /// even if it exceeds the memory limit.
    pub fn extend(&mut self, elements: impl IntoIterator<Item = T>) {
        let elements = elements.into_iter().inspect(|element| {
            let element_size = element.real_size();
            self.memory_tracker
                .increment_used_memory(element_size.as_bytes_u64());
            self.current_size += element_size;
        });
        self.buffer.extend(elements);
    }

    /// Always appends the element into the buffer, even if it exceeds the memory limit.
    pub fn append(&mut self, element: T) {
        let element_size = element.real_size();
        self.memory_tracker
            .increment_used_memory(element_size.as_bytes_u64());
        self.current_size += element_size;
        self.buffer.push(element);
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.buffer.iter()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn get_metrics(&self) -> CacheMetrics {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_ratio = if total > 0 {
            hits as f32 / total as f32
        } else {
            0.0
        };

        CacheMetrics {
            hits,
            misses,
            hit_ratio,
        }
    }

    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
}

impl<T> Index<usize> for SmartCache<T>
where
    T: RealSize + Clone + Debug,
{
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.buffer[index]
    }
}

impl<T: RealSize + Clone + Debug> Default for SmartCache<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub hit_ratio: f32,
}
