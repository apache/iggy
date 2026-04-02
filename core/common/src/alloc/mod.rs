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

pub(crate) mod buffer;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod memory_pool;

// On WASM, provide the core alloc types that buffer.rs needs without the pool.
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm_alloc_types {
    use aligned_vec::{AVec, ConstAlign};
    pub const ALIGNMENT: usize = 4096;
    pub type AlignedBuffer = AVec<u8, ConstAlign<4096>>;
}
