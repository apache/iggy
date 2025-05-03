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

use iggy::utils::byte_size::IggyByteSize;

/// Trait for types that return their size in bytes.
/// repeated from the sdk because of the coherence rule, see messages.rs
pub trait LocalSizeable {
    fn get_size_bytes(&self) -> IggyByteSize;
}

/// Trait for calculating the real memory size of a type, including all its fields
/// and any additional overhead from containers like Arc, Vec, etc.
pub trait RealSize {
    fn real_size(&self) -> IggyByteSize;
}
