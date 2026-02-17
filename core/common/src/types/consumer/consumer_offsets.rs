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

use super::consumer_offset::ConsumerOffset;

#[derive(Debug, Clone)]
pub struct ConsumerOffsets(papaya::HashMap<usize, ConsumerOffset>);

impl ConsumerOffsets {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(papaya::HashMap::with_capacity(capacity))
    }
}

impl<I> From<I> for ConsumerOffsets
where
    I: IntoIterator<Item = (usize, ConsumerOffset)>,
{
    fn from(iter: I) -> Self {
        Self(papaya::HashMap::from_iter(iter))
    }
}

impl std::ops::Deref for ConsumerOffsets {
    type Target = papaya::HashMap<usize, ConsumerOffset>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ConsumerOffsets {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
