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

//! The reads of the object store that rebuild the lost segment chain of a
//! tiered partition, see `partitions::StoreViewReader`. The partition
//! reconciler runs them before it loads such a partition.

use crate::IggyShard;
use crate::shards_table::ShardsTable;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::StoreViewReader;
use std::rc::Rc;

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the reader of what the store holds of a partition
    /// incarnation, or take it away with `None`. A read already in flight
    /// ends on the reader that started it.
    pub fn set_store_view_reader(&self, reader: Option<Rc<dyn StoreViewReader>>) {
        *self.store_view_reader.borrow_mut() = reader;
    }

    /// The reader of what the store holds of a partition incarnation. `None`
    /// on a shard that was given none, as before its node passes the store
    /// identity check.
    #[must_use]
    pub fn store_view_reader(&self) -> Option<Rc<dyn StoreViewReader>> {
        self.store_view_reader.borrow().clone()
    }
}
