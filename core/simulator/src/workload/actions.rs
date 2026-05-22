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

//! Workload-emittable server commands. Variant order is part of the
//! determinism contract; the first three positions lock the hash baseline.
//! Append only; never reorder or insert.

use strum::{EnumCount, EnumIter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumCount, EnumIter)]
#[repr(u8)]
pub enum Action {
    // DO NOT REORDER (hash baseline depends on these indices).
    CreateStream,
    SendMessages,
    StoreConsumerOffset2,
    DeleteStream,
    UpdateStream,
    PurgeStream,
    CreateTopic,
    UpdateTopic,
    DeleteTopic,
    PurgeTopic,
    CreatePartitions,
    DeletePartitions,
    DeleteSegments,
    CreateConsumerGroup,
    DeleteConsumerGroup,
    CreateUser,
    UpdateUser,
    DeleteUser,
    ChangePassword,
    UpdatePermissions,
    CreatePersonalAccessToken,
    DeletePersonalAccessToken,
    StoreConsumerOffset,
    DeleteConsumerOffset,
    DeleteConsumerOffset2,
}

// Lock the discriminants of the first three variants. Inserting a new variant before
// any of these (or reordering them) shifts every downstream discriminant
// and silently corrupts both indexing and the locked baseline.
const _: () = {
    assert!(Action::CreateStream as u8 == 0);
    assert!(Action::SendMessages as u8 == 1);
    assert!(Action::StoreConsumerOffset2 as u8 == 2);
};
