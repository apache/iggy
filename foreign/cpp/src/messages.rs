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

use crate::ffi;
use iggy::prelude::{IggyMessage as RustIggyMessage, PolledMessages as RustPolledMessages};

impl From<RustIggyMessage> for ffi::IggyMessagePolled {
    fn from(m: RustIggyMessage) -> Self {
        let id = m.header.id;
        ffi::IggyMessagePolled {
            id_lo: id as u64,
            id_hi: (id >> 64) as u64,
            offset: m.header.offset,
            timestamp: m.header.timestamp,
            origin_timestamp: m.header.origin_timestamp,
            payload: m.payload.to_vec(),
            user_headers: m.user_headers.map(|h| h.to_vec()).unwrap_or_default(),
        }
    }
}

impl From<RustPolledMessages> for ffi::PolledMessages {
    fn from(p: RustPolledMessages) -> Self {
        ffi::PolledMessages {
            partition_id: p.partition_id,
            current_offset: p.current_offset,
            count: p.count,
            messages: p.messages.into_iter().map(Into::into).collect(),
        }
    }
}
