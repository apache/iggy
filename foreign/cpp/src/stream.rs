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
use iggy::prelude::StreamDetails as RustStreamDetails;

impl From<RustStreamDetails> for ffi::StreamDetails {
    fn from(stream: RustStreamDetails) -> Self {
        ffi::StreamDetails {
            id: stream.id,
            created_at: stream.created_at.as_micros(),
            name: stream.name,
            size_bytes: stream.size.as_bytes_u64(),
            messages_count: stream.messages_count,
            topics_count: stream.topics_count,
            topics: stream.topics.into_iter().map(ffi::Topic::from).collect(),
        }
    }
}
