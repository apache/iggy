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

use iggy::prelude::StreamDetails as RustStreamDetails;
use std::sync::Arc;

pub(crate) struct StreamDetails {
    pub(crate) inner: Arc<RustStreamDetails>,
}

impl StreamDetails {
    pub(crate) fn new(inner: RustStreamDetails) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) fn id(&self) -> u32 {
        self.inner.id
    }

    pub(crate) fn name(&self) -> String {
        self.inner.name.to_string()
    }

    pub(crate) fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    pub(crate) fn topics_count(&self) -> u32 {
        self.inner.topics_count
    }
}

pub(crate) unsafe fn delete_stream_details(details: *mut StreamDetails) {
    if details.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(details));
    }
}
