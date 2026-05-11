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

use ext_php_rs::{php_class, php_impl};
use iggy::prelude::StreamDetails as RustStreamDetails;

#[php_class]
pub struct StreamDetails {
    pub(crate) inner: RustStreamDetails,
}

impl From<RustStreamDetails> for StreamDetails {
    fn from(stream_details: RustStreamDetails) -> Self {
        Self {
            inner: stream_details,
        }
    }
}

#[php_impl]
impl StreamDetails {
    #[php(getter)]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    #[php(getter)]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    #[php(getter)]
    pub fn topics_count(&self) -> u32 {
        self.inner.topics_count
    }
}
