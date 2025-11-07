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

use bytes::Bytes;
use iggy::prelude::IggyMessage;

#[must_use]
pub fn create_test_messages(count: u32) -> Vec<IggyMessage> {
    let mut messages = Vec::new();
    for i in 0..count {
        let id = u128::from(i + 1);
        let payload = Bytes::from(format!("test message {i}"));
        messages.push(
            IggyMessage::builder()
                .id(id)
                .payload(payload)
                .build()
                .expect("Should be able to create message"),
        );
    }
    messages
}
