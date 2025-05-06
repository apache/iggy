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

mod index;
mod index_view;
mod indexes;
mod message;
mod message_header;
mod message_header_view;
mod message_view;
mod messages_batch;
mod user_headers;

pub const INDEX_SIZE: usize = 16;

pub use index::IggyIndex;
pub use index_view::IggyIndexView;
pub use indexes::IggyIndexes;
pub use message::{IggyMessage, MAX_PAYLOAD_SIZE, MAX_USER_HEADERS_SIZE};
pub use message_header::{
    IggyMessageHeader, IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE,
    IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE, IGGY_MESSAGE_HEADER_RANGE, IGGY_MESSAGE_HEADER_SIZE,
    IGGY_MESSAGE_ID_OFFSET_RANGE, IGGY_MESSAGE_OFFSET_OFFSET_RANGE,
    IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE, IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE,
    IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE,
};
pub use message_header_view::IggyMessageHeaderView;
pub use message_view::{IggyMessageView, IggyMessageViewIterator};
pub use messages_batch::IggyMessagesBatch;
pub use user_headers::{HeaderKey, HeaderKind, HeaderValue};
