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

mod iggy_message;
mod index;
mod index_view;
mod indexes;
mod indexes_mut;
mod message_header;
mod message_header_view;
mod message_header_view_mut;
mod message_view;
mod message_view_mut;
mod messages_batch;
mod messages_batch_mut;
mod messages_batch_set;
pub mod partitioning;
pub mod partitioning_kind;
mod poll_metadata;
pub mod polled_messages;
pub mod polling_kind;
pub mod polling_strategy;
mod user_headers;

pub const INDEX_SIZE: usize = 16;

pub use crate::commands::messages::flush_unsaved_buffer::FlushUnsavedBuffer;
pub use crate::commands::messages::poll_messages::PollMessages;
pub use crate::commands::messages::send_messages::SendMessages;
pub use iggy_message::{IggyMessage, MAX_PAYLOAD_SIZE, MAX_USER_HEADERS_SIZE};
pub use index::IggyIndex;
pub use index_view::IggyIndexView;
pub use indexes::IggyIndexes;
pub use indexes_mut::IggyIndexesMut;
pub use message_header::{
    IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE, IGGY_MESSAGE_HEADER_RANGE, IGGY_MESSAGE_HEADER_SIZE,
    IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE, IGGY_MESSAGE_ID_OFFSET_RANGE,
    IGGY_MESSAGE_OFFSET_OFFSET_RANGE, IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE,
    IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE, IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE,
    IggyMessageHeader,
};
pub use message_header_view::IggyMessageHeaderView;
pub use message_header_view_mut::IggyMessageHeaderViewMut;
pub use message_view::{IggyMessageView, IggyMessageViewIterator};
pub use message_view_mut::{IggyMessageViewMut, IggyMessageViewMutIterator};
pub use messages_batch::IggyMessagesBatch;
pub use messages_batch_mut::IggyMessagesBatchMut;
pub use messages_batch_set::IggyMessagesBatchSet;
pub use partitioning::Partitioning;
pub use partitioning_kind::PartitioningKind;
pub use poll_metadata::IggyPollMetadata;
pub use polled_messages::PolledMessages;
pub use polling_kind::PollingKind;
pub use polling_strategy::PollingStrategy;
pub use user_headers::{
    HeaderEntry, HeaderField, HeaderKey, HeaderKind, HeaderValue, KeyMarker, UserHeaders,
    ValueMarker, deserialize_headers, serialize_headers,
};
