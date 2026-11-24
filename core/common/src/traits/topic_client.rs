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

use crate::{
    Identifier, IggyError, NamedPageQuery, Page, Topic, TopicCreateOptions, TopicDetails,
    TopicUpdateOptions,
};
use async_trait::async_trait;

/// This trait defines the methods to interact with the topic module.
#[allow(clippy::too_many_arguments)]
#[async_trait]
#[whichever::dispatchable]
pub trait TopicClient {
    /// Get the info about a specific topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to read the topics.
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<TopicDetails>, IggyError>;
    /// Get the info about all the topics, in id order.
    ///
    /// The list is read page by page, so it is not one consistent read. A topic
    /// that someone creates or deletes during the read can appear or not
    /// appear. If the stream goes missing after the first page, the read
    /// fails as [`Self::get_topics_page`] does for a caller with a global
    /// read grant, and ends there for any other caller.
    /// A server that answers a page with the cursor it was sent fails the read
    /// with `IggyError::InvalidFormat`.
    ///
    /// Authentication is required. The list holds the topics the caller may read.
    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError>;
    /// One page of the topics of a stream. Read the next page with
    /// [`NamedPageQuery::next`] until the page has no `next`.
    ///
    /// For a missing stream, or one the caller may read no topic of, the first
    /// page is empty and has no `next`. For a caller with a global read grant,
    /// a page read with a cursor fails instead, because the stream was deleted
    /// during the walk: a binary transport returns
    /// `IggyError::StreamIdNotFound`, and `HttpClient` returns
    /// `IggyError::ResourceNotFound`.
    ///
    /// Authentication is required. The page holds the topics the caller may read.
    async fn get_topics_page(
        &self,
        stream_id: &Identifier,
        query: &NamedPageQuery,
    ) -> Result<Page<Topic>, IggyError>;
    /// Create a new topic.
    ///
    /// Every knob rides `options`; an absent key resolves against the
    /// server's defaults at admission, so a future key costs no signature
    /// change here. Authentication is required, and the permission to manage
    /// the topics.
    ///
    /// If the new partitions would take the node past `[metadata]
    /// partitions_max`, a binary transport returns
    /// `IggyError::PartitionsLimitReached`. `HttpClient` returns
    /// `IggyError::HttpResponseError` instead.
    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        options: &TopicCreateOptions,
    ) -> Result<TopicDetails, IggyError>;
    /// Patch a topic by unique ID or name: rename it, change its updatable
    /// options, or reset them to their defaults. Keys `options` leaves out keep
    /// their values.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        options: &TopicUpdateOptions,
    ) -> Result<(), IggyError>;
    /// Delete a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError>;
}
