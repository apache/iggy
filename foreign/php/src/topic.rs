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

use std::sync::Arc;

use ext_php_rs::{
    boxed::ZBox,
    exception::PhpResult,
    php_class, php_impl,
    types::{ZendHashTable, Zval},
};
use iggy::prelude::{
    IggyByteSize, IggyDuration, Partition as RustPartition, Topic as RustTopic,
    TopicDetails as RustTopicDetails,
};

use crate::options::{options_array, unsigned_scalar};

#[php_class]
#[php(name = "Iggy\\TopicDetails")]
pub struct TopicDetails {
    pub(crate) inner: RustTopicDetails,
}

impl From<RustTopicDetails> for TopicDetails {
    fn from(inner: RustTopicDetails) -> Self {
        Self { inner }
    }
}

#[php_impl]
impl TopicDetails {
    #[php(getter)]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    #[php(getter, change_case = "snake_case")]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    /// One `Iggy\Partition` per partition of the topic.
    ///
    /// The partitions are rebuilt on each getter call; cache the result in PHP if they will
    /// be read repeatedly.
    #[php(getter)]
    pub fn partitions(&self) -> Vec<Partition> {
        self.inner
            .partitions
            .iter()
            .cloned()
            .map(Partition::from)
            .collect()
    }

    /// Options the creating or updating client set, as option name to scalar value.
    /// Integers above `PHP_INT_MAX`, such as never-expire and unlimited
    /// (18446744073709551615), are decimal strings.
    #[php(getter)]
    pub fn options(&self) -> PhpResult<ZBox<ZendHashTable>> {
        options_array(&self.inner.options, true)
    }

    /// Options the server filled in from its catalog defaults.
    #[php(getter, change_case = "snake_case")]
    pub fn derived_options(&self) -> PhpResult<ZBox<ZendHashTable>> {
        options_array(&self.inner.options, false)
    }

    /// What the topic is, `normal` or `geo`, fixed at creation.
    #[php(getter, change_case = "snake_case")]
    pub fn topic_type(&self) -> String {
        self.inner.resolved_options().topic_type.to_string()
    }

    /// How a geo topic writes its messages, `geo` or `geo_tiered`, fixed at creation. Null
    /// for a normal topic.
    #[php(getter, change_case = "snake_case")]
    pub fn geo_mode(&self) -> Option<String> {
        self.inner
            .resolved_options()
            .geo_mode
            .map(|geo_mode| geo_mode.to_string())
    }

    /// The longest time a send waits for its upload to start, in microseconds (a decimal
    /// string above `PHP_INT_MAX`). Null when the topic sets none, and each node applies its own
    /// `[tiered_storage] fresh_upload_interval`.
    #[php(getter, change_case = "snake_case")]
    pub fn fresh_upload_interval_micros(&self) -> PhpResult<Zval> {
        duration_micros(
            self.inner
                .resolved_options()
                .geo_durations
                .fresh_upload_interval,
        )
    }

    /// The shortest time between two uploads of the positions that stored consumer group
    /// offsets wait for, in microseconds (a decimal string above `PHP_INT_MAX`). Null when the
    /// topic sets none, and each node applies its own `[geo] consumer_offset_upload_interval`.
    #[php(getter, change_case = "snake_case")]
    pub fn consumer_offset_upload_interval_micros(&self) -> PhpResult<Zval> {
        duration_micros(
            self.inner
                .resolved_options()
                .geo_durations
                .consumer_offset_upload_interval,
        )
    }

    /// How often a node uploads the changed positions of the topic's consumer groups, in
    /// microseconds (a decimal string above `PHP_INT_MAX`). Null when the topic sets none, and
    /// each node applies its own `[geo] positions_publish_interval`.
    #[php(getter, change_case = "snake_case")]
    pub fn positions_publish_interval_micros(&self) -> PhpResult<Zval> {
        duration_micros(
            self.inner
                .resolved_options()
                .geo_durations
                .positions_publish_interval,
        )
    }

    /// How long the tombstone and the objects of the deleted topic stay in the object store, in
    /// microseconds (a decimal string above `PHP_INT_MAX`). Null when the topic sets none, and
    /// each node applies its own `[geo] tombstone_grace`.
    #[php(getter, change_case = "snake_case")]
    pub fn tombstone_grace_micros(&self) -> PhpResult<Zval> {
        duration_micros(self.inner.resolved_options().geo_durations.tombstone_grace)
    }

    /// How long, after a node starts, the group polls of the topic's partitions wait for the
    /// positions of the geo peers, in microseconds (a decimal string above `PHP_INT_MAX`). Null
    /// when the topic sets none, and each node applies its own `[geo] positions_resume_timeout`.
    #[php(getter, change_case = "snake_case")]
    pub fn positions_resume_timeout_micros(&self) -> PhpResult<Zval> {
        duration_micros(
            self.inner
                .resolved_options()
                .geo_durations
                .positions_resume_timeout,
        )
    }

    /// The longest wait between two long-term rewrites of a partition, in microseconds (a
    /// decimal string above `PHP_INT_MAX`). Null when the topic sets none, and each node applies
    /// its own `[geo] long_term_rewrite_max_interval`.
    #[php(getter, change_case = "snake_case")]
    pub fn long_term_rewrite_max_interval_micros(&self) -> PhpResult<Zval> {
        duration_micros(
            self.inner
                .resolved_options()
                .geo_durations
                .long_term_rewrite_max_interval,
        )
    }

    /// The memory each node keeps for what the topic's reads fetch from the object store, in
    /// bytes (a decimal string above `PHP_INT_MAX`). 0 keeps nothing they fetch. Null when the
    /// topic sets none, and each node applies its own `[tiered_storage] read_cache_size`.
    #[php(getter, change_case = "snake_case")]
    pub fn read_cache_size(&self) -> PhpResult<Zval> {
        byte_size(self.inner.resolved_options().read_cache.size)
    }

    /// The most that readahead fetches in front of a sequential reader of the topic, in bytes (a
    /// decimal string above `PHP_INT_MAX`). 0 turns readahead off. Null when the topic sets none,
    /// and each node applies its own `[tiered_storage] read_ahead`.
    #[php(getter, change_case = "snake_case")]
    pub fn read_ahead(&self) -> PhpResult<Zval> {
        byte_size(self.inner.resolved_options().read_cache.read_ahead)
    }
}

/// One topic of a `TopicsPage`.
#[php_class]
#[php(name = "Iggy\\Topic")]
#[derive(Clone)]
pub struct Topic {
    // The Rust type is not Clone, and each read of the page's items copies the topic.
    inner: Arc<RustTopic>,
}

impl From<RustTopic> for Topic {
    fn from(topic: RustTopic) -> Self {
        Self {
            inner: Arc::new(topic),
        }
    }
}

#[php_impl]
impl Topic {
    #[php(getter)]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// Creation time in microseconds.
    #[php(getter, change_case = "snake_case")]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.as_micros()
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn size_bytes(&self) -> u64 {
        self.inner.size.as_bytes_u64()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    #[php(getter, change_case = "snake_case")]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    /// Options the creating or updating client set, as option name to scalar value.
    /// Integers above `PHP_INT_MAX`, such as never-expire and unlimited
    /// (18446744073709551615), are decimal strings.
    #[php(getter)]
    pub fn options(&self) -> PhpResult<ZBox<ZendHashTable>> {
        options_array(&self.inner.options, true)
    }

    /// Options the server filled in from its catalog defaults.
    #[php(getter, change_case = "snake_case")]
    pub fn derived_options(&self) -> PhpResult<ZBox<ZendHashTable>> {
        options_array(&self.inner.options, false)
    }

    /// What the topic is, `normal` or `geo`, fixed at creation.
    #[php(getter, change_case = "snake_case")]
    pub fn topic_type(&self) -> String {
        self.inner.resolved_options().topic_type.to_string()
    }

    /// How a geo topic writes its messages, `geo` or `geo_tiered`, fixed at creation. Null
    /// for a normal topic.
    #[php(getter, change_case = "snake_case")]
    pub fn geo_mode(&self) -> Option<String> {
        self.inner
            .resolved_options()
            .geo_mode
            .map(|geo_mode| geo_mode.to_string())
    }

    /// The longest time a send waits for its upload to start, in microseconds (a decimal
    /// string above `PHP_INT_MAX`). Null when the topic sets none, and each node applies its own
    /// `[tiered_storage] fresh_upload_interval`.
    #[php(getter, change_case = "snake_case")]
    pub fn fresh_upload_interval_micros(&self) -> PhpResult<Zval> {
        duration_micros(
            self.inner
                .resolved_options()
                .geo_durations
                .fresh_upload_interval,
        )
    }

    /// The shortest time between two uploads of the positions that stored consumer group
    /// offsets wait for, in microseconds (a decimal string above `PHP_INT_MAX`). Null when the
    /// topic sets none, and each node applies its own `[geo] consumer_offset_upload_interval`.
    #[php(getter, change_case = "snake_case")]
    pub fn consumer_offset_upload_interval_micros(&self) -> PhpResult<Zval> {
        duration_micros(
            self.inner
                .resolved_options()
                .geo_durations
                .consumer_offset_upload_interval,
        )
    }

    /// How often a node uploads the changed positions of the topic's consumer groups, in
    /// microseconds (a decimal string above `PHP_INT_MAX`). Null when the topic sets none, and
    /// each node applies its own `[geo] positions_publish_interval`.
    #[php(getter, change_case = "snake_case")]
    pub fn positions_publish_interval_micros(&self) -> PhpResult<Zval> {
        duration_micros(
            self.inner
                .resolved_options()
                .geo_durations
                .positions_publish_interval,
        )
    }

    /// How long the tombstone and the objects of the deleted topic stay in the object store, in
    /// microseconds (a decimal string above `PHP_INT_MAX`). Null when the topic sets none, and
    /// each node applies its own `[geo] tombstone_grace`.
    #[php(getter, change_case = "snake_case")]
    pub fn tombstone_grace_micros(&self) -> PhpResult<Zval> {
        duration_micros(self.inner.resolved_options().geo_durations.tombstone_grace)
    }

    /// How long, after a node starts, the group polls of the topic's partitions wait for the
    /// positions of the geo peers, in microseconds (a decimal string above `PHP_INT_MAX`). Null
    /// when the topic sets none, and each node applies its own `[geo] positions_resume_timeout`.
    #[php(getter, change_case = "snake_case")]
    pub fn positions_resume_timeout_micros(&self) -> PhpResult<Zval> {
        duration_micros(
            self.inner
                .resolved_options()
                .geo_durations
                .positions_resume_timeout,
        )
    }

    /// The longest wait between two long-term rewrites of a partition, in microseconds (a
    /// decimal string above `PHP_INT_MAX`). Null when the topic sets none, and each node applies
    /// its own `[geo] long_term_rewrite_max_interval`.
    #[php(getter, change_case = "snake_case")]
    pub fn long_term_rewrite_max_interval_micros(&self) -> PhpResult<Zval> {
        duration_micros(
            self.inner
                .resolved_options()
                .geo_durations
                .long_term_rewrite_max_interval,
        )
    }

    /// The memory each node keeps for what the topic's reads fetch from the object store, in
    /// bytes (a decimal string above `PHP_INT_MAX`). 0 keeps nothing they fetch. Null when the
    /// topic sets none, and each node applies its own `[tiered_storage] read_cache_size`.
    #[php(getter, change_case = "snake_case")]
    pub fn read_cache_size(&self) -> PhpResult<Zval> {
        byte_size(self.inner.resolved_options().read_cache.size)
    }

    /// The most that readahead fetches in front of a sequential reader of the topic, in bytes (a
    /// decimal string above `PHP_INT_MAX`). 0 turns readahead off. Null when the topic sets none,
    /// and each node applies its own `[tiered_storage] read_ahead`.
    #[php(getter, change_case = "snake_case")]
    pub fn read_ahead(&self) -> PhpResult<Zval> {
        byte_size(self.inner.resolved_options().read_cache.read_ahead)
    }
}

/// One partition of a topic.
#[php_class]
#[php(name = "Iggy\\Partition")]
#[derive(Clone)]
pub struct Partition {
    pub(crate) inner: RustPartition,
}

impl From<RustPartition> for Partition {
    fn from(inner: RustPartition) -> Self {
        Self { inner }
    }
}

#[php_impl]
impl Partition {
    #[php(getter)]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn segments_count(&self) -> u32 {
        self.inner.segments_count
    }

    #[php(getter, change_case = "snake_case")]
    pub fn current_offset(&self) -> u64 {
        self.inner.current_offset
    }

    #[php(getter, change_case = "snake_case")]
    pub fn size_bytes(&self) -> u64 {
        self.inner.size.as_bytes_u64()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    /// Node ids that store the partition, in member order. While one is down, a write still
    /// commits if a quorum of the others is in sync.
    #[php(getter)]
    pub fn members(&self) -> Vec<u8> {
        self.inner.members.clone()
    }

    /// Node ids among `members` that the partition's primary last reported in sync and that
    /// are up. Empty until the primary first reports, and while no member that is up can
    /// lead it.
    #[php(getter, change_case = "snake_case")]
    pub fn in_sync_members(&self) -> Vec<u8> {
        self.inner.in_sync_members.clone()
    }
}

/// A duration of a geo topic in microseconds, null when the topic sets none.
fn duration_micros(duration: Option<IggyDuration>) -> PhpResult<Zval> {
    duration.map_or_else(
        || Ok(Zval::new()),
        |duration| unsigned_scalar(duration.as_micros()),
    )
}

/// A read cache setting of a topic in bytes, null when the topic sets none.
fn byte_size(size: Option<IggyByteSize>) -> PhpResult<Zval> {
    size.map_or_else(
        || Ok(Zval::new()),
        |size| unsigned_scalar(size.as_bytes_u64()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::WireDecode;
    use iggy_binary_protocol::responses::topics::GetTopicResponse;
    use serde_json::Value;

    const FIXTURE: &str = include_str!("../tests/fixtures/resource_options.json");

    type Getter<T> = fn(&T) -> PhpResult<Zval>;

    #[test]
    fn given_topic_response_vectors_when_reading_durations_and_read_cache_should_match_projections()
    {
        let details_getters: [(&str, Getter<TopicDetails>); 8] = [
            (
                "fresh_upload_interval",
                TopicDetails::fresh_upload_interval_micros,
            ),
            (
                "consumer_offset_upload_interval",
                TopicDetails::consumer_offset_upload_interval_micros,
            ),
            (
                "positions_publish_interval",
                TopicDetails::positions_publish_interval_micros,
            ),
            ("tombstone_grace", TopicDetails::tombstone_grace_micros),
            (
                "positions_resume_timeout",
                TopicDetails::positions_resume_timeout_micros,
            ),
            (
                "long_term_rewrite_max_interval",
                TopicDetails::long_term_rewrite_max_interval_micros,
            ),
            ("read_cache_size", TopicDetails::read_cache_size),
            ("read_ahead", TopicDetails::read_ahead),
        ];
        let topic_getters: [Getter<Topic>; 8] = [
            Topic::fresh_upload_interval_micros,
            Topic::consumer_offset_upload_interval_micros,
            Topic::positions_publish_interval_micros,
            Topic::tombstone_grace_micros,
            Topic::positions_resume_timeout_micros,
            Topic::long_term_rewrite_max_interval_micros,
            Topic::read_cache_size,
            Topic::read_ahead,
        ];
        let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
        for vector in fixture["topic_responses"].as_array().expect("vector list") {
            let bytes = decode_hex(vector["response_hex"].as_str().expect("hex"));
            let (response, _) = GetTopicResponse::decode(&bytes).expect("topic decodes");
            let details = RustTopicDetails::try_from(response).expect("topic converts");
            let topic = Topic::from(RustTopic {
                id: details.id,
                created_at: details.created_at,
                name: details.name.clone(),
                size: details.size,
                messages_count: details.messages_count,
                partitions_count: details.partitions_count,
                options: details.options.clone(),
            });
            let details = TopicDetails::from(details);
            let expected = &vector["expected"];

            for ((key, details_getter), topic_getter) in
                details_getters.into_iter().zip(topic_getters)
            {
                // The projection lists only the settings the topic sets, and 0 is one of them.
                let number = expected[key]
                    .as_str()
                    .map(|number| number.parse::<i64>().expect("value fits a PHP int"));
                for value in [details_getter(&details), topic_getter(&topic)] {
                    let value = value.expect("getter reads");
                    assert_eq!(value.long(), number, "{}: {key}", vector["case"]);
                    assert_eq!(
                        value.is_null(),
                        number.is_none(),
                        "{}: {key}",
                        vector["case"]
                    );
                }
            }
        }
    }

    fn decode_hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex byte"))
            .collect()
    }
}
