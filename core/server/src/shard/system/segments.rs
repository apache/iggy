use crate::shard::IggyShard;
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
use super::COMPONENT;
use crate::streaming::session::Session;
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::locking::IggyRwLockFn;

impl IggyShard {
    pub async fn delete_segments(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        segments_count: u32,
    ) -> Result<(), IggyError> {
        // Assert authentication.
        self.ensure_authenticated(session)?;

        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream_id: {stream_id}"
                )
            })?;
            let topic = self.find_topic(session, &stream, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream_id: {stream_id}, topic_id: {topic_id}"))?;

            self.permissioner.borrow().delete_segments(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to delete segments for user {} on Stream ID: {}, Topic ID: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;
        }

        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let topic = stream
            .get_topic(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with ID: {stream_id}"
                    )
            })?;

        // Lock the current partition.
        let partition_lock = topic.get_partition(partition_id)?;
        let mut partition = partition_lock.write().await;

        partition.segments.sort_by_key(|a| a.start_offset());

        // Retrieve the oldest segments for this partition.
        let segments = partition
            .segments
            .iter()
            // Filter to only get the closed segments.
            .filter(|segment| segment.is_closed())
            .take(
                segments_count
                    .try_into()
                    .map_err(|_| IggyError::InvalidSegmentsCount(segments_count))?,
            )
            // coerce to tuple of u64 as this has copy implicit.
            .map(|segment| (segment.start_offset(), segment.get_messages_count()))
            .collect::<Vec<_>>();

        // Delete the segments in sequence.
        let (deleted_segments_count, deleted_messages_count) = {
            let mut segments_count = 0;
            let mut messages_count = 0;

            for segment in segments {
                // delete the segment.
                let _ = partition.delete_segment(segment.0).await?;

                // increment metrics.
                segments_count += 1;
                messages_count += segment.1 as u64;
            }

            (segments_count, messages_count)
        };
        drop(stream);
        drop(partition);

        let mut stream = self
            .get_stream_mut(stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with ID: {stream_id}"
                )
            })?;
        let topic = stream
            .get_topic_mut(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to topic with ID: {topic_id} in stream with ID: {stream_id}"
                )
            })?;
        topic.reassign_consumer_groups();

        self.metrics.decrement_segments(deleted_segments_count);
        self.metrics.decrement_messages(deleted_messages_count);
        Ok(())
    }
}
