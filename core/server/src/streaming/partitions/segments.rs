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

pub struct DeletedSegment {
    pub end_offset: u64,
    pub messages_count: u32,
}

/*
impl Partition {
    pub fn get_segments_count(&self) -> u32 {
        self.segments.len() as u32
    }

    pub fn get_segments(&self) -> &Vec<Segment> {
        &self.segments
    }

    pub fn get_segments_mut(&mut self) -> &mut Vec<Segment> {
        &mut self.segments
    }

    pub fn get_segment(&self, start_offset: u64) -> Option<&Segment> {
        self.segments
            .iter()
            .find(|s| s.start_offset() == start_offset)
    }

    pub fn get_segment_mut(&mut self, start_offset: u64) -> Option<&mut Segment> {
        self.segments
            .iter_mut()
            .find(|s| s.start_offset() == start_offset)
    }

    pub async fn get_expired_segments_start_offsets(&self, now: IggyTimestamp) -> Vec<u64> {
        let mut expired_segments = Vec::new();
        for segment in &self.segments {
            if segment.is_expired(now).await {
                expired_segments.push(segment.start_offset());
            }
        }

        expired_segments.sort();
        expired_segments
    }

    pub async fn add_persisted_segment(&mut self, start_offset: u64) -> Result<(), IggyError> {
        info!(
            "Creating the new segment for partition with ID: {}, stream with ID: {}, topic with ID: {}...",
            self.partition_id, self.stream_id, self.topic_id
        );
        let mut new_segment = Segment::create(
            self.stream_id,
            self.topic_id,
            self.partition_id,
            start_offset,
            self.config.clone(),
            self.message_expiry,
            self.size_of_parent_stream.clone(),
            self.size_of_parent_topic.clone(),
            self.size_bytes.clone(),
            self.messages_count_of_parent_stream.clone(),
            self.messages_count_of_parent_topic.clone(),
            self.messages_count.clone(),
            true,
        );
        new_segment.open().await.with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to persist new segment: {new_segment}",)
        })?;
        self.segments.push(new_segment);
        self.segments_count_of_parent_stream
            .fetch_add(1, Ordering::SeqCst);
        self.segments.sort_by_key(|a| a.start_offset());
        Ok(())
    }

    pub async fn delete_segment(&mut self, start_offset: u64) -> Result<DeletedSegment, IggyError> {
        let deleted_segment;
        {
            let segment = self.get_segment_mut(start_offset);
            if segment.is_none() {
                return Err(IggyError::SegmentNotFound);
            }

            let segment = segment.unwrap();
            segment.delete().await.with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete segment: {segment}",)
            })?;

            deleted_segment = DeletedSegment {
                end_offset: segment.end_offset(),
                messages_count: segment.get_messages_count(),
            };
        }

        self.segments_count_of_parent_stream
            .fetch_sub(1, Ordering::SeqCst);

        self.segments.retain(|s| s.start_offset() != start_offset);
        self.segments.sort_by_key(|a| a.start_offset());
        info!(
            "Segment with start offset: {} has been deleted from partition with ID: {}, stream with ID: {}, topic with ID: {}",
            start_offset, self.partition_id, self.stream_id, self.topic_id
        );
        Ok(deleted_segment)
    }
}

*/
