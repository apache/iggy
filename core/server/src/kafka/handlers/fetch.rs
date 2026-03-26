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

use crate::configs::kafka::KafkaConfig;
use crate::kafka::protocol::codec::{write_i16, write_i32, write_i64, write_nullable_bytes};
use crate::kafka::protocol::record_batch::encode_varint;
use crate::kafka::protocol::types::{FetchRequest, FetchResponse};
use crate::kafka::protocol::{NONE, UNKNOWN_TOPIC_OR_PARTITION};
use crate::shard::IggyShard;
use crate::shard::system::messages::PollingArgs;
use crate::streaming::session::Session;
use iggy_common::{
    Consumer, ConsumerKind, Identifier, IggyMessagesBatchSet, PollingKind, PollingStrategy,
};
use std::rc::Rc;
use tracing::warn;

pub async fn handle(
    shard: &Rc<IggyShard>,
    session: &Session,
    config: &KafkaConfig,
    body: &[u8],
) -> Vec<u8> {
    let mut pos = 0;
    let req = match FetchRequest::decode(body, &mut pos) {
        Ok(r) => r,
        Err(_) => return error_response(),
    };

    let stream_ident = match Identifier::from_str_value(&config.default_stream) {
        Ok(id) => id,
        Err(_) => return error_response(),
    };

    let mut response_body = Vec::new();
    write_i32(&mut response_body, 0); // throttle_time_ms
    write_i32(&mut response_body, req.topics.len() as i32);

    for fetch_topic in &req.topics {
        write_i16(&mut response_body, fetch_topic.name.len() as i16);
        response_body.extend_from_slice(fetch_topic.name.as_bytes());
        write_i32(&mut response_body, fetch_topic.partitions.len() as i32);

        let topic_ident = match Identifier::from_str_value(&fetch_topic.name) {
            Ok(id) => id,
            Err(_) => {
                for fp in &fetch_topic.partitions {
                    write_partition_error(&mut response_body, fp.partition_index);
                }
                continue;
            }
        };

        let resolved = match shard.resolve_topic(&stream_ident, &topic_ident) {
            Ok(r) => r,
            Err(_) => {
                for fp in &fetch_topic.partitions {
                    write_partition_error(&mut response_body, fp.partition_index);
                }
                continue;
            }
        };

        for fp in &fetch_topic.partitions {
            let consumer = Consumer {
                kind: ConsumerKind::Consumer,
                id: Identifier::numeric(session.client_id).unwrap(),
            };

            let strategy = PollingStrategy {
                kind: PollingKind::Offset,
                value: fp.fetch_offset as u64,
            };
            let count = (fp.partition_max_bytes / 1024).max(1) as u32;
            let args = PollingArgs::new(strategy, count, false);

            match shard
                .poll_messages(
                    session.client_id,
                    resolved,
                    consumer,
                    Some(fp.partition_index as u32),
                    args,
                )
                .await
            {
                Ok((metadata, batch_set)) => {
                    write_i32(&mut response_body, fp.partition_index);
                    write_i16(&mut response_body, NONE);
                    write_i64(&mut response_body, metadata.current_offset as i64);
                    write_i64(&mut response_body, metadata.current_offset as i64); // last_stable_offset
                    write_i32(&mut response_body, -1); // aborted_transactions = null
                    let records = encode_fetched_messages(&batch_set, fp.fetch_offset as u64);
                    write_nullable_bytes(&mut response_body, Some(&records));
                }
                Err(e) => {
                    warn!(
                        "poll_messages failed for {}/{}: {e}",
                        fetch_topic.name, fp.partition_index
                    );
                    write_partition_error(&mut response_body, fp.partition_index);
                }
            }
        }
    }

    response_body
}

fn encode_fetched_messages(batch_set: &IggyMessagesBatchSet, base_offset: u64) -> Vec<u8> {
    if batch_set.count() == 0 {
        return vec![];
    }

    let mut record_count = 0u32;
    let mut first_timestamp = 0i64;
    let mut max_timestamp = 0i64;

    // Collect all records first
    let mut encoded_records = Vec::new();
    for batch in batch_set.iter() {
        for msg in batch.iter() {
            let header = msg.header();
            let offset_delta = header.offset().saturating_sub(base_offset) as i32;
            let timestamp = header.timestamp() as i64;
            if record_count == 0 {
                first_timestamp = timestamp;
            }
            max_timestamp = timestamp;

            let mut record = Vec::new();
            encode_varint(&mut record, 0); // attributes
            encode_varint(&mut record, timestamp - first_timestamp); // timestampDelta
            encode_varint(&mut record, offset_delta as i64); // offsetDelta
            encode_varint(&mut record, -1); // key = null
            let payload = msg.payload();
            encode_varint(&mut record, payload.len() as i64);
            record.extend_from_slice(payload);
            encode_varint(&mut record, 0); // headerCount

            let mut framed = Vec::new();
            encode_varint(&mut framed, record.len() as i64);
            framed.extend_from_slice(&record);
            encoded_records.push(framed);
            record_count += 1;
        }
    }

    if record_count == 0 {
        return vec![];
    }

    // Build batch header
    let mut batch_data = Vec::new();
    for r in &encoded_records {
        batch_data.extend_from_slice(r);
    }

    let mut buf = Vec::new();
    buf.extend_from_slice(&(base_offset as i64).to_be_bytes()); // baseOffset
    let batch_len = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + batch_data.len();
    buf.extend_from_slice(&(batch_len as i32).to_be_bytes()); // batchLength
    buf.extend_from_slice(&0i32.to_be_bytes()); // partitionLeaderEpoch
    buf.push(2); // magic
    buf.extend_from_slice(&0i32.to_be_bytes()); // crc (not validated by consumers typically)
    buf.extend_from_slice(&0i16.to_be_bytes()); // attributes
    buf.extend_from_slice(&((record_count - 1) as i32).to_be_bytes()); // lastOffsetDelta
    buf.extend_from_slice(&first_timestamp.to_be_bytes()); // baseTimestamp
    buf.extend_from_slice(&max_timestamp.to_be_bytes()); // maxTimestamp
    buf.extend_from_slice(&(-1i64).to_be_bytes()); // producerId
    buf.extend_from_slice(&(-1i16).to_be_bytes()); // producerEpoch
    buf.extend_from_slice(&(-1i32).to_be_bytes()); // baseSequence
    buf.extend_from_slice(&(record_count as i32).to_be_bytes()); // recordCount
    buf.extend_from_slice(&batch_data);

    buf
}

fn write_partition_error(buf: &mut Vec<u8>, partition_index: i32) {
    write_i32(buf, partition_index);
    write_i16(buf, UNKNOWN_TOPIC_OR_PARTITION);
    write_i64(buf, -1); // high_watermark
    write_i64(buf, -1); // last_stable_offset
    write_i32(buf, -1); // aborted_transactions = null
    write_nullable_bytes(buf, None); // records = null
}

fn error_response() -> Vec<u8> {
    FetchResponse {
        throttle_time_ms: 0,
        responses: vec![],
    }
    .encode()
}
