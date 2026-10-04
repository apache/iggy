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

pub mod delete_consumer_offset;
pub mod get_consumer_offset;
pub mod store_consumer_offset;

pub use delete_consumer_offset::DeleteConsumerOffsetRequest;
pub use get_consumer_offset::GetConsumerOffsetRequest;
pub use store_consumer_offset::StoreConsumerOffsetRequest;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireIdentifier;
    use crate::codec::{WireDecode, WireEncode};
    use crate::primitives::ack_level::AckLevel;
    use crate::primitives::consumer::WireConsumer;

    #[test]
    fn offset_writes_share_the_routing_request_prefix() {
        let identifiers = [
            WireIdentifier::numeric(7),
            WireIdentifier::named("named-resource").unwrap(),
        ];
        for consumer_id in &identifiers {
            for consumer in [
                WireConsumer::consumer(consumer_id.clone()),
                WireConsumer::consumer_group(consumer_id.clone()),
            ] {
                for stream_id in &identifiers {
                    for topic_id in &identifiers {
                        for partition_id in [None, Some(0), Some(u32::MAX)] {
                            let route = GetConsumerOffsetRequest {
                                consumer: consumer.clone(),
                                stream_id: stream_id.clone(),
                                topic_id: topic_id.clone(),
                                partition_id,
                            };
                            for ack in [AckLevel::NoAck, AckLevel::Quorum] {
                                let store = StoreConsumerOffsetRequest {
                                    consumer: consumer.clone(),
                                    stream_id: stream_id.clone(),
                                    topic_id: topic_id.clone(),
                                    partition_id,
                                    offset: u64::MAX,
                                    ack,
                                }
                                .to_bytes();
                                let delete = DeleteConsumerOffsetRequest {
                                    consumer: consumer.clone(),
                                    stream_id: stream_id.clone(),
                                    topic_id: topic_id.clone(),
                                    partition_id,
                                    ack,
                                }
                                .to_bytes();
                                for payload in [store, delete] {
                                    let (decoded, consumed) =
                                        GetConsumerOffsetRequest::decode(&payload).unwrap();
                                    assert_eq!(decoded, route);
                                    assert_eq!(consumed, route.encoded_size());
                                    assert_eq!(&payload[..consumed], route.to_bytes().as_ref());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
