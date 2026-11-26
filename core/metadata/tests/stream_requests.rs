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

use iggy_binary_protocol::{WireDecode, WireEncode, WireIdentifier};
use metadata::stm::stream::TruncatePartitionRequest;
use std::mem::size_of;

#[test]
fn truncate_partition_request_round_trips() {
    let request = TruncatePartitionRequest {
        stream_id: WireIdentifier::numeric(7),
        topic_id: WireIdentifier::numeric(3),
        partition_id: 5,
        up_to_offset: 1234,
        expected_history: Some(42),
    };
    let bytes = request.to_bytes();
    let (decoded, consumed) = TruncatePartitionRequest::decode(&bytes).expect("decode");
    assert_eq!(consumed, bytes.len());
    assert_eq!(decoded.stream_id, request.stream_id);
    assert_eq!(decoded.topic_id, request.topic_id);
    assert_eq!(decoded.partition_id, request.partition_id);
    assert_eq!(decoded.up_to_offset, request.up_to_offset);
    assert_eq!(decoded.expected_history, request.expected_history);
    let unguarded = TruncatePartitionRequest {
        expected_history: None,
        ..request
    };
    let unguarded_bytes = unguarded.to_bytes();
    let (decoded, consumed) =
        TruncatePartitionRequest::decode(&unguarded_bytes).expect("unguarded decode");
    assert_eq!(consumed, unguarded_bytes.len());
    assert_eq!(decoded.expected_history, None);
    assert_eq!(bytes.len(), unguarded_bytes.len() + size_of::<u64>());
    for truncated_len in unguarded_bytes.len() + 1..bytes.len() {
        assert!(TruncatePartitionRequest::decode(&bytes[..truncated_len]).is_err());
    }
}
