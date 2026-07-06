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

use bytes::Bytes;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_METADATA, BrokerAdvertise, handle_request,
};
use iggy_gateway_kafka::protocol::codec::Encoder;

#[test]
fn golden_apiversions_v1_response_fixture() {
    let broker = BrokerAdvertise::default();
    let actual = handle_request(API_KEY_API_VERSIONS, 1, Bytes::new(), &broker)
        .expect("test request has acks != 0 and expects a response");

    // error_code=0, api_count=6
    // key 0  (Produce)      min=0  max=9 (KAFKA-18659 advertise min=0)
    // key 1  (Fetch)        min=4  max=12
    // key 2  (ListOffsets)  min=1  max=6
    // key 3  (Metadata)     min=0  max=9
    // key 18 (ApiVersions)  min=0  max=3
    // key 19 (CreateTopics) min=2  max=5
    // throttle_ms=0
    let expected: [u8; 46] = [
        0x00, 0x00, // error_code
        0x00, 0x00, 0x00, 0x06, // api count = 6
        0x00, 0x00, 0x00, 0x00, 0x00, 0x09, // key 0:  Produce      0–9 (advertised)
        0x00, 0x01, 0x00, 0x04, 0x00, 0x0C, // key 1:  Fetch        4–12
        0x00, 0x02, 0x00, 0x01, 0x00, 0x06, // key 2:  ListOffsets  1–6
        0x00, 0x03, 0x00, 0x00, 0x00, 0x09, // key 3:  Metadata     0–9
        0x00, 0x12, 0x00, 0x00, 0x00, 0x03, // key 18: ApiVersions  0–3
        0x00, 0x13, 0x00, 0x02, 0x00, 0x05, // key 19: CreateTopics 2–5
        0x00, 0x00, 0x00, 0x00, // throttle_ms
    ];
    assert_eq!(actual.as_ref(), &expected);
}

#[test]
fn golden_metadata_v0_single_topic_response_fixture() {
    let mut request = Encoder::with_capacity(32);
    request.write_i32(1); // one topic
    request
        .write_nullable_string(Some("orders"))
        .expect("topic name fits");
    let req_bytes = request.freeze();

    let actual = handle_request(API_KEY_METADATA, 0, req_bytes, &BrokerAdvertise::default())
        .expect("test request has acks != 0 and expects a response");

    // Metadata v0 layout: brokers[], topics[]  (no controller_id — added in v1)
    // brokers[1]: node_id=1, host=127.0.0.1, port=9093
    // topics[1]: topic_error=3, topic_name=orders (echoed from the request), partitions[0]
    let expected: [u8; 41] = [
        0x00, 0x00, 0x00, 0x01, // broker count
        0x00, 0x00, 0x00, 0x01, // node id
        0x00, 0x09, // host len
        0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, // "127.0.0.1"
        0x00, 0x00, 0x23, 0x85, // port 9093
        0x00, 0x00, 0x00, 0x01, // topic count
        0x00, 0x03, // topic error code
        0x00, 0x06, // topic name len
        0x6f, 0x72, 0x64, 0x65, 0x72, 0x73, // "orders"
        0x00, 0x00, 0x00, 0x00, // partition count
    ];
    assert_eq!(actual.as_ref(), &expected);
}
