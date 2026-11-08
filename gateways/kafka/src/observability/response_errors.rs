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

//! The error codes a response carries, read back from the body the gateway encoded.
//!
//! Read from the encoded body rather than reported by each handler, so a code is counted exactly
//! when a client receives it, whichever path of a handler set it. Kafka counts its
//! `ErrorsPerSec` the same way, from the response it sends (`AbstractResponse.errorCounts`).
//!
//! The decode costs one pass over the response's topics and partitions. Records stay in the
//! buffer: `kafka_protocol` slices them out of the `Bytes` without a copy.

use bytes::Bytes;
use kafka_protocol::messages::{
    AlterConfigsResponse, CreatePartitionsResponse, CreateTopicsResponse, DeleteGroupsResponse,
    DeleteTopicsResponse, DescribeAclsResponse, DescribeClusterResponse, DescribeConfigsResponse,
    DescribeGroupsResponse, FetchResponse, FindCoordinatorResponse, HeartbeatResponse,
    IncrementalAlterConfigsResponse, InitProducerIdResponse, JoinGroupResponse, LeaveGroupResponse,
    ListGroupsResponse, ListOffsetsResponse, MetadataResponse, OffsetCommitResponse,
    OffsetDeleteResponse, OffsetFetchResponse, ProduceResponse, SaslAuthenticateResponse,
    SaslHandshakeResponse, SyncGroupResponse,
};
use kafka_protocol::protocol::Decodable;

use crate::protocol::api::{
    API_KEY_ALTER_CONFIGS, API_KEY_API_VERSIONS, API_KEY_CREATE_PARTITIONS, API_KEY_CREATE_TOPICS,
    API_KEY_DELETE_GROUPS, API_KEY_DELETE_TOPICS, API_KEY_DESCRIBE_ACLS, API_KEY_DESCRIBE_CLUSTER,
    API_KEY_DESCRIBE_CONFIGS, API_KEY_DESCRIBE_GROUPS, API_KEY_FETCH, API_KEY_FIND_COORDINATOR,
    API_KEY_HEARTBEAT, API_KEY_INCREMENTAL_ALTER_CONFIGS, API_KEY_INIT_PRODUCER_ID,
    API_KEY_JOIN_GROUP, API_KEY_LEAVE_GROUP, API_KEY_LIST_GROUPS, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_OFFSET_COMMIT, API_KEY_OFFSET_DELETE, API_KEY_OFFSET_FETCH,
    API_KEY_PRODUCE, API_KEY_SASL_AUTHENTICATE, API_KEY_SASL_HANDSHAKE, API_KEY_SYNC_GROUP,
    ERROR_NONE,
};

/// Calls `found` with each error code other than NONE in `body`, the response to a request of
/// `api_key` at `api_version`: the top-level code, then the code of each topic, partition, group
/// or resource. A body that does not decode reports nothing.
pub(super) fn error_codes(
    api_key: i16,
    api_version: i16,
    body: &Bytes,
    found: &mut impl FnMut(i16),
) {
    let mut each = |code: i16| {
        if code != ERROR_NONE {
            found(code);
        }
    };
    let each: &mut dyn FnMut(i16) = &mut each;
    let version = api_version;
    match api_key {
        API_KEY_PRODUCE => scan::<ProduceResponse>(version, body, each),
        API_KEY_FETCH => scan::<FetchResponse>(version, body, each),
        API_KEY_LIST_OFFSETS => scan::<ListOffsetsResponse>(version, body, each),
        API_KEY_METADATA => scan::<MetadataResponse>(version, body, each),
        API_KEY_OFFSET_COMMIT => scan::<OffsetCommitResponse>(version, body, each),
        API_KEY_OFFSET_FETCH => scan::<OffsetFetchResponse>(version, body, each),
        API_KEY_FIND_COORDINATOR => scan::<FindCoordinatorResponse>(version, body, each),
        API_KEY_JOIN_GROUP => scan::<JoinGroupResponse>(version, body, each),
        API_KEY_HEARTBEAT => scan::<HeartbeatResponse>(version, body, each),
        API_KEY_LEAVE_GROUP => scan::<LeaveGroupResponse>(version, body, each),
        API_KEY_SYNC_GROUP => scan::<SyncGroupResponse>(version, body, each),
        API_KEY_DESCRIBE_GROUPS => scan::<DescribeGroupsResponse>(version, body, each),
        API_KEY_LIST_GROUPS => scan::<ListGroupsResponse>(version, body, each),
        API_KEY_SASL_HANDSHAKE => scan::<SaslHandshakeResponse>(version, body, each),
        // Read from the first two bytes, not decoded: KIP-511 answers a version the gateway does
        // not serve in the v0 shape, and v5 goes out in the v4 shape. The error code leads every
        // version of the response.
        API_KEY_API_VERSIONS => {
            if let Some(code) = body.get(..2) {
                each(i16::from_be_bytes([code[0], code[1]]));
            }
        }
        API_KEY_CREATE_TOPICS => scan::<CreateTopicsResponse>(version, body, each),
        API_KEY_DELETE_TOPICS => scan::<DeleteTopicsResponse>(version, body, each),
        API_KEY_INIT_PRODUCER_ID => scan::<InitProducerIdResponse>(version, body, each),
        API_KEY_DESCRIBE_ACLS => scan::<DescribeAclsResponse>(version, body, each),
        API_KEY_DESCRIBE_CONFIGS => scan::<DescribeConfigsResponse>(version, body, each),
        API_KEY_ALTER_CONFIGS => scan::<AlterConfigsResponse>(version, body, each),
        API_KEY_SASL_AUTHENTICATE => scan::<SaslAuthenticateResponse>(version, body, each),
        API_KEY_CREATE_PARTITIONS => scan::<CreatePartitionsResponse>(version, body, each),
        API_KEY_DELETE_GROUPS => scan::<DeleteGroupsResponse>(version, body, each),
        API_KEY_INCREMENTAL_ALTER_CONFIGS => {
            scan::<IncrementalAlterConfigsResponse>(version, body, each);
        }
        API_KEY_OFFSET_DELETE => scan::<OffsetDeleteResponse>(version, body, each),
        API_KEY_DESCRIBE_CLUSTER => scan::<DescribeClusterResponse>(version, body, each),
        _ => {}
    }
}

/// Decodes `body` as `T` at `version`, and calls `each` with every error code it carries.
fn scan<T: ErrorCodes>(version: i16, body: &Bytes, each: &mut dyn FnMut(i16)) {
    match T::decode(&mut body.clone(), version) {
        Ok(response) => response.each_code(each),
        Err(error) => {
            tracing::debug!(%error, version, "could not read the error codes of a response");
        }
    }
}

/// A response whose error codes [`error_codes`] reads: the top-level code first, then those of
/// its topics, partitions, groups or resources.
trait ErrorCodes: Decodable {
    fn each_code(&self, each: &mut dyn FnMut(i16));
}

/// A response with one error code, at its top level.
macro_rules! top_level_code {
    ($($response:ty),* $(,)?) => {$(
        impl ErrorCodes for $response {
            fn each_code(&self, each: &mut dyn FnMut(i16)) {
                each(self.error_code);
            }
        }
    )*};
}

top_level_code!(
    JoinGroupResponse,
    HeartbeatResponse,
    SyncGroupResponse,
    ListGroupsResponse,
    SaslHandshakeResponse,
    InitProducerIdResponse,
    DescribeAclsResponse,
    SaslAuthenticateResponse,
    DescribeClusterResponse,
);

impl ErrorCodes for ProduceResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for topic in &self.responses {
            for partition in &topic.partition_responses {
                each(partition.error_code);
            }
        }
    }
}

impl ErrorCodes for FetchResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        each(self.error_code);
        for topic in &self.responses {
            for partition in &topic.partitions {
                each(partition.error_code);
            }
        }
    }
}

impl ErrorCodes for ListOffsetsResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for topic in &self.topics {
            for partition in &topic.partitions {
                each(partition.error_code);
            }
        }
    }
}

impl ErrorCodes for MetadataResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        each(self.error_code);
        for topic in &self.topics {
            each(topic.error_code);
            for partition in &topic.partitions {
                each(partition.error_code);
            }
        }
    }
}

impl ErrorCodes for OffsetCommitResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for topic in &self.topics {
            for partition in &topic.partitions {
                each(partition.error_code);
            }
        }
    }
}

impl ErrorCodes for OffsetFetchResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        each(self.error_code);
        for topic in &self.topics {
            for partition in &topic.partitions {
                each(partition.error_code);
            }
        }
        for group in &self.groups {
            each(group.error_code);
            for topic in &group.topics {
                for partition in &topic.partitions {
                    each(partition.error_code);
                }
            }
        }
    }
}

impl ErrorCodes for FindCoordinatorResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        each(self.error_code);
        for coordinator in &self.coordinators {
            each(coordinator.error_code);
        }
    }
}

impl ErrorCodes for LeaveGroupResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        each(self.error_code);
        for member in &self.members {
            each(member.error_code);
        }
    }
}

impl ErrorCodes for DescribeGroupsResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for group in &self.groups {
            each(group.error_code);
        }
    }
}

impl ErrorCodes for CreateTopicsResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for topic in &self.topics {
            each(topic.error_code);
        }
    }
}

impl ErrorCodes for DeleteTopicsResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for topic in &self.responses {
            each(topic.error_code);
        }
    }
}

impl ErrorCodes for DescribeConfigsResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for result in &self.results {
            each(result.error_code);
        }
    }
}

impl ErrorCodes for AlterConfigsResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for resource in &self.responses {
            each(resource.error_code);
        }
    }
}

impl ErrorCodes for IncrementalAlterConfigsResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for resource in &self.responses {
            each(resource.error_code);
        }
    }
}

impl ErrorCodes for CreatePartitionsResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for topic in &self.results {
            each(topic.error_code);
        }
    }
}

impl ErrorCodes for DeleteGroupsResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        for group in &self.results {
            each(group.error_code);
        }
    }
}

impl ErrorCodes for OffsetDeleteResponse {
    fn each_code(&self, each: &mut dyn FnMut(i16)) {
        each(self.error_code);
        for topic in &self.topics {
            for partition in &topic.partitions {
                each(partition.error_code);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kafka_protocol::messages::MetadataRequest;
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::protocol::StrBytes;

    use super::*;
    use crate::protocol::api::{
        ConnectionState, ERROR_NOT_COORDINATOR, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        ERROR_UNSUPPORTED_VERSION, GatewayState, handle_connection_request,
    };
    use crate::protocol::handlers::{api_versions, fetch, list_offsets};

    fn codes(api_key: i16, api_version: i16, body: &Bytes) -> Vec<i16> {
        let mut found = Vec::new();
        error_codes(api_key, api_version, body, &mut |code| found.push(code));
        found
    }

    #[test]
    fn given_an_error_response_should_read_its_code_back() {
        let fetch = fetch::encode_error_response(12, ERROR_NOT_COORDINATOR).unwrap();
        let offsets = list_offsets::encode_error_response(7, ERROR_NOT_COORDINATOR).unwrap();

        assert!(codes(API_KEY_FETCH, 12, &fetch).contains(&ERROR_NOT_COORDINATOR));
        assert!(codes(API_KEY_LIST_OFFSETS, 7, &offsets).contains(&ERROR_NOT_COORDINATOR));
    }

    #[test]
    fn given_an_api_versions_answer_in_the_v0_shape_should_read_its_code() {
        let advertisement = api_versions::Advertisement::default();
        let refused = api_versions::encode_response(0, ERROR_UNSUPPORTED_VERSION, advertisement)
            .expect("a v0 answer encodes");
        let served = api_versions::encode_response(3, ERROR_NONE, advertisement)
            .expect("a v3 answer encodes");

        assert_eq!(
            codes(API_KEY_API_VERSIONS, 99, &refused),
            [ERROR_UNSUPPORTED_VERSION]
        );
        assert_eq!(codes(API_KEY_API_VERSIONS, 3, &served), [0_i16; 0]);
    }

    /// A stub gateway answers Metadata for topics it does not hold, and every code the client
    /// sees has to come back out.
    #[tokio::test]
    async fn given_a_stub_answer_should_report_what_the_client_receives() {
        let state = GatewayState::stub(8 * 1024 * 1024);
        let connection = ConnectionState::default();
        let topic = |name: &'static str| {
            MetadataRequestTopic::default().with_name(Some(StrBytes::from_static_str(name).into()))
        };
        let request =
            MetadataRequest::default().with_topics(Some(vec![topic("orders"), topic("refunds")]));
        let body = iggy_kafka_wire::codec::encode_message(&request, 9, 64).unwrap();

        let outcome =
            handle_connection_request(&state, &connection, None, API_KEY_METADATA, 9, body).await;
        let response = outcome.expect_response("Metadata answers");

        assert_eq!(
            codes(API_KEY_METADATA, 9, &response),
            [
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                ERROR_UNKNOWN_TOPIC_OR_PARTITION
            ]
        );
    }

    #[test]
    fn given_a_body_that_does_not_decode_should_report_nothing() {
        assert_eq!(
            codes(API_KEY_PRODUCE, 9, &Bytes::from_static(&[0xFF])),
            [0_i16; 0]
        );
        assert_eq!(codes(API_KEY_API_VERSIONS, 3, &Bytes::new()), [0_i16; 0]);
    }
}
