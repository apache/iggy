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

package ierror

var (
	ResourceNotFound = &IggyError{
		Code:    20,
		Message: "resource_not_found",
	}
	InvalidConfiguration = &IggyError{
		Code:    2,
		Message: "invalid_configuration",
	}
	InvalidIdentifier = &IggyError{
		Code:    6,
		Message: "invalid_identifier",
	}
	StreamIdNotFound = &IggyError{
		Code:    1009,
		Message: "stream_id_not_found",
	}
	TopicIdNotFound = &IggyError{
		Code:    2010,
		Message: "topic_id_not_found",
	}
	ConsumerOffsetNotFound = &IggyError{
		Code:    3021,
		Message: "consumer_offset_not_found",
	}
	InvalidMessagesCount = &IggyError{
		Code:    4009,
		Message: "invalid_messages_count",
	}
	InvalidMessagePayloadLength = &IggyError{
		Code:    4025,
		Message: "invalid_message_payload_length",
	}
	TooBigUserMessagePayload = &IggyError{
		Code:    4022,
		Message: "too_big_message_payload",
	}
	TooBigUserHeaders = &IggyError{
		Code:    4017,
		Message: "too_big_headers_payload",
	}
	ConsumerGroupIdNotFound = &IggyError{
		Code:    5000,
		Message: "consumer_group_not_found",
	}
)
