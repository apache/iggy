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

import "fmt"

type IggyError struct {
	Code    int
	Message string
}

func (e *IggyError) Error() string {
	return fmt.Sprintf("%v: '%v'", e.Code, e.Message)
}

func CustomError(message string) error {
	return &IggyError{
		Code:    9999,
		Message: message,
	}
}

func TextTooLong(message string) error {
	return &IggyError{
		Code:    9999,
		Message: message + "_too_long",
	}
}

func MapFromCode(code int) error {
	return &IggyError{
		Code:    code,
		Message: TranslateErrorCode(code),
	}
}

func TranslateErrorCode(code int) string {
	switch code {
	case 1:
		return "error"
	case 2:
		return "invalid_configuration"
	case 3:
		return "invalid_command"
	case 4:
		return "invalid_format"
	case 5:
		return "feature_unavailable"
	case 6:
		return "invalid_identifier"
	case 10:
		return "cannot_create_base_directory"
	case 20:
		return "resource_not_found"
	case 21:
		return "cannot_load_resource"
	case 22:
		return "cannot_save_resource"
	case 23:
		return "cannot_delete_resource"
	case 24:
		return "cannot_serialize_resource"
	case 25:
		return "cannot_deserialize_resource"
	case 40:
		return "unauthenticated"
	case 41:
		return "unauthorized"
	case 42:
		return "invalid_credentials"
	case 43:
		return "invalid_username"
	case 44:
		return "invalid_password"
	case 51:
		return "not_connected"
	case 52:
		return "request_error"
	case 60:
		return "invalid_encryption_key"
	case 61:
		return "cannot_encrypt_data"
	case 62:
		return "cannot_decrypt_data"
	case 100:
		return "client_not_found"
	case 101:
		return "invalid_client_id"
	case 200:
		return "io_error"
	case 201:
		return "write_error"
	case 202:
		return "cannot_parse_utf8"
	case 203:
		return "cannot_parse_int"
	case 204:
		return "cannot_parse_slice"
	case 206:
		return "connection_closed"
	case 300:
		return "http_response_error"
	case 301:
		return "request_middleware_error"
	case 302:
		return "cannot_create_endpoint"
	case 303:
		return "cannot_parse_url"
	case 304:
		return "invalid_response"
	case 305:
		return "empty_response"
	case 306:
		return "cannot_parse_address"
	case 307:
		return "read_error"
	case 308:
		return "connection_error"
	case 309:
		return "read_to_end_error"
	case 1000:
		return "cannot_create_streams_directory"
	case 1001:
		return "cannot_create_stream_directory"
	case 1002:
		return "cannot_create_stream_info"
	case 1003:
		return "cannot_update_stream_info"
	case 1004:
		return "cannot_open_stream_info"
	case 1005:
		return "cannot_read_stream_info"
	case 1006:
		return "cannot_create_stream"
	case 1007:
		return "cannot_delete_stream"
	case 1008:
		return "cannot_delete_stream_directory"
	case 1009:
		return "stream_id_not_found"
	case 1010:
		return "stream_name_not_found"
	case 1011:
		return "stream_id_already_exists"
	case 1012:
		return "stream_name_already_exists"
	case 1013:
		return "invalid_stream_name"
	case 1014:
		return "invalid_stream_id"
	case 1015:
		return "cannot_read_streams"
	case 2000:
		return "cannot_create_topics_directory"
	case 2001:
		return "cannot_create_topic_directory"
	case 2002:
		return "cannot_create_topic_info"
	case 2003:
		return "cannot_update_topic_info"
	case 2004:
		return "cannot_open_topic_info"
	case 2005:
		return "cannot_read_topic_info"
	case 2006:
		return "cannot_create_topic"
	case 2007:
		return "cannot_delete_topic"
	case 2008:
		return "cannot_delete_topic_directory"
	case 2009:
		return "cannot_poll_topic"
	case 2010:
		return "topic_id_not_found"
	case 2011:
		return "topic_name_not_found"
	case 2012:
		return "topic_id_already_exists"
	case 2013:
		return "topic_name_already_exists"
	case 2014:
		return "invalid_topic_name"
	case 2015:
		return "too_many_partitions"
	case 2016:
		return "invalid_topic_id"
	case 2017:
		return "cannot_read_topics"
	case 3000:
		return "cannot_create_partition"
	case 3001:
		return "cannot_create_partitions_directory"
	case 3002:
		return "cannot_create_partition_directory"
	case 3003:
		return "cannot_open_partition_log_file"
	case 3004:
		return "cannot_read_partitions"
	case 3005:
		return "cannot_delete_partition"
	case 3006:
		return "cannot_delete_partition_directory"
	case 3007:
		return "partition_not_found"
	case 3008:
		return "no_partitions"
	case 3021:
		return "consumer_offset_not_found"
	case 4000:
		return "segment_not_found"
	case 4001:
		return "segment_closed"
	case 4002:
		return "invalid_segment_size"
	case 4003:
		return "cannot_create_segment_log_file"
	case 4004:
		return "cannot_create_segment_index_file"
	case 4005:
		return "cannot_create_segment_time_index_file"
	case 4006:
		return "cannot_save_messages_to_segment"
	case 4007:
		return "cannot_save_index_to_segment"
	case 4008:
		return "cannot_save_time_index_to_segment"
	case 4009:
		return "invalid_messages_count"
	case 4010:
		return "cannot_append_message"
	case 4011:
		return "cannot_read_message"
	case 4012:
		return "cannot_read_message_id"
	case 4013:
		return "cannot_read_message_state"
	case 4014:
		return "cannot_read_message_timestamp"
	case 4015:
		return "cannot_read_headers_length"
	case 4016:
		return "cannot_read_headers_payload"
	case 4017:
		return "too_big_headers_payload"
	case 4018:
		return "invalid_header_key"
	case 4019:
		return "invalid_header_value"
	case 4020:
		return "cannot_read_message_length"
	case 4021:
		return "cannot_read_message_payload"
	case 4022:
		return "too_big_message_payload"
	case 4023:
		return "too_many_messages"
	case 4024:
		return "empty_message_payload"
	case 4025:
		return "invalid_message_payload_length"
	case 4026:
		return "cannot_read_message_checksum"
	case 4027:
		return "invalid_message_checksum"
	case 4028:
		return "invalid_key_value_length"
	case 4032:
		return "non_zero_timestamp"
	case 4036:
		return "invalid_messages_size"
	case 4100:
		return "invalid_offset"
	case 4101:
		return "cannot_read_consumer_offsets"
	case 5000:
		return "consumer_group_not_found"
	case 5001:
		return "consumer_group_already_exists"
	case 5002:
		return "consumer_group_member_not_found"
	case 5003:
		return "invalid_consumer_group_id"
	case 5004:
		return "cannot_create_consumer_groups_directory"
	case 5005:
		return "cannot_read_consumer_groups"
	case 5006:
		return "cannot_create_consumer_group_info"
	case 5007:
		return "cannot_delete_consumer_group_info"
	default:
		return "error"
	}
}
