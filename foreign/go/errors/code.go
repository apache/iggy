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

// ErrCode defines the error Code type.
type ErrCode uint32

// Enum of Iggy error codes.
const (
	OK                                         = 0
	Error                                      = 1
	InvalidConfiguration                       = 2
	InvalidCommand                             = 3
	InvalidFormat                              = 4
	FeatureUnavailable                         = 5
	InvalidIdentifier                          = 6
	InvalidVersion                             = 7
	Disconnected                               = 8
	CannotEstablishConnection                  = 9
	CannotCreateBaseDirectory                  = 10
	CannotCreateRuntimeDirectory               = 11
	CannotRemoveRuntimeDirectory               = 12
	CannotCreateStateDirectory                 = 13
	StateFileNotFound                          = 14
	StateFileCorrupted                         = 15
	InvalidStateEntryChecksum                  = 16
	CannotOpenDatabase                         = 19
	ResourceNotFound                           = 20
	StaleClient                                = 30
	TcpError                                   = 31
	QuicError                                  = 32
	InvalidServerAddress                       = 33
	InvalidClientAddress                       = 34
	InvalidIpAddress                           = 35
	Unauthenticated                            = 40
	Unauthorized                               = 41
	InvalidCredentials                         = 42
	InvalidUsername                            = 43
	InvalidPassword                            = 44
	InvalidUserStatus                          = 45
	UserAlreadyExists                          = 46
	UserInactive                               = 47
	CannotDeleteUser                           = 48
	CannotChangePermissions                    = 49
	InvalidPersonalAccessTokenName             = 50
	PersonalAccessTokenAlreadyExists           = 51
	PersonalAccessTokensLimitReached           = 52
	InvalidPersonalAccessToken                 = 53
	PersonalAccessTokenExpired                 = 54
	UsersLimitReached                          = 55
	NotConnected                               = 61
	ClientShutdown                             = 63
	InvalidTlsDomain                           = 64
	InvalidTlsCertificatePath                  = 65
	InvalidTlsCertificate                      = 66
	FailedToAddCertificate                     = 67
	InvalidEncryptionKey                       = 70
	CannotEncryptData                          = 71
	CannotDecryptData                          = 72
	InvalidJwtAlgorithm                        = 73
	InvalidJwtSecret                           = 74
	JwtMissing                                 = 75
	CannotGenerateJwt                          = 76
	AccessTokenMissing                         = 77
	InvalidAccessToken                         = 78
	InvalidSizeBytes                           = 80
	InvalidUtf8                                = 81
	InvalidNumberEncoding                      = 82
	InvalidBooleanValue                        = 83
	InvalidNumberValue                         = 84
	ClientNotFound                             = 100
	InvalidClientId                            = 101
	ConnectionClosed                           = 206
	CannotParseHeaderKind                      = 209
	HttpResponseError                          = 300
	InvalidHttpRequest                         = 301
	InvalidJsonResponse                        = 302
	InvalidBytesResponse                       = 303
	EmptyResponse                              = 304
	CannotCreateEndpoint                       = 305
	CannotParseUrl                             = 306
	CannotCreateStreamsDirectory               = 1000
	CannotCreateStreamDirectory                = 1001
	CannotCreateStreamInfo                     = 1002
	CannotUpdateStreamInfo                     = 1003
	CannotOpenStreamInfo                       = 1004
	CannotReadStreamInfo                       = 1005
	CannotCreateStream                         = 1006
	CannotDeleteStream                         = 1007
	CannotDeleteStreamDirectory                = 1008
	StreamIdNotFound                           = 1009
	StreamNameNotFound                         = 1010
	StreamIdAlreadyExists                      = 1011
	StreamNameAlreadyExists                    = 1012
	InvalidStreamName                          = 1013
	InvalidStreamId                            = 1014
	CannotReadStreams                          = 1015
	InvalidTopicSize                           = 1019
	CannotCreateTopicsDirectory                = 2000
	CannotCreateTopicDirectory                 = 2001
	CannotCreateTopicInfo                      = 2002
	CannotUpdateTopicInfo                      = 2003
	CannotOpenTopicInfo                        = 2004
	CannotReadTopicInfo                        = 2005
	CannotCreateTopic                          = 2006
	CannotDeleteTopic                          = 2007
	CannotDeleteTopicDirectory                 = 2008
	CannotPollTopic                            = 2009
	TopicIdNotFound                            = 2010
	TopicNameNotFound                          = 2011
	TopicIdAlreadyExists                       = 2012
	TopicNameAlreadyExists                     = 2013
	InvalidTopicName                           = 2014
	TooManyPartitions                          = 2015
	InvalidTopicId                             = 2016
	CannotReadTopics                           = 2017
	InvalidReplicationFactor                   = 2018
	CannotCreatePartition                      = 3000
	CannotCreatePartitionsDirectory            = 3001
	CannotCreatePartitionDirectory             = 3002
	CannotOpenPartitionLogFile                 = 3003
	CannotReadPartitions                       = 3004
	CannotDeletePartition                      = 3005
	CannotDeletePartitionDirectory             = 3006
	PartitionNotFound                          = 3007
	NoPartitions                               = 3008
	TopicFull                                  = 3009
	CannotDeleteConsumerOffsetsDirectory       = 3010
	CannotDeleteConsumerOffsetFile             = 3011
	CannotCreateConsumerOffsetsDirectory       = 3012
	CannotReadConsumerOffsets                  = 3020
	ConsumerOffsetNotFound                     = 3021
	SegmentNotFound                            = 4000
	SegmentClosed                              = 4001
	InvalidSegmentSize                         = 4002
	CannotCreateSegmentLogFile                 = 4003
	CannotCreateSegmentIndexFile               = 4004
	CannotCreateSegmentTimeIndexFile           = 4005
	CannotSaveMessagesToSegment                = 4006
	CannotSaveIndexToSegment                   = 4007
	CannotSaveTimeIndexToSegment               = 4008
	InvalidMessagesCount                       = 4009
	CannotAppendMessage                        = 4010
	CannotReadMessage                          = 4011
	CannotReadMessageId                        = 4012
	CannotReadMessageState                     = 4013
	CannotReadMessageTimestamp                 = 4014
	CannotReadHeadersLength                    = 4015
	CannotReadHeadersPayload                   = 4016
	TooBigUserHeaders                          = 4017
	InvalidHeaderKey                           = 4018
	InvalidHeaderValue                         = 4019
	CannotReadMessageLength                    = 4020
	CannotReadMessagePayload                   = 4021
	TooBigMessagePayload                       = 4022
	TooManyMessages                            = 4023
	EmptyMessagePayload                        = 4024
	InvalidMessagePayloadLength                = 4025
	CannotReadMessageChecksum                  = 4026
	InvalidMessageChecksum                     = 4027
	InvalidKeyValueLength                      = 4028
	CommandLengthError                         = 4029
	InvalidSegmentsCount                       = 4030
	NonZeroOffset                              = 4031
	NonZeroTimestamp                           = 4032
	MissingIndex                               = 4033
	InvalidIndexesByteSize                     = 4034
	InvalidIndexesCount                        = 4035
	InvalidMessagesSize                        = 4036
	TooSmallMessage                            = 4037
	CannotSendMessagesDueToClientDisconnection = 4050
	BackgroundSendError                        = 4051
	BackgroundSendTimeout                      = 4052
	BackgroundSendBufferFull                   = 4053
	BackgroundWorkerDisconnected               = 4054
	BackgroundSendBufferOverflow               = 4055
	ProducerSendFailed                         = 4056
	ProducerClosed                             = 4057
	InvalidOffset                              = 4100
	ConsumerGroupIdNotFound                    = 5000
	ConsumerGroupIdAlreadyExists               = 5001
	InvalidConsumerGroupId                     = 5002
	ConsumerGroupNameNotFound                  = 5003
	ConsumerGroupNameAlreadyExists             = 5004
	InvalidConsumerGroupName                   = 5005
	ConsumerGroupMemberNotFound                = 5006
	CannotCreateConsumerGroupInfo              = 5007
	CannotDeleteConsumerGroupInfo              = 5008
	MissingBaseOffsetRetainedMessageBatch      = 6000
	MissingLastOffsetDeltaRetainedMessageBatch = 6001
	MissingMaxTimestampRetainedMessageBatch    = 6002
	MissingLengthRetainedMessageBatch          = 6003
	MissingPayloadRetainedMessageBatch         = 6004
	CannotReadBatchBaseOffset                  = 7000
	CannotReadBatchLength                      = 7001
	CannotReadLastOffsetDelta                  = 7002
	CannotReadMaxTimestamp                     = 7003
	CannotReadBatchPayload                     = 7004
	InvalidConnectionString                    = 8000
	SnapshotFileCompletionFailed               = 9000
	CannotSerializeResource                    = 10000
	CannotDeserializeResource                  = 10001
	CannotReadFile                             = 10002
	CannotReadFileMetadata                     = 10003
	CannotSeekFile                             = 10004
	CannotAppendToFile                         = 10005
	CannotWriteToFile                          = 10006
	CannotOverwriteFile                        = 10007
	CannotDeleteFile                           = 10008
	CannotSyncFile                             = 10009
	CannotReadIndexOffset                      = 10010
	CannotReadIndexPosition                    = 10011
	CannotReadIndexTimestamp                   = 10012
)

// String maps error codes to their snake_case string.
func (c ErrCode) String() string {
	switch c {
	case Error:
		return "error"
	case InvalidConfiguration:
		return "invalid_configuration"
	case InvalidCommand:
		return "invalid_command"
	case InvalidFormat:
		return "invalid_format"
	case FeatureUnavailable:
		return "feature_unavailable"
	case InvalidIdentifier:
		return "invalid_identifier"
	case InvalidVersion:
		return "invalid_version"
	case Disconnected:
		return "disconnected"
	case CannotEstablishConnection:
		return "cannot_establish_connection"
	case CannotCreateBaseDirectory:
		return "cannot_create_base_directory"
	case CannotCreateRuntimeDirectory:
		return "cannot_create_runtime_directory"
	case CannotRemoveRuntimeDirectory:
		return "cannot_remove_runtime_directory"
	case CannotCreateStateDirectory:
		return "cannot_create_state_directory"
	case StateFileNotFound:
		return "state_file_not_found"
	case StateFileCorrupted:
		return "state_file_corrupted"
	case InvalidStateEntryChecksum:
		return "invalid_state_entry_checksum"
	case CannotOpenDatabase:
		return "cannot_open_database"
	case ResourceNotFound:
		return "resource_not_found"
	case StaleClient:
		return "stale_client"
	case TcpError:
		return "tcp_error"
	case QuicError:
		return "quic_error"
	case InvalidServerAddress:
		return "invalid_server_address"
	case InvalidClientAddress:
		return "invalid_client_address"
	case InvalidIpAddress:
		return "invalid_ip_address"
	case Unauthenticated:
		return "unauthenticated"
	case Unauthorized:
		return "unauthorized"
	case InvalidCredentials:
		return "invalid_credentials"
	case InvalidUsername:
		return "invalid_username"
	case InvalidPassword:
		return "invalid_password"
	case InvalidUserStatus:
		return "invalid_user_status"
	case UserAlreadyExists:
		return "user_already_exists"
	case UserInactive:
		return "user_inactive"
	case CannotDeleteUser:
		return "cannot_delete_user"
	case CannotChangePermissions:
		return "cannot_change_permissions"
	case InvalidPersonalAccessTokenName:
		return "invalid_personal_access_token_name"
	case PersonalAccessTokenAlreadyExists:
		return "personal_access_token_already_exists"
	case PersonalAccessTokensLimitReached:
		return "personal_access_tokens_limit_reached"
	case InvalidPersonalAccessToken:
		return "invalid_personal_access_token"
	case PersonalAccessTokenExpired:
		return "personal_access_token_expired"
	case UsersLimitReached:
		return "users_limit_reached"
	case NotConnected:
		return "not_connected"
	case ClientShutdown:
		return "client_shutdown"
	case InvalidTlsDomain:
		return "invalid_tls_domain"
	case InvalidTlsCertificatePath:
		return "invalid_tls_certificate_path"
	case InvalidTlsCertificate:
		return "invalid_tls_certificate"
	case FailedToAddCertificate:
		return "failed_to_add_certificate"
	case InvalidEncryptionKey:
		return "invalid_encryption_key"
	case CannotEncryptData:
		return "cannot_encrypt_data"
	case CannotDecryptData:
		return "cannot_decrypt_data"
	case InvalidJwtAlgorithm:
		return "invalid_jwt_algorithm"
	case InvalidJwtSecret:
		return "invalid_jwt_secret"
	case JwtMissing:
		return "jwt_missing"
	case CannotGenerateJwt:
		return "cannot_generate_jwt"
	case AccessTokenMissing:
		return "access_token_missing"
	case InvalidAccessToken:
		return "invalid_access_token"
	case InvalidSizeBytes:
		return "invalid_size_bytes"
	case InvalidUtf8:
		return "invalid_utf8"
	case InvalidNumberEncoding:
		return "invalid_number_encoding"
	case InvalidBooleanValue:
		return "invalid_boolean_value"
	case InvalidNumberValue:
		return "invalid_number_value"
	case ClientNotFound:
		return "client_not_found"
	case InvalidClientId:
		return "invalid_client_id"
	case ConnectionClosed:
		return "connection_closed"
	case CannotParseHeaderKind:
		return "cannot_parse_header_kind"
	case HttpResponseError:
		return "http_response_error"
	case InvalidHttpRequest:
		return "invalid_http_request"
	case InvalidJsonResponse:
		return "invalid_json_response"
	case InvalidBytesResponse:
		return "invalid_bytes_response"
	case EmptyResponse:
		return "empty_response"
	case CannotCreateEndpoint:
		return "cannot_create_endpoint"
	case CannotParseUrl:
		return "cannot_parse_url"
	case CannotCreateStreamsDirectory:
		return "cannot_create_streams_directory"
	case CannotCreateStreamDirectory:
		return "cannot_create_stream_directory"
	case CannotCreateStreamInfo:
		return "cannot_create_stream_info"
	case CannotUpdateStreamInfo:
		return "cannot_update_stream_info"
	case CannotOpenStreamInfo:
		return "cannot_open_stream_info"
	case CannotReadStreamInfo:
		return "cannot_read_stream_info"
	case CannotCreateStream:
		return "cannot_create_stream"
	case CannotDeleteStream:
		return "cannot_delete_stream"
	case CannotDeleteStreamDirectory:
		return "cannot_delete_stream_directory"
	case StreamIdNotFound:
		return "stream_id_not_found"
	case StreamNameNotFound:
		return "stream_name_not_found"
	case StreamIdAlreadyExists:
		return "stream_id_already_exists"
	case StreamNameAlreadyExists:
		return "stream_name_already_exists"
	case InvalidStreamName:
		return "invalid_stream_name"
	case InvalidStreamId:
		return "invalid_stream_id"
	case CannotReadStreams:
		return "cannot_read_streams"
	case InvalidTopicSize:
		return "invalid_topic_size"
	case CannotCreateTopicsDirectory:
		return "cannot_create_topics_directory"
	case CannotCreateTopicDirectory:
		return "cannot_create_topic_directory"
	case CannotCreateTopicInfo:
		return "cannot_create_topic_info"
	case CannotUpdateTopicInfo:
		return "cannot_update_topic_info"
	case CannotOpenTopicInfo:
		return "cannot_open_topic_info"
	case CannotReadTopicInfo:
		return "cannot_read_topic_info"
	case CannotCreateTopic:
		return "cannot_create_topic"
	case CannotDeleteTopic:
		return "cannot_delete_topic"
	case CannotDeleteTopicDirectory:
		return "cannot_delete_topic_directory"
	case CannotPollTopic:
		return "cannot_poll_topic"
	case TopicIdNotFound:
		return "topic_id_not_found"
	case TopicNameNotFound:
		return "topic_name_not_found"
	case TopicIdAlreadyExists:
		return "topic_id_already_exists"
	case TopicNameAlreadyExists:
		return "topic_name_already_exists"
	case InvalidTopicName:
		return "invalid_topic_name"
	case TooManyPartitions:
		return "too_many_partitions"
	case InvalidTopicId:
		return "invalid_topic_id"
	case CannotReadTopics:
		return "cannot_read_topics"
	case InvalidReplicationFactor:
		return "invalid_replication_factor"
	case CannotCreatePartition:
		return "cannot_create_partition"
	case CannotCreatePartitionsDirectory:
		return "cannot_create_partitions_directory"
	case CannotCreatePartitionDirectory:
		return "cannot_create_partition_directory"
	case CannotOpenPartitionLogFile:
		return "cannot_open_partition_log_file"
	case CannotReadPartitions:
		return "cannot_read_partitions"
	case CannotDeletePartition:
		return "cannot_delete_partition"
	case CannotDeletePartitionDirectory:
		return "cannot_delete_partition_directory"
	case PartitionNotFound:
		return "partition_not_found"
	case NoPartitions:
		return "no_partitions"
	case TopicFull:
		return "topic_full"
	case CannotDeleteConsumerOffsetsDirectory:
		return "cannot_delete_consumer_offsets_directory"
	case CannotDeleteConsumerOffsetFile:
		return "cannot_delete_consumer_offset_file"
	case CannotCreateConsumerOffsetsDirectory:
		return "cannot_create_consumer_offsets_directory"
	case CannotReadConsumerOffsets:
		return "cannot_read_consumer_offsets"
	case ConsumerOffsetNotFound:
		return "consumer_offset_not_found"
	case SegmentNotFound:
		return "segment_not_found"
	case SegmentClosed:
		return "segment_closed"
	case InvalidSegmentSize:
		return "invalid_segment_size"
	case CannotCreateSegmentLogFile:
		return "cannot_create_segment_log_file"
	case CannotCreateSegmentIndexFile:
		return "cannot_create_segment_index_file"
	case CannotCreateSegmentTimeIndexFile:
		return "cannot_create_segment_time_index_file"
	case CannotSaveMessagesToSegment:
		return "cannot_save_messages_to_segment"
	case CannotSaveIndexToSegment:
		return "cannot_save_index_to_segment"
	case CannotSaveTimeIndexToSegment:
		return "cannot_save_time_index_to_segment"
	case InvalidMessagesCount:
		return "invalid_messages_count"
	case CannotAppendMessage:
		return "cannot_append_message"
	case CannotReadMessage:
		return "cannot_read_message"
	case CannotReadMessageId:
		return "cannot_read_message_id"
	case CannotReadMessageState:
		return "cannot_read_message_state"
	case CannotReadMessageTimestamp:
		return "cannot_read_message_timestamp"
	case CannotReadHeadersLength:
		return "cannot_read_headers_length"
	case CannotReadHeadersPayload:
		return "cannot_read_headers_payload"
	case TooBigUserHeaders:
		return "too_big_user_headers"
	case InvalidHeaderKey:
		return "invalid_header_key"
	case InvalidHeaderValue:
		return "invalid_header_value"
	case CannotReadMessageLength:
		return "cannot_read_message_length"
	case CannotReadMessagePayload:
		return "cannot_read_message_payload"
	case TooBigMessagePayload:
		return "too_big_message_payload"
	case TooManyMessages:
		return "too_many_messages"
	case EmptyMessagePayload:
		return "empty_message_payload"
	case InvalidMessagePayloadLength:
		return "invalid_message_payload_length"
	case CannotReadMessageChecksum:
		return "cannot_read_message_checksum"
	case InvalidMessageChecksum:
		return "invalid_message_checksum"
	case InvalidKeyValueLength:
		return "invalid_key_value_length"
	case CommandLengthError:
		return "command_length_error"
	case InvalidSegmentsCount:
		return "invalid_segments_count"
	case NonZeroOffset:
		return "non_zero_offset"
	case NonZeroTimestamp:
		return "non_zero_timestamp"
	case MissingIndex:
		return "missing_index"
	case InvalidIndexesByteSize:
		return "invalid_indexes_byte_size"
	case InvalidIndexesCount:
		return "invalid_indexes_count"
	case InvalidMessagesSize:
		return "invalid_messages_size"
	case TooSmallMessage:
		return "too_small_message"
	case CannotSendMessagesDueToClientDisconnection:
		return "cannot_send_messages_due_to_client_disconnection"
	case BackgroundSendError:
		return "background_send_error"
	case BackgroundSendTimeout:
		return "background_send_timeout"
	case BackgroundSendBufferFull:
		return "background_send_buffer_full"
	case BackgroundWorkerDisconnected:
		return "background_worker_disconnected"
	case BackgroundSendBufferOverflow:
		return "background_send_buffer_overflow"
	case ProducerSendFailed:
		return "producer_send_failed"
	case ProducerClosed:
		return "producer_closed"
	case InvalidOffset:
		return "invalid_offset"
	case ConsumerGroupIdNotFound:
		return "consumer_group_id_not_found"
	case ConsumerGroupIdAlreadyExists:
		return "consumer_group_id_already_exists"
	case InvalidConsumerGroupId:
		return "invalid_consumer_group_id"
	case ConsumerGroupNameNotFound:
		return "consumer_group_name_not_found"
	case ConsumerGroupNameAlreadyExists:
		return "consumer_group_name_already_exists"
	case InvalidConsumerGroupName:
		return "invalid_consumer_group_name"
	case ConsumerGroupMemberNotFound:
		return "consumer_group_member_not_found"
	case CannotCreateConsumerGroupInfo:
		return "cannot_create_consumer_group_info"
	case CannotDeleteConsumerGroupInfo:
		return "cannot_delete_consumer_group_info"
	case MissingBaseOffsetRetainedMessageBatch:
		return "missing_base_offset_retained_message_batch"
	case MissingLastOffsetDeltaRetainedMessageBatch:
		return "missing_last_offset_delta_retained_message_batch"
	case MissingMaxTimestampRetainedMessageBatch:
		return "missing_max_timestamp_retained_message_batch"
	case MissingLengthRetainedMessageBatch:
		return "missing_length_retained_message_batch"
	case MissingPayloadRetainedMessageBatch:
		return "missing_payload_retained_message_batch"
	case CannotReadBatchBaseOffset:
		return "cannot_read_batch_base_offset"
	case CannotReadBatchLength:
		return "cannot_read_batch_length"
	case CannotReadLastOffsetDelta:
		return "cannot_read_last_offset_delta"
	case CannotReadMaxTimestamp:
		return "cannot_read_max_timestamp"
	case CannotReadBatchPayload:
		return "cannot_read_batch_payload"
	case InvalidConnectionString:
		return "invalid_connection_string"
	case SnapshotFileCompletionFailed:
		return "snapshot_file_completion_failed"
	case CannotSerializeResource:
		return "cannot_serialize_resource"
	case CannotDeserializeResource:
		return "cannot_deserialize_resource"
	case CannotReadFile:
		return "cannot_read_file"
	case CannotReadFileMetadata:
		return "cannot_read_file_metadata"
	case CannotSeekFile:
		return "cannot_seek_file"
	case CannotAppendToFile:
		return "cannot_append_to_file"
	case CannotWriteToFile:
		return "cannot_write_to_file"
	case CannotOverwriteFile:
		return "cannot_overwrite_file"
	case CannotDeleteFile:
		return "cannot_delete_file"
	case CannotSyncFile:
		return "cannot_sync_file"
	case CannotReadIndexOffset:
		return "cannot_read_index_offset"
	case CannotReadIndexPosition:
		return "cannot_read_index_position"
	case CannotReadIndexTimestamp:
		return "cannot_read_index_timestamp"
	default:
		return "unknown_error_code"
	}
}
