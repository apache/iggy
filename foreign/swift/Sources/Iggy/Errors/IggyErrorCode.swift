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

// Generated from `core/common/src/error/iggy_error.rs` by
// `Tools/golden-vectors`; every discriminant the server can answer with.

/// Numeric error code shared with every other Apache Iggy SDK and the server.
///
/// The raw value is the code that travels on the wire. Codes are stable across
/// releases; a gap in the numbering is a retired code, never free space.
public enum IggyErrorCode: UInt32, Sendable, Hashable, CaseIterable, Codable {
    case error = 1
    case invalidConfiguration = 2
    case invalidCommand = 3
    case invalidFormat = 4
    case featureUnavailable = 5
    case invalidIdentifier = 6
    case invalidVersion = 7
    case disconnected = 8
    case cannotEstablishConnection = 9
    case cannotCreateBaseDirectory = 10
    case cannotCreateRuntimeDirectory = 11
    case cannotRemoveRuntimeDirectory = 12
    case cannotCreateStateDirectory = 13
    case stateFileNotFound = 14
    case stateFileCorrupted = 15
    case invalidStateEntryChecksum = 16
    case cannotOpenDatabase = 19
    case resourceNotFound = 20
    case cannotCloseWebSocketConnection = 21
    case staleClient = 30
    case tcpError = 31
    case quicError = 32
    case invalidServerAddress = 33
    case invalidClientAddress = 34
    case invalidIpAddress = 35
    case httpError = 36
    case invalidApiUrl = 37
    case unauthenticated = 40
    case unauthorized = 41
    case invalidCredentials = 42
    case invalidUsername = 43
    case invalidPassword = 44
    case invalidUserStatus = 45
    case userAlreadyExists = 46
    case userInactive = 47
    case cannotDeleteUser = 48
    case cannotChangePermissions = 49
    case invalidPersonalAccessTokenName = 50
    case personalAccessTokenAlreadyExists = 51
    case personalAccessTokensLimitReached = 52
    case invalidPersonalAccessToken = 53
    case personalAccessTokenExpired = 54
    case usersLimitReached = 55
    case invalidPersonalAccessTokenExpiry = 56
    case transientNotCommitted = 57
    case transientNotAccepted = 58
    case requestAlreadyApplied = 59
    case notConnected = 61
    case clientShutdown = 63
    case invalidTlsDomain = 64
    case invalidTlsCertificatePath = 65
    case invalidTlsCertificate = 66
    case failedToAddCertificate = 67
    case invalidEncryptionKey = 70
    case cannotEncryptData = 71
    case cannotDecryptData = 72
    case invalidJwtAlgorithm = 73
    case invalidJwtSecret = 74
    case jwtMissing = 75
    case cannotGenerateJwt = 76
    case accessTokenMissing = 77
    case invalidAccessToken = 78
    case cannotFetchJwks = 79
    case invalidSizeBytes = 80
    case invalidUtf8 = 81
    case invalidNumberEncoding = 82
    case invalidBooleanValue = 83
    case invalidNumberValue = 84
    case clientNotFound = 100
    case invalidClientId = 101
    case connectionClosed = 206
    case cannotParseHeaderKind = 209
    case httpResponseError = 300
    case invalidHttpRequest = 301
    case invalidJsonResponse = 302
    case invalidBytesResponse = 303
    case emptyResponse = 304
    case cannotCreateEndpoint = 305
    case cannotParseUrl = 306
    case webSocketError = 400
    case webSocketConnectionError = 401
    case webSocketCloseError = 402
    case webSocketReceiveError = 403
    case webSocketSendError = 404
    case cannotCreateStreamsDirectory = 1000
    case cannotCreateStreamDirectory = 1001
    case cannotCreateStreamInfo = 1002
    case cannotUpdateStreamInfo = 1003
    case cannotOpenStreamInfo = 1004
    case cannotReadStreamInfo = 1005
    case cannotCreateStream = 1006
    case cannotDeleteStream = 1007
    case cannotDeleteStreamDirectory = 1008
    case streamIdNotFound = 1009
    case streamNameNotFound = 1010
    case streamDirectoryNotFound = 1011
    case streamNameAlreadyExists = 1012
    case invalidStreamName = 1013
    case invalidStreamId = 1014
    case cannotReadStreams = 1015
    case invalidTopicSize = 1019
    case tooManyStreams = 1020
    case cannotCreateTopicsDirectory = 2000
    case cannotCreateTopicDirectory = 2001
    case cannotCreateTopicInfo = 2002
    case cannotUpdateTopicInfo = 2003
    case cannotOpenTopicInfo = 2004
    case cannotReadTopicInfo = 2005
    case cannotCreateTopic = 2006
    case cannotDeleteTopic = 2007
    case cannotDeleteTopicDirectory = 2008
    case cannotPollTopic = 2009
    case topicIdNotFound = 2010
    case topicNameNotFound = 2011
    case topicNameAlreadyExists = 2013
    case invalidTopicName = 2014
    case tooManyPartitions = 2015
    case invalidTopicId = 2016
    case cannotReadTopics = 2017
    case invalidReplicationFactor = 2018
    case invalidPartitionsCount = 2019
    case topicDirectoryNotFound = 2020
    case tooManyTopics = 2021
    case cannotCreatePartition = 3000
    case cannotCreatePartitionsDirectory = 3001
    case cannotCreatePartitionDirectory = 3002
    case cannotOpenPartitionLogFile = 3003
    case cannotReadPartitions = 3004
    case cannotDeletePartition = 3005
    case cannotDeletePartitionDirectory = 3006
    case partitionNotFound = 3007
    case noPartitions = 3008
    case topicFull = 3009
    case cannotDeleteConsumerOffsetsDirectory = 3010
    case cannotDeleteConsumerOffsetFile = 3011
    case cannotCreateConsumerOffsetsDirectory = 3012
    case partitionIdSpaceExhausted = 3013
    case cannotReadConsumerOffsets = 3020
    case consumerOffsetNotFound = 3021
    case notResolvedConsumer = 3022
    case cannotOpenConsumerOffsetsFile = 3023
    case tooManyConsumerOffsets = 3024
    case segmentNotFound = 4000
    case segmentClosed = 4001
    case invalidSegmentSize = 4002
    case cannotCreateSegmentLogFile = 4003
    case cannotCreateSegmentIndexFile = 4004
    case cannotCreateSegmentTimeIndexFile = 4005
    case cannotSaveMessagesToSegment = 4006
    case cannotSaveIndexToSegment = 4007
    case cannotSaveTimeIndexToSegment = 4008
    case invalidMessagesCount = 4009
    case cannotAppendMessage = 4010
    case cannotReadMessage = 4011
    case cannotReadMessageId = 4012
    case cannotReadMessageState = 4013
    case cannotReadMessageTimestamp = 4014
    case cannotReadHeadersLength = 4015
    case cannotReadHeadersPayload = 4016
    case tooBigUserHeaders = 4017
    case invalidHeaderKey = 4018
    case invalidHeaderValue = 4019
    case cannotReadMessageLength = 4020
    case cannotReadMessagePayload = 4021
    case tooBigMessagePayload = 4022
    case tooManyMessages = 4023
    case emptyMessagePayload = 4024
    case invalidMessagePayloadLength = 4025
    case cannotReadMessageChecksum = 4026
    case invalidMessageChecksum = 4027
    case invalidKeyValueLength = 4028
    case commandLengthError = 4029
    case invalidSegmentsCount = 4030
    case nonZeroOffset = 4031
    case nonZeroTimestamp = 4032
    case missingIndex = 4033
    case invalidIndexesByteSize = 4034
    case invalidIndexesCount = 4035
    case invalidMessagesSize = 4036
    case tooSmallMessage = 4037
    case invalidMessageTimestampDelta = 4038
    case invalidBatchChecksum = 4039
    case invalidHeaderKind = 4040
    case unsupportedOptionKey = 4041
    case invalidOptionValue = 4042
    case optionsBlockTooLarge = 4043
    case cannotSendMessagesDueToClientDisconnection = 4050
    case backgroundSendError = 4051
    case backgroundSendTimeout = 4052
    case backgroundSendBufferFull = 4053
    case backgroundWorkerDisconnected = 4054
    case backgroundSendBufferOverflow = 4055
    case producerSendFailed = 4056
    case producerClosed = 4057
    case invalidOffset = 4100
    case invalidReservedField = 4101
    case segmentSizeMismatchAtOpen = 4102
    case consumerGroupIdNotFound = 5000
    case invalidConsumerGroupId = 5002
    case consumerGroupNameNotFound = 5003
    case consumerGroupNameAlreadyExists = 5004
    case invalidConsumerGroupName = 5005
    case consumerGroupMemberNotFound = 5006
    case cannotCreateConsumerGroupInfo = 5007
    case cannotDeleteConsumerGroupInfo = 5008
    case consumerGroupPartitionNotOwned = 5009
    case missingBaseOffsetRetainedMessageBatch = 6000
    case missingLastOffsetDeltaRetainedMessageBatch = 6001
    case missingMaxTimestampRetainedMessageBatch = 6002
    case missingLengthRetainedMessageBatch = 6003
    case missingPayloadRetainedMessageBatch = 6004
    case cannotReadBatchBaseOffset = 7000
    case cannotReadBatchLength = 7001
    case cannotReadLastOffsetDelta = 7002
    case cannotReadMaxTimestamp = 7003
    case cannotReadBatchPayload = 7004
    case invalidConnectionString = 8000
    case snapshotFileCompletionFailed = 9000
    case cannotSerializeResource = 10000
    case cannotDeserializeResource = 10001
    case cannotReadFile = 10002
    case cannotReadFileMetadata = 10003
    case cannotSeekFile = 10004
    case cannotAppendToFile = 10005
    case cannotWriteToFile = 10006
    case cannotOverwriteFile = 10007
    case cannotDeleteFile = 10008
    case cannotSyncFile = 10009
    case cannotReadIndexOffset = 10010
    case cannotReadIndexPosition = 10011
    case cannotReadIndexTimestamp = 10012
    case timestampOutOfRange = 10013
    case shardNotFound = 11000
    case shardCommunicationError = 11001
    case cannotBindToSocket = 12000
    case taskTimeout = 12001
    case ioError = 13000
    case alreadyAuthenticated = 14000
    case invalidSession = 14001
    case incompatibleProtocolVersion = 14003
}

extension IggyErrorCode {
    /// Snake-case name of the code, identical to the Rust SDK spelling.
    public var name: String {
        switch self {
        case .error: "error"
        case .invalidConfiguration: "invalid_configuration"
        case .invalidCommand: "invalid_command"
        case .invalidFormat: "invalid_format"
        case .featureUnavailable: "feature_unavailable"
        case .invalidIdentifier: "invalid_identifier"
        case .invalidVersion: "invalid_version"
        case .disconnected: "disconnected"
        case .cannotEstablishConnection: "cannot_establish_connection"
        case .cannotCreateBaseDirectory: "cannot_create_base_directory"
        case .cannotCreateRuntimeDirectory: "cannot_create_runtime_directory"
        case .cannotRemoveRuntimeDirectory: "cannot_remove_runtime_directory"
        case .cannotCreateStateDirectory: "cannot_create_state_directory"
        case .stateFileNotFound: "state_file_not_found"
        case .stateFileCorrupted: "state_file_corrupted"
        case .invalidStateEntryChecksum: "invalid_state_entry_checksum"
        case .cannotOpenDatabase: "cannot_open_database"
        case .resourceNotFound: "resource_not_found"
        case .cannotCloseWebSocketConnection: "cannot_close_web_socket_connection"
        case .staleClient: "stale_client"
        case .tcpError: "tcp_error"
        case .quicError: "quic_error"
        case .invalidServerAddress: "invalid_server_address"
        case .invalidClientAddress: "invalid_client_address"
        case .invalidIpAddress: "invalid_ip_address"
        case .httpError: "http_error"
        case .invalidApiUrl: "invalid_api_url"
        case .unauthenticated: "unauthenticated"
        case .unauthorized: "unauthorized"
        case .invalidCredentials: "invalid_credentials"
        case .invalidUsername: "invalid_username"
        case .invalidPassword: "invalid_password"
        case .invalidUserStatus: "invalid_user_status"
        case .userAlreadyExists: "user_already_exists"
        case .userInactive: "user_inactive"
        case .cannotDeleteUser: "cannot_delete_user"
        case .cannotChangePermissions: "cannot_change_permissions"
        case .invalidPersonalAccessTokenName: "invalid_personal_access_token_name"
        case .personalAccessTokenAlreadyExists: "personal_access_token_already_exists"
        case .personalAccessTokensLimitReached: "personal_access_tokens_limit_reached"
        case .invalidPersonalAccessToken: "invalid_personal_access_token"
        case .personalAccessTokenExpired: "personal_access_token_expired"
        case .usersLimitReached: "users_limit_reached"
        case .invalidPersonalAccessTokenExpiry: "invalid_personal_access_token_expiry"
        case .transientNotCommitted: "transient_not_committed"
        case .transientNotAccepted: "transient_not_accepted"
        case .requestAlreadyApplied: "request_already_applied"
        case .notConnected: "not_connected"
        case .clientShutdown: "client_shutdown"
        case .invalidTlsDomain: "invalid_tls_domain"
        case .invalidTlsCertificatePath: "invalid_tls_certificate_path"
        case .invalidTlsCertificate: "invalid_tls_certificate"
        case .failedToAddCertificate: "failed_to_add_certificate"
        case .invalidEncryptionKey: "invalid_encryption_key"
        case .cannotEncryptData: "cannot_encrypt_data"
        case .cannotDecryptData: "cannot_decrypt_data"
        case .invalidJwtAlgorithm: "invalid_jwt_algorithm"
        case .invalidJwtSecret: "invalid_jwt_secret"
        case .jwtMissing: "jwt_missing"
        case .cannotGenerateJwt: "cannot_generate_jwt"
        case .accessTokenMissing: "access_token_missing"
        case .invalidAccessToken: "invalid_access_token"
        case .cannotFetchJwks: "cannot_fetch_jwks"
        case .invalidSizeBytes: "invalid_size_bytes"
        case .invalidUtf8: "invalid_utf8"
        case .invalidNumberEncoding: "invalid_number_encoding"
        case .invalidBooleanValue: "invalid_boolean_value"
        case .invalidNumberValue: "invalid_number_value"
        case .clientNotFound: "client_not_found"
        case .invalidClientId: "invalid_client_id"
        case .connectionClosed: "connection_closed"
        case .cannotParseHeaderKind: "cannot_parse_header_kind"
        case .httpResponseError: "http_response_error"
        case .invalidHttpRequest: "invalid_http_request"
        case .invalidJsonResponse: "invalid_json_response"
        case .invalidBytesResponse: "invalid_bytes_response"
        case .emptyResponse: "empty_response"
        case .cannotCreateEndpoint: "cannot_create_endpoint"
        case .cannotParseUrl: "cannot_parse_url"
        case .webSocketError: "web_socket_error"
        case .webSocketConnectionError: "web_socket_connection_error"
        case .webSocketCloseError: "web_socket_close_error"
        case .webSocketReceiveError: "web_socket_receive_error"
        case .webSocketSendError: "web_socket_send_error"
        case .cannotCreateStreamsDirectory: "cannot_create_streams_directory"
        case .cannotCreateStreamDirectory: "cannot_create_stream_directory"
        case .cannotCreateStreamInfo: "cannot_create_stream_info"
        case .cannotUpdateStreamInfo: "cannot_update_stream_info"
        case .cannotOpenStreamInfo: "cannot_open_stream_info"
        case .cannotReadStreamInfo: "cannot_read_stream_info"
        case .cannotCreateStream: "cannot_create_stream"
        case .cannotDeleteStream: "cannot_delete_stream"
        case .cannotDeleteStreamDirectory: "cannot_delete_stream_directory"
        case .streamIdNotFound: "stream_id_not_found"
        case .streamNameNotFound: "stream_name_not_found"
        case .streamDirectoryNotFound: "stream_directory_not_found"
        case .streamNameAlreadyExists: "stream_name_already_exists"
        case .invalidStreamName: "invalid_stream_name"
        case .invalidStreamId: "invalid_stream_id"
        case .cannotReadStreams: "cannot_read_streams"
        case .invalidTopicSize: "invalid_topic_size"
        case .tooManyStreams: "too_many_streams"
        case .cannotCreateTopicsDirectory: "cannot_create_topics_directory"
        case .cannotCreateTopicDirectory: "cannot_create_topic_directory"
        case .cannotCreateTopicInfo: "cannot_create_topic_info"
        case .cannotUpdateTopicInfo: "cannot_update_topic_info"
        case .cannotOpenTopicInfo: "cannot_open_topic_info"
        case .cannotReadTopicInfo: "cannot_read_topic_info"
        case .cannotCreateTopic: "cannot_create_topic"
        case .cannotDeleteTopic: "cannot_delete_topic"
        case .cannotDeleteTopicDirectory: "cannot_delete_topic_directory"
        case .cannotPollTopic: "cannot_poll_topic"
        case .topicIdNotFound: "topic_id_not_found"
        case .topicNameNotFound: "topic_name_not_found"
        case .topicNameAlreadyExists: "topic_name_already_exists"
        case .invalidTopicName: "invalid_topic_name"
        case .tooManyPartitions: "too_many_partitions"
        case .invalidTopicId: "invalid_topic_id"
        case .cannotReadTopics: "cannot_read_topics"
        case .invalidReplicationFactor: "invalid_replication_factor"
        case .invalidPartitionsCount: "invalid_partitions_count"
        case .topicDirectoryNotFound: "topic_directory_not_found"
        case .tooManyTopics: "too_many_topics"
        case .cannotCreatePartition: "cannot_create_partition"
        case .cannotCreatePartitionsDirectory: "cannot_create_partitions_directory"
        case .cannotCreatePartitionDirectory: "cannot_create_partition_directory"
        case .cannotOpenPartitionLogFile: "cannot_open_partition_log_file"
        case .cannotReadPartitions: "cannot_read_partitions"
        case .cannotDeletePartition: "cannot_delete_partition"
        case .cannotDeletePartitionDirectory: "cannot_delete_partition_directory"
        case .partitionNotFound: "partition_not_found"
        case .noPartitions: "no_partitions"
        case .topicFull: "topic_full"
        case .cannotDeleteConsumerOffsetsDirectory: "cannot_delete_consumer_offsets_directory"
        case .cannotDeleteConsumerOffsetFile: "cannot_delete_consumer_offset_file"
        case .cannotCreateConsumerOffsetsDirectory: "cannot_create_consumer_offsets_directory"
        case .partitionIdSpaceExhausted: "partition_id_space_exhausted"
        case .cannotReadConsumerOffsets: "cannot_read_consumer_offsets"
        case .consumerOffsetNotFound: "consumer_offset_not_found"
        case .notResolvedConsumer: "not_resolved_consumer"
        case .cannotOpenConsumerOffsetsFile: "cannot_open_consumer_offsets_file"
        case .tooManyConsumerOffsets: "too_many_consumer_offsets"
        case .segmentNotFound: "segment_not_found"
        case .segmentClosed: "segment_closed"
        case .invalidSegmentSize: "invalid_segment_size"
        case .cannotCreateSegmentLogFile: "cannot_create_segment_log_file"
        case .cannotCreateSegmentIndexFile: "cannot_create_segment_index_file"
        case .cannotCreateSegmentTimeIndexFile: "cannot_create_segment_time_index_file"
        case .cannotSaveMessagesToSegment: "cannot_save_messages_to_segment"
        case .cannotSaveIndexToSegment: "cannot_save_index_to_segment"
        case .cannotSaveTimeIndexToSegment: "cannot_save_time_index_to_segment"
        case .invalidMessagesCount: "invalid_messages_count"
        case .cannotAppendMessage: "cannot_append_message"
        case .cannotReadMessage: "cannot_read_message"
        case .cannotReadMessageId: "cannot_read_message_id"
        case .cannotReadMessageState: "cannot_read_message_state"
        case .cannotReadMessageTimestamp: "cannot_read_message_timestamp"
        case .cannotReadHeadersLength: "cannot_read_headers_length"
        case .cannotReadHeadersPayload: "cannot_read_headers_payload"
        case .tooBigUserHeaders: "too_big_user_headers"
        case .invalidHeaderKey: "invalid_header_key"
        case .invalidHeaderValue: "invalid_header_value"
        case .cannotReadMessageLength: "cannot_read_message_length"
        case .cannotReadMessagePayload: "cannot_read_message_payload"
        case .tooBigMessagePayload: "too_big_message_payload"
        case .tooManyMessages: "too_many_messages"
        case .emptyMessagePayload: "empty_message_payload"
        case .invalidMessagePayloadLength: "invalid_message_payload_length"
        case .cannotReadMessageChecksum: "cannot_read_message_checksum"
        case .invalidMessageChecksum: "invalid_message_checksum"
        case .invalidKeyValueLength: "invalid_key_value_length"
        case .commandLengthError: "command_length_error"
        case .invalidSegmentsCount: "invalid_segments_count"
        case .nonZeroOffset: "non_zero_offset"
        case .nonZeroTimestamp: "non_zero_timestamp"
        case .missingIndex: "missing_index"
        case .invalidIndexesByteSize: "invalid_indexes_byte_size"
        case .invalidIndexesCount: "invalid_indexes_count"
        case .invalidMessagesSize: "invalid_messages_size"
        case .tooSmallMessage: "too_small_message"
        case .invalidMessageTimestampDelta: "invalid_message_timestamp_delta"
        case .invalidBatchChecksum: "invalid_batch_checksum"
        case .invalidHeaderKind: "invalid_header_kind"
        case .unsupportedOptionKey: "unsupported_option_key"
        case .invalidOptionValue: "invalid_option_value"
        case .optionsBlockTooLarge: "options_block_too_large"
        case .cannotSendMessagesDueToClientDisconnection: "cannot_send_messages_due_to_client_disconnection"
        case .backgroundSendError: "background_send_error"
        case .backgroundSendTimeout: "background_send_timeout"
        case .backgroundSendBufferFull: "background_send_buffer_full"
        case .backgroundWorkerDisconnected: "background_worker_disconnected"
        case .backgroundSendBufferOverflow: "background_send_buffer_overflow"
        case .producerSendFailed: "producer_send_failed"
        case .producerClosed: "producer_closed"
        case .invalidOffset: "invalid_offset"
        case .invalidReservedField: "invalid_reserved_field"
        case .segmentSizeMismatchAtOpen: "segment_size_mismatch_at_open"
        case .consumerGroupIdNotFound: "consumer_group_id_not_found"
        case .invalidConsumerGroupId: "invalid_consumer_group_id"
        case .consumerGroupNameNotFound: "consumer_group_name_not_found"
        case .consumerGroupNameAlreadyExists: "consumer_group_name_already_exists"
        case .invalidConsumerGroupName: "invalid_consumer_group_name"
        case .consumerGroupMemberNotFound: "consumer_group_member_not_found"
        case .cannotCreateConsumerGroupInfo: "cannot_create_consumer_group_info"
        case .cannotDeleteConsumerGroupInfo: "cannot_delete_consumer_group_info"
        case .consumerGroupPartitionNotOwned: "consumer_group_partition_not_owned"
        case .missingBaseOffsetRetainedMessageBatch: "missing_base_offset_retained_message_batch"
        case .missingLastOffsetDeltaRetainedMessageBatch: "missing_last_offset_delta_retained_message_batch"
        case .missingMaxTimestampRetainedMessageBatch: "missing_max_timestamp_retained_message_batch"
        case .missingLengthRetainedMessageBatch: "missing_length_retained_message_batch"
        case .missingPayloadRetainedMessageBatch: "missing_payload_retained_message_batch"
        case .cannotReadBatchBaseOffset: "cannot_read_batch_base_offset"
        case .cannotReadBatchLength: "cannot_read_batch_length"
        case .cannotReadLastOffsetDelta: "cannot_read_last_offset_delta"
        case .cannotReadMaxTimestamp: "cannot_read_max_timestamp"
        case .cannotReadBatchPayload: "cannot_read_batch_payload"
        case .invalidConnectionString: "invalid_connection_string"
        case .snapshotFileCompletionFailed: "snapshot_file_completion_failed"
        case .cannotSerializeResource: "cannot_serialize_resource"
        case .cannotDeserializeResource: "cannot_deserialize_resource"
        case .cannotReadFile: "cannot_read_file"
        case .cannotReadFileMetadata: "cannot_read_file_metadata"
        case .cannotSeekFile: "cannot_seek_file"
        case .cannotAppendToFile: "cannot_append_to_file"
        case .cannotWriteToFile: "cannot_write_to_file"
        case .cannotOverwriteFile: "cannot_overwrite_file"
        case .cannotDeleteFile: "cannot_delete_file"
        case .cannotSyncFile: "cannot_sync_file"
        case .cannotReadIndexOffset: "cannot_read_index_offset"
        case .cannotReadIndexPosition: "cannot_read_index_position"
        case .cannotReadIndexTimestamp: "cannot_read_index_timestamp"
        case .timestampOutOfRange: "timestamp_out_of_range"
        case .shardNotFound: "shard_not_found"
        case .shardCommunicationError: "shard_communication_error"
        case .cannotBindToSocket: "cannot_bind_to_socket"
        case .taskTimeout: "task_timeout"
        case .ioError: "io_error"
        case .alreadyAuthenticated: "already_authenticated"
        case .invalidSession: "invalid_session"
        case .incompatibleProtocolVersion: "incompatible_protocol_version"
        }
    }
}
