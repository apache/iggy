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

import (
	"errors"
	"fmt"
	"testing"
)

var allErrors = []struct {
	name string
	code Code
	err  IggyError
}{
	{"Error", ErrorCode, ErrError},
	{"InvalidConfiguration", InvalidConfigurationCode, ErrInvalidConfiguration},
	{"InvalidCommand", InvalidCommandCode, ErrInvalidCommand},
	{"InvalidFormat", InvalidFormatCode, ErrInvalidFormat},
	{"FeatureUnavailable", FeatureUnavailableCode, ErrFeatureUnavailable},
	{"InvalidIdentifier", InvalidIdentifierCode, ErrInvalidIdentifier},
	{"InvalidVersion", InvalidVersionCode, ErrInvalidVersion},
	{"Disconnected", DisconnectedCode, ErrDisconnected},
	{"CannotEstablishConnection", CannotEstablishConnectionCode, ErrCannotEstablishConnection},
	{"CannotCreateBaseDirectory", CannotCreateBaseDirectoryCode, ErrCannotCreateBaseDirectory},
	{"CannotCreateRuntimeDirectory", CannotCreateRuntimeDirectoryCode, ErrCannotCreateRuntimeDirectory},
	{"CannotRemoveRuntimeDirectory", CannotRemoveRuntimeDirectoryCode, ErrCannotRemoveRuntimeDirectory},
	{"CannotCreateStateDirectory", CannotCreateStateDirectoryCode, ErrCannotCreateStateDirectory},
	{"StateFileNotFound", StateFileNotFoundCode, ErrStateFileNotFound},
	{"StateFileCorrupted", StateFileCorruptedCode, ErrStateFileCorrupted},
	{"InvalidStateEntryChecksum", InvalidStateEntryChecksumCode, ErrInvalidStateEntryChecksum},
	{"CannotOpenDatabase", CannotOpenDatabaseCode, ErrCannotOpenDatabase},
	{"ResourceNotFound", ResourceNotFoundCode, ErrResourceNotFound},
	{"StaleClient", StaleClientCode, ErrStaleClient},
	{"TcpError", TcpErrorCode, ErrTcpError},
	{"QuicError", QuicErrorCode, ErrQuicError},
	{"InvalidServerAddress", InvalidServerAddressCode, ErrInvalidServerAddress},
	{"InvalidClientAddress", InvalidClientAddressCode, ErrInvalidClientAddress},
	{"InvalidIpAddress", InvalidIpAddressCode, ErrInvalidIpAddress},
	{"Unauthenticated", UnauthenticatedCode, ErrUnauthenticated},
	{"Unauthorized", UnauthorizedCode, ErrUnauthorized},
	{"InvalidCredentials", InvalidCredentialsCode, ErrInvalidCredentials},
	{"InvalidUsername", InvalidUsernameCode, ErrInvalidUsername},
	{"InvalidPassword", InvalidPasswordCode, ErrInvalidPassword},
	{"InvalidUserStatus", InvalidUserStatusCode, ErrInvalidUserStatus},
	{"UserAlreadyExists", UserAlreadyExistsCode, ErrUserAlreadyExists},
	{"UserInactive", UserInactiveCode, ErrUserInactive},
	{"CannotDeleteUser", CannotDeleteUserCode, ErrCannotDeleteUser},
	{"CannotChangePermissions", CannotChangePermissionsCode, ErrCannotChangePermissions},
	{"InvalidPersonalAccessTokenName", InvalidPersonalAccessTokenNameCode, ErrInvalidPersonalAccessTokenName},
	{"PersonalAccessTokenAlreadyExists", PersonalAccessTokenAlreadyExistsCode, ErrPersonalAccessTokenAlreadyExists},
	{"PersonalAccessTokensLimitReached", PersonalAccessTokensLimitReachedCode, ErrPersonalAccessTokensLimitReached},
	{"InvalidPersonalAccessToken", InvalidPersonalAccessTokenCode, ErrInvalidPersonalAccessToken},
	{"PersonalAccessTokenExpired", PersonalAccessTokenExpiredCode, ErrPersonalAccessTokenExpired},
	{"UsersLimitReached", UsersLimitReachedCode, ErrUsersLimitReached},
	{"NotConnected", NotConnectedCode, ErrNotConnected},
	{"ClientShutdown", ClientShutdownCode, ErrClientShutdown},
	{"InvalidTlsDomain", InvalidTlsDomainCode, ErrInvalidTlsDomain},
	{"InvalidTlsCertificatePath", InvalidTlsCertificatePathCode, ErrInvalidTlsCertificatePath},
	{"InvalidTlsCertificate", InvalidTlsCertificateCode, ErrInvalidTlsCertificate},
	{"FailedToAddCertificate", FailedToAddCertificateCode, ErrFailedToAddCertificate},
	{"InvalidEncryptionKey", InvalidEncryptionKeyCode, ErrInvalidEncryptionKey},
	{"CannotEncryptData", CannotEncryptDataCode, ErrCannotEncryptData},
	{"CannotDecryptData", CannotDecryptDataCode, ErrCannotDecryptData},
	{"InvalidJwtAlgorithm", InvalidJwtAlgorithmCode, ErrInvalidJwtAlgorithm},
	{"InvalidJwtSecret", InvalidJwtSecretCode, ErrInvalidJwtSecret},
	{"JwtMissing", JwtMissingCode, ErrJwtMissing},
	{"CannotGenerateJwt", CannotGenerateJwtCode, ErrCannotGenerateJwt},
	{"AccessTokenMissing", AccessTokenMissingCode, ErrAccessTokenMissing},
	{"InvalidAccessToken", InvalidAccessTokenCode, ErrInvalidAccessToken},
	{"InvalidSizeBytes", InvalidSizeBytesCode, ErrInvalidSizeBytes},
	{"InvalidUtf8", InvalidUtf8Code, ErrInvalidUtf8},
	{"InvalidNumberEncoding", InvalidNumberEncodingCode, ErrInvalidNumberEncoding},
	{"InvalidBooleanValue", InvalidBooleanValueCode, ErrInvalidBooleanValue},
	{"InvalidNumberValue", InvalidNumberValueCode, ErrInvalidNumberValue},
	{"ClientNotFound", ClientNotFoundCode, ErrClientNotFound},
	{"InvalidClientId", InvalidClientIdCode, ErrInvalidClientId},
	{"ConnectionClosed", ConnectionClosedCode, ErrConnectionClosed},
	{"CannotParseHeaderKind", CannotParseHeaderKindCode, ErrCannotParseHeaderKind},
	{"HttpResponseError", HttpResponseErrorCode, ErrHttpResponseError},
	{"InvalidHttpRequest", InvalidHttpRequestCode, ErrInvalidHttpRequest},
	{"InvalidJsonResponse", InvalidJsonResponseCode, ErrInvalidJsonResponse},
	{"InvalidBytesResponse", InvalidBytesResponseCode, ErrInvalidBytesResponse},
	{"EmptyResponse", EmptyResponseCode, ErrEmptyResponse},
	{"CannotCreateEndpoint", CannotCreateEndpointCode, ErrCannotCreateEndpoint},
	{"CannotParseUrl", CannotParseUrlCode, ErrCannotParseUrl},
	{"CannotCreateStreamsDirectory", CannotCreateStreamsDirectoryCode, ErrCannotCreateStreamsDirectory},
	{"CannotCreateStreamDirectory", CannotCreateStreamDirectoryCode, ErrCannotCreateStreamDirectory},
	{"CannotCreateStreamInfo", CannotCreateStreamInfoCode, ErrCannotCreateStreamInfo},
	{"CannotUpdateStreamInfo", CannotUpdateStreamInfoCode, ErrCannotUpdateStreamInfo},
	{"CannotOpenStreamInfo", CannotOpenStreamInfoCode, ErrCannotOpenStreamInfo},
	{"CannotReadStreamInfo", CannotReadStreamInfoCode, ErrCannotReadStreamInfo},
	{"CannotCreateStream", CannotCreateStreamCode, ErrCannotCreateStream},
	{"CannotDeleteStream", CannotDeleteStreamCode, ErrCannotDeleteStream},
	{"CannotDeleteStreamDirectory", CannotDeleteStreamDirectoryCode, ErrCannotDeleteStreamDirectory},
	{"StreamIdNotFound", StreamIdNotFoundCode, ErrStreamIdNotFound},
	{"StreamNameNotFound", StreamNameNotFoundCode, ErrStreamNameNotFound},
	{"StreamIdAlreadyExists", StreamIdAlreadyExistsCode, ErrStreamIdAlreadyExists},
	{"StreamNameAlreadyExists", StreamNameAlreadyExistsCode, ErrStreamNameAlreadyExists},
	{"InvalidStreamName", InvalidStreamNameCode, ErrInvalidStreamName},
	{"InvalidStreamId", InvalidStreamIdCode, ErrInvalidStreamId},
	{"CannotReadStreams", CannotReadStreamsCode, ErrCannotReadStreams},
	{"InvalidTopicSize", InvalidTopicSizeCode, ErrInvalidTopicSize},
	{"CannotCreateTopicsDirectory", CannotCreateTopicsDirectoryCode, ErrCannotCreateTopicsDirectory},
	{"CannotCreateTopicDirectory", CannotCreateTopicDirectoryCode, ErrCannotCreateTopicDirectory},
	{"CannotCreateTopicInfo", CannotCreateTopicInfoCode, ErrCannotCreateTopicInfo},
	{"CannotUpdateTopicInfo", CannotUpdateTopicInfoCode, ErrCannotUpdateTopicInfo},
	{"CannotOpenTopicInfo", CannotOpenTopicInfoCode, ErrCannotOpenTopicInfo},
	{"CannotReadTopicInfo", CannotReadTopicInfoCode, ErrCannotReadTopicInfo},
	{"CannotCreateTopic", CannotCreateTopicCode, ErrCannotCreateTopic},
	{"CannotDeleteTopic", CannotDeleteTopicCode, ErrCannotDeleteTopic},
	{"CannotDeleteTopicDirectory", CannotDeleteTopicDirectoryCode, ErrCannotDeleteTopicDirectory},
	{"CannotPollTopic", CannotPollTopicCode, ErrCannotPollTopic},
	{"TopicIdNotFound", TopicIdNotFoundCode, ErrTopicIdNotFound},
	{"TopicNameNotFound", TopicNameNotFoundCode, ErrTopicNameNotFound},
	{"TopicIdAlreadyExists", TopicIdAlreadyExistsCode, ErrTopicIdAlreadyExists},
	{"TopicNameAlreadyExists", TopicNameAlreadyExistsCode, ErrTopicNameAlreadyExists},
	{"InvalidTopicName", InvalidTopicNameCode, ErrInvalidTopicName},
	{"TooManyPartitions", TooManyPartitionsCode, ErrTooManyPartitions},
	{"InvalidTopicId", InvalidTopicIdCode, ErrInvalidTopicId},
	{"CannotReadTopics", CannotReadTopicsCode, ErrCannotReadTopics},
	{"InvalidReplicationFactor", InvalidReplicationFactorCode, ErrInvalidReplicationFactor},
	{"CannotCreatePartition", CannotCreatePartitionCode, ErrCannotCreatePartition},
	{"CannotCreatePartitionsDirectory", CannotCreatePartitionsDirectoryCode, ErrCannotCreatePartitionsDirectory},
	{"CannotCreatePartitionDirectory", CannotCreatePartitionDirectoryCode, ErrCannotCreatePartitionDirectory},
	{"CannotOpenPartitionLogFile", CannotOpenPartitionLogFileCode, ErrCannotOpenPartitionLogFile},
	{"CannotReadPartitions", CannotReadPartitionsCode, ErrCannotReadPartitions},
	{"CannotDeletePartition", CannotDeletePartitionCode, ErrCannotDeletePartition},
	{"CannotDeletePartitionDirectory", CannotDeletePartitionDirectoryCode, ErrCannotDeletePartitionDirectory},
	{"PartitionNotFound", PartitionNotFoundCode, ErrPartitionNotFound},
	{"NoPartitions", NoPartitionsCode, ErrNoPartitions},
	{"TopicFull", TopicFullCode, ErrTopicFull},
	{"CannotDeleteConsumerOffsetsDirectory", CannotDeleteConsumerOffsetsDirectoryCode, ErrCannotDeleteConsumerOffsetsDirectory},
	{"CannotDeleteConsumerOffsetFile", CannotDeleteConsumerOffsetFileCode, ErrCannotDeleteConsumerOffsetFile},
	{"CannotCreateConsumerOffsetsDirectory", CannotCreateConsumerOffsetsDirectoryCode, ErrCannotCreateConsumerOffsetsDirectory},
	{"CannotReadConsumerOffsets", CannotReadConsumerOffsetsCode, ErrCannotReadConsumerOffsets},
	{"ConsumerOffsetNotFound", ConsumerOffsetNotFoundCode, ErrConsumerOffsetNotFound},
	{"SegmentNotFound", SegmentNotFoundCode, ErrSegmentNotFound},
	{"SegmentClosed", SegmentClosedCode, ErrSegmentClosed},
	{"InvalidSegmentSize", InvalidSegmentSizeCode, ErrInvalidSegmentSize},
	{"CannotCreateSegmentLogFile", CannotCreateSegmentLogFileCode, ErrCannotCreateSegmentLogFile},
	{"CannotCreateSegmentIndexFile", CannotCreateSegmentIndexFileCode, ErrCannotCreateSegmentIndexFile},
	{"CannotCreateSegmentTimeIndexFile", CannotCreateSegmentTimeIndexFileCode, ErrCannotCreateSegmentTimeIndexFile},
	{"CannotSaveMessagesToSegment", CannotSaveMessagesToSegmentCode, ErrCannotSaveMessagesToSegment},
	{"CannotSaveIndexToSegment", CannotSaveIndexToSegmentCode, ErrCannotSaveIndexToSegment},
	{"CannotSaveTimeIndexToSegment", CannotSaveTimeIndexToSegmentCode, ErrCannotSaveTimeIndexToSegment},
	{"InvalidMessagesCount", InvalidMessagesCountCode, ErrInvalidMessagesCount},
	{"CannotAppendMessage", CannotAppendMessageCode, ErrCannotAppendMessage},
	{"CannotReadMessage", CannotReadMessageCode, ErrCannotReadMessage},
	{"CannotReadMessageId", CannotReadMessageIdCode, ErrCannotReadMessageId},
	{"CannotReadMessageState", CannotReadMessageStateCode, ErrCannotReadMessageState},
	{"CannotReadMessageTimestamp", CannotReadMessageTimestampCode, ErrCannotReadMessageTimestamp},
	{"CannotReadHeadersLength", CannotReadHeadersLengthCode, ErrCannotReadHeadersLength},
	{"CannotReadHeadersPayload", CannotReadHeadersPayloadCode, ErrCannotReadHeadersPayload},
	{"TooBigUserHeaders", TooBigUserHeadersCode, ErrTooBigUserHeaders},
	{"InvalidHeaderKey", InvalidHeaderKeyCode, ErrInvalidHeaderKey},
	{"InvalidHeaderValue", InvalidHeaderValueCode, ErrInvalidHeaderValue},
	{"CannotReadMessageLength", CannotReadMessageLengthCode, ErrCannotReadMessageLength},
	{"CannotReadMessagePayload", CannotReadMessagePayloadCode, ErrCannotReadMessagePayload},
	{"TooBigMessagePayload", TooBigMessagePayloadCode, ErrTooBigMessagePayload},
	{"TooManyMessages", TooManyMessagesCode, ErrTooManyMessages},
	{"EmptyMessagePayload", EmptyMessagePayloadCode, ErrEmptyMessagePayload},
	{"InvalidMessagePayloadLength", InvalidMessagePayloadLengthCode, ErrInvalidMessagePayloadLength},
	{"CannotReadMessageChecksum", CannotReadMessageChecksumCode, ErrCannotReadMessageChecksum},
	{"InvalidMessageChecksum", InvalidMessageChecksumCode, ErrInvalidMessageChecksum},
	{"InvalidKeyValueLength", InvalidKeyValueLengthCode, ErrInvalidKeyValueLength},
	{"CommandLengthError", CommandLengthErrorCode, ErrCommandLengthError},
	{"InvalidSegmentsCount", InvalidSegmentsCountCode, ErrInvalidSegmentsCount},
	{"NonZeroOffset", NonZeroOffsetCode, ErrNonZeroOffset},
	{"NonZeroTimestamp", NonZeroTimestampCode, ErrNonZeroTimestamp},
	{"MissingIndex", MissingIndexCode, ErrMissingIndex},
	{"InvalidIndexesByteSize", InvalidIndexesByteSizeCode, ErrInvalidIndexesByteSize},
	{"InvalidIndexesCount", InvalidIndexesCountCode, ErrInvalidIndexesCount},
	{"InvalidMessagesSize", InvalidMessagesSizeCode, ErrInvalidMessagesSize},
	{"TooSmallMessage", TooSmallMessageCode, ErrTooSmallMessage},
	{"CannotSendMessagesDueToClientDisconnection", CannotSendMessagesDueToClientDisconnectionCode, ErrCannotSendMessagesDueToClientDisconnection},
	{"BackgroundSendError", BackgroundSendErrorCode, ErrBackgroundSendError},
	{"BackgroundSendTimeout", BackgroundSendTimeoutCode, ErrBackgroundSendTimeout},
	{"BackgroundSendBufferFull", BackgroundSendBufferFullCode, ErrBackgroundSendBufferFull},
	{"BackgroundWorkerDisconnected", BackgroundWorkerDisconnectedCode, ErrBackgroundWorkerDisconnected},
	{"BackgroundSendBufferOverflow", BackgroundSendBufferOverflowCode, ErrBackgroundSendBufferOverflow},
	{"ProducerSendFailed", ProducerSendFailedCode, ErrProducerSendFailed},
	{"ProducerClosed", ProducerClosedCode, ErrProducerClosed},
	{"InvalidOffset", InvalidOffsetCode, ErrInvalidOffset},
	{"ConsumerGroupIdNotFound", ConsumerGroupIdNotFoundCode, ErrConsumerGroupIdNotFound},
	{"ConsumerGroupIdAlreadyExists", ConsumerGroupIdAlreadyExistsCode, ErrConsumerGroupIdAlreadyExists},
	{"InvalidConsumerGroupId", InvalidConsumerGroupIdCode, ErrInvalidConsumerGroupId},
	{"ConsumerGroupNameNotFound", ConsumerGroupNameNotFoundCode, ErrConsumerGroupNameNotFound},
	{"ConsumerGroupNameAlreadyExists", ConsumerGroupNameAlreadyExistsCode, ErrConsumerGroupNameAlreadyExists},
	{"InvalidConsumerGroupName", InvalidConsumerGroupNameCode, ErrInvalidConsumerGroupName},
	{"ConsumerGroupMemberNotFound", ConsumerGroupMemberNotFoundCode, ErrConsumerGroupMemberNotFound},
	{"CannotCreateConsumerGroupInfo", CannotCreateConsumerGroupInfoCode, ErrCannotCreateConsumerGroupInfo},
	{"CannotDeleteConsumerGroupInfo", CannotDeleteConsumerGroupInfoCode, ErrCannotDeleteConsumerGroupInfo},
	{"MissingBaseOffsetRetainedMessageBatch", MissingBaseOffsetRetainedMessageBatchCode, ErrMissingBaseOffsetRetainedMessageBatch},
	{"MissingLastOffsetDeltaRetainedMessageBatch", MissingLastOffsetDeltaRetainedMessageBatchCode, ErrMissingLastOffsetDeltaRetainedMessageBatch},
	{"MissingMaxTimestampRetainedMessageBatch", MissingMaxTimestampRetainedMessageBatchCode, ErrMissingMaxTimestampRetainedMessageBatch},
	{"MissingLengthRetainedMessageBatch", MissingLengthRetainedMessageBatchCode, ErrMissingLengthRetainedMessageBatch},
	{"MissingPayloadRetainedMessageBatch", MissingPayloadRetainedMessageBatchCode, ErrMissingPayloadRetainedMessageBatch},
	{"CannotReadBatchBaseOffset", CannotReadBatchBaseOffsetCode, ErrCannotReadBatchBaseOffset},
	{"CannotReadBatchLength", CannotReadBatchLengthCode, ErrCannotReadBatchLength},
	{"CannotReadLastOffsetDelta", CannotReadLastOffsetDeltaCode, ErrCannotReadLastOffsetDelta},
	{"CannotReadMaxTimestamp", CannotReadMaxTimestampCode, ErrCannotReadMaxTimestamp},
	{"CannotReadBatchPayload", CannotReadBatchPayloadCode, ErrCannotReadBatchPayload},
	{"InvalidConnectionString", InvalidConnectionStringCode, ErrInvalidConnectionString},
	{"SnapshotFileCompletionFailed", SnapshotFileCompletionFailedCode, ErrSnapshotFileCompletionFailed},
	{"CannotSerializeResource", CannotSerializeResourceCode, ErrCannotSerializeResource},
	{"CannotDeserializeResource", CannotDeserializeResourceCode, ErrCannotDeserializeResource},
	{"CannotReadFile", CannotReadFileCode, ErrCannotReadFile},
	{"CannotReadFileMetadata", CannotReadFileMetadataCode, ErrCannotReadFileMetadata},
	{"CannotSeekFile", CannotSeekFileCode, ErrCannotSeekFile},
	{"CannotAppendToFile", CannotAppendToFileCode, ErrCannotAppendToFile},
	{"CannotWriteToFile", CannotWriteToFileCode, ErrCannotWriteToFile},
	{"CannotOverwriteFile", CannotOverwriteFileCode, ErrCannotOverwriteFile},
	{"CannotDeleteFile", CannotDeleteFileCode, ErrCannotDeleteFile},
	{"CannotSyncFile", CannotSyncFileCode, ErrCannotSyncFile},
	{"CannotReadIndexOffset", CannotReadIndexOffsetCode, ErrCannotReadIndexOffset},
	{"CannotReadIndexPosition", CannotReadIndexPositionCode, ErrCannotReadIndexPosition},
	{"CannotReadIndexTimestamp", CannotReadIndexTimestampCode, ErrCannotReadIndexTimestamp},
}

func TestFromCode_ReturnsCorrectErrors(t *testing.T) {
	for _, tt := range allErrors {
		t.Run(tt.name, func(t *testing.T) {
			got := FromCode(tt.code)
			if got == nil {
				t.Fatalf("FromCode(%d) returned nil", tt.code)
			}
			if got.Code() != tt.code {
				t.Fatalf("FromCode(%d).Code() = %d, want %d", tt.code, got.Code(), tt.code)
			}
			if got.Error() == "" {
				t.Fatalf("FromCode(%d).Error() returned empty string", tt.code)
			}
			if !errors.Is(got, tt.err) {
				t.Fatalf("errors.Is(FromCode(%d), expected) = false, want true", tt.code)
			}
		})
	}
}

func TestFromCode_UnknownCode(t *testing.T) {
	got := FromCode(Code(99999))
	if got == nil {
		t.Fatalf("FromCode(99999) returned nil, expected fallback error")
	}
	if got.Code() != ErrorCode {
		t.Fatalf("FromCode(99999).Code() = %d, want %d (fallback to ErrError)", got.Code(), ErrorCode)
	}
}

func TestCodeString_AllCodes(t *testing.T) {
	for _, tt := range allErrors {
		s := tt.code.String()
		if s == "" {
			t.Fatalf("Code(%d).String() returned empty string", tt.code)
		}
		if s == "Unknown error code" {
			t.Fatalf("Code(%d).String() returned %q, expected a known name", tt.code, s)
		}
	}
}

func TestCodeString_UnknownCode(t *testing.T) {
	s := Code(99999).String()
	if s != "Unknown error code" {
		t.Fatalf("Code(99999).String() = %q, want %q", s, "Unknown error code")
	}
}

func TestErrorsIs_CrossType(t *testing.T) {
	if errors.Is(ErrInvalidCommand, ErrInvalidFormat) {
		t.Fatalf("errors.Is(ErrInvalidCommand, ErrInvalidFormat) = true, want false")
	}
	if errors.Is(ErrUnauthenticated, ErrUnauthorized) {
		t.Fatalf("errors.Is(ErrUnauthenticated, ErrUnauthorized) = true, want false")
	}
	if errors.Is(ErrStreamIdNotFound, ErrTopicIdNotFound) {
		t.Fatalf("errors.Is(ErrStreamIdNotFound, ErrTopicIdNotFound) = true, want false")
	}
}

func TestIs_DirectCall_AllTypes(t *testing.T) {
	type isChecker interface {
		Is(error) bool
	}

	for _, tt := range allErrors {
		t.Run(tt.name+"/self_match", func(t *testing.T) {
			checker, ok := tt.err.(isChecker)
			if !ok {
				t.Fatalf("%s does not implement Is(error) bool", tt.name)
			}
			if !checker.Is(tt.err) {
				t.Fatalf("%s.Is(self) = false, want true", tt.name)
			}
		})
		t.Run(tt.name+"/cross_type_reject", func(t *testing.T) {
			checker, ok := tt.err.(isChecker)
			if !ok {
				t.Fatalf("%s does not implement Is(error) bool", tt.name)
			}
			other := fmt.Errorf("unrelated error")
			if checker.Is(other) {
				t.Fatalf("%s.Is(unrelated) = true, want false", tt.name)
			}
		})
	}
}

func TestWrappedErrorsIs(t *testing.T) {
	wrapped := fmt.Errorf("context: %w", ErrInvalidCommand)
	if !errors.Is(wrapped, ErrInvalidCommand) {
		t.Fatalf("errors.Is(wrapped, ErrInvalidCommand) = false, want true")
	}
	if errors.Is(wrapped, ErrInvalidFormat) {
		t.Fatalf("errors.Is(wrapped, ErrInvalidFormat) = true for wrong type")
	}
}
