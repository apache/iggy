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

package vsr

import (
	"encoding/binary"
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replyHeader builds a reply header for a body of the given length.
func replyHeader(operation Operation, status uint32, bodyLength int) *[HeaderSize]byte {
	var header [HeaderSize]byte
	binary.LittleEndian.PutUint32(header[replyOffsetSize:], uint32(HeaderSize+bodyLength))
	header[replyOffsetCommand] = byte(FrameReply)
	header[replyOffsetOperation] = byte(operation)
	binary.LittleEndian.PutUint32(header[replyOffsetStatus:], status)
	return &header
}

// evictionHeader builds a header-only eviction frame.
func evictionHeader(reason EvictionReason, version, versionMin uint32) *[HeaderSize]byte {
	var header [HeaderSize]byte
	binary.LittleEndian.PutUint32(header[replyOffsetSize:], HeaderSize)
	header[evictionOffsetCommand] = byte(FrameEviction)
	binary.LittleEndian.PutUint32(header[evictionOffsetServerProtocolVersion:], version)
	binary.LittleEndian.PutUint32(header[evictionOffsetServerProtocolVersionMin:], versionMin)
	header[evictionOffsetReason] = byte(reason)
	return &header
}

// resultSection builds [count u32][{index u32, result u32} x count].
func resultSection(codes ...uint32) []byte {
	section := binary.LittleEndian.AppendUint32(nil, uint32(len(codes)))
	for index, code := range codes {
		section = binary.LittleEndian.AppendUint32(section, uint32(index))
		section = binary.LittleEndian.AppendUint32(section, code)
	}
	return section
}

// answeredRequestID is the request id every exchange stamps and echoes.
const answeredRequestID = 42

// exchange builds a stamped request and an uncommitted reply that answers it
// with a body of the given length.
func exchange(operation Operation, status uint32, bodyLength int) (*[HeaderSize]byte, *[HeaderSize]byte) {
	var request [HeaderSize]byte
	EncodeRequestHeader(&request, RequestFields{Size: HeaderSize, Client: ClientID{Lo: 7, Hi: 9}, Request: answeredRequestID, Operation: operation, Session: 3})
	reply := replyHeader(operation, status, bodyLength)
	binary.LittleEndian.PutUint64(reply[replyOffsetRequest:], answeredRequestID)
	return &request, reply
}

func TestDecodeReplyFor_ReturnsAnUnframedBodyVerbatim(t *testing.T) {
	body := []byte{1, 2, 3, 4}
	request, reply := exchange(OperationNonReplicated, 0, len(body))

	outcome, err := DecodeReplyFor(request, reply, body)
	require.NoError(t, err)
	require.NoError(t, outcome.Err)
	assert.Equal(t, body, outcome.Body)
}

func TestDecodeReplyFor_RejectsABodyLongerThanDeclared(t *testing.T) {
	request, reply := exchange(OperationNonReplicated, 0, 4)

	_, err := DecodeReplyFor(request, reply, []byte{1, 2, 3, 4, 5, 6})
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReplyFor_RejectsABodyShorterThanDeclared(t *testing.T) {
	request, reply := exchange(OperationNonReplicated, 0, 8)

	_, err := DecodeReplyFor(request, reply, []byte{1, 2, 3})
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReplyFor_StripsTheResultSectionOnSuccess(t *testing.T) {
	payload := []byte{9, 9}
	body := append(resultSection(), payload...)
	request, reply := exchange(OperationCreateStream, 0, len(body))

	outcome, err := DecodeReplyFor(request, reply, body)
	require.NoError(t, err)
	require.NoError(t, outcome.Err)
	assert.Equal(t, payload, outcome.Body)
}

func TestDecodeReplyFor_SurfacesACommittedRejection(t *testing.T) {
	body := resultSection(uint32(ierror.StreamIdNotFoundCode))
	request, reply := exchange(OperationCreateStream, 0, len(body))

	outcome, err := DecodeReplyFor(request, reply, body)
	require.NoError(t, err)
	assert.ErrorIs(t, outcome.Err, ierror.FromCode(ierror.StreamIdNotFoundCode))
	assert.Empty(t, outcome.Body)
}

func TestDecodeReplyFor_ChecksTheStatusBeforeTheBody(t *testing.T) {
	// A denial ships an empty body, so a body-first decode would misread it.
	request, reply := exchange(OperationCreateStream, uint32(ierror.UnauthorizedCode), 0)

	outcome, err := DecodeReplyFor(request, reply, nil)
	require.NoError(t, err)
	assert.ErrorIs(t, outcome.Err, ierror.ErrUnauthorized)
	assert.False(t, outcome.Committed)
}

func TestDecodeReplyFor_RejectsAnEmptySendMessagesReply(t *testing.T) {
	request, reply := exchange(OperationSendMessages, 0, 0)

	_, err := DecodeReplyFor(request, reply, nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReplyFor_RejectsAReplyToAnotherRequest(t *testing.T) {
	request, reply := exchange(OperationNonReplicated, 0, 0)
	binary.LittleEndian.PutUint64(reply[replyOffsetRequest:], answeredRequestID+1)

	_, err := DecodeReplyFor(request, reply, nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReplyFor_RejectsAnUnknownFrameDiscriminant(t *testing.T) {
	request, reply := exchange(OperationNonReplicated, 0, 0)
	reply[replyOffsetCommand] = byte(FrameRequest)

	_, err := DecodeReplyFor(request, reply, nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReplyFor_RejectsASizeBelowTheHeader(t *testing.T) {
	request, reply := exchange(OperationNonReplicated, 0, 0)
	binary.LittleEndian.PutUint32(reply[replyOffsetSize:], HeaderSize-1)

	_, err := DecodeReplyFor(request, reply, nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReplyFor_RejectsAFrameAboveTheTransportLimit(t *testing.T) {
	// The body matches the declared size, so only the transport limit rejects it.
	body := make([]byte, MaxFrameSize+1-HeaderSize)
	request, reply := exchange(OperationNonReplicated, 0, len(body))

	_, err := DecodeReplyFor(request, reply, body)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReplyFor_RejectsAnUndeclaredReplyOperation(t *testing.T) {
	for _, operation := range []Operation{4, 63, 129, 133, 142, 151, 163, 255} {
		request, reply := exchange(OperationNonReplicated, 0, 0)
		reply[replyOffsetOperation] = byte(operation)

		_, err := DecodeReplyFor(request, reply, nil)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand, "operation %d", operation)
	}
}

func TestDecodeReplyFor_SurfacesAnEviction(t *testing.T) {
	request, _ := exchange(OperationNonReplicated, 0, 0)

	_, err := DecodeReplyFor(request, evictionHeader(EvictionInvalidCredentials, 0, 0), nil)

	var eviction *EvictionError
	require.ErrorAs(t, err, &eviction)
	assert.Equal(t, EvictionInvalidCredentials, eviction.Reason)
	assert.ErrorIs(t, err, ierror.ErrInvalidCredentials)
}

func TestSplitMetadataResult_PassesUnframedOperationsThrough(t *testing.T) {
	body := []byte{1, 2, 3}
	for _, operation := range []Operation{
		OperationNonReplicated,
		OperationLogout,
		OperationSendMessages,
		OperationDeleteSegments,
	} {
		got, err := SplitMetadataResult(operation, body)
		require.NoError(t, err, "operation %d", operation)
		assert.Equal(t, body, got, "operation %d", operation)
	}
}

func TestSplitMetadataResult_PassesAnEmptyRegisterBodyThrough(t *testing.T) {
	got, err := SplitMetadataResult(OperationRegister, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestSplitMetadataResult_StripsANonEmptyRegisterBody(t *testing.T) {
	payload := []byte{7, 7, 7}
	body := append(resultSection(), payload...)

	got, err := SplitMetadataResult(OperationRegister, body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestSplitMetadataResult_StripsMultipleEntriesWhenTheFirstSucceeded(t *testing.T) {
	payload := []byte{5}
	body := append(resultSection(0, 0, 0), payload...)

	got, err := SplitMetadataResult(OperationCreateTopic, body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestSplitMetadataResult_RejectsATruncatedSection(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "empty", body: []byte{}},
		{name: "short count", body: []byte{1, 0, 0}},
		{name: "missing entry", body: resultSection(0)[:8]},
		{name: "count past the body", body: binary.LittleEndian.AppendUint32(nil, 1024)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := SplitMetadataResult(OperationCreateStream, test.body)
			assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
		})
	}
}

func TestSplitMetadataResult_DoesNotOverflowOnAHugeCount(t *testing.T) {
	body := binary.LittleEndian.AppendUint32(nil, 0xFFFFFFFF)

	_, err := SplitMetadataResult(OperationCreateStream, body)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestNewEvictionError_MapsEveryReason(t *testing.T) {
	tests := []struct {
		reason EvictionReason
		want   ierror.Code
	}{
		{reason: EvictionReserved, want: ierror.InvalidCommandCode},
		{reason: EvictionNoSession, want: ierror.UnauthenticatedCode},
		{reason: EvictionClientReleaseTooLow, want: ierror.InvalidCommandCode},
		{reason: EvictionClientReleaseTooHigh, want: ierror.InvalidCommandCode},
		{reason: EvictionInvalidRequestOperation, want: ierror.InvalidCommandCode},
		{reason: EvictionInvalidRequestBody, want: ierror.InvalidCommandCode},
		{reason: EvictionInvalidRequestBodySize, want: ierror.InvalidCommandCode},
		{reason: EvictionSessionTooLow, want: ierror.UnauthenticatedCode},
		{reason: EvictionSessionReleaseMismatch, want: ierror.UnauthenticatedCode},
		{reason: EvictionInvalidCredentials, want: ierror.InvalidCredentialsCode},
		{reason: EvictionInvalidToken, want: ierror.InvalidPersonalAccessTokenCode},
		{reason: EvictionUserInactive, want: ierror.UnauthenticatedCode},
		{reason: EvictionSessionError, want: ierror.UnauthenticatedCode},
		{reason: EvictionStaleClient, want: ierror.StaleClientCode},
		{reason: EvictionMalformedLogin, want: ierror.InvalidFormatCode},
		// An unrecognized reason maps like the core/common fallback and must
		// not become a reconnectable error.
		{reason: EvictionReason(200), want: ierror.InvalidCommandCode},
	}
	for _, test := range tests {
		got := NewEvictionError(Eviction{Reason: test.reason})
		assert.Equal(t, test.want, got.Code(), "reason %d", test.reason)
	}
}

func TestNewEvictionError_MapsAUsableProtocolWindow(t *testing.T) {
	got := NewEvictionError(Eviction{
		Reason:                   EvictionIncompatibleProtocol,
		ServerProtocolVersion:    12288,
		ServerProtocolVersionMin: 11264,
	})

	assert.Equal(t, ierror.IncompatibleProtocolVersionCode, got.Code())
	assert.Equal(t, uint32(12288), got.ServerProtocolVersion)
	assert.Equal(t, uint32(11264), got.ServerProtocolVersionMin)

	var mapped ierror.IncompatibleProtocolVersion
	require.True(t, errors.As(got.Unwrap(), &mapped))
	assert.Equal(t, ProtocolVersion, mapped.ClientVersion)
	assert.Equal(t, uint32(11264), mapped.ServerVersionMin)
	assert.Equal(t, uint32(12288), mapped.ServerVersionMax)
}

func TestNewEvictionError_DegradesAnUnusableProtocolWindow(t *testing.T) {
	tests := []struct {
		name       string
		version    uint32
		versionMin uint32
	}{
		{name: "zero minimum", version: 12288, versionMin: 0},
		{name: "inverted window", version: 11000, versionMin: 11264},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := NewEvictionError(Eviction{
				Reason:                   EvictionIncompatibleProtocol,
				ServerProtocolVersion:    test.version,
				ServerProtocolVersionMin: test.versionMin,
			})
			assert.Equal(t, ierror.UnauthenticatedCode, got.Code())
		})
	}
}

func TestEvictionError_ReportsTheReasonAndMappedError(t *testing.T) {
	got := NewEvictionError(Eviction{Reason: EvictionStaleClient})

	assert.Contains(t, got.Error(), "13")
	assert.Contains(t, got.Error(), ierror.ErrStaleClient.Error())
	assert.ErrorIs(t, got, ierror.ErrStaleClient)
}

func TestDecodeReplyForRequiresCompleteFinalResultProof(t *testing.T) {
	var request [HeaderSize]byte
	EncodeRequestHeader(&request, RequestFields{Size: HeaderSize, Client: ClientID{Lo: 7, Hi: 9}, Request: 42, Operation: OperationCreateTopic, Session: 3})
	request[requestOffsetChecksum] = 11
	StampPartitionContext(&request, iggcon.PartitionContext{Incarnation: 8})
	body := resultSection(uint32(ierror.UnauthorizedCode))
	header := replyHeader(OperationCreateTopicWithAssignments, 0, len(body))
	header[replyOffsetResult] = committedResult
	copy(header[replyOffsetClient:replyOffsetClient+16], request[requestOffsetClient:requestOffsetClient+16])
	copy(header[replyOffsetChecksum:replyOffsetChecksum+16], request[requestOffsetChecksum:requestOffsetChecksum+16])
	binary.LittleEndian.PutUint64(header[replyOffsetRequest:], 42)
	binary.LittleEndian.PutUint64(header[replyOffsetOp:], 17)
	binary.LittleEndian.PutUint64(header[replyOffsetCommit:], 17)
	binary.LittleEndian.PutUint64(header[replyOffsetIncarnation:], 8)
	outcome, err := DecodeReplyFor(&request, header, body)
	require.NoError(t, err)
	require.True(t, outcome.Committed)
	require.ErrorIs(t, outcome.Err, ierror.ErrUnauthorized)
	for name, offset := range map[string]int{"unknown marker": replyOffsetResult, "client": replyOffsetClient, "checksum": replyOffsetChecksum, "incarnation": replyOffsetIncarnation, "operation": replyOffsetOperation, "commit": replyOffsetCommit} {
		t.Run(name, func(t *testing.T) {
			corrupt := *header
			corrupt[offset] ^= 2
			got, err := DecodeReplyFor(&request, &corrupt, body)
			if name == "unknown marker" {
				require.NoError(t, err)
				require.False(t, got.Committed)
			} else {
				require.ErrorIs(t, err, ierror.ErrInvalidCommand)
			}
		})
	}
	for _, code := range []ierror.Code{ierror.TransientNotCommittedCode, ierror.TransientNotAcceptedCode, ierror.NotPrimaryCode} {
		outcome, err = DecodeReplyFor(&request, header, resultSection(uint32(code)))
		require.NoError(t, err)
		require.False(t, outcome.Committed)
	}
	truncated := append([]byte(nil), body...)
	binary.LittleEndian.PutUint32(truncated, ^uint32(0))
	_, err = DecodeReplyFor(&request, header, truncated)
	require.ErrorIs(t, err, ierror.ErrInvalidCommand)
	denied := *header
	binary.LittleEndian.PutUint32(denied[replyOffsetStatus:], uint32(ierror.UnauthorizedCode))
	outcome, err = DecodeReplyFor(&request, &denied, body)
	require.NoError(t, err)
	require.False(t, outcome.Committed)
}

func TestDecodeReplyFor_ResolvedUpdate(t *testing.T) {
	t.Run("given_resolved_update_reply_when_correlating_should_accept_only_matching_request_identity", func(t *testing.T) {
		var request [HeaderSize]byte
		EncodeRequestHeader(&request, RequestFields{Size: HeaderSize, Client: ClientID{Lo: 7, Hi: 9}, Request: 42, Operation: OperationUpdateResource, Session: 3})
		request[requestOffsetChecksum] = 11
		body := resultSection()
		for _, resolved := range []Operation{OperationUpdateStreamResolved, OperationUpdateTopicResolved, OperationUpdateUserResolved} {
			header := replyHeader(resolved, 0, len(body))
			header[replyOffsetResult] = committedResult
			copy(header[replyOffsetClient:replyOffsetClient+16], request[requestOffsetClient:requestOffsetClient+16])
			copy(header[replyOffsetChecksum:replyOffsetChecksum+16], request[requestOffsetChecksum:requestOffsetChecksum+16])
			binary.LittleEndian.PutUint64(header[replyOffsetRequest:], 42)
			binary.LittleEndian.PutUint64(header[replyOffsetOp:], 17)
			binary.LittleEndian.PutUint64(header[replyOffsetCommit:], 17)

			outcome, err := DecodeReplyFor(&request, header, body)
			require.NoError(t, err, "operation %d", resolved)
			assert.True(t, outcome.Committed, "operation %d", resolved)
			assert.NoError(t, outcome.Err, "operation %d", resolved)

			for name, mismatch := range map[string]func(*[HeaderSize]byte){
				"client":    func(reply *[HeaderSize]byte) { reply[replyOffsetClient] ^= 1 },
				"checksum":  func(reply *[HeaderSize]byte) { reply[replyOffsetChecksum] ^= 1 },
				"operation": func(reply *[HeaderSize]byte) { reply[replyOffsetOperation] = byte(OperationCreateTopicWithAssignments) },
				"rewrap":    func(reply *[HeaderSize]byte) { reply[replyOffsetOperation] = byte(OperationRewrapStreamKeys) },
			} {
				unrelated := *header
				mismatch(&unrelated)
				_, err := DecodeReplyFor(&request, &unrelated, body)
				assert.ErrorIs(t, err, ierror.ErrInvalidCommand, "operation %d, mismatched %s", resolved, name)
			}

			assert.False(t, ReplyOperationMatches(resolved, OperationUpdateResource), "the rewrite is directed")
			assert.False(t, ReplyOperationMatches(OperationCreateStream, resolved))
		}
		assert.False(t, ReplyOperationMatches(OperationUpdateResource, OperationRewrapStreamKeys))
	})
}

func TestReplyOperationMatchesOnlyCanonicalDirectedRewrites(t *testing.T) {
	for _, pair := range [][2]Operation{{OperationCreateTopic, OperationCreateTopicWithAssignments}, {OperationCreatePartitions, OperationCreatePartitionsWithAssignments}, {OperationDeleteSegments, OperationTruncatePartition}} {
		require.True(t, ReplyOperationMatches(pair[0], pair[1]))
		require.False(t, ReplyOperationMatches(pair[1], pair[0]))
	}
	require.False(t, ReplyOperationMatches(OperationCreateTopic, OperationCreatePartitionsWithAssignments))
}
