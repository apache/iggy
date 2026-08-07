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

func TestDecodeReply_ReturnsAnUnframedBodyVerbatim(t *testing.T) {
	body := []byte{1, 2, 3, 4}

	got, err := DecodeReply(replyHeader(OperationNonReplicated, 0, len(body)), body)
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

func TestDecodeReply_TrimsTheBodyToTheDeclaredSize(t *testing.T) {
	body := []byte{1, 2, 3, 4, 5, 6}

	got, err := DecodeReply(replyHeader(OperationNonReplicated, 0, 4), body)
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3, 4}, got)
}

func TestDecodeReply_StripsTheResultSectionOnSuccess(t *testing.T) {
	payload := []byte{9, 9}
	body := append(resultSection(), payload...)

	got, err := DecodeReply(replyHeader(OperationCreateStream, 0, len(body)), body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestDecodeReply_SurfacesACommittedRejection(t *testing.T) {
	body := resultSection(uint32(ierror.StreamIdNotFoundCode))

	_, err := DecodeReply(replyHeader(OperationCreateStream, 0, len(body)), body)
	assert.ErrorIs(t, err, ierror.FromCode(ierror.StreamIdNotFoundCode))
}

func TestDecodeReply_ChecksTheStatusBeforeTheBody(t *testing.T) {
	// A denial ships an empty body, so a body-first decode would misread it.
	header := replyHeader(OperationCreateStream, uint32(ierror.UnauthorizedCode), 0)

	_, err := DecodeReply(header, nil)
	assert.ErrorIs(t, err, ierror.ErrUnauthorized)
}

func TestDecodeReply_RejectsAnUnknownFrameDiscriminant(t *testing.T) {
	header := replyHeader(OperationNonReplicated, 0, 0)
	header[replyOffsetCommand] = byte(FrameRequest)

	_, err := DecodeReply(header, nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReply_RejectsASizeBelowTheHeader(t *testing.T) {
	header := replyHeader(OperationNonReplicated, 0, 0)
	binary.LittleEndian.PutUint32(header[replyOffsetSize:], HeaderSize-1)

	_, err := DecodeReply(header, nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReply_RejectsAFrameAboveTheTransportLimit(t *testing.T) {
	header := replyHeader(OperationNonReplicated, 0, 0)
	binary.LittleEndian.PutUint32(header[replyOffsetSize:], MaxFrameSize+1)

	_, err := DecodeReply(header, nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReply_RejectsABodyShorterThanDeclared(t *testing.T) {
	header := replyHeader(OperationNonReplicated, 0, 8)

	_, err := DecodeReply(header, []byte{1, 2, 3})
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestDecodeReply_RejectsAnUndeclaredReplyOperation(t *testing.T) {
	for _, operation := range []Operation{4, 63, 150, 163, 255} {
		header := replyHeader(operation, 0, 0)
		_, err := DecodeReply(header, nil)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand, "operation %d", operation)
	}
}

func TestDecodeReply_SurfacesAnEviction(t *testing.T) {
	header := evictionHeader(EvictionInvalidCredentials, 0, 0)

	_, err := DecodeReply(header, nil)

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
		{reason: EvictionReason(200), want: ierror.UnauthenticatedCode},
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
