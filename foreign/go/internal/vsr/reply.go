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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

// ReplyOutcome separates a decoded business rejection from malformed wire data.
type ReplyOutcome struct {
	Body      []byte
	Err       error
	Committed bool
}

// DecodeReplyFor verifies the final-result proof against the frozen request.
func DecodeReplyFor(request, header *[HeaderSize]byte, body []byte) (ReplyOutcome, error) {
	if PeekCommand(header) == FrameEviction {
		return ReplyOutcome{}, NewEvictionError(ReadEviction(header))
	}
	if PeekCommand(header) != FrameReply || ReadSize(header) < HeaderSize ||
		ReadSize(header) > MaxFrameSize || int(ReadSize(header))-HeaderSize != len(body) ||
		ReadReplyRequestID(header) != StampedRequestID(request[:]) {
		return ReplyOutcome{}, ierror.ErrInvalidCommand
	}
	if status := ReadStatus(header); status != 0 {
		return ReplyOutcome{Err: ierror.FromCode(ierror.Code(status))}, nil
	}
	operation := ReadReplyOperation(header)
	// SendMessages is not result-framed, so an empty reply would otherwise
	// reach the send path, which reports undecodable confirmations as a
	// committed send.
	if !IsKnownOperation(operation) || operation == OperationSendMessages && len(body) == 0 {
		return ReplyOutcome{}, ierror.ErrInvalidCommand
	}
	if IsResultFramed(operation) || operation == OperationRegister && len(body) > 0 {
		if len(body) < resultCountLength ||
			uint64(resultCountLength)+uint64(binary.LittleEndian.Uint32(body))*resultEntryLength > uint64(len(body)) {
			return ReplyOutcome{}, ierror.ErrInvalidCommand
		}
	}
	response, err := SplitMetadataResult(operation, body)
	op := binary.LittleEndian.Uint64(header[replyOffsetOp:])
	identity := op != 0 &&
		op == binary.LittleEndian.Uint64(header[replyOffsetCommit:]) &&
		bytes.Equal(header[replyOffsetClient:replyOffsetClient+16], request[requestOffsetClient:requestOffsetClient+16]) &&
		bytes.Equal(header[replyOffsetChecksum:replyOffsetChecksum+16], request[requestOffsetChecksum:requestOffsetChecksum+16]) &&
		bytes.Equal(header[replyOffsetIncarnation:replyOffsetIncarnation+8], request[requestOffsetContext:requestOffsetContext+8]) &&
		ReplyOperationMatches(Operation(request[requestOffsetOperation]), operation)
	if header[replyOffsetResult] == committedResult && !identity {
		return ReplyOutcome{}, ierror.ErrInvalidCommand
	}
	committed := header[replyOffsetResult] == committedResult && identity && !errors.Is(err, ierror.ErrTransientNotCommitted) && !errors.Is(err, ierror.ErrTransientNotAccepted) &&
		!errors.Is(err, ierror.ErrNotPrimary)
	return ReplyOutcome{Body: response, Err: err, Committed: committed}, nil
}

// ReplyOperationMatches includes only the primary's canonical prepare rewrites.
func ReplyOperationMatches(request, reply Operation) bool {
	return request == reply ||
		request == OperationCreateTopic && reply == OperationCreateTopicWithAssignments ||
		request == OperationCreatePartitions && reply == OperationCreatePartitionsWithAssignments ||
		request == OperationDeleteSegments && reply == OperationTruncatePartition ||
		request == OperationUpdateResource && (reply == OperationUpdateStreamResolved ||
			reply == OperationUpdateTopicResolved || reply == OperationUpdateUserResolved)
}

// Result section layout: [count u32][{index u32, result u32} x count].
const (
	resultCountLength = 4
	resultEntryLength = 8
)

// EvictionError is an eviction frame. The mapped IggyError is
// what callers match on. The protocol window is kept for diagnostics on a
// version mismatch.
type EvictionError struct {
	Reason                   EvictionReason
	ServerProtocolVersion    uint32
	ServerProtocolVersionMin uint32
	mapped                   ierror.IggyError
}

func (e *EvictionError) Error() string {
	return fmt.Sprintf("vsr: evicted with reason %d: %s", e.Reason, e.mapped.Error())
}

// Unwrap returns the IggyError the reason maps to, so errors.Is matches the
// mapped sentinel.
func (e *EvictionError) Unwrap() error {
	return e.mapped
}

// Code returns the IggyError code the reason maps to.
func (e *EvictionError) Code() ierror.Code {
	return e.mapped.Code()
}

// SplitMetadataResult strips the committed result section leading a
// result-framed reply body. Success carries count 0 then the typed payload; a
// committed business rejection carries one entry and no payload, with the
// header status still 0. A register reply is result-framed too, except a
// terminal register failure ships an empty body, which passes through so the
// typed decode fails.
func SplitMetadataResult(operation Operation, body []byte) ([]byte, error) {
	framed := IsResultFramed(operation) ||
		(operation == OperationRegister && len(body) > 0)
	if !framed {
		return body, nil
	}

	if len(body) < resultCountLength {
		return nil, ierror.ErrInvalidCommand
	}
	count := uint64(binary.LittleEndian.Uint32(body))
	sectionLength := resultCountLength + count*resultEntryLength
	if uint64(len(body)) < sectionLength {
		return nil, ierror.ErrInvalidCommand
	}
	if count > 0 {
		// First entry is {index u32, result u32}. The result shares the
		// IggyError code space.
		if code := binary.LittleEndian.Uint32(body[resultCountLength+4:]); code != 0 {
			return nil, ierror.FromCode(ierror.Code(code))
		}
	}
	return body[sectionLength:], nil
}

// NewEvictionError maps an eviction frame to a typed error, the same mapping
// as eviction_reason_to_error in core/common. An unusable protocol window
// degrades to an authentication error rather than trusting the remote frame.
func NewEvictionError(eviction Eviction) *EvictionError {
	var mapped ierror.IggyError
	switch eviction.Reason {
	case EvictionInvalidCredentials:
		mapped = ierror.ErrInvalidCredentials
	case EvictionInvalidToken:
		mapped = ierror.ErrInvalidPersonalAccessToken
	case EvictionNoSession, EvictionSessionTooLow, EvictionSessionReleaseMismatch,
		EvictionUserInactive, EvictionSessionError:
		mapped = ierror.ErrUnauthenticated
	case EvictionStaleClient:
		mapped = ierror.ErrStaleClient
	case EvictionIncompatibleProtocol:
		if eviction.ServerProtocolVersionMin == 0 ||
			eviction.ServerProtocolVersion < eviction.ServerProtocolVersionMin {
			mapped = ierror.ErrUnauthenticated
		} else {
			mapped = ierror.IncompatibleProtocolVersion{
				ClientVersion:    ProtocolVersion,
				ServerVersionMin: eviction.ServerProtocolVersionMin,
				ServerVersionMax: eviction.ServerProtocolVersion,
			}
		}
	case EvictionMalformedLogin:
		mapped = ierror.ErrInvalidFormat
	default:
		// Everything else, including a reason byte this SDK does not know,
		// maps to InvalidCommand like the core/common fallback. The unknown
		// byte must not map to a reconnectable error, or a newer server's new
		// reason silently drives a disconnect and re-login loop.
		mapped = ierror.ErrInvalidCommand
	}
	return &EvictionError{
		Reason:                   eviction.Reason,
		ServerProtocolVersion:    eviction.ServerProtocolVersion,
		ServerProtocolVersionMin: eviction.ServerProtocolVersionMin,
		mapped:                   mapped,
	}
}
