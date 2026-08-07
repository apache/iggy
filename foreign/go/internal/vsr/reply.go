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
	"fmt"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

// Result section layout: [count u32][{index u32, result u32} x count].
const (
	resultCountLength = 4
	resultEntryLength = 8
)

// EvictionError is a session-terminal eviction frame. The mapped IggyError is
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

// DecodeReply turns one complete consensus frame into its reply body. The
// order is fixed: frame discriminant, declared size, pre-commit denial status
// before any body read, then the committed result section.
func DecodeReply(header *[HeaderSize]byte, body []byte) ([]byte, error) {
	switch PeekCommand(header) {
	case FrameEviction:
		return nil, NewEvictionError(ReadEviction(header))
	case FrameReply:
	default:
		return nil, ierror.ErrInvalidCommand
	}

	size := ReadSize(header)
	if size < HeaderSize || size > MaxFrameSize {
		return nil, ierror.ErrInvalidCommand
	}
	expected := int(size) - HeaderSize
	if len(body) < expected {
		return nil, ierror.ErrInvalidCommand
	}

	// A pre-commit denial always ships an empty body. The status channel and
	// the committed result section are mutually exclusive.
	if status := ReadStatus(header); status != 0 {
		return nil, ierror.FromCode(ierror.Code(status))
	}

	operation := ReadReplyOperation(header)
	if !IsKnownOperation(operation) {
		return nil, ierror.ErrInvalidCommand
	}
	if operation == OperationSendMessages && expected == 0 {
		// A committed send always carries a confirmation section. The server
		// answers a replicated request on a dead session with an empty
		// status-0 reply, expecting the decoder to reject it. Every other
		// replicated operation is result-framed and fails on the missing
		// section; SendMessages is not, so without this guard a send that
		// wrote nothing would read as a success.
		return nil, ierror.ErrInvalidCommand
	}
	return SplitMetadataResult(operation, body[:expected])
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
