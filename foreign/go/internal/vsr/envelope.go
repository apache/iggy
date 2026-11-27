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
	"crypto/rand"
	"encoding/binary"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
)

// EncodeRequest returns the complete request frame for a command code and its
// already-encoded payload.
func EncodeRequest(session *Session, code uint32, payload []byte) ([]byte, error) {
	frame := make([]byte, HeaderSize+len(payload))
	copy(frame[HeaderSize:], payload)
	if err := StampRequestHeader(session, code, frame); err != nil {
		return nil, err
	}
	return frame, nil
}

// StampRequestHeader writes the request header into the first HeaderSize bytes
// of frame, deriving sequencing from the command code and the session state.
// Callers that encode a payload into a pooled buffer reserve the prologue up
// front and stamp it here, which keeps the frame a single allocation.
//
// frame must be at least HeaderSize bytes long.
func StampRequestHeader(session *Session, code uint32, frame []byte) error {
	if len(frame) < HeaderSize || len(frame) > MaxFrameSize {
		return ierror.ErrInvalidConfiguration
	}
	operation := OperationForCode(code)
	replicated := operation != OperationRegister && operation != OperationNonReplicated

	// A replicated operation needs a bound session; refusing it here leaves
	// the request-id counter untouched.
	if replicated && !session.Bound() {
		return ierror.ErrUnauthenticated
	}

	var request, sessionID uint64
	var err error
	switch operation {
	case OperationRegister:
		request = session.BeginRegister()
	case OperationNonReplicated:
		request, err = session.NextReadRequestID()
		if err != nil {
			return err
		}
		sessionID = session.SessionID()
	default:
		sessionID = session.SessionID()
		// Partition operations consume an id too: dedup needs each send to
		// carry a distinct number, and the metadata watermark tolerates the
		// gaps.
		if request, err = session.NextRequestID(); err != nil {
			return err
		}
	}

	fields := RequestFields{
		Size:      uint32(len(frame)),
		Client:    session.ClientID(),
		Request:   request,
		Operation: operation,
		Session:   sessionID,
	}
	if operation == OperationNonReplicated {
		fields.NonReplicatedCode = code
	}
	EncodeRequestHeader((*[HeaderSize]byte)(frame[:HeaderSize]), fields)
	if operation != OperationNonReplicated && operation < OperationSendMessages {
		for {
			if _, err := rand.Read(frame[requestOffsetChecksum : requestOffsetChecksum+16]); err != nil {
				return err
			}
			if binary.LittleEndian.Uint64(frame[requestOffsetChecksum:]) != 0 ||
				binary.LittleEndian.Uint64(frame[requestOffsetChecksum+8:]) != 0 {
				break
			}
		}
	}
	return nil
}

// DeferredCancellation returns the frame that cancels the deferred poll or fetch in frame: a header without a body that names the
// same client, session and request. It must follow that request on the same
// connection. It returns false for any other request.
func DeferredCancellation(frame []byte) ([]byte, bool) {
	code := binary.LittleEndian.Uint32(frame[requestOffsetReserved:])
	if Operation(frame[requestOffsetOperation]) != OperationNonReplicated ||
		code != uint32(command.PollMessagesDeferredCode) &&
			code != uint32(command.FetchMessagesCode) {
		return nil, false
	}
	cancellation := make([]byte, HeaderSize)
	EncodeRequestHeader((*[HeaderSize]byte)(cancellation), RequestFields{
		Size: HeaderSize,
		Client: ClientID{
			Lo: binary.LittleEndian.Uint64(frame[requestOffsetClient:]),
			Hi: binary.LittleEndian.Uint64(frame[requestOffsetClient+8:]),
		},
		Request:           StampedRequestID(frame),
		Operation:         OperationNonReplicated,
		Session:           binary.LittleEndian.Uint64(frame[requestOffsetSession:]),
		NonReplicatedCode: uint32(command.CancelDeferredRequestCode),
	})
	return cancellation, true
}
