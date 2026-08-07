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

import ierror "github.com/apache/iggy/foreign/go/errors"

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
// of frame, deriving routing and sequencing from the payload that follows it.
// Callers that encode a payload into a pooled buffer reserve the prologue up
// front and stamp it here, which keeps the frame a single allocation.
//
// frame must be at least HeaderSize bytes long.
func StampRequestHeader(session *Session, code uint32, frame []byte) error {
	if len(frame) > MaxFrameSize {
		return ierror.ErrInvalidConfiguration
	}
	payload := frame[HeaderSize:]
	operation := OperationForCode(code)
	replicated := operation != OperationRegister && operation != OperationNonReplicated

	// A replicated operation needs a bound session. Checking that ahead of the
	// payload means an unauthenticated caller hears about the missing session
	// rather than about the request it could not have sent anyway.
	if replicated && !session.Bound() {
		return ierror.ErrUnauthenticated
	}

	// Namespace derivation can fail on a malformed payload. Run it before
	// taking a request id so a local failure never burns one, which would gap
	// the sequence and make the primary drop the next metadata request.
	namespace, err := NamespaceForRequest(code, payload, operation)
	if err != nil {
		return err
	}

	var request, sessionID uint64
	switch operation {
	case OperationRegister:
		request = session.BeginRegister()
	case OperationNonReplicated:
		// Non-replicated operations bypass server-side dedup and are routed by
		// transport identity, so they travel sessionless before Register.
		request = session.CurrentRequestID()
		sessionID = session.SessionID()
	default:
		sessionID = session.SessionID()
		if IsPartition(operation) {
			// Partition operations replicate in their own per-partition group
			// with no client-table dedup, so they read the watermark without
			// consuming it.
			request = session.CurrentRequestID()
		} else if request, err = session.NextRequestID(); err != nil {
			return err
		}
	}

	fields := RequestFields{
		Size:      uint32(len(frame)),
		Client:    session.ClientID(),
		Request:   request,
		Operation: operation,
		Namespace: namespace,
		Session:   sessionID,
	}
	if operation == OperationNonReplicated {
		fields.NonReplicatedCode = code
	}
	EncodeRequestHeader((*[HeaderSize]byte)(frame[:HeaderSize]), fields)
	return nil
}
