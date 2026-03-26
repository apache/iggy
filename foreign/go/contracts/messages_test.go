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

package iggcon

import (
	"bytes"
	"errors"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestNewIggyMessage_Success(t *testing.T) {
	payload := []byte("hello")
	msg, err := NewIggyMessage(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(msg.Payload, payload) {
		t.Fatalf("expected payload %v, got %v", payload, msg.Payload)
	}
	if msg.Header.PayloadLength != uint32(len(payload)) {
		t.Fatalf("expected payload length %d, got %d", len(payload), msg.Header.PayloadLength)
	}
	if msg.Header.UserHeaderLength != 0 {
		t.Fatalf("expected user header length 0, got %d", msg.Header.UserHeaderLength)
	}
}

func TestNewIggyMessage_NilPayload(t *testing.T) {
	_, err := NewIggyMessage(nil)
	if !errors.Is(err, ierror.ErrInvalidMessagePayloadLength) {
		t.Fatalf("expected ErrInvalidMessagePayloadLength, got %v", err)
	}
}

func TestNewIggyMessage_EmptyPayload(t *testing.T) {
	_, err := NewIggyMessage([]byte{})
	if !errors.Is(err, ierror.ErrInvalidMessagePayloadLength) {
		t.Fatalf("expected ErrInvalidMessagePayloadLength, got %v", err)
	}
}

func TestNewIggyMessage_TooBigPayload(t *testing.T) {
	payload := make([]byte, MaxPayloadSize+1)
	_, err := NewIggyMessage(payload)
	if !errors.Is(err, ierror.ErrTooBigMessagePayload) {
		t.Fatalf("expected ErrTooBigMessagePayload, got %v", err)
	}
}

func TestWithID_SetsMessageID(t *testing.T) {
	var id [16]byte
	copy(id[:], []byte("0123456789abcdef"))

	payload := []byte("hello")
	msg, err := NewIggyMessage(payload, WithID(id))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(msg.Header.Id[:], id[:]) {
		t.Fatalf("expected id %v, got %v", id, msg.Header.Id)
	}
}
