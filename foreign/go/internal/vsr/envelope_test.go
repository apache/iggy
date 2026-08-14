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
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// boundSession returns a session already registered and bound to fence 100.
func boundSession(t *testing.T) *Session {
	t.Helper()
	session := NewSessionWithClientID(ClientID{Lo: 0xAA, Hi: 0xBB})
	session.BeginRegister()
	require.NoError(t, session.Bind(100))
	return session
}

func frameHeader(t *testing.T, frame []byte) *[HeaderSize]byte {
	t.Helper()
	require.GreaterOrEqual(t, len(frame), HeaderSize)
	return (*[HeaderSize]byte)(frame[:HeaderSize])
}

func TestEncodeRequest_EncodesARegisterFrame(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 0xAA, Hi: 0xBB})
	payload := []byte{1, 2, 3}

	frame, err := EncodeRequest(session, uint32(command.LoginRegisterCode), payload)
	require.NoError(t, err)

	header := frameHeader(t, frame)
	assert.Len(t, frame, HeaderSize+len(payload))
	assert.Equal(t, payload, frame[HeaderSize:])
	assert.Equal(t, byte(OperationRegister), header[requestOffsetOperation])
	assert.Zero(t, binary.LittleEndian.Uint64(header[requestOffsetRequest:]))
	assert.Zero(t, binary.LittleEndian.Uint64(header[requestOffsetSession:]))
	assert.Zero(t, binary.LittleEndian.Uint32(header[requestOffsetReserved:]))
}

func TestEncodeRequest_EncodesTheTokenRegisterCode(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 1})

	frame, err := EncodeRequest(session, uint32(command.LoginRegisterWithPATCode), nil)
	require.NoError(t, err)

	assert.Equal(t, byte(OperationRegister), frameHeader(t, frame)[requestOffsetOperation])
}

func TestEncodeRequest_EncodesALogoutFrame(t *testing.T) {
	session := boundSession(t)

	frame, err := EncodeRequest(session, uint32(command.LogoutUserCode), nil)
	require.NoError(t, err)

	header := frameHeader(t, frame)
	assert.Equal(t, byte(OperationLogout), header[requestOffsetOperation])
	assert.Equal(t, uint64(1), binary.LittleEndian.Uint64(header[requestOffsetRequest:]),
		"logout advances the metadata watermark")
	assert.Equal(t, uint64(2), session.CurrentRequestID())
}

func TestEncodeRequest_CarriesTheCodeOfANonReplicatedCommand(t *testing.T) {
	session := boundSession(t)

	frame, err := EncodeRequest(session, uint32(command.PingCode), nil)
	require.NoError(t, err)

	header := frameHeader(t, frame)
	assert.Equal(t, byte(OperationNonReplicated), header[requestOffsetOperation])
	assert.Equal(t, uint32(command.PingCode),
		binary.LittleEndian.Uint32(header[requestOffsetReserved:]))
	assert.Equal(t, uint64(100), binary.LittleEndian.Uint64(header[requestOffsetSession:]))
}

func TestEncodeRequest_CarriesAVendorCode(t *testing.T) {
	session := boundSession(t)

	frame, err := EncodeRequest(session, 60000, []byte{7})
	require.NoError(t, err)

	header := frameHeader(t, frame)
	assert.Equal(t, byte(OperationNonReplicated), header[requestOffsetOperation])
	assert.Equal(t, uint32(60000), binary.LittleEndian.Uint32(header[requestOffsetReserved:]))
}

func TestEncodeRequest_SendsNonReplicatedCommandsBeforeRegister(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 1})

	frame, err := EncodeRequest(session, uint32(command.PingCode), nil)
	require.NoError(t, err)

	header := frameHeader(t, frame)
	assert.Zero(t, binary.LittleEndian.Uint64(header[requestOffsetSession:]))
	assert.Equal(t, uint64(1), binary.LittleEndian.Uint64(header[requestOffsetRequest:]))
}

func TestEncodeRequest_DoesNotAdvanceTheWatermarkOffTheMetadataPlane(t *testing.T) {
	session := boundSession(t)

	for range 3 {
		_, err := EncodeRequest(session, uint32(command.PingCode), nil)
		require.NoError(t, err)
	}
	assert.Equal(t, uint64(1), session.CurrentRequestID())

	for range 3 {
		_, err := EncodeRequest(session, uint32(command.SendMessagesCode), []byte{1})
		require.NoError(t, err)
	}
	assert.Equal(t, uint64(1), session.CurrentRequestID())
}

func TestEncodeRequest_AdvancesTheWatermarkPerMetadataCommand(t *testing.T) {
	session := boundSession(t)

	for expected := uint64(1); expected <= 3; expected++ {
		frame, err := EncodeRequest(session, uint32(command.CreateStreamCode), []byte{1})
		require.NoError(t, err)
		header := frameHeader(t, frame)
		assert.Equal(t, expected, binary.LittleEndian.Uint64(header[requestOffsetRequest:]))
		assert.Equal(t, byte(OperationCreateStream), header[requestOffsetOperation])
	}
	assert.Equal(t, uint64(4), session.CurrentRequestID())
}

func TestEncodeRequest_RejectsAReplicatedCommandOnAnUnboundSession(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 1})

	_, err := EncodeRequest(session, uint32(command.CreateStreamCode), []byte{1})
	assert.ErrorIs(t, err, ierror.ErrUnauthenticated)
	assert.Equal(t, uint64(1), session.CurrentRequestID(), "the rejection burns no id")
}

func TestEncodeRequest_RejectsAPartitionCommandOnAnUnboundSession(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 1})

	_, err := EncodeRequest(session, uint32(command.SendMessagesCode), []byte{1})
	assert.ErrorIs(t, err, ierror.ErrUnauthenticated)
}

func TestEncodeRequest_EncodesASendMessagesFrame(t *testing.T) {
	session := boundSession(t)

	frame, err := EncodeRequest(session, uint32(command.SendMessagesCode), []byte{1})
	require.NoError(t, err)

	header := frameHeader(t, frame)
	assert.Equal(t, byte(OperationSendMessages), header[requestOffsetOperation])
	assert.Zero(t, binary.LittleEndian.Uint32(header[requestOffsetReserved:]),
		"only non-replicated frames carry the code")
}

func TestEncodeRequest_CarriesTheClientIDOfTheSession(t *testing.T) {
	session := boundSession(t)

	frame, err := EncodeRequest(session, uint32(command.PingCode), nil)
	require.NoError(t, err)

	header := frameHeader(t, frame)
	assert.Equal(t, uint64(0xAA), binary.LittleEndian.Uint64(header[requestOffsetClient:]))
	assert.Equal(t, uint64(0xBB), binary.LittleEndian.Uint64(header[requestOffsetClient+8:]))
}

func TestEncodeRequest_WritesTheFrameSize(t *testing.T) {
	session := boundSession(t)
	payload := make([]byte, 300)

	frame, err := EncodeRequest(session, uint32(command.PingCode), payload)
	require.NoError(t, err)

	assert.Equal(t, uint32(HeaderSize+300),
		binary.LittleEndian.Uint32(frameHeader(t, frame)[requestOffsetSize:]))
	assert.Len(t, frame, HeaderSize+300)
}

func TestEncodeRequest_EncodesAnEmptyPayloadAsABareHeader(t *testing.T) {
	session := boundSession(t)

	frame, err := EncodeRequest(session, uint32(command.PingCode), nil)
	require.NoError(t, err)

	assert.Len(t, frame, HeaderSize)
	assert.Equal(t, uint32(HeaderSize),
		binary.LittleEndian.Uint32(frameHeader(t, frame)[requestOffsetSize:]))
}

func TestStampRequestHeader_FillsAReservedPrologue(t *testing.T) {
	session := boundSession(t)
	payload := []byte{4, 5, 6}
	frame := append(make([]byte, HeaderSize), payload...)

	require.NoError(t, StampRequestHeader(session, uint32(command.PingCode), frame))

	assert.Equal(t, payload, frame[HeaderSize:], "the payload is left untouched")
	assert.Equal(t, byte(FrameRequest), frame[requestOffsetCommand])
	assert.Equal(t, uint32(command.PingCode),
		binary.LittleEndian.Uint32(frame[requestOffsetReserved:]))
}

func TestStampRequestHeader_ClearsAReusedPrologue(t *testing.T) {
	session := boundSession(t)
	frame := make([]byte, HeaderSize)
	for i := range frame {
		frame[i] = 0xFF
	}

	require.NoError(t, StampRequestHeader(session, uint32(command.CreateStreamCode), frame))

	assert.Zero(t, binary.LittleEndian.Uint32(frame[requestOffsetReserved:]))
	assert.Zero(t, binary.LittleEndian.Uint64(frame[requestOffsetTimestamp:]))
}
