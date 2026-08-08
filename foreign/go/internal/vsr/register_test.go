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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// registerReplyBody builds [user_id u32][session u64][protocol u32][version].
func registerReplyBody(userID uint32, session uint64, protocol uint32, version string) []byte {
	body := binary.LittleEndian.AppendUint32(nil, userID)
	body = binary.LittleEndian.AppendUint64(body, session)
	body = binary.LittleEndian.AppendUint32(body, protocol)
	body = append(body, byte(len(version)))
	return append(body, version...)
}

func TestProtocolVersion_PacksTheWireContractSemver(t *testing.T) {
	assert.Equal(t, uint32(11264), ProtocolVersion)
	assert.Equal(t, uint32(0), ProtocolVersion>>20)
	assert.Equal(t, uint32(11), (ProtocolVersion>>10)&0x3FF)
	assert.Equal(t, uint32(0), ProtocolVersion&0x3FF)
}

func TestSerializeLoginRegister_EncodesTheDeclaredLayout(t *testing.T) {
	body, err := SerializeLoginRegister("iggy", "secret", "0.9.0-edge.1")
	require.NoError(t, err)

	offset := 0
	assert.Equal(t, ProtocolVersion, binary.LittleEndian.Uint32(body[offset:]))
	offset += 4

	assert.Equal(t, byte(len(SDKName)), body[offset])
	assert.Equal(t, SDKName, string(body[offset+1:offset+1+len(SDKName)]))
	offset += 1 + len(SDKName)

	assert.Equal(t, byte(12), body[offset])
	assert.Equal(t, "0.9.0-edge.1", string(body[offset+1:offset+13]))
	offset += 13

	assert.Equal(t, byte(4), body[offset])
	assert.Equal(t, "iggy", string(body[offset+1:offset+5]))
	offset += 5

	assert.Equal(t, byte(6), body[offset])
	assert.Equal(t, "secret", string(body[offset+1:offset+7]))
	offset += 7

	assert.Equal(t, []byte{0, 0, 0, 0}, body[offset:], "empty context")
	assert.Len(t, body, offset+4)
}

func TestSerializeLoginRegister_AllowsAnEmptyPassword(t *testing.T) {
	body, err := SerializeLoginRegister("iggy", "", "1.0.0")
	require.NoError(t, err)
	assert.NotEmpty(t, body)
}

func TestSerializeLoginRegister_RejectsNamesPastTheLengthPrefix(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
		version  string
	}{
		{name: "empty username", username: "", password: "p", version: "1"},
		{name: "long username", username: strings.Repeat("u", 256), password: "p", version: "1"},
		{name: "long password", username: "u", password: strings.Repeat("p", 256), version: "1"},
		{name: "empty version", username: "u", password: "p", version: ""},
		{name: "long version", username: "u", password: "p", version: strings.Repeat("v", 256)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := SerializeLoginRegister(test.username, test.password, test.version)
			assert.ErrorIs(t, err, ErrWireNameLength)
		})
	}
}

func TestSerializeLoginRegister_AcceptsTheLengthPrefixBoundary(t *testing.T) {
	_, err := SerializeLoginRegister(strings.Repeat("u", 255), strings.Repeat("p", 255), "1")
	assert.NoError(t, err)
}

func TestSerializeLoginRegister_CountsUTF8Bytes(t *testing.T) {
	// The rune is two UTF-8 bytes, so 128 of them are 256 bytes and overflow
	// the u8 prefix even though the rune count fits.
	_, err := SerializeLoginRegister(strings.Repeat("ł", 128), "p", "1")
	assert.ErrorIs(t, err, ErrWireNameLength)

	body, err := SerializeLoginRegister("łł", "p", "1")
	require.NoError(t, err)
	assert.Contains(t, string(body), "łł")
}

func TestSerializeLoginRegisterWithToken_EncodesTheDeclaredLayout(t *testing.T) {
	body, err := SerializeLoginRegisterWithToken("token-value", "0.9.0-edge.1")
	require.NoError(t, err)

	offset := 4 + 1 + len(SDKName) + 1 + len("0.9.0-edge.1")
	assert.Equal(t, byte(11), body[offset])
	assert.Equal(t, "token-value", string(body[offset+1:offset+12]))
	assert.Equal(t, []byte{0, 0, 0, 0}, body[offset+12:])
}

func TestSerializeLoginRegisterWithToken_RejectsATokenPastTheLengthPrefix(t *testing.T) {
	_, err := SerializeLoginRegisterWithToken(strings.Repeat("t", 256), "1")
	assert.ErrorIs(t, err, ErrWireNameLength)
}

func TestDecodeLoginRegister_DecodesTheGoldenReply(t *testing.T) {
	body := registerReplyBody(7, 128, 11264, "0.11.0-edge.1")

	response, err := DecodeLoginRegister(body)
	require.NoError(t, err)
	assert.Equal(t, uint32(7), response.UserID)
	assert.Equal(t, uint64(128), response.Session)
	assert.Equal(t, uint32(11264), response.ServerProtocolVersion)
	assert.Equal(t, "0.11.0-edge.1", response.ServerVersion)
}

func TestDecodeLoginRegister_AcceptsAnEmptyServerVersion(t *testing.T) {
	response, err := DecodeLoginRegister(registerReplyBody(1, 2, 3, ""))
	require.NoError(t, err)
	assert.Empty(t, response.ServerVersion)
}

func TestDecodeLoginRegister_RejectsEveryTruncation(t *testing.T) {
	body := registerReplyBody(7, 128, 11264, "0.11.0-edge.1")
	for length := range len(body) {
		_, err := DecodeLoginRegister(body[:length])
		assert.ErrorIs(t, err, ErrTruncatedRegisterReply, "truncated to %d bytes", length)
	}
}

func TestDecodeLoginRegister_IgnoresTrailingBytes(t *testing.T) {
	body := append(registerReplyBody(7, 128, 11264, "1.0.0"), 0xFF, 0xFF)

	response, err := DecodeLoginRegister(body)
	require.NoError(t, err)
	assert.Equal(t, "1.0.0", response.ServerVersion)
}
