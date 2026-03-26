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

package command

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestCreatePersonalAccessToken_MarshalBinary(t *testing.T) {
	request := CreatePersonalAccessToken{
		Name:   "mytoken",
		Expiry: 3600,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expectedLen := 1 + len("mytoken") + 8
	if len(serialized) != expectedLen {
		t.Fatalf("expected length %d, got %d", expectedLen, len(serialized))
	}

	if serialized[0] != byte(len("mytoken")) {
		t.Fatalf("expected name length byte %d, got %d", len("mytoken"), serialized[0])
	}

	if !bytes.Equal(serialized[1:1+len("mytoken")], []byte("mytoken")) {
		t.Fatalf("expected name bytes %v, got %v", []byte("mytoken"), serialized[1:1+len("mytoken")])
	}

	expiry := binary.LittleEndian.Uint32(serialized[len(serialized)-4:])
	if expiry != 3600 {
		t.Fatalf("expected expiry 3600, got %d", expiry)
	}
}

func TestGetPersonalAccessTokens_MarshalBinary(t *testing.T) {
	request := GetPersonalAccessTokens{}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(serialized) != 0 {
		t.Fatalf("expected empty bytes, got %v", serialized)
	}
}

func TestDeletePersonalAccessToken_MarshalBinary(t *testing.T) {
	request := DeletePersonalAccessToken{
		Name: "mytoken",
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x07,                                     // name length = 7
		0x6D, 0x79, 0x74, 0x6F, 0x6B, 0x65, 0x6E, // "mytoken"
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}
