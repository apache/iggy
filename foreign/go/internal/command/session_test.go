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

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// buildExpectedLoginUser constructs the expected byte representation for a LoginUser command.
// Format: [username_len(1)][username][password_len(1)][password][version_len(4)][version][context_len(4)][context]
func buildExpectedLoginUser(username, password string) []byte {
	versionBytes := []byte(iggcon.Version)
	contextBytes := []byte("")

	totalLength := 1 + len(username) + 1 + len(password) +
		4 + len(versionBytes) + 4 + len(contextBytes)

	buf := make([]byte, totalLength)
	pos := 0

	buf[pos] = byte(len(username))
	pos++
	copy(buf[pos:], username)
	pos += len(username)

	buf[pos] = byte(len(password))
	pos++
	copy(buf[pos:], password)
	pos += len(password)

	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(versionBytes)))
	pos += 4
	copy(buf[pos:], versionBytes)
	pos += len(versionBytes)

	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(contextBytes)))
	pos += 4
	copy(buf[pos:], contextBytes)

	return buf
}

// TestSerialize_LoginUser_ContainsVersion verifies that the SDK version is included in login serialization.
func TestSerialize_LoginUser_ContainsVersion(t *testing.T) {
	request := LoginUser{
		Username: "iggy",
		Password: "iggy",
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LoginUser: %v", err)
	}

	// Skip past username (1-byte len + "iggy") and password (1-byte len + "iggy")
	pos := 1 + len("iggy") + 1 + len("iggy")

	// Read version length (u32 LE)
	versionLen := binary.LittleEndian.Uint32(serialized[pos : pos+4])
	pos += 4

	// Read version string
	version := string(serialized[pos : pos+int(versionLen)])

	if version != iggcon.Version {
		t.Errorf("Version mismatch. Expected: %q, Got: %q", iggcon.Version, version)
	}
}

// TestSerialize_LoginUser tests normal login with username and password
func TestSerialize_LoginUser(t *testing.T) {
	cmd := LoginUser{
		Username: "admin",
		Password: "secret123",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LoginUser: %v", err)
	}

	expected := buildExpectedLoginUser("admin", "secret123")

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_LoginUser_EmptyCredentials tests edge case with empty username and password
func TestSerialize_LoginUser_EmptyCredentials(t *testing.T) {
	cmd := LoginUser{
		Username: "",
		Password: "",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LoginUser with empty credentials: %v", err)
	}

	expected := buildExpectedLoginUser("", "")

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_LoginUser_LongCredentials tests with longer username and password
func TestSerialize_LoginUser_LongCredentials(t *testing.T) {
	cmd := LoginUser{
		Username: "user@example.com",
		Password: "very_secure_password_123!",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LoginUser with long credentials: %v", err)
	}

	expected := buildExpectedLoginUser("user@example.com", "very_secure_password_123!")

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_LoginUser_SingleCharCredentials tests edge case with single character credentials
func TestSerialize_LoginUser_SingleCharCredentials(t *testing.T) {
	cmd := LoginUser{
		Username: "a",
		Password: "b",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LoginUser with single char credentials: %v", err)
	}

	expected := buildExpectedLoginUser("a", "b")

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_LoginWithPersonalAccessToken tests login with token
func TestSerialize_LoginWithPersonalAccessToken(t *testing.T) {
	cmd := LoginWithPersonalAccessToken{
		Token: "my_access_token_12345",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LoginWithPersonalAccessToken: %v", err)
	}

	expected := []byte{
		0x15, // Token length = 21
		// "my_access_token_12345"
		0x6D, 0x79, 0x5F, 0x61, 0x63, 0x63, 0x65, 0x73,
		0x73, 0x5F, 0x74, 0x6F, 0x6B, 0x65, 0x6E, 0x5F,
		0x31, 0x32, 0x33, 0x34, 0x35,
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_LoginWithPersonalAccessToken_ShortToken tests with short token
func TestSerialize_LoginWithPersonalAccessToken_ShortToken(t *testing.T) {
	cmd := LoginWithPersonalAccessToken{
		Token: "abc",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LoginWithPersonalAccessToken with short token: %v", err)
	}

	expected := []byte{
		0x03,             // Token length = 3
		0x61, 0x62, 0x63, // "abc"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_LoginWithPersonalAccessToken_EmptyToken tests edge case with empty token
func TestSerialize_LoginWithPersonalAccessToken_EmptyToken(t *testing.T) {
	cmd := LoginWithPersonalAccessToken{
		Token: "",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LoginWithPersonalAccessToken with empty token: %v", err)
	}

	expected := []byte{
		0x00, // Token length = 0
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_LoginWithPersonalAccessToken_LongToken tests with longer token
func TestSerialize_LoginWithPersonalAccessToken_LongToken(t *testing.T) {
	cmd := LoginWithPersonalAccessToken{
		Token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LoginWithPersonalAccessToken with long token: %v", err)
	}

	expected := []byte{
		0x6F, // Token length = 111
		// "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ"
		0x65, 0x79, 0x4A, 0x68, 0x62, 0x47, 0x63, 0x69, 0x4F, 0x69, 0x4A, 0x49, 0x55, 0x7A, 0x49, 0x31,
		0x4E, 0x69, 0x49, 0x73, 0x49, 0x6E, 0x52, 0x35, 0x63, 0x43, 0x49, 0x36, 0x49, 0x6B, 0x70, 0x58,
		0x56, 0x43, 0x4A, 0x39, 0x2E, 0x65, 0x79, 0x4A, 0x7A, 0x64, 0x57, 0x49, 0x69, 0x4F, 0x69, 0x49,
		0x78, 0x4D, 0x6A, 0x4D, 0x30, 0x4E, 0x54, 0x59, 0x33, 0x4F, 0x44, 0x6B, 0x77, 0x49, 0x69, 0x77,
		0x69, 0x62, 0x6D, 0x46, 0x74, 0x5A, 0x53, 0x49, 0x36, 0x49, 0x6B, 0x70, 0x76, 0x61, 0x47, 0x34,
		0x67, 0x52, 0x47, 0x39, 0x6C, 0x49, 0x69, 0x77, 0x69, 0x61, 0x57, 0x46, 0x30, 0x49, 0x6A, 0x6F,
		0x78, 0x4E, 0x54, 0x45, 0x32, 0x4D, 0x6A, 0x4D, 0x35, 0x4D, 0x44, 0x49, 0x79, 0x66, 0x51,
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_LogoutUser tests LogoutUser serialization
func TestSerialize_LogoutUser(t *testing.T) {
	cmd := LogoutUser{}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LogoutUser: %v", err)
	}

	expected := []byte{} // Empty byte array

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}
