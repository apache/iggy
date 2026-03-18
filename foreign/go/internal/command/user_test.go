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

// Helper to create test permissions with only global permissions
func createTestGlobalPermissions(all bool) iggcon.GlobalPermissions {
	return iggcon.GlobalPermissions{
		ManageServers: all,
		ReadServers:   all,
		ManageUsers:   all,
		ReadUsers:     all,
		ManageStreams: all,
		ReadStreams:   all,
		ManageTopics:  all,
		ReadTopics:    all,
		PollMessages:  all,
		SendMessages:  all,
	}
}

func createTestPermissions(global iggcon.GlobalPermissions) *iggcon.Permissions {
	return &iggcon.Permissions{
		Global:  global,
		Streams: nil, // Simple case: no stream-specific permissions
	}
}

func TestSerialize_CreateUser_NilPermissions(t *testing.T) {
	request := CreateUser{
		Username: "u",
		Password: "p",
		Status:   iggcon.Active,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateUser: %v", err)
	}

	// Wire format: [username_len:u8][username][password_len:u8][password][status:u8][has_permissions:u8]
	expected := []byte{
		0x01, // username_len
		0x75, // 'u'
		0x01, // password_len
		0x70, // 'p'
		0x01, // status (Active=1)
		0x00, // has_permissions=0
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("CreateUser nil permissions:\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerialize_CreateUser_WithPermissions(t *testing.T) {
	perms := &iggcon.Permissions{
		Global: iggcon.GlobalPermissions{
			ManageServers: true,
		},
		Streams: nil,
	}

	request := CreateUser{
		Username:    "user",
		Password:    "pass",
		Status:      iggcon.Inactive,
		Permissions: perms,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateUser: %v", err)
	}

	permBytes, err := perms.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize Permissions: %v", err)
	}

	// 1(username_len) + 4(username) + 1(password_len) + 4(password) + 1(status) + 1(has_perm) + 4(perm_len) + permBytes
	expectedLen := 1 + 4 + 1 + 4 + 1 + 1 + 4 + len(permBytes)
	if len(serialized) != expectedLen {
		t.Fatalf("Expected length %d, got %d", expectedLen, len(serialized))
	}

	pos := 0

	if serialized[pos] != 4 {
		t.Errorf("username_len: expected 4, got %d", serialized[pos])
	}
	pos++
	if string(serialized[pos:pos+4]) != "user" {
		t.Errorf("username: expected 'user', got %q", serialized[pos:pos+4])
	}
	pos += 4

	if serialized[pos] != 4 {
		t.Errorf("password_len: expected 4, got %d", serialized[pos])
	}
	pos++
	if string(serialized[pos:pos+4]) != "pass" {
		t.Errorf("password: expected 'pass', got %q", serialized[pos:pos+4])
	}
	pos += 4

	if serialized[pos] != 2 {
		t.Errorf("status: expected 2 (Inactive), got %d", serialized[pos])
	}
	pos++

	if serialized[pos] != 1 {
		t.Errorf("has_permissions: expected 1, got %d", serialized[pos])
	}
	pos++

	permLen := binary.LittleEndian.Uint32(serialized[pos : pos+4])
	if int(permLen) != len(permBytes) {
		t.Errorf("permissions_len: expected %d, got %d", len(permBytes), permLen)
	}
	pos += 4

	if !bytes.Equal(serialized[pos:], permBytes) {
		t.Errorf("permissions payload mismatch")
	}
}

func TestSerialize_UpdatePermissions_NilPermissions(t *testing.T) {
	userId, err := iggcon.NewIdentifier(uint32(42))
	if err != nil {
		t.Fatalf("Failed to create identifier: %v", err)
	}

	request := UpdatePermissions{
		UserID:      userId,
		Permissions: nil,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize UpdatePermissions: %v", err)
	}

	userIdBytes, _ := userId.MarshalBinary()

	// [user_id bytes][has_permissions:u8=0]
	expectedLen := len(userIdBytes) + 1
	if len(serialized) != expectedLen {
		t.Fatalf("Expected length %d, got %d", expectedLen, len(serialized))
	}

	if !bytes.Equal(serialized[:len(userIdBytes)], userIdBytes) {
		t.Errorf("UserID bytes mismatch")
	}

	if serialized[len(userIdBytes)] != 0 {
		t.Errorf("has_permissions: expected 0, got %d", serialized[len(userIdBytes)])
	}
}

func TestSerialize_UpdatePermissions_WithPermissions(t *testing.T) {
	userId, err := iggcon.NewIdentifier(uint32(7))
	if err != nil {
		t.Fatalf("Failed to create identifier: %v", err)
	}

	perms := &iggcon.Permissions{
		Global: iggcon.GlobalPermissions{
			ManageServers: true,
			ReadUsers:     true,
		},
		Streams: nil,
	}

	request := UpdatePermissions{
		UserID:      userId,
		Permissions: perms,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize UpdatePermissions: %v", err)
	}

	userIdBytes, _ := userId.MarshalBinary()
	permBytes, _ := perms.MarshalBinary()

	// [user_id][has_permissions:u8=1][permissions_len:u32_le][permissions]
	expectedLen := len(userIdBytes) + 1 + 4 + len(permBytes)
	if len(serialized) != expectedLen {
		t.Fatalf("Expected length %d, got %d", expectedLen, len(serialized))
	}

	pos := len(userIdBytes)

	if serialized[pos] != 1 {
		t.Errorf("has_permissions: expected 1, got %d", serialized[pos])
	}
	pos++

	permLen := binary.LittleEndian.Uint32(serialized[pos : pos+4])
	if int(permLen) != len(permBytes) {
		t.Errorf("permissions_len: expected %d, got %d", len(permBytes), permLen)
	}
	pos += 4

	if !bytes.Equal(serialized[pos:], permBytes) {
		t.Errorf("permissions payload mismatch")
	}
}

// TestSerialize_CreateUser_WithPermissions_ActiveStatus tests CreateUser with permissions and Active status
func TestSerialize_CreateUser_WithPermissions_ActiveStatus(t *testing.T) {
	globalPerms := createTestGlobalPermissions(true)
	permissions := createTestPermissions(globalPerms)

	cmd := CreateUser{
		Username:    "admin",
		Password:    "secret",
		Status:      iggcon.Active,
		Permissions: permissions,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateUser: %v", err)
	}

	expected := []byte{
		0x05,                         // Username length = 5
		0x61, 0x64, 0x6D, 0x69, 0x6E, // "admin"
		0x06,                               // Password length = 6
		0x73, 0x65, 0x63, 0x72, 0x65, 0x74, // "secret"
		0x01,                   // Status = Active (1)
		0x01,                   // Has permissions = 1
		0x0B, 0x00, 0x00, 0x00, // Permissions length = 11
		// Global permissions (10 bytes) - all true
		0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
		0x00, // No stream-specific permissions
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreateUser_InactiveStatus tests CreateUser with Inactive status
func TestSerialize_CreateUser_InactiveStatus(t *testing.T) {
	cmd := CreateUser{
		Username:    "inactive_user",
		Password:    "pwd",
		Status:      iggcon.Inactive,
		Permissions: nil,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateUser with inactive status: %v", err)
	}

	expected := []byte{
		0x0D,                                                                         // Username length = 13
		0x69, 0x6E, 0x61, 0x63, 0x74, 0x69, 0x76, 0x65, 0x5F, 0x75, 0x73, 0x65, 0x72, // "inactive_user"
		0x03,             // Password length = 3
		0x70, 0x77, 0x64, // "pwd"
		0x02, // Status = Inactive (2)
		0x00, // Has permissions = 0
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreateUser_EmptyCredentials tests edge case with empty username/password
func TestSerialize_CreateUser_EmptyCredentials(t *testing.T) {
	cmd := CreateUser{
		Username:    "",
		Password:    "",
		Status:      iggcon.Active,
		Permissions: nil,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateUser with empty credentials: %v", err)
	}

	expected := []byte{
		0x00, // Username length = 0
		0x00, // Password length = 0
		0x01, // Status = Active (1)
		0x00, // Has permissions = 0
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreateUser_PartialPermissions tests with partial global permissions
func TestSerialize_CreateUser_PartialPermissions(t *testing.T) {
	globalPerms := iggcon.GlobalPermissions{
		ManageServers: true,
		ReadServers:   true,
		ManageUsers:   false,
		ReadUsers:     true,
		ManageStreams: false,
		ReadStreams:   true,
		ManageTopics:  false,
		ReadTopics:    true,
		PollMessages:  true,
		SendMessages:  false,
	}
	permissions := createTestPermissions(globalPerms)

	cmd := CreateUser{
		Username:    "user",
		Password:    "pass",
		Status:      iggcon.Active,
		Permissions: permissions,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateUser with partial permissions: %v", err)
	}

	expected := []byte{
		0x04,                   // Username length = 4
		0x75, 0x73, 0x65, 0x72, // "user"
		0x04,                   // Password length = 4
		0x70, 0x61, 0x73, 0x73, // "pass"
		0x01,                   // Status = Active (1)
		0x01,                   // Has permissions = 1
		0x0B, 0x00, 0x00, 0x00, // Permissions length = 11
		// Global permissions: 1,1,0,1,0,1,0,1,1,0
		0x01, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x01, 0x00,
		0x00, // No stream-specific permissions
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetUsers tests GetUsers serialization (empty)
func TestSerialize_GetUsers(t *testing.T) {
	cmd := GetUsers{}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetUsers: %v", err)
	}

	expected := []byte{} // Empty byte array

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetUser_NumericId tests GetUser with numeric identifier
func TestSerialize_GetUser_NumericId(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(123))

	cmd := GetUser{
		Id: userId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetUser with numeric ID: %v", err)
	}

	expected := []byte{
		0x01,                   // Kind = NumericId
		0x04,                   // Length = 4
		0x7B, 0x00, 0x00, 0x00, // Value = 123
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetUser_StringId tests GetUser with string identifier
func TestSerialize_GetUser_StringId(t *testing.T) {
	userId, _ := iggcon.NewIdentifier("admin")

	cmd := GetUser{
		Id: userId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetUser with string ID: %v", err)
	}

	expected := []byte{
		0x02,                         // Kind = StringId
		0x05,                         // Length = 5
		0x61, 0x64, 0x6D, 0x69, 0x6E, // "admin"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_ChangePassword tests ChangePassword serialization
func TestSerialize_ChangePassword(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(1))

	cmd := ChangePassword{
		UserID:          userId,
		CurrentPassword: "old_pass",
		NewPassword:     "new_pass",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize ChangePassword: %v", err)
	}

	expected := []byte{
		0x01,                   // UserId Kind = NumericId
		0x04,                   // UserId Length = 4
		0x01, 0x00, 0x00, 0x00, // UserId Value = 1
		0x08,                                           // CurrentPassword length = 8
		0x6F, 0x6C, 0x64, 0x5F, 0x70, 0x61, 0x73, 0x73, // "old_pass"
		0x08,                                           // NewPassword length = 8
		0x6E, 0x65, 0x77, 0x5F, 0x70, 0x61, 0x73, 0x73, // "new_pass"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_ChangePassword_EmptyPasswords tests edge case with empty passwords
func TestSerialize_ChangePassword_EmptyPasswords(t *testing.T) {
	userId, _ := iggcon.NewIdentifier("admin")

	cmd := ChangePassword{
		UserID:          userId,
		CurrentPassword: "",
		NewPassword:     "",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize ChangePassword with empty passwords: %v", err)
	}

	expected := []byte{
		0x02,                         // UserId Kind = StringId
		0x05,                         // UserId Length = 5
		0x61, 0x64, 0x6D, 0x69, 0x6E, // "admin"
		0x00, // CurrentPassword length = 0
		0x00, // NewPassword length = 0
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeleteUser_NumericId tests DeleteUser with numeric identifier
func TestSerialize_DeleteUser_NumericId(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(999))

	cmd := DeleteUser{
		Id: userId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeleteUser with numeric ID: %v", err)
	}

	expected := []byte{
		0x01,                   // Kind = NumericId
		0x04,                   // Length = 4
		0xE7, 0x03, 0x00, 0x00, // Value = 999
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeleteUser_StringId tests DeleteUser with string identifier
func TestSerialize_DeleteUser_StringId(t *testing.T) {
	userId, _ := iggcon.NewIdentifier("test_user")

	cmd := DeleteUser{
		Id: userId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeleteUser with string ID: %v", err)
	}

	expected := []byte{
		0x02,                                                 // Kind = StringId
		0x09,                                                 // Length = 9
		0x74, 0x65, 0x73, 0x74, 0x5F, 0x75, 0x73, 0x65, 0x72, // "test_user"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}
