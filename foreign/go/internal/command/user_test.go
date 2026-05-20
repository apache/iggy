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
