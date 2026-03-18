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
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// Helper functions for creating pointers
func stringPtr(s string) *string {
	return &s
}

func userStatusPtr(s iggcon.UserStatus) *iggcon.UserStatus {
	return &s
}

// TestSerialize_UpdateUser_BothFields tests UpdateUser with both username and status
func TestSerialize_UpdateUser_BothFields(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(42))
	username := "new_admin"
	status := iggcon.Active

	cmd := UpdateUser{
		UserID:   userId,
		Username: &username,
		Status:   &status,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize UpdateUser: %v", err)
	}

	expected := []byte{
		0x01,                   // UserId Kind = NumericId
		0x04,                   // UserId Length = 4
		0x2A, 0x00, 0x00, 0x00, // UserId Value = 42
		0x01,                                                 // Has username = 1
		0x09,                                                 // Username length = 9
		0x6E, 0x65, 0x77, 0x5F, 0x61, 0x64, 0x6D, 0x69, 0x6E, // "new_admin"
		0x01, // Has status = 1
		0x01, // Status = Active (1)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_UpdateUser_OnlyUsername tests UpdateUser with only username
func TestSerialize_UpdateUser_OnlyUsername(t *testing.T) {
	userId, _ := iggcon.NewIdentifier("user123")

	cmd := UpdateUser{
		UserID:   userId,
		Username: stringPtr("updated_name"),
		Status:   nil,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize UpdateUser with only username: %v", err)
	}

	expected := []byte{
		0x02,                                     // UserId Kind = StringId
		0x07,                                     // UserId Length = 7
		0x75, 0x73, 0x65, 0x72, 0x31, 0x32, 0x33, // "user123"
		0x01,                                                                   // Has username = 1
		0x0C,                                                                   // Username length = 12
		0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x5F, 0x6E, 0x61, 0x6D, 0x65, // "updated_name"
		0x00, // Has status = 0
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_UpdateUser_OnlyStatus tests UpdateUser with only status
func TestSerialize_UpdateUser_OnlyStatus(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(1))

	cmd := UpdateUser{
		UserID:   userId,
		Username: nil,
		Status:   userStatusPtr(iggcon.Inactive),
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize UpdateUser with only status: %v", err)
	}

	expected := []byte{
		0x01,                   // UserId Kind = NumericId
		0x04,                   // UserId Length = 4
		0x01, 0x00, 0x00, 0x00, // UserId Value = 1
		0x00, // Has username = 0
		0x01, // Has status = 1
		0x02, // Status = Inactive (2)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_UpdateUser_NeitherField tests UpdateUser with no updates (both nil)
func TestSerialize_UpdateUser_NeitherField(t *testing.T) {
	userId, _ := iggcon.NewIdentifier("admin")

	cmd := UpdateUser{
		UserID:   userId,
		Username: nil,
		Status:   nil,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize UpdateUser with no fields: %v", err)
	}

	expected := []byte{
		0x02,                         // UserId Kind = StringId
		0x05,                         // UserId Length = 5
		0x61, 0x64, 0x6D, 0x69, 0x6E, // "admin"
		0x00, // Has username = 0
		0x00, // Has status = 0
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_UpdateUser_EmptyUsername tests UpdateUser with empty username string
func TestSerialize_UpdateUser_EmptyUsername(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(99))

	cmd := UpdateUser{
		UserID:   userId,
		Username: stringPtr(""),
		Status:   userStatusPtr(iggcon.Active),
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize UpdateUser with empty username: %v", err)
	}

	expected := []byte{
		0x01,                   // UserId Kind = NumericId
		0x04,                   // UserId Length = 4
		0x63, 0x00, 0x00, 0x00, // UserId Value = 99
		0x00, // Has username = 0 (empty string treated as no username)
		0x01, // Has status = 1
		0x01, // Status = Active (1)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_UpdateUser_SingleCharUsername tests with single character username
func TestSerialize_UpdateUser_SingleCharUsername(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(5))

	cmd := UpdateUser{
		UserID:   userId,
		Username: stringPtr("a"),
		Status:   userStatusPtr(iggcon.Inactive),
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize UpdateUser with single char username: %v", err)
	}

	expected := []byte{
		0x01,                   // UserId Kind = NumericId
		0x04,                   // UserId Length = 4
		0x05, 0x00, 0x00, 0x00, // UserId Value = 5
		0x01, // Has username = 1
		0x01, // Username length = 1
		0x61, // "a"
		0x01, // Has status = 1
		0x02, // Status = Inactive (2)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_UpdateUser_LongUsername tests with a longer username
func TestSerialize_UpdateUser_LongUsername(t *testing.T) {
	userId, _ := iggcon.NewIdentifier("test")

	cmd := UpdateUser{
		UserID:   userId,
		Username: stringPtr("very_long_username_for_testing"),
		Status:   nil,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize UpdateUser with long username: %v", err)
	}

	expected := []byte{
		0x02,                   // UserId Kind = StringId
		0x04,                   // UserId Length = 4
		0x74, 0x65, 0x73, 0x74, // "test"
		0x01, // Has username = 1
		0x1E, // Username length = 30
		// "very_long_username_for_testing"
		0x76, 0x65, 0x72, 0x79, 0x5F, 0x6C, 0x6F, 0x6E,
		0x67, 0x5F, 0x75, 0x73, 0x65, 0x72, 0x6E, 0x61,
		0x6D, 0x65, 0x5F, 0x66, 0x6F, 0x72, 0x5F, 0x74,
		0x65, 0x73, 0x74, 0x69, 0x6E, 0x67,
		0x00, // Has status = 0
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}
