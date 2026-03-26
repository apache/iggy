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

func TestCreateUser_MarshalBinary(t *testing.T) {
	request := CreateUser{
		Username:    "admin",
		Password:    "pass",
		Status:      iggcon.Active,
		Permissions: nil,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x05,                         // username length = 5
		0x61, 0x64, 0x6D, 0x69, 0x6E, // "admin"
		0x04,                   // password length = 4
		0x70, 0x61, 0x73, 0x73, // "pass"
		0x01, // status = Active (wire format 1)
		0x00, // no permissions
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestGetUser_MarshalBinary(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(42))

	request := GetUser{
		Id: userId,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x01, 0x04, 0x2A, 0x00, 0x00, 0x00, // numeric id = 42
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestGetUsers_MarshalBinary(t *testing.T) {
	request := GetUsers{}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(serialized) != 0 {
		t.Fatalf("expected empty bytes, got %v", serialized)
	}
}

func TestUpdatePermissions_MarshalBinary_NilPermissions(t *testing.T) {
	userId, _ := iggcon.NewIdentifier("admin")

	request := UpdatePermissions{
		UserID:      userId,
		Permissions: nil,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x02, 0x05, 0x61, 0x64, 0x6D, 0x69, 0x6E, // user id = "admin"
		0x00, // no permissions
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestChangePassword_MarshalBinary(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(1))

	request := ChangePassword{
		UserID:          userId,
		CurrentPassword: "old",
		NewPassword:     "new",
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x01, 0x04, 0x01, 0x00, 0x00, 0x00, // user id = 1
		0x03,             // current password length = 3
		0x6F, 0x6C, 0x64, // "old"
		0x03,             // new password length = 3
		0x6E, 0x65, 0x77, // "new"
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestDeleteUser_MarshalBinary(t *testing.T) {
	userId, _ := iggcon.NewIdentifier("admin")

	request := DeleteUser{
		Id: userId,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x02, 0x05, 0x61, 0x64, 0x6D, 0x69, 0x6E, // string id = "admin"
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}
