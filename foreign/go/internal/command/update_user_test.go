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

func TestUpdateUser_MarshalBinary_WithUsernameAndStatus(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(1))
	username := "admin"
	status := iggcon.Active

	request := UpdateUser{
		UserID:   userId,
		Username: &username,
		Status:   &status,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x01, 0x04, 0x01, 0x00, 0x00, 0x00, // user id = 1
		0x01,                         // has username
		0x05,                         // username length = 5
		0x61, 0x64, 0x6D, 0x69, 0x6E, // "admin"
		0x01, // has status
		0x01, // status = Active (wire format 1)
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestUpdateUser_MarshalBinary_NilUsernameAndNilStatus(t *testing.T) {
	userId, _ := iggcon.NewIdentifier(uint32(1))

	request := UpdateUser{
		UserID:   userId,
		Username: nil,
		Status:   nil,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x01, 0x04, 0x01, 0x00, 0x00, 0x00, // user id = 1
		0x00, // no username
		0x00, // no status
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}
