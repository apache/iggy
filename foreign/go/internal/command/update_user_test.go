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
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// The options block decodes to the end of the payload, so any slack the sizing
// leaves behind reaches the server as an entry with kind 0, which it rejects.
func TestUpdateUser_MarshalBinaryLeavesNoSlack(t *testing.T) {
	username := "user"
	status := iggcon.Active
	userID, err := iggcon.NewIdentifier(uint32(7))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	userIDBytes, err := userID.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tests := []struct {
		name     string
		username *string
		status   *iggcon.UserStatus
		want     int
	}{
		{"both set", &username, &status, len(userIDBytes) + 2 + len(username) + 2},
		{"username only", &username, nil, len(userIDBytes) + 2 + len(username) + 1},
		{"status only", nil, &status, len(userIDBytes) + 1 + 2},
		{"neither set", nil, nil, len(userIDBytes) + 2},
	}
	for _, test := range tests {
		command := UpdateUser{UserID: userID, Username: test.username, Status: test.status}
		payload, err := command.MarshalBinary()
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", test.name, err)
		}
		if len(payload) != test.want {
			t.Errorf("%s: payload length = %d, want %d", test.name, len(payload), test.want)
		}
	}
}

func TestUpdateUser_MarshalBinaryDoesNotMutateUsername(t *testing.T) {
	userID, err := iggcon.NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	command := UpdateUser{UserID: userID}

	if _, err = command.MarshalBinary(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if command.Username != nil {
		t.Errorf("Username = %v, want nil", command.Username)
	}
}
