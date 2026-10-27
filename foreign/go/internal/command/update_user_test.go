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
	"errors"
	"slices"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
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
		want     []string
	}{
		{"both set", &username, &status, []string{"status", "username"}},
		{"username only", &username, nil, []string{"username"}},
		{"status only", nil, &status, []string{"status"}},
	}
	for _, test := range tests {
		command := UpdateUser{UserID: userID, Username: test.username, Status: test.status}
		payload, err := command.MarshalBinary()
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", test.name, err)
		}
		prefix := 1 + len(userIDBytes) + 1
		if len(payload) < prefix || payload[0] != byte(iggcon.OptionsScopeUser) || payload[prefix-1] != 0 {
			t.Fatalf("%s: payload %v does not lead with the user scope, id and no resets", test.name, payload)
		}
		entries, err := iggcon.DeserializeHeaders(payload[prefix:])
		if err != nil {
			t.Fatalf("%s: options block does not decode to the end: %v", test.name, err)
		}
		var keys []string
		for _, entry := range entries {
			keys = append(keys, string(entry.Key.Value))
		}
		if !slices.Equal(keys, test.want) {
			t.Errorf("%s: keys = %v, want %v", test.name, keys, test.want)
		}
	}
}

func TestUpdateUser_EmptyPatchIsRefused(t *testing.T) {
	userID, err := iggcon.NewIdentifier(uint32(7))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	command := UpdateUser{UserID: userID}

	if _, err := command.MarshalBinary(); !errors.Is(err, ierror.ErrInvalidCommand) {
		t.Errorf("err = %v, want InvalidCommand", err)
	}
}

func TestUpdateUser_MarshalBinaryDoesNotMutateUsername(t *testing.T) {
	userID, err := iggcon.NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	status := iggcon.Inactive
	command := UpdateUser{UserID: userID, Status: &status}

	if _, err = command.MarshalBinary(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if command.Username != nil {
		t.Errorf("Username = %v, want nil", command.Username)
	}
}
