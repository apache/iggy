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

package iggcon

import (
	"errors"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestClusterNodeRoleTryFrom(t *testing.T) {
	tests := []struct {
		name    string
		input   byte
		want    ClusterNodeRole
		wantErr bool
	}{
		{"leader", 0, RoleLeader, false},
		{"follower", 1, RoleFollower, false},
		{"first invalid byte", 2, 0, true},
		{"max byte", 255, 0, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ClusterNodeRoleTryFrom(test.input)
			if test.wantErr {
				if !errors.Is(err, ierror.ErrInvalidCommand) {
					t.Fatalf("ClusterNodeRoleTryFrom(%d) error = %v, want %v",
						test.input, err, ierror.ErrInvalidCommand)
				}
				return
			}
			if err != nil {
				t.Fatalf("ClusterNodeRoleTryFrom(%d) unexpected error: %v", test.input, err)
			}
			if got != test.want {
				t.Errorf("ClusterNodeRoleTryFrom(%d) = %d, want %d", test.input, got, test.want)
			}
		})
	}
}

func TestClusterNodeRole_MarshalUnmarshalRoundTrip(t *testing.T) {
	for _, role := range []ClusterNodeRole{RoleLeader, RoleFollower} {
		data, err := role.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary(%d) unexpected error: %v", role, err)
		}
		if len(data) != 1 || data[0] != byte(role) {
			t.Fatalf("MarshalBinary(%d) = %v, want [%d]", role, data, role)
		}

		var decoded ClusterNodeRole
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatalf("UnmarshalBinary(%v) unexpected error: %v", data, err)
		}
		if decoded != role {
			t.Errorf("round trip of %d decoded to %d", role, decoded)
		}
	}
}

func TestClusterNodeRole_UnmarshalBinaryRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{"nil slice", nil},
		{"empty slice", []byte{}},
		{"invalid byte", []byte{2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			role := RoleFollower
			err := role.UnmarshalBinary(test.input)
			if !errors.Is(err, ierror.ErrInvalidCommand) {
				t.Fatalf("UnmarshalBinary(%v) error = %v, want %v",
					test.input, err, ierror.ErrInvalidCommand)
			}
			if role != RoleFollower {
				t.Errorf("UnmarshalBinary(%v) modified the receiver to %d on error", test.input, role)
			}
		})
	}
}

func TestClusterNodeRole_String(t *testing.T) {
	tests := []struct {
		role ClusterNodeRole
		want string
	}{
		{RoleLeader, "leader"},
		{RoleFollower, "follower"},
		{ClusterNodeRole(2), "unknown"},
	}
	for _, test := range tests {
		if got := test.role.String(); got != test.want {
			t.Errorf("ClusterNodeRole(%d).String() = %q, want %q", test.role, got, test.want)
		}
	}
}
