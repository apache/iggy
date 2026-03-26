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

import "testing"

func TestClusterNodeRoleTryFrom_Leader(t *testing.T) {
	r, err := ClusterNodeRoleTryFrom(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r != RoleLeader {
		t.Fatalf("expected RoleLeader, got %d", r)
	}
}

func TestClusterNodeRoleTryFrom_Follower(t *testing.T) {
	r, err := ClusterNodeRoleTryFrom(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r != RoleFollower {
		t.Fatalf("expected RoleFollower, got %d", r)
	}
}

func TestClusterNodeRoleTryFrom_Invalid(t *testing.T) {
	_, err := ClusterNodeRoleTryFrom(99)
	if err == nil {
		t.Fatalf("expected error for invalid role byte 99, got nil")
	}
}

func TestClusterNodeRole_MarshalUnmarshal_Roundtrip(t *testing.T) {
	for _, role := range []ClusterNodeRole{RoleLeader, RoleFollower} {
		data, err := role.MarshalBinary()
		if err != nil {
			t.Fatalf("unexpected marshal error for role %d: %v", role, err)
		}
		var decoded ClusterNodeRole
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatalf("unexpected unmarshal error for role %d: %v", role, err)
		}
		if decoded != role {
			t.Fatalf("expected role %d, got %d", role, decoded)
		}
	}
}

func TestClusterNodeRole_String_Leader(t *testing.T) {
	r := RoleLeader
	if r.String() != "leader" {
		t.Fatalf("expected 'leader', got '%s'", r.String())
	}
}

func TestClusterNodeRole_String_Follower(t *testing.T) {
	r := RoleFollower
	if r.String() != "follower" {
		t.Fatalf("expected 'follower', got '%s'", r.String())
	}
}
