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

func TestTryFrom_ValidStatuses(t *testing.T) {
	tests := []struct {
		input    byte
		expected ClusterNodeStatus
	}{
		{0, Healthy},
		{1, Starting},
		{2, Stopping},
		{3, Unreachable},
		{4, Maintenance},
	}
	for _, tc := range tests {
		s, err := TryFrom(tc.input)
		if err != nil {
			t.Fatalf("unexpected error for byte %d: %v", tc.input, err)
		}
		if s != tc.expected {
			t.Fatalf("expected status %d for byte %d, got %d", tc.expected, tc.input, s)
		}
	}
}

func TestTryFrom_Invalid(t *testing.T) {
	_, err := TryFrom(99)
	if err == nil {
		t.Fatalf("expected error for invalid status byte 99, got nil")
	}
}

func TestClusterNodeStatus_MarshalUnmarshal_Roundtrip(t *testing.T) {
	for _, status := range []ClusterNodeStatus{Healthy, Starting, Stopping, Unreachable, Maintenance} {
		data, err := status.MarshalBinary()
		if err != nil {
			t.Fatalf("unexpected marshal error for status %d: %v", status, err)
		}
		var decoded ClusterNodeStatus
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatalf("unexpected unmarshal error for status %d: %v", status, err)
		}
		if decoded != status {
			t.Fatalf("expected status %d, got %d", status, decoded)
		}
	}
}

func TestClusterNodeStatus_String(t *testing.T) {
	tests := []struct {
		status   ClusterNodeStatus
		expected string
	}{
		{Healthy, "healthy"},
		{Starting, "starting"},
		{Stopping, "stopping"},
		{Unreachable, "unreachable"},
		{Maintenance, "maintenance"},
	}
	for _, tc := range tests {
		got := tc.status.String()
		if got != tc.expected {
			t.Fatalf("expected '%s' for status %d, got '%s'", tc.expected, tc.status, got)
		}
	}
}
