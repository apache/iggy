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

func TestClusterMetadata_MarshalUnmarshal_Roundtrip(t *testing.T) {
	original := ClusterMetadata{
		Name: "test-cluster",
		Nodes: []ClusterNode{
			{
				Name:      "node-1",
				IP:        "192.168.1.10",
				Endpoints: NewTransportEndpoints(8090, 8091, 3000, 8080),
				Role:      RoleLeader,
				Status:    Healthy,
			},
			{
				Name:      "node-2",
				IP:        "192.168.1.11",
				Endpoints: NewTransportEndpoints(9090, 9091, 4000, 9080),
				Role:      RoleFollower,
				Status:    Starting,
			},
		},
	}

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	var decoded ClusterMetadata
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if decoded.Name != original.Name {
		t.Fatalf("Name mismatch: expected %s, got %s", original.Name, decoded.Name)
	}
	if len(decoded.Nodes) != len(original.Nodes) {
		t.Fatalf("Nodes count mismatch: expected %d, got %d", len(original.Nodes), len(decoded.Nodes))
	}
	for i := range original.Nodes {
		if decoded.Nodes[i].Name != original.Nodes[i].Name {
			t.Fatalf("Node[%d].Name mismatch: expected %s, got %s", i, original.Nodes[i].Name, decoded.Nodes[i].Name)
		}
		if decoded.Nodes[i].IP != original.Nodes[i].IP {
			t.Fatalf("Node[%d].IP mismatch: expected %s, got %s", i, original.Nodes[i].IP, decoded.Nodes[i].IP)
		}
		if decoded.Nodes[i].Role != original.Nodes[i].Role {
			t.Fatalf("Node[%d].Role mismatch: expected %d, got %d", i, original.Nodes[i].Role, decoded.Nodes[i].Role)
		}
		if decoded.Nodes[i].Status != original.Nodes[i].Status {
			t.Fatalf("Node[%d].Status mismatch: expected %d, got %d", i, original.Nodes[i].Status, decoded.Nodes[i].Status)
		}
	}
}

func TestClusterMetadata_BufferSize_MatchesMarshaledLength(t *testing.T) {
	m := ClusterMetadata{
		Name: "cluster",
		Nodes: []ClusterNode{
			{
				Name:      "n1",
				IP:        "10.0.0.1",
				Endpoints: NewTransportEndpoints(1, 2, 3, 4),
				Role:      RoleLeader,
				Status:    Healthy,
			},
		},
	}

	data, err := m.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	if len(data) != m.BufferSize() {
		t.Fatalf("expected marshaled length %d to match BufferSize %d", len(data), m.BufferSize())
	}
}

func TestClusterMetadata_UnmarshalBinary_EmptyData(t *testing.T) {
	var m ClusterMetadata
	if err := m.UnmarshalBinary([]byte{}); err == nil {
		t.Fatalf("expected error for empty data, got nil")
	}
}
