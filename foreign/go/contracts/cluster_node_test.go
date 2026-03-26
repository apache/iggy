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
	"strings"
	"testing"
)

func TestClusterNode_MarshalUnmarshal_Roundtrip(t *testing.T) {
	original := ClusterNode{
		Name:      "node-1",
		IP:        "192.168.1.10",
		Endpoints: NewTransportEndpoints(8090, 8091, 3000, 8080),
		Role:      RoleLeader,
		Status:    Healthy,
	}

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	var decoded ClusterNode
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if decoded.Name != original.Name {
		t.Fatalf("Name mismatch: expected %s, got %s", original.Name, decoded.Name)
	}
	if decoded.IP != original.IP {
		t.Fatalf("IP mismatch: expected %s, got %s", original.IP, decoded.IP)
	}
	if decoded.Endpoints.Tcp != original.Endpoints.Tcp {
		t.Fatalf("Tcp mismatch: expected %d, got %d", original.Endpoints.Tcp, decoded.Endpoints.Tcp)
	}
	if decoded.Endpoints.Quic != original.Endpoints.Quic {
		t.Fatalf("Quic mismatch: expected %d, got %d", original.Endpoints.Quic, decoded.Endpoints.Quic)
	}
	if decoded.Endpoints.Http != original.Endpoints.Http {
		t.Fatalf("Http mismatch: expected %d, got %d", original.Endpoints.Http, decoded.Endpoints.Http)
	}
	if decoded.Endpoints.WebSocket != original.Endpoints.WebSocket {
		t.Fatalf("WebSocket mismatch: expected %d, got %d", original.Endpoints.WebSocket, decoded.Endpoints.WebSocket)
	}
	if decoded.Role != original.Role {
		t.Fatalf("Role mismatch: expected %d, got %d", original.Role, decoded.Role)
	}
	if decoded.Status != original.Status {
		t.Fatalf("Status mismatch: expected %d, got %d", original.Status, decoded.Status)
	}
}

func TestClusterNode_BufferSize_MatchesMarshaledLength(t *testing.T) {
	node := ClusterNode{
		Name:      "node-1",
		IP:        "192.168.1.10",
		Endpoints: NewTransportEndpoints(8090, 8091, 3000, 8080),
		Role:      RoleFollower,
		Status:    Starting,
	}

	data, err := node.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	if len(data) != node.BufferSize() {
		t.Fatalf("expected marshaled length %d to match BufferSize %d", len(data), node.BufferSize())
	}
}

func TestClusterNode_String_ContainsNameAndIP(t *testing.T) {
	node := ClusterNode{
		Name:      "test-node",
		IP:        "10.0.0.1",
		Endpoints: NewTransportEndpoints(1, 2, 3, 4),
		Role:      RoleLeader,
		Status:    Healthy,
	}

	s := node.String()
	if !strings.Contains(s, "test-node") {
		t.Fatalf("expected string to contain 'test-node', got %s", s)
	}
	if !strings.Contains(s, "10.0.0.1") {
		t.Fatalf("expected string to contain '10.0.0.1', got %s", s)
	}
}
