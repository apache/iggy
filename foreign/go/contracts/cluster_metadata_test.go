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
	"encoding/binary"
	"math"
	"testing"
)

func TestClusterMetadataUnmarshalBinaryRejectsOversizedName(t *testing.T) {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint32(payload, math.MaxUint32)

	var metadata ClusterMetadata
	if err := metadata.UnmarshalBinary(payload); err == nil {
		t.Fatal("expected an error for an oversized cluster name")
	}
}

func TestClusterNodeUnmarshalBinaryRejectsOversizedLengths(t *testing.T) {
	tests := map[string][]byte{
		"name": func() []byte {
			payload := make([]byte, 19)
			binary.LittleEndian.PutUint32(payload[1:], math.MaxUint32)
			return payload
		}(),
		"IP": func() []byte {
			payload := make([]byte, 19)
			binary.LittleEndian.PutUint32(payload[5:], math.MaxUint32)
			return payload
		}(),
	}

	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			var node ClusterNode
			if err := node.UnmarshalBinary(payload); err == nil {
				t.Fatal("expected an error for an oversized field")
			}
		})
	}
}

func TestClusterMetadataPreservesReplicaIDsIndependentOfOrder(t *testing.T) {
	want := ClusterMetadata{Name: "cluster", Nodes: []ClusterNode{
		{ReplicaID: 2, Name: "third", IP: "127.0.0.3", Endpoints: TransportEndpoints{Tcp: 8090}, Role: RoleFollower, Status: Healthy},
		{ReplicaID: 0, Name: "first", IP: "127.0.0.1", Endpoints: TransportEndpoints{Tcp: 8091}, Role: RoleLeader, Status: Healthy},
	}}
	body, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if body[4+len(want.Name)+4] != 2 {
		t.Fatal("node encoding must begin with configured replica id")
	}
	var got ClusterMetadata
	if err := got.UnmarshalBinary(body); err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 2 || got.Nodes[0] != want.Nodes[0] || got.Nodes[1] != want.Nodes[1] {
		t.Fatalf("replica ids or fields changed: %#v", got)
	}
}

func TestPartitionRouteRequiresItsExactWireLayout(t *testing.T) {
	body := binary.LittleEndian.AppendUint64(nil, 17)
	body = binary.LittleEndian.AppendUint64(body, 42)
	body = binary.LittleEndian.AppendUint32(body, 7)
	body = binary.LittleEndian.AppendUint32(body, 3)
	body = append(body, 2)
	var route PartitionRoute
	if err := route.UnmarshalBinary(body); err != nil {
		t.Fatal(err)
	}
	want := PartitionRoute{Incarnation: 17, MetadataOp: 42, View: 7, PartitionsCount: 3, PrimaryReplica: 2}
	if route != want {
		t.Fatalf("route: %#v, want %#v", route, want)
	}
	if context := route.Context(); context != (PartitionContext{Incarnation: 17, MetadataOp: 42}) {
		t.Fatalf("route fabricated an owner or misplaced metadata: %#v", context)
	}
	for length := range PartitionRouteSize {
		if err := route.UnmarshalBinary(body[:length]); err == nil {
			t.Fatalf("accepted %d-byte route", length)
		}
	}
	if err := route.UnmarshalBinary(append(body, 0)); err == nil {
		t.Fatal("accepted trailing byte")
	}
}
