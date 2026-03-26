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

func TestNewTransportEndpoints_SetsAllFields(t *testing.T) {
	ep := NewTransportEndpoints(8090, 8091, 3000, 8080)
	if ep.Tcp != 8090 {
		t.Fatalf("expected Tcp 8090, got %d", ep.Tcp)
	}
	if ep.Quic != 8091 {
		t.Fatalf("expected Quic 8091, got %d", ep.Quic)
	}
	if ep.Http != 3000 {
		t.Fatalf("expected Http 3000, got %d", ep.Http)
	}
	if ep.WebSocket != 8080 {
		t.Fatalf("expected WebSocket 8080, got %d", ep.WebSocket)
	}
}

func TestTransportEndpoints_MarshalUnmarshal_Roundtrip(t *testing.T) {
	original := NewTransportEndpoints(8090, 8091, 3000, 8080)
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	var decoded TransportEndpoints
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if decoded.Tcp != original.Tcp {
		t.Fatalf("Tcp mismatch: expected %d, got %d", original.Tcp, decoded.Tcp)
	}
	if decoded.Quic != original.Quic {
		t.Fatalf("Quic mismatch: expected %d, got %d", original.Quic, decoded.Quic)
	}
	if decoded.Http != original.Http {
		t.Fatalf("Http mismatch: expected %d, got %d", original.Http, decoded.Http)
	}
	if decoded.WebSocket != original.WebSocket {
		t.Fatalf("WebSocket mismatch: expected %d, got %d", original.WebSocket, decoded.WebSocket)
	}
}

func TestTransportEndpoints_GetBufferSize(t *testing.T) {
	ep := NewTransportEndpoints(1, 2, 3, 4)
	if ep.GetBufferSize() != 8 {
		t.Fatalf("expected buffer size 8, got %d", ep.GetBufferSize())
	}
}

func TestTransportEndpoints_String(t *testing.T) {
	ep := NewTransportEndpoints(8090, 8091, 3000, 8080)
	s := ep.String()
	if !strings.Contains(s, "8090") {
		t.Fatalf("expected string to contain '8090', got %s", s)
	}
	if !strings.Contains(s, "8091") {
		t.Fatalf("expected string to contain '8091', got %s", s)
	}
	if !strings.Contains(s, "3000") {
		t.Fatalf("expected string to contain '3000', got %s", s)
	}
	if !strings.Contains(s, "8080") {
		t.Fatalf("expected string to contain '8080', got %s", s)
	}
}
