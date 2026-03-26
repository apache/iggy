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
	"bytes"
	"testing"
)

func TestNewHeaderKeyString_Success(t *testing.T) {
	k, err := NewHeaderKeyString("key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if k.Kind != String {
		t.Fatalf("expected kind String (%d), got %d", String, k.Kind)
	}
	if !bytes.Equal(k.Value, []byte("key")) {
		t.Fatalf("expected value %v, got %v", []byte("key"), k.Value)
	}
}

func TestNewHeaderKeyString_EmptyReturnsError(t *testing.T) {
	_, err := NewHeaderKeyString("")
	if err == nil {
		t.Fatalf("expected error for empty string, got nil")
	}
}

func TestNewHeaderKeyRaw_Success(t *testing.T) {
	input := []byte{1, 2, 3}
	k, err := NewHeaderKeyRaw(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if k.Kind != Raw {
		t.Fatalf("expected kind Raw (%d), got %d", Raw, k.Kind)
	}
	if !bytes.Equal(k.Value, input) {
		t.Fatalf("expected value %v, got %v", input, k.Value)
	}
}

func TestNewHeaderKeyRaw_NilReturnsError(t *testing.T) {
	_, err := NewHeaderKeyRaw(nil)
	if err == nil {
		t.Fatalf("expected error for nil bytes, got nil")
	}
}

func TestNewHeaderKeyInt32_ReturnsInt32KindWith4Bytes(t *testing.T) {
	k := NewHeaderKeyInt32(42)
	if k.Kind != Int32 {
		t.Fatalf("expected kind Int32 (%d), got %d", Int32, k.Kind)
	}
	if len(k.Value) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(k.Value))
	}
}

func TestHeaderKind_ExpectedSize(t *testing.T) {
	tests := []struct {
		kind     HeaderKind
		expected int
	}{
		{Bool, 1},
		{Int16, 2},
		{Int32, 4},
		{Int64, 8},
		{Int128, 16},
		{Raw, -1},
		{String, -1},
	}
	for _, tc := range tests {
		got := tc.kind.ExpectedSize()
		if got != tc.expected {
			t.Fatalf("HeaderKind(%d).ExpectedSize() = %d, want %d", tc.kind, got, tc.expected)
		}
	}
}

func TestGetHeadersBytes_DeserializeHeaders_Roundtrip(t *testing.T) {
	key, err := NewHeaderKeyString("mykey")
	if err != nil {
		t.Fatalf("unexpected error creating header key: %v", err)
	}
	value := HeaderValue{Kind: String, Value: []byte("myval")}
	entry := HeaderEntry{Key: key, Value: value}

	serialized := GetHeadersBytes([]HeaderEntry{entry})
	if len(serialized) == 0 {
		t.Fatalf("expected non-empty serialized bytes")
	}

	deserialized, err := DeserializeHeaders(serialized)
	if err != nil {
		t.Fatalf("unexpected error deserializing: %v", err)
	}
	if len(deserialized) != 1 {
		t.Fatalf("expected 1 header entry, got %d", len(deserialized))
	}
	if deserialized[0].Key.Kind != String {
		t.Fatalf("expected key kind String, got %d", deserialized[0].Key.Kind)
	}
	if !bytes.Equal(deserialized[0].Key.Value, []byte("mykey")) {
		t.Fatalf("expected key value 'mykey', got %v", deserialized[0].Key.Value)
	}
	if deserialized[0].Value.Kind != String {
		t.Fatalf("expected value kind String, got %d", deserialized[0].Value.Kind)
	}
	if !bytes.Equal(deserialized[0].Value.Value, []byte("myval")) {
		t.Fatalf("expected value 'myval', got %v", deserialized[0].Value.Value)
	}
}
