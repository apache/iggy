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
	"encoding/binary"
	"testing"

	"github.com/google/uuid"
)

func TestNone_ReturnsBalancedWithEmptyValue(t *testing.T) {
	p := None()
	if p.Kind != Balanced {
		t.Fatalf("expected kind Balanced (%d), got %d", Balanced, p.Kind)
	}
	if p.Length != 0 {
		t.Fatalf("expected length 0, got %d", p.Length)
	}
	if len(p.Value) != 0 {
		t.Fatalf("expected empty value, got %v", p.Value)
	}
}

func TestPartitionId_ReturnsPartitionIdKindWithLEValue(t *testing.T) {
	p := PartitionId(42)
	if p.Kind != PartitionIdKind {
		t.Fatalf("expected kind PartitionIdKind (%d), got %d", PartitionIdKind, p.Kind)
	}
	if p.Length != 4 {
		t.Fatalf("expected length 4, got %d", p.Length)
	}
	expected := make([]byte, 4)
	binary.LittleEndian.PutUint32(expected, 42)
	if !bytes.Equal(p.Value, expected) {
		t.Fatalf("expected value %v, got %v", expected, p.Value)
	}
}

func TestEntityIdString_ReturnsMessageKeyWithStringBytes(t *testing.T) {
	p, err := EntityIdString("test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Kind != MessageKey {
		t.Fatalf("expected kind MessageKey (%d), got %d", MessageKey, p.Kind)
	}
	if p.Length != 4 {
		t.Fatalf("expected length 4, got %d", p.Length)
	}
	if !bytes.Equal(p.Value, []byte("test")) {
		t.Fatalf("expected value %v, got %v", []byte("test"), p.Value)
	}
}

func TestEntityIdString_EmptyReturnsError(t *testing.T) {
	_, err := EntityIdString("")
	if err == nil {
		t.Fatalf("expected error for empty string, got nil")
	}
}

func TestEntityIdBytes_ReturnsMessageKey(t *testing.T) {
	input := []byte{1, 2}
	p, err := EntityIdBytes(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Kind != MessageKey {
		t.Fatalf("expected kind MessageKey (%d), got %d", MessageKey, p.Kind)
	}
	if p.Length != 2 {
		t.Fatalf("expected length 2, got %d", p.Length)
	}
	if !bytes.Equal(p.Value, input) {
		t.Fatalf("expected value %v, got %v", input, p.Value)
	}
}

func TestEntityIdBytes_NilReturnsError(t *testing.T) {
	_, err := EntityIdBytes(nil)
	if err == nil {
		t.Fatalf("expected error for nil bytes, got nil")
	}
}

func TestEntityIdBytes_EmptyReturnsError(t *testing.T) {
	_, err := EntityIdBytes([]byte{})
	if err == nil {
		t.Fatalf("expected error for empty bytes, got nil")
	}
}

func TestEntityIdInt_ReturnsMessageKeyWithLEUint32(t *testing.T) {
	p := EntityIdInt(7)
	if p.Kind != MessageKey {
		t.Fatalf("expected kind MessageKey (%d), got %d", MessageKey, p.Kind)
	}
	if p.Length != 4 {
		t.Fatalf("expected length 4, got %d", p.Length)
	}
	expected := make([]byte, 4)
	binary.LittleEndian.PutUint32(expected, 7)
	if !bytes.Equal(p.Value, expected) {
		t.Fatalf("expected value %v, got %v", expected, p.Value)
	}
}

func TestEntityIdUlong_ReturnsMessageKeyWithLEUint64(t *testing.T) {
	p := EntityIdUlong(99)
	if p.Kind != MessageKey {
		t.Fatalf("expected kind MessageKey (%d), got %d", MessageKey, p.Kind)
	}
	if p.Length != 8 {
		t.Fatalf("expected length 8, got %d", p.Length)
	}
	expected := make([]byte, 8)
	binary.LittleEndian.PutUint64(expected, 99)
	if !bytes.Equal(p.Value, expected) {
		t.Fatalf("expected value %v, got %v", expected, p.Value)
	}
}

func TestEntityIdGuid_ReturnsMessageKeyWith16Bytes(t *testing.T) {
	fixedUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	p := EntityIdGuid(fixedUUID)
	if p.Kind != MessageKey {
		t.Fatalf("expected kind MessageKey (%d), got %d", MessageKey, p.Kind)
	}
	if p.Length != 16 {
		t.Fatalf("expected length 16, got %d", p.Length)
	}
	if !bytes.Equal(p.Value, fixedUUID[:]) {
		t.Fatalf("expected value %v, got %v", fixedUUID[:], p.Value)
	}
}

func TestPartitioning_MarshalBinary_Balanced(t *testing.T) {
	p := None()
	data, err := p.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []byte{byte(Balanced), 0x00}
	if !bytes.Equal(data, expected) {
		t.Fatalf("expected %v, got %v", expected, data)
	}
}

func TestPartitioning_MarshalBinary_PartitionId(t *testing.T) {
	p := PartitionId(42)
	data, err := p.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 6 {
		t.Fatalf("expected length 6, got %d", len(data))
	}
	if data[0] != byte(PartitionIdKind) {
		t.Fatalf("expected kind byte %d, got %d", PartitionIdKind, data[0])
	}
	if data[1] != 4 {
		t.Fatalf("expected length byte 4, got %d", data[1])
	}
	val := binary.LittleEndian.Uint32(data[2:6])
	if val != 42 {
		t.Fatalf("expected value 42, got %d", val)
	}
}
