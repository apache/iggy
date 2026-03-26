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
	"errors"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestSerializeIdentifier_StringId(t *testing.T) {
	// Test case for StringId
	identifier, _ := NewIdentifier("Hello")

	// Serialize the identifier
	serialized, err := identifier.MarshalBinary()
	if err != nil {
		t.Errorf("Error serializing identifier: %v", err)
	}

	// Expected serialized bytes for StringId
	expected := []byte{
		0x02,                         // Kind (StringId)
		0x05,                         // Length (5)
		0x48, 0x65, 0x6C, 0x6C, 0x6F, // Value ("Hello")
	}

	// Check if the serialized bytes match the expected bytes
	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect for StringId. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerializeIdentifier_NumericId(t *testing.T) {
	// Test case for NumericId
	identifier, _ := NewIdentifier(uint32(123))

	// Serialize the identifier
	serialized, err := identifier.MarshalBinary()
	if err != nil {
		t.Errorf("Error serializing identifier: %v", err)
	}

	// Expected serialized bytes for NumericId
	expected := []byte{
		0x01,                   // Kind (NumericId)
		0x04,                   // Length (4)
		0x7B, 0x00, 0x00, 0x00, // Value (123)
	}

	// Check if the serialized bytes match the expected bytes
	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect for NumericId. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerializeIdentifier_EmptyStringId(t *testing.T) {
	// Test case for an empty StringId
	_, err := NewIdentifier("")

	// Check if the serialized bytes match the expected bytes
	if !errors.Is(err, ierror.ErrInvalidIdentifier) {
		t.Errorf("Expected error: %v, got: %v", ierror.ErrInvalidIdentifier, err)
	}
}

func TestIdentifier_Uint32(t *testing.T) {
	id, err := NewIdentifier(uint32(42))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val, err := id.Uint32()
	if err != nil {
		t.Fatalf("unexpected error getting uint32: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestIdentifier_Uint32_StringIdReturnsError(t *testing.T) {
	id, err := NewIdentifier("hello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = id.Uint32()
	if err == nil {
		t.Fatalf("expected error when calling Uint32 on string identifier, got nil")
	}
}

func TestIdentifier_String(t *testing.T) {
	id, err := NewIdentifier("hello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val, err := id.String()
	if err != nil {
		t.Fatalf("unexpected error getting string: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got '%s'", val)
	}
}

func TestIdentifier_String_NumericIdReturnsError(t *testing.T) {
	id, err := NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = id.String()
	if err == nil {
		t.Fatalf("expected error when calling String on numeric identifier, got nil")
	}
}

func TestAppendBinary(t *testing.T) {
	id, err := NewIdentifier(uint32(10))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	prefix := []byte{0xAA, 0xBB}
	result, err := id.AppendBinary(prefix)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0] != 0xAA || result[1] != 0xBB {
		t.Fatalf("expected prefix bytes preserved, got %v", result[:2])
	}
	marshaledOnly, _ := id.MarshalBinary()
	if !bytes.Equal(result[2:], marshaledOnly) {
		t.Fatalf("expected appended bytes %v, got %v", marshaledOnly, result[2:])
	}
}

func TestMarshalIdentifiers(t *testing.T) {
	id1, err := NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	id2, err := NewIdentifier("ab")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	combined, err := MarshalIdentifiers(id1, id2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b1, _ := id1.MarshalBinary()
	b2, _ := id2.MarshalBinary()
	expected := append(b1, b2...)
	if !bytes.Equal(combined, expected) {
		t.Fatalf("expected %v, got %v", expected, combined)
	}
}
